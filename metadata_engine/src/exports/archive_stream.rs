//! Hold one final chunk until source verification and pin/manifest cleanup
//! succeed. A known Content-Length must never hide a late producer failure.
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use std::io;
use tokio::{io::DuplexStream, sync::oneshot};
use tokio_util::io::ReaderStream;

pub(super) fn completed_stream(
    reader: DuplexStream,
    completion: oneshot::Receiver<io::Result<()>>,
) -> impl Stream<Item = io::Result<Bytes>> + Send {
    let state = (ReaderStream::new(reader), Some(completion), None, false);
    futures_util::stream::unfold(
        state,
        |(mut source, mut completion, mut pending, done)| async move {
            if done {
                return None;
            }
            loop {
                match source.next().await {
                    Some(Ok(bytes)) => {
                        if let Some(previous) = pending.replace(bytes) {
                            return Some((Ok(previous), (source, completion, pending, false)));
                        }
                    }
                    Some(Err(error)) => {
                        return Some((Err(error), (source, completion, None, true)));
                    }
                    None => {
                        let result = completion
                            .take()
                            .expect("one terminal completion")
                            .await
                            .unwrap_or_else(|_| {
                                Err(io::Error::other("archive worker stopped before completion"))
                            });
                        let last = match result {
                            Ok(()) => Ok(pending.unwrap_or_default()),
                            Err(error) => Err(error),
                        };
                        return Some((last, (source, None, None, true)));
                    }
                }
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::TryStreamExt;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn final_bytes_wait_for_verified_cleanup() {
        let (reader, mut writer) = tokio::io::duplex(1024);
        writer.write_all(b"complete archive").await.unwrap();
        drop(writer);
        let (sender, receiver) = oneshot::channel();
        let stream = completed_stream(reader, receiver);
        tokio::pin!(stream);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), stream.next())
                .await
                .is_err()
        );
        sender.send(Ok(())).unwrap();
        assert_eq!(
            stream.try_next().await.unwrap().unwrap(),
            "complete archive"
        );
        assert!(stream.try_next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn late_errors_and_missing_workers_cannot_complete_an_archive() {
        for missing_worker in [false, true] {
            let (reader, mut writer) = tokio::io::duplex(1024);
            writer.write_all(b"unverified final bytes").await.unwrap();
            drop(writer);
            let (sender, receiver) = oneshot::channel();
            if missing_worker {
                drop(sender);
            } else {
                sender
                    .send(Err(io::Error::other("source or cleanup failed")))
                    .unwrap();
            }
            let stream = completed_stream(reader, receiver);
            tokio::pin!(stream);
            assert!(stream.try_next().await.is_err());
            assert!(stream.try_next().await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn earlier_chunks_stream_before_terminal_completion() {
        let (reader, mut writer) = tokio::io::duplex(1024);
        let (sender, receiver) = oneshot::channel();
        let producer = tokio::spawn(async move {
            writer.write_all(&vec![7; 32768]).await.unwrap();
        });
        let stream = completed_stream(reader, receiver);
        tokio::pin!(stream);
        let first = tokio::time::timeout(Duration::from_secs(1), stream.try_next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(!first.is_empty());
        // Completion is deliberately still pending while data are flowing.
        sender.send(Ok(())).unwrap();
        let mut total = first.len();
        while let Some(chunk) = stream.try_next().await.unwrap() {
            total += chunk.len();
        }
        producer.await.unwrap();
        assert_eq!(total, 32768);
    }
}
