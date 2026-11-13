//! Fragmented transport reads must preserve framing, rejection draining and EOF.
use super::super::feed_body;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

struct Fragmented<'a> {
    remaining: &'a [u8],
    chunk: usize,
}
impl AsyncRead for Fragmented<'_> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let length = self.remaining.len().min(self.chunk).min(out.remaining());
        out.put_slice(&self.remaining[..length]);
        self.remaining = &self.remaining[length..];
        Poll::Ready(Ok(()))
    }
}

#[derive(Default)]
struct Receiver {
    bytes: Vec<u8>,
    attempts: usize,
    reject: bool,
}
impl AsyncWrite for Receiver {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.attempts += 1;
        if self.reject {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        self.bytes.extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn fragmented_reads_coalesce_without_consuming_the_next_control_frame() {
    let length = 2 * 128 * 1024 + 31;
    let mut source = (0..length).map(|i| (i % 251) as u8).collect::<Vec<_>>();
    source.extend_from_slice(b"next control frame");
    for chunk in [1, 17, 16384] {
        let mut reader = Fragmented {
            remaining: &source,
            chunk,
        };
        let mut receiver = Receiver::default();
        feed_body(&mut reader, &mut receiver, length as u64)
            .await
            .unwrap();
        assert_eq!(receiver.bytes, source[..length]);
        assert_eq!(
            receiver.attempts, 3,
            "handoffs are bounded chunks, not transport fragments"
        );
        assert_eq!(reader.remaining, b"next control frame");
    }
}

#[tokio::test]
async fn rejected_upload_drains_exactly_its_declared_body() {
    let length = 2 * 128 * 1024 + 31;
    let mut source = vec![3; length];
    source.extend_from_slice(b"next control frame");
    let mut reader = Fragmented {
        remaining: &source,
        chunk: 16384,
    };
    let mut receiver = Receiver {
        reject: true,
        ..Default::default()
    };
    feed_body(&mut reader, &mut receiver, length as u64)
        .await
        .unwrap();
    assert_eq!(receiver.attempts, 1);
    assert!(receiver.bytes.is_empty());
    assert_eq!(reader.remaining, b"next control frame");
}

#[tokio::test]
async fn truncated_final_chunk_is_an_error_even_after_receiver_rejection() {
    let source = vec![4; 128 * 1024 + 17];
    for reject in [false, true] {
        let mut reader = Fragmented {
            remaining: &source,
            chunk: 16384,
        };
        let mut receiver = Receiver {
            reject,
            ..Default::default()
        };
        let result = feed_body(&mut reader, &mut receiver, (source.len() + 1) as u64).await;
        assert!(
            matches!(result, Err(crate::core::Error::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof)
        );
        assert!(reader.remaining.is_empty());
        assert_eq!(receiver.bytes.len(), if reject { 0 } else { 128 * 1024 });
    }
}
