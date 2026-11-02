//! The native worker owns its socket until admitted reads and writes drain.
//! Verified payload goes directly to the transport, without a body relay.
use super::write_frame;
use crate::core::{Error, Result, Store};
use n1_link::noa::Response;
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::AsyncWrite;

pub(super) async fn stream<S: AsyncWrite + Unpin + Send + 'static>(
    mut socket: S,
    store: Arc<Store>,
    id: u64,
    scope: [u8; 16],
    key: String,
    offset: u64,
    length: Option<u64>,
) -> Result<S> {
    let activity = store.activity.clone();
    activity
        .run(async move {
            match store.prepare_download(scope, key, offset, length).await {
                Ok(download) => {
                    let mut response = Response::ok(
                        id,
                        serde_json::to_value(download.info())
                            .map_err(|e| Error::Invalid(e.to_string()))?,
                    );
                    response.body_len = download.length;
                    write_frame(&mut socket, &response).await?;
                    download
                        .write(&mut ProgressWriter {
                            socket: &mut socket,
                            idle: Box::pin(tokio::time::sleep(Duration::from_secs(30))),
                            stalled: false,
                        })
                        .await?;
                }
                Err(error) => {
                    write_frame(
                        &mut socket,
                        &Response::error(id, error.status(), error.to_string()),
                    )
                    .await?;
                }
            }
            Ok(socket)
        })
        .await
}

// Only stalled transport I/O has a deadline. Disk/verification time and the
// total object-transfer duration do not consume the socket's progress budget.
struct ProgressWriter<'a, W> {
    socket: &'a mut W,
    idle: Pin<Box<tokio::time::Sleep>>,
    stalled: bool,
}
impl<W> ProgressWriter<'_, W> {
    fn progress<T>(
        &mut self,
        cx: &mut Context<'_>,
        result: Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        match result {
            Poll::Ready(result) => {
                self.stalled = false;
                Poll::Ready(result)
            }
            Poll::Pending => {
                if !self.stalled {
                    self.idle
                        .as_mut()
                        .reset(tokio::time::Instant::now() + Duration::from_secs(30));
                    self.stalled = true;
                }
                match self.idle.as_mut().poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(()) => Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "native download receiver stalled",
                    ))),
                }
            }
        }
    }
}
impl<W: AsyncWrite + Unpin> AsyncWrite for ProgressWriter<'_, W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut *this.socket).poll_write(cx, bytes);
        this.progress(cx, result)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let result = Pin::new(&mut *this.socket).poll_flush(cx);
        this.progress(cx, result)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let result = Pin::new(&mut *this.socket).poll_shutdown(cx);
        this.progress(cx, result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::tests::Fixture;
    use n1_link::Durability;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn cancelled_native_caller_keeps_socket_and_shard_ownership_until_io_drains() {
        let fixture = Fixture::new();
        let store = fixture.enroll();
        let scope = [11; 16];
        let data = vec![73; 512 * 1024];
        let stripe = fixture.layout.stripe_bytes();
        let init = store
            .init(
                scope,
                0,
                data.len() as u64,
                (data.len() as u64).div_ceil(stripe) * stripe,
                Durability::Durable,
                3600,
            )
            .unwrap();
        store
            .put(scope, init.upload_key, std::io::Cursor::new(data.clone()))
            .await
            .unwrap();
        let (sender, mut receiver) = tokio::io::duplex(4096);
        let caller = tokio::spawn(stream(
            sender,
            store.clone(),
            17,
            scope,
            init.object_key.clone(),
            0,
            None,
        ));
        let size = receiver.read_u32().await.unwrap() as usize;
        let mut control = vec![0; size];
        receiver.read_exact(&mut control).await.unwrap();
        let response: Response = rmp_serde::from_slice(&control).unwrap();
        assert_eq!(response.id, 17);
        assert_eq!(response.status, 200);
        assert_eq!(response.body_len, data.len() as u64);
        assert_eq!(receiver.read_u8().await.unwrap(), 73);
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        let deletion = {
            let store = store.clone();
            let key = init.object_key.clone();
            tokio::spawn(async move { store.delete(scope, key).await })
        };
        let mut deletion = deletion;
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut deletion)
                .await
                .is_err()
        );
        drop(receiver);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), deletion)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            4
        );
        store.activity.wait_idle().await.unwrap();
        assert!(matches!(
            store.stat(scope, init.object_key).await,
            Err(Error::Retired)
        ));
    }

    struct StalledSocket {
        first_byte: bool,
    }
    impl AsyncWrite for StalledSocket {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            if self.first_byte && !bytes.is_empty() {
                self.first_byte = false;
                Poll::Ready(Ok(1))
            } else {
                Poll::Pending
            }
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn socket_stall_deadline_expires_and_progress_resets_it() {
        use tokio::io::AsyncWriteExt;
        let mut socket = StalledSocket { first_byte: false };
        let mut writer = ProgressWriter {
            socket: &mut socket,
            idle: Box::pin(tokio::time::sleep(Duration::ZERO)),
            stalled: true,
        };
        assert_eq!(
            writer.write_all(b"a").await.unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );

        let mut socket = StalledSocket { first_byte: true };
        let mut writer = ProgressWriter {
            socket: &mut socket,
            idle: Box::pin(tokio::time::sleep(Duration::ZERO)),
            stalled: true,
        };
        // The first byte makes progress. The next stall gets a fresh budget,
        // even though the previous transport deadline has already expired.
        assert!(
            tokio::time::timeout(Duration::from_millis(10), writer.write_all(b"ab"))
                .await
                .is_err()
        );
    }
}
