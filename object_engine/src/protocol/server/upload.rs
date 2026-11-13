//! The native owner keeps its socket and engine lease until payload I/O drains.
//! Multipart bytes go directly to the encoder, without a task/body relay.
use super::write_frame;
use crate::{
    core::{Result, Store},
    middleware::auth_middleware::Claims,
    protocol::ops::Service,
};
use n1_link::noa::{Command, Response};
use serde_json::json;
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};

pub(super) async fn stream<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    socket: S,
    service: Arc<Service>,
    id: u64,
    claims: Claims,
    key: String,
    offset: u64,
    bytes: u64,
) -> Result<S> {
    let activity = service.store.activity.clone();
    activity
        .run(async move {
            let authorization = service.authorize(
                &claims,
                &Command::UploadPart {
                    object_key: key.clone(),
                    offset,
                },
            );
            part(
                socket,
                service.store.clone(),
                Part {
                    id,
                    scope: claims.sub.0,
                    key,
                    offset,
                    bytes,
                },
                authorization,
            )
            .await
        })
        .await
}

struct Part {
    id: u64,
    scope: [u8; 16],
    key: String,
    offset: u64,
    bytes: u64,
}

async fn part<S: AsyncRead + AsyncWrite + Unpin + Send>(
    mut socket: S,
    store: Arc<Store>,
    request: Part,
    authorization: Result<()>,
) -> Result<S> {
    let result = {
        let mut body = ProgressReader::new(&mut socket).take(request.bytes);
        let result = match authorization {
            Ok(()) => {
                store
                    .write_part_owned(request.scope, &request.key, request.offset, true, &mut body)
                    .await
            }
            Err(error) => Err(error),
        };
        // A rejected part must drain exactly its own remaining bytes before
        // the next control frame. A truncated/stalled body closes the socket.
        if body.limit() != 0 {
            tokio::io::copy(&mut body, &mut tokio::io::sink()).await?;
            if body.limit() != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "incomplete native upload body",
                )
                .into());
            }
        }
        result
    };
    let response = match result {
        Ok(()) => Response::ok(request.id, json!({"offset":request.offset,"complete":true})),
        Err(error) => Response::error(request.id, error.status(), error.to_string()),
    };
    write_frame(&mut socket, &response).await?;
    Ok(socket)
}

// Only waiting on the sender consumes this deadline. Time spent on the disk,
// admission, encoding or durability barriers cannot expire a progressing upload.
struct ProgressReader<'a, R> {
    socket: &'a mut R,
    idle: Pin<Box<tokio::time::Sleep>>,
    stalled: bool,
}
impl<'a, R> ProgressReader<'a, R> {
    fn new(socket: &'a mut R) -> Self {
        Self {
            socket,
            idle: Box::pin(tokio::time::sleep(Duration::from_secs(30))),
            stalled: false,
        }
    }
}
impl<R: AsyncRead + Unpin> AsyncRead for ProgressReader<'_, R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match Pin::new(&mut *this.socket).poll_read(cx, buffer) {
            Poll::Ready(result) => {
                this.stalled = false;
                Poll::Ready(result)
            }
            Poll::Pending => {
                if !this.stalled {
                    this.idle
                        .as_mut()
                        .reset(tokio::time::Instant::now() + Duration::from_secs(30));
                    this.stalled = true;
                }
                match this.idle.as_mut().poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(()) => Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "native upload sender stalled",
                    ))),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
