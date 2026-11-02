use crate::{NativeClientError, NativeClientResult, Response};
use n1_link::noa::MAX_CONTROL_BYTES;
use serde::Serialize;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
pub(crate) async fn write<W: AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    value: &T,
) -> NativeClientResult<()> {
    let bytes = rmp_serde::to_vec_named(value)?;
    if bytes.is_empty() || bytes.len() > MAX_CONTROL_BYTES {
        return Err(NativeClientError::protocol("native control exceeds limit"));
    }
    tokio::time::timeout(Duration::from_secs(30), async {
        writer
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .await?;
        writer.write_all(&bytes).await?;
        writer.flush().await
    })
    .await
    .map_err(|_| NativeClientError::protocol("native control write stalled"))??;
    Ok(())
}
pub(crate) async fn read<R: AsyncRead + Unpin>(reader: &mut R) -> NativeClientResult<Response> {
    let mut prefix = [0; 4];
    tokio::time::timeout(Duration::from_secs(300), reader.read_exact(&mut prefix))
        .await
        .map_err(|_| NativeClientError::protocol("native response stalled"))??;
    let length = u32::from_be_bytes(prefix) as usize;
    if length == 0 || length > MAX_CONTROL_BYTES {
        return Err(NativeClientError::protocol(
            "native response control exceeds limit",
        ));
    }
    let mut bytes = vec![0; length];
    tokio::time::timeout(Duration::from_secs(30), reader.read_exact(&mut bytes))
        .await
        .map_err(|_| NativeClientError::protocol("native response control stalled"))??;
    Ok(rmp_serde::from_slice(&bytes)?)
}

pub(crate) async fn copy_exact<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut R,
    writer: &mut W,
    mut remaining: u64,
) -> NativeClientResult<()> {
    let mut buffer = vec![0; 128 * 1024];
    while remaining > 0 {
        let take = remaining.min(buffer.len() as u64) as usize;
        let n = tokio::time::timeout(Duration::from_secs(120), reader.read(&mut buffer[..take]))
            .await
            .map_err(|_| NativeClientError::protocol("native source stalled"))??;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "native body shorter than declared length",
            )
            .into());
        }
        tokio::time::timeout(Duration::from_secs(120), writer.write_all(&buffer[..n]))
            .await
            .map_err(|_| NativeClientError::protocol("native destination stalled"))??;
        remaining -= n as u64;
    }
    writer.flush().await?;
    Ok(())
}
