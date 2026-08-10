use crate::error::{NativeClientError, NativeClientResult};
use crate::types::{MAX_NATIVE_BODY_BYTES, MAX_NATIVE_FRAME_BYTES, NativeResponse};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub async fn write_control_frame<W, T>(writer: &mut W, value: &T) -> NativeClientResult<()>
where
    W: AsyncWrite + Unpin,
    T: serde::Serialize,
{
    let bytes = rmp_serde::to_vec(value)?;
    write_frame(writer, &bytes).await
}

pub async fn read_response<R>(reader: &mut R) -> NativeClientResult<NativeResponse>
where
    R: AsyncRead + Unpin,
{
    let bytes = read_frame(reader).await?;
    let response = rmp_serde::from_slice::<NativeResponse>(&bytes)?;
    Ok(response)
}

pub async fn read_response_body<R>(
    reader: &mut R,
    response: &mut NativeResponse,
) -> NativeClientResult<()>
where
    R: AsyncRead + Unpin,
{
    if response.body.is_some() {
        return Ok(());
    }
    let Some(body_len) = response.body_len else {
        return Ok(());
    };
    let body_len = usize::try_from(body_len)
        .map_err(|_| NativeClientError::protocol("native response body length overflow"))?;
    if body_len > MAX_NATIVE_BODY_BYTES {
        return Err(NativeClientError::protocol(format!(
            "native response body exceeds {MAX_NATIVE_BODY_BYTES} bytes"
        )));
    }
    let mut body = vec![0u8; body_len];
    if !body.is_empty() {
        reader.read_exact(&mut body).await?;
    }
    response.body = Some(serde_bytes::ByteBuf::from(body));
    Ok(())
}

pub async fn stream_response_body<R, F>(
    reader: &mut R,
    body_len: u64,
    mut on_chunk: F,
) -> NativeClientResult<u64>
where
    R: AsyncRead + Unpin,
    F: FnMut(&[u8]) -> NativeClientResult<()>,
{
    if body_len > MAX_NATIVE_BODY_BYTES as u64 {
        return Err(NativeClientError::protocol(format!(
            "native response body exceeds {MAX_NATIVE_BODY_BYTES} bytes"
        )));
    }

    let mut remaining = body_len;
    let mut total = 0u64;
    let mut buffer = vec![0u8; 256 * 1024];
    while remaining > 0 {
        let take = buffer.len().min(remaining as usize);
        reader.read_exact(&mut buffer[..take]).await?;
        on_chunk(&buffer[..take])?;
        remaining -= take as u64;
        total += take as u64;
    }
    Ok(total)
}

async fn read_frame<R>(reader: &mut R) -> NativeClientResult<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len == 0 || len > MAX_NATIVE_FRAME_BYTES {
        return Err(NativeClientError::protocol(format!(
            "native frame size {len} is invalid"
        )));
    }
    let mut frame = vec![0u8; len];
    reader.read_exact(&mut frame).await?;
    Ok(frame)
}

async fn write_frame<W>(writer: &mut W, bytes: &[u8]) -> NativeClientResult<()>
where
    W: AsyncWrite + Unpin,
{
    if bytes.is_empty() || bytes.len() > MAX_NATIVE_FRAME_BYTES {
        return Err(NativeClientError::protocol(format!(
            "native frame size {} is invalid",
            bytes.len()
        )));
    }
    writer
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .await?;
    writer.write_all(bytes).await?;
    Ok(())
}
