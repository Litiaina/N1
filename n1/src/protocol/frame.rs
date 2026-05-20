use crate::protocol::types::{
    MAX_NATIVE_BODY_BYTES, MAX_NATIVE_FRAME_BYTES, NativeRequest, NativeResponse,
};
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub async fn read_request<R>(reader: &mut R) -> io::Result<Option<NativeRequest>>
where
    R: AsyncRead + Unpin,
{
    let Some(bytes) = read_frame(reader).await? else {
        return Ok(None);
    };
    let mut request: NativeRequest = rmp_serde::from_slice(&bytes).map_err(invalid_data)?;
    if request.body.is_none() {
        if let Some(body_len) = request.body_len {
            let body_len = body_len as usize;
            if body_len > MAX_NATIVE_BODY_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("native body exceeds {} bytes", MAX_NATIVE_BODY_BYTES),
                ));
            }
            if body_len > 0 {
                let mut body = vec![0u8; body_len];
                reader.read_exact(&mut body).await?;
                request.body = Some(serde_bytes::ByteBuf::from(body));
            }
        }
    }
    Ok(Some(request))
}

pub async fn write_response<W>(writer: &mut W, response: &NativeResponse) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let bytes = rmp_serde::to_vec(response).map_err(invalid_data)?;
    write_frame(writer, &bytes).await
}

async fn read_frame<R>(reader: &mut R) -> io::Result<Option<Vec<u8>>>
where
    R: AsyncRead + Unpin,
{
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(err)
            if matches!(
                err.kind(),
                io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::BrokenPipe
            ) =>
        {
            return Ok(None);
        }
        Err(err) => return Err(err),
    }

    let len = u32::from_be_bytes(len_buf) as usize;
    if len == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty native frame",
        ));
    }
    if len > MAX_NATIVE_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("native frame exceeds {} bytes", MAX_NATIVE_FRAME_BYTES),
        ));
    }

    let mut bytes = vec![0u8; len];
    reader.read_exact(&mut bytes).await?;
    Ok(Some(bytes))
}

async fn write_frame<W>(writer: &mut W, bytes: &[u8]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    if bytes.is_empty() || bytes.len() > MAX_NATIVE_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("native frame size {} is not supported", bytes.len()),
        ));
    }
    writer
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .await?;
    writer.write_all(bytes).await?;
    writer.flush().await
}

fn invalid_data(err: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err.to_string())
}
