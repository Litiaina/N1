use crate::protocol::types::{
    MAX_NATIVE_BODY_BYTES, MAX_NATIVE_FRAME_BYTES, NativeRequest, NativeResponse,
};
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub async fn read_request<R>(reader: &mut R) -> io::Result<Option<NativeRequest>>
where
    R: AsyncRead + Unpin,
{
    let Some(mut request) = read_request_control(reader).await? else {
        return Ok(None);
    };
    if request.body.is_none() {
        if let Some(body_len) = request.body_len {
            request.body = Some(serde_bytes::ByteBuf::from(
                read_request_body(reader, body_len).await?,
            ));
        }
    }
    Ok(Some(request))
}

pub async fn read_request_control<R>(reader: &mut R) -> io::Result<Option<NativeRequest>>
where
    R: AsyncRead + Unpin,
{
    let Some(bytes) = read_frame(reader).await? else {
        return Ok(None);
    };
    rmp_serde::from_slice(&bytes)
        .map(Some)
        .map_err(invalid_data)
}

pub async fn read_request_body<R>(reader: &mut R, body_len: u64) -> io::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let body_len = usize::try_from(body_len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("native body exceeds {} bytes", MAX_NATIVE_BODY_BYTES),
        )
    })?;
    if body_len > MAX_NATIVE_BODY_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("native body exceeds {} bytes", MAX_NATIVE_BODY_BYTES),
        ));
    }
    let mut body = vec![0u8; body_len];
    if body_len > 0 {
        reader.read_exact(&mut body).await?;
    }
    Ok(body)
}

pub async fn write_response<W>(writer: &mut W, response: &NativeResponse) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let raw_body = response.body.as_ref().map(|body| body.as_slice());
    let mut control = response.clone();
    if let Some(body) = raw_body {
        if body.len() > MAX_NATIVE_BODY_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "native response body exceeds {} bytes",
                    MAX_NATIVE_BODY_BYTES
                ),
            ));
        }
        control.body = None;
        control.body_len = Some(body.len() as u64);
    }
    let bytes = rmp_serde::to_vec(&control).map_err(invalid_data)?;
    write_frame(writer, &bytes).await?;
    if let Some(body) = raw_body {
        writer.write_all(body).await?;
    }
    writer.flush().await
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
