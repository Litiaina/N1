use crate::error::{NativeClientError, NativeClientResult};
use crate::frame::{read_response, read_response_body, stream_response_body, write_control_frame};
use crate::types::{NativeAuthFragmentRequest, NativeBearerRequest, NativeRequest, NativeResponse};
use crate::url::parse_native_address;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

const NATIVE_STREAM_BUFFER_BYTES: usize = 1024 * 1024;

pub struct N2NativeClient {
    stream: TcpStream,
    next_id: u64,
    request_prefix: String,
}

pub struct NativeOperationResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

impl N2NativeClient {
    pub async fn connect(url: &str) -> NativeClientResult<Self> {
        let address = parse_native_address(url)?;
        let stream = TcpStream::connect(address).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            next_id: 0,
            request_prefix: "n2-native-client".to_string(),
        })
    }

    pub fn with_request_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.request_prefix = prefix.into();
        self
    }

    pub async fn auth_fragment(&mut self, fragment: &str, secret: &str) -> NativeClientResult<()> {
        let response = self
            .round_trip_json(
                "auth.fragment",
                &NativeAuthFragmentRequest {
                    fragment: fragment.to_string(),
                    secret: secret.to_string(),
                    read: true,
                    write: true,
                    modify: true,
                    delete: true,
                    expires_in: Some(21_600),
                },
                None,
            )
            .await?;
        ensure_native_ok(&response)?;
        Ok(())
    }

    pub async fn auth_bearer(&mut self, token: &str) -> NativeClientResult<()> {
        let response = self
            .round_trip_json(
                "auth.bearer",
                &NativeBearerRequest {
                    token: token.to_string(),
                },
                None,
            )
            .await?;
        ensure_native_ok(&response)?;
        Ok(())
    }

    pub async fn raw_json<T: Serialize>(
        &mut self,
        op: &str,
        payload: &T,
        body: Option<Vec<u8>>,
    ) -> NativeClientResult<Value> {
        let response = self.round_trip_json(op, payload, body).await?;
        ensure_native_ok(&response)?;
        let payload = response.payload.unwrap_or_else(|| "null".to_string());
        Ok(serde_json::from_str(&payload)?)
    }

    pub async fn operation<T: Serialize>(
        &mut self,
        op: &str,
        payload: &T,
        body: Option<Vec<u8>>,
    ) -> NativeClientResult<NativeOperationResponse> {
        let response = self.round_trip_json(op, payload, body).await?;
        ensure_native_ok(&response)?;
        Ok(NativeOperationResponse {
            status: response.status.unwrap_or(500),
            body: response
                .body
                .map(|body| body.into_vec())
                .unwrap_or_default(),
        })
    }

    pub async fn operation_json<T, R>(
        &mut self,
        op: &str,
        payload: &T,
        body: Option<Vec<u8>>,
    ) -> NativeClientResult<R>
    where
        T: Serialize,
        R: DeserializeOwned,
    {
        let response = self.operation(op, payload, body).await?;
        Ok(rmp_serde::from_slice(&response.body)?)
    }

    pub async fn operation_reader<T, R>(
        &mut self,
        op: &str,
        payload: &T,
        reader: &mut R,
        body_len: u64,
    ) -> NativeClientResult<NativeOperationResponse>
    where
        T: Serialize,
        R: AsyncRead + Unpin,
    {
        let response = self
            .round_trip_reader(op, Some(serde_json::to_string(payload)?), reader, body_len)
            .await?;
        ensure_native_ok(&response)?;
        Ok(NativeOperationResponse {
            status: response.status.unwrap_or(500),
            body: response
                .body
                .map(|body| body.into_vec())
                .unwrap_or_default(),
        })
    }

    pub async fn round_trip_json<T: Serialize>(
        &mut self,
        op: &str,
        payload: &T,
        body: Option<Vec<u8>>,
    ) -> NativeClientResult<NativeResponse> {
        let payload = serde_json::to_string(payload)?;
        self.round_trip(op, Some(payload), body).await
    }

    pub async fn round_trip(
        &mut self,
        op: &str,
        payload: Option<String>,
        body: Option<Vec<u8>>,
    ) -> NativeClientResult<NativeResponse> {
        self.next_id = self.next_id.saturating_add(1);
        let id = format!("{}-{}", self.request_prefix, self.next_id);
        let request = NativeRequest {
            id: id.clone(),
            op: op.to_string(),
            payload,
            body: None,
            body_len: body.as_ref().map(|body| body.len() as u64),
        };
        write_control_frame(&mut self.stream, &request).await?;
        if let Some(body) = body {
            self.stream.write_all(&body).await?;
        }
        self.stream.flush().await?;

        let mut response = read_response(&mut self.stream).await?;
        read_response_body(&mut self.stream, &mut response).await?;
        ensure_response_id(&response, &id)?;
        Ok(response)
    }

    pub async fn round_trip_reader<R>(
        &mut self,
        op: &str,
        payload: Option<String>,
        reader: &mut R,
        body_len: u64,
    ) -> NativeClientResult<NativeResponse>
    where
        R: AsyncRead + Unpin,
    {
        self.next_id = self.next_id.saturating_add(1);
        let id = format!("{}-{}", self.request_prefix, self.next_id);
        let request = NativeRequest {
            id: id.clone(),
            op: op.to_string(),
            payload,
            body: None,
            body_len: Some(body_len),
        };
        write_control_frame(&mut self.stream, &request).await?;
        let mut buffered = BufReader::with_capacity(NATIVE_STREAM_BUFFER_BYTES, reader);
        let copied = tokio::io::copy_buf(&mut buffered, &mut self.stream).await?;
        if copied != body_len {
            return Err(NativeClientError::protocol(format!(
                "native request body copied {copied} bytes, expected {body_len}"
            )));
        }
        self.stream.flush().await?;

        let mut response = read_response(&mut self.stream).await?;
        read_response_body(&mut self.stream, &mut response).await?;
        ensure_response_id(&response, &id)?;
        Ok(response)
    }

    pub async fn stream_response<T, F>(
        &mut self,
        op: &str,
        payload: &T,
        mut on_chunk: F,
    ) -> NativeClientResult<(NativeResponse, u64)>
    where
        T: Serialize,
        F: FnMut(&[u8]) -> NativeClientResult<()>,
    {
        self.next_id = self.next_id.saturating_add(1);
        let id = format!("{}-{}", self.request_prefix, self.next_id);
        let request = NativeRequest {
            id: id.clone(),
            op: op.to_string(),
            payload: Some(serde_json::to_string(payload)?),
            body: None,
            body_len: None,
        };
        write_control_frame(&mut self.stream, &request).await?;
        self.stream.flush().await?;

        let mut response = read_response(&mut self.stream).await?;
        ensure_response_id(&response, &id)?;
        if !response.ok {
            read_response_body(&mut self.stream, &mut response).await?;
            return Ok((response, 0));
        }
        let body_len = response.body_len.unwrap_or(0);
        let bytes =
            stream_response_body(&mut self.stream, body_len, |chunk| on_chunk(chunk)).await?;
        Ok((response, bytes))
    }

    pub async fn stream_response_to_writer<T, W>(
        &mut self,
        op: &str,
        payload: &T,
        writer: &mut W,
    ) -> NativeClientResult<(NativeResponse, u64)>
    where
        T: Serialize,
        W: AsyncWrite + Unpin,
    {
        let response = self.begin_stream_response(op, payload).await?;
        let bytes = self
            .stream_response_body_to_writer(response.body_len.unwrap_or(0), writer)
            .await?;
        Ok((response, bytes))
    }

    pub async fn begin_stream_response<T: Serialize>(
        &mut self,
        op: &str,
        payload: &T,
    ) -> NativeClientResult<NativeResponse> {
        self.next_id = self.next_id.saturating_add(1);
        let id = format!("{}-{}", self.request_prefix, self.next_id);
        let request = NativeRequest {
            id: id.clone(),
            op: op.to_string(),
            payload: Some(serde_json::to_string(payload)?),
            body: None,
            body_len: None,
        };
        write_control_frame(&mut self.stream, &request).await?;
        self.stream.flush().await?;

        let response = read_response(&mut self.stream).await?;
        ensure_response_id(&response, &id)?;
        Ok(response)
    }

    pub async fn stream_response_body_to_writer<W>(
        &mut self,
        body_len: u64,
        writer: &mut W,
    ) -> NativeClientResult<u64>
    where
        W: AsyncWrite + Unpin,
    {
        let limited = (&mut self.stream).take(body_len);
        let mut buffered = BufReader::with_capacity(NATIVE_STREAM_BUFFER_BYTES, limited);
        let bytes = tokio::io::copy_buf(&mut buffered, writer).await?;
        if bytes != body_len {
            return Err(NativeClientError::protocol(format!(
                "native response streamed {bytes} bytes, expected {body_len}"
            )));
        }
        Ok(bytes)
    }
}

fn ensure_response_id(response: &NativeResponse, expected: &str) -> NativeClientResult<()> {
    if response.id == expected {
        return Ok(());
    }
    Err(NativeClientError::protocol(format!(
        "native response id mismatch: got {} expected {expected}",
        response.id
    )))
}

pub fn ensure_native_ok(response: &NativeResponse) -> NativeClientResult<()> {
    if response.ok {
        return Ok(());
    }
    let (code, message) = response
        .error
        .as_ref()
        .map(|err| (err.code.clone(), err.message.clone()))
        .or_else(|| {
            response.body.as_ref().and_then(|body| {
                let value = rmp_serde::from_slice::<Value>(body.as_ref()).ok()?;
                let message = value
                    .get("error")
                    .and_then(Value::as_str)
                    .or_else(|| value.get("message").and_then(Value::as_str))
                    .map(str::to_string)?;
                let code = response
                    .status
                    .map(|status| format!("status_{status}"))
                    .unwrap_or_else(|| "native_error".to_string());
                Some((code, message))
            })
        })
        .unwrap_or_else(|| {
            (
                "native_error".to_string(),
                "unknown native error".to_string(),
            )
        });
    Err(NativeClientError::Native { code, message })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_bytes::ByteBuf;
    use serde_json::json;

    #[test]
    fn ensure_native_ok_reports_msgpack_error_body() {
        let response = NativeResponse {
            id: "test".to_string(),
            ok: false,
            status: Some(400),
            payload: None,
            body: Some(ByteBuf::from(
                rmp_serde::to_vec_named(&json!({ "error": "Fragment not found" })).unwrap(),
            )),
            body_len: None,
            error: None,
        };

        let err = ensure_native_ok(&response).expect_err("response should fail");
        assert!(matches!(
            err,
            NativeClientError::Native { code, message }
                if code == "status_400" && message == "Fragment not found"
        ));
    }

    #[test]
    fn ensure_native_ok_reports_control_frame_error() {
        let response = NativeResponse {
            id: "test".to_string(),
            ok: false,
            status: Some(404),
            payload: None,
            body: None,
            body_len: None,
            error: Some(crate::types::NativeError {
                code: "status_404".to_string(),
                message: "Not found".to_string(),
            }),
        };

        let err = ensure_native_ok(&response).expect_err("response should fail");
        assert!(matches!(
            err,
            NativeClientError::Native { code, message }
                if code == "status_404" && message == "Not found"
        ));
    }
}
