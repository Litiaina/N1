use crate::error::{NativeClientError, NativeClientResult};
use crate::frame::{read_response, read_response_body, stream_response_body, write_control_frame};
use crate::types::{NativeAuthFragmentRequest, NativeRequest, NativeResponse};
use crate::url::parse_native_address;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

pub struct N1NativeClient {
    stream: TcpStream,
    next_id: u64,
    request_prefix: String,
}

pub struct NativeOperationResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

impl N1NativeClient {
    pub async fn connect(url: &str) -> NativeClientResult<Self> {
        let address = parse_native_address(url)?;
        let stream = TcpStream::connect(address).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            next_id: 0,
            request_prefix: "n1-native-client".to_string(),
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

        let response = read_response(&mut self.stream).await?;
        ensure_response_id(&response, &id)?;
        let body_len = response.body_len.unwrap_or(0);
        let bytes =
            stream_response_body(&mut self.stream, body_len, |chunk| on_chunk(chunk)).await?;
        Ok((response, bytes))
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
        .unwrap_or_else(|| {
            (
                "native_error".to_string(),
                "unknown native error".to_string(),
            )
        });
    Err(NativeClientError::Native { code, message })
}
