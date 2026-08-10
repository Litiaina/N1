use axum::body::Body;
use futures_util::TryStreamExt;
use n2_native_client::{N2NativeClient, NativeClientError, NativeOperationResponse};
use serde_json::{Value, json};
use tokio::io::AsyncWrite;
use tokio_util::io::StreamReader;

use crate::config::NodeConfig;

#[derive(Debug)]
pub struct NativeCallError {
    pub status: Option<u16>,
    pub message: String,
}

impl NativeCallError {
    fn contextual(context: String, error: NativeClientError) -> Self {
        let status = match &error {
            NativeClientError::Native { status, .. } => *status,
            _ => None,
        };
        Self {
            status,
            message: format!("{context}: {error}"),
        }
    }

    fn direct(error: NativeClientError) -> Self {
        let status = match &error {
            NativeClientError::Native { status, .. } => *status,
            _ => None,
        };
        Self {
            status,
            message: error.to_string(),
        }
    }
}

pub struct NativeDownload {
    client: N2NativeClient,
    pub status: u16,
    pub body_len: u64,
}

pub struct NativeDownloadError {
    pub status: Option<u16>,
    pub message: String,
}

impl NativeDownload {
    pub async fn stream_to_writer<W>(mut self, writer: &mut W) -> Result<u64, String>
    where
        W: AsyncWrite + Unpin,
    {
        self.client
            .stream_response_body_to_writer(self.body_len, writer)
            .await
            .map_err(native_error)
    }
}

pub async fn connect_with_bearer(
    node: &NodeConfig,
    bearer: &str,
) -> Result<N2NativeClient, NativeCallError> {
    let mut client = N2NativeClient::connect(&node.native_url)
        .await
        .map_err(|err| NativeCallError::contextual(format!("connect {}", node.native_url), err))?
        .with_request_prefix("n2-gateway");
    client.auth_bearer(bearer).await.map_err(|err| {
        NativeCallError::contextual(format!("native bearer auth on {}", node.id), err)
    })?;
    Ok(client)
}

pub async fn native_json(
    node: &NodeConfig,
    bearer: &str,
    op: &str,
    payload: Value,
) -> Result<(u16, Value), NativeCallError> {
    let mut client = connect_with_bearer(node, bearer).await?;
    let response = client
        .operation(op, &payload, None)
        .await
        .map_err(NativeCallError::direct)?;
    decode_operation_response(response).map_err(|message| NativeCallError {
        status: None,
        message,
    })
}

pub async fn native_upload_body(
    node: &NodeConfig,
    bearer: &str,
    op: &str,
    payload: Value,
    body: Body,
    body_len: u64,
) -> Result<(u16, Value), NativeCallError> {
    let mut client = connect_with_bearer(node, bearer).await?;
    let stream = body
        .into_data_stream()
        .map_err(|err| std::io::Error::other(err.to_string()));
    let mut reader = StreamReader::new(stream);
    let response = client
        .operation_reader(op, &payload, &mut reader, body_len)
        .await
        .map_err(NativeCallError::direct)?;
    decode_operation_response(response).map_err(|message| NativeCallError {
        status: None,
        message,
    })
}

pub async fn begin_native_download(
    node: &NodeConfig,
    bearer: &str,
    payload: Value,
) -> Result<NativeDownload, NativeDownloadError> {
    let mut client =
        connect_with_bearer(node, bearer)
            .await
            .map_err(|error| NativeDownloadError {
                status: error.status,
                message: error.message,
            })?;
    let response = client
        .begin_stream_response("object.download_stream", &payload)
        .await
        .map_err(|err| NativeDownloadError {
            status: None,
            message: native_error(err),
        })?;
    if !response.ok {
        let message = response
            .error
            .map(|err| format!("{}: {}", err.code, err.message))
            .unwrap_or_else(|| "native download failed".to_string());
        return Err(NativeDownloadError {
            status: response.status,
            message,
        });
    }
    Ok(NativeDownload {
        status: response.status.unwrap_or(200),
        body_len: response.body_len.unwrap_or(0),
        client,
    })
}

pub fn msgpack_to_json(body: &[u8]) -> Result<Value, String> {
    if body.is_empty() {
        return Ok(json!({}));
    }
    rmp_serde::from_slice::<Value>(body).map_err(|err| format!("decode native body: {err}"))
}

fn decode_operation_response(response: NativeOperationResponse) -> Result<(u16, Value), String> {
    Ok((response.status, msgpack_to_json(&response.body)?))
}

fn native_error(err: NativeClientError) -> String {
    err.to_string()
}

pub fn bearer_from_headers(headers: &axum::http::HeaderMap) -> Result<String, String> {
    let value = headers
        .get(axum::http::header::AUTHORIZATION)
        .ok_or_else(|| "missing Authorization bearer token".to_string())?
        .to_str()
        .map_err(|_| "Authorization header is not valid UTF-8".to_string())?;
    let Some(token) = value.strip_prefix("Bearer ") else {
        return Err("Authorization header must be Bearer token".to_string());
    };
    Ok(token.trim().to_string())
}

pub fn content_length(headers: &axum::http::HeaderMap) -> Result<u64, String> {
    headers
        .get(axum::http::header::CONTENT_LENGTH)
        .ok_or_else(|| "Content-Length is required for native gateway streaming".to_string())?
        .to_str()
        .map_err(|_| "Content-Length header is not valid UTF-8".to_string())?
        .parse::<u64>()
        .map_err(|err| format!("invalid Content-Length: {err}"))
}

pub fn content_type(headers: &axum::http::HeaderMap) -> Option<String> {
    headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string())
}
