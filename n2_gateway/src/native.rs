use axum::body::Body;
use futures_util::TryStreamExt;
use n2_native_client::{N2NativeClient, NativeClientError, NativeOperationResponse};
use serde_json::{Value, json};
use tokio::io::AsyncWrite;
use tokio_util::io::StreamReader;

use crate::config::NodeConfig;

pub async fn connect_with_bearer(
    node: &NodeConfig,
    bearer: &str,
) -> Result<N2NativeClient, String> {
    let mut client = N2NativeClient::connect(&node.native_url)
        .await
        .map_err(|err| format!("connect {}: {err}", node.native_url))?
        .with_request_prefix("n2-gateway");
    client
        .auth_bearer(bearer)
        .await
        .map_err(|err| format!("native bearer auth on {}: {err}", node.id))?;
    Ok(client)
}

pub async fn native_json(
    node: &NodeConfig,
    bearer: &str,
    op: &str,
    payload: Value,
) -> Result<(u16, Value), String> {
    let mut client = connect_with_bearer(node, bearer).await?;
    let response = client
        .operation(op, &payload, None)
        .await
        .map_err(native_error)?;
    Ok(decode_operation_response(response)?)
}

pub async fn native_upload_body(
    node: &NodeConfig,
    bearer: &str,
    op: &str,
    payload: Value,
    body: Body,
    body_len: u64,
) -> Result<(u16, Value), String> {
    let mut client = connect_with_bearer(node, bearer).await?;
    let stream = body
        .into_data_stream()
        .map_err(|err| std::io::Error::other(err.to_string()));
    let mut reader = StreamReader::new(stream);
    let response = client
        .operation_reader(op, &payload, &mut reader, body_len)
        .await
        .map_err(native_error)?;
    Ok(decode_operation_response(response)?)
}

pub async fn native_download_to_writer<W>(
    node: &NodeConfig,
    bearer: &str,
    payload: Value,
    writer: &mut W,
) -> Result<(u16, u64), String>
where
    W: AsyncWrite + Unpin,
{
    let mut client = connect_with_bearer(node, bearer).await?;
    let (response, bytes) = client
        .stream_response_to_writer("object.download_stream", &payload, writer)
        .await
        .map_err(native_error)?;
    if !response.ok {
        let message = response
            .error
            .map(|err| format!("{}: {}", err.code, err.message))
            .unwrap_or_else(|| "native download failed".to_string());
        return Err(message);
    }
    Ok((response.status.unwrap_or(200), bytes))
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
