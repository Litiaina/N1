use n2_native_client::{N2NativeClient, NativeClientError, NativeOperationResponse};
use serde_json::{Value, json};

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

pub fn msgpack_to_json(body: &[u8]) -> Result<Value, String> {
    if body.is_empty() {
        return Ok(json!({}));
    }
    rmp_serde::from_slice::<Value>(body).map_err(|err| format!("decode native body: {err}"))
}

fn decode_operation_response(response: NativeOperationResponse) -> Result<(u16, Value), String> {
    Ok((response.status, msgpack_to_json(&response.body)?))
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
