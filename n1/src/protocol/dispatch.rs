use crate::protocol::auth::NativeSession;
use crate::protocol::types::{MAX_NATIVE_FRAME_BYTES, NativeApiRequest, NativeApiResponse};
use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::ConnectInfo;
use axum::http::{HeaderName, HeaderValue, Method, Request, Uri};
use serde_json::Value;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::str::FromStr;
use tower::ServiceExt;

#[derive(Clone)]
pub struct NativeApiDispatcher {
    router: Router,
}

impl NativeApiDispatcher {
    pub fn new(router: Router) -> Self {
        Self { router }
    }

    pub async fn dispatch(
        &self,
        session: &NativeSession,
        peer: SocketAddr,
        request: NativeApiRequest,
        body: Option<Vec<u8>>,
    ) -> Result<(NativeApiResponse, Vec<u8>), String> {
        let method = Method::from_bytes(request.method.as_bytes())
            .map_err(|_| format!("invalid method '{}'", request.method))?;
        let uri = Uri::from_str(&request.path).map_err(|err| format!("invalid path: {err}"))?;

        let body_bytes = match (body, request.json) {
            (Some(bytes), _) => bytes,
            (None, Some(json)) => serde_json::to_vec(&json)
                .map_err(|err| format!("request json encode failed: {err}"))?,
            (None, None) => Vec::new(),
        };

        let mut builder = Request::builder().method(method).uri(uri);
        {
            let headers = builder
                .headers_mut()
                .ok_or_else(|| "failed to build request headers".to_string())?;
            if !body_bytes.is_empty() && !request.headers.contains_key("content-type") {
                headers.insert(
                    HeaderName::from_static("content-type"),
                    HeaderValue::from_static("application/json"),
                );
            }
            if let Some(token) = session.bearer_token() {
                let value = HeaderValue::from_str(&format!("Bearer {token}"))
                    .map_err(|err| format!("invalid bearer token header: {err}"))?;
                headers.insert(HeaderName::from_static("authorization"), value);
            }
            for (name, value) in request.headers {
                let name = HeaderName::from_str(&name)
                    .map_err(|_| format!("invalid header name '{name}'"))?;
                let value = HeaderValue::from_str(&value)
                    .map_err(|_| format!("invalid header value for '{name}'"))?;
                headers.insert(name, value);
            }
        }

        let mut request = builder
            .body(Body::from(body_bytes))
            .map_err(|err| format!("request build failed: {err}"))?;
        request.extensions_mut().insert(ConnectInfo(peer));

        let response = self
            .router
            .clone()
            .oneshot(request)
            .await
            .map_err(|err| format!("native dispatch failed: {err}"))?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_string(), value.to_string()))
            })
            .collect::<BTreeMap<_, _>>();
        let body = to_bytes(response.into_body(), MAX_NATIVE_FRAME_BYTES)
            .await
            .map_err(|err| format!("response body read failed: {err}"))?
            .to_vec();
        let json = parse_json_body(&headers, &body);
        Ok((
            NativeApiResponse {
                status,
                headers,
                json,
                body_len: body.len(),
            },
            body,
        ))
    }
}

pub fn api_request(
    method: impl Into<String>,
    path: impl Into<String>,
    json: Option<Value>,
) -> NativeApiRequest {
    NativeApiRequest {
        method: method.into(),
        path: path.into(),
        headers: BTreeMap::new(),
        json,
    }
}

fn parse_json_body(headers: &BTreeMap<String, String>, body: &[u8]) -> Option<Value> {
    if body.is_empty() {
        return None;
    }
    let content_type = headers
        .get("content-type")
        .or_else(|| headers.get("Content-Type"))
        .map(String::as_str)
        .unwrap_or_default();
    if !content_type.contains("application/json") {
        return None;
    }
    serde_json::from_slice(body).ok()
}
