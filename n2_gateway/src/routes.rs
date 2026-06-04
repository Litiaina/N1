use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderName, Method, Response, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use percent_encoding::percent_decode_str;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::io::ReaderStream;
use tower_http::cors::{Any, CorsLayer};

use crate::config::NodeConfig;
use crate::native::{
    MultipartInitResponse, bearer_from_headers, content_length, content_type,
    native_download_to_writer, native_json, native_upload_body,
};
use crate::node::NodeStatus;
use crate::proxy::{proxy_http, text_response};
use crate::session_db::{UploadRoute, now_unix};
use crate::state::GatewayState;

const DEFAULT_UPLOAD_ROUTE_TTL_SECS: i64 = 24 * 60 * 60;

pub fn router(state: GatewayState) -> Router {
    let state = Arc::new(state);
    Router::new()
        .route("/gateway/health", get(gateway_health))
        .route("/gateway/nodes", get(gateway_nodes))
        .route("/gateway/routes/uploads", get(gateway_upload_routes))
        .route(
            "/gateway/routes/uploads/{session_id}",
            get(gateway_upload_route),
        )
        .route("/n2/auth/fragment", post(auth_fragment))
        .route("/n2/fragments", post(create_fragment))
        .route(
            "/n2/upload/multipart/init/{*object_key}",
            post(multipart_init),
        )
        .route(
            "/n2/upload/multipart/{version_id}/{part_index}",
            put(multipart_put_part),
        )
        .route(
            "/n2/upload/multipart/{version_id}/{session_id}/finalize",
            post(multipart_finalize),
        )
        .route("/n2/upload/multipart/{version_id}", get(multipart_status))
        .route(
            "/n2/upload/multipart/{version_id}",
            delete(multipart_cancel),
        )
        .route("/n2/upload/one-shot/{*object_key}", post(one_shot_upload))
        .route("/n2/objects/download", get(download_object))
        .fallback(proxy_fallback)
        .layer(build_cors_layer())
        .with_state(state)
}

fn build_cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(Any)
        .allow_headers([
            HeaderName::from_static("authorization"),
            HeaderName::from_static("uid"),
            HeaderName::from_static("content-type"),
            HeaderName::from_static("digest"),
            HeaderName::from_static("buffer"),
            HeaderName::from_static("range"),
            HeaderName::from_static("upgrade"),
            HeaderName::from_static("connection"),
            HeaderName::from_static("sec-websocket-key"),
            HeaderName::from_static("sec-websocket-version"),
            HeaderName::from_static("sec-websocket-protocol"),
        ])
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PATCH,
            Method::PUT,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .max_age(std::time::Duration::from_secs(86400))
}

async fn gateway_health(State(state): State<Arc<GatewayState>>) -> impl IntoResponse {
    let nodes = state.nodes.snapshots().await;
    let online_nodes = nodes
        .iter()
        .filter(|node| matches!(&node.status, NodeStatus::Online))
        .count();
    let available_nodes = nodes
        .iter()
        .filter(|node| !matches!(&node.status, NodeStatus::Offline))
        .count();
    let status = if online_nodes > 0 {
        "healthy"
    } else if available_nodes > 0 {
        "warming"
    } else {
        "unavailable"
    };
    let active_upload_routes = state.sessions.list_active().await.len();
    Json(json!({
        "status": status,
        "server": "Litiaina N2 Gateway",
        "nodes": nodes,
        "online_nodes": online_nodes,
        "available_nodes": available_nodes,
        "active_upload_routes": active_upload_routes,
    }))
}

async fn gateway_nodes(State(state): State<Arc<GatewayState>>) -> impl IntoResponse {
    Json(json!({ "nodes": state.nodes.snapshots().await }))
}

async fn gateway_upload_routes(State(state): State<Arc<GatewayState>>) -> impl IntoResponse {
    Json(json!({ "routes": state.sessions.list_active().await }))
}

async fn gateway_upload_route(
    State(state): State<Arc<GatewayState>>,
    Path(session_id): Path<String>,
) -> impl IntoResponse {
    match state.sessions.get(&session_id).await {
        Some(route) => Json(route).into_response(),
        None => text_response(StatusCode::NOT_FOUND, "upload route not found").into_response(),
    }
}

async fn auth_fragment(
    State(state): State<Arc<GatewayState>>,
    Json(payload): Json<Value>,
) -> Response<Body> {
    let nodes = state.nodes.available_configs().await;
    if nodes.is_empty() {
        return text_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "gateway has no available N2 nodes",
        );
    }

    let mut first_client_error: Option<Response<Body>> = None;
    let mut last_error: Option<Response<Body>> = None;

    for node in nodes {
        match post_json_to_node(&state, &node, "/n2/auth/fragment", &payload).await {
            Ok((status, body)) if status.is_success() => {
                state.nodes.mark_route(&node.id).await;
                return json_response_value(status, body);
            }
            Ok((status, body)) if status.is_client_error() => {
                first_client_error.get_or_insert_with(|| json_response_value(status, body));
            }
            Ok((status, body)) => {
                last_error = Some(json_response_value(status, body));
                state.nodes.mark_failure(&node.id).await;
            }
            Err(err) => {
                state.nodes.mark_failure(&node.id).await;
                last_error = Some(text_response(
                    StatusCode::BAD_GATEWAY,
                    format!("gateway upstream {} failed: {err}", node.id),
                ));
            }
        }
    }

    first_client_error.or(last_error).unwrap_or_else(|| {
        text_response(StatusCode::UNAUTHORIZED, "fragment authentication failed")
    })
}

async fn create_fragment(
    State(state): State<Arc<GatewayState>>,
    Json(mut payload): Json<Value>,
) -> Response<Body> {
    let nodes = state.nodes.available_configs().await;
    if nodes.is_empty() {
        return text_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "gateway has no available N2 nodes",
        );
    }

    let mut stable_fragment_id = payload
        .get("stable_fragment_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string);
    let mut fragment_hash = String::new();
    let mut created = Vec::new();
    let mut errors = Vec::new();
    let mut first_body: Option<Value> = None;
    let mut first_status = StatusCode::CREATED;

    for node in nodes {
        if let Some(stable) = &stable_fragment_id {
            payload["stable_fragment_id"] = Value::String(stable.clone());
        }

        match post_json_to_node(&state, &node, "/n2/fragments", &payload).await {
            Ok((status, body)) if status.is_success() => {
                state.nodes.mark_route(&node.id).await;
                if first_body.is_none() {
                    first_status = status;
                    first_body = Some(body.clone());
                }
                if stable_fragment_id.is_none() {
                    stable_fragment_id = body
                        .get("stable_fragment_id")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                }
                if fragment_hash.is_empty() {
                    fragment_hash = body
                        .get("fragment_hash")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                }
                created.push(json!({ "node": node.id, "status": status.as_u16() }));
            }
            Ok((status, body)) => {
                if status.is_server_error() {
                    state.nodes.mark_failure(&node.id).await;
                }
                errors.push(json!({
                    "node": node.id,
                    "status": status.as_u16(),
                    "error": body.get("error")
                        .or_else(|| body.get("message"))
                        .cloned()
                        .unwrap_or(body)
                }));
            }
            Err(err) => {
                state.nodes.mark_failure(&node.id).await;
                errors.push(json!({ "node": node.id, "status": 502, "error": err }));
            }
        }
    }

    if !errors.is_empty() {
        return json_response_value(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "error": "fragment creation did not complete on every N2 node",
                "created": created,
                "errors": errors,
                "fragment_hash": fragment_hash,
                "stable_fragment_id": stable_fragment_id,
            }),
        );
    }

    let mut body = first_body.unwrap_or_else(|| json!({}));
    body["gateway"] = json!({
        "cluster_created": true,
        "nodes": created,
    });
    json_response_value(first_status, body)
}

async fn multipart_init(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(object_key): Path<String>,
    Json(mut payload): Json<Value>,
) -> Response<Body> {
    let bearer = match bearer_from_headers(&headers) {
        Ok(value) => value,
        Err(err) => return text_response(StatusCode::UNAUTHORIZED, err),
    };
    let node = match best_node(&state).await {
        Ok(node) => node,
        Err(response) => return response,
    };
    payload["object_key"] = Value::String(decode_path(&object_key));
    let (status, body) = match native_json(&node, &bearer, "upload.multipart.init", payload).await {
        Ok(value) => value,
        Err(err) => return upstream_failed(&state, &node, err).await,
    };
    let parsed = serde_json::from_value::<MultipartInitResponse>(body.clone());
    if let Ok(parsed) = parsed {
        let route = UploadRoute {
            session_id: parsed.session_id,
            version_id: parsed.version_id,
            coordinator_node_id: node.id.clone(),
            fragment: None,
            object_key: Some(decode_path(&object_key)),
            created_at: now_unix(),
            expires_at: now_unix() + DEFAULT_UPLOAD_ROUTE_TTL_SECS,
            finalized_at: None,
            cancelled_at: None,
        };
        if let Err(err) = state.sessions.upsert(route).await {
            return text_response(StatusCode::INTERNAL_SERVER_ERROR, err);
        }
    }
    json_response(status, body)
}

async fn multipart_put_part(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path((version_id, part_index)): Path<(String, u64)>,
    Query(query): Query<HashMap<String, String>>,
    body: Body,
) -> Response<Body> {
    let bearer = match bearer_from_headers(&headers) {
        Ok(value) => value,
        Err(err) => return text_response(StatusCode::UNAUTHORIZED, err),
    };
    let node = match coordinator_node(&state, &version_id).await {
        Ok(node) => node,
        Err(response) => return response,
    };
    let body_len = match content_length(&headers) {
        Ok(value) => value,
        Err(err) => return text_response(StatusCode::BAD_REQUEST, err),
    };
    let mut payload = query_to_json(query);
    payload["version_id"] = Value::String(version_id);
    payload["part_index"] = Value::Number(part_index.into());
    match native_upload_body(
        &node,
        &bearer,
        "upload.multipart.put_part",
        payload,
        body,
        body_len,
    )
    .await
    {
        Ok((status, value)) => json_response(status, value),
        Err(err) => upstream_failed(&state, &node, err).await,
    }
}

async fn multipart_status(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(version_id): Path<String>,
) -> Response<Body> {
    let bearer = match bearer_from_headers(&headers) {
        Ok(value) => value,
        Err(err) => return text_response(StatusCode::UNAUTHORIZED, err),
    };
    let node = match coordinator_node(&state, &version_id).await {
        Ok(node) => node,
        Err(response) => return response,
    };
    match native_json(
        &node,
        &bearer,
        "uploads.status",
        json!({ "version_id": version_id }),
    )
    .await
    {
        Ok((status, value)) => json_response(status, value),
        Err(err) => upstream_failed(&state, &node, err).await,
    }
}

async fn multipart_finalize(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path((version_id, session_id)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
    Json(mut payload): Json<Value>,
) -> Response<Body> {
    let bearer = match bearer_from_headers(&headers) {
        Ok(value) => value,
        Err(err) => return text_response(StatusCode::UNAUTHORIZED, err),
    };
    let node = match coordinator_node(&state, &session_id).await {
        Ok(node) => node,
        Err(response) => return response,
    };
    payload["version_id"] = Value::String(version_id.clone());
    payload["session_id"] = Value::String(session_id.clone());
    if let Some(object_key) = query.get("object_key") {
        payload["object_key"] = Value::String(object_key.clone());
    }
    match native_json(&node, &bearer, "upload.multipart.finalize", payload).await {
        Ok((status, value)) => {
            if status < 300 {
                let _ = state
                    .sessions
                    .mark_finalized(&session_id, &version_id)
                    .await;
            }
            json_response(status, value)
        }
        Err(err) => upstream_failed(&state, &node, err).await,
    }
}

async fn multipart_cancel(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(version_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response<Body> {
    let bearer = match bearer_from_headers(&headers) {
        Ok(value) => value,
        Err(err) => return text_response(StatusCode::UNAUTHORIZED, err),
    };
    let node = match coordinator_node(&state, &version_id).await {
        Ok(node) => node,
        Err(response) => return response,
    };
    let mut payload = query_to_json(query);
    payload["version_id"] = Value::String(version_id.clone());
    match native_json(&node, &bearer, "upload.multipart.cancel", payload).await {
        Ok((status, value)) => {
            if status < 300 {
                let _ = state.sessions.mark_cancelled(None, &version_id).await;
            }
            json_response(status, value)
        }
        Err(err) => upstream_failed(&state, &node, err).await,
    }
}

async fn one_shot_upload(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(object_key): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    body: Body,
) -> Response<Body> {
    let bearer = match bearer_from_headers(&headers) {
        Ok(value) => value,
        Err(err) => return text_response(StatusCode::UNAUTHORIZED, err),
    };
    let node = match best_node(&state).await {
        Ok(node) => node,
        Err(response) => return response,
    };
    let body_len = match content_length(&headers) {
        Ok(value) => value,
        Err(err) => return text_response(StatusCode::BAD_REQUEST, err),
    };
    let mut payload = query_to_json(query);
    payload["object_key"] = Value::String(decode_path(&object_key));
    if let Some(content_type) = content_type(&headers) {
        payload["content_type"] = Value::String(content_type);
    }
    match native_upload_body(&node, &bearer, "upload.one_shot", payload, body, body_len).await {
        Ok((status, value)) => json_response(status, value),
        Err(err) => upstream_failed(&state, &node, err).await,
    }
}

#[derive(Debug, Deserialize)]
struct DownloadQuery {
    object_key: String,
    #[serde(default)]
    version_id: Option<String>,
}

async fn download_object(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Query(query): Query<DownloadQuery>,
) -> Response<Body> {
    let bearer = match bearer_from_headers(&headers) {
        Ok(value) => value,
        Err(err) => return text_response(StatusCode::UNAUTHORIZED, err),
    };
    let node = match best_node(&state).await {
        Ok(node) => node,
        Err(response) => return response,
    };
    let (mut writer, reader) = tokio::io::duplex(1024 * 1024);
    let payload = json!({
        "object_key": query.object_key,
        "version_id": query.version_id,
    });
    let failed_state = state.clone();
    let failed_node = node.clone();
    tokio::spawn(async move {
        if let Err(err) = native_download_to_writer(&node, &bearer, payload, &mut writer).await {
            failed_state.nodes.mark_failure(&failed_node.id).await;
            tracing::warn!("gateway native download failed: {}", err);
        }
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from_stream(ReaderStream::new(reader)))
        .expect("valid download response")
}

async fn proxy_fallback(
    State(state): State<Arc<GatewayState>>,
    request: axum::extract::Request,
) -> Response<Body> {
    let node = match best_node(&state).await {
        Ok(node) => node,
        Err(response) => return response,
    };
    match proxy_http(&state, node.clone(), request).await {
        Ok(response) => response,
        Err(response) => {
            state.nodes.mark_failure(&node.id).await;
            response
        }
    }
}

async fn best_node(state: &GatewayState) -> Result<NodeConfig, Response<Body>> {
    state.nodes.select_best().await.ok_or_else(|| {
        text_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "gateway has no available N2 nodes",
        )
    })
}

async fn coordinator_node(state: &GatewayState, key: &str) -> Result<NodeConfig, Response<Body>> {
    let route = state.sessions.get(key).await.ok_or_else(|| {
        text_response(
            StatusCode::CONFLICT,
            "multipart session route not found; retry through the original node or restart upload",
        )
    })?;
    state.nodes.mark_route(&route.coordinator_node_id).await;
    if !state.nodes.is_available(&route.coordinator_node_id).await {
        return Err(text_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "multipart coordinator node is unavailable",
        ));
    }
    state
        .nodes
        .get(&route.coordinator_node_id)
        .await
        .ok_or_else(|| {
            text_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "multipart coordinator node is not configured",
            )
        })
}

async fn upstream_failed(state: &GatewayState, node: &NodeConfig, err: String) -> Response<Body> {
    state.nodes.mark_failure(&node.id).await;
    text_response(
        StatusCode::SERVICE_UNAVAILABLE,
        format!("gateway upstream {} failed: {err}", node.id),
    )
}

fn query_to_json(query: HashMap<String, String>) -> Value {
    let mut map = serde_json::Map::new();
    for (key, value) in query {
        if let Ok(parsed) = value.parse::<u64>() {
            map.insert(key, Value::Number(parsed.into()));
        } else if value == "true" || value == "false" {
            map.insert(key, Value::Bool(value == "true"));
        } else {
            map.insert(key, Value::String(value));
        }
    }
    Value::Object(map)
}

fn decode_path(path: &str) -> String {
    percent_decode_str(path).decode_utf8_lossy().to_string()
}

fn json_response(status: u16, value: Value) -> Response<Body> {
    let status = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
    json_response_value(status, value)
}

fn json_response_value(status: StatusCode, value: Value) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&value).unwrap_or_else(|_| b"{}".to_vec()),
        ))
        .expect("valid json response")
}

async fn post_json_to_node(
    state: &GatewayState,
    node: &NodeConfig,
    path: &str,
    payload: &Value,
) -> Result<(StatusCode, Value), String> {
    let upstream = state
        .http
        .post(format!("{}{}", node.http_url, path))
        .json(payload)
        .send()
        .await
        .map_err(|err| err.to_string())?;
    let status = upstream.status();
    let bytes = upstream.bytes().await.map_err(|err| err.to_string())?;
    let body = if bytes.is_empty() {
        json!({})
    } else {
        serde_json::from_slice::<Value>(&bytes).unwrap_or_else(
            |_| json!({ "error": String::from_utf8_lossy(&bytes).trim().to_string() }),
        )
    };
    Ok((status, body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, header};
    use http_body_util::BodyExt;
    use n2_native_client::{NativeRequest, NativeResponse};
    use serde_bytes::ByteBuf;
    use std::path::PathBuf;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::Mutex;
    use tower::ServiceExt;

    struct MockNativeNode {
        native_url: String,
        operations: Arc<Mutex<Vec<String>>>,
    }

    async fn spawn_mock_native_node(node_id: &'static str) -> MockNativeNode {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let native_url = format!("n2://{}", listener.local_addr().unwrap());
        let operations = Arc::new(Mutex::new(Vec::new()));
        let recorded = operations.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let recorded = recorded.clone();
                tokio::spawn(async move {
                    handle_mock_native_connection(stream, node_id, recorded).await;
                });
            }
        });
        MockNativeNode {
            native_url,
            operations,
        }
    }

    async fn handle_mock_native_connection(
        mut stream: TcpStream,
        node_id: &str,
        operations: Arc<Mutex<Vec<String>>>,
    ) {
        loop {
            let Some(request) = read_native_request(&mut stream).await else {
                break;
            };
            if let Some(body_len) = request.body_len {
                let mut body = vec![0u8; body_len as usize];
                if stream.read_exact(&mut body).await.is_err() {
                    break;
                }
            }
            operations.lock().await.push(request.op.clone());
            let (status, body) = match request.op.as_str() {
                "auth.bearer" => (200, json!({ "node": node_id, "authenticated": true })),
                "upload.multipart.init" => (
                    201,
                    json!({ "session_id": "sess-a", "version_id": "ver-a" }),
                ),
                "upload.multipart.put_part" => (200, json!({ "stored": true })),
                "uploads.status" => (200, json!({ "status": "open" })),
                "upload.multipart.finalize" => (200, json!({ "finalized": true })),
                "upload.multipart.cancel" => (200, json!({ "cancelled": true })),
                "upload.one_shot" => (201, json!({ "version_id": "one-shot" })),
                "ping" => (200, json!({ "status": "ok" })),
                other => (200, json!({ "op": other, "node": node_id })),
            };
            write_native_json_response(&mut stream, request.id, status, body)
                .await
                .unwrap();
        }
    }

    async fn read_native_request(stream: &mut TcpStream) -> Option<NativeRequest> {
        let mut len = [0u8; 4];
        if stream.read_exact(&mut len).await.is_err() {
            return None;
        }
        let len = u32::from_be_bytes(len) as usize;
        let mut frame = vec![0u8; len];
        stream.read_exact(&mut frame).await.ok()?;
        rmp_serde::from_slice::<NativeRequest>(&frame).ok()
    }

    async fn write_native_json_response(
        stream: &mut TcpStream,
        id: String,
        status: u16,
        body: Value,
    ) -> std::io::Result<()> {
        let response = NativeResponse {
            id,
            ok: true,
            status: Some(status),
            payload: Some(body.to_string()),
            body: Some(ByteBuf::from(rmp_serde::to_vec(&body).unwrap())),
            body_len: None,
            error: None,
        };
        let frame = rmp_serde::to_vec(&response).unwrap();
        stream
            .write_all(&(frame.len() as u32).to_be_bytes())
            .await?;
        stream.write_all(&frame).await?;
        stream.flush().await
    }

    fn gateway_config(state_path: PathBuf, nodes: Vec<NodeConfig>) -> crate::config::GatewayConfig {
        crate::config::GatewayConfig {
            host: "127.0.0.1".to_string(),
            https_port: 0,
            cert_path: PathBuf::from("./certificates/cert.pem"),
            key_path: PathBuf::from("./certificates/key.pem"),
            state_path,
            health_interval: Duration::from_secs(60),
            request_timeout: Duration::from_secs(10),
            nodes,
        }
    }

    fn temp_state_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "n2-gateway-routes-{name}-{}.json",
            std::process::id()
        ))
    }

    async fn response_json(response: Response<Body>) -> Value {
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn multipart_flow_routes_every_step_to_original_coordinator() {
        let node_a = spawn_mock_native_node("node-a").await;
        let node_b = spawn_mock_native_node("node-b").await;
        let state_path = temp_state_path("multipart");
        let _ = tokio::fs::remove_file(&state_path).await;
        let state = GatewayState::new(gateway_config(
            state_path.clone(),
            vec![
                NodeConfig {
                    id: "node-a".to_string(),
                    http_url: "https://127.0.0.1:51001".to_string(),
                    native_url: node_a.native_url.clone(),
                    weight: 1,
                },
                NodeConfig {
                    id: "node-b".to_string(),
                    http_url: "https://127.0.0.1:51003".to_string(),
                    native_url: node_b.native_url.clone(),
                    weight: 1,
                },
            ],
        ))
        .await
        .unwrap();
        state.nodes.mark_online_for_test("node-a", 1).await;
        state.nodes.mark_online_for_test("node-b", 1).await;
        let app = router(state.clone());

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/n2/upload/multipart/init/videos/test.mp4")
                    .header(header::AUTHORIZATION, "Bearer token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = response_json(response).await;
        assert_eq!(body["session_id"], "sess-a");
        assert_eq!(
            state
                .sessions
                .get("ver-a")
                .await
                .unwrap()
                .coordinator_node_id,
            "node-a"
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/n2/upload/multipart/ver-a/0?crc32=123")
                    .header(header::AUTHORIZATION, "Bearer token")
                    .header(header::CONTENT_LENGTH, "5")
                    .body(Body::from("hello"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/n2/upload/multipart/ver-a")
                    .header(header::AUTHORIZATION, "Bearer token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/n2/upload/multipart/ver-a/sess-a/finalize")
                    .header(header::AUTHORIZATION, "Bearer token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(state.sessions.get("ver-a").await.is_none());

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/n2/upload/multipart/ver-a")
                    .header(header::AUTHORIZATION, "Bearer token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let ops_a = node_a.operations.lock().await.clone();
        let ops_b = node_b.operations.lock().await.clone();
        assert!(ops_a.contains(&"upload.multipart.init".to_string()));
        assert!(ops_a.contains(&"upload.multipart.put_part".to_string()));
        assert!(ops_a.contains(&"uploads.status".to_string()));
        assert!(ops_a.contains(&"upload.multipart.finalize".to_string()));
        assert!(!ops_b.contains(&"upload.multipart.put_part".to_string()));
        let _ = tokio::fs::remove_file(state_path).await;
    }

    #[tokio::test]
    async fn offline_multipart_coordinator_returns_service_unavailable() {
        let state_path = temp_state_path("offline");
        let _ = tokio::fs::remove_file(&state_path).await;
        let state = GatewayState::new(gateway_config(
            state_path.clone(),
            vec![NodeConfig {
                id: "node-a".to_string(),
                http_url: "https://127.0.0.1:51001".to_string(),
                native_url: "n2://127.0.0.1:51002".to_string(),
                weight: 1,
            }],
        ))
        .await
        .unwrap();
        state
            .sessions
            .upsert(UploadRoute {
                session_id: "sess-a".to_string(),
                version_id: "ver-a".to_string(),
                coordinator_node_id: "node-a".to_string(),
                fragment: None,
                object_key: Some("object.bin".to_string()),
                created_at: now_unix(),
                expires_at: now_unix() + 60,
                finalized_at: None,
                cancelled_at: None,
            })
            .await
            .unwrap();
        state.nodes.mark_offline_for_test("node-a").await;

        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/n2/upload/multipart/ver-a")
                    .header(header::AUTHORIZATION, "Bearer token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let _ = tokio::fs::remove_file(state_path).await;
    }
}
