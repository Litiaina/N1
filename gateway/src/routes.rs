use axum::body::Body;
use axum::extract::{OriginalUri, Path, Query, State};
use axum::http::{HeaderMap, HeaderName, Method, Response, StatusCode, Uri, header};
use axum::response::IntoResponse;
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use futures_util::future::join_all;
use percent_encoding::percent_decode_str;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tower_http::cors::{Any, CorsLayer};

#[path = "routes/proxy.rs"]
mod proxy_routes;
#[path = "routes/support.rs"]
mod support;
mod websocket;

use proxy_routes::*;
use support::*;

#[cfg(test)]
use axum::extract::ws::{CloseFrame, Message as AxumWsMessage};
use websocket::websocket_proxy;
#[cfg(test)]
use websocket::{axum_to_upstream_message, upstream_to_axum_message, websocket_upstream_url};

use crate::config::NodeConfig;
use crate::native::{NativeCallError, bearer_from_headers, native_json};
use crate::node::NodeStatus;
use crate::proxy::{proxy_http, text_response};
use crate::state::GatewayState;

pub fn router(state: GatewayState) -> Router {
    let state = Arc::new(state);
    Router::new()
        .route("/gateway/health", get(gateway_health))
        .route("/gateway/nodes", get(gateway_nodes))
        .route("/noa/v1/ws", get(websocket_proxy))
        .route("/noa/v1/auth/fragment", post(auth_fragment))
        .route("/noa/v1/auth/refresh", post(auth_refresh))
        .route("/noa/v1/auth/logout", post(auth_logout))
        .route("/noa/v1/fragments", post(create_fragment))
        .route(
            "/noa/v1/cluster/rebalance/start",
            post(cluster_rebalance_start),
        )
        .route(
            "/noa/v1/cluster/rebalance/status",
            get(cluster_rebalance_status),
        )
        .route(
            "/noa/v1/cluster/rebalance/stop",
            post(cluster_rebalance_stop),
        )
        .route("/noa/v1/cluster/repair/start", post(cluster_repair_start))
        .route("/noa/v1/cluster/repair/status", get(cluster_repair_status))
        .route("/noa/v1/cluster/repair/stop", post(cluster_repair_stop))
        .route(
            "/noa/v1/upload/multipart/init/{*object_key}",
            post(multipart_init),
        )
        .route("/noa/v1/upload/multipart", get(multipart_list))
        .route(
            "/noa/v1/upload/multipart/{version_id}/{part_index}",
            put(multipart_put_part),
        )
        .route(
            "/noa/v1/upload/multipart/{version_id}/{session_id}/finalize",
            post(multipart_finalize),
        )
        .route(
            "/noa/v1/upload/multipart/{version_id}",
            get(multipart_status),
        )
        .route(
            "/noa/v1/upload/multipart/{version_id}",
            delete(multipart_cancel),
        )
        .route(
            "/noa/v1/upload/multipart/durable/init/{*object_key}",
            post(multipart_durable_init),
        )
        .route(
            "/noa/v1/upload/multipart/durable",
            get(multipart_durable_list),
        )
        .route(
            "/noa/v1/upload/multipart/durable/{version_id}/{part_index}",
            put(multipart_durable_put_part),
        )
        .route(
            "/noa/v1/upload/multipart/durable/{version_id}/{session_id}/finalize",
            post(multipart_durable_finalize),
        )
        .route(
            "/noa/v1/upload/multipart/durable/{version_id}",
            get(multipart_durable_status),
        )
        .route(
            "/noa/v1/upload/multipart/durable/{version_id}",
            delete(multipart_durable_cancel),
        )
        .route(
            "/noa/v1/upload/one-shot/{*object_key}",
            post(one_shot_upload),
        )
        .route("/noa/v1/objects/download", get(download_object))
        .fallback(proxy_fallback)
        .layer(build_cors_layer())
        .with_state(state)
}

#[derive(Clone, Copy)]
enum MultipartMode {
    Standard,
    Durable,
}

impl MultipartMode {
    fn init_op(self) -> &'static str {
        match self {
            Self::Standard => "upload.multipart.standard.init",
            Self::Durable => "upload.multipart.durable.init",
        }
    }

    fn status_op(self) -> &'static str {
        match self {
            Self::Standard => "uploads.standard.status",
            Self::Durable => "uploads.durable.status",
        }
    }

    fn finalize_op(self) -> &'static str {
        match self {
            Self::Standard => "upload.multipart.standard.finalize",
            Self::Durable => "upload.multipart.durable.finalize",
        }
    }

    fn cancel_op(self) -> &'static str {
        match self {
            Self::Standard => "upload.multipart.standard.cancel",
            Self::Durable => "upload.multipart.durable.cancel",
        }
    }

    fn http_path(self) -> &'static str {
        match self {
            Self::Standard => "/noa/v1/upload/multipart",
            Self::Durable => "/noa/v1/upload/multipart/durable",
        }
    }
}

fn build_cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(Any)
        .allow_headers([
            HeaderName::from_static("authorization"),
            HeaderName::from_static("x-n2-coordinator-node"),
            HeaderName::from_static("uid"),
            HeaderName::from_static("content-type"),
            HeaderName::from_static("digest"),
            HeaderName::from_static("buffer"),
            HeaderName::from_static("range"),
            HeaderName::from_static("if-range"),
            HeaderName::from_static("if-none-match"),
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
        .expose_headers([
            header::CONTENT_LENGTH,
            header::CONTENT_RANGE,
            header::ACCEPT_RANGES,
            header::ETAG,
            header::CONTENT_DISPOSITION,
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
    Json(json!({
        "status": status,
        "server": "Litiaina N2 Gateway",
        "nodes": nodes,
        "online_nodes": online_nodes,
        "available_nodes": available_nodes,
        "role": "route_only",
        "resumability_owner": "client",
    }))
}

async fn gateway_nodes(State(state): State<Arc<GatewayState>>) -> impl IntoResponse {
    Json(json!({ "nodes": state.nodes.snapshots().await }))
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
        match post_json_to_node(&state, &node, "/noa/v1/auth/fragment", &payload).await {
            Ok((status, mut body)) if status.is_success() => {
                state.nodes.mark_route(&node.id).await;
                body["coordinator_node_id"] = Value::String(node.id.clone());
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

async fn auth_refresh(
    State(state): State<Arc<GatewayState>>,
    Json(payload): Json<Value>,
) -> Response<Body> {
    route_auth_session_request(&state, "/noa/v1/auth/refresh", payload, true).await
}

async fn auth_logout(
    State(state): State<Arc<GatewayState>>,
    Json(payload): Json<Value>,
) -> Response<Body> {
    route_auth_session_request(&state, "/noa/v1/auth/logout", payload, false).await
}

/// Refresh sessions are intentionally owned by the node that minted them. The
/// gateway returns that node ID with the authentication response and routes all
/// later refresh/logout operations back to the same owner. For sessions issued
/// before coordinator ownership was exposed, probe the configured nodes once so
/// an upgrade does not force every active Console session to log in again.
async fn route_auth_session_request(
    state: &GatewayState,
    path: &str,
    mut payload: Value,
    return_coordinator: bool,
) -> Response<Body> {
    let requested_coordinator = payload
        .get("coordinator_node_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    if let Some(object) = payload.as_object_mut() {
        object.remove("coordinator_node_id");
    }

    let nodes = if let Some(coordinator_id) = requested_coordinator.as_deref() {
        let Some(node) = state.nodes.get(coordinator_id).await else {
            return text_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "authentication coordinator node is not configured",
            );
        };
        if !state.nodes.is_available(coordinator_id).await
            && !state.nodes.probe_node(coordinator_id).await
        {
            return text_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "authentication coordinator node is unavailable",
            );
        }
        vec![node]
    } else {
        state.nodes.available_configs().await
    };

    if nodes.is_empty() {
        return text_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "gateway has no available N2 nodes",
        );
    }

    let mut first_client_error: Option<Response<Body>> = None;
    let mut last_error: Option<Response<Body>> = None;
    for node in nodes {
        match post_json_to_node(state, &node, path, &payload).await {
            Ok((status, mut body)) if status.is_success() => {
                state.nodes.mark_route(&node.id).await;
                if return_coordinator {
                    body["coordinator_node_id"] = Value::String(node.id.clone());
                }
                return json_response_value(status, body);
            }
            Ok((status, body)) if status.is_client_error() => {
                first_client_error.get_or_insert_with(|| json_response_value(status, body));
            }
            Ok((status, body)) => {
                last_error = Some(json_response_value(status, body));
            }
            Err(err) => {
                state.nodes.probe_node(&node.id).await;
                last_error = Some(text_response(
                    StatusCode::BAD_GATEWAY,
                    format!("gateway authentication upstream {} failed: {err}", node.id),
                ));
            }
        }
    }

    first_client_error.or(last_error).unwrap_or_else(|| {
        text_response(
            StatusCode::UNAUTHORIZED,
            "authentication session was not found",
        )
    })
}

async fn create_fragment(
    State(state): State<Arc<GatewayState>>,
    Json(payload): Json<Value>,
) -> Response<Body> {
    let node = match best_node(&state).await {
        Ok(node) => node,
        Err(response) => return response,
    };
    match post_json_to_node(&state, &node, "/noa/v1/fragments", &payload).await {
        Ok((status, body)) => {
            if status.is_success() {
                state.nodes.mark_route(&node.id).await;
            } else if status.is_server_error() {
                state.nodes.mark_failure(&node.id).await;
            }
            json_response_value(status, body)
        }
        Err(err) => upstream_failed(&state, &node, err).await,
    }
}

async fn multipart_init(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(object_key): Path<String>,
    Json(payload): Json<Value>,
) -> Response<Body> {
    multipart_init_mode(state, headers, object_key, payload, MultipartMode::Standard).await
}

async fn multipart_durable_init(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(object_key): Path<String>,
    Json(payload): Json<Value>,
) -> Response<Body> {
    multipart_init_mode(state, headers, object_key, payload, MultipartMode::Durable).await
}

async fn multipart_init_mode(
    state: Arc<GatewayState>,
    headers: HeaderMap,
    object_key: String,
    mut payload: Value,
    mode: MultipartMode,
) -> Response<Body> {
    let bearer = match bearer_from_headers(&headers) {
        Ok(value) => value,
        Err(err) => return text_response(StatusCode::UNAUTHORIZED, err),
    };
    let object_key = decode_path(&object_key);
    let node = match best_node(&state).await {
        Ok(node) => node,
        Err(response) => return response,
    };
    payload["object_key"] = Value::String(object_key.clone());
    let (status, mut body) = match native_json(&node, &bearer, mode.init_op(), payload).await {
        Ok(value) => value,
        Err(err) => return native_call_failed(&state, &node, err).await,
    };
    if status < 300 {
        body["coordinator_node_id"] = Value::String(node.id.clone());
        body["gateway"] = json!({
            "coordinator_node_id": node.id,
            "route_only": true,
            "resumability_owner": "client",
        });
    }
    json_response(status, body)
}

async fn multipart_list(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response<Body> {
    multipart_list_mode(state, headers, query, MultipartMode::Standard).await
}

async fn multipart_durable_list(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response<Body> {
    multipart_list_mode(state, headers, query, MultipartMode::Durable).await
}

async fn multipart_list_mode(
    state: Arc<GatewayState>,
    headers: HeaderMap,
    query: HashMap<String, String>,
    mode: MultipartMode,
) -> Response<Body> {
    let bearer = match bearer_from_headers(&headers) {
        Ok(value) => value,
        Err(err) => return text_response(StatusCode::UNAUTHORIZED, err),
    };
    let limit = query
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(50)
        .min(100);
    let offset = query
        .get("offset")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    let per_node_target = offset.saturating_add(limit);
    let nodes = state.nodes.available_configs().await;
    if nodes.is_empty() {
        return text_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "gateway has no available N2 nodes",
        );
    }

    let results = join_all(nodes.into_iter().map(|node| {
        let bearer = bearer.clone();
        let http = state.http.clone();
        async move {
            let result =
                list_multipart_sessions_on_node(&http, &node, &bearer, per_node_target, mode).await;
            (node, result)
        }
    }))
    .await;

    let mut sessions = Vec::new();
    let mut total = 0usize;
    let mut successful_nodes = 0usize;
    let mut unavailable_nodes = Vec::new();
    for (node, result) in results {
        match result {
            Ok((mut node_sessions, node_total)) => {
                successful_nodes += 1;
                total = total.saturating_add(node_total);
                for session in &mut node_sessions {
                    if let Some(object) = session.as_object_mut() {
                        object.insert(
                            "coordinator_node_id".to_string(),
                            Value::String(node.id.clone()),
                        );
                    }
                }
                sessions.extend(node_sessions);
            }
            Err(MultipartListNodeError::Authorization(err)) => {
                return text_response(StatusCode::UNAUTHORIZED, err);
            }
            Err(MultipartListNodeError::Unavailable(err)) => {
                state.nodes.mark_failure(&node.id).await;
                unavailable_nodes.push(json!({
                    "node_id": node.id,
                    "error": err,
                }));
            }
        }
    }

    if successful_nodes == 0 {
        return text_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "gateway could not query active uploads from any N2 node",
        );
    }

    sessions.sort_by(|left, right| {
        session_created_at(right)
            .cmp(&session_created_at(left))
            .then_with(|| {
                session_string(left, "version_id").cmp(session_string(right, "version_id"))
            })
            .then_with(|| {
                session_string(left, "session_id").cmp(session_string(right, "session_id"))
            })
            .then_with(|| {
                session_string(left, "coordinator_node_id")
                    .cmp(session_string(right, "coordinator_node_id"))
            })
    });
    let sessions = sessions
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();

    json_response(
        200,
        json!({
            "sessions": sessions,
            "total": total,
            "partial": !unavailable_nodes.is_empty(),
            "queried_nodes": successful_nodes,
            "unavailable_nodes": unavailable_nodes,
        }),
    )
}

enum MultipartListNodeError {
    Authorization(String),
    Unavailable(String),
}

async fn list_multipart_sessions_on_node(
    http: &reqwest::Client,
    node: &NodeConfig,
    bearer: &str,
    target: usize,
    mode: MultipartMode,
) -> Result<(Vec<Value>, usize), MultipartListNodeError> {
    let mut sessions = Vec::new();
    let mut total = 0usize;
    let mut node_offset = 0usize;

    loop {
        let remaining = target.saturating_sub(sessions.len());
        let page_limit = remaining.min(100);
        let response = http
            .get(format!(
                "{}{}",
                node.http_url.trim_end_matches('/'),
                mode.http_path()
            ))
            .bearer_auth(bearer)
            .query(&[("limit", page_limit), ("offset", node_offset)])
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .map_err(|err| {
                MultipartListNodeError::Unavailable(format!(
                    "node {} active-upload listing failed: {}",
                    node.id, err
                ))
            })?;
        let status = response.status();
        if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
            return Err(MultipartListNodeError::Authorization(format!(
                "node {} rejected the bearer token",
                node.id
            )));
        }
        if !status.is_success() {
            return Err(MultipartListNodeError::Unavailable(format!(
                "node {} active-upload listing returned HTTP {}",
                node.id, status
            )));
        }
        let body = response.json::<Value>().await.map_err(|err| {
            MultipartListNodeError::Unavailable(format!(
                "node {} returned an invalid active-upload response: {}",
                node.id, err
            ))
        })?;

        total = body
            .get("total")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(total);
        let page = body
            .get("sessions")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                MultipartListNodeError::Unavailable(format!(
                    "node {} returned an invalid active-upload response",
                    node.id
                ))
            })?;
        let page_len = page.len();
        sessions.extend(page.iter().cloned());
        node_offset = node_offset.saturating_add(page_len);

        if target == 0 || page_len == 0 || sessions.len() >= target || node_offset >= total {
            break;
        }
    }

    Ok((sessions, total))
}

fn session_created_at(session: &Value) -> i64 {
    session
        .get("created_at")
        .and_then(Value::as_i64)
        .unwrap_or_default()
}

fn session_string<'a>(session: &'a Value, field: &str) -> &'a str {
    session
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_default()
}

async fn cluster_rebalance_start(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Response<Body> {
    cluster_job_start(state, headers, "cluster.rebalance.start", payload).await
}

async fn cluster_repair_start(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Response<Body> {
    cluster_job_start(state, headers, "cluster.repair.start", payload).await
}

async fn cluster_rebalance_status(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response<Body> {
    cluster_job_followup(state, headers, query, "cluster.rebalance.status").await
}

async fn cluster_repair_status(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response<Body> {
    cluster_job_followup(state, headers, query, "cluster.repair.status").await
}

async fn cluster_rebalance_stop(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response<Body> {
    cluster_job_followup(state, headers, query, "cluster.rebalance.stop").await
}

async fn cluster_repair_stop(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response<Body> {
    cluster_job_followup(state, headers, query, "cluster.repair.stop").await
}

async fn cluster_job_start(
    state: Arc<GatewayState>,
    headers: HeaderMap,
    op: &str,
    payload: Value,
) -> Response<Body> {
    let bearer = match bearer_from_headers(&headers) {
        Ok(value) => value,
        Err(err) => return text_response(StatusCode::UNAUTHORIZED, err),
    };
    let node = match best_node(&state).await {
        Ok(node) => node,
        Err(response) => return response,
    };
    let (status, mut body) = match native_json(&node, &bearer, op, payload).await {
        Ok(value) => value,
        Err(err) => return native_call_failed(&state, &node, err).await,
    };
    if status < 300 {
        body["coordinator_node_id"] = Value::String(node.id.clone());
        body["gateway"] = json!({
            "coordinator_node_id": node.id,
            "route_only": true,
            "job_state_owner": "coordinator_node",
        });
    }
    json_response(status, body)
}

async fn cluster_job_followup(
    state: Arc<GatewayState>,
    headers: HeaderMap,
    query: HashMap<String, String>,
    op: &str,
) -> Response<Body> {
    let bearer = match bearer_from_headers(&headers) {
        Ok(value) => value,
        Err(err) => return text_response(StatusCode::UNAUTHORIZED, err),
    };
    let node = match coordinator_node(&state, &headers, &query).await {
        Ok(node) => node,
        Err(response) => return response,
    };
    let (status, mut body) = match native_json(&node, &bearer, op, json!({})).await {
        Ok(value) => value,
        Err(err) => return native_call_failed(&state, &node, err).await,
    };
    if status < 300 {
        body["coordinator_node_id"] = Value::String(node.id.clone());
        body["gateway"] = json!({
            "coordinator_node_id": node.id,
            "route_only": true,
            "job_state_owner": "coordinator_node",
        });
    }
    json_response(status, body)
}

async fn multipart_put_part(
    State(state): State<Arc<GatewayState>>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Path((_version_id, _part_index)): Path<(String, u64)>,
    Query(query): Query<HashMap<String, String>>,
    body: Body,
) -> Response<Body> {
    multipart_put_part_mode(state, uri, headers, query, body).await
}

async fn multipart_durable_put_part(
    State(state): State<Arc<GatewayState>>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Path((_version_id, _part_index)): Path<(String, u64)>,
    Query(query): Query<HashMap<String, String>>,
    body: Body,
) -> Response<Body> {
    multipart_put_part_mode(state, uri, headers, query, body).await
}

async fn multipart_put_part_mode(
    state: Arc<GatewayState>,
    uri: Uri,
    headers: HeaderMap,
    query: HashMap<String, String>,
    body: Body,
) -> Response<Body> {
    let node = match coordinator_node(&state, &headers, &query).await {
        Ok(node) => node,
        Err(response) => return response,
    };
    proxy_selected_http(&state, node, Method::PUT, uri, headers, body).await
}

async fn multipart_status(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(version_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response<Body> {
    multipart_status_mode(state, headers, version_id, query, MultipartMode::Standard).await
}

async fn multipart_durable_status(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(version_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response<Body> {
    multipart_status_mode(state, headers, version_id, query, MultipartMode::Durable).await
}

async fn multipart_status_mode(
    state: Arc<GatewayState>,
    headers: HeaderMap,
    version_id: String,
    query: HashMap<String, String>,
    mode: MultipartMode,
) -> Response<Body> {
    let bearer = match bearer_from_headers(&headers) {
        Ok(value) => value,
        Err(err) => return text_response(StatusCode::UNAUTHORIZED, err),
    };
    let node = match coordinator_node(&state, &headers, &query).await {
        Ok(node) => node,
        Err(response) => return response,
    };
    match native_json(
        &node,
        &bearer,
        mode.status_op(),
        json!({ "version_id": version_id }),
    )
    .await
    {
        Ok((status, value)) => json_response(status, value),
        Err(err) => native_call_failed(&state, &node, err).await,
    }
}

async fn multipart_finalize(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path((version_id, session_id)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
    Json(payload): Json<Value>,
) -> Response<Body> {
    multipart_finalize_mode(
        state,
        headers,
        version_id,
        session_id,
        query,
        payload,
        MultipartMode::Standard,
    )
    .await
}

async fn multipart_durable_finalize(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path((version_id, session_id)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
    Json(payload): Json<Value>,
) -> Response<Body> {
    multipart_finalize_mode(
        state,
        headers,
        version_id,
        session_id,
        query,
        payload,
        MultipartMode::Durable,
    )
    .await
}

async fn multipart_finalize_mode(
    state: Arc<GatewayState>,
    headers: HeaderMap,
    version_id: String,
    session_id: String,
    query: HashMap<String, String>,
    mut payload: Value,
    mode: MultipartMode,
) -> Response<Body> {
    let bearer = match bearer_from_headers(&headers) {
        Ok(value) => value,
        Err(err) => return text_response(StatusCode::UNAUTHORIZED, err),
    };
    let node = match coordinator_node(&state, &headers, &query).await {
        Ok(node) => node,
        Err(response) => return response,
    };
    payload["version_id"] = Value::String(version_id.clone());
    payload["session_id"] = Value::String(session_id.clone());
    if let Some(object_key) = query.get("object_key") {
        payload["object_key"] = Value::String(object_key.clone());
    }
    match native_json(&node, &bearer, mode.finalize_op(), payload).await {
        Ok((status, value)) => json_response(status, value),
        Err(err) => native_call_failed(&state, &node, err).await,
    }
}

async fn multipart_cancel(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(version_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response<Body> {
    multipart_cancel_mode(state, headers, version_id, query, MultipartMode::Standard).await
}

async fn multipart_durable_cancel(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(version_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response<Body> {
    multipart_cancel_mode(state, headers, version_id, query, MultipartMode::Durable).await
}

async fn multipart_cancel_mode(
    state: Arc<GatewayState>,
    headers: HeaderMap,
    version_id: String,
    query: HashMap<String, String>,
    mode: MultipartMode,
) -> Response<Body> {
    let bearer = match bearer_from_headers(&headers) {
        Ok(value) => value,
        Err(err) => return text_response(StatusCode::UNAUTHORIZED, err),
    };
    let node = match coordinator_node(&state, &headers, &query).await {
        Ok(node) => node,
        Err(response) => return response,
    };
    let mut payload = query_to_json(query);
    payload["version_id"] = Value::String(version_id.clone());
    match native_json(&node, &bearer, mode.cancel_op(), payload).await {
        Ok((status, value)) => json_response(status, value),
        Err(err) => native_call_failed(&state, &node, err).await,
    }
}

#[cfg(test)]
#[path = "routes/tests.rs"]
mod tests;
