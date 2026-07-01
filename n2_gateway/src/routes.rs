use axum::body::Body;
use axum::extract::ws::{CloseFrame, Message as AxumWsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{OriginalUri, Path, Query, State};
use axum::http::{HeaderMap, HeaderName, Method, Response, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use percent_encoding::percent_decode_str;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use tokio_tungstenite::Connector;
use tokio_tungstenite::tungstenite::Message as UpstreamWsMessage;
use tokio_util::io::ReaderStream;
use tower_http::cors::{Any, CorsLayer};

use crate::config::NodeConfig;
use crate::native::{
    bearer_from_headers, begin_native_download, content_length, content_type, native_json,
    native_upload_body,
};
use crate::node::NodeStatus;
use crate::proxy::{proxy_http, text_response};
use crate::state::GatewayState;

pub fn router(state: GatewayState) -> Router {
    let state = Arc::new(state);
    Router::new()
        .route("/gateway/health", get(gateway_health))
        .route("/gateway/nodes", get(gateway_nodes))
        .route("/ws", get(websocket_proxy))
        .route("/n2/auth/fragment", post(auth_fragment))
        .route("/n2/fragments", post(create_fragment))
        .route("/n2/cluster/rebalance/start", post(cluster_rebalance_start))
        .route(
            "/n2/cluster/rebalance/status",
            get(cluster_rebalance_status),
        )
        .route("/n2/cluster/rebalance/stop", post(cluster_rebalance_stop))
        .route("/n2/cluster/repair/start", post(cluster_repair_start))
        .route("/n2/cluster/repair/status", get(cluster_repair_status))
        .route("/n2/cluster/repair/stop", post(cluster_repair_stop))
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

async fn websocket_proxy(
    State(state): State<Arc<GatewayState>>,
    OriginalUri(uri): OriginalUri,
    ws: WebSocketUpgrade,
) -> Response<Body> {
    let node = match best_node(&state).await {
        Ok(node) => node,
        Err(response) => return response,
    };
    let upstream_url = match websocket_upstream_url(&node.http_url, &uri) {
        Ok(url) => url,
        Err(message) => return text_response(StatusCode::BAD_GATEWAY, message),
    };
    let tls = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyUpstreamCertificate))
        .with_no_client_auth();
    let upstream = match tokio_tungstenite::connect_async_tls_with_config(
        upstream_url,
        None,
        true,
        Some(Connector::Rustls(Arc::new(tls))),
    )
    .await
    {
        Ok((socket, _)) => socket,
        Err(error) => {
            state.nodes.mark_failure(&node.id).await;
            return text_response(
                StatusCode::BAD_GATEWAY,
                format!("gateway websocket upstream {} failed: {error}", node.id),
            );
        }
    };

    state.nodes.mark_route(&node.id).await;
    ws.on_upgrade(move |client| relay_websocket(client, upstream))
        .into_response()
}

fn websocket_upstream_url(http_url: &str, uri: &axum::http::Uri) -> Result<String, String> {
    let base = if let Some(rest) = http_url.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = http_url.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        return Err(format!("unsupported upstream URL scheme: {http_url}"));
    };
    let path_and_query = uri
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("/ws");
    Ok(format!("{}{path_and_query}", base.trim_end_matches('/')))
}

async fn relay_websocket(
    client: WebSocket,
    upstream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) {
    use futures_util::{SinkExt, StreamExt};

    let (mut client_sink, mut client_stream) = client.split();
    let (mut upstream_sink, mut upstream_stream) = upstream.split();

    loop {
        tokio::select! {
            incoming = client_stream.next() => {
                match incoming {
                    Some(Ok(message)) => {
                        let close = matches!(message, AxumWsMessage::Close(_));
                        if upstream_sink.send(axum_to_upstream_message(message)).await.is_err() || close {
                            break;
                        }
                    }
                    Some(Err(error)) => {
                        tracing::debug!("gateway websocket client receive failed: {error}");
                        break;
                    }
                    None => break,
                }
            }
            incoming = upstream_stream.next() => {
                match incoming {
                    Some(Ok(message)) => {
                        let Some(message) = upstream_to_axum_message(message) else {
                            continue;
                        };
                        let close = matches!(message, AxumWsMessage::Close(_));
                        if client_sink.send(message).await.is_err() || close {
                            break;
                        }
                    }
                    Some(Err(error)) => {
                        tracing::debug!("gateway websocket upstream receive failed: {error}");
                        break;
                    }
                    None => break,
                }
            }
        }
    }
    let _ = upstream_sink.close().await;
    let _ = client_sink.close().await;
}

fn axum_to_upstream_message(message: AxumWsMessage) -> UpstreamWsMessage {
    match message {
        AxumWsMessage::Text(value) => UpstreamWsMessage::Text(value.to_string().into()),
        AxumWsMessage::Binary(value) => UpstreamWsMessage::Binary(value),
        AxumWsMessage::Ping(value) => UpstreamWsMessage::Ping(value),
        AxumWsMessage::Pong(value) => UpstreamWsMessage::Pong(value),
        AxumWsMessage::Close(frame) => UpstreamWsMessage::Close(frame.map(|frame| {
            tokio_tungstenite::tungstenite::protocol::CloseFrame {
                code: frame.code.into(),
                reason: frame.reason.to_string().into(),
            }
        })),
    }
}

fn upstream_to_axum_message(message: UpstreamWsMessage) -> Option<AxumWsMessage> {
    match message {
        UpstreamWsMessage::Text(value) => Some(AxumWsMessage::Text(value.to_string().into())),
        UpstreamWsMessage::Binary(value) => Some(AxumWsMessage::Binary(value)),
        UpstreamWsMessage::Ping(value) => Some(AxumWsMessage::Ping(value)),
        UpstreamWsMessage::Pong(value) => Some(AxumWsMessage::Pong(value)),
        UpstreamWsMessage::Close(frame) => {
            Some(AxumWsMessage::Close(frame.map(|frame| CloseFrame {
                code: frame.code.into(),
                reason: frame.reason.to_string().into(),
            })))
        }
        UpstreamWsMessage::Frame(_) => None,
    }
}

#[derive(Debug)]
struct AcceptAnyUpstreamCertificate;

impl rustls::client::danger::ServerCertVerifier for AcceptAnyUpstreamCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        use rustls::SignatureScheme::*;
        vec![
            RSA_PKCS1_SHA1,
            ECDSA_SHA1_Legacy,
            RSA_PKCS1_SHA256,
            ECDSA_NISTP256_SHA256,
            RSA_PKCS1_SHA384,
            ECDSA_NISTP384_SHA384,
            RSA_PKCS1_SHA512,
            ECDSA_NISTP521_SHA512,
            RSA_PSS_SHA256,
            RSA_PSS_SHA384,
            RSA_PSS_SHA512,
            ED25519,
            ED448,
        ]
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
    Json(payload): Json<Value>,
) -> Response<Body> {
    let node = match best_node(&state).await {
        Ok(node) => node,
        Err(response) => return response,
    };
    match post_json_to_node(&state, &node, "/n2/fragments", &payload).await {
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
    Json(mut payload): Json<Value>,
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
    let (status, mut body) =
        match native_json(&node, &bearer, "upload.multipart.init", payload).await {
            Ok(value) => value,
            Err(err) => return upstream_failed(&state, &node, err).await,
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
        Err(err) => return upstream_failed(&state, &node, err).await,
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
        Err(err) => return upstream_failed(&state, &node, err).await,
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
    headers: HeaderMap,
    Path((version_id, part_index)): Path<(String, u64)>,
    Query(query): Query<HashMap<String, String>>,
    body: Body,
) -> Response<Body> {
    let bearer = match bearer_from_headers(&headers) {
        Ok(value) => value,
        Err(err) => return text_response(StatusCode::UNAUTHORIZED, err),
    };
    let node = match coordinator_node(&state, &headers, &query).await {
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
    Query(query): Query<HashMap<String, String>>,
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
    let node = match coordinator_node(&state, &headers, &query).await {
        Ok(node) => node,
        Err(response) => return response,
    };
    payload["version_id"] = Value::String(version_id.clone());
    payload["session_id"] = Value::String(session_id.clone());
    if let Some(object_key) = query.get("object_key") {
        payload["object_key"] = Value::String(object_key.clone());
    }
    match native_json(&node, &bearer, "upload.multipart.finalize", payload).await {
        Ok((status, value)) => json_response(status, value),
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
    let node = match coordinator_node(&state, &headers, &query).await {
        Ok(node) => node,
        Err(response) => return response,
    };
    let mut payload = query_to_json(query);
    payload["version_id"] = Value::String(version_id.clone());
    match native_json(&node, &bearer, "upload.multipart.cancel", payload).await {
        Ok((status, value)) => json_response(status, value),
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
    let node = match node_for_download(&state, query.version_id.as_deref()).await {
        Ok(node) => node,
        Err(response) => return response,
    };
    let payload = json!({
        "object_key": query.object_key,
        "version_id": query.version_id,
    });
    let download = match begin_native_download(&node, &bearer, payload).await {
        Ok(download) => download,
        Err(err) => {
            if let Some(status) = err.status {
                return text_response(
                    StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
                    err.message,
                );
            }
            return upstream_failed(&state, &node, err.message).await;
        }
    };
    let response_status = StatusCode::from_u16(download.status).unwrap_or(StatusCode::BAD_GATEWAY);
    let response_length = download.body_len;
    let (mut writer, reader) = tokio::io::duplex(1024 * 1024);
    let failed_state = state.clone();
    let failed_node = node.clone();
    tokio::spawn(async move {
        if let Err(err) = download.stream_to_writer(&mut writer).await {
            failed_state.nodes.mark_failure(&failed_node.id).await;
            tracing::warn!("gateway native download failed: {}", err);
        }
    });
    Response::builder()
        .status(response_status)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, response_length.to_string())
        .body(Body::from_stream(ReaderStream::new(reader)))
        .expect("valid download response")
}

async fn proxy_fallback(
    State(state): State<Arc<GatewayState>>,
    request: axum::extract::Request,
) -> Response<Body> {
    let path = request.uri().path().to_string();
    let node = match node_for_proxy_path(&state, &path).await {
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

async fn preferred_node(
    state: &GatewayState,
    preferred_id: &str,
) -> Result<NodeConfig, Response<Body>> {
    if state.nodes.is_available(preferred_id).await {
        if let Some(node) = state.nodes.get(preferred_id).await {
            state.nodes.mark_route(preferred_id).await;
            return Ok(node);
        }
    }
    best_node(state).await
}

async fn node_for_download(
    state: &GatewayState,
    _version_id: Option<&str>,
) -> Result<NodeConfig, Response<Body>> {
    preferred_node(state, read_preferred_node_id().as_str()).await
}

async fn node_for_proxy_path(
    state: &GatewayState,
    path: &str,
) -> Result<NodeConfig, Response<Body>> {
    if path == "/n2/objects/signed-url"
        || path == "/n2/objects/signed-read"
        || path.starts_with("/n2/objects/status/")
        || path.starts_with("/n2/objects/repair/")
        || path.starts_with("/n2/objects/get/")
        || path.starts_with("/n2/objects/stream/")
        || path == "/n2/trash"
        || path == "/n2/objects"
        || path.starts_with("/n2/posix/")
        || path.starts_with("/n2/objects/versions/")
        || path.starts_with("/n2/objects/meta/")
    {
        return preferred_node(state, read_preferred_node_id().as_str()).await;
    }

    best_node(state).await
}

fn read_preferred_node_id() -> String {
    std::env::var("N2_GATEWAY_READ_NODE_ID")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "node-a".to_string())
}

async fn best_node(state: &GatewayState) -> Result<NodeConfig, Response<Body>> {
    state.nodes.select_best().await.ok_or_else(|| {
        text_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "gateway has no available N2 nodes",
        )
    })
}

async fn coordinator_node(
    state: &GatewayState,
    headers: &HeaderMap,
    query: &HashMap<String, String>,
) -> Result<NodeConfig, Response<Body>> {
    let coordinator_node_id = coordinator_node_id(headers, query).ok_or_else(|| {
        text_response(
            StatusCode::BAD_REQUEST,
            "coordinator_node_id is required for coordinator-owned gateway routing",
        )
    })?;
    state.nodes.mark_route(&coordinator_node_id).await;
    if !state.nodes.is_available(&coordinator_node_id).await {
        return Err(text_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "coordinator node is unavailable",
        ));
    }
    state.nodes.get(&coordinator_node_id).await.ok_or_else(|| {
        text_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "coordinator node is not configured",
        )
    })
}

fn coordinator_node_id(headers: &HeaderMap, query: &HashMap<String, String>) -> Option<String> {
    headers
        .get("x-n2-coordinator-node")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| {
            ["coordinator_node_id", "coordinator", "node_id"]
                .into_iter()
                .find_map(|key| {
                    query
                        .get(key)
                        .map(String::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(str::to_string)
                })
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
        if matches!(
            key.as_str(),
            "coordinator_node_id" | "coordinator" | "node_id"
        ) {
            continue;
        }
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
    if status == StatusCode::NO_CONTENT || status == StatusCode::NOT_MODIFIED {
        return Response::builder()
            .status(status)
            .body(Body::empty())
            .expect("valid empty response");
    }
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
    use n2_native_client::{NativeError, NativeRequest, NativeResponse};
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
            if request.op == "object.download_stream" {
                let object_key = request
                    .payload
                    .as_deref()
                    .and_then(|payload| serde_json::from_str::<Value>(payload).ok())
                    .and_then(|payload| payload["object_key"].as_str().map(str::to_string))
                    .unwrap_or_default();
                if object_key == "missing" {
                    write_native_stream_response(
                        &mut stream,
                        request.id,
                        404,
                        false,
                        &[],
                        Some(NativeError {
                            code: "not_found".to_string(),
                            message: "object not found".to_string(),
                        }),
                    )
                    .await
                    .unwrap();
                } else {
                    write_native_stream_response(
                        &mut stream,
                        request.id,
                        200,
                        true,
                        b"download-bytes",
                        None,
                    )
                    .await
                    .unwrap();
                }
                continue;
            }
            let (status, body) = match request.op.as_str() {
                "auth.bearer" => (200, json!({ "node": node_id, "authenticated": true })),
                "upload.multipart.init" => (
                    201,
                    json!({ "session_id": "sess-a", "version_id": "ver-a" }),
                ),
                "upload.multipart.put_part" => (200, json!({ "stored": true })),
                "uploads.status" => (200, json!({ "status": "open" })),
                "upload.multipart.finalize" => (200, json!({ "finalized": true })),
                "upload.multipart.cancel" => (204, json!({})),
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

    async fn write_native_stream_response(
        stream: &mut TcpStream,
        id: String,
        status: u16,
        ok: bool,
        body: &[u8],
        error: Option<NativeError>,
    ) -> std::io::Result<()> {
        let response = NativeResponse {
            id,
            ok,
            status: Some(status),
            payload: None,
            body: None,
            body_len: Some(body.len() as u64),
            error,
        };
        let frame = rmp_serde::to_vec(&response).unwrap();
        stream
            .write_all(&(frame.len() as u32).to_be_bytes())
            .await?;
        stream.write_all(&frame).await?;
        stream.write_all(body).await?;
        stream.flush().await
    }

    fn gateway_config(nodes: Vec<NodeConfig>) -> crate::config::GatewayConfig {
        crate::config::GatewayConfig {
            host: "127.0.0.1".to_string(),
            https_port: 0,
            cert_path: PathBuf::from("./certificates/cert.pem"),
            key_path: PathBuf::from("./certificates/key.pem"),
            health_interval: Duration::from_secs(60),
            request_timeout: Duration::from_secs(10),
            nodes,
        }
    }

    async fn response_json(response: Response<Body>) -> Value {
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    async fn two_node_gateway_state(
        node_a: &MockNativeNode,
        node_b: &MockNativeNode,
    ) -> GatewayState {
        let state = GatewayState::new(gateway_config(vec![
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
        ]))
        .await
        .unwrap();
        state.nodes.mark_online_for_test("node-a", 1).await;
        state.nodes.mark_online_for_test("node-b", 1).await;
        state
    }

    #[tokio::test]
    async fn multipart_init_returns_client_owned_coordinator() {
        let node_a = spawn_mock_native_node("node-a").await;
        let node_b = spawn_mock_native_node("node-b").await;
        let state = two_node_gateway_state(&node_a, &node_b).await;

        let response = router(state)
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
        assert_eq!(body["version_id"], "ver-a");
        assert_eq!(body["coordinator_node_id"], "node-a");
        assert_eq!(body["gateway"]["route_only"], true);
        assert_eq!(body["gateway"]["resumability_owner"], "client");
    }

    #[tokio::test]
    async fn download_waits_for_upstream_status_before_committing_http_response() {
        let node_a = spawn_mock_native_node("node-a").await;
        let node_b = spawn_mock_native_node("node-b").await;
        let state = two_node_gateway_state(&node_a, &node_b).await;
        let app = router(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/n2/objects/download?object_key=present")
                    .header(header::AUTHORIZATION, "Bearer token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "download-bytes"
        );

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/n2/objects/download?object_key=missing")
                    .header(header::AUTHORIZATION, "Bearer token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "not_found: object not found"
        );
    }

    #[tokio::test]
    async fn multipart_followups_use_client_supplied_coordinator() {
        let node_a = spawn_mock_native_node("node-a").await;
        let node_b = spawn_mock_native_node("node-b").await;
        let state = two_node_gateway_state(&node_a, &node_b).await;
        let app = router(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/n2/upload/multipart/ver-a/0?crc32=123&coordinator_node_id=node-a")
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
                    .uri("/n2/upload/multipart/ver-a?coordinator_node_id=node-a")
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
                    .uri("/n2/upload/multipart/ver-a/sess-a/finalize?coordinator_node_id=node-a")
                    .header(header::AUTHORIZATION, "Bearer token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/n2/upload/multipart/ver-a?coordinator_node_id=node-a")
                    .header(header::AUTHORIZATION, "Bearer token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .is_empty()
        );

        let ops_a = node_a.operations.lock().await.clone();
        let ops_b = node_b.operations.lock().await.clone();
        assert!(ops_a.contains(&"upload.multipart.put_part".to_string()));
        assert!(ops_a.contains(&"uploads.status".to_string()));
        assert!(ops_a.contains(&"upload.multipart.finalize".to_string()));
        assert!(ops_a.contains(&"upload.multipart.cancel".to_string()));
        assert!(!ops_b.contains(&"upload.multipart.put_part".to_string()));
    }

    #[tokio::test]
    async fn multipart_followup_without_client_coordinator_is_rejected() {
        let node_a = spawn_mock_native_node("node-a").await;
        let node_b = spawn_mock_native_node("node-b").await;
        let state = two_node_gateway_state(&node_a, &node_b).await;

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
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn offline_client_supplied_coordinator_returns_service_unavailable() {
        let state = GatewayState::new(gateway_config(vec![NodeConfig {
            id: "node-a".to_string(),
            http_url: "https://127.0.0.1:51001".to_string(),
            native_url: "n2://127.0.0.1:51002".to_string(),
            weight: 1,
        }]))
        .await
        .unwrap();
        state.nodes.mark_offline_for_test("node-a").await;

        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/n2/upload/multipart/ver-a?coordinator_node_id=node-a")
                    .header(header::AUTHORIZATION, "Bearer token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn cluster_jobs_are_client_sticky_through_coordinator() {
        let node_a = spawn_mock_native_node("node-a").await;
        let node_b = spawn_mock_native_node("node-b").await;
        let state = two_node_gateway_state(&node_a, &node_b).await;
        let app = router(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/n2/cluster/rebalance/start")
                    .header(header::AUTHORIZATION, "Bearer token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"fragment":"frag","mismatched_only":true}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["coordinator_node_id"], "node-a");
        assert_eq!(body["gateway"]["route_only"], true);
        assert_eq!(body["gateway"]["job_state_owner"], "coordinator_node");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/n2/cluster/rebalance/status?coordinator_node_id=node-a")
                    .header(header::AUTHORIZATION, "Bearer token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/n2/cluster/rebalance/status")
                    .header(header::AUTHORIZATION, "Bearer token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let ops_a = node_a.operations.lock().await.clone();
        let ops_b = node_b.operations.lock().await.clone();
        assert!(ops_a.contains(&"cluster.rebalance.start".to_string()));
        assert!(ops_a.contains(&"cluster.rebalance.status".to_string()));
        assert!(!ops_b.contains(&"cluster.rebalance.status".to_string()));
    }

    #[test]
    fn websocket_upstream_url_preserves_the_authenticated_path_and_query() {
        let uri = "/ws?token=header.payload.signature".parse().unwrap();
        assert_eq!(
            websocket_upstream_url("https://10.0.0.2:53101/", &uri).unwrap(),
            "wss://10.0.0.2:53101/ws?token=header.payload.signature"
        );
        assert_eq!(
            websocket_upstream_url("http://127.0.0.1:53101", &uri).unwrap(),
            "ws://127.0.0.1:53101/ws?token=header.payload.signature"
        );
    }

    #[test]
    fn websocket_message_conversion_preserves_application_frames() {
        for message in [
            AxumWsMessage::Text("hello".into()),
            AxumWsMessage::Binary(bytes::Bytes::from_static(b"binary")),
            AxumWsMessage::Ping(bytes::Bytes::from_static(b"ping")),
            AxumWsMessage::Pong(bytes::Bytes::from_static(b"pong")),
        ] {
            let round_trip = upstream_to_axum_message(axum_to_upstream_message(message.clone()))
                .expect("application frame");
            assert_eq!(round_trip, message);
        }
    }

    #[test]
    fn websocket_message_conversion_preserves_close_frame() {
        let message = AxumWsMessage::Close(Some(CloseFrame {
            code: 1001,
            reason: "node draining".into(),
        }));
        let round_trip = upstream_to_axum_message(axum_to_upstream_message(message.clone()))
            .expect("close frame");
        assert_eq!(round_trip, message);
    }
}
