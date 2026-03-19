use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use assert_cmd::Command;
use axum::{
    Json, Router,
    extract::{Path, Query},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, task::JoinHandle};

const SECRET: &str = "test-secret";
const TOKEN: &str = "cli-token";
const STALE_TOKEN: &str = "stale-cli-token";
const INITIAL_REFRESH_TOKEN: &str = "refresh-token-1";
const ROTATED_REFRESH_TOKEN: &str = "refresh-token-2";

#[derive(serde::Deserialize)]
struct AuthPayload {
    fragment: String,
    secret: String,
    read: bool,
    write: bool,
    modify: bool,
    delete: bool,
}

#[derive(serde::Deserialize)]
struct RefreshPayload {
    refresh_token: String,
}

#[derive(serde::Deserialize)]
struct ClusterViewQuery {
    expected_epoch: Option<u64>,
}

#[derive(serde::Deserialize)]
struct PlacementQuery {
    uid: Option<String>,
    version_id: Option<String>,
    kc: Option<u8>,
    mc: Option<u8>,
}

async fn auth_handler(Json(payload): Json<AuthPayload>) -> impl IntoResponse {
    if payload.fragment != "admin"
        || payload.secret != SECRET
        || !payload.read
        || !payload.write
        || !payload.modify
        || !payload.delete
    {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": "bad auth"}))).into_response();
    }
    (StatusCode::OK, Json(json!({ "access_token": TOKEN }))).into_response()
}

async fn auth_with_refresh_handler(Json(payload): Json<AuthPayload>) -> impl IntoResponse {
    if payload.fragment != "admin"
        || payload.secret != SECRET
        || !payload.read
        || !payload.write
        || !payload.modify
        || !payload.delete
    {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": "bad auth"}))).into_response();
    }
    (
        StatusCode::OK,
        Json(json!({
            "access_token": STALE_TOKEN,
            "expires_in": 900_u64,
            "refresh_token": INITIAL_REFRESH_TOKEN,
            "refresh_expires_in": 3600_u64
        })),
    )
        .into_response()
}

async fn refresh_handler(
    refresh_calls: axum::extract::State<Arc<AtomicUsize>>,
    Json(payload): Json<RefreshPayload>,
) -> impl IntoResponse {
    if payload.refresh_token != INITIAL_REFRESH_TOKEN {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "bad refresh"})),
        )
            .into_response();
    }
    refresh_calls.fetch_add(1, Ordering::SeqCst);
    (
        StatusCode::OK,
        Json(json!({
            "access_token": TOKEN,
            "expires_in": 900_u64,
            "refresh_token": ROTATED_REFRESH_TOKEN,
            "refresh_expires_in": 3600_u64
        })),
    )
        .into_response()
}

fn authorized(headers: &HeaderMap) -> bool {
    authorized_with(headers, TOKEN)
}

fn authorized_with(headers: &HeaderMap, expected_token: &str) -> bool {
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(|value| value == format!("Bearer {expected_token}"))
        .unwrap_or(false)
}

async fn public_cluster_view_handler(Query(query): Query<ClusterViewQuery>) -> impl IntoResponse {
    if query.expected_epoch != Some(7) {
        return StatusCode::BAD_REQUEST.into_response();
    }

    let body = json!({
        "node_id": "node-a",
        "node_endpoint": "https://node-a:50001",
        "membership_epoch": 7_u64,
        "expected_epoch": 7_u64,
        "total_nodes": 3_usize,
        "active_nodes": 3_usize,
        "local_node_present": true,
        "local_node_active": true,
        "write_ready": true,
        "reasons": []
    });
    (StatusCode::OK, Json(body)).into_response()
}

async fn membership_get_handler(headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    let body = json!({
        "epoch": 9_u64,
        "nodes": [
            {
                "node_id": "node-a",
                "endpoint": "https://node-a:50001",
                "epoch": 9_u64,
                "active": true
            },
            {
                "node_id": "node-b",
                "endpoint": "https://node-b:50001",
                "epoch": 9_u64,
                "active": true
            }
        ]
    });
    (StatusCode::OK, Json(body)).into_response()
}

async fn membership_get_refresh_handler(headers: HeaderMap) -> impl IntoResponse {
    if authorized_with(&headers, STALE_TOKEN) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if !authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    let body = json!({
        "epoch": 9_u64,
        "nodes": [
            {
                "node_id": "node-a",
                "endpoint": "https://node-a:50001",
                "epoch": 9_u64,
                "active": true
            }
        ]
    });
    (StatusCode::OK, Json(body)).into_response()
}

async fn placement_handler(
    headers: HeaderMap,
    Path((fragment, object_key)): Path<(String, String)>,
    Query(query): Query<PlacementQuery>,
) -> impl IntoResponse {
    if !authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if fragment != "frag-1"
        || object_key != "image.png"
        || query.uid.as_deref() != Some("uid-1")
        || query.version_id.as_deref() != Some("ver-9")
        || query.kc != Some(2)
        || query.mc != Some(1)
    {
        return StatusCode::BAD_REQUEST.into_response();
    }

    let body = json!({
        "epoch": 9_u64,
        "object": {
            "uid": "uid-1",
            "fragment": "frag-1",
            "object_key": "image.png",
            "version_id": "ver-9"
        },
        "kc": 2_u8,
        "mc": 1_u8,
        "nc": 3_usize,
        "write_quorum": 2_u8,
        "targets": [
            {
                "node_id": "node-a",
                "endpoint": "https://node-a:50001",
                "epoch": 9_u64,
                "active": true
            },
            {
                "node_id": "node-b",
                "endpoint": "https://node-b:50001",
                "epoch": 9_u64,
                "active": true
            },
            {
                "node_id": "node-c",
                "endpoint": "https://node-c:50001",
                "epoch": 9_u64,
                "active": true
            }
        ]
    });
    (StatusCode::OK, Json(body)).into_response()
}

async fn spawn_server(router: Router) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (format!("http://{addr}"), handle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_cluster_view_skips_auth_and_renders() {
    let router = Router::new().route(
        "/v4/n2/cluster/view/public",
        get(public_cluster_view_handler),
    );
    let (base_url, handle) = spawn_server(router).await;

    let output = Command::cargo_bin("litiaina-n2-cli")
        .unwrap()
        .args([
            "--url",
            &base_url,
            "cluster-view",
            "--public",
            "--expected-epoch",
            "7",
        ])
        .output()
        .unwrap();

    handle.abort();

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "stdout={stdout}\nstderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout.contains("N2 Cluster View"));
    assert!(stdout.contains("write_ready"));
    assert!(stdout.contains("node-a"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn membership_get_can_emit_json() {
    let router = Router::new()
        .route("/authenticate/fragment", post(auth_handler))
        .route("/v4/n2/membership", get(membership_get_handler));
    let (base_url, handle) = spawn_server(router).await;

    let output = Command::cargo_bin("litiaina-n2-cli")
        .unwrap()
        .args([
            "--url",
            &base_url,
            "--secret",
            SECRET,
            "--json",
            "membership",
            "get",
        ])
        .output()
        .unwrap();

    handle.abort();

    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(parsed["epoch"], 9);
    assert_eq!(parsed["nodes"].as_array().unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn placement_passes_query_and_renders_targets() {
    let router = Router::new()
        .route("/authenticate/fragment", post(auth_handler))
        .route(
            "/v4/n2/placement/{fragment}/{object_key}",
            get(placement_handler),
        );
    let (base_url, handle) = spawn_server(router).await;

    let output = Command::cargo_bin("litiaina-n2-cli")
        .unwrap()
        .args([
            "--url",
            &base_url,
            "--secret",
            SECRET,
            "placement",
            "--fragment",
            "frag-1",
            "--object-key",
            "image.png",
            "--uid",
            "uid-1",
            "--version-id",
            "ver-9",
            "--kc",
            "2",
            "--mc",
            "1",
        ])
        .output()
        .unwrap();

    handle.abort();

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "stdout={stdout}\nstderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout.contains("N2 Placement"));
    assert!(stdout.contains("write_quorum"));
    assert!(stdout.contains("node-c"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn membership_get_refreshes_after_unauthorized_response() {
    let refresh_calls = Arc::new(AtomicUsize::new(0));
    let router = Router::new()
        .route("/authenticate/fragment", post(auth_with_refresh_handler))
        .route("/authenticate/refresh", post(refresh_handler))
        .route("/v4/n2/membership", get(membership_get_refresh_handler))
        .with_state(refresh_calls.clone());
    let (base_url, handle) = spawn_server(router).await;

    let output = Command::cargo_bin("litiaina-n2-cli")
        .unwrap()
        .args([
            "--url",
            &base_url,
            "--secret",
            SECRET,
            "--json",
            "membership",
            "get",
        ])
        .output()
        .unwrap();

    handle.abort();

    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(parsed["epoch"], 9);
    assert_eq!(refresh_calls.load(Ordering::SeqCst), 1);
}
