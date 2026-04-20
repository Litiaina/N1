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
const EXPIRING_TOKEN: &str = "expiring-cli-token";
const INITIAL_REFRESH_TOKEN: &str = "refresh-token-1";
const ROTATED_REFRESH_TOKEN: &str = "refresh-token-2";
const AUTH_FRAGMENT_NAME: &str = "mainframe";
const AUTH_FRAGMENT_HASH: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

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
struct FragmentCreatePayload {
    name: String,
    key: String,
    secret: String,
}

#[derive(serde::Deserialize)]
struct FragmentHashPayload {
    fragment: String,
}

#[derive(serde::Deserialize)]
struct HealthQuery {
    deep_scan: Option<String>,
}

async fn auth_handler(Json(payload): Json<AuthPayload>) -> impl IntoResponse {
    if payload.fragment != AUTH_FRAGMENT_HASH
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
    if payload.fragment != AUTH_FRAGMENT_HASH
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
            "access_token": EXPIRING_TOKEN,
            "expires_in": 0_u64,
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

async fn create_fragment_handler(Json(payload): Json<FragmentCreatePayload>) -> impl IntoResponse {
    if payload.name != "clinic" || payload.key != "clinic-key" || payload.secret != SECRET {
        return (StatusCode::BAD_REQUEST, "bad fragment payload").into_response();
    }
    (
        StatusCode::CREATED,
        Json(json!({
            "fragment_hash": "clinic-hash",
            "name": "clinic"
        })),
    )
        .into_response()
}

async fn fragment_hash_handler(Json(payload): Json<FragmentHashPayload>) -> impl IntoResponse {
    if payload.fragment != AUTH_FRAGMENT_NAME && payload.fragment != AUTH_FRAGMENT_HASH {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "fragment not found"})),
        )
            .into_response();
    }
    (
        StatusCode::OK,
        Json(json!({
            "fragment": payload.fragment,
            "fragment_hash": AUTH_FRAGMENT_HASH
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

async fn health_handler(headers: HeaderMap, Query(query): Query<HealthQuery>) -> impl IntoResponse {
    if !authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if query.deep_scan.as_deref() != Some("true") {
        return StatusCode::BAD_REQUEST.into_response();
    }

    let body = json!({
        "sampled_at": 1_710_000_000_i64,
        "status": "healthy",
        "disks": [
            {
                "path": "/mnt/DISK0",
                "role": "blob",
                "status": "online",
                "total_bytes": 100_u64,
                "used_bytes": 75_u64,
                "available_bytes": 25_u64,
                "usage_percent": 75.0_f64,
                "latency_ms": 2_u64,
                "error": null
            }
        ],
        "consistency": {
            "total_objects_scanned": 0_u64,
            "index_mirror_health": "healthy",
            "mirror_counts": null
        },
        "system": {
            "cpu_usage_percent": 10.5_f64,
            "cpu_cores": 8_u64,
            "ram_total_bytes": 1024_u64,
            "ram_used_bytes": 512_u64,
            "ram_usage_percent": 50.0_f64,
            "load_avg_1m": 0.3_f64,
            "uptime_seconds": 100_u64,
            "process_resident_bytes": 128_u64,
            "process_virtual_bytes": 256_u64,
            "process_open_fds": 12_u64
        },
        "uploads": {
            "active_fragments": 1_u64,
            "active_sessions": 2_u64,
            "reserved_bytes": 0_u64,
            "committed_parts": 0_u64,
            "committed_bytes": 0_u64,
            "oldest_session_age_seconds": null
        },
        "index_runtime": {
            "loaded_partition_runtimes": 1_u64,
            "wal_appenders": 1_u64,
            "prefix_projection_fragments_pending": 0_u64
        },
        "storage": {
            "index_roots_total": 1_u64,
            "index_roots_online": 1_u64,
            "blob_devices_total": 1_u64,
            "blob_devices_online": 1_u64,
            "raw_total_bytes": 100_u64,
            "raw_used_bytes": 75_u64,
            "raw_available_bytes": 25_u64,
            "heterogeneous_blob_capacity": true,
            "blob_capacity_imbalance_ratio": 4.0_f64,
            "placement_limiter_path": "/mnt/DISK0",
            "placement_limiter_available_bytes": 25_u64,
            "placement_limiter_usage_percent": 75.0_f64
        },
        "maintenance": {
            "repair_running": false,
            "repair_phase": "Idle",
            "redistribution_running": false,
            "redistribution_phase": "Idle",
            "gc_running": false,
            "gc_phase": "Idle"
        },
        "summary": "all green"
    });

    (StatusCode::OK, Json(body)).into_response()
}

async fn upload_status_handler(
    headers: HeaderMap,
    Path(version_id): Path<String>,
) -> impl IntoResponse {
    if !authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if version_id != "ver-123" {
        return StatusCode::BAD_REQUEST.into_response();
    }

    let body = json!({
        "uploaded_parts": [1, 2],
        "committed_parts": 2_u64,
        "committed_bytes": 2048_u64,
        "expected_parts": 2_u64,
        "total_size_bytes": 2048_u64,
        "created_at": 1_710_000_000_i64,
        "session_id": "sess-1",
        "version_id": "ver-123",
        "object_key": "images/xray.png"
    });
    (StatusCode::OK, Json(body)).into_response()
}

async fn gc_status_handler(headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    (
        StatusCode::OK,
        Json(json!({
            "is_running": false,
            "phase": "Completed",
            "is_manual_run": true,
            "total_fragments": 1_u64,
            "total_fragments_scanned": 1_u64,
            "total_live_versions_seen": 3_u64,
            "total_trash_versions_expired": 1_u64,
            "total_sessions_deleted": 0_u64,
            "total_orphan_blob_dirs_deleted": 0_u64,
            "total_refund_bytes": 1024_u64,
            "total_errors": 0_u64,
            "current_fragment": "frag-a",
            "last_error": null,
            "duration_seconds": 2_u64,
            "start_timestamp": 1_710_000_000_i64
        })),
    )
        .into_response()
}

async fn repair_status_handler(headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    (
        StatusCode::OK,
        Json(json!({
            "is_running": false,
            "phase": "Completed",
            "total_scanned": 10_u64,
            "total_repaired": 1_u64,
            "corrupt_found": 0_u64,
            "missing_meta_repaired": 0_u64,
            "divergent_meta_repaired": 0_u64,
            "current_fragment": "frag-a",
            "duration_seconds": 3_u64,
            "scan_rate_per_sec": 5.0_f64,
            "last_repaired": null,
            "last_disagreement": null,
            "last_error": null,
            "start_timestamp": 1_710_000_000_i64
        })),
    )
        .into_response()
}

async fn redistribution_status_handler(headers: HeaderMap) -> impl IntoResponse {
    if !authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    (
        StatusCode::OK,
        Json(json!({
            "is_running": false,
            "phase": "Completed",
            "total_fragments_scanned": 1_u64,
            "total_objects_scanned": 20_u64,
            "total_objects_need_redistribution": 0_u64,
            "total_objects_migrated": 0_u64,
            "total_errors": 0_u64,
            "current_fragment": "frag-a",
            "duration_seconds": 4_u64,
            "scan_rate_per_sec": 5.0_f64,
            "last_object": null,
            "last_error": null,
            "start_timestamp": 1_710_000_000_i64
        })),
    )
        .into_response()
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
async fn fragment_create_uses_direct_route() {
    let router = Router::new().route("/n1/fragments", post(create_fragment_handler));
    let (base_url, handle) = spawn_server(router).await;

    let output = Command::cargo_bin("litiaina-n1-cli")
        .unwrap()
        .args([
            "--url",
            &base_url,
            "--secret",
            SECRET,
            "fragment",
            "create",
            "--name",
            "clinic",
            "--key",
            "clinic-key",
        ])
        .output()
        .unwrap();

    handle.abort();

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"fragment_hash\":\"clinic-hash\""));
    assert!(stdout.contains("\"name\":\"clinic\""));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_renders_disk_usage_and_health() {
    let router = Router::new()
        .route("/n1/fragments/hash", get(fragment_hash_handler))
        .route("/n1/auth/fragment", post(auth_handler))
        .route("/n1/maintenance/health", get(health_handler));
    let (base_url, handle) = spawn_server(router).await;

    let output = Command::cargo_bin("litiaina-n1-cli")
        .unwrap()
        .args([
            "--url",
            &base_url,
            "--secret",
            SECRET,
            "--fragment",
            AUTH_FRAGMENT_NAME,
            "health",
            "--deep-scan",
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
    assert!(stdout.contains("N1 2.0 Health"));
    assert!(stdout.contains("/mnt/DISK0"));
    assert!(stdout.contains("75.00%"));
    assert!(stdout.contains("healthy"));
    assert!(stdout.contains("heterogeneous (4.00x max/min)"));
    assert!(stdout.contains("placement_limiter"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uploads_status_can_emit_json() {
    let router = Router::new()
        .route("/n1/fragments/hash", get(fragment_hash_handler))
        .route("/n1/auth/fragment", post(auth_handler))
        .route(
            "/n1/upload/multipart/{version_id}",
            get(upload_status_handler),
        );
    let (base_url, handle) = spawn_server(router).await;

    let output = Command::cargo_bin("litiaina-n1-cli")
        .unwrap()
        .args([
            "--url",
            &base_url,
            "--secret",
            SECRET,
            "--fragment",
            AUTH_FRAGMENT_NAME,
            "--json",
            "uploads",
            "status",
            "--version-id",
            "ver-123",
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
    assert_eq!(parsed["version_id"], "ver-123");
    assert_eq!(parsed["object_key"], "images/xray.png");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_auto_refreshes_expiring_access_token() {
    let refresh_calls = Arc::new(AtomicUsize::new(0));
    let router = Router::new()
        .route("/n1/fragments/hash", get(fragment_hash_handler))
        .route("/n1/auth/fragment", post(auth_with_refresh_handler))
        .route("/n1/auth/refresh", post(refresh_handler))
        .route("/n1/maintenance/health", get(health_handler))
        .with_state(refresh_calls.clone());
    let (base_url, handle) = spawn_server(router).await;

    let output = Command::cargo_bin("litiaina-n1-cli")
        .unwrap()
        .args([
            "--url",
            &base_url,
            "--secret",
            SECRET,
            "--fragment",
            AUTH_FRAGMENT_NAME,
            "health",
            "--deep-scan",
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
    assert!(stdout.contains("N1 2.0 Health"));
    assert_eq!(refresh_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_status_commands_use_n1_2_routes() {
    let router = Router::new()
        .route("/n1/fragments/hash", get(fragment_hash_handler))
        .route("/n1/auth/fragment", post(auth_handler))
        .route("/n1/maintenance/gc/status", get(gc_status_handler))
        .route("/n1/maintenance/repair/status", get(repair_status_handler))
        .route(
            "/n1/maintenance/redistribution/status",
            get(redistribution_status_handler),
        );
    let (base_url, handle) = spawn_server(router).await;

    for args in [
        vec![
            "--url",
            &base_url,
            "--secret",
            SECRET,
            "--fragment",
            AUTH_FRAGMENT_NAME,
            "--json",
            "gc",
            "status",
        ],
        vec![
            "--url",
            &base_url,
            "--secret",
            SECRET,
            "--fragment",
            AUTH_FRAGMENT_NAME,
            "--json",
            "repair",
            "status",
        ],
        vec![
            "--url",
            &base_url,
            "--secret",
            SECRET,
            "--fragment",
            AUTH_FRAGMENT_NAME,
            "--json",
            "redistribute",
            "status",
        ],
    ] {
        let output = Command::cargo_bin("litiaina-n1-cli")
            .unwrap()
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "stderr={}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        let parsed: Value = serde_json::from_str(&stdout).unwrap();
        assert_eq!(parsed["is_running"], false);
        assert_eq!(parsed["phase"], "Completed");
    }

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protected_commands_require_fragment_or_auth_fragment() {
    let output = Command::cargo_bin("litiaina-n1-cli")
        .unwrap()
        .args([
            "--url",
            "https://storage.example.test",
            "--secret",
            SECRET,
            "health",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("N1 auth is fragment-scoped"));
}
