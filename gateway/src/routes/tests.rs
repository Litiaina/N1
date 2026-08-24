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
    http_url: String,
    native_url: String,
    operations: Arc<Mutex<Vec<String>>>,
}

async fn spawn_mock_native_node(node_id: &'static str) -> MockNativeNode {
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_url = format!("http://{}", http_listener.local_addr().unwrap());
    let operations = Arc::new(Mutex::new(Vec::new()));
    let auth_fragment = move || async move {
        Json(json!({
            "access_token": format!("access-{node_id}"),
            "refresh_token": format!("refresh-{node_id}"),
            "expires_in": 900,
            "refresh_expires_in": 604800
        }))
    };
    let auth_refresh = move |Json(payload): Json<Value>| async move {
        if payload["refresh_token"] == format!("refresh-{node_id}") {
            (
                StatusCode::OK,
                Json(json!({
                    "access_token": format!("rotated-access-{node_id}"),
                    "refresh_token": format!("rotated-refresh-{node_id}"),
                    "expires_in": 900,
                    "refresh_expires_in": 603900
                })),
            )
        } else {
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "refresh session not found" })),
            )
        }
    };
    let auth_logout = move |Json(payload): Json<Value>| async move {
        if payload["refresh_token"] == format!("refresh-{node_id}") {
            (StatusCode::NO_CONTENT, Json(json!({})))
        } else {
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "refresh session not found" })),
            )
        }
    };
    let multipart_listing = move || async move {
        Json(json!({
            "sessions": [{
                "session_id": format!("session-{node_id}"),
                "version_id": format!("version-{node_id}"),
                "object_key": format!("uploads/{node_id}.bin"),
                "created_at": if node_id == "node-a" { 2 } else { 1 },
                "reserved_size": 0,
                "total_size_bytes": 1024,
                "expected_parts": 1,
                "committed_parts": 0,
                "committed_bytes": 0
            }],
            "total": 1
        }))
    };
    let batch_upload = move || async move {
        (
            StatusCode::CREATED,
            Json(json!({
                "requested": 1,
                "succeeded": 1,
                "failed": 0,
                "results": [{
                    "object_key": "proof/batch.bin",
                    "status": 201,
                    "version_id": format!("batch-{node_id}"),
                    "size_bytes": 4
                }]
            })),
        )
    };
    let namespace_operation = move |Path(operation_id): Path<String>| async move {
        Json(json!({
            "operation_id": operation_id,
            "coordinator_node_id": node_id,
            "state": "running"
        }))
    };
    let standard_put_operations = operations.clone();
    let standard_put = move |body: Body| {
        let operations = standard_put_operations.clone();
        async move {
            let _ = body.collect().await;
            operations
                .lock()
                .await
                .push("upload.multipart.standard.put_part".to_string());
            Json(json!({ "stored": true }))
        }
    };
    let durable_put_operations = operations.clone();
    let durable_put = move |body: Body| {
        let operations = durable_put_operations.clone();
        async move {
            let _ = body.collect().await;
            operations
                .lock()
                .await
                .push("upload.multipart.durable.put_part".to_string());
            Json(json!({ "stored": true }))
        }
    };
    let one_shot_operations = operations.clone();
    let one_shot = move |body: Body| {
        let operations = one_shot_operations.clone();
        async move {
            let _ = body.collect().await;
            operations.lock().await.push("upload.one_shot".to_string());
            (
                StatusCode::CREATED,
                Json(json!({ "version_id": "one-shot" })),
            )
        }
    };
    let download = |Query(query): Query<DownloadQuery>| async move {
        if query._object_key == "missing" {
            (
                StatusCode::NOT_FOUND,
                Body::from("not_found: object not found"),
            )
        } else {
            (StatusCode::OK, Body::from("download-bytes"))
        }
    };
    let http_app = Router::new()
        .route("/noa/v1/auth/fragment", post(auth_fragment))
        .route("/noa/v1/auth/refresh", post(auth_refresh))
        .route("/noa/v1/auth/logout", post(auth_logout))
        .route("/noa/v1/upload/multipart", get(multipart_listing))
        .route("/noa/v1/upload/multipart/durable", get(multipart_listing))
        .route(
            "/noa/v1/upload/multipart/{version_id}/{part_index}",
            put(standard_put),
        )
        .route(
            "/noa/v1/upload/multipart/durable/{version_id}/{part_index}",
            put(durable_put),
        )
        .route("/noa/v1/upload/one-shot/{*object_key}", post(one_shot))
        .route("/noa/v1/objects/download", get(download))
        .route("/noa/v1/upload/batch", post(batch_upload))
        .route(
            "/noa/v1/namespace/operations/{operation_id}",
            get(namespace_operation),
        );
    tokio::spawn(async move {
        axum::serve(http_listener, http_app).await.unwrap();
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let native_url = format!("n2://{}", listener.local_addr().unwrap());
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
        http_url,
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
        let missing_cancel_session = matches!(
            request.op.as_str(),
            "upload.multipart.cancel"
                | "upload.multipart.standard.cancel"
                | "upload.multipart.durable.cancel"
        ) && request
            .payload
            .as_deref()
            .and_then(|payload| serde_json::from_str::<Value>(payload).ok())
            .and_then(|payload| payload["version_id"].as_str().map(str::to_string))
            .is_some_and(|version_id| version_id == "missing-session");
        if missing_cancel_session {
            write_native_error_response(
                &mut stream,
                request.id,
                400,
                "bad_request",
                "Standard upload session not found",
            )
            .await
            .unwrap();
            continue;
        }
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
            "upload.multipart.init"
            | "upload.multipart.standard.init"
            | "upload.multipart.durable.init" => (
                201,
                json!({ "session_id": "sess-a", "version_id": "ver-a" }),
            ),
            "upload.multipart.put_part"
            | "upload.multipart.standard.put_part"
            | "upload.multipart.durable.put_part" => (200, json!({ "stored": true })),
            "uploads.status" | "uploads.standard.status" | "uploads.durable.status" => {
                (200, json!({ "status": "open" }))
            }
            "upload.multipart.finalize"
            | "upload.multipart.standard.finalize"
            | "upload.multipart.durable.finalize" => (200, json!({ "finalized": true })),
            "upload.multipart.cancel"
            | "upload.multipart.standard.cancel"
            | "upload.multipart.durable.cancel" => (204, json!({})),
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

async fn write_native_error_response(
    stream: &mut TcpStream,
    id: String,
    status: u16,
    code: &str,
    message: &str,
) -> std::io::Result<()> {
    let response = NativeResponse {
        id,
        ok: false,
        status: Some(status),
        payload: None,
        body: None,
        body_len: None,
        error: Some(NativeError {
            code: code.to_string(),
            message: message.to_string(),
        }),
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

async fn two_node_gateway_state(node_a: &MockNativeNode, node_b: &MockNativeNode) -> GatewayState {
    let state = GatewayState::new(gateway_config(vec![
        NodeConfig {
            id: "node-a".to_string(),
            http_url: node_a.http_url.clone(),
            native_url: node_a.native_url.clone(),
            weight: 1,
        },
        NodeConfig {
            id: "node-b".to_string(),
            http_url: node_b.http_url.clone(),
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
                .uri("/noa/v1/upload/multipart/init/videos/test.mp4")
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
async fn fragment_auth_returns_the_refresh_session_coordinator() {
    let node_a = spawn_mock_native_node("node-a").await;
    let node_b = spawn_mock_native_node("node-b").await;
    let state = two_node_gateway_state(&node_a, &node_b).await;

    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/noa/v1/auth/fragment")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"fragment":"test","secret":"test"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["refresh_token"], "refresh-node-a");
    assert_eq!(body["coordinator_node_id"], "node-a");
}

#[tokio::test]
async fn refresh_and_logout_route_to_the_authentication_coordinator() {
    let node_a = spawn_mock_native_node("node-a").await;
    let node_b = spawn_mock_native_node("node-b").await;
    let state = two_node_gateway_state(&node_a, &node_b).await;
    let app = router(state);

    let refresh = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/noa/v1/auth/refresh")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"refresh_token":"refresh-node-b","coordinator_node_id":"node-b"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(refresh.status(), StatusCode::OK);
    let body = response_json(refresh).await;
    assert_eq!(body["access_token"], "rotated-access-node-b");
    assert_eq!(body["coordinator_node_id"], "node-b");

    let logout = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/noa/v1/auth/logout")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"refresh_token":"refresh-node-b","coordinator_node_id":"node-b"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(logout.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn legacy_refresh_without_coordinator_finds_its_owning_node_once() {
    let node_a = spawn_mock_native_node("node-a").await;
    let node_b = spawn_mock_native_node("node-b").await;
    let state = two_node_gateway_state(&node_a, &node_b).await;

    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/noa/v1/auth/refresh")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"refresh_token":"refresh-node-b"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["access_token"], "rotated-access-node-b");
    assert_eq!(body["coordinator_node_id"], "node-b");
}

#[tokio::test]
async fn standard_batch_streams_through_gateway_without_route_state() {
    let node_a = spawn_mock_native_node("node-a").await;
    let node_b = spawn_mock_native_node("node-b").await;
    let state = two_node_gateway_state(&node_a, &node_b).await;
    let boundary = "n2-batch-boundary";
    let body = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"object\"; filename=\"proof%2Fbatch.bin\"\r\nContent-Type: application/octet-stream\r\n\r\ndata\r\n--{boundary}--\r\n"
    );

    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/noa/v1/upload/batch?concurrency=8")
                .header(header::AUTHORIZATION, "Bearer token")
                .header(
                    header::CONTENT_TYPE,
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = response_json(response).await;
    assert_eq!(body["requested"], 1);
    assert_eq!(body["succeeded"], 1);
    assert_eq!(body["failed"], 0);
}

#[tokio::test]
async fn multipart_list_aggregates_coordinators_with_global_pagination() {
    let node_a = spawn_mock_native_node("node-a").await;
    let node_b = spawn_mock_native_node("node-b").await;
    let state = two_node_gateway_state(&node_a, &node_b).await;
    let app = router(state);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/noa/v1/upload/multipart?limit=100")
                .header(header::AUTHORIZATION, "Bearer token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["total"], 2);
    assert_eq!(body["queried_nodes"], 2);
    assert_eq!(body["partial"], false);
    assert_eq!(body["sessions"].as_array().unwrap().len(), 2);
    assert_eq!(body["sessions"][0]["coordinator_node_id"], "node-a");
    assert_eq!(body["sessions"][0]["object_key"], "uploads/node-a.bin");
    assert_eq!(body["sessions"][1]["coordinator_node_id"], "node-b");
    assert_eq!(body["sessions"][1]["object_key"], "uploads/node-b.bin");

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/noa/v1/upload/multipart?limit=1&offset=1")
                .header(header::AUTHORIZATION, "Bearer token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["total"], 2);
    assert_eq!(body["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(body["sessions"][0]["coordinator_node_id"], "node-b");
}

#[tokio::test]
async fn multipart_list_returns_truthful_partial_result_when_one_node_is_unavailable() {
    let node_a = spawn_mock_native_node("node-a").await;
    let node_b = spawn_mock_native_node("node-b").await;
    let unavailable_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let unavailable_url = format!("http://{}", unavailable_listener.local_addr().unwrap());
    drop(unavailable_listener);
    let state = GatewayState::new(gateway_config(vec![
        NodeConfig {
            id: "node-a".to_string(),
            http_url: node_a.http_url.clone(),
            native_url: node_a.native_url.clone(),
            weight: 1,
        },
        NodeConfig {
            id: "node-b".to_string(),
            http_url: unavailable_url,
            native_url: node_b.native_url.clone(),
            weight: 1,
        },
    ]))
    .await
    .unwrap();
    state.nodes.mark_online_for_test("node-a", 1).await;
    state.nodes.mark_online_for_test("node-b", 1).await;

    let response = router(state)
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/noa/v1/upload/multipart?limit=100")
                .header(header::AUTHORIZATION, "Bearer token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["total"], 1);
    assert_eq!(body["queried_nodes"], 1);
    assert_eq!(body["partial"], true);
    assert_eq!(body["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(body["sessions"][0]["coordinator_node_id"], "node-a");
    assert_eq!(body["unavailable_nodes"][0]["node_id"], "node-b");
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
                .uri("/noa/v1/objects/download?object_key=present")
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
                .uri("/noa/v1/objects/download?object_key=missing")
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
async fn namespace_operation_status_uses_client_supplied_coordinator() {
    let node_a = spawn_mock_native_node("node-a").await;
    let node_b = spawn_mock_native_node("node-b").await;
    let state = two_node_gateway_state(&node_a, &node_b).await;

    let response = router(state)
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/noa/v1/namespace/operations/01K00000000000000000000000?coordinator_node_id=node-b")
                .header(header::AUTHORIZATION, "Bearer token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["operation_id"], "01K00000000000000000000000");
    assert_eq!(body["coordinator_node_id"], "node-b");
}

#[test]
fn proxy_query_parameters_decodes_coordinator_identity() {
    let query = proxy_query_parameters(Some(
        "coordinator_node_id=node%2Db&ignored=value%2Fwith%2Fslashes",
    ));
    assert_eq!(
        query.get("coordinator_node_id").map(String::as_str),
        Some("node-b")
    );
    assert_eq!(
        query.get("ignored").map(String::as_str),
        Some("value/with/slashes")
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
                .uri("/noa/v1/upload/multipart/ver-a/0?sha256=123&coordinator_node_id=node-a")
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
                .uri("/noa/v1/upload/multipart/ver-a?coordinator_node_id=node-a")
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
                .uri("/noa/v1/upload/multipart/ver-a/sess-a/finalize?coordinator_node_id=node-a")
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
                .uri("/noa/v1/upload/multipart/ver-a?coordinator_node_id=node-a")
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
    assert!(ops_a.contains(&"upload.multipart.standard.put_part".to_string()));
    assert!(ops_a.contains(&"uploads.standard.status".to_string()));
    assert!(ops_a.contains(&"upload.multipart.standard.finalize".to_string()));
    assert!(ops_a.contains(&"upload.multipart.standard.cancel".to_string()));
    assert!(!ops_b.contains(&"upload.multipart.standard.put_part".to_string()));
}

#[tokio::test]
async fn durable_multipart_followups_remain_on_the_selected_coordinator() {
    let node_a = spawn_mock_native_node("node-a").await;
    let node_b = spawn_mock_native_node("node-b").await;
    let state = two_node_gateway_state(&node_a, &node_b).await;
    let app = router(state);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(
                    "/noa/v1/upload/multipart/durable/ver-a/0?sha256=123&coordinator_node_id=node-a",
                )
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
                .uri("/noa/v1/upload/multipart/durable/ver-a?coordinator_node_id=node-a")
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
                .uri(
                    "/noa/v1/upload/multipart/durable/ver-a/sess-a/finalize?coordinator_node_id=node-a",
                )
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
                .uri("/noa/v1/upload/multipart/durable/ver-a?coordinator_node_id=node-a")
                .header(header::AUTHORIZATION, "Bearer token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let ops_a = node_a.operations.lock().await.clone();
    let ops_b = node_b.operations.lock().await.clone();
    assert!(ops_a.contains(&"upload.multipart.durable.put_part".to_string()));
    assert!(ops_a.contains(&"uploads.durable.status".to_string()));
    assert!(ops_a.contains(&"upload.multipart.durable.finalize".to_string()));
    assert!(ops_a.contains(&"upload.multipart.durable.cancel".to_string()));
    assert!(!ops_b.contains(&"upload.multipart.durable.put_part".to_string()));
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
                .uri("/noa/v1/upload/multipart/ver-a")
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
                .uri("/noa/v1/upload/multipart/ver-a?coordinator_node_id=node-a")
                .header(header::AUTHORIZATION, "Bearer token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn stale_offline_coordinator_is_reprobed_before_multipart_cancel() {
    let node_a = spawn_mock_native_node("node-a").await;
    let node_b = spawn_mock_native_node("node-b").await;
    let state = two_node_gateway_state(&node_a, &node_b).await;
    state.nodes.mark_offline_for_test("node-a").await;

    let response = router(state.clone())
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/noa/v1/upload/multipart/ver-a?object_key=videos%2Ftest.mp4&coordinator_node_id=node-a")
                .header(header::AUTHORIZATION, "Bearer token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let node_a_snapshot = state
        .nodes
        .snapshots()
        .await
        .into_iter()
        .find(|node| node.id == "node-a")
        .unwrap();
    assert!(matches!(node_a_snapshot.status, NodeStatus::Online));
    assert!(
        node_a
            .operations
            .lock()
            .await
            .contains(&"upload.multipart.standard.cancel".to_string())
    );
}

#[tokio::test]
async fn multipart_cancel_preserves_native_session_not_found_status() {
    let node_a = spawn_mock_native_node("node-a").await;
    let node_b = spawn_mock_native_node("node-b").await;
    let state = two_node_gateway_state(&node_a, &node_b).await;

    let response = router(state.clone())
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/noa/v1/upload/multipart/missing-session?object_key=videos%2Ftest.mp4&coordinator_node_id=node-a")
                .header(header::AUTHORIZATION, "Bearer token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "bad_request: Standard upload session not found"
    );
    let node_a_snapshot = state
        .nodes
        .snapshots()
        .await
        .into_iter()
        .find(|node| node.id == "node-a")
        .unwrap();
    assert!(matches!(node_a_snapshot.status, NodeStatus::Online));
    assert_eq!(node_a_snapshot.failure_count, 0);
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
                .uri("/noa/v1/cluster/rebalance/start")
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
                .uri("/noa/v1/cluster/rebalance/status?coordinator_node_id=node-a")
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
                .uri("/noa/v1/cluster/rebalance/status")
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

#[tokio::test]
async fn proxy_retry_replays_only_safe_empty_body_requests() {
    for method in [Method::GET, Method::HEAD] {
        let request = Request::builder()
            .method(method.clone())
            .uri("/noa/v1/objects/stream/video.mp4?version=latest")
            .header(header::AUTHORIZATION, "Bearer token")
            .body(Body::empty())
            .unwrap();
        let template = replay_safe_proxy_template(&request).expect("safe request template");
        let replay = replay_safe_proxy_request(&template);
        assert_eq!(replay.method(), method);
        assert_eq!(replay.uri(), request.uri());
        assert_eq!(
            replay.headers().get(header::AUTHORIZATION),
            request.headers().get(header::AUTHORIZATION)
        );
        assert!(
            replay
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .is_empty()
        );
    }

    for method in [Method::POST, Method::PUT, Method::PATCH, Method::DELETE] {
        let request = Request::builder()
            .method(method)
            .uri("/noa/v1/objects")
            .body(Body::from("mutation"))
            .unwrap();
        assert!(replay_safe_proxy_template(&request).is_none());
    }
}

#[test]
fn websocket_upstream_url_preserves_the_authenticated_path_and_query() {
    let uri = "/noa/v1/ws?token=header.payload.signature".parse().unwrap();
    assert_eq!(
        websocket_upstream_url("https://10.0.0.2:53101/", &uri).unwrap(),
        "wss://10.0.0.2:53101/noa/v1/ws?token=header.payload.signature"
    );
    assert_eq!(
        websocket_upstream_url("http://127.0.0.1:53101", &uri).unwrap(),
        "ws://127.0.0.1:53101/noa/v1/ws?token=header.payload.signature"
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
    let round_trip =
        upstream_to_axum_message(axum_to_upstream_message(message.clone())).expect("close frame");
    assert_eq!(round_trip, message);
}
