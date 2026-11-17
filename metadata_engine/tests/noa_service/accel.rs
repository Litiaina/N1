//! Actual independently built Accel -> M1 -> native N1 read/search integration.
use super::*;
#[path = "accel/tls.rs"]
mod tls;
use futures_util::StreamExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires real N1/Accel binaries and isolated storage fixture roots"]
async fn configured_accel_uses_authorized_pinned_reads_and_namespace_search_with_origin_fallback() {
    let mut stack = Stack::new().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (alias, token, _) = maintenance::auth(&client, &base, "accel-integration").await;
    let payload = (0..1048595).map(|n| (n % 251) as u8).collect::<Vec<_>>();
    let uploaded = json_response(
        client
            .post(format!("{base}/upload/one-shot/media/video.bin"))
            .bearer_auth(&token)
            .body(payload.clone())
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    let version = uploaded["version_id"].as_str().unwrap();
    let accel_root = stack.root.join("accel");
    std::fs::create_dir(&accel_root).unwrap();
    let accel_port = port();
    let metadata_env = std::fs::read_to_string(stack.root.join("m1.env")).unwrap();
    let jwt = metadata_env
        .lines()
        .find_map(|line| line.strip_prefix("JWT_SECRET="))
        .unwrap();
    let config = format!(
        "[server]\nbind=127.0.0.1:{accel_port}\ntls_cert=\ntls_key=\n\
        [cache]\ncapacity_bytes=16777216\nmax_entry_bytes=8388608\nttl_seconds=3600\nidle_seconds=900\nmemory_budget_bytes=67108864\nblock_bytes=262144\nbody_idle_timeout_ms=10000\nfill_wait_timeout_ms=10000\nmax_active_requests=16\n\
        [search]\ncapacity_bytes=8388608\nmax_fragment_entries=10000\nrefresh_seconds=30\norigin_idle_timeout_ms=10000\n\
        [security]\nadmin_key_env=ACCEL_ADMIN_KEY\nservice_key_env=ACCEL_SERVICE_KEY\n\
        [backend:n1-primary]\nenabled=true\nengine=n1\norigin={}\njwt_secret_env=ACCEL_N1_JWT_SECRET\nallow_invalid_tls=false\ncache_private_responses=true\nconnect_timeout_ms=1000\nheader_timeout_ms=10000\nqueue_timeout_ms=1000\nmax_inflight_requests=16\n",
        stack.url
    );
    std::fs::write(accel_root.join("accel_config.ini"), config).unwrap();
    std::fs::write(accel_root.join("accel.env"), format!("ACCEL_ADMIN_KEY=test-private-admin\nACCEL_SERVICE_KEY=test-private-service\nACCEL_N1_JWT_SECRET={jwt}\n")).unwrap();
    std::fs::set_permissions(
        accel_root.join("accel.env"),
        std::fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    let binary = PathBuf::from(
        std::env::var_os("N1_ACCEL_TEST_BINARY").expect("explicit Accel binary required"),
    );
    let mut accel = Some(spawn(&binary, &[], &accel_root, "accel.log"));
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        accel.as_mut().unwrap().0.try_wait().unwrap().is_none(),
        "{}",
        std::fs::read_to_string(accel_root.join("accel.log")).unwrap()
    );
    drop(stack.metadata.take());
    std::fs::write(stack.root.join("m1.env"), format!("{metadata_env}N1_ACCEL_READ_URL=http://127.0.0.1:{accel_port}\nN1_ACCEL_SEARCH_URL=http://127.0.0.1:{accel_port}\n")).unwrap();
    stack.start_metadata().await;

    let url = format!("{base}/objects/get/media/video.bin");
    for _ in 0..3 {
        let read = client.get(&url).bearer_auth(&token).send().await.unwrap();
        assert_eq!(read.status(), 200);
        assert!(
            read.headers().contains_key("x-accel-cache"),
            "read bypassed configured Accel"
        );
        assert_eq!(read.headers()["etag"], format!("\"{version}\""));
        assert_eq!(read.bytes().await.unwrap().as_ref(), payload);
    }
    let read = client
        .get(&url)
        .bearer_auth(&token)
        .header("range", "bytes=19-173")
        .send()
        .await
        .unwrap();
    assert_eq!(read.status(), 206);
    assert!(read.headers().contains_key("x-accel-cache"));
    assert_eq!(read.bytes().await.unwrap().as_ref(), &payload[19..174]);
    // Cache origin recursion guard uses the ordinary named read and authenticates.
    let direct = client
        .get(&url)
        .bearer_auth(&token)
        .header("x-litiaina-accel-origin", "1")
        .send()
        .await
        .unwrap();
    assert!(!direct.headers().contains_key("x-accel-cache"));
    assert_eq!(direct.bytes().await.unwrap().as_ref(), payload);
    assert_eq!(client.get(&url).send().await.unwrap().status(), 401);
    let search = client
        .get(format!("{base}/namespace/search"))
        .bearer_auth(&token)
        .query(&[
            ("q", "video.bin"),
            ("field", "filename"),
            ("match", "exact"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(search.status(), 200);
    assert!(search.headers().contains_key("x-accel-search"));
    let search: Value = search.json().await.unwrap();
    assert!(search.to_string().contains("media/video.bin"), "{search}");
    maintenance::put(&client, &base, &token, "UPPER/Case.bin", b"case").await;
    for (scope, count) in [("UPPER", 1), ("upper", 0)] {
        let scoped = client
            .get(format!("{base}/namespace/search"))
            .bearer_auth(&token)
            .query(&[("q", "case"), ("scope", scope)])
            .send()
            .await
            .unwrap();
        assert_eq!(scoped.status(), 200);
        assert_eq!(scoped.headers()["x-m1-search"], "native");
        let scoped: Value = scoped.json().await.unwrap();
        assert_eq!(scoped["results"].as_array().unwrap().len(), count);
    }
    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", stack.native))
        .await
        .unwrap();
    assert!(
        native(&mut socket, "auth.bearer", json!({"token":token}), None)
            .await
            .0
            .ok
    );
    let (response, bytes) = native(
        &mut socket,
        "namespace.search",
        json!({"q":"video.bin","field":"filename","match":"exact"}),
        None,
    )
    .await;
    assert!(response.ok);
    assert!(
        String::from_utf8(bytes)
            .unwrap()
            .contains("media/video.bin")
    );
    drop(socket);

    // Revoke the fragment's prior tokens: cached bytes cannot bypass metadata auth.
    let changed = client
        .patch(format!("{base}/fragments/{alias}"))
        .bearer_auth(&token)
        .json(&json!({"secret":"test-only-master","new_key":"replacement-fragment-key"}))
        .send()
        .await
        .unwrap();
    assert_eq!(changed.status(), 200);
    assert_eq!(
        client
            .get(&url)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    let new_token = json_response(client.post(format!("{base}/auth/fragment"))
        .json(&json!({"fragment":alias,"secret":"replacement-fragment-key","read":true,"write":true,"modify":true,"delete":true})).send().await.unwrap(), 200).await;
    let token = new_token["access_token"].as_str().unwrap();
    maintenance::put(&client, &base, token, "media/video-copy.bin", b"second").await;
    let read = client.get(&url).bearer_auth(token).send().await.unwrap();
    assert!(read.headers().contains_key("x-accel-cache"));
    assert_eq!(read.bytes().await.unwrap().as_ref(), payload);
    drop(accel.take());
    let read = client.get(&url).bearer_auth(token).send().await.unwrap();
    assert_eq!(read.status(), 200);
    assert!(!read.headers().contains_key("x-accel-cache"));
    assert_eq!(read.bytes().await.unwrap().as_ref(), payload);
    let search = client
        .get(format!("{base}/namespace/search?q=video.bin"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(search.status(), 200);
    assert_eq!(search.headers()["x-m1-search"], "native");
    let search: Value = search.json().await.unwrap();
    assert!(search.to_string().contains("media/video.bin"), "{search}");
    let first: Value = client
        .get(format!("{base}/namespace/search?q=video&limit=1"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let continuation = first["continuation_token"].as_str().unwrap();
    accel = Some(spawn(&binary, &[], &accel_root, "accel-restarted.log"));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let available = client
            .get(format!("{base}/namespace/search?q=video"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap();
        if available.headers().contains_key("x-accel-search") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Accel did not resume"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let resumed = client
        .get(format!("{base}/namespace/search"))
        .bearer_auth(token)
        .query(&[
            ("q", "video"),
            ("limit", "1"),
            ("continuation_token", continuation),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(resumed.status(), 200);
    assert_eq!(resumed.headers()["x-m1-search"], "native");
    let resumed: Value = resumed.json().await.unwrap();
    assert_ne!(
        resumed["results"][0]["object_key"],
        first["results"][0]["object_key"]
    );
    drop(accel.take());

    // A faulty accelerator may only cause a pre-header fallback. Once its
    // bytes are published, a truncated body must fail instead of being spliced
    // together with a second origin read.
    use axum::{body::Body, http::Response};
    use std::sync::{
        Arc,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    };
    let mode = Arc::new(AtomicU8::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let handler = {
        let mode = mode.clone();
        let calls = calls.clone();
        let version = version.to_owned();
        let size = payload.len();
        move || {
            let mode = mode.load(Ordering::Relaxed);
            let calls = calls.clone();
            let version = version.clone();
            async move {
                calls.fetch_add(1, Ordering::Relaxed);
                match mode {
                    0 => Response::builder().status(502).body(Body::empty()).unwrap(),
                    1 => Response::builder()
                        .header("etag", "\"wrong-version\"")
                        .body(Body::from("wrong"))
                        .unwrap(),
                    2 => Response::builder()
                        .status(206)
                        .header("etag", format!("\"{version}\""))
                        .header("content-length", "155")
                        .header("content-range", format!("bytes 0-154/{size}"))
                        .body(Body::from(vec![0; 155]))
                        .unwrap(),
                    3 => Response::builder()
                        .status(302)
                        .header("location", "/must-not-follow")
                        .body(Body::empty())
                        .unwrap(),
                    _ => Response::builder()
                        .header("etag", format!("\"{version}\""))
                        .header("content-length", size)
                        .body(Body::from_stream(
                            futures_util::stream::once(async {
                                Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"partial"))
                            })
                            .chain(futures_util::stream::once(
                                async {
                                    tokio::time::sleep(Duration::from_millis(200)).await;
                                    Err(std::io::Error::other("injected cache truncation"))
                                },
                            )),
                        ))
                        .unwrap(),
                }
            }
        }
    };
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", accel_port))
        .await
        .unwrap();
    let fault_server = tokio::spawn(async move {
        axum::serve(listener, axum::Router::new().fallback(handler))
            .await
            .unwrap();
    });
    for fault in 0..4 {
        mode.store(fault, Ordering::Relaxed);
        let previous = calls.load(Ordering::Relaxed);
        let mut request = client.get(&url).bearer_auth(token);
        if fault == 2 {
            request = request.header("range", "bytes=19-173");
        }
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), if fault == 2 { 206 } else { 200 });
        assert!(!response.headers().contains_key("x-accel-cache"));
        assert_eq!(
            response.bytes().await.unwrap().as_ref(),
            if fault == 2 {
                &payload[19..174]
            } else {
                &payload
            }
        );
        assert_eq!(calls.load(Ordering::Relaxed), previous + 1);
    }
    mode.store(4, Ordering::Relaxed);
    let previous = calls.load(Ordering::Relaxed);
    let response = client.get(&url).bearer_auth(token).send().await.unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.bytes().await.is_err());
    assert_eq!(calls.load(Ordering::Relaxed), previous + 1);
    fault_server.abort();
    let _ = fault_server.await;
}
