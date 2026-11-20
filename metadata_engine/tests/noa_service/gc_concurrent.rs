//! GC must defer active native/bridged writes, then remove their exact work.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real slow HTTP bodies, native/bridged ACK fencing and concurrent indexed trash GC"]
async fn trash_gc_defers_active_parts_and_fences_acknowledgements_before_cleanup() {
    let mut stack = Stack::with_block_bytes(262144).await;
    drop(stack.metadata.take());
    let config = stack.root.join("m1_config.ini");
    let mut ini = ini::Ini::load_from_file(&config).unwrap();
    ini.with_section(Some("garbage_collector"))
        .set("enabled", "false")
        .set("trash_retention_seconds", "0")
        .set("pause_millis", "0");
    ini.write_to_file(&config).unwrap();
    let staging = m1::server::ServerConfig::load(&config).unwrap().staging_dir;
    stack.start_metadata().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token, _) = maintenance::auth(&client, &base, "Active upload GC").await;
    let survivor = vec![41; 65537];
    maintenance::put(&client, &base, &token, "survivor.bin", &survivor).await;
    let config = serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    let stripe_bytes = native_part_bytes(&config);
    let mut results = Vec::new();
    for (path, part_bytes, bridge) in [
        ("native.bin", stripe_bytes, false),
        ("bridge.bin", 300001usize, true),
    ] {
        maintenance::put(&client, &base, &token, path, &[17; 31]).await;
        let route = format!("{base}/upload/multipart/durable");
        let upload = json_response(
            client
                .post(format!("{route}/init/{path}"))
                .bearer_auth(&token)
                .json(&json!({"total_size_bytes":part_bytes*3,
                    "part_size_bytes":part_bytes,"expected_parts":3}))
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        let version = upload["version_id"].as_str().unwrap();
        resume_progress::put(&client, &route, &token, version, 0, vec![7; part_bytes]).await;
        let (source, mut producer) = tokio::io::duplex(4096);
        let request = client
            .put(format!("{route}/{version}/1"))
            .bearer_auth(&token)
            .header("content-length", part_bytes)
            .body(reqwest::Body::wrap_stream(
                tokio_util::io::ReaderStream::new(source),
            ));
        let writer = tokio::spawn(async move { request.send().await.unwrap() });
        producer.write_all(&[7]).await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let mut native = maintenance::direct(&stack).await;
        loop {
            let active = if bridge {
                std::fs::read_dir(&staging).ok().is_some_and(|fragments| {
                    fragments.filter_map(Result::ok).any(|fragment| {
                        std::fs::read_dir(fragment.path().join(version))
                            .ok()
                            .is_some_and(|files| {
                                files.filter_map(Result::ok).any(|file| {
                                    file.path().extension().is_some_and(|ext| ext == "tmp")
                                })
                            })
                    })
                })
            } else {
                cli_core::result(native.control(n1_link::noa::Command::Health).await.unwrap())
                    .unwrap()["operations"]["active_operations"]
                    .as_u64()
                    .unwrap()
                    > 0
            };
            if active {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "writer never acquired its resources"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        json_response(
            client
                .delete(format!("{base}/objects/{path}"))
                .bearer_auth(&token)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        let start = || {
            client
                .post(format!("{base}/maintenance/gc/start"))
                .bearer_auth(&token)
        };
        json_response(start().send().await.unwrap(), 200).await;
        let deferred = wait_gc(&client, &base, &token).await;
        assert_eq!(deferred["failures"], 0, "{deferred}");
        assert!(deferred["deferred"].as_u64().unwrap() > 0, "{deferred}");
        producer.write_all(&vec![7; part_bytes - 1]).await.unwrap();
        producer.shutdown().await.unwrap();
        let response = writer.await.unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        json_response(start().send().await.unwrap(), 200).await;
        let cleaned = wait_gc(&client, &base, &token).await;
        assert_eq!(cleaned["phase"], "completed", "{cleaned}");
        assert_eq!(cleaned["failures"], 0, "{cleaned}");
        assert_eq!(cleaned["deferred"], 0, "{cleaned}");
        assert_eq!(
            client
                .get(format!("{route}/{version}"))
                .bearer_auth(&token)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        results.push(json!({"bridge":bridge,"deferred":deferred,"cleaned":cleaned}));
    }
    drop(stack.metadata.take());
    drop(stack.object.take());
    let db = maintenance::db(&stack);
    assert_eq!(maintenance::links(&db).len(), 1);
    db.verify().unwrap();
    drop(db);
    stack.start_object().await;
    stack.start_metadata().await;
    let read = client
        .get(format!("{base}/objects/get/survivor.bin"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(read.status(), StatusCode::OK);
    assert_eq!(read.bytes().await.unwrap().as_ref(), survivor);
    reliability::record(
        &stack.root.join("GC-CONCURRENT-PASSED.json"),
        &json!({"passed":true,"results":results,"survivor_verified_after_restart":true}),
    );
}

async fn wait_gc(client: &Client, base: &str, token: &str) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let state = json_response(
            client
                .get(format!("{base}/maintenance/gc/status"))
                .bearer_auth(token)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        if state["running"] == false {
            return state;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "GC waited for an active writer: {state}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
