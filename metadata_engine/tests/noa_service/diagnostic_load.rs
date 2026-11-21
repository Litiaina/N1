//! Busy admission is incomplete verification; actual damaged frames must fail.
use super::*;
use m1::object_store::{NativeObjectStore, ObjectStoreConfig};

async fn scan(client: &Client, base: &str, token: &str) -> Value {
    let start = client
        .get(format!("{base}/maintenance/pools/health?deep_scan=true"))
        .bearer_auth(token)
        .send();
    let repeated = client
        .get(format!(
            "{base}/maintenance/health?deep_scan=true&refresh=true"
        ))
        .bearer_auth(token)
        .send();
    let (first, second) = tokio::join!(start, repeated);
    json_response(first.unwrap(), 200).await;
    json_response(second.unwrap(), 200).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let value = json_response(
            client
                .get(format!("{base}/maintenance/pools/health"))
                .bearer_auth(token)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        let state = &value["pools"][0]["integrity"];
        if state["status"] != "running" {
            return state.clone();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scan did not finish: {state}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real M1/N1 with held object admission and explicit private roots"]
async fn scan_progress_repeated_start_and_scoped_stop_preserve_object_bytes() {
    let mut stack = Stack::new().await;
    drop(stack.metadata.take());
    drop(stack.object.take());
    let mut config: Value =
        serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    config["max_operations"] = json!(1);
    config["runtime"]["io_control"]["foreground_wait_timeout_ms"] = json!(1000);
    std::fs::write(
        &stack.object_config,
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();
    stack.start_object().await;
    stack.start_metadata().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (alias, token, _) = maintenance::auth(&client, &base, "Scan controls").await;
    let (_, other, _) = maintenance::auth(&client, &base, "Other scan scope").await;
    let bytes = vec![29; 4096];
    let mut versions = Vec::new();
    for i in 0..16 {
        versions.push(
            maintenance::put(&client, &base, &token, &format!("file-{i:02}.bin"), &bytes).await,
        );
    }
    let credentials: Value = serde_json::from_slice(
        &std::fs::read(stack.object_config.with_extension("credentials.json")).unwrap(),
    )
    .unwrap();
    let objects = NativeObjectStore::new(ObjectStoreConfig {
        native_url: credentials["native_url"].as_str().unwrap().into(),
        fragment: credentials["fragment"].as_str().unwrap().into(),
        secret: credentials["secret"].as_str().unwrap().into(),
        max_connections: 2,
        tls: None,
    })
    .unwrap();
    json_response(
        client
            .get(format!("{base}/maintenance/health"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let part_bytes = native_part_bytes(&config);
    let held = objects
        .init(None, part_bytes as u64, part_bytes as u64, true, 3600)
        .await
        .unwrap();
    let (mut source, mut producer) = tokio::io::duplex(64);
    let (writer_objects, plan) = (objects.clone(), held.clone());
    let writer = tokio::spawn(async move {
        writer_objects
            .part(&plan, 0, part_bytes as u64, &mut source)
            .await
    });
    producer.write_all(&[7]).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let health = objects
            .control(n1_link::noa::Command::Health)
            .await
            .unwrap();
        if health["operations"]["active_operations"].as_u64().unwrap() > 0 {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let scan_url =
        format!("{base}/maintenance/fragments/{alias}/health?deep_scan=true&refresh=true");
    let started = json_response(
        client
            .get(&scan_url)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(started["integrity_scan"]["status"], "running");
    let mut repeats = Vec::new();
    for _ in 0..8 {
        let (client, url, token) = (client.clone(), scan_url.clone(), token.clone());
        repeats.push(tokio::spawn(async move {
            json_response(
                client.get(url).bearer_auth(token).send().await.unwrap(),
                200,
            )
            .await
        }));
    }
    for repeat in repeats {
        let value = repeat.await.unwrap();
        assert_eq!(
            value["integrity_scan"]["started_at"],
            started["integrity_scan"]["started_at"]
        );
        assert_eq!(value["integrity_scan"]["status"], "running");
    }
    let stop_url = format!("{base}/maintenance/fragments/{alias}/integrity/stop");
    assert_eq!(
        client
            .post(&stop_url)
            .bearer_auth(&other)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let stopped = json_response(
        client
            .post(&stop_url)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(stopped["stop_requested"], true);
    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", stack.native))
        .await
        .unwrap();
    assert!(
        native(&mut socket, "auth.bearer", json!({"token":token}), None)
            .await
            .0
            .ok
    );
    let (reply, body) = native(&mut socket, "maintenance.integrity.stop", json!({}), None).await;
    assert!(reply.ok);
    let report: Value = serde_json::from_slice(&body).unwrap();
    assert!(matches!(
        report["status"].as_str(),
        Some("running" | "cancelled")
    ));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let state = json_response(
            client
                .get(format!("{base}/maintenance/pools/health"))
                .bearer_auth(&token)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        if state["integrity_scan"]["status"] != "running" {
            assert_eq!(state["integrity_scan"]["status"], "cancelled");
            assert_eq!(state["integrity_scan"]["failed_versions"], 0);
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    producer.write_all(&vec![7; part_bytes - 1]).await.unwrap();
    producer.shutdown().await.unwrap();
    writer.await.unwrap().unwrap();
    objects.purge(&held.object_key).await.unwrap();
    let completed = scan(&client, &base, &token).await;
    assert_eq!(completed["status"], "passed");
    assert_eq!(completed["verified_versions"], 16);
    for (i, version) in versions.iter().enumerate() {
        let response = client
            .get(format!(
                "{base}/objects/get/file-{i:02}.bin?version_id={version}"
            ))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
    }
    let before = resume_progress::health_submitted(&client, &base, &token).await;
    for _ in 0..16 {
        json_response(
            client
                .get(format!("{base}/maintenance/health"))
                .bearer_auth(&token)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
    }
    assert_eq!(
        before,
        resume_progress::health_submitted(&client, &base, &token).await,
        "ordinary monitoring read object payloads"
    );
    objects.shutdown().await;
    reliability::record(
        &stack.root.join("MONITORING-CONTROLS-PASSED.json"),
        &json!({"passed":true,"duplicate_starts":8,"scoped_stop":true,"verified_objects":16,"ordinary_monitoring_payload_io":0}),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real object admission pressure and private shard corruption; explicit fixture roots required"]
async fn deep_scan_distinguishes_saturated_upload_admission_from_confirmed_corruption() {
    let mut stack = Stack::new().await;
    drop(stack.metadata.take());
    drop(stack.object.take());
    let mut config: Value =
        serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    config["max_operations"] = json!(1);
    config["read_repair"] = json!(false);
    config["runtime"]["io_control"]["foreground_wait_timeout_ms"] = json!(1000);
    std::fs::write(
        &stack.object_config,
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();
    stack.start_object().await;
    stack.start_metadata().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(40))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token, _) = maintenance::auth(&client, &base, "Concurrent diagnostic witness").await;
    let bytes = vec![41; 8197];
    maintenance::put(&client, &base, &token, "verified.bin", &bytes).await;
    let credentials: Value = serde_json::from_slice(
        &std::fs::read(stack.object_config.with_extension("credentials.json")).unwrap(),
    )
    .unwrap();
    let objects = NativeObjectStore::new(ObjectStoreConfig {
        native_url: credentials["native_url"].as_str().unwrap().into(),
        fragment: credentials["fragment"].as_str().unwrap().into(),
        secret: credentials["secret"].as_str().unwrap().into(),
        max_connections: 2,
        tls: None,
    })
    .unwrap();
    let part_bytes = native_part_bytes(&config);
    let upload = objects
        .init(None, part_bytes as u64, part_bytes as u64, true, 3600)
        .await
        .unwrap();
    let held = objects.clone();
    let plan = upload.clone();
    let (mut source, mut producer) = tokio::io::duplex(64);
    let writer =
        tokio::spawn(async move { held.part(&plan, 0, part_bytes as u64, &mut source).await });
    producer.write_all(&[7]).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let health = objects
            .control(n1_link::noa::Command::Health)
            .await
            .unwrap();
        if health["operations"]["active_operations"].as_u64().unwrap() > 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "upload did not own admission"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let busy = scan(&client, &base, &token).await;
    assert_eq!(busy["status"], "incomplete", "{busy}");
    assert_eq!(busy["failed_versions"], 0, "{busy}");
    assert_eq!(busy["deferred_versions"], 1, "{busy}");
    // The namespace lookup still succeeds while the only N1 operation slot
    // is held. A failure before payload delivery must reach the HTTP caller.
    for range in [None, Some("bytes=1-8")] {
        let mut request = client
            .get(format!("{base}/objects/get/verified.bin"))
            .bearer_auth(&token);
        if let Some(range) = range {
            request = request.header("range", range);
        }
        let response = request.send().await.unwrap();
        assert_eq!(
            response.status(),
            503,
            "success headers sent before admission"
        );
        let error = json_response(response, 503).await;
        assert!(error["error"].is_string());
    }
    producer.write_all(&vec![7; part_bytes - 1]).await.unwrap();
    producer.shutdown().await.unwrap();
    writer.await.unwrap().unwrap();
    objects.purge(&upload.object_key).await.unwrap();
    let idle = scan(&client, &base, &token).await;
    assert_eq!(idle["status"], "passed", "{idle}");
    assert_eq!(idle["verified_versions"], 1);
    let read = client
        .get(format!("{base}/objects/get/verified.bin"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(read.status(), 200);
    assert_eq!(read.bytes().await.unwrap().as_ref(), bytes);
    objects.shutdown().await;
    drop(stack.metadata.take());
    drop(stack.object.take());
    let database = maintenance::db(&stack);
    let keys = maintenance::links(&database);
    drop(database);
    assert_eq!(keys.len(), 1);
    let fingerprint = n1_link::ObjectKey::inspect(&keys[0]).unwrap().fingerprint();
    let layout: n1_link::PoolLayout =
        serde_json::from_value(config["pools"][0]["layout"].clone()).unwrap();
    for (slot, root) in config["pools"][0]["roots"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        let shard = layout.shard_at_slot(fingerprint, slot).unwrap();
        let path = Path::new(root.as_str().unwrap()).join(fingerprint.shard_path(shard));
        let mut corrupt = std::fs::read(&path).unwrap();
        corrupt[n1_link::shard::HEADER_BYTES + n1_link::shard::FRAME_HEADER_BYTES + 10] ^= 0x80;
        std::fs::write(&path, corrupt).unwrap();
    }
    stack.start_object().await;
    stack.start_metadata().await;
    let damaged = scan(&client, &base, &token).await;
    assert_eq!(damaged["status"], "failed", "{damaged}");
    assert_eq!(damaged["failed_versions"], 1);
    reliability::record(
        &stack.root.join("DIAGNOSTIC-ADMISSION-PASSED.json"),
        &json!({"passed":true,"busy":busy,"idle":idle,"damaged":damaged,"source_verified_before_corruption":true}),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real M1/N1 streams with deliberate damage to the last stripe on private members"]
async fn named_reads_fail_on_late_damage_without_publishing_corrupt_bytes() {
    use n1_link::shard::{FRAME_HEADER_BYTES, HEADER_BYTES, ShardHeader};
    use std::os::unix::fs::FileExt;

    let mut stack = Stack::with_block_bytes(262144).await;
    let client = Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token, _) = maintenance::auth(&client, &base, "Interrupted read").await;
    let bytes = vec![43; (12 << 20) + 113];
    maintenance::put(&client, &base, &token, "late.bin", &bytes).await;
    drop(stack.metadata.take());
    drop(stack.object.take());
    let keys = maintenance::links(&maintenance::db(&stack));
    assert_eq!(keys.len(), 1);
    let fingerprint = n1_link::ObjectKey::inspect(&keys[0]).unwrap().fingerprint();
    let config: Value =
        serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    let pool = &config["pools"][0];
    let layout: n1_link::PoolLayout = serde_json::from_value(pool["layout"].clone()).unwrap();
    let roots: Vec<PathBuf> = serde_json::from_value(pool["roots"].clone()).unwrap();
    for (slot, root) in roots.iter().enumerate() {
        let shard = layout.shard_at_slot(fingerprint, slot).unwrap();
        let file = std::fs::File::options()
            .read(true)
            .write(true)
            .open(root.join(fingerprint.shard_path(shard)))
            .unwrap();
        let mut raw = [0; HEADER_BYTES];
        file.read_exact_at(&mut raw, 0).unwrap();
        let header = ShardHeader::decode(&raw, &layout, fingerprint, shard).unwrap();
        let offset = header.frame_offset(header.stripes() - 1).unwrap() + FRAME_HEADER_BYTES as u64;
        let mut byte = [0];
        file.read_exact_at(&mut byte, offset).unwrap();
        byte[0] ^= 1;
        file.write_all_at(&byte, offset).unwrap();
        file.sync_data().unwrap();
    }
    stack.start_object().await;
    stack.start_metadata().await;
    let url = format!("{base}/objects/get/late.bin");
    for start in [0, 1] {
        let mut request = client.get(&url).bearer_auth(&token);
        if start > 0 {
            request = request.header("range", format!("bytes={start}-"));
        }
        let mut response = request.send().await.unwrap();
        assert_eq!(response.status(), if start == 0 { 200 } else { 206 });
        assert_eq!(
            response.content_length().unwrap(),
            (bytes.len() - start) as u64
        );
        let mut end = start;
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) => {
                    assert_eq!(chunk.as_ref(), &bytes[end..end + chunk.len()]);
                    end += chunk.len();
                }
                Ok(None) => panic!("late corruption reported as a successful download"),
                Err(_) => break,
            }
        }
        assert!(end > start && end < bytes.len());
    }
}
