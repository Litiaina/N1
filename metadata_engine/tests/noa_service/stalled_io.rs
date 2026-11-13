//! A real delayed block device, controlled outside both production engines.
//! Only an explicitly supplied private loop filesystem may be delayed. The
//! controller and acknowledgement evidence stay on healthy storage.
use super::*;
use m1::object_store::{NativeObjectStore, ObjectStoreConfig};

async fn wait_for(path: &Path) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !path.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "controller: {path:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn measure_status(client: &Client, route: &str, token: &str, version: &str) -> Value {
    let mut elapsed = Vec::new();
    for _ in 0..100 {
        let since = std::time::Instant::now();
        let state = tokio::time::timeout(
            Duration::from_secs(2),
            resume_progress::status(client, route, token, version),
        )
        .await
        .expect("indexed status waited for a request body instead of observing committed ACKs");
        assert_eq!(state["uploaded_parts"], json!([0]));
        elapsed.push(since.elapsed().as_secs_f64() * 1000.0);
    }
    preparation_bench::distribution(elapsed)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real service processes, 100 idle/active resume measurements and interrupted HTTP body"]
async fn indexed_status_observes_committed_acks_without_waiting_for_an_incomplete_body() {
    let mut stack = Stack::with_block_bytes(262144).await;
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token) = reliability::fragment(&client, &base, "Concurrent indexed status").await;
    let route = format!("{base}/upload/multipart/durable");
    let bytes = vec![39; 2 * 1024 * 1024];
    let upload = json_response(
        client
            .post(format!("{route}/init/concurrent.bin"))
            .bearer_auth(&token)
            .json(&json!({"total_size_bytes":bytes.len(),
        "part_size_bytes":bytes.len()/2,"expected_parts":2}))
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let version = upload["version_id"].as_str().unwrap();
    resume_progress::put(
        &client,
        &route,
        &token,
        version,
        0,
        bytes[..bytes.len() / 2].to_vec(),
    )
    .await;
    let pid = stack.metadata.as_ref().unwrap().0.id();
    let before_idle = preparation_bench::resources(pid);
    let idle = measure_status(&client, &route, &token, version).await;
    let after_idle = preparation_bench::resources(pid);
    reliability::record(
        &stack.root.join("STATUS-IDLE.json"),
        &json!({"latency":idle,"before":before_idle,"after":after_idle}),
    );
    let url = reqwest::Url::parse(&stack.url).unwrap();
    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", url.port().unwrap()))
        .await
        .unwrap();
    let header = format!(
        "PUT /noa/v1/upload/multipart/durable/{version}/1 HTTP/1.1\r\n\
        Host: localhost\r\nAuthorization: Bearer {token}\r\n\
        Content-Length: {}\r\nConnection: close\r\n\r\n",
        bytes.len() / 2
    );
    socket.write_all(header.as_bytes()).await.unwrap();
    socket.write_all(&bytes[..4096]).await.unwrap();
    // Wait for N1 ownership, rather than hoping a sleep let the HTTP body enter.
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
    let before_active = preparation_bench::resources(pid);
    let active = measure_status(&client, &route, &token, version).await;
    let after_active = preparation_bench::resources(pid);
    reliability::record(
        &stack.root.join("STATUS-ACTIVE.json"),
        &json!({"latency":active,"before":before_active,"after":after_active}),
    );
    socket
        .write_all(&bytes[4096..bytes.len() / 2])
        .await
        .unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), socket.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(
        response.starts_with(b"HTTP/1.1 200 "),
        "part request did not ACK"
    );
    let finish = format!(
        "{route}/{version}/{}/finalize",
        upload["session_id"].as_str().unwrap()
    );
    for _ in 0..2 {
        json_response(
            client
                .post(&finish)
                .bearer_auth(&token)
                .json(&json!({}))
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
    }
    objects.shutdown().await;
    drop(stack.metadata.take());
    drop(stack.object.take());
    stack.start_object().await;
    stack.start_metadata().await;
    let read = client
        .get(format!("{base}/objects/get/concurrent.bin"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(read.status(), StatusCode::OK);
    assert_eq!(
        read.headers()["etag"].to_str().unwrap().trim_matches('"'),
        version
    );
    assert_eq!(read.bytes().await.unwrap().as_ref(), bytes);
    reliability::record(
        &stack.root.join("STATUS-PASSED.json"),
        &json!({"passed":true,"idle_samples":100,"active_samples":100,
            "no_uncommitted_ack":true,"restart_full_payload_and_version":true}),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an externally controlled private dm-delay filesystem and healthy ACK storage"]
async fn delayed_member_preserves_acks_and_bounds_competing_admission_without_false_publication() {
    use std::os::unix::fs::MetadataExt;
    let control = PathBuf::from(std::env::var_os("N1_STALL_CONTROL_ROOT").unwrap());
    let member = PathBuf::from(std::env::var_os("N1_OBJECT_TEST_FIRST_ROOT").unwrap());
    assert_ne!(
        std::fs::metadata(&control).unwrap().dev(),
        std::fs::metadata(&member).unwrap().dev(),
        "fault control must not live on the delayed filesystem"
    );
    let mut stack = Stack::with_block_bytes(262144).await;
    drop(stack.metadata.take());
    drop(stack.object.take());
    let mut settings: Value =
        serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    settings["max_operations"] = json!(1);
    settings["runtime"]["io_control"]["foreground_wait_timeout_ms"] = json!(1000);
    settings["runtime"]["io_control"]["stall_threshold_ms"] = json!(1000);
    std::fs::write(
        &stack.object_config,
        serde_json::to_vec_pretty(&settings).unwrap(),
    )
    .unwrap();
    stack.start_object().await;
    stack.start_metadata().await;
    let credentials: Value = serde_json::from_slice(
        &std::fs::read(stack.object_config.with_extension("credentials.json")).unwrap(),
    )
    .unwrap();
    let objects = NativeObjectStore::new(ObjectStoreConfig {
        native_url: credentials["native_url"].as_str().unwrap().into(),
        fragment: credentials["fragment"].as_str().unwrap().into(),
        secret: credentials["secret"].as_str().unwrap().into(),
        max_connections: 4,
        tls: None,
    })
    .unwrap();
    let client = Client::builder()
        .timeout(Duration::from_secs(90))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token) = reliability::fragment(&client, &base, "Delayed member").await;
    let bytes: Vec<_> = (0..4 * 1024 * 1024).map(|i| (i * 31 % 251) as u8).collect();
    let seed = maintenance::put(&client, &base, &token, "seed.bin", &bytes[..65537]).await;
    let route = format!("{base}/upload/multipart/durable");
    let hint = json!({"total_size_bytes":bytes.len(),"part_size_bytes":bytes.len()/2,
        "expected_parts":2});
    let mut uploads = Vec::new();
    for path in ["delayed.bin", "competing.bin"] {
        uploads.push(
            json_response(
                client
                    .post(format!("{route}/init/{path}"))
                    .bearer_auth(&token)
                    .json(&hint)
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await,
        );
    }
    let version = uploads[0]["version_id"].as_str().unwrap();
    resume_progress::put(
        &client,
        &route,
        &token,
        version,
        0,
        bytes[..bytes.len() / 2].to_vec(),
    )
    .await;
    reliability::record(
        &control.join("ready-for-delay.json"),
        &json!({"seed_version":seed,"upload":uploads[0],"uploaded_parts":[0],"fixture":stack.root}),
    );
    wait_for(&control.join("delay-active")).await;
    let started = std::time::Instant::now();
    let pending = client
        .put(format!("{route}/{version}/1"))
        .bearer_auth(&token)
        .body(bytes[bytes.len() / 2..].to_vec())
        .send();
    let writer = tokio::spawn(pending);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut health = Vec::new();
    loop {
        let since = std::time::Instant::now();
        let sample = tokio::time::timeout(
            Duration::from_secs(3),
            objects.control(n1_link::noa::Command::Health),
        )
        .await
        .expect("health stalled behind data I/O")
        .unwrap();
        let stalled = sample["io"]["scheduler"]["pressure"] == "stalled";
        health.push(json!({"seconds":since.elapsed().as_secs_f64(),"snapshot":sample}));
        if stalled {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no delayed-I/O observation"
        );
        assert!(
            !writer.is_finished(),
            "delayed write completed before the fault was observed"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let competing = uploads[1]["version_id"].as_str().unwrap();
    let since = std::time::Instant::now();
    let rejected = tokio::time::timeout(
        Duration::from_secs(5),
        client
            .put(format!("{route}/{competing}/0"))
            .bearer_auth(&token)
            .body(bytes[..bytes.len() / 2].to_vec())
            .send(),
    )
    .await
    .expect("competing admission did not honor its configured deadline")
    .unwrap();
    assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
    let admission_seconds = since.elapsed().as_secs_f64();
    reliability::record(
        &control.join("admission.json"),
        &json!({"health":health,"admission_seconds":admission_seconds}),
    );
    let since = std::time::Instant::now();
    let progress = tokio::time::timeout(
        Duration::from_secs(2),
        resume_progress::status(&client, &route, &token, version),
    )
    .await
    .expect("indexed resume status blocked behind an active disk write");
    let status_seconds = since.elapsed().as_secs_f64();
    assert_eq!(
        progress["uploaded_parts"],
        json!([0]),
        "uncompleted part acknowledged"
    );
    assert_eq!(
        client
            .get(format!("{base}/objects/get/delayed.bin"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    reliability::record(
        &control.join("release-delay.json"),
        &json!({"health":health,"admission_seconds":admission_seconds,
            "status_seconds":status_seconds,"progress":progress}),
    );
    wait_for(&control.join("delay-released")).await;
    json_response(writer.await.unwrap().unwrap(), 200).await;
    let delayed_part_seconds = started.elapsed().as_secs_f64();
    assert!(
        delayed_part_seconds >= 5.0,
        "kernel delay was not exercised"
    );
    let finish = format!(
        "{route}/{version}/{}/finalize",
        uploads[0]["session_id"].as_str().unwrap()
    );
    for _ in 0..2 {
        let done = json_response(
            client
                .post(&finish)
                .bearer_auth(&token)
                .json(&json!({}))
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        assert_eq!(done["version_id"], version);
    }
    objects.shutdown().await;
    drop(stack.metadata.take());
    drop(stack.object.take());
    stack.start_object().await;
    stack.start_metadata().await;
    for (name, expected, expected_version) in [
        ("seed.bin", &bytes[..65537], seed.as_str()),
        ("delayed.bin", bytes.as_slice(), version),
    ] {
        let read = client
            .get(format!("{base}/objects/get/{name}"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(read.status(), StatusCode::OK);
        assert_eq!(
            read.headers()["etag"].to_str().unwrap().trim_matches('"'),
            expected_version
        );
        assert_eq!(read.bytes().await.unwrap().as_ref(), expected);
    }
    assert_eq!(
        resume_progress::status(&client, &route, &token, competing).await["uploaded_parts"],
        json!([])
    );
    for index in 0..2 {
        resume_progress::put(
            &client,
            &route,
            &token,
            competing,
            index,
            bytes[index as usize * bytes.len() / 2..(index as usize + 1) * bytes.len() / 2]
                .to_vec(),
        )
        .await;
    }
    let finish = format!(
        "{route}/{competing}/{}/finalize",
        uploads[1]["session_id"].as_str().unwrap()
    );
    json_response(
        client
            .post(&finish)
            .bearer_auth(&token)
            .json(&json!({}))
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let read = client
        .get(format!("{base}/objects/get/competing.bin"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(read.status(), StatusCode::OK);
    assert_eq!(read.bytes().await.unwrap().as_ref(), bytes);
    drop(stack.metadata.take());
    let records = maintenance::db(&stack).verify().unwrap();
    reliability::record(
        &control.join("PASSED.json"),
        &json!({"passed":true,
        "fault":"private kernel dm-delay write latency; no host power interruption",
        "delayed_part_seconds":delayed_part_seconds,"admission_seconds":admission_seconds,
        "status_seconds":status_seconds,
        "prior_part_acks":1,"published_seed_retained":true,"restart_readback":true,
        "competing_request_status":503,"competing_request_retried":true,"offline_records":records}),
    );
}
