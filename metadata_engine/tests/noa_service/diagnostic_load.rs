//! Busy admission is incomplete verification; actual damaged frames must fail.
use super::*;
use m1::object_store::{NativeObjectStore, ObjectStoreConfig};

async fn scan(client: &Client, base: &str, token: &str) -> Value {
    json_response(
        client
            .get(format!("{base}/maintenance/pools/health?deep_scan=true"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
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
    let upload = objects.init(None, 4096, 4096, true, 3600).await.unwrap();
    let held = objects.clone();
    let plan = upload.clone();
    let (mut source, mut producer) = tokio::io::duplex(64);
    let writer = tokio::spawn(async move { held.part(&plan, 0, 4096, &mut source).await });
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
    producer.write_all(&vec![7; 4095]).await.unwrap();
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
