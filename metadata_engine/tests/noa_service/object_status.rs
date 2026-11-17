//! Browser health rendering and repair with an independent Rust byte oracle.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "isolated two-member pool, explicit Console build, Node and Chromium"]
async fn console_object_status_matches_link_health_and_verifies_repair() {
    let mut stack = Stack::new().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (fragment, _, operator) = maintenance::auth(&client, &base, "Object status witness").await;
    let bytes: Vec<u8> = (0..131_199).map(|i| (i % 251) as u8).collect();
    let version = maintenance::put(&client, &base, &operator, "status.bin", &bytes).await;
    drop(stack.metadata.take());
    drop(stack.object.take());
    let database = maintenance::db(&stack);
    let keys = maintenance::links(&database);
    drop(database);
    assert_eq!(keys.len(), 1);
    let config: Value =
        serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    let layout: n1_link::PoolLayout =
        serde_json::from_value(config["pools"][0]["layout"].clone()).unwrap();
    assert_eq!(layout.width(), 2, "requires the explicit 1+1 fixture");
    assert_eq!(layout.data_shards, 1);
    let roots: Vec<PathBuf> = serde_json::from_value(config["pools"][0]["roots"].clone()).unwrap();
    let fingerprint = n1_link::ObjectKey::inspect(&keys[0]).unwrap().fingerprint();
    let shard = roots[layout.member_slot(fingerprint, 0).unwrap()].join(fingerprint.shard_path(0));
    let original = std::fs::read(&shard).unwrap();
    std::fs::remove_file(&shard).unwrap();
    std::fs::File::open(shard.parent().unwrap())
        .unwrap()
        .sync_all()
        .unwrap();
    stack.start_object().await;
    stack.start_metadata().await;
    let before = json_response(
        client
            .get(format!("{base}/objects/status/status.bin"))
            .bearer_auth(&operator)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    assert_eq!(before["status"], "degraded");
    assert_eq!(before["missing_shards"], json!([0]));
    let access = stack.root.join("object-status-access.json");
    std::fs::write(
        &access,
        serde_json::to_vec_pretty(&json!({
            "metadata": stack.url, "fragment": fragment, "token": operator,
            "console_port": port(), "output": stack.root
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::set_permissions(&access, std::fs::Permissions::from_mode(0o600)).unwrap();
    let log = std::fs::File::create(stack.root.join("object-status-console.log")).unwrap();
    let mut child = Process(
        Command::new(std::env::var_os("N1_BROWSER_NODE").expect("browser Node required"))
            .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/noa_service/object_status.mjs"))
            .arg(&access)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap(),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(
                status.success(),
                "{}",
                std::fs::read_to_string(stack.root.join("object-status-console.log")).unwrap()
            );
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "browser timed out");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(std::fs::read(&shard).unwrap(), original);
    let read = client
        .get(format!(
            "{base}/objects/get/status.bin?version_id={version}"
        ))
        .bearer_auth(&operator)
        .send()
        .await
        .unwrap();
    assert_eq!(read.status(), 200);
    assert_eq!(read.bytes().await.unwrap().as_ref(), bytes);
    reliability::record(
        &stack.root.join("OBJECT-STATUS-PASSED.json"),
        &json!({"passed": true, "removed_shard": 0, "browser_repair": true,
            "exact_repaired_shard_bytes": true, "original_version_readback": true}),
    );
}
