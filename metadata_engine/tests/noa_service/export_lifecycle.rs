//! Retained archive recovery, accounting and download/expiry races through NOA.
use super::*;
use futures_util::TryStreamExt;

async fn configure(stack: &mut Stack, values: &[(&str, &str)]) {
    drop(stack.metadata.take());
    let path = stack.root.join("m1_config.ini");
    let mut ini = ini::Ini::load_from_file(&path).unwrap();
    for (name, value) in values {
        ini.with_section(Some("export")).set(*name, *value);
    }
    ini.write_to_file(path).unwrap();
    stack.start_metadata().await;
}

async fn create(client: &Client, base: &str, token: &str) -> String {
    json_response(
        client
            .post(format!("{base}/exports"))
            .bearer_auth(token)
            .json(&json!({"keys":["source.bin"],"format":"tar"}))
            .send()
            .await
            .unwrap(),
        202,
    )
    .await["export_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn wait(client: &Client, base: &str, token: &str, id: &str, phase: &str) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
    loop {
        let status = json_response(
            client
                .get(format!("{base}/exports/{id}"))
                .bearer_auth(token)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        if status["phase"] == phase {
            return status;
        }
        assert_ne!(status["phase"], "failed", "{status}");
        assert!(tokio::time::Instant::now() < deadline, "{status}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn source(client: &Client, base: &str, token: &str, bytes: Vec<u8>) {
    json_response(
        client
            .post(format!(
                "{base}/upload/one-shot/source.bin?durability=strict"
            ))
            .bearer_auth(token)
            .body(bytes)
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
}

async fn verify_tar(bytes: &[u8], expected: &[u8]) {
    let mut archive = tokio_tar::Archive::new(bytes);
    let mut entries = archive.entries().unwrap();
    let mut entry = entries.try_next().await.unwrap().unwrap();
    assert_eq!(entry.path().unwrap().to_str().unwrap(), "source.bin");
    let mut actual = Vec::new();
    entry.read_to_end(&mut actual).await.unwrap();
    assert_eq!(actual, expected);
    assert!(entries.try_next().await.unwrap().is_none());
}

async fn stop_metadata(stack: &mut Stack) {
    let mut process = stack.metadata.take().unwrap();
    assert_eq!(
        unsafe { libc::kill(process.0.id() as i32, libc::SIGTERM) },
        0
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(40);
    loop {
        if let Some(status) = process.0.try_wait().unwrap() {
            assert!(status.success(), "{status}");
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "metadata did not drain exports"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn audit(stack: &mut Stack, expected_jobs: usize) -> u64 {
    audit_phase(stack, expected_jobs, "ready").await
}

async fn audit_phase(stack: &mut Stack, expected_jobs: usize, phase: &str) -> u64 {
    stop_metadata(stack).await;
    let config = m1::server::ServerConfig::load(&stack.root.join("m1_config.ini")).unwrap();
    let db = m1::MetadataEngine::open(config.data_dir, m1::Options::default()).unwrap();
    let catalog = n1_link::PoolId([0; 16]);
    let view = db.snapshot();
    let rows = view
        .scan(m1::Table::Reclamation, catalog, "export/", None, 1000)
        .unwrap();
    assert_eq!(rows.len(), expected_jobs);
    let sum = rows
        .iter()
        .map(|(_, bytes)| {
            let record: Value = serde_json::from_slice(bytes).unwrap();
            assert_eq!(record["phase"], phase);
            assert_eq!(record["reserved_bytes"], record["artifact_size_bytes"]);
            record["reserved_bytes"].as_u64().unwrap()
        })
        .sum::<u64>();
    let counter = m1::RecordKey::new(catalog, "export-retained-bytes").unwrap();
    let used = view
        .get(m1::Table::Reclamation, &counter)
        .unwrap()
        .map(|bytes| u64::from_be_bytes(bytes.try_into().unwrap()))
        .unwrap_or(0);
    assert_eq!(used, sum);
    assert_eq!(
        view.scan(
            m1::Table::Reclamation,
            catalog,
            "export-expiry/",
            None,
            1000
        )
        .unwrap()
        .len(),
        expected_jobs
    );
    assert!(
        view.scan(m1::Table::Reclamation, catalog, "pin-owner/", None, 1000)
            .unwrap()
            .is_empty()
    );
    used
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real artifact permission failure, independent cleanup and process restart"]
async fn failed_artifact_cleanup_retains_charge_without_blocking_other_jobs_and_retries_after_restart()
 {
    use std::os::unix::fs::PermissionsExt;
    assert_ne!(
        unsafe { libc::geteuid() },
        0,
        "permission fault requires a non-root user"
    );
    let mut stack = Stack::new().await;
    configure(&mut stack, &[("cleanup_interval_seconds", "5")]).await;
    let client = Client::builder()
        .timeout(Duration::from_secs(240))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token, _) = maintenance::auth(&client, &base, "retained-cleanup-failure").await;
    let bytes = vec![19; 32769];
    source(&client, &base, &token, bytes.clone()).await;
    let first = create(&client, &base, &token).await;
    let first_ready = wait(&client, &base, &token, &first, "ready").await;
    let second = create(&client, &base, &token).await;
    wait(&client, &base, &token, &second, "ready").await;
    let config = m1::server::ServerConfig::load(&stack.root.join("m1_config.ini")).unwrap();
    let blocked = config.staging_dir.join("exports/artifacts").join(&first);
    std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o500)).unwrap();
    for id in [&first, &second] {
        json_response(
            client
                .delete(format!("{base}/exports/{id}"))
                .bearer_auth(&token)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let response = client
            .get(format!("{base}/exports/{second}"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        if response.status() == 404 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "one failed job blocked later cleanup"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(blocked.is_dir());
    assert_eq!(
        audit_phase(&mut stack, 1, "cancelled").await,
        first_ready["artifact_size_bytes"].as_u64().unwrap()
    );
    stack.start_metadata().await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        client
            .get(format!("{base}/exports/{first}"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        410
    );
    std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o700)).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let response = client
            .get(format!("{base}/exports/{first}"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        if response.status() == 404 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "cleanup did not retry after restart"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!blocked.exists());
    assert_eq!(audit(&mut stack, 0).await, 0);
    stack.start_metadata().await;
    let source = client
        .get(format!("{base}/objects/get/source.bin"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(source.status(), 200);
    assert_eq!(source.bytes().await.unwrap().as_ref(), bytes);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real N1/M1 processes, interrupted exports and queue downsizing"]
async fn retained_exports_recover_beyond_queue_limit_and_reclaim_accounted_bytes() {
    let mut stack = Stack::new().await;
    configure(&mut stack, &[("cleanup_interval_seconds", "5")]).await;
    let client = Client::builder()
        .timeout(Duration::from_secs(240))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token, _) = maintenance::auth(&client, &base, "retained-restart").await;
    let bytes = (0..32769).map(|n| (n % 251) as u8).collect::<Vec<_>>();
    source(&client, &base, &token, bytes.clone()).await;
    let pid = stack.object.as_ref().unwrap().0.id();
    assert_eq!(unsafe { libc::kill(pid as i32, libc::SIGSTOP) }, 0);
    let mut ids = Vec::new();
    for _ in 0..5 {
        ids.push(create(&client, &base, &token).await);
    }
    drop(stack.metadata.take());
    assert_eq!(unsafe { libc::kill(pid as i32, libc::SIGCONT) }, 0);
    configure(
        &mut stack,
        &[("max_queued_jobs", "1"), ("max_concurrent_jobs", "1")],
    )
    .await;
    let mut total = 0;
    for id in &ids {
        let status = wait(&client, &base, &token, id, "ready").await;
        total += status["artifact_size_bytes"].as_u64().unwrap();
        let response = client
            .get(format!("{base}/exports/{id}/download"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        verify_tar(&response.bytes().await.unwrap(), &bytes).await;
    }
    assert_eq!(audit(&mut stack, 5).await, total);
    stack.start_metadata().await;
    for id in &ids[..2] {
        json_response(
            client
                .delete(format!("{base}/exports/{id}"))
                .bearer_auth(&token)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    for id in &ids[..2] {
        loop {
            let response = client
                .get(format!("{base}/exports/{id}"))
                .bearer_auth(&token)
                .send()
                .await
                .unwrap();
            if response.status() == 404 {
                break;
            }
            assert!(matches!(response.status().as_u16(), 200 | 410));
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    assert_eq!(audit(&mut stack, 3).await, total * 3 / 5);
    stack.start_metadata().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "actual retention deadline with a client holding an archive body"]
async fn expired_retained_archive_waits_for_active_download_then_leaves_no_residue() {
    let mut stack = Stack::new().await;
    configure(
        &mut stack,
        &[
            ("retention_seconds", "60"),
            ("cleanup_interval_seconds", "5"),
        ],
    )
    .await;
    let client = Client::builder()
        .timeout(Duration::from_secs(240))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token, _) = maintenance::auth(&client, &base, "retained-expiry").await;
    let bytes = (0..16 * 1024 * 1024)
        .map(|n| (n % 251) as u8)
        .collect::<Vec<_>>();
    source(&client, &base, &token, bytes.clone()).await;
    let id = create(&client, &base, &token).await;
    let ready = wait(&client, &base, &token, &id, "ready").await;
    let response = client
        .get(format!("{base}/exports/{id}/download"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let deadline = ready["expires_at_unix"].as_u64().unwrap() + 7;
    while std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        < deadline
    {
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let config = m1::server::ServerConfig::load(&stack.root.join("m1_config.ini")).unwrap();
    let directory = config.staging_dir.join("exports/artifacts").join(&id);
    assert!(
        directory.is_dir(),
        "expiry removed an actively leased archive"
    );
    assert_eq!(
        client
            .get(format!("{base}/exports/{id}/download"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        410
    );
    verify_tar(&response.bytes().await.unwrap(), &bytes).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while directory.exists()
        || client
            .get(format!("{base}/exports/{id}"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status()
            != 404
    {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(audit(&mut stack, 0).await, 0);
    assert_eq!(
        std::fs::read_dir(config.staging_dir.join("exports/manifests"))
            .unwrap()
            .count(),
        0
    );
    stack.start_metadata().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real export reservation refusal and failed-job artifact cleanup"]
async fn retention_quota_refusal_preserves_source_and_does_not_leak_reserved_bytes() {
    let mut stack = Stack::new().await;
    configure(&mut stack, &[("max_retained_bytes", "2097152")]).await;
    let client = Client::builder()
        .timeout(Duration::from_secs(180))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token, _) = maintenance::auth(&client, &base, "retained-quota").await;
    let bytes = vec![41; 3 * 1024 * 1024];
    source(&client, &base, &token, bytes.clone()).await;
    let id = create(&client, &base, &token).await;
    let status = wait(&client, &base, &token, &id, "failed").await;
    assert!(status["error"].as_str().is_some());
    let response = client
        .get(format!("{base}/objects/get/source.bin"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
    stop_metadata(&mut stack).await;
    let config = m1::server::ServerConfig::load(&stack.root.join("m1_config.ini")).unwrap();
    let db = m1::MetadataEngine::open(&config.data_dir, m1::Options::default()).unwrap();
    let catalog = n1_link::PoolId([0; 16]);
    let key = m1::RecordKey::new(catalog, format!("export/{id}")).unwrap();
    let record: Value = serde_json::from_slice(
        &db.snapshot()
            .get(m1::Table::Reclamation, &key)
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(record["reserved_bytes"], 0);
    assert!(
        !config
            .staging_dir
            .join("exports/artifacts")
            .join(&id)
            .exists()
    );
    assert!(
        db.snapshot()
            .scan(m1::Table::Reclamation, catalog, "pin-owner/", None, 1000)
            .unwrap()
            .is_empty()
    );
    drop(db);
    stack.start_metadata().await;
}
