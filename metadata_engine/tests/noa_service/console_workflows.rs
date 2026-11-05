//! Real Console authorization, database recovery and existing upload workflows.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated services, explicit Console build, Node and Chromium"]
async fn console_operator_backup_restores_versions_and_runs_original_upload_workflows() {
    let mut stack = Stack::new().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (fragment, token, _) =
        maintenance::auth(&client, &base, "Console whole-product witness").await;
    let first: Vec<u8> = (0..131199).map(|i| (i % 251) as u8).collect();
    let second: Vec<u8> = (0..262267).map(|i| (i % 239) as u8).collect();
    let one = maintenance::put(&client, &base, &token, "recovery/versioned.bin", &first).await;
    let two = maintenance::put(&client, &base, &token, "recovery/versioned.bin", &second).await;
    maintenance::put(&client, &base, &token, "recovery/trashed.bin", &first).await;
    json_response(
        client
            .delete(format!("{base}/objects/recovery/trashed.bin"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap(),
        200,
    )
    .await;
    let access = stack.root.join("console-workflows-access.json");
    let settings = json!({"metadata":stack.url,"fragment":fragment,"token":token,"console_port":port(),"output":stack.root});
    std::fs::write(&access, serde_json::to_vec_pretty(&settings).unwrap()).unwrap();
    std::fs::set_permissions(&access, std::fs::Permissions::from_mode(0o600)).unwrap();
    let log = std::fs::File::create(stack.root.join("console-workflows.log")).unwrap();
    let mut child = Process(
        Command::new(std::env::var_os("N1_BROWSER_NODE").expect("browser Node required"))
            .arg(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/noa_service/console_workflows.mjs"),
            )
            .arg(&access)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap(),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(600);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(
                status.success(),
                "{}",
                std::fs::read_to_string(stack.root.join("console-workflows.log")).unwrap()
            );
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Console workflows timed out"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let archive = stack.root.join("console-downloaded.n1meta");
    let restored = stack.root.join("console-restored-metadata");
    let records = n1_metadata_engine::MetadataEngine::restore_backup(&archive, &restored).unwrap();
    assert!(records > 0);
    drop(stack.metadata.take());
    let mut ini = ini::Ini::load_from_file(stack.root.join("n1_config.ini")).unwrap();
    ini.with_section(Some("metadata_engine"))
        .set("data_dir", restored.display().to_string());
    ini.write_to_file(stack.root.join("n1_config.ini")).unwrap();
    stack.start_metadata().await;
    for (version, expected) in [(&one, &first), (&two, &second)] {
        let read = client
            .get(format!(
                "{base}/objects/get/recovery/versioned.bin?version_id={version}"
            ))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(read.status(), 200);
        assert_eq!(read.bytes().await.unwrap().as_ref(), expected);
    }
    let hidden = client
        .get(format!("{base}/objects/get/recovery/trashed.bin"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(
        hidden.status(),
        404,
        "trash state must survive Console snapshot recovery"
    );
    reliability::record(
        &stack.root.join("CONSOLE-WHOLE-PRODUCT-PASSED.json"),
        &json!({"passed":true,"downloaded_console_snapshot_restored":true,"both_immutable_versions_exact":true,"trash_state_restored":true,"restored_records":records,"original_live_console_cases":4}),
    );
}
