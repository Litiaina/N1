//! Portable recovery keeps pool/member identities and signing credentials;
//! mount paths, filesystem inodes and hardware serial numbers may change.
use super::*;
fn offline(stack: &Stack, args: &[&str]) -> Value {
    let result = Command::new(&stack.object_binary)
        .args(args)
        .current_dir(&stack.root)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    serde_json::from_slice(&result.stdout).unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real two-HDD portable backup/replication/blank-member replacement"]
async fn object_pool_backup_replication_restore_and_blank_replacement_preserve_keys_and_retirements()
 {
    let mut stack = Stack::new().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let fragment=json_response(client.post(format!("{base}/fragments")).json(&json!({"name":"Portable recovery witness","secret":"test-only-master","key":"test-only-fragment"})).send().await.unwrap(),201).await;
    let logged=json_response(client.post(format!("{base}/auth/fragment")).json(&json!({"fragment":fragment["fragment_hash"],"secret":"test-only-fragment","read":true,"write":true,"modify":true,"delete":true})).send().await.unwrap(),200).await;
    let token = logged["access_token"].as_str().unwrap();
    let bytes = (0..8 * 1024 * 1024 + 19)
        .map(|n| (n % 251) as u8)
        .collect::<Vec<_>>();
    json_response(
        client
            .post(format!("{base}/upload/one-shot/recovery.bin"))
            .bearer_auth(token)
            .body(bytes.clone())
            .send()
            .await
            .unwrap(),
        201,
    )
    .await;
    let operator=json_response(client.post(format!("{base}/auth/fragment")).json(&json!({"fragment":fragment["fragment_hash"],"secret":"test-only-master","read":true,"write":true,"modify":true,"delete":true})).send().await.unwrap(),200).await;
    let operator = operator["access_token"].as_str().unwrap();
    let backup_route = format!("{base}/maintenance/metadata/backups");
    let metadata_point = json_response(
        client
            .post(&backup_route)
            .bearer_auth(operator)
            .send()
            .await
            .unwrap(),
        202,
    )
    .await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let backup = json_response(
            client
                .get(&backup_route)
                .bearer_auth(operator)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        if backup["job"]["phase"] == "completed" {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "{backup}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let metadata_snapshot = stack
        .root
        .join("metadata-backups")
        .join(format!("{}.n1meta", metadata_point["id"].as_str().unwrap()));
    let credentials: Value = serde_json::from_slice(
        &std::fs::read(stack.object_config.with_extension("credentials.json")).unwrap(),
    )
    .unwrap();
    let mut native =
        n1_native_client::N1NativeClient::connect(credentials["native_url"].as_str().unwrap())
            .await
            .unwrap();
    native
        .auth_fragment(
            credentials["fragment"].as_str().unwrap(),
            credentials["secret"].as_str().unwrap(),
        )
        .await
        .unwrap();
    let retired = cli_core::result(
        native
            .control(n1_link::noa::Command::UploadInit {
                pool: None,
                total_bytes: 4096,
                part_bytes: 4096,
                durable: true,
                ttl_seconds: 3600,
            })
            .await
            .unwrap(),
    )
    .unwrap();
    native
        .put(
            retired["object_key"].as_str().unwrap().into(),
            4096,
            &mut std::io::Cursor::new(vec![29; 4096]),
        )
        .await
        .unwrap();
    native
        .control(n1_link::noa::Command::ObjectDelete {
            object_key: retired["object_key"].as_str().unwrap().into(),
        })
        .await
        .unwrap();
    drop(native);
    drop(stack.metadata.take());
    drop(stack.object.take());
    let restored_metadata = stack.root.join("restored-namespace-database");
    assert!(
        n1_metadata_engine::MetadataEngine::restore_backup(&metadata_snapshot, &restored_metadata)
            .unwrap()
            > 0
    );
    let ini_path = stack.root.join("n1_config.ini");
    let mut ini = ini::Ini::load_from_file(&ini_path).unwrap();
    ini.with_section(Some("metadata_engine"))
        .set("data_dir", restored_metadata.display().to_string());
    ini.write_to_file(&ini_path).unwrap();
    let settings: Value =
        serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    let roots = settings["pools"][0]["roots"].as_array().unwrap();
    assert_eq!(roots.len(), 2, "two physical members required");
    let parent0 = PathBuf::from(roots[0].as_str().unwrap())
        .parent()
        .unwrap()
        .to_owned();
    let parent1 = PathBuf::from(roots[1].as_str().unwrap())
        .parent()
        .unwrap()
        .to_owned();
    let label = ulid::Ulid::new().to_string();
    let key0 = stack.root.join("backup-source.key");
    let key1 = stack.root.join("backup-destination.key");
    let repo0 = parent0.join(format!("backup-{label}"));
    let repo1 = parent1.join(format!("replica-{label}"));
    for key in [&key0, &key1] {
        offline(
            &stack,
            &["backup", "keygen", "--output", key.to_str().unwrap()],
        );
    }
    for (repo, key) in [(&repo0, &key0), (&repo1, &key1)] {
        offline(
            &stack,
            &[
                "backup",
                "init",
                "--repository",
                repo.to_str().unwrap(),
                "--key-file",
                key.to_str().unwrap(),
            ],
        );
    }
    let point = offline(
        &stack,
        &[
            "backup",
            "create",
            "--repository",
            repo0.to_str().unwrap(),
            "--key-file",
            key0.to_str().unwrap(),
            "--config",
            stack.object_config.to_str().unwrap(),
        ],
    );
    let snapshot = point["id"].as_str().unwrap();
    offline(
        &stack,
        &[
            "backup",
            "verify",
            "--repository",
            repo0.to_str().unwrap(),
            "--key-file",
            key0.to_str().unwrap(),
            "--snapshot",
            snapshot,
        ],
    );
    offline(
        &stack,
        &[
            "backup",
            "replicate",
            "--repository",
            repo0.to_str().unwrap(),
            "--key-file",
            key0.to_str().unwrap(),
            "--snapshot",
            snapshot,
            "--destination",
            repo1.to_str().unwrap(),
            "--destination-key",
            key1.to_str().unwrap(),
        ],
    );
    offline(
        &stack,
        &[
            "backup",
            "verify",
            "--repository",
            repo1.to_str().unwrap(),
            "--key-file",
            key1.to_str().unwrap(),
            "--snapshot",
            snapshot,
        ],
    );
    // Total source-pool loss: make both original members unavailable before
    // restore. Only the verified replicated backup may supply recovered bytes.
    for root in roots {
        let root = PathBuf::from(root.as_str().unwrap());
        let lost = root.with_extension("lost-original-pool-evidence");
        assert!(!lost.exists());
        std::fs::rename(&root, &lost).unwrap();
        assert!(!root.exists());
    }
    let restored0 = parent0.join(format!("restored-{label}"));
    let restored1 = parent1.join(format!("restored-{label}"));
    for path in [&restored0, &restored1] {
        std::fs::create_dir(path).unwrap();
    }
    let config = stack.root.join("restored-object.json");
    offline(
        &stack,
        &[
            "backup",
            "restore",
            "--repository",
            repo1.to_str().unwrap(),
            "--key-file",
            key1.to_str().unwrap(),
            "--snapshot",
            snapshot,
            "--target",
            restored0.to_str().unwrap(),
            "--target",
            restored1.to_str().unwrap(),
            "--output-config",
            config.to_str().unwrap(),
        ],
    );
    stack.object_config = config;
    // start_object reads the original deployment's client descriptor. The
    // restored service uses those same credentials and native address.
    std::fs::copy(
        stack.root.join("object.credentials.json"),
        stack.object_config.with_extension("credentials.json"),
    )
    .unwrap();
    stack.start_object().await;
    stack.start_metadata().await;
    let response = client
        .get(format!("{base}/objects/get/recovery.bin"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
    let mut native =
        n1_native_client::N1NativeClient::connect(credentials["native_url"].as_str().unwrap())
            .await
            .unwrap();
    native
        .auth_fragment(
            credentials["fragment"].as_str().unwrap(),
            credentials["secret"].as_str().unwrap(),
        )
        .await
        .unwrap();
    assert!(
        native
            .control(n1_link::noa::Command::ObjectStat {
                object_key: retired["object_key"].as_str().unwrap().into()
            })
            .await
            .is_err()
    );
    drop(native);
    drop(stack.metadata.take());
    drop(stack.object.take());
    let mut config: Value =
        serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    let replacement = parent0.join(format!("replacement-{label}"));
    std::fs::create_dir(&replacement).unwrap();
    let old_marker = std::fs::read(restored0.join(".n1-arc-pool.json")).unwrap();
    config["pools"][0]["roots"][0] = json!(replacement);
    std::fs::write(
        &stack.object_config,
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();
    let pool = config["pools"][0]["layout"]["id"].as_str().unwrap();
    offline(
        &stack,
        &[
            "pool",
            "replace",
            "--config",
            stack.object_config.to_str().unwrap(),
            "--pool",
            pool,
            "--member",
            "0",
            "--authorize",
        ],
    );
    assert_eq!(
        std::fs::read(replacement.join(".n1-arc-pool.json")).unwrap(),
        old_marker
    );
    use std::os::unix::fs::MetadataExt;
    assert_ne!(
        std::fs::metadata(&replacement).unwrap().ino(),
        std::fs::metadata(&restored0).unwrap().ino()
    );
    offline(
        &stack,
        &[
            "pool",
            "verify",
            "--config",
            stack.object_config.to_str().unwrap(),
            "--pool",
            pool,
        ],
    );
    stack.start_object().await;
    stack.start_metadata().await;
    let response = client
        .get(format!("{base}/objects/get/recovery.bin"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
    let mut native =
        n1_native_client::N1NativeClient::connect(credentials["native_url"].as_str().unwrap())
            .await
            .unwrap();
    native
        .auth_fragment(
            credentials["fragment"].as_str().unwrap(),
            credentials["secret"].as_str().unwrap(),
        )
        .await
        .unwrap();
    assert!(
        native
            .control(n1_link::noa::Command::ObjectStat {
                object_key: retired["object_key"].as_str().unwrap().into()
            })
            .await
            .is_err()
    );
    let report = json!({"pool":pool,"backup_replicated_across_hdds":true,"portable_restore":true,"metadata_snapshot_restored":true,"metadata_and_objects_restored_together":true,"blank_member_replacement":true,"source_and_replacement_have_different_inodes":true,"bytes_verified":bytes.len(),"retirement_preserved":true});
    std::fs::write(
        stack.root.join("RECOVERY-PASSED.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
}
