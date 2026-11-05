//! Regression witnesses for key-only creation and zero-residue purge.
use super::*;
use std::time::Duration;

#[tokio::test]
async fn failed_begin_reclaims_empty_allocation_directories_and_successful_peers() {
    const CHILD: &str = "N1_TEST_FAILED_BEGIN_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "core::tests::purge::failed_begin_reclaims_empty_allocation_directories_and_successful_peers",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let fixture = Fixture::new();
    let store = fixture.enroll();
    // This child runs only this test; no other test shares its environment.
    unsafe {
        std::env::set_var("N1_TEST_FAIL_STAGE_ALLOCATION", &fixture.paths[3]);
    }
    let failed = store
        .init(SCOPE, 0, 1, 12288, Durability::Durable, 3600)
        .await;
    unsafe {
        std::env::remove_var("N1_TEST_FAIL_STAGE_ALLOCATION");
    }
    assert!(matches!(failed, Err(Error::Io(error)) if error.raw_os_error() == Some(libc::ENOSPC)));
    for root in &fixture.paths {
        let uploads = root.join(".uploads");
        assert!(uploads.is_dir());
        assert_eq!(
            std::fs::read_dir(uploads).unwrap().count(),
            0,
            "failed Begin must leave neither shard files nor empty identity directories"
        );
    }
    drop(store);
    let store = fixture.reopen();
    let init = store
        .init(SCOPE, 0, 1, 12288, Durability::Durable, 3600)
        .await
        .unwrap();
    store
        .put(
            SCOPE,
            init.object_key.clone(),
            std::io::Cursor::new(vec![7]),
        )
        .await
        .unwrap();
    let (bytes, _) = store
        .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
        .await
        .unwrap();
    assert_eq!(bytes, [7]);
    store.delete(SCOPE, init.object_key.clone()).await.unwrap();
    absent(&fixture, &init.object_key);
}

#[tokio::test]
async fn begin_allocates_headers_only_and_payload_capacity_is_checked_before_writing() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    // Deliberately larger than this fixture's filesystem, with valid geometry.
    let init = store
        .init(
            SCOPE,
            0,
            1_u64 << 50,
            12288 * 1024 * 1024,
            Durability::Durable,
            3600,
        )
        .await
        .unwrap();
    let fp = n1_link::ObjectKey::inspect(&init.object_key)
        .unwrap()
        .fingerprint();
    for root in &fixture.paths {
        assert_eq!(root.join(fp.staging_path()).metadata().unwrap().len(), 128);
    }
    let result = store
        .write_at(
            SCOPE,
            init.object_key.clone(),
            0,
            std::io::Cursor::new(Vec::<u8>::new()),
        )
        .await;
    assert!(matches!(result,Err(Error::Io(error)) if error.raw_os_error()==Some(libc::ENOSPC)));
    for root in &fixture.paths {
        assert_eq!(root.join(fp.staging_path()).metadata().unwrap().len(), 128);
    }
    store.delete(SCOPE, init.object_key.clone()).await.unwrap();
    absent(&fixture, &init.object_key);
}

fn absent(fixture: &Fixture, key: &str) {
    let fp = n1_link::ObjectKey::inspect(key).unwrap().fingerprint();
    for root in &fixture.paths {
        assert!(!root.join(format!(".uploads/{fp}")).exists());
        for shard in 0..fixture.layout.width() {
            assert!(!root.join(fp.shard_path(shard as u16)).exists());
        }
    }
}

#[tokio::test]
async fn purge_waits_for_online_backup_to_finish_pinning_final_paths() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let init = store
        .init(SCOPE, 0, 1, 12288, Durability::Durable, 3600)
        .await
        .unwrap();
    store
        .put(
            SCOPE,
            init.object_key.clone(),
            std::io::Cursor::new(vec![7]),
        )
        .await
        .unwrap();
    let gate = store.snapshot_gate.clone().write_owned().await;
    let worker = store.clone();
    let key = init.object_key.clone();
    let mut purge = tokio::spawn(async move { worker.delete(SCOPE, key).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut purge)
            .await
            .is_err()
    );
    let fp = n1_link::ObjectKey::inspect(&init.object_key)
        .unwrap()
        .fingerprint();
    for shard in 0..4 {
        assert!(
            fixture.paths[fixture.layout.member_slot(fp, shard).unwrap()]
                .join(fp.shard_path(shard))
                .exists()
        );
    }
    drop(gate);
    purge.await.unwrap().unwrap();
    absent(&fixture, &init.object_key);
}

#[tokio::test]
async fn a_failed_member_never_acknowledges_purge_and_retry_finishes_survivors() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let init = store
        .init(SCOPE, 0, 1, 12288, Durability::Durable, 3600)
        .await
        .unwrap();
    store
        .put(
            SCOPE,
            init.object_key.clone(),
            std::io::Cursor::new(vec![7]),
        )
        .await
        .unwrap();
    let fp = n1_link::ObjectKey::inspect(&init.object_key)
        .unwrap()
        .fingerprint();
    let path = fixture.paths[fixture.layout.member_slot(fp, 3).unwrap()].join(fp.shard_path(3));
    let bytes = std::fs::read(&path).unwrap();
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    assert!(store.delete(SCOPE, init.object_key.clone()).await.is_err());
    for shard in 0..3 {
        let path = fixture.paths[fixture.layout.member_slot(fp, shard).unwrap()]
            .join(fp.shard_path(shard));
        assert!(
            !path.exists(),
            "successful member workers must finish even when a peer fails"
        );
    }
    std::fs::remove_dir(&path).unwrap();
    std::fs::write(&path, bytes).unwrap();
    drop(store);
    let store = fixture.reopen();
    assert_eq!(
        store.delete(SCOPE, init.object_key.clone()).await.unwrap(),
        1
    );
    absent(&fixture, &init.object_key);
}

#[tokio::test]
async fn an_unavailable_member_refuses_purge_before_any_member_is_changed() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let init = store
        .init(SCOPE, 0, 1, 12288, Durability::Durable, 3600)
        .await
        .unwrap();
    store
        .put(
            SCOPE,
            init.object_key.clone(),
            std::io::Cursor::new(vec![7]),
        )
        .await
        .unwrap();
    let root = &fixture.paths[0];
    let offline = root.with_extension("offline");
    std::fs::rename(root, &offline).unwrap();
    assert!(store.delete(SCOPE, init.object_key.clone()).await.is_err());
    std::fs::rename(&offline, root).unwrap();
    let fp = n1_link::ObjectKey::inspect(&init.object_key)
        .unwrap()
        .fingerprint();
    for shard in 0..4 {
        assert!(
            fixture.paths[fixture.layout.member_slot(fp, shard).unwrap()]
                .join(fp.shard_path(shard))
                .exists()
        );
    }
    store.delete(SCOPE, init.object_key.clone()).await.unwrap();
    absent(&fixture, &init.object_key);
}

#[tokio::test]
async fn begin_creates_incomplete_headers_and_one_link_identity_survives_resume() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let init = store
        .init(SCOPE, 0, 24577, 12288, Durability::Durable, 3600)
        .await
        .unwrap();
    assert!(init.object_key.starts_with("N1A1."));
    assert!(
        serde_json::to_value(&init)
            .unwrap()
            .get("upload_key")
            .is_none()
    );
    let fp = n1_link::ObjectKey::inspect(&init.object_key)
        .unwrap()
        .fingerprint();
    for (slot, root) in fixture.paths.iter().enumerate() {
        let bytes = std::fs::read(root.join(fp.staging_path())).unwrap();
        assert_eq!(bytes.len(), 128);
        let shard = fixture.layout.shard_at_slot(fp, slot).unwrap();
        let header =
            n1_link::shard::ShardHeader::decode(&bytes, &fixture.layout, fp, shard).unwrap();
        assert_eq!(header.total_bytes, 24577);
        assert_eq!(header.part_bytes, 12288);
        assert!(!root.join(fp.shard_path(shard)).exists());
        assert_eq!(
            std::fs::read_dir(root.join(format!(".uploads/{fp}")))
                .unwrap()
                .count(),
            1
        );
    }
    drop(store);
    let store = fixture.reopen();
    let bytes = source(24577);
    for (part, data) in bytes.chunks(12288).enumerate() {
        store
            .write_at(
                SCOPE,
                init.object_key.clone(),
                part as u64 * 12288,
                std::io::Cursor::new(data.to_vec()),
            )
            .await
            .unwrap();
    }
    for repeated in [false, true] {
        let result = store
            .finalize(SCOPE, init.object_key.clone())
            .await
            .unwrap();
        assert_eq!(result.object_key, init.object_key);
        assert_eq!(result.already_finalized, repeated);
    }
    assert_eq!(
        store
            .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
            .await
            .unwrap()
            .0,
        bytes
    );
    store.delete(SCOPE, init.object_key.clone()).await.unwrap();
    absent(&fixture, &init.object_key);
}

#[tokio::test]
async fn purge_empty_partial_and_live_objects_rejects_every_continuation_after_restart() {
    for (length, finalize) in [(0, false), (0, true), (24577, false), (24577, true)] {
        let fixture = Fixture::new();
        let store = fixture.enroll();
        let init = store
            .init(SCOPE, 0, length, 12288, Durability::Durable, 3600)
            .await
            .unwrap();
        if length > 0 {
            for part in 0..if finalize { 3 } else { 1 } {
                let size = (length - part * 12288).min(12288);
                store
                    .write_at(
                        SCOPE,
                        init.object_key.clone(),
                        part * 12288,
                        std::io::Cursor::new(vec![7; size as usize]),
                    )
                    .await
                    .unwrap();
            }
        }
        if finalize {
            store
                .finalize(SCOPE, init.object_key.clone())
                .await
                .unwrap();
        }
        assert_eq!(
            store.delete(SCOPE, init.object_key.clone()).await.unwrap(),
            4
        );
        absent(&fixture, &init.object_key);
        drop(store);
        let store = fixture.reopen();
        assert_eq!(
            store.delete(SCOPE, init.object_key.clone()).await.unwrap(),
            0
        );
        assert!(matches!(
            store.finalize(SCOPE, init.object_key.clone()).await,
            Err(Error::NotFound)
        ));
        assert!(matches!(
            store
                .write_at(
                    SCOPE,
                    init.object_key.clone(),
                    0,
                    std::io::Cursor::new(vec![7; 12288])
                )
                .await,
            Err(Error::NotFound)
        ));
        assert!(matches!(
            store
                .upload_status(SCOPE, init.object_key.clone(), 0, 1)
                .await,
            Err(Error::NotFound)
        ));
        assert!(matches!(
            store.stat(SCOPE, init.object_key.clone()).await,
            Err(Error::NotFound)
        ));
        assert!(matches!(
            store
                .object_health(SCOPE, init.object_key.clone(), true)
                .await,
            Err(Error::NotFound)
        ));
        assert!(matches!(
            store.repair(SCOPE, init.object_key.clone()).await,
            Err(Error::NotFound)
        ));
        assert!(matches!(
            store
                .put(
                    SCOPE,
                    init.object_key.clone(),
                    std::io::Cursor::new(Vec::<u8>::new())
                )
                .await,
            Err(Error::NotFound)
        ));
        assert!(matches!(
            store
                .put_batch(
                    SCOPE,
                    vec![init.object_key.clone()],
                    std::io::Cursor::new(Vec::<u8>::new())
                )
                .await,
            Err(Error::NotFound)
        ));
        absent(&fixture, &init.object_key);
    }
}

#[tokio::test]
async fn a_valid_link_address_without_begin_cannot_create_incomplete_data() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let identity = n1_link::Identity {
        pool: fixture.layout.id,
        generation: fixture.layout.generation,
        token: [99; 32],
    };
    let key = store
        .signer
        .object_key(&SCOPE, identity)
        .as_str()
        .to_owned();
    assert!(matches!(
        store
            .write_at(SCOPE, key.clone(), 0, std::io::Cursor::new(vec![7; 1]))
            .await,
        Err(Error::NotFound)
    ));
    assert!(matches!(
        store.finalize(SCOPE, key.clone()).await,
        Err(Error::NotFound)
    ));
    absent(&fixture, &key);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn purge_drains_an_inflight_write_before_removing_its_shards() {
    use tokio::io::AsyncWriteExt;
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let init = store
        .init(SCOPE, 0, 12288, 12288, Durability::Durable, 3600)
        .await
        .unwrap();
    let (reader, mut writer) = tokio::io::duplex(64);
    let work = store.clone();
    let key = init.object_key.clone();
    let write = tokio::spawn(async move { work.write_at(SCOPE, key, 0, reader).await });
    // More than the duplex capacity proves the actual shard writer consumed
    // bytes while holding its lifecycle guard, rather than just probing headers.
    writer.write_all(&vec![7; 256]).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let fp = n1_link::ObjectKey::inspect(&init.object_key)
        .unwrap()
        .fingerprint();
    loop {
        let lease = store.lease(fp).unwrap();
        if lease.state().lifecycle.try_write().is_err() {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    let work = store.clone();
    let key = init.object_key.clone();
    let mut purge = tokio::spawn(async move { work.delete(SCOPE, key).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut purge)
            .await
            .is_err()
    );
    writer.write_all(&vec![7; 12032]).await.unwrap();
    writer.shutdown().await.unwrap();
    write.await.unwrap().unwrap();
    purge.await.unwrap().unwrap();
    absent(&fixture, &init.object_key);
    assert!(matches!(
        store.finalize(SCOPE, init.object_key).await,
        Err(Error::NotFound)
    ));
}

#[tokio::test]
async fn repeated_churn_keeps_zero_files_for_all_deleted_identities() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    for cycle in 0..128 {
        let init = store
            .init(SCOPE, 0, 1, 12288, Durability::Durable, 3600)
            .await
            .unwrap();
        store
            .put(
                SCOPE,
                init.object_key.clone(),
                std::io::Cursor::new(vec![cycle as u8]),
            )
            .await
            .unwrap();
        store.delete(SCOPE, init.object_key.clone()).await.unwrap();
        absent(&fixture, &init.object_key);
        for root in &fixture.paths {
            let files = walkdir::WalkDir::new(root)
                .into_iter()
                .map(|r| r.unwrap())
                .filter(|e| e.file_type().is_file())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            assert_eq!(files, vec![".n1-arc-pool.json"], "cycle {cycle}");
            assert_eq!(std::fs::read_dir(root.join(".uploads")).unwrap().count(), 0);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigkill_after_three_of_four_removals_then_restart_and_retry_leaves_nothing() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let init = store
        .init(SCOPE, 0, 1, 12288, Durability::Durable, 3600)
        .await
        .unwrap();
    store
        .put(
            SCOPE,
            init.object_key.clone(),
            std::io::Cursor::new(vec![7]),
        )
        .await
        .unwrap();
    let fp = n1_link::ObjectKey::inspect(&init.object_key)
        .unwrap()
        .fingerprint();
    let config = fixture.base.join("purge-child.json");
    std::fs::write(&config,serde_json::to_vec(&serde_json::json!({"layout":fixture.layout,"roots":fixture.paths,"key":init.object_key})).unwrap()).unwrap();
    drop(store);
    let log = std::fs::File::create(fixture.base.join("purge-child.log")).unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "core::tests::purge::purge_child",
            "--ignored",
            "--nocapture",
        ])
        .env("N1_PURGE_CHILD_CONFIG", &config)
        .env("N1_TEST_PAUSE_PURGE_LAST", fp.to_string())
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let remaining = (0..fixture.layout.width())
            .filter(|shard| {
                fixture.paths[fixture.layout.member_slot(fp, *shard as u16).unwrap()]
                    .join(fp.shard_path(*shard as u16))
                    .exists()
            })
            .count();
        if remaining == 1 {
            break;
        }
        if child.try_wait().unwrap().is_some() || tokio::time::Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "purge child failed: {}",
                std::fs::read_to_string(fixture.base.join("purge-child.log")).unwrap()
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    child.kill().unwrap();
    let status = child.wait().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(9));
    }
    let store = fixture.reopen();
    assert!(
        store
            .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
            .await
            .is_err()
    );
    assert!(
        store
            .finalize(SCOPE, init.object_key.clone())
            .await
            .is_err()
    );
    assert_eq!(
        store.delete(SCOPE, init.object_key.clone()).await.unwrap(),
        1
    );
    absent(&fixture, &init.object_key);
    drop(store);
    let store = fixture.reopen();
    assert!(matches!(
        store
            .write_at(
                SCOPE,
                init.object_key.clone(),
                0,
                std::io::Cursor::new(vec![7])
            )
            .await,
        Err(Error::NotFound)
    ));
    absent(&fixture, &init.object_key);
}

#[tokio::test]
#[ignore = "subprocess fixture explicitly exercised by the SIGKILL parent"]
async fn purge_child() {
    let file = std::env::var_os("N1_PURGE_CHILD_CONFIG").expect("parent-owned fixture required");
    let value: serde_json::Value = serde_json::from_slice(&std::fs::read(file).unwrap()).unwrap();
    let layout = serde_json::from_value(value["layout"].clone()).unwrap();
    let roots: Vec<PathBuf> = serde_json::from_value(value["roots"].clone()).unwrap();
    let pool = storage_pool::Pool::reopen_fixture(layout, &roots).unwrap();
    let store = Store::new(vec![pool], [17; 32], 16).unwrap();
    store
        .delete(SCOPE, value["key"].as_str().unwrap().into())
        .await
        .unwrap();
    panic!("fault fixture did not pause");
}
