//! RAM functional recovery tests; physical restore/replication/power-loss
//! qualification remains separate. The enrollment bypass exists only in tests.
use super::*;
use crate::core::{Error as StoreError, Store, storage_pool::Pool, tests::Fixture};
use n1_link::Durability;
use std::{path::PathBuf, sync::Arc};
const SCOPE: [u8; 16] = [11; 16];

#[tokio::test]
async fn encrypted_service_identity_survives_replication_gc_and_portable_restore() {
    use crate::settings::config::{Config, ConfiguredPool, Fragment};
    use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
    use n1_link::{PoolId, noa::Permissions};
    use std::os::unix::fs::PermissionsExt;
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let config = Config {
        format: crate::settings::config::CONFIG_FORMAT.into(),
        native_address: "127.0.0.1:19001".parse().unwrap(),
        http_address: None,
        signing_key_file: fixture.base.join("source-signing-key"),
        state_dir: None,
        max_connections: 16,
        max_operations: 16,
        operation_memory_bytes: 256 * 1024 * 1024,
        read_repair: true,
        concurrency: Default::default(),
        rate_limits: Default::default(),
        repositories: Default::default(),
        maintenance: Default::default(),
        runtime: Default::default(),
        tls: None,
        pools: vec![ConfiguredPool {
            layout: fixture.layout.clone(),
            members: store.pools[0].members[0]
                .as_ref()
                .unwrap()
                .marker
                .members
                .clone(),
            roots: fixture.paths.clone(),
            sealed: false,
        }],
        fragments: vec![Fragment {
            id: PoolId(SCOPE),
            alias: "test".into(),
            secret_hash: Argon2::default()
                .hash_password(b"test-secret", &SaltString::encode_b64(&[3; 16]).unwrap())
                .unwrap()
                .to_string(),
            permissions: Permissions::all(),
            operator: true,
        }],
    };
    assert!(
        store
            .configure_recovery_control(config.clone(), [18; 32])
            .is_err()
    );
    store.configure_recovery_control(config, [17; 32]).unwrap();
    let live = put(&store, vec![97; 12345]).await;
    let retired = put(&store, vec![98; 23456]).await;
    store
        .delete(SCOPE, retired.object_key.clone())
        .await
        .unwrap();
    let source = repository(&fixture, "control-source");
    let point = capture::offline(&source, &store).unwrap();
    assert!(point.service_control.is_some());
    let descriptor = serde_json::to_string(&point).unwrap();
    assert!(!descriptor.contains("secret_hash"));
    assert!(!descriptor.contains("signing_key"));
    source.verify(&point).unwrap();
    let target = repository(&fixture, "control-replica");
    assert_ne!(source.key.id, target.key.id);
    transfer::replicate(&source, &target, &point.id).unwrap();
    retention::gc(&target, true).unwrap();
    let replica = target.snapshot(&point.id).unwrap();
    target.verify(&replica).unwrap();
    let roots = targets(&fixture, "control-restored");
    restore::restore_fixture(&target, &replica, &roots).unwrap();
    let directory = fixture.base.join("recovered-service");
    std::fs::create_dir(&directory).unwrap();
    let output = directory.join("n1_config.json");
    control::restore(&target, &replica, &output, &roots).unwrap();
    control::restore(&target, &replica, &output, &roots).unwrap();
    let config = Config::load(&output).unwrap();
    assert_eq!(config.signing_secret().unwrap(), [17; 32]);
    assert_eq!(
        std::fs::metadata(&output).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(config.fragments[0].id, PoolId(SCOPE));
    let recovered = Store::new(
        vec![Pool::reopen_fixture(config.pools[0].layout.clone(), &config.pools[0].roots).unwrap()],
        config.signing_secret().unwrap(),
        16,
    )
    .unwrap();
    let blocked = directory.join("already-serving.json");
    assert!(control::restore(&target, &replica, &blocked, &roots).is_err());
    assert!(!blocked.exists());
    assert!(!blocked.with_extension("signing-key").exists());
    assert_eq!(
        recovered
            .download(SCOPE, live.object_key, 0, None, Vec::new())
            .await
            .unwrap()
            .0,
        vec![97; 12345]
    );
    assert!(matches!(
        recovered.stat(SCOPE, retired.object_key).await,
        Err(StoreError::Retired)
    ));
    assert!(matches!(
        recovered
            .write_part(
                SCOPE,
                retired.upload_key,
                0,
                std::io::Cursor::new(vec![98; 12288])
            )
            .await,
        Err(StoreError::Retired)
    ));
    std::fs::write(&output, b"operator edits must survive").unwrap();
    assert!(control::restore(&target, &replica, &output, &roots).is_err());
    assert_eq!(
        std::fs::read(&output).unwrap(),
        b"operator edits must survive"
    );
}
fn repository(fixture: &Fixture, name: &str) -> repository::Repository {
    let key = fixture.base.join(format!("{name}.key"));
    create_key(&key).unwrap();
    let path = fixture.base.join(name);
    repository::Repository::initialize(&path, &key).unwrap();
    repository::Repository::open(&path, &key, true).unwrap()
}
fn targets(fixture: &Fixture, prefix: &str) -> Vec<PathBuf> {
    (0..4)
        .map(|i| {
            let path = fixture.base.join(format!("{prefix}-{i}"));
            std::fs::create_dir(&path).unwrap();
            path
        })
        .collect()
}
async fn put(store: &Arc<Store>, data: Vec<u8>) -> crate::core::InitializedUpload {
    let part = (data.len() as u64).max(1).div_ceil(12288) * 12288;
    let init = store
        .init(SCOPE, 0, data.len() as u64, part, Durability::Durable, 3600)
        .unwrap();
    store
        .put(SCOPE, init.upload_key.clone(), std::io::Cursor::new(data))
        .await
        .unwrap();
    init
}
#[tokio::test]
async fn encrypted_incremental_portable_restore_preserves_live_and_permanently_retired_keys() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let repo = repository(&fixture, "repository");
    let data = (0..(13usize * 1024 * 1024 + 13))
        .map(|n| (n * 197 % 251) as u8)
        .collect::<Vec<_>>();
    let live = put(&store, data.clone()).await;
    let deleted = put(&store, vec![31; 9000]).await;
    store
        .delete(SCOPE, deleted.object_key.clone())
        .await
        .unwrap();
    let first = capture::offline(&repo, &store).unwrap();
    repo.verify(&first).unwrap();
    let count = walkdir::WalkDir::new(repo.path.join("blocks"))
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .count();
    let second = capture::offline(&repo, &store).unwrap();
    repo.verify(&second).unwrap();
    assert_ne!(first.id, second.id);
    let count2 = walkdir::WalkDir::new(repo.path.join("blocks"))
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .count();
    assert_eq!(
        count, count2,
        "identical payload and manifest blocks are reused"
    );
    let paths = targets(&fixture, "restore");
    restore::restore_fixture(&repo, &second, &paths).unwrap();
    let restored = Store::new(
        vec![Pool::reopen_fixture(fixture.layout.clone(), &paths).unwrap()],
        [17; 32],
        16,
    )
    .unwrap();
    restored.configure_read_repair(false);
    assert_eq!(
        restored
            .download(SCOPE, live.object_key, 0, None, Vec::new())
            .await
            .unwrap()
            .0,
        data
    );
    assert!(matches!(
        restored.stat(SCOPE, deleted.object_key).await,
        Err(StoreError::Retired)
    ));
    assert!(matches!(
        restored.finalize(SCOPE, deleted.upload_key).await,
        Err(StoreError::Retired)
    ));
    retention::prune(&repo, 1, &second.id).unwrap();
    assert_eq!(repo.list().unwrap().len(), 1);
    repo.verify(&second).unwrap();
}
#[tokio::test]
async fn offline_recovery_preserves_durable_multipart_holes_and_status_without_receipts() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let repo = repository(&fixture, "repository");
    let data = (0..30000)
        .map(|n| (n * 197 % 251) as u8)
        .collect::<Vec<_>>();
    let init = store
        .init(
            SCOPE,
            0,
            data.len() as u64,
            12288,
            Durability::Durable,
            3600,
        )
        .unwrap();
    for index in [0, 2] {
        let start = index * 12288;
        let end = (start + 12288).min(data.len());
        store
            .write_part(
                SCOPE,
                init.upload_key.clone(),
                index as u64,
                std::io::Cursor::new(data[start..end].to_vec()),
            )
            .await
            .unwrap();
    }
    let point = capture::offline(&repo, &store).unwrap();
    repo.verify(&point).unwrap();
    let paths = targets(&fixture, "resume");
    restore::restore_fixture(&repo, &point, &paths).unwrap();
    let restored = Store::new(
        vec![Pool::reopen_fixture(fixture.layout.clone(), &paths).unwrap()],
        [17; 32],
        16,
    )
    .unwrap();
    assert_eq!(
        restored
            .upload_status(SCOPE, init.upload_key.clone(), 0, 100)
            .await
            .unwrap()
            .missing_parts,
        vec![1]
    );
    restored
        .write_part(
            SCOPE,
            init.upload_key.clone(),
            1,
            std::io::Cursor::new(data[12288..24576].to_vec()),
        )
        .await
        .unwrap();
    restored.finalize(SCOPE, init.upload_key).await.unwrap();
    assert_eq!(
        restored
            .download(SCOPE, init.object_key, 0, None, Vec::new())
            .await
            .unwrap()
            .0,
        data
    );
}
#[tokio::test]
async fn replication_reencrypts_reuses_blocks_and_detects_corruption_before_clearing_restore_fences()
 {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let source = repository(&fixture, "source");
    let destination = repository(&fixture, "destination");
    let live = put(&store, vec![99; 50000]).await;
    let point = capture::offline(&source, &store).unwrap();
    transfer::replicate(&source, &destination, &point.id).unwrap();
    transfer::replicate(&source, &destination, &point.id).unwrap();
    destination
        .verify(&destination.snapshot(&point.id).unwrap())
        .unwrap();
    let path = destination
        .path
        .join("blocks")
        .join(&point.manifest[0][..2])
        .join(&point.manifest[0]);
    let mut damaged = std::fs::read(&path).unwrap();
    damaged[0] ^= 1;
    std::fs::write(&path, damaged).unwrap();
    assert!(destination.verify(&point).is_err());
    let paths = targets(&fixture, "fenced");
    assert!(restore::restore_fixture(&destination, &point, &paths).is_err());
    for path in &paths {
        assert!(path.join(restore::RESTORE_FENCE).is_file());
    }
    assert!(Pool::reopen_fixture(fixture.layout.clone(), &paths).is_err());
    assert!(source.verify(&point).is_ok());
    assert_eq!(
        store
            .download(SCOPE, live.object_key, 0, None, Vec::new())
            .await
            .unwrap()
            .0,
        vec![99; 50000]
    );
}
#[tokio::test]
async fn corrupted_live_shards_and_missing_markers_do_not_become_valid_recovery_points() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let repo = repository(&fixture, "repository");
    let init = put(&store, vec![9; 9000]).await;
    let plan = store.inspect_upload(SCOPE, &init.upload_key).unwrap();
    let fp = plan.identity.fingerprint();
    let pool = &store.pools[0];
    let path = pool.holder(fp, 0).unwrap().path.join(fp.shard_path(0));
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[128 + 96 + 3] ^= 1;
    std::fs::write(path, bytes).unwrap();
    assert!(capture::offline(&repo, &store).is_err());
    assert!(repo.list().unwrap().is_empty());
    let empty = capture::descriptors(&store.pools, "offline_all").unwrap();
    assert!(repo.verify(&empty).is_err());
}

#[tokio::test]
async fn online_capture_pins_live_and_terminal_files_excludes_private_uploads_and_releases_publication_gate()
 {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let repo = repository(&fixture, "online");
    let path = repo.path.clone();
    drop(repo);
    let live = put(&store, vec![7; 50000]).await;
    let deleted = put(&store, vec![8; 9000]).await;
    store
        .delete(SCOPE, deleted.object_key.clone())
        .await
        .unwrap();
    let unfinished = store
        .init(SCOPE, 0, 12288, 12288, Durability::Durable, 3600)
        .unwrap();
    store
        .write_part(
            SCOPE,
            unfinished.upload_key.clone(),
            0,
            std::io::Cursor::new(vec![9; 12288]),
        )
        .await
        .unwrap();
    let point = store
        .snapshot_online(path.clone(), fixture.base.join("online.key"), 30)
        .await
        .unwrap();
    assert_eq!(point.capture_mode, "online_committed");
    let repo =
        repository::Repository::open(&path, &fixture.base.join("online.key"), false).unwrap();
    repo.verify(&point).unwrap();
    let paths = targets(&fixture, "online-restore");
    restore::restore_fixture(&repo, &point, &paths).unwrap();
    let restored = Store::new(
        vec![Pool::reopen_fixture(fixture.layout.clone(), &paths).unwrap()],
        [17; 32],
        16,
    )
    .unwrap();
    assert_eq!(
        restored
            .download(SCOPE, live.object_key, 0, None, Vec::new())
            .await
            .unwrap()
            .0,
        vec![7; 50000]
    );
    assert!(matches!(
        restored.stat(SCOPE, deleted.object_key).await,
        Err(StoreError::Retired)
    ));
    assert!(matches!(
        restored
            .upload_status(SCOPE, unfinished.upload_key.clone(), 0, 100)
            .await,
        Err(StoreError::NotFound)
    ));
    store.finalize(SCOPE, unfinished.upload_key).await.unwrap();
}

#[tokio::test]
async fn cancelled_online_capture_never_publishes_a_partial_point_or_retains_a_member_lease() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let repo = repository(&fixture, "cancelled");
    let path = repo.path.clone();
    drop(repo);
    put(&store, vec![42; 50000]).await;
    let cancel = tokio_util::sync::CancellationToken::new();
    cancel.cancel();
    let error = store
        .snapshot_online_controlled(path.clone(), fixture.base.join("cancelled.key"), 30, cancel)
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::Interrupted
    );
    let repo =
        repository::Repository::open(&path, &fixture.base.join("cancelled.key"), true).unwrap();
    assert!(repo.list().unwrap().is_empty());
    drop(repo);
    store
        .snapshot_online(path, fixture.base.join("cancelled.key"), 30)
        .await
        .unwrap();
}

#[tokio::test]
async fn failed_restore_resumes_only_with_the_same_owned_point_and_preserves_terminal_state() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let repo = repository(&fixture, "repository");
    let live = put(&store, vec![61; 50000]).await;
    let dead = put(&store, vec![62; 2000]).await;
    store.delete(SCOPE, dead.object_key.clone()).await.unwrap();
    let point = capture::offline(&repo, &store).unwrap();
    let hash = &point.manifest[0];
    let path = repo.path.join("blocks").join(&hash[..2]).join(hash);
    let original = std::fs::read(&path).unwrap();
    let mut bad = original.clone();
    bad[1] ^= 1;
    std::fs::write(&path, bad).unwrap();
    let paths = targets(&fixture, "resume-restore");
    assert!(restore::restore_fixture(&repo, &point, &paths).is_err());
    std::fs::write(path, original).unwrap();
    assert!(restore::restore_fixture(&repo, &point, &paths).is_err());
    restore::resume_fixture(&repo, &point, &paths).unwrap();
    let restored = Store::new(
        vec![Pool::reopen_fixture(fixture.layout.clone(), &paths).unwrap()],
        [17; 32],
        16,
    )
    .unwrap();
    assert_eq!(
        restored
            .download(SCOPE, live.object_key, 0, None, Vec::new())
            .await
            .unwrap()
            .0,
        vec![61; 50000]
    );
    assert!(matches!(
        restored.stat(SCOPE, dead.object_key).await,
        Err(StoreError::Retired)
    ));
}

#[tokio::test]
async fn older_payload_restore_cannot_revive_a_key_retired_in_a_known_recovery_point() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let repo = repository(&fixture, "repository");
    let object = put(&store, vec![17; 30000]).await;
    let older = capture::offline(&repo, &store).unwrap();
    store
        .delete(SCOPE, object.object_key.clone())
        .await
        .unwrap();
    let newer = capture::offline(&repo, &store).unwrap();
    repo.verify(&newer).unwrap();
    let paths = targets(&fixture, "older-restore");
    restore::restore_fixture(&repo, &older, &paths).unwrap();
    let restored = Store::new(
        vec![Pool::reopen_fixture(fixture.layout.clone(), &paths).unwrap()],
        [17; 32],
        16,
    )
    .unwrap();
    assert!(matches!(
        restored.stat(SCOPE, object.object_key).await,
        Err(StoreError::Retired)
    ));
    assert!(matches!(
        restored.finalize(SCOPE, object.upload_key).await,
        Err(StoreError::Retired)
    ));
}

#[tokio::test]
async fn retention_refuses_to_forget_the_only_known_deletion_before_changing_any_point() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let repo = repository(&fixture, "retention");
    let object = put(&store, vec![17; 30000]).await;
    let older = capture::offline(&repo, &store).unwrap();
    store
        .delete(SCOPE, object.object_key.clone())
        .await
        .unwrap();
    let retired = capture::offline(&repo, &store).unwrap();
    assert!(
        retention::forget(&repo, &retired.id)
            .unwrap_err()
            .to_string()
            .contains("last saved retirement")
    );
    assert!(
        retention::prune(&repo, 1, &older.id)
            .unwrap_err()
            .to_string()
            .contains("last saved retirement")
    );
    assert_eq!(repo.list().unwrap().len(), 2);
    for point in [&older, &retired] {
        repo.verify(point).unwrap();
    }
    assert_eq!(
        std::fs::read_dir(repo.path.join("pending"))
            .unwrap()
            .count(),
        0
    );
    let successor = capture::offline(&repo, &store).unwrap();
    retention::forget(&repo, &retired.id).unwrap();
    retention::prune(&repo, 1, &successor.id).unwrap();
    let paths = targets(&fixture, "retention-restore");
    restore::restore_fixture(&repo, &successor, &paths).unwrap();
    let restored = Store::new(
        vec![Pool::reopen_fixture(fixture.layout.clone(), &paths).unwrap()],
        [17; 32],
        16,
    )
    .unwrap();
    assert!(matches!(
        restored.stat(SCOPE, object.object_key).await,
        Err(StoreError::Retired)
    ));
    assert!(matches!(
        restored.finalize(SCOPE, object.upload_key).await,
        Err(StoreError::Retired)
    ));
}

#[tokio::test]
async fn retirement_overlay_reclaims_restored_private_payload_and_revokes_its_old_ticket() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let repo = repository(&fixture, "repository");
    let object = store
        .init(SCOPE, 0, 12288, 12288, Durability::Durable, 3600)
        .unwrap();
    store
        .write_part(
            SCOPE,
            object.upload_key.clone(),
            0,
            std::io::Cursor::new(vec![66; 12288]),
        )
        .await
        .unwrap();
    let fp = store
        .inspect_upload(SCOPE, &object.upload_key)
        .unwrap()
        .identity
        .fingerprint();
    let older = capture::offline(&repo, &store).unwrap();
    store
        .delete(SCOPE, object.object_key.clone())
        .await
        .unwrap();
    capture::offline(&repo, &store).unwrap();
    let paths = targets(&fixture, "private-retirement");
    restore::restore_fixture(&repo, &older, &paths).unwrap();
    for path in &paths {
        assert!(!path.join(fp.staging_path()).exists());
        assert_eq!(std::fs::read_dir(path.join(".uploads")).unwrap().count(), 0);
    }
    let restored = Store::new(
        vec![Pool::reopen_fixture(fixture.layout.clone(), &paths).unwrap()],
        [17; 32],
        16,
    )
    .unwrap();
    assert!(matches!(
        restored.finalize(SCOPE, object.upload_key).await,
        Err(StoreError::Retired)
    ));
}

#[tokio::test]
async fn bounded_bidirectional_replication_reuses_a_completed_point_and_preserves_retirements() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let source = Arc::new(repository(&fixture, "source"));
    let dest = fixture.base.join("destination");
    repository::Repository::initialize(&dest, &fixture.base.join("source.key")).unwrap();
    let destination = Arc::new(
        repository::Repository::open(&dest, &fixture.base.join("source.key"), true).unwrap(),
    );
    let live = put(&store, vec![27; 200000]).await;
    let dead = put(&store, vec![28; 9000]).await;
    store.delete(SCOPE, dead.object_key.clone()).await.unwrap();
    let point = capture::offline(&source, &store).unwrap();
    for _ in 0..2 {
        let (mut sender, mut receiver) = tokio::io::duplex(64 * 1024);
        let (sent, received) = tokio::join!(
            network::send(&mut sender, source.clone(), point.id.clone()),
            network::receive(&mut receiver, destination.clone())
        );
        assert_eq!(sent.unwrap().id, point.id);
        assert_eq!(received.unwrap().id, point.id);
    }
    destination
        .verify(&destination.snapshot(&point.id).unwrap())
        .unwrap();
    let paths = targets(&fixture, "network-restore");
    restore::restore_fixture(&destination, &point, &paths).unwrap();
    let restored = Store::new(
        vec![Pool::reopen_fixture(fixture.layout.clone(), &paths).unwrap()],
        [17; 32],
        16,
    )
    .unwrap();
    assert_eq!(
        restored
            .download(SCOPE, live.object_key, 0, None, Vec::new())
            .await
            .unwrap()
            .0,
        vec![27; 200000]
    );
    assert!(matches!(
        restored.stat(SCOPE, dead.object_key).await,
        Err(StoreError::Retired)
    ));
}

#[tokio::test]
async fn hierarchical_manifest_restores_and_replicates_with_bounded_multi_level_indices() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let source = repository(&fixture, "source");
    let destination = repository(&fixture, "destination");
    let mut keys = Vec::new();
    for _ in 0..20 {
        keys.push(put(&store, Vec::new()).await.object_key);
    }
    let point = capture::offline(&source, &store).unwrap();
    assert!(point.manifest_depth >= 2);
    assert!(point.manifest.len() <= 4);
    source.verify(&point).unwrap();
    transfer::replicate(&source, &destination, &point.id).unwrap();
    let paths = targets(&fixture, "tree-restore");
    restore::restore_fixture(&destination, &point, &paths).unwrap();
    let restored = Store::new(
        vec![Pool::reopen_fixture(fixture.layout.clone(), &paths).unwrap()],
        [17; 32],
        16,
    )
    .unwrap();
    for key in keys {
        assert_eq!(restored.stat(SCOPE, key).await.unwrap().total_bytes, 0);
    }
    retention::gc(&destination, false).unwrap();
    destination.verify(&point).unwrap();
}

#[tokio::test]
async fn interrupted_replication_keeps_only_unpublished_pins_and_resumes_the_same_point() {
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    struct Cut {
        inner: tokio::io::DuplexStream,
        left: usize,
    }
    impl AsyncRead for Cut {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            out: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(cx, out)
        }
    }
    impl AsyncWrite for Cut {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            if self.left == 0 {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "injected transfer disconnect",
                )));
            }
            let n = bytes.len().min(self.left);
            match Pin::new(&mut self.inner).poll_write(cx, &bytes[..n]) {
                Poll::Ready(Ok(n)) => {
                    self.left -= n;
                    Poll::Ready(Ok(n))
                }
                other => other,
            }
        }
        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let source = Arc::new(repository(&fixture, "source"));
    put(&store, vec![41; 200000]).await;
    let point = capture::offline(&source, &store).unwrap();
    let destination_path = fixture.base.join("destination");
    repository::Repository::initialize(&destination_path, &fixture.base.join("source.key"))
        .unwrap();
    let destination = Arc::new(
        repository::Repository::open(&destination_path, &fixture.base.join("source.key"), true)
            .unwrap(),
    );
    let (sender, mut receiver) = tokio::io::duplex(64 * 1024);
    let source1 = source.clone();
    let id = point.id.clone();
    let (sent, received) = tokio::join!(
        async move {
            let mut sender = Cut {
                inner: sender,
                left: 20000,
            };
            network::send(&mut sender, source1, id).await
        },
        network::receive(&mut receiver, destination.clone())
    );
    assert!(sent.is_err());
    assert!(received.is_err());
    assert!(destination.list().unwrap().is_empty());
    assert!(
        walkdir::WalkDir::new(destination.path.join("blocks"))
            .into_iter()
            .filter_map(|e| e.ok())
            .any(|e| e.file_type().is_file())
    );
    let (mut sender, mut receiver) = tokio::io::duplex(64 * 1024);
    let (sent, received) = tokio::join!(
        network::send(&mut sender, source, point.id.clone()),
        network::receive(&mut receiver, destination.clone())
    );
    sent.unwrap();
    received.unwrap();
    destination
        .verify(&destination.snapshot(&point.id).unwrap())
        .unwrap();
}
