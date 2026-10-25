use super::*;
use crate::core::disk_manager::{ObjectPathManager, STORAGE_DISKS};
use crate::distribution::membership::{
    ClusterMembership, ClusterNode, ClusterPolicy, N2_MEMBERSHIP_FORMAT_VERSION, NodeState,
    PoolState, ServerPool, StorageRoot,
};
use crate::test_support::global_test_lock;
use std::time::Duration;

struct SavePause {
    started: std::sync::atomic::AtomicBool,
    released: Mutex<bool>,
    wake: std::sync::Condvar,
}

static SAVE_PAUSE: LazyLock<Mutex<Option<(VersionKey, Arc<SavePause>)>>> =
    LazyLock::new(|| Mutex::new(None));

struct SavePauseCleanup(Arc<SavePause>);

impl Drop for SavePauseCleanup {
    fn drop(&mut self) {
        if let Ok(mut selected) = SAVE_PAUSE.lock() {
            *selected = None;
        }
        if let Ok(mut released) = self.0.released.lock() {
            *released = true;
            self.0.wake.notify_all();
        }
    }
}

pub(super) fn pause_save_if_requested(record: &UploadLifecycleRecord) {
    let selected = SAVE_PAUSE
        .lock()
        .unwrap()
        .as_ref()
        .and_then(|(key, pause)| {
            (key == &(
                record.identity.fragment.clone(),
                record.identity.version_id.clone(),
            ))
                .then(|| pause.clone())
        });
    if let Some(pause) = selected {
        pause
            .started
            .store(true, std::sync::atomic::Ordering::Release);
        let mut released = pause.released.lock().unwrap();
        while !*released {
            released = pause.wake.wait(released).unwrap();
        }
    }
}

struct Fixture {
    runtime: ClusterRuntime,
    identity: UploadIdentity,
    root: PathBuf,
    second: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let roots = STORAGE_DISKS.get_or_init(|| {
            let path =
                std::env::temp_dir().join(format!("n2-ownership-shared-{}", std::process::id()));
            fs::create_dir_all(&path).unwrap();
            vec![path.to_string_lossy().into_owned()]
        });
        let root = PathBuf::from(&roots[0]);
        let second = std::env::temp_dir().join(format!("n2-ownership-second-{}", Ulid::new()));
        fs::create_dir_all(&second).unwrap();
        let storage_roots = vec![
            StorageRoot {
                id: "r0".into(),
                physical_id: Some("physical-0".into()),
                path: root.to_string_lossy().into_owned(),
            },
            StorageRoot {
                id: "r1".into(),
                physical_id: Some("physical-1".into()),
                path: second.to_string_lossy().into_owned(),
            },
        ];
        let nodes = (0..2)
            .map(|i| ClusterNode {
                id: format!("n{i}"),
                instance_id: format!("instance-{i}"),
                binding_epoch: 1,
                endpoint: format!("http://127.0.0.1:{}", 53000 + i),
                native_endpoint: None,
                state: Some(NodeState::Online),
                weight: 1,
                storage_roots: storage_roots.clone(),
                joined_epoch: 1,
                capacity_bytes: None,
                used_bytes: None,
                last_seen_unix: None,
            })
            .collect::<Vec<_>>();
        let runtime = ClusterRuntime {
            local_node_id: "n0".into(),
            local_instance_id: "instance-0".into(),
            local_is_reserve: false,
            local_pool_id: "pool-a".into(),
            local_storage_roots: storage_roots,
            membership_path: second.join("membership.json"),
            layout_policy: crate::distribution::layout::ClusterLayoutPolicy {
                data_shards: 1,
                parity_shards: 1,
            },
            membership: ClusterMembership {
                format_version: N2_MEMBERSHIP_FORMAT_VERSION,
                cluster_id: "ownership-test-cluster".into(),
                epoch: 1,
                binding_epoch: 1,
                pools: vec![ServerPool {
                    id: "pool-a".into(),
                    generation: 1,
                    policy: ClusterPolicy {
                        object_data_shards: 1,
                        object_parity_shards: 1,
                    },
                    state: PoolState::Writable,
                    node_ids: vec!["n0".into(), "n1".into()],
                }],
                nodes,
                reserve_nodes: Vec::new(),
                retired_instances: Vec::new(),
            },
            auto_rebalance: false,
        };
        let identity = UploadIdentity {
            cluster_id: runtime.membership.cluster_id.clone(),
            pool_id: "pool-a".into(),
            pool_generation: 1,
            fragment: Ulid::new().to_string(),
            object_id: Ulid::new().to_string(),
            version_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            data_shards: 1,
            parity_shards: 1,
            created_at_unix: 1,
        };
        Self {
            runtime,
            identity,
            root,
            second,
        }
    }

    async fn read_guard(&self) -> Result<UploadReadGuard, AppError> {
        let id = &self.identity;
        guard_shard_write(
            &self.runtime,
            &id.pool_id,
            &id.fragment,
            &id.object_id,
            &id.version_id,
        )
        .await
    }

    fn publish_ncdx(&self) {
        let id = &self.identity;
        let dir = ObjectPathManager::new().get_shard_dir_on_disk(
            &self.root,
            &id.fragment,
            &id.object_id,
            &id.version_id,
        );
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("part_0_shard_0.shard"), b"committed piece").unwrap();
        index_store::upsert_live_version(&ObjectMetadata {
            object_key: id.object_id.clone(),
            packed_ext_u64: 0,
            fragment: id.fragment.clone(),
            version_id: id.version_id.clone(),
            upload_session_id: id.session_id.clone(),
            owner_uid: id.fragment.clone(),
            content_type: "application/octet-stream".into(),
            total_size_bytes: 15,
            created_at: 1,
            pool_id: id.pool_id.clone(),
            pool_generation: 1,
            membership_epoch: 1,
            data_shards: 1,
            parity_shards: 1,
            parts: Vec::new(),
            is_delete_marker: false,
            stripe_size_bytes: 4096,
        })
        .unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for root in ObjectPathManager::new().get_storage_roots() {
            let _ = fs::remove_dir_all(physical_fragment_root_on(&root, &self.identity.fragment));
        }
        let _ = fs::remove_dir_all(&self.second);
    }
}

#[tokio::test]
async fn parallel_parts_block_abort_until_finish_and_durable_fence_survives_reload() {
    let _global = global_test_lock().lock().await;
    let fixture = Fixture::new();
    assert!(
        fixture.read_guard().await.is_err(),
        "unregistered upload cannot write"
    );
    ensure_open(&fixture.runtime, &fixture.identity)
        .await
        .unwrap();
    let first = fixture.read_guard().await.unwrap();
    let second = tokio::time::timeout(Duration::from_secs(1), fixture.read_guard())
        .await
        .unwrap()
        .unwrap();
    let runtime = fixture.runtime.clone();
    let identity = fixture.identity.clone();
    let abort = tokio::spawn(async move { abort_upload(&runtime, &identity).await });
    tokio::task::yield_now().await;
    assert!(
        !abort.is_finished(),
        "abort cannot overlap an admitted part"
    );
    drop(first);
    tokio::task::yield_now().await;
    assert!(!abort.is_finished(), "every active part must drain");
    drop(second);
    let aborted = tokio::time::timeout(Duration::from_secs(3), abort)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(aborted.state, UploadLifecycleState::Aborted);
    for root in [&fixture.root, &fixture.second] {
        assert_eq!(
            read_record(&OwnershipStore::path(root, &fixture.identity))
                .unwrap()
                .unwrap()
                .state,
            UploadLifecycleState::Aborted
        );
    }
    assert_eq!(
        OwnershipStore::from_runtime(&fixture.runtime)
            .unwrap()
            .load(&fixture.identity)
            .unwrap()
            .unwrap()
            .state,
        UploadLifecycleState::Aborted
    );
    assert!(
        ensure_open(&fixture.runtime, &fixture.identity)
            .await
            .is_err(),
        "delayed begin cannot revive an aborted version"
    );
    assert!(fixture.read_guard().await.is_err());
    assert!(
        guard_metadata_publication(&fixture.runtime, &fixture.identity)
            .await
            .is_err()
    );
    assert!(
        guard_aborted_cleanup(&fixture.runtime, &fixture.identity)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn metadata_commit_excludes_writers_and_abort_but_allows_verified_repair_admission() {
    let _global = global_test_lock().lock().await;
    let fixture = Fixture::new();
    ensure_open(&fixture.runtime, &fixture.identity)
        .await
        .unwrap();
    let part = fixture.read_guard().await.unwrap();
    let runtime = fixture.runtime.clone();
    let identity = fixture.identity.clone();
    let pending =
        tokio::spawn(async move { guard_metadata_publication(&runtime, &identity).await });
    tokio::task::yield_now().await;
    assert!(!pending.is_finished());
    drop(part);
    let mut commit = tokio::time::timeout(Duration::from_secs(3), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    fixture.publish_ncdx();
    commit.mark_committed().await.unwrap();
    drop(commit);
    assert!(
        abort_upload(&fixture.runtime, &fixture.identity)
            .await
            .is_err()
    );
    assert!(
        fixture.read_guard().await.is_err(),
        "ordinary upload cannot rewrite committed data"
    );
    let id = &fixture.identity;
    assert!(
        guard_committed_repair(
            &fixture.runtime,
            &id.pool_id,
            &id.fragment,
            &id.object_id,
            &id.version_id
        )
        .await
        .is_ok()
    );
    assert!(guard_aborted_cleanup(&fixture.runtime, id).await.is_err());
    assert_eq!(
        inspect_upload(&fixture.runtime, id)
            .await
            .unwrap()
            .unwrap()
            .state,
        UploadLifecycleState::Committed
    );
}

#[tokio::test]
async fn durable_ncdx_before_state_marker_survives_abort_without_any_ncnx() {
    let _global = global_test_lock().lock().await;
    let fixture = Fixture::new();
    ensure_open(&fixture.runtime, &fixture.identity)
        .await
        .unwrap();
    fixture.publish_ncdx();
    assert!(
        abort_upload(&fixture.runtime, &fixture.identity)
            .await
            .is_err()
    );
    assert_eq!(
        inspect_upload(&fixture.runtime, &fixture.identity)
            .await
            .unwrap()
            .unwrap()
            .state,
        UploadLifecycleState::Committed
    );
    let dir = ObjectPathManager::new().get_shard_dir_on_disk(
        &fixture.root,
        &fixture.identity.fragment,
        &fixture.identity.object_id,
        &fixture.identity.version_id,
    );
    assert_eq!(
        fs::read(dir.join("part_0_shard_0.shard")).unwrap(),
        b"committed piece"
    );
}

#[tokio::test]
async fn missing_root_corrupt_record_identity_changes_and_partial_terminal_copies_fail_closed() {
    let _global = global_test_lock().lock().await;
    let fixture = Fixture::new();
    ensure_open(&fixture.runtime, &fixture.identity)
        .await
        .unwrap();
    let mut changed = fixture.identity.clone();
    changed.session_id = Ulid::new().to_string();
    assert!(ensure_open(&fixture.runtime, &changed).await.is_err());
    changed = fixture.identity.clone();
    changed.object_id = Ulid::new().to_string();
    assert!(
        ensure_open(&fixture.runtime, &changed).await.is_err(),
        "version file shares a lock across forged object identity"
    );
    let path = OwnershipStore::path(&fixture.second, &fixture.identity);
    let bytes = fs::read(&path).unwrap();
    fs::write(&path, b"corrupt ownership").unwrap();
    assert!(fixture.read_guard().await.is_err());
    assert!(
        abort_upload(&fixture.runtime, &fixture.identity)
            .await
            .is_err()
    );
    fs::write(&path, &bytes).unwrap();

    let record = UploadLifecycleRecord {
        identity: fixture.identity.clone(),
        state: UploadLifecycleState::Aborted,
        updated_at_unix: 2,
    };
    OwnershipStore {
        roots: vec![fixture.second.clone()],
        binding: None,
    }
    .save(&record)
    .unwrap();
    assert!(
        fixture.read_guard().await.is_err(),
        "one durable abort copy fences stale Open copies"
    );
    assert!(
        ensure_open(&fixture.runtime, &fixture.identity)
            .await
            .is_err()
    );
    let mut conflicting = record.clone();
    conflicting.state = UploadLifecycleState::Committed;
    OwnershipStore {
        roots: vec![fixture.root.clone()],
        binding: None,
    }
    .save(&conflicting)
    .unwrap();
    assert!(
        inspect_upload(&fixture.runtime, &fixture.identity)
            .await
            .is_err()
    );

    let moved = fixture.second.with_extension("missing");
    fs::rename(&fixture.second, &moved).unwrap();
    assert!(
        inspect_upload(&fixture.runtime, &fixture.identity)
            .await
            .is_err(),
        "missing local control copy is not absence"
    );
    fs::rename(moved, &fixture.second).unwrap();
}

#[tokio::test]
async fn ownership_listing_is_page_bounded_and_does_not_walk_object_payloads() {
    let _global = global_test_lock().lock().await;
    let fixture = Fixture::new();
    let mut expected = Vec::new();
    let store = OwnershipStore::from_runtime(&fixture.runtime).unwrap();
    for index in 0..37 {
        let mut identity = fixture.identity.clone();
        identity.version_id = Ulid::from_parts(index + 1, 1).to_string();
        expected.push(identity.version_id.clone());
        store
            .save(&UploadLifecycleRecord {
                identity,
                state: UploadLifecycleState::Open,
                updated_at_unix: 1,
            })
            .unwrap();
    }
    let ignored =
        physical_fragment_root_on(&fixture.root, &fixture.identity.fragment).join(".objects");
    fs::create_dir_all(&ignored).unwrap();
    std::os::unix::fs::symlink(
        "/path-that-cannot-be-traversed",
        ignored.join("unknown-object"),
    )
    .unwrap();
    let mut inventory = UploadInventory::new(&fixture.runtime, &fixture.identity.fragment).unwrap();
    let mut found = Vec::new();
    let mut scanned = 0;
    loop {
        let page = inventory.next_page(5).await.unwrap();
        assert!(page.records.len() <= 5);
        assert!(page.scanned_entries <= 5);
        scanned += page.scanned_entries;
        found.extend(page.records.into_iter().map(|r| r.identity.version_id));
        if page.finished {
            break;
        }
    }
    found.sort();
    assert_eq!(found, expected);
    assert!(scanned <= 37 * 2 + 6, "each root is enumerated once");
}

#[tokio::test]
async fn idle_abort_defers_active_parts_and_rechecks_checkpointed_activity() {
    let _global = global_test_lock().lock().await;
    let fixture = Fixture::new();
    let store = OwnershipStore::from_runtime(&fixture.runtime).unwrap();
    store
        .save(&UploadLifecycleRecord {
            identity: fixture.identity.clone(),
            state: UploadLifecycleState::Open,
            updated_at_unix: 1,
        })
        .unwrap();
    let part = fixture.read_guard().await.unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_secs(1),
            abort_upload_if_idle(&fixture.runtime, &fixture.identity, Some(10))
        )
        .await
        .unwrap()
        .is_err()
    );
    part.touch_activity().await.unwrap();
    drop(part);
    assert!(
        abort_upload_if_idle(&fixture.runtime, &fixture.identity, Some(10))
            .await
            .is_err(),
        "stale GC observation must not abort fresh part activity"
    );
    assert_eq!(
        inspect_upload(&fixture.runtime, &fixture.identity)
            .await
            .unwrap()
            .unwrap()
            .state,
        UploadLifecycleState::Open
    );
    abort_upload(&fixture.runtime, &fixture.identity)
        .await
        .unwrap();
}

#[tokio::test]
async fn terminal_fences_survive_compaction_without_historical_work_directories() {
    let _global = global_test_lock().lock().await;
    let fixture = Fixture::new();
    ensure_open(&fixture.runtime, &fixture.identity)
        .await
        .unwrap();
    abort_upload(&fixture.runtime, &fixture.identity)
        .await
        .unwrap();
    guard_aborted_cleanup(&fixture.runtime, &fixture.identity)
        .await
        .unwrap()
        .cleanup_aborted_payload()
        .await
        .unwrap();
    for root in [&fixture.root, &fixture.second] {
        assert!(
            !OwnershipStore::path(root, &fixture.identity)
                .parent()
                .unwrap()
                .exists(),
            "finished one-shot must not leave an empty active work directory"
        );
        assert!(OwnershipStore::terminal_path(root, &fixture.identity).is_file());
    }
    let mut inventory = UploadInventory::new(&fixture.runtime, &fixture.identity.fragment).unwrap();
    loop {
        let page = inventory.next_page(5).await.unwrap();
        assert!(
            page.records.is_empty(),
            "terminal fences are not active work"
        );
        if page.finished {
            break;
        }
    }
    assert!(
        ensure_open(&fixture.runtime, &fixture.identity)
            .await
            .is_err()
    );
}

#[path = "inventory_tests.rs"]
mod inventory_tests;

#[tokio::test]
async fn commit_probe_waits_for_publication_and_rejects_different_metadata() {
    let _global = global_test_lock().lock().await;
    let fixture = Fixture::new();
    ensure_open(&fixture.runtime, &fixture.identity)
        .await
        .unwrap();
    let mut publication = guard_metadata_publication(&fixture.runtime, &fixture.identity)
        .await
        .unwrap();
    fixture.publish_ncdx();
    let metadata = index_store::read_live_version(
        &fixture.identity.fragment,
        &fixture.identity.object_id,
        &fixture.identity.version_id,
    )
    .unwrap()
    .unwrap();
    let inventory = index_store::list_local_committed_heads(&fixture.identity.fragment).unwrap();
    assert!(
        inventory
            .iter()
            .any(|head| head.version_id == metadata.version_id
                && head.pool_id == metadata.pool_id
                && head.upload_session_id == metadata.upload_session_id),
        "headless committed NCDX must remain in exact peer inventory"
    );
    let runtime = fixture.runtime.clone();
    let expected = metadata.clone();
    let probe = tokio::spawn(async move { probe_committed_metadata(&runtime, &expected).await });
    tokio::task::yield_now().await;
    assert!(
        !probe.is_finished(),
        "commit proof cannot race an unfinished publication durability boundary"
    );
    publication.mark_committed().await.unwrap();
    drop(publication);
    assert!(
        tokio::time::timeout(Duration::from_secs(3), probe)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    );
    let mut changed = metadata;
    changed.content_type = "different/metadata".into();
    assert!(
        !probe_committed_metadata(&fixture.runtime, &changed)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn cancellation_cannot_remove_registry_owner_while_persistence_worker_is_blocked() {
    let _global = global_test_lock().lock().await;
    let fixture = Fixture::new();
    ensure_open(&fixture.runtime, &fixture.identity)
        .await
        .unwrap();
    let pause = Arc::new(SavePause {
        started: std::sync::atomic::AtomicBool::new(false),
        released: Mutex::new(false),
        wake: std::sync::Condvar::new(),
    });
    let _cleanup = SavePauseCleanup(pause.clone());
    *SAVE_PAUSE.lock().unwrap() = Some((
        (
            fixture.identity.fragment.clone(),
            fixture.identity.version_id.clone(),
        ),
        pause.clone(),
    ));
    let runtime = fixture.runtime.clone();
    let identity = fixture.identity.clone();
    let caller = tokio::spawn(async move { abort_upload(&runtime, &identity).await });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while !pause.started.load(std::sync::atomic::Ordering::Acquire) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "persist worker must reach controlled barrier"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    caller.abort();
    let _ = caller.await;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(30),
            ensure_open(&fixture.runtime, &fixture.identity)
        )
        .await
        .is_err(),
        "successor must not create a different registry lock around cancelled disk worker"
    );
    *SAVE_PAUSE.lock().unwrap() = None;
    *pause.released.lock().unwrap() = true;
    pause.wake.notify_all();
    assert!(
        tokio::time::timeout(
            Duration::from_secs(3),
            ensure_open(&fixture.runtime, &fixture.identity)
        )
        .await
        .unwrap()
        .is_err()
    );
    assert_eq!(
        inspect_upload(&fixture.runtime, &fixture.identity)
            .await
            .unwrap()
            .unwrap()
            .state,
        UploadLifecycleState::Aborted
    );
}

#[test]
fn commit_k_and_abort_m_plus_one_certificates_cannot_both_exist() {
    for n in 2usize..=12 {
        for k in 1..n {
            let m = n - k;
            for committed in 0..1usize << n {
                if (committed.count_ones() as usize) < k {
                    continue;
                }
                let can_abort = n - committed.count_ones() as usize;
                assert!(
                    can_abort < m + 1,
                    "terminal holder states force commit/abort quorum intersection"
                );
            }
        }
    }
}
