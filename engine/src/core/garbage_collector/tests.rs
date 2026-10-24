use super::*;
use crate::core::disk_manager::{PartMetadata, STORAGE_DISKS};
use crate::core::index_store::{self, PendingUploadBinding};
use crate::core::upload::UploadSessionManifest;
use crate::namespace::ncnx_store::{NcnxRecord, NcnxStore, TrashLocator};
use crate::test_support::global_test_lock;
use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use ulid::Ulid;

struct Fixture {
    root: PathBuf,
    fragment: String,
}

impl Fixture {
    fn new() -> Self {
        let roots = STORAGE_DISKS.get_or_init(|| {
            let root =
                std::env::temp_dir().join(format!("n2-upload-expiry-tests-{}", std::process::id()));
            fs::create_dir_all(&root).expect("create test storage root");
            vec![root.to_string_lossy().into_owned()]
        });
        let root = PathBuf::from(roots.first().expect("test storage root"));
        let fragment = Ulid::new().to_string();
        fs::create_dir_all(physical_fragment_root_on(&root, &fragment))
            .expect("create isolated test fragment");
        Self { root, fragment }
    }

    fn manifest(&self) -> UploadSessionManifest {
        UploadSessionManifest {
            fragment: self.fragment.clone(),
            object_key: Ulid::new().to_string(),
            packed_ext_u64: 0,
            version_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            total_size_bytes: 17,
            expected_parts: 1,
            part_size_bytes: 17,
            content_type: "application/octet-stream".into(),
            created_at: 1,
            pool_id: "pool-a".into(),
            pool_generation: 1,
            stripe_size_bytes: 4096,
            membership_epoch: 1,
            data_shards: 1,
            parity_shards: 1,
        }
    }

    fn path(&self) -> PathBuf {
        physical_fragment_root_on(&self.root, &self.fragment)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for root in ObjectPathManager::new().get_storage_roots() {
            let _ = fs::remove_dir_all(physical_fragment_root_on(&root, &self.fragment));
        }
    }
}

fn write(path: &Path, bytes: &[u8]) {
    fs::create_dir_all(path.parent().expect("parent")).expect("create test directory");
    fs::write(path, bytes).expect("write test payload");
}

fn snapshot(root: &Path) -> BTreeMap<PathBuf, (u64, Vec<u8>)> {
    let mut result = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let metadata = fs::symlink_metadata(&path).expect("snapshot metadata");
        let bytes = if metadata.is_file() {
            fs::read(&path).expect("snapshot bytes")
        } else {
            Vec::new()
        };
        result.insert(path.clone(), (metadata.ino(), bytes));
        if metadata.is_dir() {
            stack.extend(
                fs::read_dir(path)
                    .expect("snapshot directory")
                    .map(|entry| entry.expect("snapshot entry").path()),
            );
        }
    }
    result
}

#[tokio::test]
async fn expired_sessions_retain_binding_receipts_headless_metadata_and_payload() {
    let _guard = global_test_lock().lock().await;
    let fixture = Fixture::new();
    let ttl = 60;
    let now = Utc::now()
        .timestamp()
        .saturating_add(ttl)
        .saturating_add(100);
    let mut manifests = Vec::new();
    for _ in 0..6 {
        let manifest = fixture.manifest();
        index_store::open_session(&manifest, None).expect("open real durable session");
        let payload = ObjectPathManager::new().get_shard_dir_on_disk(
            &fixture.root,
            &fixture.fragment,
            &manifest.object_key,
            &manifest.version_id,
        );
        write(&payload.join("part_0_shard_0.shard"), b"uncommitted-piece");
        manifests.push(manifest);
    }
    let bound = &manifests[1];
    index_store::write_pending_binding(&PendingUploadBinding {
        fragment: fixture.fragment.clone(),
        logical_path: "report.bin".into(),
        physical_object_id: bound.object_key.clone(),
        version_id: bound.version_id.clone(),
        session_id: bound.session_id.clone(),
        created_at_unix: 1,
    })
    .expect("persist pending NCNX binding");
    let receipt = index::SessionPartReceipt {
        part_index: 0,
        size_bytes: 17,
        shard_size_bytes: 17,
        created_at: 1,
        checksum: Some([7; 32]),
        shard_checksums: vec![[7; 32], [8; 32]],
    };
    index_store::commit_session_part_receipt(&fixture.fragment, &manifests[2].version_id, &receipt)
        .expect("persist abandoned part receipt");
    let committed = &manifests[3];
    index_store::upsert_live_version(&crate::core::disk_manager::ObjectMetadata {
        object_key: committed.object_key.clone(),
        packed_ext_u64: 0,
        fragment: fixture.fragment.clone(),
        version_id: committed.version_id.clone(),
        upload_session_id: committed.session_id.clone(),
        owner_uid: fixture.fragment.clone(),
        content_type: committed.content_type.clone(),
        total_size_bytes: 17,
        created_at: 1,
        pool_id: committed.pool_id.clone(),
        pool_generation: 1,
        membership_epoch: 1,
        data_shards: 1,
        parity_shards: 1,
        parts: Vec::new(),
        is_delete_marker: false,
        stripe_size_bytes: 4096,
    })
    .expect("persist headless committed NCDX before NCNX publication");
    let mut recent_receipt = receipt.clone();
    recent_receipt.created_at = now - 1;
    index_store::commit_session_part_receipt(
        &fixture.fragment,
        &manifests[4].version_id,
        &recent_receipt,
    )
    .expect("persist recent receipt on an old manifest");
    // Compact filename receipts do not encode their completion time. The
    // distributed durable path also persists PartMetadata, which does.
    index_store::commit_session_part(
        &fixture.fragment,
        &manifests[4].version_id,
        &PartMetadata {
            part_index: 0,
            size_bytes: 17,
            created_at: now - 1,
            shards: Vec::new(),
            checksum: None,
        },
    )
    .expect("persist the durable part completion time");
    index_store::commit_session_part(
        &fixture.fragment,
        &manifests[5].version_id,
        &PartMetadata {
            part_index: 0,
            size_bytes: 17,
            created_at: now - 1,
            shards: Vec::new(),
            checksum: None,
        },
    )
    .expect("persist recent part on an old manifest");

    let before = snapshot(&fixture.path());
    let mut stats = FragmentGcStats::default();
    inspect_expired_upload_sessions(&fixture.fragment, now, ttl, &mut stats)
        .await
        .expect("expire without unproven reclamation");
    assert_eq!(stats.sessions_deleted, 0);
    assert_eq!(stats.sessions_deferred, 4);
    assert_eq!(stats.refund_bytes, 0);
    assert_eq!(snapshot(&fixture.path()), before);
    for manifest in manifests {
        assert!(
            index_store::load_session(&fixture.fragment, &manifest.version_id)
                .expect("load retained session")
                .is_some()
        );
    }
}

#[test]
fn existing_receipts_and_part_times_define_latest_activity_without_extra_writes() {
    let manifest = UploadSessionManifest {
        fragment: "fragment".into(),
        object_key: "object".into(),
        packed_ext_u64: 0,
        version_id: Ulid::new().to_string(),
        session_id: Ulid::new().to_string(),
        total_size_bytes: 1,
        expected_parts: 1,
        part_size_bytes: 1,
        content_type: "application/octet-stream".into(),
        created_at: 10,
        pool_id: "pool-a".into(),
        pool_generation: 1,
        stripe_size_bytes: 4096,
        membership_epoch: 1,
        data_shards: 1,
        parity_shards: 0,
    };
    let mut session = index::IndexSessionState {
        manifest,
        reservation: None,
        parts: BTreeMap::new(),
        part_receipts: BTreeMap::new(),
        last_activity_at: 20,
        finalized_at: None,
        aborted_at: None,
    };
    assert_eq!(session_last_activity(&session), 20);
    session.parts.insert(
        0,
        PartMetadata {
            part_index: 0,
            size_bytes: 1,
            created_at: 30,
            shards: Vec::new(),
            checksum: None,
        },
    );
    assert_eq!(session_last_activity(&session), 30);
    session.part_receipts.insert(
        0,
        index::SessionPartReceipt {
            part_index: 0,
            size_bytes: 1,
            shard_size_bytes: 1,
            created_at: 40,
            checksum: None,
            shard_checksums: Vec::new(),
        },
    );
    assert_eq!(session_last_activity(&session), 40);
    session.manifest.created_at = 50;
    assert_eq!(session_last_activity(&session), 50);
}

#[tokio::test]
async fn uncoordinated_expired_reclamation_retains_last_pointer_and_concurrent_recovery_payload() {
    let _guard = global_test_lock().lock().await;
    let fixture = Fixture::new();
    let manifest = fixture.manifest();
    let payload = ObjectPathManager::new().get_shard_dir_on_disk(
        &fixture.root,
        &fixture.fragment,
        &manifest.object_key,
        &manifest.version_id,
    );
    write(
        &payload.join("part_0_shard_0.shard"),
        b"committed immutable payload",
    );
    index_store::upsert_live_version(&crate::core::disk_manager::ObjectMetadata {
        object_key: manifest.object_key.clone(),
        packed_ext_u64: 0,
        fragment: fixture.fragment.clone(),
        version_id: manifest.version_id.clone(),
        upload_session_id: manifest.session_id.clone(),
        owner_uid: fixture.fragment.clone(),
        content_type: "application/octet-stream".into(),
        total_size_bytes: 27,
        created_at: 1,
        pool_id: "pool-a".into(),
        pool_generation: 1,
        membership_epoch: 1,
        data_shards: 1,
        parity_shards: 1,
        parts: Vec::new(),
        is_delete_marker: false,
        stripe_size_bytes: 4096,
    })
    .unwrap();
    let store = NcnxStore::new("test-root", &fixture.root);
    let bytes = NcnxRecord::new(
        &fixture.fragment,
        &manifest.object_key,
        Ulid::new().to_string(),
        1,
    )
    .unwrap()
    .encode()
    .unwrap();
    let trash = TrashLocator {
        logical_key: "report.bin".into(),
        deleted_at_unix_nanos: 1,
        delete_mutation_id: Ulid::new().to_string(),
    };
    store
        .create_trash(&fixture.fragment, &trash, &bytes)
        .unwrap();
    let before = snapshot(&fixture.path());
    let mut stats = FragmentGcStats::default();
    defer_unproven_expired_reclamation(&mut stats);
    assert_eq!(
        snapshot(&fixture.path()),
        before,
        "expiry without object ownership must retain the last recoverable pointer and every payload byte"
    );
    assert!(
        store
            .read_trash(&fixture.fragment, &trash)
            .unwrap()
            .is_some()
    );

    // A prior reference scan observed no live key. Recover completes before
    // that old observation reaches the reclamation branch. Deferral is safe
    // in both orders; no old observation authorizes a physical peer purge.
    store
        .create_live(&fixture.fragment, &trash.logical_key, &bytes)
        .unwrap();
    store
        .remove_trash_if_exact(&fixture.fragment, &trash, &bytes)
        .unwrap();
    let recovered = snapshot(&fixture.path());
    defer_unproven_expired_reclamation(&mut stats);
    assert_eq!(snapshot(&fixture.path()), recovered);
    assert!(
        store
            .read_live(&fixture.fragment, &trash.logical_key)
            .unwrap()
            .is_some()
    );
    assert_eq!(
        fs::read(payload.join("part_0_shard_0.shard")).unwrap(),
        b"committed immutable payload"
    );
    assert_eq!(stats.trash_reclamations_deferred, 2);
    assert_eq!(stats.trash_versions_expired, 0);
    assert_eq!(stats.refund_bytes, 0);
}
