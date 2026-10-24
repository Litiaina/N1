use super::*;
use crate::core::disk_manager::{
    FS_STORAGE_ROOT, ObjectMetadata, ObjectPathManager, STORAGE_DISKS,
};
use crate::core::fragment_identity::physical_fragment_root_on;
use crate::core::index_store;
use crate::core::upload::UploadSessionManifest;
use crate::namespace::ncnx_store::{NcnxRecord, NcnxStore, TrashLocator};
use crate::test_support::global_test_lock;
use std::collections::BTreeMap;
use std::fs::{self, File, FileTimes};
use std::os::unix::fs::{MetadataExt, symlink};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use ulid::Ulid;

struct Fixture {
    root: PathBuf,
    fragment: String,
}

impl Fixture {
    fn new() -> Self {
        let roots = STORAGE_DISKS.get_or_init(|| {
            let root =
                std::env::temp_dir().join(format!("n2-maintenance-tests-{}", std::process::id()));
            fs::create_dir_all(&root).expect("create maintenance test root");
            vec![root.to_string_lossy().into_owned()]
        });
        let root = PathBuf::from(roots.first().expect("test storage root"));
        let fragment = Ulid::new().to_string();
        fs::create_dir_all(physical_fragment_root_on(&root, &fragment))
            .expect("create isolated fragment");
        Self { root, fragment }
    }

    fn path(&self) -> PathBuf {
        physical_fragment_root_on(&self.root, &self.fragment)
    }

    fn claims(&self) -> Claims {
        Claims {
            fragment: self.fragment.clone(),
            fragment_alias: None,
            read: true,
            write: true,
            modify: true,
            delete: true,
            exp: usize::MAX,
        }
    }

    fn pin_path(&self) -> PathBuf {
        let fs_root = FS_STORAGE_ROOT.get_or_init(|| {
            self.root
                .join("maintenance-state")
                .to_string_lossy()
                .into_owned()
        });
        PathBuf::from(fs_root)
            .join(".n2-export-pins")
            .join(format!("{}.json", self.fragment))
    }

    async fn inspect(&self) -> serde_json::Value {
        let response = sync_fragment_metadata(self.claims())
            .await
            .expect("inspect fragment")
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read response");
        serde_json::from_slice(&body).expect("decode cleanup response")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Never remove shared roots: other tests may use the OnceLock roots.
        for root in ObjectPathManager::new().get_storage_roots() {
            let _ = fs::remove_dir_all(physical_fragment_root_on(&root, &self.fragment));
        }
        if let Some(root) = FS_STORAGE_ROOT.get() {
            let _ = fs::remove_file(
                PathBuf::from(root)
                    .join(".n2-export-pins")
                    .join(format!("{}.json", self.fragment)),
            );
        }
    }
}

fn write(path: &Path, bytes: &[u8]) {
    fs::create_dir_all(path.parent().expect("parent")).expect("create fixture parent");
    fs::write(path, bytes).expect("write fixture");
}

fn metadata(fragment: &str, object: &str, version: &str, size: u64) -> ObjectMetadata {
    ObjectMetadata {
        object_key: object.to_string(),
        packed_ext_u64: 0,
        fragment: fragment.to_string(),
        version_id: version.to_string(),
        upload_session_id: Ulid::new().to_string(),
        owner_uid: "owner".into(),
        content_type: "application/octet-stream".into(),
        total_size_bytes: size,
        created_at: 1,
        pool_id: "pool-a".into(),
        pool_generation: 1,
        membership_epoch: 1,
        data_shards: 2,
        parity_shards: 1,
        parts: Vec::new(),
        is_delete_marker: false,
        stripe_size_bytes: 262_144,
    }
}

// Include directory/link identity and every file's exact bytes. In particular,
// an implementation cannot pass by deleting an empty unknown directory or by
// recreating an unchanged-looking committed file at a different inode.
type Snapshot = BTreeMap<PathBuf, (u64, u64, u32, Vec<u8>)>;

fn snapshot(root: &Path) -> Snapshot {
    let mut out = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let stat = fs::symlink_metadata(&path).expect("inspect snapshot entry");
        let bytes = if stat.file_type().is_symlink() {
            fs::read_link(&path)
                .expect("read fixture symlink")
                .as_os_str()
                .as_encoded_bytes()
                .to_vec()
        } else if stat.is_file() {
            fs::read(&path).expect("read snapshot bytes")
        } else {
            Vec::new()
        };
        out.insert(
            path.strip_prefix(root)
                .expect("relative fixture")
                .to_path_buf(),
            (stat.dev(), stat.ino(), stat.mode(), bytes),
        );
        if stat.is_dir() {
            stack.extend(
                fs::read_dir(path)
                    .expect("enumerate fixture")
                    .map(|entry| entry.expect("fixture entry").path()),
            );
        }
    }
    out
}

fn age_directories(root: &Path) {
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        if !fs::symlink_metadata(&path)
            .expect("inspect directory age")
            .is_dir()
        {
            continue;
        }
        stack.extend(
            fs::read_dir(&path)
                .expect("enumerate directories")
                .map(|entry| entry.expect("directory entry").path()),
        );
        File::open(&path)
            .expect("open directory")
            .set_times(FileTimes::new().set_modified(SystemTime::UNIX_EPOCH))
            .expect("age directory");
    }
}

#[tokio::test]
async fn cleanup_preserves_aged_fanout_history_trash_sessions_pins_and_unknown_layout() {
    let _guard = global_test_lock().lock().await;
    let fixture = Fixture::new();
    let fragment_meta = index_store::FragmentMeta {
        fragment_id: fixture.fragment.clone(),
        state: "active".into(),
        committed_bytes: 74,
        reserved_bytes: 37,
        trash_bytes: 0,
        created_at_unix: 1,
        updated_at_unix: 1,
        key_phc: Some("fixture-key".into()),
        nt_hash_hex: None,
    };
    write(
        &fixture.path().join(".fragment.ncdx"),
        &rmp_serde::to_vec(&fragment_meta).expect("encode fragment metadata"),
    );
    let manager = ObjectPathManager::new();
    let object = Ulid::new().to_string();
    let versions = [
        Ulid::from_parts(1, 1).to_string(),
        Ulid::from_parts(2, 1).to_string(),
    ];
    for (index, version) in versions.iter().enumerate() {
        let dir = manager.get_shard_dir_on_disk(&fixture.root, &fixture.fragment, &object, version);
        write(&dir.join("part_0_shard_0.shard"), &[index as u8; 37]);
        index_store::upsert_live_version(&metadata(&fixture.fragment, &object, version, 37))
            .expect("publish real immutable metadata");
    }

    let record = NcnxRecord::new(&fixture.fragment, &object, Ulid::new().to_string(), 1)
        .expect("NCNX record")
        .encode()
        .expect("encode NCNX");
    let store = NcnxStore::new("test-root", &fixture.root);
    store
        .create_live(&fixture.fragment, "docs/report.bin", &record)
        .expect("publish live NCNX");
    store
        .create_trash(
            &fixture.fragment,
            &TrashLocator {
                logical_key: "docs/old-report.bin".into(),
                deleted_at_unix_nanos: 1,
                delete_mutation_id: Ulid::new().to_string(),
            },
            &record,
        )
        .expect("publish recoverable trash NCNX");

    let session = UploadSessionManifest {
        fragment: fixture.fragment.clone(),
        object_key: Ulid::new().to_string(),
        packed_ext_u64: 0,
        version_id: Ulid::new().to_string(),
        session_id: Ulid::new().to_string(),
        total_size_bytes: 37,
        expected_parts: 1,
        part_size_bytes: 37,
        content_type: "application/octet-stream".into(),
        created_at: 1,
        pool_id: "pool-a".into(),
        pool_generation: 1,
        stripe_size_bytes: 262_144,
        membership_epoch: 1,
        data_shards: 2,
        parity_shards: 1,
    };
    index_store::open_session(&session, None).expect("publish active upload session");
    let pending_dir = manager.get_shard_dir_on_disk(
        &fixture.root,
        &fixture.fragment,
        &session.object_key,
        &session.version_id,
    );
    write(
        &pending_dir.join("part_0_shard_0.shard"),
        b"active uncommitted shard",
    );

    // The historical usage field comes from this node's recorded HEAD
    // inventory; it must not be advertised as complete cluster usage. Keep a
    // compatibility pointer to exercise the existing nonzero computation.
    symlink(
        format!("@o_{object}"),
        fixture.path().join("legacy-report.bin"),
    )
    .expect("publish compatibility pointer");
    let objects = fixture.path().join(".objects");
    write(
        &objects.join("future-layout/unknown-piece"),
        b"unknown ownership",
    );
    fs::create_dir_all(objects.join("unrecognized-empty-directory"))
        .expect("create unknown empty directory");
    let legacy_version = Ulid::new().to_string();
    write(
        &objects.join(legacy_version).join("legacy-piece"),
        b"unproven legacy ownership",
    );
    let outside = fixture.path().join("external-payload");
    write(&outside.join("keep"), b"symlink target bytes");
    symlink("../external-payload", objects.join("linked-directory"))
        .expect("create unknown directory symlink");
    write(
        &fixture.pin_path(),
        &serde_json::to_vec(&serde_json::json!({
            "id": "source",
            "fragment": fixture.fragment,
            "expires_at_unix": i64::MAX,
            "membership_epoch": 1,
            "binding_epoch": 1,
            "cluster_id": "cluster-a"
        }))
        .expect("encode retained source pin"),
    );
    assert!(
        crate::core::export::pins::fragment_is_pinned(&fixture.fragment)
            .await
            .expect("load persisted export retention pin")
    );
    age_directories(&objects);
    let fanout = ObjectPathManager::object_fanout_segments(&object);
    assert_eq!(
        fs::metadata(objects.join(&fanout[0]))
            .expect("inspect actual aged fanout")
            .modified()
            .expect("fanout mtime"),
        SystemTime::UNIX_EPOCH,
    );
    let before = snapshot(&fixture.path());
    let pin_before = fs::read(fixture.pin_path()).expect("read retained export pin");
    for _ in 0..2 {
        let response = fixture.inspect().await;
        assert_eq!(response["usage_bytes"], 37);
        assert_eq!(response["usage_scope"], "local_recorded_live_heads");
        assert_eq!(response["orphan_blob_dirs_removed"], 0);
        assert_eq!(response["orphan_reclamation_status"], "deferred");
        assert!(
            response["message"]
                .as_str()
                .unwrap()
                .contains("no blob directories were removed")
        );
        assert_eq!(snapshot(&fixture.path()), before);
        assert_eq!(
            fs::read(fixture.pin_path()).expect("retained export pin"),
            pin_before
        );
        assert!(
            crate::core::export::pins::fragment_is_pinned(&fixture.fragment)
                .await
                .expect("export remains pinned")
        );
        assert!(matches!(
            crate::core::export::pins::guard_purge(&fixture.fragment).await,
            Err(AppError::Conflict(_))
        ));
    }
    for version in &versions {
        let retained = index_store::read_live_version(&fixture.fragment, &object, version)
            .expect("read retained immutable metadata")
            .expect("historical version retained");
        assert_eq!(retained.version_id, *version);
    }
    assert!(
        index_store::load_session(&fixture.fragment, &session.version_id)
            .expect("load unchanged active upload")
            .is_some()
    );
}

#[tokio::test]
async fn cleanup_does_not_treat_empty_inventory_as_authority_to_delete_unknown_data() {
    let _guard = global_test_lock().lock().await;
    let fixture = Fixture::new();
    write(
        &fixture.path().join(".objects/ab/unknown/payload"),
        b"unproven data",
    );
    write(
        &fixture.path().join(".uploads/unknown/manifest.ncdx"),
        b"unknown upload format",
    );
    write(
        &fixture.path().join(".trash/unknown.ncnx"),
        b"unknown trash format",
    );
    age_directories(&fixture.path().join(".objects"));
    let before = snapshot(&fixture.path());
    let response = fixture.inspect().await;
    assert_eq!(response["usage_bytes"], 0);
    assert_eq!(response["orphan_reclamation_status"], "deferred");
    assert_eq!(response["orphan_blob_dirs_removed"], 0);
    assert_eq!(snapshot(&fixture.path()), before);
}

#[tokio::test]
async fn cleanup_requires_modify_permission_before_inspecting_storage() {
    let claims = Claims {
        fragment: "not-opened".into(),
        fragment_alias: None,
        read: true,
        write: true,
        modify: false,
        delete: true,
        exp: usize::MAX,
    };
    assert!(matches!(
        sync_fragment_metadata(claims).await,
        Err(AppError::Forbidden(_))
    ));
}
