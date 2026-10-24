use super::*;
use crate::core::disk_manager::STORAGE_DISKS;
use crate::core::fragment_identity::physical_fragment_root_on;
use crate::core::index_store;
use crate::test_support::global_test_lock;
use std::fs;
use std::os::unix::fs::{MetadataExt, symlink};

struct Fixture {
    root: PathBuf,
    manifest: UploadSessionManifest,
}

impl Fixture {
    fn new() -> Self {
        let roots = STORAGE_DISKS.get_or_init(|| {
            let root =
                std::env::temp_dir().join(format!("n2-cancellation-tests-{}", std::process::id()));
            fs::create_dir_all(&root).expect("create test storage root");
            vec![root.to_string_lossy().into_owned()]
        });
        let root = PathBuf::from(roots.first().expect("test storage root"));
        let manifest = UploadSessionManifest {
            fragment: Ulid::new().to_string(),
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
        };
        index_store::open_session(&manifest, None).expect("open durable session");
        Self { root, manifest }
    }

    fn claims(&self) -> Claims {
        Claims {
            fragment: self.manifest.fragment.clone(),
            fragment_alias: None,
            read: true,
            write: true,
            modify: true,
            delete: true,
            exp: usize::MAX,
        }
    }

    fn payload(&self) -> PathBuf {
        ObjectPathManager::new().get_shard_dir_on_disk(
            &self.root,
            &self.manifest.fragment,
            &self.manifest.object_key,
            &self.manifest.version_id,
        )
    }

    fn metadata(&self) -> ObjectMetadata {
        ObjectMetadata {
            object_key: self.manifest.object_key.clone(),
            packed_ext_u64: 0,
            fragment: self.manifest.fragment.clone(),
            version_id: self.manifest.version_id.clone(),
            upload_session_id: self.manifest.session_id.clone(),
            owner_uid: self.manifest.fragment.clone(),
            content_type: self.manifest.content_type.clone(),
            total_size_bytes: self.manifest.total_size_bytes,
            created_at: 1,
            pool_id: self.manifest.pool_id.clone(),
            pool_generation: 1,
            membership_epoch: 1,
            data_shards: 1,
            parity_shards: 1,
            parts: Vec::new(),
            is_delete_marker: false,
            stripe_size_bytes: 4096,
        }
    }

    async fn cancel(&self) -> Result<(), AppError> {
        cancel_upload_session_direct(
            self.claims(),
            self.manifest.fragment.clone(),
            self.manifest.version_id.clone(),
            self.manifest.object_key.clone(),
        )
        .await
    }

    fn session_is_retained(&self) -> bool {
        index_store::load_session(&self.manifest.fragment, &self.manifest.version_id)
            .expect("load retained manifest")
            .is_some()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for root in ObjectPathManager::new().get_storage_roots() {
            let _ = fs::remove_dir_all(physical_fragment_root_on(&root, &self.manifest.fragment));
        }
    }
}

fn write(path: &Path, bytes: &[u8]) {
    fs::create_dir_all(path.parent().expect("parent")).expect("create fixture directory");
    fs::write(path, bytes).expect("write fixture payload");
}

#[test]
fn cancellation_propagates_uncertain_lookup_and_rejects_distributed_absence_without_fence() {
    assert!(cancel_lookup_allows_cleanup(Err(AppError::NotFound), false).is_ok());
    assert!(matches!(
        cancel_lookup_allows_cleanup(Err(AppError::NotFound), true),
        Err(AppError::ServiceUnavailable(_))
    ));
    for distributed in [false, true] {
        assert!(matches!(
            cancel_lookup_allows_cleanup(
                Err(AppError::ServiceUnavailable("unproven holders".into())),
                distributed,
            ),
            Err(AppError::ServiceUnavailable(_))
        ));
        assert!(matches!(
            cancel_lookup_allows_cleanup(Err(AppError::Timeout("metadata".into())), distributed),
            Err(AppError::Timeout(_))
        ));
        assert!(matches!(
            cancel_lookup_allows_cleanup(
                Err(AppError::InternalServerError("corrupt metadata".into())),
                distributed,
            ),
            Err(AppError::InternalServerError(_))
        ));
    }
}

#[tokio::test]
async fn local_cancellation_removes_only_exact_session_version_and_keeps_history() {
    let _guard = global_test_lock().lock().await;
    let fixture = Fixture::new();
    assert!(crate::distribution::membership::try_active_cluster_runtime().is_none());
    let target = fixture.payload();
    write(&target.join("part_0_shard_0.shard"), b"cancelled payload");
    write(
        &target.join("part_0_shard_0.shard.tmp.attempt"),
        b"temporary bytes",
    );
    let manager = ObjectPathManager::new();
    let history_version = Ulid::new().to_string();
    let history = manager.get_shard_dir_on_disk(
        &fixture.root,
        &fixture.manifest.fragment,
        &fixture.manifest.object_key,
        &history_version,
    );
    write(&history.join("keep.shard"), b"committed historical bytes");
    let mut history_metadata = fixture.metadata();
    history_metadata.version_id = history_version;
    index_store::upsert_live_version(&history_metadata).expect("publish historical metadata");
    let other_object = Ulid::new().to_string();
    let other_same_version = manager.get_shard_dir_on_disk(
        &fixture.root,
        &fixture.manifest.fragment,
        &other_object,
        &fixture.manifest.version_id,
    );
    write(
        &other_same_version.join("keep.shard"),
        b"other object, same version name",
    );
    let history_inode = fs::metadata(history.join("keep.shard"))
        .expect("history stat")
        .ino();
    let other_inode = fs::metadata(other_same_version.join("keep.shard"))
        .expect("other object stat")
        .ino();

    fixture
        .cancel()
        .await
        .expect("cancel isolated local session");
    assert!(!target.exists());
    assert!(!fixture.session_is_retained());
    assert_eq!(
        fs::read(history.join("keep.shard")).expect("history retained"),
        b"committed historical bytes"
    );
    assert_eq!(
        fs::metadata(history.join("keep.shard"))
            .expect("history inode")
            .ino(),
        history_inode
    );
    assert_eq!(
        fs::read(other_same_version.join("keep.shard")).expect("other retained"),
        b"other object, same version name"
    );
    assert_eq!(
        fs::metadata(other_same_version.join("keep.shard"))
            .expect("other inode")
            .ino(),
        other_inode
    );
    assert!(
        index_store::read_live_version(
            &fixture.manifest.fragment,
            &fixture.manifest.object_key,
            &history_metadata.version_id,
        )
        .expect("read unchanged historical metadata")
        .is_some()
    );
}

#[tokio::test]
async fn cancellation_preserves_committed_version_and_corrupt_metadata_uncertainty() {
    let _guard = global_test_lock().lock().await;
    let fixture = Fixture::new();
    let target = fixture.payload();
    let shard = target.join("part_0_shard_0.shard");
    write(&shard, b"committed shard");
    index_store::upsert_live_version(&fixture.metadata()).expect("publish committed NCDX");
    let inode = fs::metadata(&shard).expect("shard inode").ino();
    assert!(matches!(fixture.cancel().await, Err(AppError::Conflict(_))));
    assert!(fixture.session_is_retained());
    assert_eq!(
        fs::read(&shard).expect("committed shard retained"),
        b"committed shard"
    );
    assert_eq!(fs::metadata(&shard).expect("same shard inode").ino(), inode);

    let object_dir = target.parent().expect("object directory");
    let ncdx = fs::read_dir(object_dir)
        .expect("enumerate exact object metadata")
        .map(|entry| entry.expect("metadata entry").path())
        .find(|path| path.extension().is_some_and(|ext| ext == "ncdx"))
        .expect("committed summary path");
    fs::write(ncdx, b"corrupt NCDX").expect("inject metadata corruption");
    assert!(matches!(
        fixture.cancel().await,
        Err(AppError::InternalServerError(_))
    ));
    assert!(fixture.session_is_retained());
    assert_eq!(
        fs::read(&shard).expect("uncertain shard retained"),
        b"committed shard"
    );
    assert_eq!(
        fs::metadata(&shard).expect("uncertain shard inode").ino(),
        inode
    );
}

#[tokio::test]
async fn failed_exact_cleanup_keeps_session_and_does_not_follow_symlinks() {
    let _guard = global_test_lock().lock().await;
    let fixture = Fixture::new();
    let target = fixture.payload();
    fs::create_dir_all(target.parent().expect("object parent")).expect("create object directory");
    let outside =
        physical_fragment_root_on(&fixture.root, &fixture.manifest.fragment).join("outside");
    write(&outside.join("keep"), b"outside bytes");
    symlink(&outside, &target).expect("replace payload directory with symlink");
    let before = fs::metadata(outside.join("keep"))
        .expect("outside inode")
        .ino();
    assert!(matches!(fixture.cancel().await, Err(AppError::Conflict(_))));
    assert!(fixture.session_is_retained());
    assert_eq!(
        fs::read(outside.join("keep")).expect("outside retained"),
        b"outside bytes"
    );
    assert_eq!(
        fs::metadata(outside.join("keep"))
            .expect("outside inode retained")
            .ino(),
        before
    );
    assert!(
        fs::symlink_metadata(target)
            .expect("symlink retained")
            .file_type()
            .is_symlink()
    );
}

#[tokio::test]
async fn missing_session_never_uses_a_request_hint_as_payload_deletion_authority() {
    let _guard = global_test_lock().lock().await;
    let fixture = Fixture::new();
    let shard = fixture.payload().join("keep.shard");
    write(&shard, b"unknown ownership");
    index_store::remove_session(&fixture.manifest.fragment, &fixture.manifest.version_id)
        .expect("simulate lost session ownership evidence");
    assert!(matches!(
        fixture.cancel().await,
        Err(AppError::BadRequest(_))
    ));
    assert_eq!(
        fs::read(shard).expect("unknown payload retained"),
        b"unknown ownership"
    );
}

#[test]
fn exact_cleanup_rejects_non_component_and_non_ulid_payload_identities() {
    let version = Ulid::new().to_string();
    assert!(validate_cancel_payload_identity("fragment", "object", &version).is_ok());
    assert!(validate_cancel_payload_identity("@f_fragment", "object", &version).is_ok());
    for fragment in [
        "../fragment",
        "fragment/child",
        "fragment\\child",
        "",
        "fragment\0",
    ] {
        assert!(validate_cancel_payload_identity(fragment, "object", &version).is_err());
    }
    for object in ["../object", "object/child", "@o_object", "", "object\0"] {
        assert!(validate_cancel_payload_identity("fragment", object, &version).is_err());
    }
    for version in ["../version", "version/child", "", ".", "not-a-version"] {
        assert!(validate_cancel_payload_identity("fragment", "object", version).is_err());
    }
}
