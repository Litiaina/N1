//! Deterministic read/delete interleavings for transient upload metadata.

use super::sessions::{SessionFsBoundary, test_session_fs_hook};
use super::*;
use std::cell::Cell;
use std::rc::Rc;

struct Fixture {
    fragment: String,
    roots: Vec<PathBuf>,
}

impl Fixture {
    fn new() -> Self {
        Self {
            fragment: Ulid::new().to_string(),
            roots: super::tests::ensure_test_index_roots(),
        }
    }

    fn open(&self, logical_path: &str) -> (UploadSessionManifest, PendingUploadBinding) {
        let manifest = UploadSessionManifest {
            fragment: self.fragment.clone(),
            object_key: new_object_id(),
            version_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            total_size_bytes: 8,
            expected_parts: 2,
            part_size_bytes: 4,
            content_type: "application/octet-stream".into(),
            created_at: 1_777_905_100,
            stripe_size_bytes: 4,
            data_shards: 1,
            parity_shards: 1,
            placement_roots: self
                .roots
                .iter()
                .map(|root| root.to_string_lossy().into_owned())
                .collect(),
        };
        let binding = PendingUploadBinding {
            fragment: self.fragment.clone(),
            logical_path: logical_path.into(),
            physical_object_id: manifest.object_key.clone(),
            version_id: manifest.version_id.clone(),
            session_id: manifest.session_id.clone(),
            created_at_unix: manifest.created_at,
        };
        open_session(&manifest, None).expect("open session fixture");
        write_pending_binding(&binding).expect("persist pending binding");
        (manifest, binding)
    }

    fn dirs(&self, version: &str) -> Vec<PathBuf> {
        self.roots
            .iter()
            .map(|root| {
                fragment_root_on(root, &self.fragment)
                    .join(UPLOADS_DIR)
                    .join(version)
            })
            .collect()
    }

    fn receipt(&self, manifest: &UploadSessionManifest, part: u64) -> SessionPartReceipt {
        let receipt = SessionPartReceipt {
            part_index: part,
            size_bytes: 4,
            shard_size_bytes: 4,
            created_at: manifest.created_at,
            checksum: Some(0x1234_0000 + part as u32),
            shard_checksums: vec![0x2345_0000 + part as u32, 0x3456_0000 + part as u32],
        };
        commit_session_part_receipt(&self.fragment, &manifest.version_id, &receipt)
            .expect("persist acknowledged receipt");
        receipt
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for root in &self.roots {
            let _ = fs::remove_dir_all(fragment_root_on(root, &self.fragment));
        }
    }
}

fn assert_binding(actual: &PendingUploadBinding, expected: &PendingUploadBinding) {
    assert_eq!(encode(actual).unwrap(), encode(expected).unwrap());
}

fn assert_manifest(actual: &UploadSessionManifest, expected: &UploadSessionManifest) {
    assert_eq!(encode(actual).unwrap(), encode(expected).unwrap());
}

#[tokio::test]
async fn reusable_upload_lookup_tolerates_an_unrelated_retiring_session_and_keeps_identity() {
    let _lock = crate::test_support::global_test_lock().lock().await;
    let fixture = Fixture::new();
    let (survivor, _) = fixture.open("existing/resume.bin");
    let receipt = fixture.receipt(&survivor, 0);
    let (retiring, _) = fixture.open("other/finished.bin");
    let fragment = fixture.fragment.clone();
    let retiring_version = retiring.version_id.clone();
    let removed = Rc::new(Cell::new(false));
    let hook_removed = removed.clone();
    let _hook = test_session_fs_hook(move |boundary, path| {
        if boundary == SessionFsBoundary::BindingRead
            && path
                .parent()
                .and_then(Path::file_name)
                .is_some_and(|name| name == retiring_version.as_str())
            && !hook_removed.replace(true)
        {
            assert!(path.is_file());
            assert!(remove_session(&fragment, &retiring_version).unwrap());
        }
    });
    let params = crate::core::disk_manager::InitiateUploadParams {
        content_type: Some(survivor.content_type.clone()),
        total_size_bytes: survivor.total_size_bytes,
        expected_parts: survivor.expected_parts,
        part_size_bytes: Some(survivor.part_size_bytes),
    };

    assert!(
        crate::routes::storage_routes::find_reusable_pending_upload(
            &fixture.fragment,
            "new/unrelated.bin",
            &params,
        )
        .await
        .expect("ordinary retirement cannot reject initialization of another key")
        .is_none()
    );
    assert!(
        removed.get(),
        "the real reuse-lookup path exercised the race"
    );
    for _ in 0..2 {
        let response = crate::routes::storage_routes::find_reusable_pending_upload(
            &fixture.fragment,
            "existing/resume.bin",
            &params,
        )
        .await
        .unwrap()
        .expect("an identical retry must find the existing durable session");
        assert_eq!(response.session_id, survivor.session_id);
        assert_eq!(response.version_id, survivor.version_id);
    }
    let mismatched = crate::core::disk_manager::InitiateUploadParams {
        total_size_bytes: params.total_size_bytes + 1,
        ..params
    };
    assert!(
        crate::routes::storage_routes::find_reusable_pending_upload(
            &fixture.fragment,
            "existing/resume.bin",
            &mismatched,
        )
        .await
        .unwrap()
        .is_none()
    );
    let actual = load_session(&fixture.fragment, &survivor.version_id)
        .unwrap()
        .unwrap();
    assert_manifest(&actual.manifest, &survivor);
    assert_eq!(actual.part_receipts.get(&0), Some(&receipt));
}

#[test]
fn pending_binding_removed_between_discovery_and_read_does_not_poison_other_uploads() {
    let _lock = crate::test_support::global_test_lock().blocking_lock();
    let fixture = Fixture::new();
    let (survivor, survivor_binding) = fixture.open("other/keep.bin");
    let survivor_receipt = fixture.receipt(&survivor, 0);
    let (retiring, _) = fixture.open("finished/retire.bin");
    let retiring_version = retiring.version_id.clone();
    let fragment = fixture.fragment.clone();
    let removed = Rc::new(Cell::new(false));
    let hook_removed = removed.clone();
    let _hook = test_session_fs_hook(move |boundary, path| {
        if boundary == SessionFsBoundary::BindingRead
            && path
                .parent()
                .and_then(Path::file_name)
                .is_some_and(|name| name == retiring_version.as_str())
            && !hook_removed.replace(true)
        {
            assert!(path.is_file(), "binding really existed before retirement");
            assert!(remove_session(&fragment, &retiring_version).unwrap());
            assert!(!path.exists(), "read must race an actual removed directory");
        }
    });

    let bindings = list_pending_bindings(&fixture.fragment)
        .expect("retiring another key is not a fatal fragment-wide lookup failure");
    assert!(
        removed.get(),
        "the controlled read/remove interleaving executed"
    );
    assert_eq!(bindings.len(), 1);
    assert_binding(&bindings[0], &survivor_binding);
    let resumed = load_session(&fixture.fragment, &survivor.version_id)
        .unwrap()
        .expect("unrelated durable session remains resumable");
    assert_manifest(&resumed.manifest, &survivor);
    assert_eq!(resumed.part_receipts.len(), 1);
    assert_eq!(resumed.part_receipts.get(&0), Some(&survivor_receipt));
    assert!(
        read_pending_binding(&fixture.fragment, &retiring.version_id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn pending_binding_missing_first_replica_uses_identical_surviving_replica() {
    let _lock = crate::test_support::global_test_lock().blocking_lock();
    let fixture = Fixture::new();
    let (manifest, binding) = fixture.open("resume/same-object.bin");
    let receipt = fixture.receipt(&manifest, 0);
    let first = fixture.dirs(&manifest.version_id)[0].join(BINDING_FILE);
    let removed = Rc::new(Cell::new(false));
    let hook_removed = removed.clone();
    let _hook = test_session_fs_hook(move |boundary, path| {
        if boundary == SessionFsBoundary::BindingRead && path == first {
            assert!(path.is_file());
            fs::remove_file(path).unwrap();
            hook_removed.set(true);
        }
    });

    let actual = read_pending_binding(&fixture.fragment, &manifest.version_id)
        .unwrap()
        .expect("the second binding replica survives");
    assert!(removed.get());
    assert_binding(&actual, &binding);
    let resumed = load_session(&fixture.fragment, &manifest.version_id)
        .unwrap()
        .unwrap();
    assert_manifest(&resumed.manifest, &manifest);
    assert_eq!(resumed.part_receipts.get(&0), Some(&receipt));
}

#[test]
fn manifest_removed_between_discovery_and_read_is_an_absent_session() {
    let _lock = crate::test_support::global_test_lock().blocking_lock();
    let fixture = Fixture::new();
    let (manifest, _) = fixture.open("retiring/manifest.bin");
    let fragment = fixture.fragment.clone();
    let version = manifest.version_id.clone();
    let removed = Rc::new(Cell::new(false));
    let hook_removed = removed.clone();
    let _hook = test_session_fs_hook(move |boundary, path| {
        if boundary == SessionFsBoundary::ManifestRead && !hook_removed.replace(true) {
            assert!(path.is_file());
            assert!(remove_session(&fragment, &version).unwrap());
        }
    });

    assert!(
        load_session(&fixture.fragment, &manifest.version_id)
            .unwrap()
            .is_none()
    );
    assert!(removed.get());
}

#[test]
fn removed_receipt_directory_does_not_hide_surviving_resume_truth() {
    let _lock = crate::test_support::global_test_lock().blocking_lock();
    let fixture = Fixture::new();
    let (manifest, binding) = fixture.open("resume/receipts.bin");
    let receipts = [fixture.receipt(&manifest, 0), fixture.receipt(&manifest, 1)];
    let first = fixture.dirs(&manifest.version_id)[0].clone();
    let removed = Rc::new(Cell::new(false));
    let hook_removed = removed.clone();
    let _hook = test_session_fs_hook(move |boundary, path| {
        if boundary == SessionFsBoundary::SessionEnumerate && path == first {
            assert!(path.is_dir());
            fs::remove_dir_all(path).unwrap();
            hook_removed.set(true);
        }
    });

    let actual = load_session(&fixture.fragment, &manifest.version_id)
        .unwrap()
        .unwrap();
    assert!(removed.get());
    assert_manifest(&actual.manifest, &manifest);
    assert_eq!(actual.part_receipts.len(), receipts.len());
    for receipt in receipts {
        assert_eq!(
            actual.part_receipts.get(&receipt.part_index),
            Some(&receipt)
        );
    }
    assert_binding(
        &read_pending_binding(&fixture.fragment, &manifest.version_id)
            .unwrap()
            .unwrap(),
        &binding,
    );
}

#[test]
fn all_receipt_directories_retired_after_manifest_read_do_not_return_a_phantom_session() {
    let _lock = crate::test_support::global_test_lock().blocking_lock();
    let fixture = Fixture::new();
    let (manifest, _) = fixture.open("cancel/all-copies.bin");
    fixture.receipt(&manifest, 0);
    let fragment = fixture.fragment.clone();
    let version = manifest.version_id.clone();
    let removed = Rc::new(Cell::new(false));
    let hook_removed = removed.clone();
    let _hook = test_session_fs_hook(move |boundary, path| {
        if boundary == SessionFsBoundary::SessionEnumerate && !hook_removed.replace(true) {
            assert!(
                path.is_dir(),
                "a valid session was discovered before retirement"
            );
            assert!(remove_session(&fragment, &version).unwrap());
        }
    });

    assert!(
        load_session(&fixture.fragment, &manifest.version_id)
            .unwrap()
            .is_none()
    );
    assert!(removed.get());
    assert!(
        fixture
            .dirs(&manifest.version_id)
            .iter()
            .all(|dir| !dir.exists())
    );
    assert!(
        read_pending_binding(&fixture.fragment, &manifest.version_id)
            .unwrap()
            .is_none()
    );
    assert!(list_pending_bindings(&fixture.fragment).unwrap().is_empty());
}

#[test]
fn pending_session_listing_survives_upload_directory_retirement() {
    let _lock = crate::test_support::global_test_lock().blocking_lock();
    let fixture = Fixture::new();
    let (manifest, binding) = fixture.open("keep/from-other-member.bin");
    let first_uploads = fixture.dirs(&manifest.version_id)[0]
        .parent()
        .unwrap()
        .to_path_buf();
    let removed = Rc::new(Cell::new(false));
    let hook_removed = removed.clone();
    let _hook = test_session_fs_hook(move |boundary, path| {
        if boundary == SessionFsBoundary::UploadsEnumerate && path == first_uploads {
            assert!(path.is_dir());
            fs::remove_dir_all(path).unwrap();
            hook_removed.set(true);
        }
    });

    let actual = list_pending_bindings(&fixture.fragment).unwrap();
    assert!(removed.get());
    assert_eq!(actual.len(), 1);
    assert_binding(&actual[0], &binding);
}

#[test]
fn session_removal_is_idempotent_when_another_remover_wins() {
    let _lock = crate::test_support::global_test_lock().blocking_lock();
    let fixture = Fixture::new();
    let (manifest, _) = fixture.open("cancel/overlap.bin");
    fixture.receipt(&manifest, 0);
    let removed = Rc::new(Cell::new(0));
    let hook_removed = removed.clone();
    let _hook = test_session_fs_hook(move |boundary, path| {
        if boundary == SessionFsBoundary::SessionRemove && path.is_dir() {
            fs::remove_dir_all(path).unwrap();
            hook_removed.set(hook_removed.get() + 1);
        }
    });

    let _ = remove_session(&fixture.fragment, &manifest.version_id)
        .expect("a concurrent successful delete must not turn retirement into an error");
    assert_eq!(removed.get(), fixture.roots.len());
    assert!(!remove_session(&fixture.fragment, &manifest.version_id).unwrap());
    assert!(
        load_session(&fixture.fragment, &manifest.version_id)
            .unwrap()
            .is_none()
    );
    assert!(
        read_pending_binding(&fixture.fragment, &manifest.version_id)
            .unwrap()
            .is_none()
    );
    assert!(
        fixture
            .dirs(&manifest.version_id)
            .iter()
            .all(|dir| !dir.exists())
    );
}

#[test]
fn pending_binding_real_read_error_is_not_silenced_as_a_retired_session() {
    let _lock = crate::test_support::global_test_lock().blocking_lock();
    let fixture = Fixture::new();
    let (manifest, _) = fixture.open("errors/not-a-file.bin");
    let first = fixture.dirs(&manifest.version_id)[0].join(BINDING_FILE);
    let changed = Rc::new(Cell::new(false));
    let hook_changed = changed.clone();
    let _hook = test_session_fs_hook(move |boundary, path| {
        if boundary == SessionFsBoundary::BindingRead && path == first {
            fs::remove_file(path).unwrap();
            fs::create_dir(path).unwrap();
            hook_changed.set(true);
        }
    });

    let error = read_pending_binding(&fixture.fragment, &manifest.version_id).unwrap_err();
    assert!(changed.get());
    match error {
        AppError::Io(error, context) => {
            assert_eq!(error.raw_os_error(), Some(libc::EISDIR));
            assert_eq!(context, "read pending upload binding");
        }
        other => panic!("expected the actual read error, not absence: {other}"),
    }
}

#[test]
fn corrupt_pending_binding_is_not_silenced_by_a_healthy_other_copy() {
    let _lock = crate::test_support::global_test_lock().blocking_lock();
    let fixture = Fixture::new();
    let (manifest, _) = fixture.open("errors/corrupt-binding.bin");
    let first = fixture.dirs(&manifest.version_id)[0].join(BINDING_FILE);
    fs::write(first, [0xc1]).unwrap();
    assert!(matches!(
        read_pending_binding(&fixture.fragment, &manifest.version_id),
        Err(AppError::InternalServerError(_))
    ));
}

#[test]
fn conflicting_manifest_copies_remain_a_conflict() {
    let _lock = crate::test_support::global_test_lock().blocking_lock();
    let fixture = Fixture::new();
    let (manifest, _) = fixture.open("errors/conflicting-manifest.bin");
    let path = fixture.dirs(&manifest.version_id)[1].join(MANIFEST_FILE);
    let mut conflicting: SessionRow = decode(&fs::read(&path).unwrap()).unwrap();
    conflicting.manifest.session_id = Ulid::new().to_string();
    fs::write(path, encode(&conflicting).unwrap()).unwrap();
    assert!(matches!(
        load_session(&fixture.fragment, &manifest.version_id),
        Err(AppError::Conflict(_))
    ));
}

#[test]
fn missing_and_corrupt_manifest_copies_do_not_become_an_empty_resume_state() {
    let _lock = crate::test_support::global_test_lock().blocking_lock();
    let fixture = Fixture::new();
    let (manifest, _) = fixture.open("errors/only-corrupt-manifest.bin");
    let dirs = fixture.dirs(&manifest.version_id);
    fs::write(dirs[0].join(MANIFEST_FILE), [0xc1]).unwrap();
    fs::remove_file(dirs[1].join(MANIFEST_FILE)).unwrap();
    assert!(matches!(
        load_session(&fixture.fragment, &manifest.version_id),
        Err(AppError::InternalServerError(_))
    ));
}

#[test]
fn corrupt_manifest_copy_can_use_existing_identical_session_receipts() {
    let _lock = crate::test_support::global_test_lock().blocking_lock();
    let fixture = Fixture::new();
    let (manifest, _) = fixture.open("resume/corrupt-first-manifest.bin");
    let receipt = fixture.receipt(&manifest, 0);
    fs::write(
        fixture.dirs(&manifest.version_id)[0].join(MANIFEST_FILE),
        [0xc1],
    )
    .unwrap();
    let actual = load_session(&fixture.fragment, &manifest.version_id)
        .unwrap()
        .unwrap();
    assert_manifest(&actual.manifest, &manifest);
    assert_eq!(actual.part_receipts.get(&0), Some(&receipt));
}

#[test]
fn part_metadata_removed_after_enumeration_uses_surviving_copy() {
    let _lock = crate::test_support::global_test_lock().blocking_lock();
    let fixture = Fixture::new();
    let (manifest, _) = fixture.open("resume/part-metadata.bin");
    let part = PartMetadata {
        part_index: 0,
        size_bytes: 4,
        created_at: manifest.created_at,
        shards: Vec::new(),
        checksum: Some("01234567".into()),
    };
    commit_session_part(&fixture.fragment, &manifest.version_id, &part).unwrap();
    let first = fixture.dirs(&manifest.version_id)[0]
        .join(format!("{PART_META_PREFIX}{:020}.ndx", part.part_index));
    let removed = Rc::new(Cell::new(false));
    let hook_removed = removed.clone();
    let _hook = test_session_fs_hook(move |boundary, path| {
        if boundary == SessionFsBoundary::PartRead && path == first {
            assert!(path.is_file());
            fs::remove_file(path).unwrap();
            hook_removed.set(true);
        }
    });

    let actual = load_session(&fixture.fragment, &manifest.version_id)
        .unwrap()
        .unwrap();
    assert!(removed.get());
    assert_manifest(&actual.manifest, &manifest);
    assert_eq!(actual.parts.len(), 1);
    assert_eq!(
        encode(actual.parts.get(&0).unwrap()).unwrap(),
        encode(&part).unwrap()
    );
}

#[test]
fn receipt_writer_does_not_recreate_a_concurrently_retired_session() {
    let _lock = crate::test_support::global_test_lock().blocking_lock();
    let fixture = Fixture::new();
    let (survivor, survivor_binding) = fixture.open("keep/acknowledged-progress.bin");
    let survivor_receipt = fixture.receipt(&survivor, 0);
    let (retiring, _) = fixture.open("cancel/late-receipt.bin");
    let late_receipt = fixture.receipt(&retiring, 0);
    let fragment = fixture.fragment.clone();
    let version = retiring.version_id.clone();
    let removed = Rc::new(Cell::new(false));
    let hook_removed = removed.clone();
    let _hook = test_session_fs_hook(move |boundary, path| {
        if boundary == SessionFsBoundary::ReceiptWrite
            && path
                .file_name()
                .is_some_and(|name| name == version.as_str())
            && !hook_removed.replace(true)
        {
            assert!(path.is_dir());
            assert!(remove_session(&fragment, &version).unwrap());
        }
    });

    let error = commit_session_part_receipt(&fixture.fragment, &retiring.version_id, &late_receipt)
        .expect_err("a late receipt cannot recreate a retired session directory");
    match error {
        AppError::Io(error, _) => assert_eq!(error.kind(), std::io::ErrorKind::NotFound),
        other => panic!("expected the actual vanished-directory write error: {other}"),
    }
    assert!(removed.get());
    assert!(
        fixture
            .dirs(&retiring.version_id)
            .iter()
            .all(|dir| !dir.exists())
    );
    assert!(
        load_session(&fixture.fragment, &retiring.version_id)
            .unwrap()
            .is_none()
    );
    let resumed = load_session(&fixture.fragment, &survivor.version_id)
        .unwrap()
        .unwrap();
    assert_manifest(&resumed.manifest, &survivor);
    assert_eq!(resumed.part_receipts.get(&0), Some(&survivor_receipt));
    assert_binding(
        &read_pending_binding(&fixture.fragment, &survivor.version_id)
            .unwrap()
            .unwrap(),
        &survivor_binding,
    );
}
