use super::{
    AlignedBuffer, DirectWriter, STANDARD_PART_COMPLETE, STANDARD_PART_EMPTY,
    STANDARD_SESSION_OPEN, StandardPartClaimResult, StandardPartRecord, StandardPartSlot,
    StandardUploadSession, UPLOAD_STRICT_DURABILITY_DEFAULT_ATOMIC, UploadDurabilityMode,
    UploadSessionManifest, acquire_upload_io_guard_with_timeout,
    choose_upload_stripe_size_for_len_with_ceiling, claim_standard_part,
    parse_upload_durability_mode, shard_specs_for_part, target_stripe_size_for_metadata,
};
use crate::core::disk_manager::{
    INDEX_STORE_DISKS, ObjectMetadata, ObjectPathManager, PartMetadata, UploadPartQuery,
};
use std::path::Path;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicUsize, Ordering};
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn strict_shard_publication_propagates_directory_sync_failure() {
    let base = std::env::temp_dir().join(format!("strict-sync-{}", ulid::Ulid::new()));
    std::fs::create_dir(&base).unwrap();
    let invalid = base.join("not-a-directory");
    std::fs::write(&invalid, b"unchanged").unwrap();
    assert!(
        super::data_plane::sync_published_directories(&[base.clone(), invalid.clone()])
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&invalid).unwrap(), b"unchanged");
    super::data_plane::sync_published_directories(std::slice::from_ref(&base))
        .await
        .unwrap();
    std::fs::remove_dir_all(base).unwrap();
}

#[test]
fn retry_cannot_claim_strict_durability_for_a_relaxed_part() {
    let session = standard_test_session(1);
    session.parts[0]
        .record
        .set(StandardPartRecord {
            strict_durability: false,
            size_bytes: 0,
            created_at: 1,
            checksum: None,
            shard_checksums: Box::new([]),
        })
        .unwrap();
    assert!(super::sessions::check_standard_retry_durability(&session, 0, true).is_err());
    assert!(super::sessions::check_standard_retry_durability(&session, 0, false).is_ok());
}

#[test]
fn standard_finalize_carries_effective_durability_to_namespace_publication() {
    for flags in [[true, true], [true, false], [false, false]] {
        let session = standard_test_session(2);
        assert!(!super::sessions::standard_session_uses_strict_durability(
            &session
        ));
        for (slot, strict) in session.parts.iter().zip(flags) {
            slot.record
                .set(StandardPartRecord {
                    strict_durability: strict,
                    size_bytes: 0,
                    created_at: 1,
                    checksum: None,
                    shard_checksums: Box::new([]),
                })
                .unwrap();
        }
        assert_eq!(
            super::sessions::standard_session_uses_strict_durability(&session),
            flags.into_iter().all(|flag| flag)
        );
    }
}

fn direct_writer_with_failed_task(errno: i32) -> DirectWriter {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open("/dev/null")
        .expect("open /dev/null");
    let mut writer = DirectWriter::new(file);
    writer.write_task = Some(tokio::spawn(async move {
        tokio::task::yield_now().await;
        Err::<AlignedBuffer, _>(std::io::Error::from_raw_os_error(errno))
    }));
    writer
}

fn standard_test_session(part_count: usize) -> StandardUploadSession {
    // These tests must also work when selected alone, before other modules
    // initialize the process-wide storage settings.
    let base = std::env::temp_dir().join(format!("n1-upload-tests-{}", std::process::id()));
    INDEX_STORE_DISKS.get_or_init(|| vec![base.join("index").display().to_string()]);
    crate::core::disk_manager::BLOB_STORE_DISKS
        .get_or_init(|| vec![base.join("blob").display().to_string()]);
    StandardUploadSession {
        manifest: UploadSessionManifest {
            fragment: "01KFRAGMENT".to_string(),
            object_key: "01KOBJECT".to_string(),
            version_id: "01KVERSION".to_string(),
            session_id: "01KSESSION".to_string(),
            total_size_bytes: part_count as u64 * 4096,
            expected_parts: part_count as u64,
            part_size_bytes: 4096,
            content_type: "application/octet-stream".to_string(),
            created_at: 1,
            stripe_size_bytes: 4096,
            data_shards: 1,
            parity_shards: 1,
            placement_roots: vec!["/tmp/a".to_string(), "/tmp/b".to_string()],
        },
        logical_path: "test.bin".to_string(),
        lifecycle: AtomicU8::new(STANDARD_SESSION_OPEN),
        in_flight_parts: AtomicUsize::new(0),
        last_activity_at: AtomicI64::new(1),
        directories_ready: tokio::sync::OnceCell::new(),
        parts: (0..part_count)
            .map(|_| StandardPartSlot::new())
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    }
}

#[test]
fn gc_protects_standard_upload_until_namespace_publication_completes() {
    let mut session = standard_test_session(1);
    session.manifest.fragment = format!("gc-session-{}", ulid::Ulid::new());
    session
        .last_activity_at
        .store(chrono::Utc::now().timestamp(), Ordering::Release);
    session
        .lifecycle
        .store(super::STANDARD_SESSION_FINALIZED, Ordering::Release);
    let fragment = session.manifest.fragment.clone();
    let version = session.manifest.version_id.clone();
    let key = super::standard_session_key(&fragment, &version);
    super::standard_registry_write()
        .unwrap()
        .insert(key, std::sync::Arc::new(session));
    let protected = super::sessions::active_standard_upload_versions_at(
        &fragment,
        chrono::Utc::now().timestamp(),
        3600,
    )
    .unwrap();
    // Clean up even if the regression assertion fails, for test isolation.
    super::complete_standard_upload(&fragment, &version).unwrap();
    assert!(protected.contains(&version));
    assert!(
        !super::sessions::active_standard_upload_versions_at(
            &fragment,
            chrono::Utc::now().timestamp(),
            3600
        )
        .unwrap()
        .contains(&version)
    );
}

#[test]
fn upload_durability_default_follows_runtime_config_or_batched_when_unset() {
    let q = UploadPartQuery {
        checksum: None,
        durability: None,
        strict_durability: None,
        io_gate: None,
    };
    let expected = if UPLOAD_STRICT_DURABILITY_DEFAULT_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(false)
    {
        UploadDurabilityMode::Strict
    } else {
        UploadDurabilityMode::Batched
    };
    assert_eq!(parse_upload_durability_mode(&q), expected);
}

#[test]
fn idle_standard_session_blocks_ttl_object_reclamation_until_removed() {
    use crate::core::disk_manager::BLOB_STORE_DISKS;
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let base = std::env::temp_dir().join(format!("ttl-standard-{}", ulid::Ulid::new()));
    let blobs = BLOB_STORE_DISKS.get_or_init(|| {
        (0..2)
            .map(|i| base.join(format!("blob-{i}")).display().to_string())
            .collect()
    });
    let indexes = INDEX_STORE_DISKS.get_or_init(|| {
        (0..2)
            .map(|i| base.join(format!("index-{i}")).display().to_string())
            .collect()
    });
    for root in blobs.iter().chain(indexes) {
        std::fs::create_dir_all(root).unwrap();
    }
    let mut session = standard_test_session(1);
    session.manifest.fragment = format!("idle-standard-{}", ulid::Ulid::new());
    let fragment = session.manifest.fragment.clone();
    let object = session.manifest.object_key.clone();
    let version = session.manifest.version_id.clone();
    let dir = ObjectPathManager::new().get_shard_dir_on_disk(
        &std::path::PathBuf::from(&blobs[0]),
        &fragment,
        &object,
        &version,
    );
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("part_0_shard_0.shard"), b"unfinished upload").unwrap();
    super::standard_registry_write().unwrap().insert(
        super::standard_session_key(&fragment, &version),
        std::sync::Arc::new(session),
    );
    let reclaimed =
        crate::core::index_store::delete_unreferenced_object_index(&fragment, &object).unwrap();
    assert!(!reclaimed);
    assert!(dir.is_dir());
    super::cancel_standard_upload_direct(
        &crate::structs::auth_structs::Claims {
            operator: false,
            fragment: fragment.clone(),
            fragment_alias: None,
            read: true,
            write: true,
            modify: true,
            delete: true,
            exp: usize::MAX,
        },
        &fragment,
        &version,
    )
    .unwrap();
    assert!(
        crate::core::index_store::delete_unreferenced_object_index(&fragment, &object).unwrap()
    );
    assert!(!dir.exists());
}

#[tokio::test]
async fn cancelled_standard_finalize_releases_busy_state_for_retry() {
    let _test_guard = crate::test_support::global_test_lock().lock().await;
    let mut session = standard_test_session(0);
    session.manifest.fragment = format!("cancel-finalize-{}", ulid::Ulid::new());
    session.manifest.object_key = ulid::Ulid::new().to_string();
    session.manifest.version_id = ulid::Ulid::new().to_string();
    let fragment = session.manifest.fragment.clone();
    let version = session.manifest.version_id.clone();
    let key = super::standard_session_key(&fragment, &version);
    let session = std::sync::Arc::new(session);
    super::standard_registry_write()
        .unwrap()
        .insert(key.clone(), session.clone());
    let claims = crate::structs::auth_structs::Claims {
        operator: false,
        fragment: fragment.clone(),
        fragment_alias: None,
        read: true,
        write: true,
        modify: true,
        delete: true,
        exp: usize::MAX,
    };
    // Stop the real finalizer exactly at its publication lock, not with a sleep.
    let _publish_lock =
        crate::core::index::acquire_object_operation_lock(&fragment, &session.manifest.object_key)
            .await
            .unwrap();
    for _ in 0..2 {
        let mut finalize = Box::pin(super::finalize_standard_upload_direct(
            claims.clone(),
            fragment.clone(),
            version.clone(),
            session.manifest.session_id.clone(),
        ));
        assert!(matches!(
            futures_util::poll!(&mut finalize),
            std::task::Poll::Pending
        ));
        assert_eq!(
            session.lifecycle.load(Ordering::Acquire),
            super::STANDARD_SESSION_FINALIZING
        );
        drop(finalize);
        let state = session.lifecycle.load(Ordering::Acquire);
        if state != STANDARD_SESSION_OPEN {
            super::standard_registry_write().unwrap().remove(&key);
        }
        assert_eq!(
            state, STANDARD_SESSION_OPEN,
            "cancelled request must not wedge the session"
        );
    }
    super::standard_registry_write().unwrap().remove(&key);
}

#[test]
fn upload_durability_accepts_string_and_bool_override() {
    let q_batched = UploadPartQuery {
        checksum: None,
        durability: Some("batched".to_string()),
        strict_durability: None,
        io_gate: None,
    };
    assert!(matches!(
        parse_upload_durability_mode(&q_batched),
        UploadDurabilityMode::Batched
    ));

    let q_strict = UploadPartQuery {
        checksum: None,
        durability: None,
        strict_durability: Some(true),
        io_gate: None,
    };
    assert!(matches!(
        parse_upload_durability_mode(&q_strict),
        UploadDurabilityMode::Strict
    ));
}

#[tokio::test]
async fn upload_io_gate_helper_bypasses_when_disabled() {
    let guard = acquire_upload_io_guard_with_timeout(false, "unit-test-fragment", "unit-test")
        .await
        .expect("gate bypass succeeds");
    assert!(guard.is_none());
}

#[tokio::test]
async fn small_uploads_honor_default_io_admission_and_explicit_override() {
    let fragment = format!("small-upload-admission-{}", ulid::Ulid::new());
    for size in [
        0,
        1,
        8192,
        super::SMALL_BUFFERED_WRITE_LIMIT,
        super::SMALL_BUFFERED_WRITE_LIMIT + 1,
    ] {
        for override_value in [None, Some(true), Some(false)] {
            let guard = super::acquire_upload_io_guard_for_size(
                override_value,
                &fragment,
                "small-upload-regression",
                size,
            )
            .await
            .unwrap();
            assert_eq!(
                guard.is_some(),
                override_value != Some(false),
                "size {size} must not silently bypass the configured gate"
            );
            drop(guard);
        }
    }
}

#[tokio::test]
async fn saturated_small_upload_waiter_is_cancel_safe_and_capacity_is_reusable() {
    let fragment = format!("small-upload-saturation-{}", ulid::Ulid::new());
    let permits =
        crate::core::io_scheduler::disk_io_scheduler_config().foreground_per_fragment_permits;
    let mut held = Vec::new();
    for _ in 0..permits {
        held.push(
            super::acquire_upload_io_guard_for_size(
                None,
                &fragment,
                "fill-small-upload-gate",
                8192,
            )
            .await
            .unwrap()
            .expect("small upload must acquire a permit"),
        );
    }
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(20),
            super::acquire_upload_io_guard_for_size(
                None,
                &fragment,
                "cancelled-small-upload-waiter",
                8192
            ),
        )
        .await
        .is_err(),
        "saturated small uploads must wait for existing admission"
    );
    drop(held.pop());
    let guard = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        super::acquire_upload_io_guard_for_size(None, &fragment, "retry-small-upload-waiter", 8192),
    )
    .await
    .unwrap()
    .unwrap()
    .expect("cancelled waiter must not leak capacity");
    drop(guard);
}

#[tokio::test]
async fn direct_writer_propagates_background_write_failure() {
    let mut writer = direct_writer_with_failed_task(libc::ENOSPC);
    let error = writer
        .write_all(b"x")
        .await
        .expect_err("background ENOSPC must reach the caller");
    assert_eq!(error.raw_os_error(), Some(libc::ENOSPC));
}

#[tokio::test]
async fn direct_writer_propagates_background_flush_failure() {
    let mut writer = direct_writer_with_failed_task(libc::EDQUOT);
    let error = writer
        .flush()
        .await
        .expect_err("background EDQUOT must reach the caller");
    assert_eq!(error.raw_os_error(), Some(libc::EDQUOT));
}

#[test]
fn dynamic_stripe_uses_small_blocks_for_small_objects_and_configured_ceiling_for_large() {
    assert_eq!(
        choose_upload_stripe_size_for_len_with_ceiling(1, 262_144),
        4096
    );
    assert_eq!(
        choose_upload_stripe_size_for_len_with_ceiling(64 * 1024, 262_144),
        4096
    );
    assert_eq!(
        choose_upload_stripe_size_for_len_with_ceiling(64 * 1024 + 1, 262_144),
        16 * 1024
    );
    assert_eq!(
        choose_upload_stripe_size_for_len_with_ceiling(1024 * 1024 + 1, 262_144),
        262_144
    );
    assert_eq!(
        choose_upload_stripe_size_for_len_with_ceiling(1, 16 * 1024),
        4096
    );
    assert_eq!(
        choose_upload_stripe_size_for_len_with_ceiling(10 * 1024 * 1024, 16 * 1024),
        16 * 1024
    );
}

#[test]
fn recorded_layout_uses_the_same_adaptive_stripe_rule_as_upload() {
    let metadata = ObjectMetadata {
        object_key: "01KOBJECT".to_string(),
        fragment: "01KFRAGMENT".to_string(),
        version_id: "01KVERSION".to_string(),
        upload_session_id: "01KSESSION".to_string(),
        owner_uid: "01KFRAGMENT".to_string(),
        content_type: "application/octet-stream".to_string(),
        total_size_bytes: 96 * 1024,
        created_at: 1,
        data_shards: 1,
        parity_shards: 1,
        placement_generation: 0,
        parts: vec![
            PartMetadata {
                part_index: 0,
                size_bytes: 48 * 1024,
                created_at: 1,
                shards: Vec::new(),
                checksum: None,
            },
            PartMetadata {
                part_index: 1,
                size_bytes: 48 * 1024,
                created_at: 1,
                shards: Vec::new(),
                checksum: None,
            },
        ],
        is_delete_marker: false,
        stripe_size_bytes: 4096,
    };

    assert_eq!(target_stripe_size_for_metadata(&metadata, 262_144), 4096);
}

#[test]
fn multipart_parts_keep_one_fixed_object_ec_set() {
    let _ = INDEX_STORE_DISKS.set(vec!["/tmp/n1-upload-test-index".to_string()]);
    let roots = ["/tmp/n1-fixed-root-a", "/tmp/n1-fixed-root-b"];
    let manifest = UploadSessionManifest {
        fragment: "01KFRAGMENT".to_string(),
        object_key: "01KOBJECT".to_string(),
        version_id: "01KVERSION".to_string(),
        session_id: "01KSESSION".to_string(),
        total_size_bytes: 8192,
        expected_parts: 2,
        part_size_bytes: 4096,
        content_type: "application/octet-stream".to_string(),
        created_at: 1,
        stripe_size_bytes: 4096,
        data_shards: 1,
        parity_shards: 1,
        placement_roots: roots.iter().map(|root| (*root).to_string()).collect(),
    };
    let path_manager = ObjectPathManager::new();
    let first = shard_specs_for_part(&path_manager, &manifest, 0).expect("first part");
    let second = shard_specs_for_part(&path_manager, &manifest, 1).expect("second part");

    for shard_index in 0..2 {
        assert!(
            first[shard_index]
                .2
                .starts_with(Path::new(roots[shard_index]))
        );
        assert!(
            second[shard_index]
                .2
                .starts_with(Path::new(roots[shard_index]))
        );
        assert_eq!(first[shard_index].0, second[shard_index].0);
    }
}

#[test]
fn standard_part_claim_is_write_once_and_duplicate_safe() {
    let session = standard_test_session(2);
    let first = match claim_standard_part(&session, 0).expect("first claim") {
        StandardPartClaimResult::Claimed(claim) => claim,
        StandardPartClaimResult::AlreadyComplete => panic!("part was not complete"),
    };
    assert!(claim_standard_part(&session, 0).is_err());
    drop(first);
    assert_eq!(
        session.parts[0].state.load(Ordering::Acquire),
        STANDARD_PART_EMPTY
    );

    let second = match claim_standard_part(&session, 0).expect("retry claim") {
        StandardPartClaimResult::Claimed(claim) => claim,
        StandardPartClaimResult::AlreadyComplete => panic!("part was not complete"),
    };
    second
        .complete(StandardPartRecord {
            strict_durability: false,
            size_bytes: 4096,
            created_at: 1,
            checksum: Some(1),
            shard_checksums: vec![2, 3].into_boxed_slice(),
        })
        .expect("complete part");
    assert_eq!(
        session.parts[0].state.load(Ordering::Acquire),
        STANDARD_PART_COMPLETE
    );
    assert!(matches!(
        claim_standard_part(&session, 0).expect("idempotent retry"),
        StandardPartClaimResult::AlreadyComplete
    ));
    assert_eq!(session.in_flight_parts.load(Ordering::Acquire), 0);
}
