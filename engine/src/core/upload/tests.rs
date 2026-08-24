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
