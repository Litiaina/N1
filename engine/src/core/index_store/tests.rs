use super::*;
use serde_json::Value;

fn metadata() -> ObjectMetadata {
    ObjectMetadata {
        object_key: "01KWOBJECT00000000000000000".into(),
        packed_ext_u64: packed_extension_key("docs/report.pdf"),
        fragment: "01KWFRAGMENT000000000000000".into(),
        version_id: "01KWVERSION0000000000000000".into(),
        upload_session_id: "01KWSESSION0000000000000000".into(),
        owner_uid: "owner".into(),
        content_type: "application/pdf".into(),
        total_size_bytes: 4096,
        created_at: 1_782_864_000,
        pool_id: "pool-a".into(),
        pool_generation: 7,
        membership_epoch: 7,
        data_shards: 2,
        parity_shards: 1,
        parts: Vec::new(),
        is_delete_marker: false,
        stripe_size_bytes: 262_144,
    }
}

#[test]
fn immutable_ncdx_uses_named_fields_without_logical_path() {
    let bytes = encode_disk_object_metadata(&metadata()).expect("encode metadata");
    let value: Value = rmp_serde::from_slice(&bytes).expect("decode metadata");
    assert!(value.is_object());
    assert_eq!(
        value.get("format_version").and_then(Value::as_u64),
        Some(NCDX_FORMAT_VERSION as u64)
    );
    assert!(value.get("payload_sha256").is_some());
    assert!(value.get("object_id").is_some());
    assert!(value.get("object_key").is_none());
    assert!(value.get("logical_path").is_none());
}

#[test]
fn immutable_ncdx_rejects_payload_mutation() {
    let bytes = encode_disk_object_metadata(&metadata()).expect("encode metadata");
    let mut disk: DiskObjectMetadata = rmp_serde::from_slice(&bytes).expect("decode disk metadata");
    disk.content_type = "application/corrupt".into();
    assert!(disk.validate().is_err());
}

#[test]
fn packed_extension_is_zero_decode_filename_data() {
    let packed = packed_extension_key("docs/Clip.PDF");
    let expected = 3u64 | (b'p' as u64) << 8 | (b'd' as u64) << 16 | (b'f' as u64) << 24;
    assert_eq!(packed, expected);
    assert_eq!(packed_extension_key("docs/archive.longexts"), 0);
}

#[test]
fn reserved_internal_planes_are_not_user_paths() {
    assert!(normalize_object_key(".trash/video.mp4").is_err());
    assert!(normalize_object_key(".objects/video.mp4").is_err());
    assert!(normalize_object_key("docs/@o_bad/file.txt").is_err());
    assert_eq!(
        normalize_object_key("docs/video.mp4").expect("valid path"),
        "docs/video.mp4"
    );
}

#[test]
fn object_record_path_has_four_hash_fanout_levels() {
    let object_id = "01KWOBJECT00000000000000000";
    let segments = ObjectPathManager::object_fanout_segments(object_id);
    assert_eq!(segments.len(), 4);
    assert!(segments.iter().all(|segment| segment.len() == 2));
    assert_eq!(
        object_dir_name_for_id(object_id).expect("object dir"),
        format!("@o_{object_id}")
    );
}

#[test]
fn ncdx_filename_round_trips_zero_decode_summary() {
    let metadata = metadata();
    let name = ncdx_file_name(&metadata);
    let summary = parse_ncdx_file_name(&name).expect("parse ncdx filename");
    assert_eq!(summary.version_id, metadata.version_id);
    assert_eq!(summary.size_bytes, metadata.total_size_bytes);
    assert_eq!(summary.packed_ext, metadata.packed_ext_u64);
}

#[test]
fn head_selection_is_max_ulid_under_clock_skew_and_restart() {
    let dir = std::env::temp_dir().join(format!("n2-head-ulid-{}", Ulid::new()));
    fs::create_dir_all(&dir).expect("create object directory");
    let rollback = Ulid::from_parts(1_000, 7).to_string();
    let present = Ulid::from_parts(2_000, 3).to_string();
    let future = Ulid::from_parts(9_000, 1).to_string();

    // Deliberately publish out of timestamp order. Directory enumeration,
    // restart, and peer response order must not influence HEAD.
    for version_id in [&future, &rollback, &present] {
        let mut metadata = metadata();
        metadata.version_id = version_id.clone();
        fs::write(
            dir.join(ncdx_file_name(&metadata)),
            encode_disk_object_metadata(&metadata).expect("encode metadata"),
        )
        .expect("write immutable metadata fixture");
    }

    for _ in 0..3 {
        let head = latest_summary_in_object_dir(&dir)
            .expect("resolve HEAD")
            .expect("HEAD exists");
        assert_eq!(head.version_id, future);
    }
    fs::remove_dir_all(&dir).expect("remove object directory");
}

#[test]
fn concurrent_ulid_version_identities_are_unique_in_stress_sample() {
    let generated = std::thread::scope(|scope| {
        let workers = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    (0..5_000)
                        .map(|_| Ulid::new().to_string())
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>();
        workers
            .into_iter()
            .flat_map(|worker| worker.join().expect("ULID worker"))
            .collect::<Vec<_>>()
    });
    let total = generated.len();
    let unique = generated.into_iter().collect::<HashSet<_>>();
    assert_eq!(unique.len(), total);
}

#[test]
fn duplicate_version_id_with_divergent_metadata_fails_closed() {
    let dir = std::env::temp_dir().join(format!("n2-ncdx-conflict-{}", Ulid::new()));
    fs::create_dir_all(&dir).expect("create object directory");
    let mut original = metadata();
    original.version_id = Ulid::new().to_string();
    let path = dir.join(ncdx_file_name(&original));
    write_immutable(
        &path,
        &encode_disk_object_metadata(&original).expect("encode original"),
    )
    .expect("publish original");

    let mut divergent = original;
    divergent.content_type = "application/divergent".into();
    let error = write_immutable(
        &path,
        &encode_disk_object_metadata(&divergent).expect("encode divergent"),
    )
    .expect_err("divergent immutable identity must fail");
    assert!(matches!(error, AppError::Conflict(_)));
    fs::remove_dir_all(&dir).expect("remove object directory");
}

#[test]
fn vanished_directory_is_reported_as_not_found() {
    let path = std::env::temp_dir().join(format!("n2-vanished-dir-{}", Ulid::new()));
    assert!(matches!(read_dir_names(&path), Err(AppError::NotFound)));
}

#[test]
fn metadata_removed_after_enumeration_is_already_absent() {
    let dir = std::env::temp_dir().join(format!("n2-gc-idempotency-{}", Ulid::new()));
    fs::create_dir_all(&dir).expect("create test object directory");
    let path = dir.join(ncdx_file_name(&metadata()));
    fs::write(
        &path,
        encode_disk_object_metadata(&metadata()).expect("encode metadata"),
    )
    .expect("write metadata");

    let names = read_dir_names(&dir).expect("enumerate object directory");
    assert_eq!(names, vec![path.file_name().unwrap().to_string_lossy()]);
    fs::remove_file(&path).expect("simulate concurrent GC reclaim");
    assert!(
        decode_metadata_file_if_present(&path)
            .expect("concurrent reclaim is idempotent")
            .is_none()
    );

    fs::remove_dir_all(&dir).expect("remove test directory");
}
