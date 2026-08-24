use super::*;
use crate::core::disk_manager::{BLOB_STORE_DISKS, INDEX_STORE_DISKS, ObjectMetadata};
use ulid::Ulid;

fn ensure_test_blob_roots() -> Vec<PathBuf> {
    if let Some(existing) = BLOB_STORE_DISKS.get() {
        let roots = existing.iter().map(PathBuf::from).collect::<Vec<_>>();
        for root in &roots {
            let _ = fs::create_dir_all(root);
        }
        return roots;
    }

    let base = std::env::temp_dir().join(format!("n1-blob-{}", std::process::id()));
    let roots = vec![base.join("blob-0"), base.join("blob-1")];
    for root in &roots {
        fs::create_dir_all(root).expect("create test blob root");
    }
    let _ = BLOB_STORE_DISKS.set(
        roots
            .iter()
            .map(|root| root.to_string_lossy().to_string())
            .collect(),
    );
    roots
}

fn ensure_test_index_roots() -> Vec<PathBuf> {
    let _ = ensure_test_blob_roots();
    if let Some(existing) = INDEX_STORE_DISKS.get() {
        let roots = existing.iter().map(PathBuf::from).collect::<Vec<_>>();
        for root in &roots {
            let _ = fs::create_dir_all(root);
        }
        return roots;
    }

    let base = std::env::temp_dir().join(format!("n1-index-{}", std::process::id()));
    let roots = vec![base.join("index-0"), base.join("index-1")];
    for root in &roots {
        fs::create_dir_all(root).expect("create test index root");
    }
    let _ = INDEX_STORE_DISKS.set(
        roots
            .iter()
            .map(|root| root.to_string_lossy().to_string())
            .collect(),
    );
    roots
}

#[test]
fn fragment_meta_encoding_is_named_and_requires_stable_identity() {
    let meta = FragmentMeta {
        engine_format_version: N1_ENGINE_FORMAT_VERSION,
        stable_fragment_id: "01KSTABLEFRAGMENT000000000".to_string(),
        state: "active".to_string(),
        committed_bytes: 0,
        reserved_bytes: 0,
        trash_bytes: 0,
        created_at_unix: 1,
        updated_at_unix: 2,
        key_phc: None,
        nt_hash_hex: None,
        index_partition_count: 64,
    };

    let encoded = encode_fragment_meta(&meta).expect("encode fragment metadata");
    assert!(
        encoded
            .first()
            .is_some_and(|byte| (*byte & 0xf0) == 0x80 || *byte == 0xde || *byte == 0xdf),
        ".fragment.ndx must be a named MessagePack map, not a positional tuple"
    );
    let decoded = decode_fragment_meta(&encoded).expect("decode named fragment metadata");
    assert_eq!(decoded.stable_fragment_id, meta.stable_fragment_id);
    assert_eq!(decoded.engine_format_version, N1_ENGINE_FORMAT_VERSION);
    assert_eq!(decoded.state, "active");
    assert_eq!(decoded.index_partition_count, 64);
}

#[test]
fn fragment_meta_rejects_pre_n1_3_engine_format() {
    #[derive(Serialize)]
    struct PreviousEngineMeta<'a> {
        engine_format_version: u16,
        stable_fragment_id: &'a str,
        state: &'a str,
        committed_bytes: u64,
        reserved_bytes: u64,
        trash_bytes: u64,
        created_at_unix: i64,
        updated_at_unix: i64,
        key_phc: Option<&'a str>,
        nt_hash_hex: Option<&'a str>,
        index_partition_count: u16,
    }

    let raw = rmp_serde::to_vec_named(&PreviousEngineMeta {
        engine_format_version: 2,
        stable_fragment_id: "01KPREVIOUSENGINE000000000",
        state: "active",
        committed_bytes: 0,
        reserved_bytes: 0,
        trash_bytes: 0,
        created_at_unix: 1,
        updated_at_unix: 2,
        key_phc: None,
        nt_hash_hex: None,
        index_partition_count: 64,
    })
    .expect("encode previous engine metadata");

    let err = decode_fragment_meta(&raw).expect_err("N1 2 engine must fail closed");
    assert!(matches!(err, AppError::Conflict(_)));
}

#[test]
fn automatic_repair_restores_only_the_missing_fragment_alias() {
    let base = std::env::temp_dir().join(format!("n1-alias-repair-{}", Ulid::new()));
    let roots = vec![base.join("index-0"), base.join("index-1")];
    let stable_fragment_id = Ulid::new().to_string();
    let alias = "d5a6b749356bda56a23882d8d75e0dca1f268616bba4cc28622916180ae193ca";
    let meta = FragmentMeta {
        engine_format_version: N1_ENGINE_FORMAT_VERSION,
        stable_fragment_id: stable_fragment_id.clone(),
        state: "active".to_string(),
        committed_bytes: 0,
        reserved_bytes: 0,
        trash_bytes: 0,
        created_at_unix: 1,
        updated_at_unix: 1,
        key_phc: None,
        nt_hash_hex: None,
        index_partition_count: 64,
    };
    let bytes = rmp_serde::to_vec_named(&meta).expect("encode fragment metadata");

    for root in &roots {
        let physical = physical_fragment_root_on(root, &stable_fragment_id);
        fs::create_dir_all(&physical).expect("create physical fragment");
        fs::write(physical.join(FRAGMENT_META_FILE), &bytes).expect("write fragment metadata");
        fs::create_dir_all(fragment_alias_dir(root)).expect("create alias directory");
    }
    unix_fs::symlink(
        PathBuf::from("..").join(physical_fragment_dir_name(&stable_fragment_id)),
        fragment_alias_path(&roots[0], alias),
    )
    .expect("create surviving alias");

    let report =
        reconcile_fragment_alias_replicas(&roots, false).expect("reconcile missing fragment alias");
    assert_eq!(report.discovered_fragments, 1);
    assert_eq!(report.missing_aliases, 1);
    assert_eq!(report.repaired_aliases, 1);
    assert_eq!(report.ambiguous_fragments, 0);
    assert_eq!(
        fs::read_link(fragment_alias_path(&roots[1], alias)).expect("read repaired alias"),
        PathBuf::from("..").join(physical_fragment_dir_name(&stable_fragment_id))
    );
    assert_eq!(
        fs::read(
            physical_fragment_root_on(&roots[1], &stable_fragment_id).join(FRAGMENT_META_FILE)
        )
        .expect("read destination metadata"),
        bytes,
        "alias repair must not rewrite fragment metadata or namespace content"
    );

    let second = reconcile_fragment_alias_replicas(&roots, false)
        .expect("repeat alias reconciliation idempotently");
    assert_eq!(second.missing_aliases, 0);
    assert_eq!(second.repaired_aliases, 0);
    fs::remove_dir_all(base).expect("remove alias repair fixture");
}

#[test]
fn fragment_meta_rejects_missing_engine_format() {
    #[derive(Serialize)]
    struct UnversionedMeta<'a> {
        stable_fragment_id: &'a str,
        state: &'a str,
        committed_bytes: u64,
        reserved_bytes: u64,
        trash_bytes: u64,
        created_at_unix: i64,
        updated_at_unix: i64,
        key_phc: Option<&'a str>,
        nt_hash_hex: Option<&'a str>,
        index_partition_count: u16,
    }

    let raw = rmp_serde::to_vec_named(&UnversionedMeta {
        stable_fragment_id: "01KUNVERSIONED00000000000",
        state: "active",
        committed_bytes: 0,
        reserved_bytes: 0,
        trash_bytes: 0,
        created_at_unix: 1,
        updated_at_unix: 2,
        key_phc: None,
        nt_hash_hex: None,
        index_partition_count: 64,
    })
    .expect("encode unversioned metadata");

    assert!(
        decode_fragment_meta(&raw).is_err(),
        "unversioned pre-N1 3 metadata must fail closed"
    );
}

fn test_metadata(fragment: &str, object_key: &str, size: u64) -> ObjectMetadata {
    let version_id = Ulid::new().to_string();
    let blob_root = ensure_test_blob_roots()
        .into_iter()
        .next()
        .expect("test blob root");
    let version_dir = live_object_dir(&blob_root, fragment, object_key)
        .expect("test object dir")
        .join(&version_id);
    fs::create_dir_all(&version_dir).expect("create test object placement");
    let shard_path = version_dir.join("part_0_shard_0.shard");
    fs::write(&shard_path, vec![0u8; size.min(4096) as usize]).expect("write test shard");
    ObjectMetadata {
        object_key: object_key.to_string(),
        fragment: fragment.to_string(),
        version_id,
        upload_session_id: "test-session".to_string(),
        owner_uid: "test-owner".to_string(),
        content_type: "application/octet-stream".to_string(),
        total_size_bytes: size,
        created_at: 1_777_905_000,
        data_shards: 1,
        parity_shards: 0,
        placement_generation: 0,
        parts: vec![PartMetadata {
            part_index: 0,
            size_bytes: size,
            created_at: 1_777_905_000,
            shards: vec![crate::core::disk_manager::ShardInfo {
                shard_index: 0,
                file_name: "part_0_shard_0.shard".to_string(),
                absolute_path: shard_path.to_string_lossy().into_owned(),
                size_bytes: size.min(4096),
                checksum: String::new(),
                is_parity: false,
            }],
            checksum: None,
        }],
        is_delete_marker: false,
        stripe_size_bytes: crate::core::disk_manager::default_stripe_size(),
    }
}

fn publish_logical_object(
    roots: &[PathBuf],
    fragment: &str,
    logical_path: &str,
    size: u64,
) -> String {
    let object_id = new_object_id();
    upsert_live_version(&test_metadata(fragment, &object_id, size)).expect("publish test ndx");
    for root in roots {
        publish_namespace_link(root, fragment, logical_path, &object_id)
            .expect("publish namespace link");
    }
    object_id
}

#[test]
fn colocated_metadata_exists_only_on_shard_bearing_roots_and_repairs_without_payload_rewrite() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let index_roots = ensure_test_index_roots();
    let blob_roots = ensure_test_blob_roots();
    assert!(blob_roots.len() >= 2);
    let fragment = format!("n1-colocated-meta-{}", Ulid::new());
    let object_id = new_object_id();
    let metadata = test_metadata(&fragment, &object_id, 4096);
    let shard_path = PathBuf::from(&metadata.parts[0].shards[0].absolute_path);
    let payload_before = fs::read(&shard_path).expect("read payload before metadata repair");

    upsert_live_version(&metadata).expect("publish colocated metadata");
    let participant_root = blob_roots
        .iter()
        .find(|root| shard_path.starts_with(root))
        .expect("shard-bearing root");
    let participant_dir =
        live_object_dir(participant_root, &fragment, &object_id).expect("participant dir");
    let metadata_path = ndx_path_for_version(&participant_dir, &metadata.version_id)
        .expect("inspect metadata")
        .expect("metadata on shard-bearing root");

    for root in blob_roots.iter().filter(|root| *root != participant_root) {
        let object_dir = live_object_dir(root, &fragment, &object_id).expect("non-participant dir");
        assert!(
            ndx_path_for_version(&object_dir, &metadata.version_id)
                .expect("inspect non-participant")
                .is_none(),
            "non-participant roots must not receive metadata copies"
        );
        assert!(!object_dir.join(&metadata.version_id).exists());
    }
    for root in &index_roots {
        assert!(
            !fragment_root_on(root, &fragment).join(OBJECTS_DIR).exists(),
            "index roots are namespace/control only"
        );
    }

    fs::remove_file(&metadata_path).expect("remove colocated metadata");
    assert!(!object_metadata_placements_healthy(&metadata).expect("inspect missing metadata"));
    upsert_live_version(&metadata).expect("restore metadata without re-encoding payload");
    assert!(object_metadata_placements_healthy(&metadata).expect("inspect restored metadata"));
    assert_eq!(
        fs::read(&shard_path).expect("read payload after metadata repair"),
        payload_before,
        "metadata-only repair must not rewrite shard bytes"
    );
}

#[test]
fn repair_fragment_replicas_repairs_namespace_only_and_preserves_objects() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    let blob_roots = ensure_test_blob_roots();
    let fragment = format!("n1-repair-source-{}", Ulid::new());
    let object_id = publish_logical_object(&roots, &fragment, "docs/video.mp4", 123);

    let bad_root = fragment_root_on(&roots[0], &fragment);
    let object_dir = live_object_dir(&blob_roots[0], &fragment, &object_id).expect("object dir");
    let object_entries_before = read_dir_names(&object_dir).expect("read object placement");
    fs::remove_file(bad_root.join("docs/video.mp4")).expect("wipe namespace symlink");

    let before = inspect_fragment_partition_replicas(&fragment, &roots);
    assert!(!before.healthy);
    assert!(before.missing_replicas > 0);

    let (missing, divergent, corrupt) =
        repair_fragment_all_partition_replicas(&fragment, false).expect("repair N1 index");
    assert!(missing + divergent > 0);
    assert_eq!(corrupt, 0);

    let repaired = inspect_fragment_partition_replicas(&fragment, &roots);
    assert!(repaired.healthy, "repair should restore mirrored N1 index");
    assert!(
        fs::symlink_metadata(bad_root.join("docs/video.mp4"))
            .expect("namespace symlink restored")
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        read_dir_names(&object_dir).expect("read object placement after repair"),
        object_entries_before,
        "namespace repair must not copy, delete, or rewrite colocated object metadata"
    );
    for root in &roots {
        assert!(
            !fragment_root_on(root, &fragment).join(OBJECTS_DIR).exists(),
            "2.0 index roots must never receive an object metadata plane"
        );
    }
}

#[test]
fn replica_inspection_detects_equal_count_different_namespace_content() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    assert!(roots.len() >= 2);
    let fragment = format!("n1-replica-content-divergence-{}", Ulid::new());
    let original_object = publish_logical_object(&roots, &fragment, "docs/report.bin", 123);
    let divergent_object = new_object_id();
    upsert_live_version(&test_metadata(&fragment, &divergent_object, 456))
        .expect("publish divergent object metadata");

    let divergent_link = fragment_root_on(&roots[1], &fragment).join("docs/report.bin");
    fs::remove_file(&divergent_link).expect("remove one replica link");
    unix_fs::symlink(
        object_symlink_target(&divergent_object).expect("divergent link target"),
        &divergent_link,
    )
    .expect("inject equal-count divergent namespace link");

    assert_ne!(original_object, divergent_object);
    let inspection = inspect_fragment_partition_replicas(&fragment, &roots);
    assert!(!inspection.healthy);
    assert_eq!(inspection.missing_replicas, 0);
    assert_eq!(inspection.divergent_replicas, 1);
    assert!(matches!(
        repair_fragment_all_partition_replicas(&fragment, false),
        Err(AppError::Conflict(_))
    ));
    assert_eq!(
        read_namespace_symlink_object_id(&roots[1], &fragment, &divergent_link)
            .expect("read divergent link after refused repair"),
        Some(divergent_object),
        "automatic repair must fail closed when two equally complete replicas disagree"
    );
}

#[test]
fn replica_content_digest_is_independent_of_directory_iteration_order() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    assert!(roots.len() >= 2);
    let fragment = format!("n1-replica-manifest-order-{}", Ulid::new());
    let first_object = new_object_id();
    let second_object = new_object_id();
    let first_root = fragment_root_on(&roots[0], &fragment);
    let second_root = fragment_root_on(&roots[1], &fragment);

    fs::create_dir_all(first_root.join("alpha")).expect("create first alpha");
    fs::create_dir_all(first_root.join("beta")).expect("create first beta");
    fs::create_dir_all(second_root.join("beta")).expect("create second beta first");
    fs::create_dir_all(second_root.join("alpha")).expect("create second alpha last");
    unix_fs::symlink(
        object_symlink_target(&first_object).expect("first target"),
        first_root.join("alpha/a.bin"),
    )
    .expect("publish first-root alpha");
    unix_fs::symlink(
        object_symlink_target(&second_object).expect("second target"),
        first_root.join("beta/b.bin"),
    )
    .expect("publish first-root beta");
    unix_fs::symlink(
        object_symlink_target(&second_object).expect("second target"),
        second_root.join("beta/b.bin"),
    )
    .expect("publish second-root beta");
    unix_fs::symlink(
        object_symlink_target(&first_object).expect("first target"),
        second_root.join("alpha/a.bin"),
    )
    .expect("publish second-root alpha");

    let inspection = inspect_fragment_partition_replicas(&fragment, &roots);
    assert!(inspection.healthy);
    assert_eq!(inspection.valid_replicas, 2);
    assert_eq!(inspection.divergent_replicas, 0);
}

#[test]
fn redistribution_refuses_equal_count_object_identity_divergence() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    assert!(roots.len() >= 2);
    let fragment = format!("n1-redistribution-generation-{}", Ulid::new());
    let original_object = publish_logical_object(&roots, &fragment, "docs/report.bin", 123);
    let replacement_object = new_object_id();
    upsert_live_version(&test_metadata(&fragment, &replacement_object, 456))
        .expect("publish replacement object metadata");
    std::thread::sleep(std::time::Duration::from_millis(20));
    let authoritative_link = fragment_root_on(&roots[0], &fragment).join("docs/report.bin");
    fs::remove_file(&authoritative_link).expect("remove prior authoritative link");
    unix_fs::symlink(
        object_symlink_target(&replacement_object).expect("replacement target"),
        &authoritative_link,
    )
    .expect("publish one-disk replacement");
    assert!(matches!(
        sync_fragment_partition_replicas(&fragment),
        Err(AppError::Conflict(_))
    ));
    assert_ne!(original_object, replacement_object);
}

#[test]
fn redistribution_repairs_a_partial_folder_rename_without_foreground_coordination() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    assert!(roots.len() >= 2);
    let fragment = format!("n1-redistribution-partial-rename-{}", Ulid::new());
    publish_logical_object(&roots, &fragment, "before/item.bin", 123);
    std::thread::sleep(std::time::Duration::from_millis(20));
    fs::rename(
        fragment_root_on(&roots[0], &fragment).join("before"),
        fragment_root_on(&roots[0], &fragment).join("after"),
    )
    .expect("rename on continuously active replica");

    let (source, _) = choose_replica_source(&fragment).expect("authoritative source");
    assert_eq!(source, fragment_root_on(&roots[0], &fragment));
    sync_fragment_partition_replicas(&fragment)
        .expect("latest filesystem mutation must repair the stale replica");
    for root in &roots {
        assert!(
            fragment_root_on(root, &fragment)
                .join("after/item.bin")
                .is_symlink(),
            "the repaired replica must contain the renamed namespace entry"
        );
        assert!(!fragment_root_on(root, &fragment).join("before").exists());
    }
    assert!(inspect_fragment_partition_replicas(&fragment, &roots).healthy);
}

#[test]
fn repair_fragment_replicas_ignores_newer_fragment_meta_on_stale_replica() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    assert!(roots.len() >= 2);
    let fragment = format!("n1-repair-stale-meta-{}", Ulid::new());
    publish_logical_object(&roots, &fragment, "docs/original.txt", 123);

    let one_disk_object_id = new_object_id();
    let one_disk_meta = test_metadata(&fragment, &one_disk_object_id, 34);
    upsert_live_version(&one_disk_meta).expect("publish colocated object metadata");
    publish_namespace_link(
        &roots[0],
        &fragment,
        "n1-redistribution/one-disk.txt",
        &one_disk_object_id,
    )
    .expect("publish one-disk namespace link");

    std::thread::sleep(std::time::Duration::from_millis(20));
    let stale_meta = fragment_root_on(&roots[1], &fragment).join(FRAGMENT_META_FILE);
    fs::write(&stale_meta, b"newer stale fragment metadata").expect("touch stale meta");

    let (source, _score) = choose_replica_source(&fragment).expect("source replica");
    assert_eq!(
        source,
        fragment_root_on(&roots[0], &fragment),
        "newer .fragment.ndx on a stale replica must not outrank the richer index plane"
    );

    repair_fragment_all_partition_replicas(&fragment, false).expect("repair N1 replicas");

    for root in &roots {
        assert!(
            fs::symlink_metadata(
                fragment_root_on(root, &fragment).join("n1-redistribution/one-disk.txt"),
            )
            .expect("one-disk namespace link")
            .file_type()
            .is_symlink(),
            "repair must preserve the namespace link created while one replica was active"
        );
        assert!(!fragment_root_on(root, &fragment).join(OBJECTS_DIR).exists());
    }
}

#[test]
fn repair_fragment_replicas_prefers_richer_replica_even_when_stale_plane_is_newer() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    assert!(roots.len() >= 2);
    let fragment = format!("n1-repair-stale-plane-{}", Ulid::new());
    publish_logical_object(&roots, &fragment, "docs/original.txt", 123);

    let one_disk_object_id = new_object_id();
    let one_disk_meta = test_metadata(&fragment, &one_disk_object_id, 34);
    upsert_live_version(&one_disk_meta).expect("publish colocated object metadata");
    publish_namespace_link(
        &roots[0],
        &fragment,
        "n1-redistribution/one-disk.txt",
        &one_disk_object_id,
    )
    .expect("publish one-disk namespace link");

    std::thread::sleep(std::time::Duration::from_millis(20));
    let stale_user_dir =
        fragment_root_on(&roots[1], &fragment).join("maintenance-touched-stale-tree");
    fs::create_dir_all(&stale_user_dir).expect("touch stale index plane");

    assert!(
        latest_index_plane_mtime_nanos(&fragment_root_on(&roots[1], &fragment))
            > latest_index_plane_mtime_nanos(&fragment_root_on(&roots[0], &fragment)),
        "stale replica must be newer to reproduce the redistribution regression"
    );

    let (source, _score) = choose_replica_source(&fragment).expect("source replica");
    assert_eq!(
        source,
        fragment_root_on(&roots[0], &fragment),
        "a touched stale plane must not outrank the richer one-disk write"
    );

    repair_fragment_all_partition_replicas(&fragment, false).expect("repair N1 replicas");

    for root in &roots {
        assert!(
            fs::symlink_metadata(
                fragment_root_on(root, &fragment).join("n1-redistribution/one-disk.txt"),
            )
            .expect("one-disk namespace link")
            .file_type()
            .is_symlink(),
            "repair must preserve the namespace link created while one replica was active"
        );
        assert!(!fragment_root_on(root, &fragment).join(OBJECTS_DIR).exists());
    }
}

#[test]
fn stale_index_on_blob_only_root_is_cleaned_without_touching_blobs() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    let blob_roots = ensure_test_blob_roots();
    let Some(blob_only_root) = blob_roots
        .iter()
        .find(|blob_root| {
            !roots
                .iter()
                .any(|index_root| same_root(blob_root, index_root))
        })
        .cloned()
    else {
        return;
    };
    let fragment = format!("n1-stale-cleanup-{}", Ulid::new());
    let object_id = publish_logical_object(&roots, &fragment, "docs/video.mp4", 123);

    let stale_fragment_root = fragment_root_on(&blob_only_root, &fragment);
    let stale_object_dir =
        live_object_dir(&blob_only_root, &fragment, &object_id).expect("stale object dir");
    fs::create_dir_all(&stale_object_dir).expect("create stale object dir");
    fs::write(
        stale_fragment_root.join(FRAGMENT_META_FILE),
        b"stale-fragment-meta",
    )
    .expect("write stale fragment meta");
    fs::create_dir_all(stale_fragment_root.join("docs")).expect("create stale namespace dir");
    fs::write(
        stale_fragment_root.join("docs/old-name.txt"),
        b"stale-link-placeholder",
    )
    .expect("write stale namespace placeholder");
    let blob_marker = stale_fragment_root
        .join(OBJECTS_DIR)
        .join("version")
        .join("part_0_shard_0.shard");
    fs::create_dir_all(blob_marker.parent().expect("blob parent")).expect("create blob dir");
    fs::write(&blob_marker, b"payload").expect("write blob marker");

    let cleaned =
        cleanup_stale_index_plane_on_blob_only_roots(&fragment).expect("cleanup stale index");
    assert_eq!(cleaned, 1);
    assert!(stale_fragment_root.join(OBJECTS_DIR).exists());
    assert!(!stale_fragment_root.join(FRAGMENT_META_FILE).exists());
    assert!(!stale_fragment_root.join("docs").exists());
    assert!(
        blob_marker.is_file(),
        "cleanup must preserve the blob plane"
    );
}

#[test]
fn packed_extension_uses_length_prefixed_lowercase_bytes() {
    let packed = packed_extension_key("docs/Clip.PDF");
    let expected = 3u64 | (b'p' as u64) << 8 | (b'd' as u64) << 16 | (b'f' as u64) << 24;
    assert_eq!(packed, expected);
    assert_eq!(packed_extension_key("docs/archive.longexts"), 0);
}

#[test]
fn reserved_user_path_segments_are_rejected() {
    assert!(normalize_object_key("docs/@o_bad/file.txt").is_err());
    assert!(normalize_object_key(".trash/video.mp4").is_err());
    assert_eq!(
        normalize_object_key(".blobs/video.mp4").unwrap(),
        ".blobs/video.mp4"
    );
    assert!(normalize_object_key("docs/.repair/file.txt").is_err());
    assert_eq!(
        normalize_object_key("docs/video.mp4").unwrap(),
        "docs/video.mp4"
    );
    assert_eq!(
        normalize_object_key("blob/video.mp4").unwrap(),
        "blob/video.mp4"
    );
}

#[test]
fn n1_object_id_validation_rejects_posix_file_names() {
    assert!(normalize_object_id("INCDT 122225.mp4").is_err());
    assert!(normalize_object_id("videos/clip.mp4").is_err());
    assert!(normalize_object_id("@o_01KQXRVALIDEDGEID0000000").is_ok());
}

#[test]
fn object_index_dir_uses_two_level_hash_fanout() {
    let root = PathBuf::from("/n1-index");
    let fragment = "fragment-a";
    let object_id = "01KQXRVALIDEDGEID0000000";
    let (fanout_a, fanout_b) = object_fanout_segments_for_id(object_id).expect("fanout segments");
    let path = object_index_dir(&root, fragment, object_id).expect("object dir");

    assert_eq!(
        path,
        fragment_root_on(&root, fragment)
            .join(OBJECTS_DIR)
            .join(fanout_a)
            .join(fanout_b)
            .join(format!("@o_{object_id}"))
    );
    assert_ne!(
        path,
        fragment_root_on(&root, fragment)
            .join(OBJECTS_DIR)
            .join(format!("@o_{object_id}")),
        "N1 object records must not use the old flat object directory"
    );
}

#[test]
fn live_head_listing_reads_filenames_without_decoding_ndx_body() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let index_roots = ensure_test_index_roots();
    let blob_roots = ensure_test_blob_roots();
    let fragment = format!("n1-list-{}", Ulid::new());
    let object_id = new_object_id();
    let logical_path = "docs/bad-body.mp4";
    let file_name = format!(
        "{}.{}.{}.{}.ndx",
        Ulid::new(),
        12345,
        1_777_905_000_000_000_000i128,
        packed_extension_key(logical_path)
    );
    for root in &blob_roots {
        let object_dir = live_object_dir(root, &fragment, &object_id).expect("object dir");
        fs::create_dir_all(&object_dir).expect("create object dir");
        fs::write(object_dir.join(&file_name), b"not-valid-msgpack").expect("write bad ndx");
    }
    for root in &index_roots {
        publish_namespace_link(root, &fragment, logical_path, &object_id)
            .expect("publish namespace link");
    }

    let listed = scan_live_heads_page(&fragment, Some("docs".to_string()), None, 10, None, true)
        .expect("zero-decode list");
    assert_eq!(listed.entries.len(), 1);
    assert_eq!(listed.objects.len(), 1);
    assert_eq!(listed.objects[0].total_size_bytes, 12345);
    assert_eq!(listed.objects[0].object_key, logical_path);
    assert_eq!(listed.objects[0].content_type, "video/mp4");
    assert!(read_current_live_head(&fragment, &object_id).is_err());
}

#[test]
fn live_head_pagination_uses_display_names_not_raw_object_dirs() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-page-{}", Ulid::new());
    for (name, size) in [
        ("docs/a.svg", 10u64),
        ("docs/b.svg", 20u64),
        ("docs/c.svg", 30u64),
    ] {
        publish_logical_object(&roots, &fragment, name, size);
    }

    let first = scan_live_heads_page(&fragment, Some("docs".to_string()), None, 2, None, true)
        .expect("first page");
    assert_eq!(
        first
            .objects
            .iter()
            .map(|meta| meta.object_key.as_str())
            .collect::<Vec<_>>(),
        vec!["docs/a.svg", "docs/b.svg"]
    );
    let second = scan_live_heads_page(
        &fragment,
        Some("docs".to_string()),
        None,
        2,
        first.continuation_token,
        true,
    )
    .expect("second page");
    assert_eq!(
        second
            .objects
            .iter()
            .map(|meta| meta.object_key.as_str())
            .collect::<Vec<_>>(),
        vec!["docs/c.svg"]
    );
}

#[test]
fn full_metadata_scan_uses_current_filesystem_path_after_rename() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-full-meta-{}", Ulid::new());
    let old_key = "docs/old-name.txt";
    let new_key = "docs/new-name.txt";
    let object_id = publish_logical_object(&roots, &fragment, old_key, 42);
    for root in &roots {
        let old_link = fragment_root_on(root, &fragment).join(old_key);
        let new_link = fragment_root_on(root, &fragment).join(new_key);
        fs::rename(&old_link, &new_link).expect("simulate N1 namespace rename");
        assert_eq!(
            read_namespace_symlink_object_id(root, &fragment, &new_link).expect("read moved link"),
            Some(object_id.clone())
        );
    }

    let page =
        scan_live_full_metadata_page(&fragment, Some("docs".to_string()), None, 10, None, true)
            .expect("full metadata page");
    assert_eq!(page.objects.len(), 1);
    assert_eq!(page.objects[0].object_key, object_id);
}

#[test]
fn full_metadata_pagination_advances_by_namespace_path() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-full-page-{}", Ulid::new());
    let first_id = publish_logical_object(&roots, &fragment, "docs/a.txt", 10);
    let second_id = publish_logical_object(&roots, &fragment, "docs/b.txt", 20);
    let third_id = publish_logical_object(&roots, &fragment, "docs/c.txt", 30);

    let first =
        scan_live_full_metadata_page(&fragment, Some("docs".to_string()), None, 2, None, true)
            .expect("first full metadata page");
    assert_eq!(
        first
            .objects
            .iter()
            .map(|meta| meta.object_key.as_str())
            .collect::<Vec<_>>(),
        vec![first_id.as_str(), second_id.as_str()]
    );
    let token = first
        .continuation_token
        .clone()
        .expect("first page continuation");
    assert_eq!(token, "docs/b.txt");

    let second = scan_live_full_metadata_page(
        &fragment,
        Some("docs".to_string()),
        None,
        2,
        first.continuation_token,
        true,
    )
    .expect("second full metadata page");
    assert_eq!(
        second
            .objects
            .iter()
            .map(|meta| meta.object_key.as_str())
            .collect::<Vec<_>>(),
        vec![third_id.as_str()]
    );
    assert!(second.continuation_token.is_none());
}

#[test]
fn trash_maintenance_scan_preserves_physical_object_id() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-trash-maintenance-{}", Ulid::new());
    let logical_path = "docs/recoverable report.txt";
    let object_id = publish_logical_object(&roots, &fragment, logical_path, 42);
    let version_ids = list_live_versions(&fragment, &object_id)
        .expect("list test versions")
        .into_iter()
        .map(|metadata| metadata.version_id)
        .collect::<Vec<_>>();
    assert!(!version_ids.is_empty());

    let generation = trash_generation_name(Utc::now().timestamp_nanos_opt().unwrap(), 2);

    for root in &roots {
        let fragment_root = fragment_root_on(root, &fragment);
        let source = fragment_root.join(logical_path);
        let destination = fragment_root
            .join(TRASH_DIR)
            .join(&generation)
            .join(logical_path);
        fs::create_dir_all(destination.parent().expect("trash parent"))
            .expect("create trash payload parent");
        fs::rename(source, destination).expect("move namespace link into trash");
    }

    let user_page =
        scan_trash_heads_page(&fragment, Some("docs".to_string()), None, 10, None, true)
            .expect("scan user trash page");
    assert_eq!(user_page.objects.len(), 1);
    assert_eq!(user_page.objects[0].object_key, logical_path);

    let maintenance_page = scan_trash_full_metadata_page(&fragment, None, None, 10, None, true)
        .expect("scan maintenance trash page");
    assert_eq!(maintenance_page.objects.len(), 1);
    assert_eq!(maintenance_page.objects[0].object_key, object_id);
    assert!(normalize_object_id(&maintenance_page.objects[0].object_key).is_ok());
}

#[test]
fn trash_maintenance_scan_descends_into_deleted_directories() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-trash-directory-maintenance-{}", Ulid::new());
    let logical_path = "archive/nested/report.bin";
    let object_id = publish_logical_object(&roots, &fragment, logical_path, 73);

    let generation = trash_generation_name(Utc::now().timestamp_nanos_opt().unwrap(), 1);

    for root in &roots {
        let fragment_root = fragment_root_on(root, &fragment);
        let destination = fragment_root
            .join(TRASH_DIR)
            .join(&generation)
            .join("archive");
        fs::create_dir_all(destination.parent().expect("trash parent"))
            .expect("create trash payload parent");
        fs::rename(fragment_root.join("archive"), destination)
            .expect("move namespace directory into trash");
    }

    let maintenance_page = scan_trash_full_metadata_page(&fragment, None, None, 10, None, true)
        .expect("scan deleted directory maintenance page");
    assert_eq!(maintenance_page.objects.len(), 1);
    assert_eq!(maintenance_page.objects[0].object_key, object_id);
}

#[test]
fn multipart_receipts_are_zero_decode_filename_records() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-upload-{}", Ulid::new());
    let version_id = Ulid::new().to_string();
    let manifest = UploadSessionManifest {
        fragment: fragment.clone(),
        object_key: "uploads/video.bin".to_string(),
        version_id: version_id.clone(),
        session_id: Ulid::new().to_string(),
        total_size_bytes: 8 * 1024,
        expected_parts: 8,
        part_size_bytes: 1024,
        content_type: "application/octet-stream".to_string(),
        created_at: 1_777_905_000,
        stripe_size_bytes: 512,
        data_shards: 2,
        parity_shards: 1,
        placement_roots: vec![
            "/blob/0".to_string(),
            "/blob/1".to_string(),
            "/blob/2".to_string(),
        ],
    };
    open_session(&manifest, None).expect("open session");
    let receipt = SessionPartReceipt {
        part_index: 7,
        size_bytes: 1024,
        shard_size_bytes: 512,
        created_at: 1_777_905_001,
        checksum: Some(0x0102_0304),
        shard_checksums: vec![0x1111_1111, 0x2222_2222, 0x3333_3333],
    };
    commit_session_part_receipt(&fragment, &version_id, &receipt).expect("write receipts");

    let dir = fragment_root_on(&roots[0], &fragment)
        .join(UPLOADS_DIR)
        .join(&version_id);
    let names = read_dir_names(&dir).expect("receipt names");
    assert!(names.contains(&"part_p00000000000000000007.01020304.receipt".to_string()));
    assert!(names.contains(&"shard_p00000000000000000007_s0000.11111111.receipt".to_string()));
    assert!(names.contains(&"shard_p00000000000000000007_s0001.22222222.receipt".to_string()));
    assert!(names.contains(&"shard_p00000000000000000007_s0002.33333333.receipt".to_string()));

    for name in names.iter().filter(|name| name.ends_with(RECEIPT_SUFFIX)) {
        fs::write(dir.join(name), b"not-msgpack").expect("receipt body is not authority");
    }

    let loaded = load_session_part_receipts(&fragment, &version_id, &manifest)
        .expect("load receipts")
        .expect("session exists");
    let loaded_receipt = loaded[7].as_ref().expect("part receipt");
    assert_eq!(loaded_receipt.part_index, 7);
    assert_eq!(loaded_receipt.size_bytes, 1024);
    assert_eq!(loaded_receipt.shard_size_bytes, 512);
    assert_eq!(loaded_receipt.created_at, manifest.created_at);
    assert_eq!(loaded_receipt.checksum, Some(0x0102_0304));
    assert_eq!(
        loaded_receipt.shard_checksums,
        vec![0x1111_1111, 0x2222_2222, 0x3333_3333]
    );
}

#[test]
fn multipart_manifest_and_binding_are_stable_session_truth() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-upload-truth-{}", Ulid::new());
    let version_id = Ulid::new().to_string();
    let session_id = Ulid::new().to_string();
    let manifest = UploadSessionManifest {
        fragment: fragment.clone(),
        object_key: "@o_01KSESSIONPHYSICALOBJECT000".to_string(),
        version_id: version_id.clone(),
        session_id: session_id.clone(),
        total_size_bytes: 6 * 1024,
        expected_parts: 3,
        part_size_bytes: 2 * 1024,
        content_type: "application/octet-stream".to_string(),
        created_at: 1_777_905_100,
        stripe_size_bytes: 512,
        data_shards: 2,
        parity_shards: 1,
        placement_roots: vec![
            "/blob/0".to_string(),
            "/blob/1".to_string(),
            "/blob/2".to_string(),
        ],
    };
    let binding = PendingUploadBinding {
        fragment: fragment.clone(),
        logical_path: "videos/movie.mp4".to_string(),
        physical_object_id: manifest.object_key.clone(),
        version_id: version_id.clone(),
        session_id: session_id.clone(),
        created_at_unix: 1_777_905_101,
    };

    open_session(&manifest, None).expect("open multipart session");
    write_pending_binding(&binding).expect("write pending namespace binding");

    let manifest_bytes_before = roots
        .iter()
        .map(|root| {
            fs::read(
                fragment_root_on(root, &fragment)
                    .join(UPLOADS_DIR)
                    .join(&version_id)
                    .join(MANIFEST_FILE),
            )
            .expect("read manifest")
        })
        .collect::<Vec<_>>();
    let binding_bytes_before = roots
        .iter()
        .map(|root| {
            fs::read(
                fragment_root_on(root, &fragment)
                    .join(UPLOADS_DIR)
                    .join(&version_id)
                    .join(BINDING_FILE),
            )
            .expect("read binding")
        })
        .collect::<Vec<_>>();

    for part_index in 0..manifest.expected_parts {
        let receipt = SessionPartReceipt {
            part_index,
            size_bytes: manifest.part_size_bytes,
            shard_size_bytes: 1024,
            created_at: 1_777_905_200 + part_index as i64,
            checksum: Some(0xaaaa_0000 + part_index as u32),
            shard_checksums: vec![
                0xbbbb_0000 + part_index as u32,
                0xcccc_0000 + part_index as u32,
                0xdddd_0000 + part_index as u32,
            ],
        };
        commit_session_part_receipt(&fragment, &version_id, &receipt)
            .expect("write zero-decode receipt");
    }

    for (idx, root) in roots.iter().enumerate() {
        let dir = fragment_root_on(root, &fragment)
            .join(UPLOADS_DIR)
            .join(&version_id);
        assert_eq!(
            fs::read(dir.join(MANIFEST_FILE)).expect("manifest after receipts"),
            manifest_bytes_before[idx],
            "part receipt commits must not rewrite manifest.ndx"
        );
        assert_eq!(
            fs::read(dir.join(BINDING_FILE)).expect("binding after receipts"),
            binding_bytes_before[idx],
            "part receipt commits must not rewrite binding.ndx"
        );
        assert!(
            !read_dir_names(&dir)
                .expect("session dir entries")
                .iter()
                .any(|name| name.starts_with(PART_META_PREFIX) && name.ends_with(".ndx")),
            "receipt-only hot path must not create heavy part metadata records"
        );
    }

    let loaded = load_session(&fragment, &version_id)
        .expect("load session")
        .expect("session exists");
    assert_eq!(loaded.manifest.version_id, manifest.version_id);
    assert_eq!(loaded.manifest.expected_parts, manifest.expected_parts);
    assert_eq!(loaded.part_receipts.len(), manifest.expected_parts as usize);

    let loaded_binding = read_pending_binding(&fragment, &version_id)
        .expect("read binding")
        .expect("binding exists");
    assert_eq!(loaded_binding.logical_path, "videos/movie.mp4");
    assert_eq!(loaded_binding.physical_object_id, manifest.object_key);
}

#[test]
fn removing_multipart_session_clears_manifest_binding_and_receipts_on_all_replicas() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-upload-remove-{}", Ulid::new());
    let version_id = Ulid::new().to_string();
    let session_id = Ulid::new().to_string();
    let manifest = UploadSessionManifest {
        fragment: fragment.clone(),
        object_key: "@o_01KSESSIONDELETE000000000".to_string(),
        version_id: version_id.clone(),
        session_id: session_id.clone(),
        total_size_bytes: 1024,
        expected_parts: 1,
        part_size_bytes: 1024,
        content_type: "application/octet-stream".to_string(),
        created_at: 1_777_905_300,
        stripe_size_bytes: 512,
        data_shards: 1,
        parity_shards: 1,
        placement_roots: vec!["/blob/0".to_string(), "/blob/1".to_string()],
    };
    open_session(&manifest, None).expect("open session");
    write_pending_binding(&PendingUploadBinding {
        fragment: fragment.clone(),
        logical_path: "docs/delete-me.bin".to_string(),
        physical_object_id: manifest.object_key.clone(),
        version_id: version_id.clone(),
        session_id,
        created_at_unix: 1_777_905_301,
    })
    .expect("write binding");
    commit_session_part_receipt(
        &fragment,
        &version_id,
        &SessionPartReceipt {
            part_index: 0,
            size_bytes: 1024,
            shard_size_bytes: 1024,
            created_at: 1_777_905_302,
            checksum: Some(0x0102_0304),
            shard_checksums: vec![0x1111_1111, 0x2222_2222],
        },
    )
    .expect("write receipt");

    assert!(remove_session(&fragment, &version_id).expect("remove session"));
    for root in roots {
        assert!(
            !fragment_root_on(&root, &fragment)
                .join(UPLOADS_DIR)
                .join(&version_id)
                .exists(),
            "session removal must clear manifest, binding, and receipts on every index replica"
        );
    }
    assert!(
        read_pending_binding(&fragment, &version_id)
            .expect("read binding after remove")
            .is_none(),
        "removed upload sessions must not leave pending namespace bindings"
    );
}

#[test]
fn root_listing_hides_internal_planes_and_blob_aliases() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-root-hidden-{}", Ulid::new());
    publish_logical_object(&roots, &fragment, "docs/video.mp4", 123);

    for root in &roots {
        let fragment_root = fragment_root_on(root, &fragment);
        for internal in [
            OBJECTS_DIR,
            UPLOADS_DIR,
            TRASH_DIR,
            TMP_DIR,
            LOCKS_DIR,
            GC_DIR,
            REPAIR_DIR,
            INTENTS_DIR,
            TRASH_JOBS_DIR,
            FRAGMENT_META_FILE,
        ] {
            let path = fragment_root.join(internal);
            if internal == FRAGMENT_META_FILE {
                fs::write(path, b"fragment-meta").expect("write internal file");
            } else {
                fs::create_dir_all(path).expect("create internal dir");
            }
        }
        fs::create_dir_all(fragment_root.join("@o_fake_user_visible"))
            .expect("create raw object-like dir");
    }

    let page = scan_live_heads_page(&fragment, Some(String::new()), None, 100, None, true)
        .expect("root list");
    let records = page
        .entries
        .iter()
        .map(|entry| entry.record.as_str())
        .collect::<Vec<_>>();
    assert_eq!(records, vec!["docs"]);
    assert_eq!(page.common_prefixes, vec!["docs"]);
}

#[test]
fn latest_head_uses_max_ulid_and_ignores_invalid_ndx_names() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let index_roots = ensure_test_index_roots();
    let blob_roots = ensure_test_blob_roots();
    let fragment = format!("n1-head-{}", Ulid::new());
    let object_id = new_object_id();
    let old_id = "01K00000000000000000000000";
    let new_id = "01K00000000000000000000001";

    let mut old = test_metadata(&fragment, &object_id, 111);
    old.version_id = old_id.to_string();
    old.created_at = 1_777_905_400;
    let old_bytes = encode(&old).expect("encode old metadata");

    let mut new = test_metadata(&fragment, &object_id, 222);
    new.version_id = new_id.to_string();
    new.created_at = 1_777_905_500;
    let new_bytes = encode(&new).expect("encode new metadata");

    for root in &index_roots {
        publish_namespace_link(root, &fragment, "docs/versioned.bin", &object_id)
            .expect("publish namespace link");
    }
    for root in &blob_roots {
        let dir = live_object_dir(root, &fragment, &object_id).expect("object dir");
        fs::create_dir_all(&dir).expect("create object dir");

        write_atomic(&dir.join(ndx_file_name(&old)), &old_bytes).expect("write old version");

        fs::write(dir.join("not-a-valid-version.ndx"), b"garbage").expect("write invalid ndx name");

        write_atomic(&dir.join(ndx_file_name(&new)), &new_bytes).expect("write new version");
    }

    let page = scan_live_heads_page(&fragment, Some("docs".to_string()), None, 10, None, true)
        .expect("list latest head");
    assert_eq!(page.objects.len(), 1);
    assert_eq!(page.objects[0].version_id, new_id);
    assert_eq!(page.objects[0].total_size_bytes, 222);

    let full = read_current_live_head(&fragment, &object_id)
        .expect("read latest")
        .expect("head exists");
    assert_eq!(full.version_id, new_id);
    assert_eq!(full.total_size_bytes, 222);
}

#[test]
fn current_head_uses_max_ulid_across_blob_placements() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let index_roots = ensure_test_index_roots();
    let blob_roots = ensure_test_blob_roots();
    assert!(
        blob_roots.len() >= 2,
        "head selection requires at least two blob placements"
    );
    let fragment = format!("n1-replica-head-{}", Ulid::new());
    let object_id = new_object_id();
    let old_id = "01K00000000000000000000000";
    let new_id = "01K00000000000000000000009";

    for root in &index_roots {
        publish_namespace_link(root, &fragment, "docs/stale-replica.pdf", &object_id)
            .expect("publish namespace link");
    }

    let old_dir =
        live_object_dir(&blob_roots[0], &fragment, &object_id).expect("old placement dir");
    fs::create_dir_all(&old_dir).expect("create old placement");
    let mut old = test_metadata(&fragment, &object_id, 4_490_023);
    old.version_id = old_id.to_string();
    old.created_at = 1_779_883_031;
    write_atomic(
        &old_dir.join(ndx_file_name(&old)),
        &encode(&old).expect("encode old replica metadata"),
    )
    .expect("write old replica");

    let new_dir =
        live_object_dir(&blob_roots[1], &fragment, &object_id).expect("new placement dir");
    fs::create_dir_all(&new_dir).expect("create new placement");
    let mut new = test_metadata(&fragment, &object_id, 7_983_741);
    new.version_id = new_id.to_string();
    new.created_at = 1_779_892_004;
    write_atomic(
        &new_dir.join(ndx_file_name(&new)),
        &encode(&new).expect("encode new replica metadata"),
    )
    .expect("write new replica");

    let full = read_current_live_head(&fragment, &object_id)
        .expect("read latest across replicas")
        .expect("head exists");
    assert_eq!(full.version_id, new_id);
    assert_eq!(full.total_size_bytes, 7_983_741);
}

#[test]
fn object_metadata_uses_highest_placement_generation_and_rejects_ties() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let _ = ensure_test_index_roots();
    let blob_roots = ensure_test_blob_roots();
    assert!(blob_roots.len() >= 2);
    let fragment = format!("n1-placement-generation-{}", Ulid::new());
    let object_id = new_object_id();
    let version_id = Ulid::new().to_string();

    let mut older = test_metadata(&fragment, &object_id, 111);
    older.version_id = version_id.clone();
    older.placement_generation = 4;
    let mut newer = older.clone();
    newer.total_size_bytes = 222;
    newer.placement_generation = 5;

    let older_dir =
        live_object_dir(&blob_roots[0], &fragment, &object_id).expect("older object dir");
    let newer_dir =
        live_object_dir(&blob_roots[1], &fragment, &object_id).expect("newer object dir");
    fs::create_dir_all(&older_dir).expect("create older metadata dir");
    fs::create_dir_all(&newer_dir).expect("create newer metadata dir");
    write_atomic(
        &older_dir.join(ndx_file_name(&older)),
        &encode(&older).expect("encode older placement"),
    )
    .expect("write older placement");
    write_atomic(
        &newer_dir.join(ndx_file_name(&newer)),
        &encode(&newer).expect("encode newer placement"),
    )
    .expect("write newer placement");

    let selected = read_live_version(&fragment, &object_id, &version_id)
        .expect("select authoritative placement")
        .expect("metadata exists");
    assert_eq!(selected.placement_generation, 5);
    assert_eq!(selected.total_size_bytes, 222);

    newer.placement_generation = 4;
    write_atomic(
        &newer_dir.join(ndx_file_name(&newer)),
        &encode(&newer).expect("encode divergent placement"),
    )
    .expect("write divergent placement");
    let error = read_live_version(&fragment, &object_id, &version_id)
        .expect_err("same generation divergence must fail closed");
    assert!(error.to_string().contains("same-generation divergent"));

    let _ = fs::remove_dir_all(fragment_root_on(&blob_roots[0], &fragment));
    let _ = fs::remove_dir_all(fragment_root_on(&blob_roots[1], &fragment));
}

#[test]
fn namespace_repair_does_not_merge_colocated_object_versions() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let index_roots = ensure_test_index_roots();
    let blob_roots = ensure_test_blob_roots();
    assert!(
        blob_roots.len() >= 2,
        "split placement proof requires at least two blob roots"
    );
    let fragment = format!("n1-repair-split-{}", Ulid::new());
    let object_id = new_object_id();
    let logical_path = "docs/split-version.pdf";
    let old_id = "01K00000000000000000000000";
    let middle_id = "01K00000000000000000000001";
    let new_id = "01K00000000000000000000009";

    for root in &index_roots {
        publish_namespace_link(root, &fragment, logical_path, &object_id)
            .expect("publish namespace link");
    }

    for (version_id, size) in [(old_id, 4_490_023), (middle_id, 4_490_023)] {
        let dir =
            live_object_dir(&blob_roots[0], &fragment, &object_id).expect("old placement dir");
        fs::create_dir_all(&dir).expect("create old placement");
        let mut meta = test_metadata(&fragment, &object_id, size);
        meta.version_id = version_id.to_string();
        meta.created_at = 1_779_883_031;
        write_atomic(
            &dir.join(ndx_file_name(&meta)),
            &encode(&meta).expect("encode old metadata"),
        )
        .expect("write old metadata");
    }

    let new_dir =
        live_object_dir(&blob_roots[1], &fragment, &object_id).expect("new placement dir");
    fs::create_dir_all(&new_dir).expect("create new placement");
    let mut new = test_metadata(&fragment, &object_id, 7_983_741);
    new.version_id = new_id.to_string();
    new.created_at = 1_779_892_004;
    write_atomic(
        &new_dir.join(ndx_file_name(&new)),
        &encode(&new).expect("encode new metadata"),
    )
    .expect("write new metadata");

    let full = read_current_live_head(&fragment, &object_id)
        .expect("read latest before repair")
        .expect("head exists before repair");
    assert_eq!(full.version_id, new_id);

    let before = blob_roots
        .iter()
        .map(|root| {
            read_dir_names(&live_object_dir(root, &fragment, &object_id).expect("object placement"))
                .unwrap_or_default()
        })
        .collect::<Vec<_>>();
    repair_fragment_all_partition_replicas(&fragment, false).expect("repair namespace replicas");
    let after = blob_roots
        .iter()
        .map(|root| {
            read_dir_names(&live_object_dir(root, &fragment, &object_id).expect("object placement"))
                .unwrap_or_default()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        after, before,
        "namespace repair must not merge, copy, or move immutable object versions"
    );

    let repaired = inspect_fragment_partition_replicas(&fragment, &index_roots);
    assert!(
        repaired.healthy,
        "namespace replicas should remain healthy after repair"
    );
}
