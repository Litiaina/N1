use super::*;
use crate::core::disk_manager::{BLOB_STORE_DISKS, INDEX_STORE_DISKS, ObjectMetadata};
use ulid::Ulid;

pub(super) fn ensure_test_blob_roots() -> Vec<PathBuf> {
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

pub(super) fn ensure_test_index_roots() -> Vec<PathBuf> {
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
fn ordinary_metadata_publication_does_not_initialize_unused_fragment_directories() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let index_roots = ensure_test_index_roots();
    let blob_roots = ensure_test_blob_roots();
    let fragment = Ulid::new().to_string();
    let object_id = new_object_id();
    let metadata = test_metadata(&fragment, &object_id, 4096);
    let shard = PathBuf::from(&metadata.parts[0].shards[0].absolute_path);
    let before = fs::read(&shard).expect("read committed payload fixture");
    let mut roots = index_roots;
    roots.extend(blob_roots);
    roots.sort();
    roots.dedup();
    for root in &roots {
        let fragment_root = fragment_root_on(root, &fragment);
        assert!(!fragment_root.join(UPLOADS_DIR).exists());
        assert!(!fragment_root.join(TRASH_DIR).exists());
    }

    upsert_live_version(&metadata).expect("publish strict NDX at existing shard placement");
    let saved = read_live_version(&fragment, &object_id, &metadata.version_id)
        .expect("read published NDX")
        .expect("published NDX exists");
    assert_eq!(encode(&saved).unwrap(), encode(&metadata).unwrap());
    assert_eq!(fs::read(&shard).unwrap(), before);
    for root in &roots {
        let fragment_root = fragment_root_on(root, &fragment);
        assert!(
            !fragment_root.join(UPLOADS_DIR).exists(),
            "ordinary metadata publication must not create an upload workspace"
        );
        assert!(
            !fragment_root.join(TRASH_DIR).exists(),
            "ordinary metadata publication must not initialize trash"
        );
        if fragment_root.exists() {
            fs::remove_dir_all(fragment_root).expect("remove owned metadata fixture");
        }
    }
}

#[test]
fn metadata_publication_rejects_missing_payload_without_recreating_fragment() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let index_roots = ensure_test_index_roots();
    let blob_roots = ensure_test_blob_roots();
    let fragment = Ulid::new().to_string();
    let object_id = new_object_id();
    let metadata = test_metadata(&fragment, &object_id, 4096);
    let mut roots = index_roots;
    roots.extend(blob_roots);
    roots.sort();
    roots.dedup();
    for root in &roots {
        let fragment_root = fragment_root_on(root, &fragment);
        if fragment_root.exists() {
            fs::remove_dir_all(fragment_root).expect("remove owned shard placement");
        }
    }

    for durability in [IndexWriteDurability::Strict, IndexWriteDurability::Relaxed] {
        assert!(matches!(
            upsert_live_version_with_durability(&metadata, durability),
            Err(AppError::ServiceUnavailable(_))
        ));
        for root in &roots {
            assert!(
                !fragment_root_on(root, &fragment).exists(),
                "failed NDX publication must not recreate a removed fragment"
            );
        }
    }
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

    let object_dir = live_object_dir(&blob_roots[0], &fragment, &object_id).expect("object dir");
    let object_entries_before = read_dir_names(&object_dir).expect("read object placement");
    fs::remove_file(crate::namespace::flat::path(
        &roots[0],
        &fragment,
        "docs/video.mp4",
    ))
    .expect("wipe namespace symlink");

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
        fs::symlink_metadata(crate::namespace::flat::path(
            &roots[0],
            &fragment,
            "docs/video.mp4"
        ))
        .expect("namespace symlink restored")
        .file_type()
        .is_file()
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
fn replica_repair_preserves_inflight_atomic_writes_and_local_workspaces() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-repair-inflight-{}", Ulid::new());
    publish_logical_object(&roots, &fragment, "docs/live.bin", 123);
    let temp_like_user_path = format!("docs/.user.tmp.{}", Ulid::new());
    publish_logical_object(&roots, &fragment, &temp_like_user_path, 123);
    assert!(!internal_namespace_entry_name(
        base_name(&temp_like_user_path).as_str()
    ));
    let repair_temporary = format!("{REPLICA_TMP_PREFIX}{}", Ulid::new());
    assert!(
        internal_namespace_entry_name(&repair_temporary),
        "repair staging must never be exposed by listing"
    );
    assert!(reject_reserved_user_path(&repair_temporary).is_err());
    let destination = fragment_root_on(&roots[0], &fragment);
    fs::remove_file(crate::namespace::flat::path(
        &roots[0],
        &fragment,
        "docs/live.bin",
    ))
    .unwrap();
    fs::remove_file(crate::namespace::flat::path(
        &roots[0],
        &fragment,
        &temp_like_user_path,
    ))
    .unwrap();
    let interrupted_copy_dir = destination.join(UPLOADS_DIR).join("interrupted-copy");
    fs::create_dir_all(&interrupted_copy_dir).unwrap();
    for _ in 0..8 {
        fs::write(
            interrupted_copy_dir.join(format!("{REPLICA_TMP_PREFIX}{}", Ulid::new())),
            b"unfinished replica copy",
        )
        .unwrap();
    }
    let mut pending = Vec::new();
    for root in &roots {
        let root = fragment_root_on(root, &fragment);
        for relative in ["..fragment.ndx.tmp.", ".uploads/session/.manifest.ndx.tmp."] {
            let path = root.join(format!("{relative}{}", Ulid::new()));
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, b"in-flight writer bytes").unwrap();
            pending.push(path);
        }
        for name in [TMP_DIR, LOCKS_DIR, GC_DIR, REPAIR_DIR] {
            let path = root.join(name).join("local-operation");
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, root.as_os_str().as_encoded_bytes()).unwrap();
            pending.push(path);
        }
    }

    repair_fragment_all_partition_replicas(&fragment, false).unwrap();
    assert!(crate::namespace::flat::path(&roots[0], &fragment, "docs/live.bin").is_file());
    assert!(
        crate::namespace::flat::path(&roots[0], &fragment, &temp_like_user_path).is_file(),
        "valid user names must not be mistaken for staging files"
    );
    for path in &pending {
        assert!(
            path.is_file(),
            "repair removed an active writer's file: {path:?}"
        );
        if path.file_name().unwrap() != "local-operation" {
            // The original writer must still be able to complete its rename.
            fs::rename(path, path.with_extension("committed")).unwrap();
        } else {
            let owner = path.parent().unwrap().parent().unwrap();
            assert_eq!(
                fs::read(path).unwrap(),
                owner.as_os_str().as_encoded_bytes()
            );
        }
    }
    for root in &roots {
        let root = fragment_root_on(root, &fragment);
        let staged = fs::read_dir(&root)
            .unwrap()
            .flatten()
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("..fragment.ndx.tmp.")
            })
            .count();
        assert_eq!(
            staged, 1,
            "repair must not copy another root's temporary files"
        );
    }
}

#[test]
fn trash_scan_ignores_empty_organizers_and_rejects_corrupt_records() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-trash-inflight-{}", Ulid::new());
    publish_logical_object(&roots, &fragment, "docs/live.bin", 123);
    let record = crate::namespace::flat::trash_key(&fragment, "docs/live.bin")
        .unwrap()
        .unwrap();
    for root in &roots {
        fs::create_dir_all(
            fragment_root_on(root, &fragment)
                .join(TRASH_DIR)
                .join("empty/prefix"),
        )
        .unwrap();
    }
    let records = scan_trash_records(&fragment).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].original_path, "docs/live.bin");
    for root in &roots {
        fs::write(
            fragment_root_on(root, &fragment)
                .join(TRASH_DIR)
                .join(&record.source_relative_path),
            b"corrupt",
        )
        .unwrap();
    }
    assert!(scan_trash_records(&fragment).is_err());
}

#[test]
fn replica_inspection_rejects_conflicting_checksums_valid_namespace_identity() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-replica-divergence-{}", Ulid::new());
    publish_logical_object(&roots, &fragment, "docs/report.bin", 123);
    let target = crate::namespace::flat::path(&roots[1], &fragment, "docs/report.bin");
    let mut divergent = crate::namespace::flat::read(&target).unwrap().unwrap();
    divergent.object_id = new_object_id();
    crate::namespace::flat::write(&roots[1], &target, &divergent, false).unwrap();
    assert!(!inspect_fragment_partition_replicas(&fragment, &roots).healthy);
    assert!(matches!(
        repair_fragment_all_partition_replicas(&fragment, false),
        Err(AppError::Conflict(_))
    ));
    assert_eq!(
        crate::namespace::flat::read(&target).unwrap(),
        Some(divergent)
    );
}

#[test]
fn replica_inspection_is_independent_of_directory_iteration_order() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-replica-order-{}", Ulid::new());
    let a = new_object_id();
    let b = new_object_id();
    publish_namespace_link(&roots[0], &fragment, "alpha/a.bin", &a).unwrap();
    publish_namespace_link(&roots[0], &fragment, "beta/b.bin", &b).unwrap();
    publish_namespace_link(&roots[1], &fragment, "beta/b.bin", &b).unwrap();
    publish_namespace_link(&roots[1], &fragment, "alpha/a.bin", &a).unwrap();
    assert!(inspect_fragment_partition_replicas(&fragment, &roots).healthy);
}

#[test]
fn redistribution_refuses_conflicting_namespace_identity() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-redistribution-divergence-{}", Ulid::new());
    publish_logical_object(&roots, &fragment, "docs/report.bin", 123);
    let target = crate::namespace::flat::path(&roots[0], &fragment, "docs/report.bin");
    let mut p = crate::namespace::flat::read(&target).unwrap().unwrap();
    p.object_id = new_object_id();
    crate::namespace::flat::write(&roots[0], &target, &p, false).unwrap();
    assert!(matches!(
        sync_fragment_partition_replicas(&fragment),
        Err(AppError::Conflict(_))
    ));
}

#[test]
fn namespace_repair_preserves_both_names_after_interrupted_copy_delete() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-repair-copy-delete-{}", Ulid::new());
    let object = publish_logical_object(&roots, &fragment, "before/item.bin", 123);
    let old = crate::namespace::flat::read(&crate::namespace::flat::path(
        &roots[0],
        &fragment,
        "before/item.bin",
    ))
    .unwrap()
    .unwrap();
    crate::namespace::flat::move_key(&fragment, "before/item.bin", "after/item.bin").unwrap();
    crate::namespace::flat::write(
        &roots[0],
        &crate::namespace::flat::path(&roots[0], &fragment, "before/item.bin"),
        &old,
        true,
    )
    .unwrap();
    assert!(
        crate::namespace::flat::lookup(&fragment, "before/item.bin")
            .unwrap()
            .is_some()
    );
    sync_fragment_partition_replicas(&fragment).unwrap();
    for root in &roots {
        assert!(crate::namespace::flat::path(root, &fragment, "before/item.bin").exists());
    }
    assert_eq!(
        crate::namespace::flat::lookup(&fragment, "after/item.bin")
            .unwrap()
            .unwrap()
            .object_id,
        object
    );
}

#[test]
fn namespace_repair_restores_checksums_from_surviving_copy() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-repair-corrupt-{}", Ulid::new());
    let object = publish_logical_object(&roots, &fragment, "docs/item.bin", 34);
    let target = crate::namespace::flat::path(&roots[0], &fragment, "docs/item.bin");
    fs::write(&target, b"corrupt").unwrap();
    let report = repair_fragment_all_partition_replicas(&fragment, false).unwrap();
    assert!(report.2 > 0);
    assert_eq!(
        crate::namespace::flat::read(&target)
            .unwrap()
            .unwrap()
            .object_id,
        object
    );
}

#[test]
fn namespace_repair_uses_each_key_not_whole_fragment_authority() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-repair-per-key-{}", Ulid::new());
    let a = publish_logical_object(&roots, &fragment, "docs/a.bin", 11);
    let b = publish_logical_object(&roots, &fragment, "docs/b.bin", 22);
    fs::remove_file(crate::namespace::flat::path(
        &roots[0],
        &fragment,
        "docs/a.bin",
    ))
    .unwrap();
    fs::remove_file(crate::namespace::flat::path(
        &roots[1],
        &fragment,
        "docs/b.bin",
    ))
    .unwrap();
    repair_fragment_all_partition_replicas(&fragment, false).unwrap();
    for root in &roots {
        assert_eq!(
            crate::namespace::flat::read(&crate::namespace::flat::path(
                root,
                &fragment,
                "docs/a.bin"
            ))
            .unwrap()
            .unwrap()
            .object_id,
            a
        );
        assert_eq!(
            crate::namespace::flat::read(&crate::namespace::flat::path(
                root,
                &fragment,
                "docs/b.bin"
            ))
            .unwrap()
            .unwrap()
            .object_id,
            b
        );
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

    let listed = scan_live_heads_page(&fragment, Some("docs/".to_string()), None, 10, None, true)
        .expect("zero-decode list");
    assert_eq!(listed.entries.len(), 1);
    assert!(listed.objects.is_empty());
    assert_eq!(listed.entries[0].record, "bad-body.mp4");
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

    let first = scan_live_heads_page(&fragment, Some("docs/".to_string()), None, 2, None, true)
        .expect("first page");
    assert_eq!(
        first
            .entries
            .iter()
            .map(|meta| meta.record.as_str())
            .collect::<Vec<_>>(),
        vec!["a.svg", "b.svg"]
    );
    let second = scan_live_heads_page(
        &fragment,
        Some("docs/".to_string()),
        None,
        2,
        first.continuation_token,
        true,
    )
    .expect("second page");
    assert_eq!(
        second
            .entries
            .iter()
            .map(|meta| meta.record.as_str())
            .collect::<Vec<_>>(),
        vec!["c.svg"]
    );
}

#[test]
fn bounded_live_pagination_covers_a_wide_replicated_directory_without_skips() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let index_roots = ensure_test_index_roots();
    let blob_root = ensure_test_blob_roots()
        .into_iter()
        .next()
        .expect("test blob root");
    let fragment = format!("n1-wide-page-{}", Ulid::new());
    let version = Ulid::new().to_string();
    let total = 4_097usize;

    for root in &index_roots {
        fs::create_dir_all(fragment_root_on(root, &fragment).join("wide"))
            .expect("create wide namespace directory");
    }
    // Only fixture construction is parallel: every holder keeps its real
    // publication barriers, and pagination starts after all workers join.
    std::thread::scope(|scope| {
        let workers = (0..8)
            .map(|worker| {
                let index_roots = &index_roots;
                let blob_root = &blob_root;
                let fragment = &fragment;
                let version = &version;
                scope.spawn(move || {
                    for index in (0..total).rev().skip(worker).step_by(8) {
                        let object_id = new_object_id();
                        let logical_name = format!("item-{index:05}.bin");
                        let object_dir = live_object_dir(blob_root, fragment, &object_id)
                            .expect("wide object dir");
                        fs::create_dir_all(&object_dir).expect("create wide object dir");
                        let ndx_name = format!(
                            "{version}.{}.{}.{}.ndx",
                            index + 1,
                            1_777_905_000_000_000i128 + index as i128,
                            packed_extension_key(&logical_name)
                        );
                        fs::write(object_dir.join(ndx_name), b"zero-decode-page-test")
                            .expect("write zero-decode ndx name");
                        for root in index_roots {
                            publish_namespace_link(
                                root,
                                fragment,
                                &format!("wide/{logical_name}"),
                                &object_id,
                            )
                            .unwrap();
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().expect("wide fixture worker");
        }
    });

    let mut cursor = None;
    let mut observed = Vec::with_capacity(total);
    loop {
        let page =
            scan_live_heads_page(&fragment, Some("wide/".to_string()), None, 73, cursor, true)
                .expect("wide namespace page");
        observed.extend(page.entries.into_iter().map(|entry| entry.record));
        cursor = page.continuation_token;
        if cursor.is_none() {
            break;
        }
    }

    assert_eq!(observed.len(), total);
    assert!(observed.windows(2).all(|window| window[0] < window[1]));
    assert_eq!(observed.first().map(String::as_str), Some("item-00000.bin"));
    assert_eq!(observed.last().map(String::as_str), Some("item-04096.bin"));
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
        let old_link = crate::namespace::flat::path(root, &fragment, old_key);
        let new_link = crate::namespace::flat::path(root, &fragment, new_key);
        fs::rename(&old_link, &new_link).expect("simulate N1 namespace rename");
        assert_eq!(
            read_namespace_symlink_object_id(root, &fragment, &new_link).expect("read moved link"),
            Some(object_id.clone())
        );
    }

    let page =
        scan_live_full_metadata_page(&fragment, Some("docs/".to_string()), None, 10, None, true)
            .expect("full metadata page");
    assert_eq!(page.objects.len(), 1);
    assert_eq!(page.objects[0].object_key, object_id);
}

#[test]
fn maintenance_object_fanout_streams_and_presence_probes_do_not_decode_ndx() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_blob_roots();
    let root = &roots[0];
    let fragment = Ulid::new().to_string();
    let version = Ulid::new().to_string();
    let mut expected = BTreeSet::new();
    let mut first = None;
    for _ in 0..257 {
        let object_id = new_object_id();
        let directory = object_index_dir(root, &fragment, &object_id).unwrap();
        fs::create_dir_all(&directory).unwrap();
        expected.insert(object_id);
        first.get_or_insert(directory);
    }
    let mut visits = 0;
    visit_local_object_dirs(root, &fragment, |_, _| {
        visits += 1;
        Ok(false)
    })
    .unwrap();
    assert_eq!(visits, 1, "presence probing must stop at its first result");
    let observed = local_object_dirs_under_root(root, &fragment)
        .unwrap()
        .into_iter()
        .map(|(_, id)| id)
        .collect::<BTreeSet<_>>();
    assert_eq!(observed, expected);
    assert!(!local_root_has_blob_payload(root, &fragment));
    assert!(!fragment_has_object_index_records(&fragment, &roots));
    let directory = first.unwrap();
    fs::write(
        directory.join(format!("{version}.1.1.0.ndx")),
        b"not decoded by presence probe",
    )
    .unwrap();
    assert!(fragment_has_object_index_records(&fragment, &roots));
    assert_eq!(replicas::count_object_index_records(root, &fragment), 1);
    assert!(!local_root_has_blob_payload(root, &fragment));
    let payload = directory.join(&version);
    fs::create_dir(&payload).unwrap();
    fs::write(payload.join("part_0_shard_0.blob"), b"payload probe").unwrap();
    assert!(local_root_has_blob_payload(root, &fragment));
    assert!(visit_local_object_dirs(root, &fragment, |_, _| Err(AppError::NotFound)).is_err());
}

#[test]
fn object_directory_cursor_resumes_without_inventory_and_does_not_follow_links() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_blob_roots();
    let root = &roots[0];
    let fragment = Ulid::new().to_string();
    let mut expected = BTreeSet::new();
    for _ in 0..259 {
        let object_id = new_object_id();
        fs::create_dir_all(object_index_dir(root, &fragment, &object_id).unwrap()).unwrap();
        expected.insert(object_id);
    }
    let external = std::env::temp_dir().join(format!("n1-gc-external-{}", Ulid::new()));
    fs::create_dir_all(external.join("aa").join("@o_external")).unwrap();
    let objects = blob_fragment_root_on(root, &fragment).join(OBJECTS_DIR);
    std::os::unix::fs::symlink(&external, objects.join("escape")).unwrap();
    let first = expected.first().unwrap();
    let first_dir = object_index_dir(root, &fragment, first).unwrap();
    std::os::unix::fs::symlink(&external, first_dir.parent().unwrap().join("@o_external")).unwrap();
    let mut cursor = LocalObjectDirCursor::new(root, &fragment).unwrap();
    let mut observed = BTreeSet::new();
    while let Some((path, id)) = cursor.next_object().unwrap() {
        assert!(path.starts_with(&objects));
        assert!(cursor.directories.len() <= 3);
        assert!(observed.insert(id));
        // The same cursor is retained across arbitrary caller batch boundaries.
        if observed.len() % 7 == 0 {
            std::thread::yield_now();
        }
    }
    assert_eq!(observed, expected);
    assert!(cursor.next_object().unwrap().is_none());
    assert!(cursor.directories.is_empty());
    fs::remove_dir_all(external).unwrap();
}

#[test]
fn object_directory_cursor_rejects_a_redirected_objects_root() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_blob_roots();
    let root = &roots[0];
    let fragment = Ulid::new().to_string();
    let objects = blob_fragment_root_on(root, &fragment).join(OBJECTS_DIR);
    fs::create_dir_all(objects.parent().unwrap()).unwrap();
    let external = std::env::temp_dir().join(format!("n1-gc-root-external-{}", Ulid::new()));
    fs::create_dir_all(&external).unwrap();
    std::os::unix::fs::symlink(&external, &objects).unwrap();
    assert!(LocalObjectDirCursor::new(root, &fragment).is_err());
    fs::remove_dir_all(external).unwrap();
}

#[test]
fn object_directory_cursor_rejects_a_redirected_physical_fragment() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_blob_roots();
    let root = &roots[0];
    let fragment = Ulid::new().to_string();
    let physical = blob_fragment_root_on(root, &fragment);
    let external = std::env::temp_dir().join(format!("n1-gc-fragment-external-{}", Ulid::new()));
    fs::create_dir_all(external.join(OBJECTS_DIR)).unwrap();
    std::os::unix::fs::symlink(&external, &physical).unwrap();
    assert!(LocalObjectDirCursor::new(root, &fragment).is_err());
    assert!(external.join(OBJECTS_DIR).is_dir());
    fs::remove_file(physical).unwrap();
    fs::remove_dir_all(external).unwrap();
}

#[test]
fn maintenance_cursor_batches_are_single_pass_and_deduplicate_surviving_replicas() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-maintenance-cursor-{}", Ulid::new());
    let mut expected = BTreeSet::new();
    for i in 0..137 {
        let key = format!("projects/p{:03}/file-{i:04}.bin", i / 16);
        let object = publish_logical_object(&roots, &fragment, &key, i + 1);
        expected.insert(object);
        // A missing lexical-first replica must not hide a live object.
        if i % 7 == 0 {
            let first = roots.iter().min().unwrap();
            fs::remove_file(crate::namespace::flat::path(first, &fragment, &key)).unwrap();
        }
    }
    let mut scan = MetadataScan::new(&fragment, false).unwrap();
    let mut observed = BTreeSet::new();
    loop {
        let batch = scan.next_batch(7).unwrap();
        assert!(batch.len() <= 7);
        if batch.is_empty() {
            break;
        }
        for metadata in batch {
            assert!(
                observed.insert(metadata.object_key),
                "replica emitted twice"
            );
        }
    }
    assert_eq!(observed, expected);
    assert!(
        scan.namespace.entries_visited() < 137 * roots.len() + 64,
        "batch advancement must not re-walk earlier entries"
    );
    let visited = scan.namespace.entries_visited();
    assert!(scan.next_batch(7).unwrap().is_empty());
    assert_eq!(
        scan.namespace.entries_visited(),
        visited,
        "completed cursor must stay complete"
    );
}

#[test]
fn maintenance_trash_cursor_survives_missing_and_corrupt_copies_without_rewriting_ndx() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-trash-cursor-{}", Ulid::new());
    let mut expected = BTreeSet::new();
    for i in 0..9 {
        let key = format!("old/project/file-{i}.txt");
        let object = publish_logical_object(&roots, &fragment, &key, 42);
        expected.insert(object);
        let record = crate::namespace::flat::trash_key(&fragment, &key)
            .unwrap()
            .unwrap();
        let first = fragment_root_on(roots.iter().min().unwrap(), &fragment)
            .join(TRASH_DIR)
            .join(record.source_relative_path);
        if i % 2 == 0 {
            fs::remove_file(first).unwrap();
        } else {
            fs::write(first, b"corrupt NCX replica").unwrap();
        }
    }
    assert!(
        MetadataScan::new(&fragment, false)
            .unwrap()
            .next_batch(1)
            .unwrap()
            .is_empty()
    );
    let ndx_before = expected
        .iter()
        .flat_map(|object| all_live_version_paths(&fragment, object).unwrap())
        .map(|path| {
            let bytes = fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect::<Vec<_>>();
    let mut scan = MetadataScan::new(&fragment, true).unwrap();
    let mut observed = BTreeSet::new();
    loop {
        let batch = scan.next_batch(2).unwrap();
        if batch.is_empty() {
            break;
        }
        for meta in batch {
            assert_eq!(meta.total_size_bytes, 42);
            assert!(observed.insert(meta.object_key));
        }
    }
    assert_eq!(observed, expected);
    for (path, bytes) in ndx_before {
        assert_eq!(fs::read(path).unwrap(), bytes);
    }
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
        scan_live_full_metadata_page(&fragment, Some("docs/".to_string()), None, 2, None, true)
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
        Some("docs/".to_string()),
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

    crate::namespace::flat::trash_key(&fragment, logical_path)
        .unwrap()
        .unwrap();

    let user_page =
        scan_trash_heads_page(&fragment, Some("docs/".to_string()), None, 10, None, true)
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

    crate::namespace::flat::trash_key(&fragment, logical_path)
        .unwrap()
        .unwrap();

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
fn offline_receipts_derive_physical_pieces_without_engine_initialization() {
    let dir = std::env::temp_dir().join(format!("n1-offline-receipts-{}", Ulid::new()));
    fs::create_dir(&dir).unwrap();
    let manifest = UploadSessionManifest {
        fragment: "alias".into(),
        object_key: Ulid::new().to_string(),
        version_id: Ulid::new().to_string(),
        session_id: Ulid::new().to_string(),
        total_size_bytes: 8192,
        expected_parts: 2,
        part_size_bytes: 4096,
        content_type: "application/octet-stream".into(),
        created_at: 1,
        stripe_size_bytes: 4096,
        data_shards: 1,
        parity_shards: 1,
        placement_roots: vec!["/first".into(), "/second".into()],
    };
    fs::write(
        dir.join("shard_p00000000000000000000_s0000.11111111.receipt"),
        [],
    )
    .unwrap();
    fs::write(
        dir.join("shard_p00000000000000000000_s0001.22222222.receipt"),
        [],
    )
    .unwrap();
    // Incomplete/unacknowledged part is not promoted to complete progress.
    fs::write(
        dir.join("shard_p00000000000000000001_s0000.33333333.receipt"),
        [],
    )
    .unwrap();
    let parts = offline_session_parts(&dir, manifest.clone(), "stable")
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].shards.len(), 2);
    assert_eq!(parts[0].shards[1].checksum, "22222222");
    assert!(
        parts[0].shards[0]
            .absolute_path
            .starts_with("/first/@f_stable/.objects/")
    );
    fs::write(
        dir.join("shard_p00000000000000000000_s0000.44444444.receipt"),
        [],
    )
    .unwrap();
    assert!(offline_session_parts(&dir, manifest, "stable").is_err());
    fs::remove_dir_all(dir).unwrap();
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

#[tokio::test]
async fn durable_finalize_retains_resume_truth_until_namespace_commit() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let blob_roots = ensure_test_blob_roots();
    let fragment = format!("durable-handoff-{}", Ulid::new());
    let object_id = new_object_id();
    let metadata = test_metadata(&fragment, &object_id, 4096);
    let manifest = UploadSessionManifest {
        fragment: fragment.clone(),
        object_key: object_id.clone(),
        version_id: metadata.version_id.clone(),
        session_id: metadata.upload_session_id.clone(),
        total_size_bytes: 4096,
        expected_parts: 1,
        part_size_bytes: 4096,
        content_type: metadata.content_type.clone(),
        created_at: chrono::Utc::now().timestamp(),
        stripe_size_bytes: 4096,
        data_shards: 1,
        parity_shards: 0,
        placement_roots: vec![blob_roots[0].to_string_lossy().into_owned()],
    };
    open_session(&manifest, None).unwrap();
    write_pending_binding(&PendingUploadBinding {
        fragment: fragment.clone(),
        logical_path: "folder/file.bin".into(),
        physical_object_id: object_id.clone(),
        version_id: manifest.version_id.clone(),
        session_id: manifest.session_id.clone(),
        created_at_unix: manifest.created_at,
    })
    .unwrap();
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
    for _ in 0..2 {
        let result = crate::core::upload::finalize_object_upload_direct(
            claims.clone(),
            fragment.clone(),
            manifest.version_id.clone(),
            manifest.session_id.clone(),
            object_id.clone(),
            crate::core::disk_manager::InitiateUploadParams {
                content_type: None,
                total_size_bytes: 4096,
                expected_parts: 1,
                part_size_bytes: Some(4096),
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            result,
            crate::core::upload::FinalizeUploadResult::Complete(_)
        ));
        assert!(
            list_active_sessions(&fragment)
                .unwrap()
                .iter()
                .any(|session| session.manifest.version_id == manifest.version_id),
            "NDX publication must not discard GC protection before the namespace commit"
        );
        assert_eq!(
            read_pending_binding(&fragment, &manifest.version_id)
                .unwrap()
                .unwrap()
                .logical_path,
            "folder/file.bin",
            "a retry must retain the original logical target"
        );
    }
    for root in &roots {
        publish_namespace_link(root, &fragment, "folder/file.bin", &object_id).unwrap();
    }
    // Namespace publication succeeds first; only then may the caller remove
    // the manifest, receipts and binding together. This also models retry.
    crate::core::index::remove_session(&fragment, &manifest.version_id, "finalized")
        .await
        .unwrap();
    assert!(
        load_session(&fragment, &manifest.version_id)
            .unwrap()
            .is_none()
    );
    assert!(
        read_pending_binding(&fragment, &manifest.version_id)
            .unwrap()
            .is_none()
    );
    assert!(
        referenced_object_ids(&fragment)
            .unwrap()
            .contains(&object_id)
    );
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
fn durable_receipts_refresh_idle_age_without_rewriting_manifest() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let roots = ensure_test_index_roots();
    let fragment = format!("durable-activity-{}", Ulid::new());
    let manifest = UploadSessionManifest {
        fragment: fragment.clone(),
        object_key: new_object_id(),
        version_id: Ulid::new().to_string(),
        session_id: Ulid::new().to_string(),
        total_size_bytes: 4096,
        expected_parts: 1,
        part_size_bytes: 4096,
        content_type: "application/octet-stream".into(),
        created_at: 1,
        stripe_size_bytes: 4096,
        data_shards: 1,
        parity_shards: 0,
        placement_roots: vec![ensure_test_blob_roots()[0].to_string_lossy().into_owned()],
    };
    open_session(&manifest, None).unwrap();
    let mut manifests = Vec::new();
    for root in &roots {
        let dir = fragment_root_on(root, &fragment)
            .join(UPLOADS_DIR)
            .join(&manifest.version_id);
        let path = dir.join(MANIFEST_FILE);
        let mut row: SessionRow = decode(&fs::read(&path).unwrap()).unwrap();
        row.last_activity_at = 1;
        let bytes = encode(&row).unwrap();
        fs::write(&path, &bytes).unwrap();
        fs::File::open(&dir)
            .unwrap()
            .set_times(
                fs::FileTimes::new()
                    .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(2)),
            )
            .unwrap();
        manifests.push((path, bytes));
    }
    let before = load_session(&fragment, &manifest.version_id)
        .unwrap()
        .unwrap();
    let receipt_time = chrono::Utc::now().timestamp();
    commit_session_part_receipt(
        &fragment,
        &manifest.version_id,
        &SessionPartReceipt {
            part_index: 0,
            size_bytes: 4096,
            shard_size_bytes: 4096,
            created_at: receipt_time,
            checksum: Some(1),
            shard_checksums: vec![2],
        },
    )
    .unwrap();
    let after = load_session(&fragment, &manifest.version_id)
        .unwrap()
        .unwrap();
    for (path, bytes) in manifests {
        assert_eq!(
            fs::read(path).unwrap(),
            bytes,
            "activity tracking must not add manifest writes"
        );
    }
    assert!(after.last_activity_at > before.last_activity_at);
    assert!(
        after.last_activity_at >= receipt_time,
        "GC idle age must reflect the latest completed receipt"
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
    assert_eq!(page.common_prefixes, vec!["docs/"]);
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

    let page = scan_live_heads_page(&fragment, Some("docs/".to_string()), None, 10, None, true)
        .expect("list latest head");
    assert_eq!(page.entries.len(), 1);
    assert!(page.objects.is_empty());

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
fn current_head_uses_a_valid_latest_replica_but_never_falls_back_to_an_old_version() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let _ = ensure_test_index_roots();
    let roots = ensure_test_blob_roots();
    assert!(roots.len() >= 2);
    let fragment = Ulid::new().to_string();
    let object_id = new_object_id();
    let mut old = test_metadata(&fragment, &object_id, 111);
    old.version_id = "01K00000000000000000000000".to_string();
    let mut newest = old.clone();
    newest.version_id = "01K00000000000000000000009".to_string();
    newest.total_size_bytes = 222;
    let newest_bytes = encode(&newest).expect("encode latest replica");
    let mut latest_paths = Vec::new();
    for root in &roots[..2] {
        let dir = live_object_dir(root, &fragment, &object_id).expect("object placement");
        fs::create_dir_all(&dir).expect("create object placement");
        fs::write(dir.join(ndx_file_name(&old)), encode(&old).unwrap())
            .expect("write older committed version");
        let path = dir.join(ndx_file_name(&newest));
        fs::write(&path, &newest_bytes).expect("write latest replica");
        latest_paths.push(path);
    }
    fs::write(&latest_paths[0], b"corrupt latest replica").expect("corrupt one latest replica");

    let head = read_current_live_head(&fragment, &object_id)
        .expect("read surviving latest replica")
        .expect("latest version exists");
    assert_eq!(head.version_id, newest.version_id);
    assert_eq!(head.total_size_bytes, 222);
    assert_eq!(fs::read(&latest_paths[1]).unwrap(), newest_bytes);
    assert_eq!(
        fs::read(&latest_paths[0]).unwrap(),
        b"corrupt latest replica"
    );

    fs::write(&latest_paths[1], b"second corrupt latest replica")
        .expect("corrupt final latest replica");
    assert!(
        read_current_live_head(&fragment, &object_id).is_err(),
        "an unreadable newest version must not silently expose an older version"
    );
    for root in roots {
        let _ = fs::remove_dir_all(fragment_root_on(&root, &fragment));
    }
}

#[test]
fn current_head_reads_a_surviving_metadata_placement_without_recreating_missing_data() {
    let _guard = crate::test_support::global_test_lock().blocking_lock();
    let _ = ensure_test_index_roots();
    let roots = ensure_test_blob_roots();
    assert!(roots.len() >= 2);
    let fragment = Ulid::new().to_string();
    let object_id = new_object_id();
    let metadata = test_metadata(&fragment, &object_id, 333);
    let missing_dir = live_object_dir(&roots[0], &fragment, &object_id).unwrap();
    let surviving_dir = live_object_dir(&roots[1], &fragment, &object_id).unwrap();
    fs::create_dir_all(&surviving_dir).expect("create surviving placement");
    let bytes = encode(&metadata).expect("encode metadata");
    let path = surviving_dir.join(ndx_file_name(&metadata));
    fs::write(&path, &bytes).expect("write surviving metadata");
    fs::remove_dir_all(&missing_dir).expect("remove failed placement");

    let head = read_current_live_head(&fragment, &object_id)
        .expect("read metadata from surviving placement")
        .expect("metadata exists");
    assert_eq!(head.version_id, metadata.version_id);
    assert_eq!(head.total_size_bytes, metadata.total_size_bytes);
    assert_eq!(fs::read(path).unwrap(), bytes);
    assert!(!missing_dir.exists(), "head lookup must remain read-only");
    for root in roots {
        let _ = fs::remove_dir_all(fragment_root_on(&root, &fragment));
    }
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
    let head = read_current_live_head(&fragment, &object_id)
        .expect("select authoritative current head")
        .expect("current head exists");
    assert_eq!(head.placement_generation, 5);
    assert_eq!(head.total_size_bytes, 222);

    newer.placement_generation = 4;
    write_atomic(
        &newer_dir.join(ndx_file_name(&newer)),
        &encode(&newer).expect("encode divergent placement"),
    )
    .expect("write divergent placement");
    let error = read_live_version(&fragment, &object_id, &version_id)
        .expect_err("same generation divergence must fail closed");
    assert!(error.to_string().contains("same-generation divergent"));
    let error = read_current_live_head(&fragment, &object_id)
        .expect_err("current head must also reject same-generation divergence");
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
