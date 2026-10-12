use super::*;
use crate::core::disk_manager::{
    BLOB_STORE_DISKS, INDEX_STORE_DISKS, ObjectMetadata, ObjectPathManager, PartMetadata, ShardInfo,
};
use crate::core::index_store;
use ulid::Ulid;

#[test]
fn cancelled_request_keeps_namespace_publication_exclusive_until_worker_finishes() {
    use futures_util::FutureExt;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let _test_guard = crate::test_support::global_test_lock().lock().await;
        ensure_test_index_roots();
        let fragment = format!("cancel-publish-{}", Ulid::new());
        let object = new_object_id();
        let metadata = test_metadata(&fragment, &object, 32);
        index_store::upsert_live_version(&metadata).unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        started_rx.await.unwrap();
        let key = format!("{fragment}\0item.bin");
        let mut request = Box::pin(
            global_namespace_runtime()
                .finalize_logical_upload(&fragment, "item.bin", &object, &metadata),
        );
        assert!(request.as_mut().now_or_never().is_none());
        let lock = finalize_guard::lock_for_test(&key).unwrap();
        assert!(lock.try_lock().is_err());
        drop(request); // Worker is queued, but cancellation cannot stop it.
        let still_exclusive = lock.try_lock().is_err();
        drop(lock);
        release_tx.send(()).unwrap();
        blocker.await.unwrap();
        // One blocking thread: this sentinel runs after the queued publisher.
        tokio::task::spawn_blocking(|| {}).await.unwrap();
        assert!(
            still_exclusive,
            "cancelled request released a still-running publisher's path lock"
        );
        assert_eq!(
            global_namespace_runtime()
                .resolve_live_object_id(&fragment, "item.bin")
                .await
                .unwrap(),
            Some(object),
        );
        assert!(
            finalize_guard::lock_for_test(&key).is_none(),
            "cancelled publication leaked a path-lock entry"
        );
    });
}

#[test]
fn trash_parent_pruning_removes_only_empty_logical_parents() {
    let base = std::env::temp_dir().join(format!(
        "n1-trash-prune-{}-{}",
        std::process::id(),
        Ulid::new()
    ));
    let trash_root = base.join(TRASH_DIR);
    let leaf_parent = trash_root.join("clinical/reports");
    std::fs::create_dir_all(&leaf_parent).expect("create trash logical parents");

    prune_empty_trash_parents(&trash_root, "clinical/reports/item.bin");

    assert!(
        trash_root.is_dir(),
        "the fragment trash root is never pruned"
    );
    assert!(!trash_root.join("clinical").exists());
    let _ = std::fs::remove_dir_all(base);
}

fn ensure_test_index_roots() -> Vec<PathBuf> {
    let base = std::env::temp_dir().join("n1-index-tests");
    let roots = vec![base.join("index-00"), base.join("index-01")];
    let blob_roots = vec![base.join("blob-00"), base.join("blob-01")];
    if BLOB_STORE_DISKS.get().is_none() {
        BLOB_STORE_DISKS
            .set(
                blob_roots
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>(),
            )
            .expect("set N1 test blob roots");
    }
    for root in BLOB_STORE_DISKS.get().expect("N1 test blob roots") {
        std::fs::create_dir_all(root).expect("create N1 test blob root");
    }
    if INDEX_STORE_DISKS.get().is_none() {
        INDEX_STORE_DISKS
            .set(
                roots
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>(),
            )
            .expect("set N1 test index roots");
    }
    let configured = INDEX_STORE_DISKS.get().expect("N1 test roots");
    for root in configured {
        std::fs::create_dir_all(root).expect("create N1 test root");
    }
    configured.iter().map(PathBuf::from).collect()
}

fn test_metadata(fragment: &str, object_key: &str, size: u64) -> ObjectMetadata {
    let version_id = Ulid::new().to_string();
    let blob_root = ObjectPathManager::new()
        .get_blob_roots()
        .into_iter()
        .next()
        .expect("N1 test blob root");
    let version_dir = ObjectPathManager::new().get_shard_dir_on_disk(
        &blob_root,
        fragment,
        object_key,
        &version_id,
    );
    std::fs::create_dir_all(&version_dir).expect("create N1 test placement");
    let shard_path = version_dir.join("part_0_shard_0.shard");
    std::fs::write(&shard_path, vec![0u8; size.min(4096) as usize]).expect("write N1 test shard");
    ObjectMetadata {
        object_key: object_key.to_string(),
        fragment: fragment.to_string(),
        version_id,
        upload_session_id: "session".to_string(),
        owner_uid: "owner".to_string(),
        content_type: "application/octet-stream".to_string(),
        total_size_bytes: size,
        created_at: Utc::now().timestamp(),
        data_shards: 1,
        parity_shards: 0,
        placement_generation: 0,
        parts: vec![PartMetadata {
            part_index: 0,
            size_bytes: size,
            created_at: Utc::now().timestamp(),
            shards: vec![ShardInfo {
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
    index_store::upsert_live_version(&test_metadata(fragment, &object_id, size))
        .expect("publish N1 ndx");
    for root in roots {
        publish_namespace_link(root, fragment, logical_path, &object_id)
            .expect("publish namespace link");
    }
    object_id
}

#[tokio::test]
async fn n1_file_rename_preserves_object_id_and_version_lineage() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-rename-{}", Ulid::new());
    let object_id = publish_logical_object(&roots, &fragment, "video/old.mp4", 123);

    let renamed = global_namespace_runtime()
        .rename(&fragment, "video/old.mp4", "video/new.mp4")
        .await
        .expect("rename file symlink");
    assert_eq!(renamed.operation, "rename");

    assert_eq!(
        global_namespace_runtime()
            .resolve_live_object_id(&fragment, "video/new.mp4")
            .await
            .expect("resolve renamed object"),
        Some(object_id.clone())
    );
    assert!(
        global_namespace_runtime()
            .entry(&fragment, "video/old.mp4")
            .await
            .expect("old entry lookup")
            .is_none()
    );

    let selected = global_namespace_runtime()
        .select_object_id_for_write(&fragment, "video/new.mp4")
        .await
        .expect("select object id for overwrite");
    assert_eq!(selected, object_id);

    index_store::upsert_live_version(&test_metadata(&fragment, &selected, 456))
        .expect("publish second version");
    assert_eq!(
        index_store::list_live_versions(&fragment, &selected)
            .expect("live versions")
            .len(),
        2
    );
}

#[tokio::test]
async fn n1_concurrent_finalize_keeps_same_path_version_lineage() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let _roots = ensure_test_index_roots();
    let fragment = format!("n1-finalize-race-{}", Ulid::new());
    let logical_path = "docs/shared.bin";
    let first_object_id = new_object_id();
    let second_object_id = new_object_id();
    let first = test_metadata(&fragment, &first_object_id, 111);
    let second = test_metadata(&fragment, &second_object_id, 222);
    index_store::upsert_live_version(&first).expect("publish first provisional version");
    index_store::upsert_live_version(&second).expect("publish second provisional version");

    let committed_first = global_namespace_runtime()
        .finalize_logical_upload(&fragment, logical_path, &first_object_id, &first)
        .await
        .expect("finalize first writer");
    assert_eq!(committed_first, first_object_id);

    let committed_second = global_namespace_runtime()
        .finalize_logical_upload(&fragment, logical_path, &second_object_id, &second)
        .await
        .expect("finalize second writer under existing path");
    assert_eq!(committed_second, first_object_id);

    assert_eq!(
        global_namespace_runtime()
            .resolve_live_object_id(&fragment, logical_path)
            .await
            .expect("resolve shared path"),
        Some(first_object_id.clone())
    );
    let versions =
        index_store::list_live_versions(&fragment, &first_object_id).expect("live versions");
    assert_eq!(versions.len(), 2);
    assert!(
        versions
            .iter()
            .all(|meta| meta.object_key == first_object_id)
    );
    assert!(
        index_store::read_current_live_head(&fragment, &second_object_id)
            .expect("read stale provisional object")
            .is_none()
    );
}

#[tokio::test]
async fn n1_finalize_rejects_corrupt_replica_before_publication() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-finalize-preflight-{}", Ulid::new());
    let object = new_object_id();
    let metadata = test_metadata(&fragment, &object, 99);
    index_store::upsert_live_version(&metadata).unwrap();
    let bad = flat::path(&roots[1], &fragment, "docs/item.bin");
    fs::create_dir_all(bad.parent().unwrap()).unwrap();
    fs::write(&bad, b"corrupt").unwrap();
    assert!(
        global_namespace_runtime()
            .finalize_logical_upload(&fragment, "docs/item.bin", &object, &metadata)
            .await
            .is_err()
    );
    assert!(!flat::path(&roots[0], &fragment, "docs/item.bin").exists());
}

#[tokio::test]
async fn n1_prefix_move_preserves_object_pointers_without_folder_objects() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-prefix-move-{}", Ulid::new());
    let object = publish_logical_object(&roots, &fragment, "docs/a/video.mp4", 77);
    assert!(matches!(
        global_namespace_runtime().mkdir(&fragment, "archive").await,
        Err(AppError::BadRequest(_))
    ));
    let result = global_namespace_runtime()
        .rename(&fragment, "docs", "archive/docs")
        .await
        .unwrap();
    assert_eq!(result.affected_objects, 1);
    assert!(result.failed_paths.is_empty());
    for root in &roots {
        assert_eq!(
            flat::read(&flat::path(root, &fragment, "archive/docs/a/video.mp4"))
                .unwrap()
                .unwrap()
                .object_id,
            object
        );
    }
}

#[tokio::test]
async fn n1_soft_delete_ttl_reclaims_only_after_namespace_expiry() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let blob_roots = ObjectPathManager::new().get_blob_roots();
    let fragment = format!("n1-trash-{}", Ulid::new());
    let object_id = publish_logical_object(&roots, &fragment, "video/my-video.mp4", 123);

    let summary = global_namespace_runtime()
        .trash(&fragment, "video/my-video.mp4")
        .await
        .expect("soft delete");
    assert_eq!(summary.operation, "trash");
    assert_eq!(summary.affected_objects, 1);
    let trash_entry_id = summary
        .trash_entry_id
        .clone()
        .expect("generation-specific trash entry id");

    for root in &roots {
        assert!(!path_exists_no_follow(&flat::path(
            root,
            &fragment,
            "video/my-video.mp4"
        )));
        assert!(path_exists_no_follow(
            &fragment_root_on(root, &fragment)
                .join(TRASH_DIR)
                .join(&trash_entry_id)
        ));
    }
    assert!(blob_roots.iter().any(|root| {
        index_store::live_object_dir(root, &fragment, &object_id)
            .map(|path| path.is_dir())
            .unwrap_or(false)
    }));

    let before_expiry = global_namespace_runtime()
        .sweep_expired_trash(
            &fragment,
            60,
            summary.deleted_at_unix.expect("deleted timestamp") + 59,
        )
        .await
        .expect("pre-expiry lifecycle sweep");
    assert_eq!(before_expiry.expired_entries, 0);
    assert_eq!(before_expiry.expired_versions, 0);

    let expired = global_namespace_runtime()
        .sweep_expired_trash(
            &fragment,
            60,
            summary.deleted_at_unix.expect("deleted timestamp") + 60,
        )
        .await
        .expect("expiry lifecycle sweep");
    assert_eq!(expired.expired_entries, 1);
    assert_eq!(expired.expired_versions, 1);
    assert_eq!(expired.reclaimed_objects, 1);
    for root in &roots {
        assert!(!path_exists_no_follow(
            &fragment_root_on(root, &fragment)
                .join(TRASH_DIR)
                .join(&trash_entry_id)
        ));
    }
    assert!(!blob_roots.iter().any(|root| {
        index_store::live_object_dir(root, &fragment, &object_id)
            .map(|path| path.exists())
            .unwrap_or(false)
    }));
    assert!(
        !index_store::referenced_object_ids(&fragment)
            .expect("referenced object ids")
            .contains(&object_id),
        "purged trash item must leave object index headless for GC"
    );
}

#[tokio::test]
async fn trash_sweep_reports_logical_versions_without_counting_physical_replicas() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("trash-version-accounting-{}", Ulid::new());
    let object = publish_logical_object(&roots, &fragment, "history.bin", 32);
    index_store::upsert_live_version(&test_metadata(&fragment, &object, 64)).unwrap();
    let deleted = global_namespace_runtime()
        .trash(&fragment, "history.bin")
        .await
        .unwrap();
    let records = scan_trash_records(&fragment).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].version_ids.len(), 2);
    let expired = global_namespace_runtime()
        .sweep_expired_trash(&fragment, 0, deleted.deleted_at_unix.unwrap())
        .await
        .unwrap();
    assert_eq!(expired.expired_entries, 1);
    assert_eq!(expired.expired_versions, 2);
    assert_eq!(expired.reclaimed_objects, 1);
    let repeated = global_namespace_runtime()
        .sweep_expired_trash(&fragment, 0, deleted.deleted_at_unix.unwrap())
        .await
        .unwrap();
    assert_eq!(repeated.expired_entries, 0);
    assert_eq!(repeated.expired_versions, 0);
    assert_eq!(repeated.reclaimed_objects, 0);
}

#[tokio::test]
async fn n1_repeated_delete_creates_independent_generations_and_recover_stays_strict() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-trash-repeat-{}", Ulid::new());
    let path = "clinical/report.bin";
    let first_object = publish_logical_object(&roots, &fragment, path, 11);
    let first = global_namespace_runtime()
        .trash(&fragment, path)
        .await
        .expect("trash first object");

    let second_object = publish_logical_object(&roots, &fragment, path, 22);
    let second = global_namespace_runtime()
        .trash(&fragment, path)
        .await
        .expect("trash replacement object");
    assert_ne!(first_object, second_object);
    assert_ne!(first.trash_entry_id, second.trash_entry_id);

    let (entries, _) = global_namespace_runtime()
        .list_dir_page(&fragment, "", true, 10, None)
        .await
        .expect("list trash");
    assert_eq!(
        entries.iter().filter(|entry| entry.path == path).count(),
        2,
        "each delete owns an independent recoverable generation"
    );

    let third_object = publish_logical_object(&roots, &fragment, path, 33);
    assert_eq!(
        global_namespace_runtime()
            .resolve_live_object_id(&fragment, path)
            .await
            .expect("resolve replacement live path"),
        Some(third_object)
    );
    assert!(matches!(
        global_namespace_runtime()
            .recover_trash_entry_id(
                &fragment,
                second.trash_entry_id.as_deref().expect("second trash id")
            )
            .await,
        Err(AppError::Conflict(_))
    ));
}

#[tokio::test]
async fn n1_prefix_delete_retains_independent_per_key_generations() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-trash-prefix-{}", Ulid::new());
    let first = publish_logical_object(&roots, &fragment, "videos/first.mp4", 11);
    global_namespace_runtime()
        .trash(&fragment, "videos")
        .await
        .unwrap();
    let second = publish_logical_object(&roots, &fragment, "videos/second.mp4", 22);
    global_namespace_runtime()
        .trash(&fragment, "videos")
        .await
        .unwrap();
    let records = scan_trash_records(&fragment).unwrap();
    assert_eq!(records.len(), 2);
    for record in records {
        global_namespace_runtime()
            .recover_trash_entry_id(&fragment, &record.trash_entry_id)
            .await
            .unwrap();
    }
    assert_eq!(
        global_namespace_runtime()
            .resolve_live_object_id(&fragment, "videos/first.mp4")
            .await
            .unwrap(),
        Some(first)
    );
    assert_eq!(
        global_namespace_runtime()
            .resolve_live_object_id(&fragment, "videos/second.mp4")
            .await
            .unwrap(),
        Some(second)
    );
}

#[tokio::test]
async fn n1_trash_copies_a_surviving_ncx_when_a_live_replica_is_missing() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-trash-preflight-{}", Ulid::new());
    let key = "clinical/report.bin";
    publish_logical_object(&roots, &fragment, key, 31);
    fs::remove_file(flat::path(&roots[1], &fragment, key)).unwrap();
    let result = global_namespace_runtime()
        .trash(&fragment, key)
        .await
        .unwrap();
    assert!(result.trash_entry_id.is_some());
    assert!(flat::lookup(&fragment, key).unwrap().is_none());
    assert_eq!(scan_trash_records(&fragment).unwrap().len(), 1);
}

#[tokio::test]
async fn n1_ttl_reclaim_preserves_an_object_still_referenced_live() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let blob_roots = ObjectPathManager::new().get_blob_roots();
    let fragment = format!("n1-trash-shared-{}", Ulid::new());
    let object_id = publish_logical_object(&roots, &fragment, "records/a.bin", 33);
    for root in &roots {
        publish_namespace_link(root, &fragment, "records/b.bin", &object_id)
            .expect("publish second live reference");
    }

    let deleted = global_namespace_runtime()
        .trash(&fragment, "records/a.bin")
        .await
        .expect("trash one reference");
    let expired = global_namespace_runtime()
        .sweep_expired_trash(
            &fragment,
            1,
            deleted.deleted_at_unix.expect("deleted timestamp") + 1,
        )
        .await
        .expect("lifecycle sweep");
    assert_eq!(expired.expired_entries, 1);
    assert_eq!(expired.reclaimed_objects, 0);
    assert_eq!(
        global_namespace_runtime()
            .resolve_live_object_id(&fragment, "records/b.bin")
            .await
            .expect("resolve retained reference"),
        Some(object_id.clone())
    );
    assert!(blob_roots.iter().any(|root| {
        index_store::live_object_dir(root, &fragment, &object_id)
            .map(|path| path.is_dir())
            .unwrap_or(false)
    }));
}

#[tokio::test]
async fn n1_posix_pagination_uses_display_names_not_raw_object_dirs() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-posix-page-{}", Ulid::new());
    for (name, size) in [
        ("docs/a.svg", 10u64),
        ("docs/b.svg", 20u64),
        ("docs/c.svg", 30u64),
    ] {
        publish_logical_object(&roots, &fragment, name, size);
    }

    let (first, token) = global_namespace_runtime()
        .list_dir_page(&fragment, "docs", false, 2, None)
        .await
        .expect("first page");
    assert_eq!(
        first
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>(),
        vec!["a.svg", "b.svg"]
    );
    let (second, token2) = global_namespace_runtime()
        .list_dir_page(&fragment, "docs", false, 2, token.as_deref())
        .await
        .expect("second page");
    assert_eq!(
        second
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>(),
        vec!["c.svg"]
    );
    assert!(token2.is_none());

    for root in &roots {
        let empty_object_dir =
            crate::core::index_store::live_object_dir(root, &fragment, &new_object_id())
                .expect("empty object dir");
        std::fs::create_dir_all(empty_object_dir).expect("empty object dir");
    }
    let (entries, _) = global_namespace_runtime()
        .list_dir_page(&fragment, "docs", false, 10, None)
        .await
        .expect("page with empty object dir");
    assert!(!entries.iter().any(|entry| entry.name == "empty.svg"));
}

#[tokio::test]
async fn n1_trash_list_shows_only_explicit_file_delete_anchor() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-trash-file-anchor-{}", Ulid::new());
    publish_logical_object(&roots, &fragment, "videos/clip.mp4", 123);

    global_namespace_runtime()
        .trash(&fragment, "videos/clip.mp4")
        .await
        .expect("trash file");

    let (entries, token) = global_namespace_runtime()
        .list_dir_page(&fragment, "", true, 10, None)
        .await
        .expect("list trash");
    assert!(token.is_none());
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].path, "videos/clip.mp4");
    assert_eq!(entries[0].kind, NamespaceEntryKind::File);
}

#[tokio::test]
async fn n1_prefix_trash_list_shows_each_deleted_object() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-trash-folder-anchor-{}", Ulid::new());
    publish_logical_object(&roots, &fragment, "videos/clip.mp4", 123);
    publish_logical_object(&roots, &fragment, "videos/deep/other.mp4", 456);

    global_namespace_runtime()
        .trash(&fragment, "videos")
        .await
        .expect("trash folder");

    let (entries, token) = global_namespace_runtime()
        .list_dir_page(&fragment, "", true, 10, None)
        .await
        .expect("list trash");
    assert!(token.is_none());
    assert_eq!(entries.len(), 2);
    assert!(
        entries
            .iter()
            .all(|entry| entry.kind == NamespaceEntryKind::File)
    );
}

#[tokio::test]
async fn n1_recursive_reference_pagination_never_skips_cursor_directory_descendants() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-recursive-page-{}", Ulid::new());
    publish_logical_object(&roots, &fragment, "a/first.bin", 1);
    publish_logical_object(&roots, &fragment, "a/z-last.bin", 2);
    publish_logical_object(&roots, &fragment, "b.bin", 3);

    let first = global_namespace_runtime()
        .list_live_object_refs_page(&fragment, None, None, 1)
        .await
        .expect("first recursive reference page");
    assert_eq!(first.entries[0].path, "a/first.bin");
    let second = global_namespace_runtime()
        .list_live_object_refs_page(&fragment, None, first.continuation_token.as_deref(), 10)
        .await
        .expect("second recursive reference page");
    assert_eq!(
        second
            .entries
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<Vec<_>>(),
        vec!["a/z-last.bin", "b.bin"]
    );
}

#[tokio::test]
async fn n1_resolve_any_and_recover_use_the_physical_trash_path() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-resolve-trash-generation-{}", Ulid::new());
    let path = "clinical/report.bin";
    let object_id = publish_logical_object(&roots, &fragment, path, 11);
    global_namespace_runtime()
        .trash(&fragment, path)
        .await
        .expect("trash object");

    assert_eq!(
        global_namespace_runtime()
            .resolve_any_object_id(&fragment, path)
            .await
            .expect("resolve trashed object"),
        Some(object_id.clone())
    );
    global_namespace_runtime()
        .recover(&fragment, path)
        .await
        .expect("recover object");
    assert_eq!(
        global_namespace_runtime()
            .resolve_live_object_id(&fragment, path)
            .await
            .expect("resolve recovered object"),
        Some(object_id)
    );
}

#[tokio::test]
async fn n1_repeated_recover_returns_not_found_instead_of_internal_error() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-recover-headless-record-{}", Ulid::new());
    publish_logical_object(&roots, &fragment, "records/report.bin", 44);
    global_namespace_runtime()
        .trash(&fragment, "records/report.bin")
        .await
        .expect("trash object");
    global_namespace_runtime()
        .recover(&fragment, "records/report.bin")
        .await
        .expect("recover once");

    assert!(matches!(
        global_namespace_runtime()
            .recover(&fragment, "records/report.bin")
            .await,
        Err(AppError::NotFound)
    ));
}

#[tokio::test]
async fn n1_namespace_mutations_reject_parent_traversal() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-parent-symlink-{}", Ulid::new());
    let outside_name = format!("n1-outside-{}", Ulid::new());
    let outside = roots[0].join(&outside_name);

    assert!(matches!(
        global_namespace_runtime()
            .mkdir(&fragment, &format!("../{outside_name}"))
            .await,
        Err(AppError::BadRequest(_))
    ));
    assert!(
        !outside.exists(),
        "normalized user paths cannot escape a fragment root"
    );
}

#[tokio::test]
async fn n1_recover_and_ttl_sweep_race_preserves_data_without_a_namespace_lock() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    for iteration in 0..32u64 {
        let fragment = format!("n1-recover-sweep-race-{iteration}-{}", Ulid::new());
        let path = "race/item.bin";
        publish_logical_object(&roots, &fragment, path, iteration + 1);
        global_namespace_runtime()
            .trash(&fragment, path)
            .await
            .expect("trash race object");

        let (recover, sweep) = tokio::join!(
            global_namespace_runtime().recover(&fragment, path),
            global_namespace_runtime().sweep_expired_trash(&fragment, 0, Utc::now().timestamp())
        );
        let live = global_namespace_runtime()
            .resolve_live_object_id(&fragment, path)
            .await
            .expect("resolve race result")
            .is_some();
        match (recover, sweep, live) {
            (Ok(_), Ok(summary), true) => assert!(summary.expired_entries <= 1),
            (Err(AppError::NotFound), Ok(summary), false) => {
                assert_eq!(summary.expired_entries, 1)
            }
            outcome => panic!("invalid recover/sweep outcome: {outcome:?}"),
        }
    }
}

#[tokio::test]
async fn ttl_reclamation_preserves_an_upload_to_the_expiring_object() {
    let _test_guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("ttl-active-upload-{}", Ulid::new());
    let path = "ongoing.bin";
    let object = publish_logical_object(&roots, &fragment, path, 32);
    let metadata = index_store::read_current_live_head(&fragment, &object)
        .unwrap()
        .unwrap();
    global_namespace_runtime()
        .trash(&fragment, path)
        .await
        .unwrap();
    // A previously initialized upload may still target the physical object
    // after the user trashes its namespace path.
    let active = crate::core::upload::UploadActivityGuard::begin(&fragment, &object).unwrap();
    let sweep = global_namespace_runtime()
        .sweep_expired_trash(&fragment, 0, Utc::now().timestamp())
        .await
        .unwrap();
    assert_eq!(sweep.expired_entries, 1);
    assert_eq!(
        sweep.reclaimed_objects, 0,
        "TTL reclaimed an active upload's object directory"
    );
    assert!(
        index_store::read_live_version(&fragment, &object, &metadata.version_id)
            .unwrap()
            .is_some()
    );
    assert!(Path::new(&metadata.parts[0].shards[0].absolute_path).is_file());
    drop(active);
    assert!(index_store::delete_unreferenced_object_index(&fragment, &object).unwrap());
}

#[tokio::test]
async fn idle_durable_session_blocks_ttl_object_reclamation_until_removed() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("ttl-idle-durable-{}", Ulid::new());
    let object = publish_logical_object(&roots, &fragment, "idle.bin", 32);
    let manifest = crate::core::upload::UploadSessionManifest {
        fragment: fragment.clone(),
        object_key: object.clone(),
        version_id: Ulid::new().to_string(),
        session_id: Ulid::new().to_string(),
        total_size_bytes: 32,
        expected_parts: 1,
        part_size_bytes: 32,
        content_type: "application/octet-stream".into(),
        created_at: Utc::now().timestamp(),
        stripe_size_bytes: 4096,
        data_shards: 1,
        parity_shards: 0,
        placement_roots: BLOB_STORE_DISKS.get().unwrap().clone(),
    };
    index_store::open_session(&manifest, None).unwrap();
    global_namespace_runtime()
        .trash(&fragment, "idle.bin")
        .await
        .unwrap();
    let sweep = global_namespace_runtime()
        .sweep_expired_trash(&fragment, 0, Utc::now().timestamp())
        .await
        .unwrap();
    assert_eq!(sweep.expired_entries, 1);
    assert_eq!(sweep.reclaimed_objects, 0);
    assert!(
        index_store::read_current_live_head(&fragment, &object)
            .unwrap()
            .is_some()
    );
    index_store::remove_session(&fragment, &manifest.version_id).unwrap();
    assert!(index_store::delete_unreferenced_object_index(&fragment, &object).unwrap());
}

fn path_exists_no_follow(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok()
}

#[tokio::test]
async fn explicit_prefix_mutations_preserve_the_same_named_object() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("prefix-selection-{}", Ulid::new());
    let exact = publish_logical_object(&roots, &fragment, "a", 8);
    let child = publish_logical_object(&roots, &fragment, "a/child", 9);
    let runtime = global_namespace_runtime();
    assert_eq!(
        runtime.entry(&fragment, "a/").await.unwrap().unwrap().kind,
        NamespaceEntryKind::Directory
    );
    let moved = runtime.rename(&fragment, "a/", "b/").await.unwrap();
    assert_eq!(moved.kind, NamespaceEntryKind::Directory);
    assert_eq!(
        runtime
            .resolve_live_object_id(&fragment, "a")
            .await
            .unwrap(),
        Some(exact.clone())
    );
    assert_eq!(
        runtime
            .resolve_live_object_id(&fragment, "b/child")
            .await
            .unwrap(),
        Some(child)
    );
    publish_logical_object(&roots, &fragment, "b", 10);
    let trashed = runtime.trash(&fragment, "b/").await.unwrap();
    assert_eq!(trashed.kind, NamespaceEntryKind::Directory);
    assert!(
        runtime
            .resolve_live_object_id(&fragment, "b")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        runtime
            .resolve_live_object_id(&fragment, "b/child")
            .await
            .unwrap()
            .is_none()
    );
    runtime.trash(&fragment, "b").await.unwrap();
    let recovered = runtime.recover(&fragment, "b/").await.unwrap();
    assert_eq!(recovered.kind, NamespaceEntryKind::Directory);
    assert!(
        runtime
            .resolve_live_object_id(&fragment, "b")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        runtime
            .resolve_live_object_id(&fragment, "b/child")
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        runtime
            .resolve_live_object_id(&fragment, "a")
            .await
            .unwrap(),
        Some(exact)
    );
}

#[tokio::test]
async fn flat_listing_distinguishes_objects_prefixes_and_internal_suffixes() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("suffix-keys-{}", Ulid::new());
    for key in ["suffix/a", "suffix/a/b", "suffix/a.ncx/b", "suffix/a.ncp/b"] {
        publish_logical_object(&roots, &fragment, key, 8);
    }
    let runtime = global_namespace_runtime();
    let mut cursor = None;
    let mut names = Vec::new();
    loop {
        let (entries, next) = runtime
            .list_dir_page(&fragment, "suffix", false, 1, cursor.as_deref())
            .await
            .unwrap();
        names.extend(entries.into_iter().map(|entry| entry.path));
        cursor = next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(
        names,
        ["suffix/a", "suffix/a.ncp/", "suffix/a.ncx/", "suffix/a/"]
    );
    let mut visited = Vec::new();
    flat::visit(&fragment, "suffix/", |key| {
        visited.push(key.to_owned());
        Ok(true)
    })
    .unwrap();
    assert_eq!(visited.len(), 4, "export must not emit duplicate replicas");
    assert_eq!(visited.into_iter().collect::<BTreeSet<_>>().len(), 4);
}

#[tokio::test]
async fn interrupted_recovery_retries_unchanged_ncx_and_tolerates_one_corrupt_trash_copy() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("retry-recover-{}", Ulid::new());
    publish_logical_object(&roots, &fragment, "restore", 8);
    let record = flat::trash_key(&fragment, "restore").unwrap().unwrap();
    let trash = index_store::fragment_root_on(&roots[0], &fragment)
        .join(TRASH_DIR)
        .join(&record.source_relative_path);
    let pointer = flat::read(&trash).unwrap().unwrap();
    // Simulate a process dying after its first destination publication.
    flat::write(
        &roots[0],
        &flat::path(&roots[0], &fragment, "restore"),
        &pointer,
        true,
    )
    .unwrap();
    fs::write(&trash, b"corrupt replica").unwrap();
    assert!(flat::recover(&fragment, &record).unwrap());
    for root in &roots {
        assert_eq!(
            flat::read(&flat::path(root, &fragment, "restore")).unwrap(),
            Some(pointer.clone())
        );
        assert!(
            !index_store::fragment_root_on(root, &fragment)
                .join(TRASH_DIR)
                .join(&record.source_relative_path)
                .exists()
        );
    }
    let damaged = flat::path(&roots[0], &fragment, "restore");
    fs::write(&damaged, b"live corruption after recover").unwrap();
    flat::repair(&fragment, false).unwrap();
    assert_eq!(flat::read(&damaged).unwrap(), Some(pointer));
}

#[tokio::test]
async fn aliases_share_stable_placement_key_locks_and_gc_protection() {
    use crate::core::upload::{UploadActivityGuard, UploadGcScan};
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let stable = Ulid::new().to_string();
    let alias = format!("alias-{}", Ulid::new());
    // Only a later member has the alias: the first member may be offline.
    crate::core::fragment_identity::ensure_fragment_identity_on_root(&roots[1], &alias, &stable)
        .await
        .unwrap();
    let resolve = index_store::stable_fragment_id_for_fragment;
    assert_eq!(resolve(&alias), stable);
    assert_eq!(resolve(&format!("@f_{stable}")), stable);
    let published = publish_logical_object(&roots, &stable, "alias-item", 8);
    fs::remove_file(flat::path(&roots[1], &stable, "alias-item")).unwrap();
    assert_eq!(
        flat::lookup(&alias, "alias-item")
            .unwrap()
            .unwrap()
            .object_id,
        published
    );
    assert_eq!(
        flat::path(&roots[0], &alias, "alias-item"),
        flat::path(&roots[0], &stable, "alias-item")
    );
    flat::repair(&alias, false).unwrap();
    assert!(flat::path(&roots[1], &stable, "alias-item").is_file());
    let pool = crate::core::storage_pool::StoragePool {
        id: "A".into(),
        generation: 1,
        data_shards: 3,
        parity_shards: 1,
        roots: (0..4)
            .map(|i| PathBuf::from(format!("/member/{i}")))
            .collect(),
        members: Vec::new(),
        legacy_roots: Vec::new(),
        sealed: false,
    };
    assert_eq!(
        pool.namespace_roots(&resolve(&alias), "key"),
        pool.namespace_roots(&stable, "key")
    );
    flat::with_keys(&alias, &["key"], || {
        assert!(flat::key_locked_for_test(&stable, "key"));
        Ok(())
    })
    .unwrap();
    let object = new_object_id();
    let upload = UploadActivityGuard::begin(&alias, &object).unwrap();
    let scan = UploadGcScan::begin(&stable).unwrap();
    assert!(scan.try_reclaim(&object).unwrap().is_none());
    drop(upload);
    // The scan must remember the overlap even after the alias upload ends.
    assert!(scan.try_reclaim(&object).unwrap().is_none());
}

#[tokio::test]
async fn interrupted_copy_delete_keeps_destination_and_retry_removes_all_sources() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("interrupted-copy-delete-{}", Ulid::new());
    publish_logical_object(&roots, &fragment, "item", 8);
    let bytes = fs::read(flat::path(&roots[0], &fragment, "item")).unwrap();
    let pointer = flat::lookup(&fragment, "item").unwrap().unwrap();
    // Disk state after every destination is durable and only one source was
    // removed. This is allowed after interruption: repair must preserve data.
    for root in &roots {
        fs::hard_link(
            flat::path(root, &fragment, "item"),
            flat::path(root, &fragment, "moved"),
        )
        .unwrap();
        fs::File::open(flat::namespace_root(root, &fragment))
            .unwrap()
            .sync_all()
            .unwrap();
    }
    fs::remove_file(flat::path(&roots[0], &fragment, "item")).unwrap();
    fs::File::open(flat::namespace_root(&roots[0], &fragment))
        .unwrap()
        .sync_all()
        .unwrap();
    flat::repair(&fragment, false).unwrap();
    assert_eq!(
        flat::lookup(&fragment, "item").unwrap(),
        Some(pointer.clone())
    );
    assert_eq!(flat::lookup(&fragment, "moved").unwrap(), Some(pointer));
    assert!(flat::move_key(&fragment, "item", "moved").unwrap());
    flat::repair(&fragment, false).unwrap();
    assert!(flat::lookup(&fragment, "item").unwrap().is_none());
    for root in &roots {
        assert!(!flat::path(root, &fragment, "item").exists());
        assert_eq!(
            fs::read(flat::path(root, &fragment, "moved")).unwrap(),
            bytes
        );
        assert!(
            !index_store::fragment_root_on(root, &fragment)
                .join(".namespace-deleted")
                .exists()
        );
    }
}

#[tokio::test]
async fn non_slash_listing_delimiters_do_not_skip_sibling_prefixes() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("delimiter-{}", Ulid::new());
    for key in ["top/a:one", "top/b:two", "top/c:three", "top/d"] {
        publish_logical_object(&roots, &fragment, key, 8);
    }
    let (names, next) = flat::page(&fragment, "top/", ":", None, 10).unwrap();
    assert_eq!(
        names,
        vec![
            ("top/a:".into(), true),
            ("top/b:".into(), true),
            ("top/c:".into(), true),
            ("top/d".into(), false)
        ]
    );
    assert!(next.is_none());
}

fn immutable_namespace_fixture_files(fragment: &str) -> BTreeMap<PathBuf, Vec<u8>> {
    let manager = ObjectPathManager::new();
    let roots = manager
        .get_all_index_roots()
        .into_iter()
        .chain(manager.get_blob_roots());
    let mut files = BTreeMap::new();
    for root in roots {
        let base = index_store::fragment_root_on(&root, fragment);
        if !base.is_dir() {
            continue;
        }
        for entry in walkdir::WalkDir::new(base).follow_links(false) {
            let entry = entry.unwrap();
            if entry.file_type().is_file()
                && matches!(
                    entry.path().extension().and_then(|e| e.to_str()),
                    Some("ndx" | "shard")
                )
            {
                files.insert(entry.path().to_path_buf(), fs::read(entry.path()).unwrap());
            }
        }
    }
    files
}

#[tokio::test]
async fn foreground_mutations_and_trash_listing_never_decode_or_change_ndx_or_blobs() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("ncx-only-mutation-{}", Ulid::new());
    for key in ["a", "folder/one", "folder/deep/two"] {
        publish_logical_object(&roots, &fragment, key, 32);
    }
    // Corruption is intentionally left in immutable NDX to prove that the
    // foreground namespace path does not even need to decode those bytes.
    let originals = immutable_namespace_fixture_files(&fragment);
    assert!(!originals.is_empty());
    for path in originals
        .keys()
        .filter(|path| path.extension().is_some_and(|e| e == "ndx"))
    {
        fs::write(path, b"intentionally undecodable immutable NDX fixture").unwrap();
    }
    let before = immutable_namespace_fixture_files(&fragment);
    let runtime = global_namespace_runtime();
    runtime.rename(&fragment, "a", "renamed").await.unwrap();
    runtime
        .rename(&fragment, "renamed", "nested/moved")
        .await
        .unwrap();
    let deleted = runtime.trash(&fragment, "nested/moved").await.unwrap();
    runtime
        .list_dir_page(&fragment, "", true, 200, None)
        .await
        .unwrap();
    runtime
        .recover_trash_entry_id(&fragment, deleted.trash_entry_id.as_deref().unwrap())
        .await
        .unwrap();
    runtime
        .rename(&fragment, "folder/", "renamed-folder/")
        .await
        .unwrap();
    runtime.trash(&fragment, "renamed-folder/").await.unwrap();
    let restored = runtime.recover(&fragment, "renamed-folder/").await.unwrap();
    assert_eq!(restored.affected_paths, 2);
    assert_eq!(immutable_namespace_fixture_files(&fragment), before);
    // Maintenance retains its metadata validation responsibility.
    let again = runtime.trash(&fragment, "nested/moved").await.unwrap();
    assert!(index_store::scan_trash_records(&fragment).is_err());
    runtime
        .recover_trash_entry_id(&fragment, again.trash_entry_id.as_deref().unwrap())
        .await
        .unwrap();
    assert_eq!(immutable_namespace_fixture_files(&fragment), before);
}

#[tokio::test]
async fn exact_trash_recovery_is_bounded_to_its_identity_and_rejects_traversal() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("point-trash-{}", Ulid::new());
    publish_logical_object(&roots, &fragment, "target.ncx/child.ncp", 32);
    publish_logical_object(&roots, &fragment, "unrelated", 32);
    let target = flat::trash_key(&fragment, "target.ncx/child.ncp")
        .unwrap()
        .unwrap();
    let unrelated = flat::trash_key(&fragment, "unrelated").unwrap().unwrap();
    for root in &roots {
        fs::write(
            index_store::fragment_root_on(root, &fragment)
                .join(TRASH_DIR)
                .join(&unrelated.source_relative_path),
            b"unrelated corrupt trash NCX",
        )
        .unwrap();
    }
    assert!(flat::trash_records(&fragment).is_err());
    assert_eq!(
        flat::trash_record_by_id(&fragment, &target.trash_entry_id).unwrap(),
        Some(target.clone())
    );
    let generation = target.trash_entry_id.split('/').next().unwrap();
    for bad in [
        "../escape.ncx".to_string(),
        format!("/{generation}/target.ncx"),
        format!("{generation}/../target.ncx"),
        format!("{generation}/a.ncp/../b.ncx"),
        format!("{generation}/a.ncp/..ncx"),
        format!("{generation}//target.ncx"),
    ] {
        assert!(
            flat::trash_record_by_id(&fragment, &bad).is_err(),
            "accepted {bad}"
        );
    }
    global_namespace_runtime()
        .recover_trash_entry_id(&fragment, &target.trash_entry_id)
        .await
        .unwrap();
    assert!(
        flat::lookup(&fragment, "target.ncx/child.ncp")
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn failed_destination_publication_never_deletes_the_source() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("failed-holder-phase-{}", Ulid::new());
    let object = publish_logical_object(&roots, &fragment, "source", 32);
    let blocked = flat::path(&roots[0], &fragment, "destination");
    fs::create_dir_all(&blocked).unwrap();
    assert!(flat::move_key(&fragment, "source", "destination").is_err());
    for root in &roots {
        assert!(
            flat::read(&flat::path(root, &fragment, "source"))
                .unwrap()
                .is_some()
        );
        assert!(
            !index_store::fragment_root_on(root, &fragment)
                .join(".namespace-deleted")
                .join("source.ncx")
                .exists()
        );
    }
    fs::remove_dir(&blocked).unwrap();
    assert!(flat::move_key(&fragment, "source", "destination").unwrap());
    assert_eq!(
        flat::lookup(&fragment, "destination")
            .unwrap()
            .unwrap()
            .object_id,
        object
    );
    assert!(flat::lookup(&fragment, "source").unwrap().is_none());
}
