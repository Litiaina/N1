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
async fn n1_finalize_preflights_replica_path_conflicts_before_publication() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    assert!(roots.len() >= 2);
    let fragment = format!("n1-finalize-preflight-{}", Ulid::new());
    let object_id = new_object_id();
    let metadata = test_metadata(&fragment, &object_id, 99);
    index_store::upsert_live_version(&metadata).expect("publish provisional object metadata");
    for root in &roots {
        std::fs::create_dir_all(fragment_root_on(root, &fragment).join("docs"))
            .expect("create finalize namespace parent");
    }
    std::fs::write(
        fragment_root_on(&roots[1], &fragment).join("docs/item.bin"),
        b"conflict",
    )
    .expect("inject later-root path conflict");

    assert!(matches!(
        global_namespace_runtime()
            .finalize_logical_upload(&fragment, "docs/item.bin", &object_id, &metadata)
            .await,
        Err(AppError::Conflict(_))
    ));
    assert!(
        !path_exists_no_follow(&fragment_root_on(&roots[0], &fragment).join("docs/item.bin")),
        "failed finalize preflight must not publish a partial namespace replica"
    );
}

#[tokio::test]
async fn n1_folder_move_keeps_symlink_object_pointers_valid() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-folder-move-{}", Ulid::new());
    let object_id = publish_logical_object(&roots, &fragment, "docs/a/video.mp4", 77);

    global_namespace_runtime()
        .mkdir(&fragment, "archive")
        .await
        .expect("mkdir archive");
    global_namespace_runtime()
        .rename(&fragment, "docs", "archive/docs")
        .await
        .expect("rename folder");

    assert_eq!(
        global_namespace_runtime()
            .resolve_live_object_id(&fragment, "archive/docs/a/video.mp4")
            .await
            .expect("resolve moved symlink"),
        Some(object_id.clone())
    );
    for root in &roots {
        let link = fragment_root_on(root, &fragment).join("archive/docs/a/video.mp4");
        let target = fs::read_link(&link).expect("read namespace link");
        assert_eq!(
            target,
            PathBuf::from(format!("@o_{object_id}")),
            "namespace links store stable object ids, not depth-relative paths"
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
        assert!(!path_exists_no_follow(
            &fragment_root_on(root, &fragment).join("video/my-video.mp4")
        ));
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

    let before_expiry = sweep_expired_trash_sync(
        &fragment,
        60,
        summary.deleted_at_unix.expect("deleted timestamp") + 59,
    )
    .expect("pre-expiry lifecycle sweep");
    assert_eq!(before_expiry.expired_entries, 0);

    let expired = sweep_expired_trash_sync(
        &fragment,
        60,
        summary.deleted_at_unix.expect("deleted timestamp") + 60,
    )
    .expect("expiry lifecycle sweep");
    assert_eq!(expired.expired_entries, 1);
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
async fn n1_repeated_directory_delete_keeps_each_generation_recoverable() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    let fragment = format!("n1-trash-repeat-dir-{}", Ulid::new());
    let first_object = publish_logical_object(&roots, &fragment, "videos/first.mp4", 11);
    let first = global_namespace_runtime()
        .trash(&fragment, "videos")
        .await
        .expect("trash first directory generation");

    let second_object = publish_logical_object(&roots, &fragment, "videos/second.mp4", 22);
    let second = global_namespace_runtime()
        .trash(&fragment, "videos")
        .await
        .expect("trash second directory generation");

    let records = scan_trash_records(&fragment).expect("scan directory generations");
    assert_eq!(
        records
            .iter()
            .filter(|record| record.original_path == "videos")
            .count(),
        2
    );
    assert_ne!(first.trash_entry_id, second.trash_entry_id);

    global_namespace_runtime()
        .recover_trash_entry_id(
            &fragment,
            second
                .trash_entry_id
                .as_deref()
                .expect("second directory id"),
        )
        .await
        .expect("recover selected directory generation");
    assert_eq!(
        global_namespace_runtime()
            .resolve_live_object_id(&fragment, "videos/second.mp4")
            .await
            .expect("resolve recovered second object"),
        Some(second_object.clone())
    );
    assert_ne!(first_object, second_object);
    assert!(matches!(
        global_namespace_runtime()
            .recover_trash_entry_id(
                &fragment,
                first.trash_entry_id.as_deref().expect("first directory id")
            )
            .await,
        Err(AppError::Conflict(_))
    ));
}

#[tokio::test]
async fn n1_trash_preflights_every_replica_before_the_first_rename() {
    let _guard = crate::test_support::global_test_lock().lock().await;
    let roots = ensure_test_index_roots();
    assert!(roots.len() >= 2);
    let fragment = format!("n1-trash-preflight-{}", Ulid::new());
    let path = "clinical/report.bin";
    publish_logical_object(&roots, &fragment, path, 31);

    std::fs::remove_file(fragment_root_on(&roots[1], &fragment).join(path))
        .expect("remove later-root live replica");

    assert!(matches!(
        global_namespace_runtime().trash(&fragment, path).await,
        Err(AppError::ServiceUnavailable(_))
    ));
    assert!(path_exists_no_follow(
        &fragment_root_on(&roots[0], &fragment).join(path)
    ));
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
    let expired = sweep_expired_trash_sync(
        &fragment,
        1,
        deleted.deleted_at_unix.expect("deleted timestamp") + 1,
    )
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
async fn n1_trash_list_shows_only_explicit_folder_delete_anchor() {
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
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].path, "videos");
    assert_eq!(entries[0].kind, NamespaceEntryKind::Directory);
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
