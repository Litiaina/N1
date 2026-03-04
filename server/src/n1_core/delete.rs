use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::extract::{Path as AxumPath, State};
use axum::{http::StatusCode, response::IntoResponse};

use futures_util::{StreamExt, stream};
use tokio::fs;
use tracing::{error, info, warn};
use ulid::Ulid;
use walkdir::WalkDir;

use crate::enums::app_error_enum::AppError;
use crate::n1_core::blob_store::{
    free_raw_shards_for_metadata, free_raw_shards_for_metadatas, metadata_contains_raw_shards,
};
use crate::n1_core::disk_manager::{
    ObjectMetadata, ObjectPathManager, compare_version_recency, default_stripe_size,
};
use crate::n1_core::index;
use crate::n1_core::metadata_parser::get_metadata;
use crate::n1_core::quota_manager::update_quota;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::ws_structs::StorageEventPayload;

fn get_trash_root(disk_root: &std::path::Path, fragment_hash: &str) -> std::path::PathBuf {
    disk_root.join(fragment_hash).join(".trash")
}

async fn collect_fragment_versions_streamed(
    fragment: &str,
    trashed: bool,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let mut versions = Vec::new();
    let mut stream = index::stream_fragment_object_versions(fragment.to_string(), trashed);
    while let Some(next_meta) = stream.recv().await {
        versions.push(next_meta?);
    }
    Ok(versions)
}

#[cfg(unix)]
#[allow(dead_code)]
pub async fn sync_dir(path: &std::path::Path) -> Result<(), AppError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        use std::fs::OpenOptions;
        use std::os::unix::fs::OpenOptionsExt;

        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY)
            .open(&path)
            .map_err(|e| AppError::Io(e, format!("open dir for sync: {:?}", path)))?;
        file.sync_all()
            .map_err(|e| AppError::Io(e, format!("sync dir: {:?}", path)))?;
        Ok::<(), AppError>(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("sync dir task: {}", e)))?
}

#[cfg(windows)]
#[allow(dead_code)]
pub async fn sync_dir(path: &std::path::Path) -> Result<(), AppError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        use std::fs::OpenOptions;
        use std::os::windows::fs::OpenOptionsExt;

        const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x02000000;
        if let Ok(file) = OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(&path)
        {
            let _ = file.sync_all();
        }
        Ok::<(), AppError>(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("sync dir task: {}", e)))?
}

#[cfg(not(any(unix, windows)))]
#[allow(dead_code)]
pub async fn sync_dir(_path: &std::path::Path) -> Result<(), AppError> {
    Ok(())
}

fn is_cross_device_rename(err: &std::io::Error) -> bool {
    #[cfg(unix)]
    {
        err.raw_os_error() == Some(libc::EXDEV)
    }
    #[cfg(not(unix))]
    {
        let _ = err;
        false
    }
}

async fn move_dir_or_copy(src: &std::path::Path, dst: &std::path::Path) -> Result<(), AppError> {
    if let Some(parent) = dst.parent() {
        let _ = fs::create_dir_all(parent).await;
    }

    match fs::rename(src, dst).await {
        Ok(_) => Ok(()),
        Err(e) => {
            if !is_cross_device_rename(&e) {
                return Err(AppError::Io(
                    e,
                    format!("rename dir {} -> {}", src.display(), dst.display()),
                ));
            }

            let src = src.to_path_buf();
            let dst = dst.to_path_buf();

            tokio::task::spawn_blocking(move || -> Result<(), AppError> {
                use std::fs as stdfs;

                stdfs::create_dir_all(&dst).map_err(|e| {
                    AppError::Io(e, format!("mkdir {} (copydir fallback)", dst.display()))
                })?;

                for entry in WalkDir::new(&src).into_iter().filter_map(|e| e.ok()) {
                    let p = entry.path();
                    let rel = match p.strip_prefix(&src) {
                        Ok(r) => r,
                        Err(_) => continue,
                    };
                    if rel.as_os_str().is_empty() {
                        continue;
                    }
                    let target = dst.join(rel);

                    if entry.file_type().is_dir() {
                        let _ = stdfs::create_dir_all(&target);
                        continue;
                    }

                    if entry.file_type().is_file() {
                        if let Some(parent) = target.parent() {
                            let _ = stdfs::create_dir_all(parent);
                        }
                        stdfs::copy(p, &target).map_err(|e| {
                            AppError::Io(e, format!("copy {} -> {}", p.display(), target.display()))
                        })?;
                    }
                }

                stdfs::remove_dir_all(&src).map_err(|e| {
                    AppError::Io(
                        e,
                        format!("remove_dir_all {} (after copydir)", src.display()),
                    )
                })?;

                Ok(())
            })
            .await
            .map_err(|e| AppError::InternalServerError(format!("copydir task: {}", e)))??;

            Ok(())
        }
    }
}

async fn write_delete_marker_for_key(
    _path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
    template: Option<&ObjectMetadata>,
) -> Result<ObjectMetadata, AppError> {
    let now = chrono::Utc::now().timestamp();
    let delete_version_id = Ulid::new().to_string();

    let mut dm = template.cloned().unwrap_or_else(|| ObjectMetadata {
        object_key: object_key.to_string(),
        fragment: fragment.to_string(),
        version_id: delete_version_id.clone(),
        upload_session_id: String::new(),
        owner_uid: fragment.to_string(),
        content_type: "application/octet-stream".to_string(),
        total_size_bytes: 0,
        created_at: now,
        parts: Vec::new(),
        is_delete_marker: true,
        stripe_size_bytes: default_stripe_size(),
    });

    dm.fragment = fragment.to_string();
    dm.owner_uid = fragment.to_string();
    dm.object_key = object_key.to_string();
    dm.version_id = delete_version_id.clone();
    dm.created_at = now;
    dm.is_delete_marker = true;
    dm.parts.clear();
    dm.total_size_bytes = 0;

    index::publish_object(&dm).await?;

    Ok(dm)
}

async fn purge_delete_markers_for_key(
    _path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
) {
    let versions = match index::list_live_object_versions_metadata(fragment, object_key).await {
        Ok(v) => v,
        Err(_) => return,
    };

    for meta in versions {
        if !meta.is_delete_marker {
            continue;
        }
        let _ = index::delete_object_version(fragment, object_key, &meta.version_id).await;
    }
}

async fn permanently_delete_trashed_version_only(
    fragment: &str,
    object_key: &str,
    meta: &ObjectMetadata,
    blob_roots: &[std::path::PathBuf],
) -> Result<u64, AppError> {
    let path_manager = ObjectPathManager::new();

    if metadata_contains_raw_shards(meta) {
        if let Err(e) = free_raw_shards_for_metadata(meta).await {
            warn!(
                "Failed to free raw shards for trashed version {}/{}/{}: {}",
                fragment, object_key, meta.version_id, e
            );
        }
    } else {
        for disk in blob_roots {
            let trash_shard = get_trash_root(disk, fragment).join("blob").join(&meta.version_id);
            let _ = fs::remove_dir_all(&trash_shard).await;

            // Best-effort cleanup for stale live shard directories with the same
            // version id. This should not affect newer live versions because
            // version ids are immutable and unique per upload.
            let live_shard = path_manager.get_shard_dir_on_disk(disk, fragment, &meta.version_id);
            let _ = fs::remove_dir_all(&live_shard).await;
        }
    }

    let removed =
        index::delete_trashed_object_version(fragment, object_key, &meta.version_id).await?;
    if !removed {
        return Err(AppError::NotFound);
    }

    Ok(meta.total_size_bytes)
}

pub async fn delete_object_version(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumPath((fragment, version_id, object_key)): AxumPath<(String, String, String)>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    let template_meta = match index::move_object_version_to_trash(&fragment, &object_key, &version_id)
        .await?
    {
        Some(meta) => meta,
        None => {
            let trashed_meta =
                index::read_trashed_object_version(&fragment, &object_key, &version_id).await?;
            let refunded = permanently_delete_trashed_version_only(
                &fragment,
                &object_key,
                &trashed_meta,
                &blob_roots,
            )
            .await?;
            if refunded > 0 {
                if let Err(e) = update_quota(&fragment, -(refunded as i64)).await {
                    error!(
                        "Failed to refund quota after permanent version delete {}/{}/{}: {}",
                        fragment, object_key, version_id, e
                    );
                }
            }

            info!(
                "Permanently deleted trashed version: {}/{} (v: {})",
                fragment, object_key, version_id
            );

            state.emit(
                &fragment,
                StorageEventPayload::FileDeleted {
                    fragment: fragment.clone(),
                    object_key,
                    version_id: Some(version_id),
                },
            );

            return Ok(StatusCode::OK);
        }
    };

    let raw_blob_version = metadata_contains_raw_shards(&template_meta);

    if !raw_blob_version {
        for root in &blob_roots {
            let src_blob_dir = path_manager.get_shard_dir_on_disk(root, &fragment, &version_id);
            if !fs::try_exists(&src_blob_dir).await.unwrap_or(false) {
                continue;
            }

            let trash_root = get_trash_root(root, &fragment);
            let dest_blob_dir = trash_root.join("blob").join(&version_id);

            match move_dir_or_copy(&src_blob_dir, &dest_blob_dir).await {
                Ok(_) => {}
                Err(e) => warn!("Failed to move blob to trash: {}", e),
            }
        }
    }

    let latest_live = match get_metadata(&fragment, &object_key, &fragment).await {
        Ok(meta) if !meta.is_delete_marker => Some(meta),
        _ => None,
    };

    if latest_live.is_none() {
        let _ = write_delete_marker_for_key(
            &path_manager,
            &fragment,
            &object_key,
            Some(&template_meta),
        )
        .await;
    }

    info!(
        "Soft deleted (moved to trash) version: {}/{} (v: {})",
        fragment, object_key, version_id
    );

    state.emit(
        &fragment,
        StorageEventPayload::FileDeleted {
            fragment: fragment.clone(),
            object_key,
            version_id: Some(version_id),
        },
    );

    Ok(StatusCode::OK)
}

pub async fn delete_object(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumPath((fragment, object_key)): AxumPath<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let path_manager = ObjectPathManager::new();
    let versions_found = index::list_live_object_versions_metadata(&fragment, &object_key).await?;

    if versions_found.is_empty() {
        return Err(AppError::NotFound);
    }

    let blob_roots = path_manager.get_blob_roots();

    let mut success_count = 0usize;
    let mut template_meta: Option<ObjectMetadata> = None;

    for meta in &versions_found {
        if template_meta.is_none() {
            template_meta = Some(meta.clone());
        }

        let moved =
            index::move_object_version_to_trash(&fragment, &object_key, &meta.version_id).await?;
        if moved.is_none() {
            continue;
        }

        if !metadata_contains_raw_shards(meta) {
            for root in &blob_roots {
                let src_blob =
                    path_manager.get_shard_dir_on_disk(root, &fragment, &meta.version_id);
                if fs::try_exists(&src_blob).await.unwrap_or(false) {
                    let trash_root = get_trash_root(root, &fragment);
                    let dst_blob = trash_root.join("blob").join(&meta.version_id);
                    let _ = move_dir_or_copy(&src_blob, &dst_blob).await;
                }
            }
        }

        success_count += 1;
    }

    let _ = write_delete_marker_for_key(
        &path_manager,
        &fragment,
        &object_key,
        template_meta.as_ref(),
    )
    .await;

    info!(
        "Soft deleted {} versions for object {}/{}",
        success_count, fragment, object_key
    );

    state.emit(
        &fragment,
        StorageEventPayload::FileDeleted {
            fragment: fragment.clone(),
            object_key,
            version_id: None,
        },
    );

    Ok(StatusCode::OK)
}

pub async fn empty_trash(
    State(_state): State<Arc<AppState>>,
    claims: Claims,
    AxumPath(fragment): AxumPath<String>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.delete {
        return Err(AppError::Forbidden("Delete permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    let trash_versions = collect_fragment_versions_streamed(&fragment, true).await?;
    let total_refund_size: u64 = trash_versions
        .iter()
        .map(|meta| meta.total_size_bytes)
        .sum();

    let raw_trash_versions = trash_versions
        .iter()
        .filter(|meta| metadata_contains_raw_shards(meta))
        .cloned()
        .collect::<Vec<_>>();
    let touched_partitions = trash_versions
        .iter()
        .map(|meta| index::object_partition_id(&meta.fragment, &meta.object_key))
        .collect::<Result<BTreeSet<_>, _>>()?;
    if !raw_trash_versions.is_empty() {
        free_raw_shards_for_metadatas(&raw_trash_versions).await?;
    }

    index::clear_fragment_trash_in_partitions(&fragment, &touched_partitions).await?;

    let mut delete_tasks: Vec<Pin<Box<dyn Future<Output = ()> + Send>>> = Vec::new();
    for root in &blob_roots {
        let trash_root = get_trash_root(root, &fragment);
        delete_tasks.push(Box::pin(async move {
            if fs::try_exists(&trash_root).await.unwrap_or(false) {
                if let Err(e) = fs::remove_dir_all(&trash_root).await {
                    warn!("Failed to empty trash blob root {:?}: {}", trash_root, e);
                }
            }
        }));
    }
    futures_util::future::join_all(delete_tasks).await;

    if total_refund_size > 0 {
        if let Err(e) = update_quota(&fragment, -(total_refund_size as i64)).await {
            error!(
                "Failed to refund quota after emptying trash for {}: {}",
                fragment, e
            );
        } else {
            info!(
                "Refunded {} bytes after emptying trash for {}",
                total_refund_size, fragment
            );
        }
    }

    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_object_permanently(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumPath((fragment, object_key)): AxumPath<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.delete {
        return Err(AppError::Forbidden("Delete permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    let mut versions_to_delete = index::list_live_object_versions_metadata(&fragment, &object_key)
        .await
        .unwrap_or_default();
    versions_to_delete.extend(
        index::list_trashed_object_versions_metadata(&fragment, &object_key)
            .await
            .unwrap_or_default(),
    );
    versions_to_delete.sort_by(|left, right| left.version_id.cmp(&right.version_id));
    versions_to_delete.dedup_by(|left, right| left.version_id == right.version_id);

    if versions_to_delete.is_empty() {
        return Err(AppError::NotFound);
    }

    let total_freed: u64 = stream::iter(versions_to_delete)
        .map(|meta| {
            let f = fragment.clone();
            let object_key = object_key.clone();
            let blobs = blob_roots.clone();
            async move {
                let pm = ObjectPathManager::new();
                if metadata_contains_raw_shards(&meta) {
                    let _ = free_raw_shards_for_metadata(&meta).await;
                } else {
                    for disk in &blobs {
                        let live_shard = pm.get_shard_dir_on_disk(disk, &f, &meta.version_id);
                        let _ = fs::remove_dir_all(&live_shard).await;

                        let trash_shard =
                            get_trash_root(disk, &f).join("blob").join(&meta.version_id);
                        let _ = fs::remove_dir_all(&trash_shard).await;
                    }
                }

                let _ = index::delete_object_version(&f, &object_key, &meta.version_id).await;
                let _ =
                    index::delete_trashed_object_version(&f, &object_key, &meta.version_id).await;

                meta.total_size_bytes
            }
        })
        .buffer_unordered(20)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .sum();

    let _ = index::clear_object_latest_states(&fragment, &object_key).await;

    if total_freed > 0 {
        if let Err(e) = update_quota(&fragment, -(total_freed as i64)).await {
            error!(
                "Failed to refund quota for permanent delete {}: {}",
                fragment, e
            );
        }
    }

    info!(
        "Permanently deleted {} bytes for {}/{}",
        total_freed, fragment, object_key
    );

    state.emit(
        &fragment,
        StorageEventPayload::FileDeleted {
            fragment: fragment.clone(),
            object_key,
            version_id: None,
        },
    );

    Ok(StatusCode::NO_CONTENT)
}

pub async fn recover_object_version(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumPath((fragment, version_id, object_key)): AxumPath<(String, String, String)>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden(
            "Write permission required to recover".into(),
        ));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    purge_delete_markers_for_key(&path_manager, &fragment, &object_key).await;

    for root in &blob_roots {
        let trash_blob_dir = get_trash_root(root, &fragment)
            .join("blob")
            .join(&version_id);

        if !fs::try_exists(&trash_blob_dir).await.unwrap_or(false) {
            continue;
        }

        let live_blob_dir = path_manager.get_shard_dir_on_disk(root, &fragment, &version_id);
        if let Some(parent) = live_blob_dir.parent() {
            let _ = fs::create_dir_all(parent).await;
        }

        let _ = move_dir_or_copy(&trash_blob_dir, &live_blob_dir).await;
    }

    let meta = index::restore_object_version_from_trash(&fragment, &object_key, &version_id, true)
        .await?
        .ok_or(AppError::NotFound)?;

    info!(
        "Recovered object version: {}/{} (v: {})",
        fragment, object_key, version_id
    );

    state.emit(&fragment, StorageEventPayload::FileUploaded(meta));

    Ok(StatusCode::OK)
}

pub async fn recover_object(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumPath((fragment, object_key)): AxumPath<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden(
            "Write permission required to recover".into(),
        ));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    let mut found_versions =
        index::list_trashed_object_versions_metadata(&fragment, &object_key).await?;

    if found_versions.is_empty() {
        return Err(AppError::NotFound);
    }

    found_versions.sort_by(|left, right| {
        compare_version_recency(
            left.created_at,
            &left.version_id,
            right.created_at,
            &right.version_id,
        )
    });
    purge_delete_markers_for_key(&path_manager, &fragment, &object_key).await;

    let mut recovered_count = 0usize;
    for meta in &found_versions {
        for root in &blob_roots {
            let trash_blob = get_trash_root(root, &fragment)
                .join("blob")
                .join(&meta.version_id);

            if fs::try_exists(&trash_blob).await.unwrap_or(false) {
                let live_blob =
                    path_manager.get_shard_dir_on_disk(root, &fragment, &meta.version_id);
                if let Some(p) = live_blob.parent() {
                    let _ = fs::create_dir_all(p).await;
                }
                let _ = move_dir_or_copy(&trash_blob, &live_blob).await;
            }
        }

        let Some(_restored) = index::restore_object_version_from_trash(
            &fragment,
            &object_key,
            &meta.version_id,
            false,
        )
        .await?
        else {
            continue;
        };

        recovered_count += 1;
    }

    info!(
        "Recovered {} versions for object {}/{}",
        recovered_count, fragment, object_key
    );

    state.emit(&fragment, StorageEventPayload::StateInvalidated);

    Ok(StatusCode::OK)
}

pub async fn recover_all_trash(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumPath(fragment): AxumPath<String>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden(
            "Write permission required to recover all".into(),
        ));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    let recovered_versions = collect_fragment_versions_streamed(&fragment, true).await?;
    let mut sorted_versions = recovered_versions;
    sorted_versions.sort_by(|left, right| {
        left.object_key.cmp(&right.object_key).then_with(|| {
            compare_version_recency(
                left.created_at,
                &left.version_id,
                right.created_at,
                &right.version_id,
            )
        })
    });

    let unique_keys = sorted_versions
        .iter()
        .map(|meta| meta.object_key.clone())
        .collect::<BTreeSet<_>>();
    for object_key in &unique_keys {
        purge_delete_markers_for_key(&path_manager, &fragment, object_key).await;
    }

    for meta in sorted_versions {
        for root in &blob_roots {
            let trash_blob = get_trash_root(root, &fragment)
                .join("blob")
                .join(&meta.version_id);
            if fs::try_exists(&trash_blob).await.unwrap_or(false) {
                let live_blob =
                    path_manager.get_shard_dir_on_disk(root, &fragment, &meta.version_id);
                if let Some(parent) = live_blob.parent() {
                    let _ = fs::create_dir_all(parent).await;
                }
                let _ = move_dir_or_copy(&trash_blob, &live_blob).await;
            }
        }

        let Some(_restored) = index::restore_object_version_from_trash(
            &fragment,
            &meta.object_key,
            &meta.version_id,
            false,
        )
        .await?
        else {
            continue;
        };
    }

    info!("Recovered all items from trash for fragment {}", fragment);
    state.emit(&fragment, StorageEventPayload::StateInvalidated);

    Ok(StatusCode::OK)
}
