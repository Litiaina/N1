use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::Json as AxumJson;
use axum::extract::{Path as AxumPath, State};
use axum::{http::StatusCode, response::IntoResponse};

use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use tokio::fs;
use tracing::{info, warn};
use walkdir::WalkDir;

use crate::enums::app_error_enum::AppError;
use crate::n1_core::disk_manager::ObjectPathManager;
use crate::n1_core::index;
use crate::n1_core::prefix_updates::mark_fragment_accelerator_stale;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::ws_structs::StorageEventPayload;

fn get_trash_root(disk_root: &std::path::Path, fragment_hash: &str) -> std::path::PathBuf {
    disk_root.join(fragment_hash).join(".trash")
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

async fn purge_delete_markers_for_key(fragment: &str, object_key: &str) {
    if let Ok(metas) = index::list_live_object_versions_metadata(fragment, object_key).await {
        for meta in metas {
            if !meta.is_delete_marker {
                continue;
            }
            let _ = index::delete_object_version(fragment, object_key, &meta.version_id).await;
        }
    }
}

async fn list_object_versions_any(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<String>, AppError> {
    index::list_live_object_version_ids(fragment, object_key).await
}

async fn list_trashed_object_versions_any(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<String>, AppError> {
    index::list_trashed_object_version_ids(fragment, object_key).await
}

async fn move_blob_versions_between_states(
    path_manager: &ObjectPathManager,
    blob_roots: &[std::path::PathBuf],
    fragment: &str,
    version_ids: &[String],
    from_trash: bool,
) {
    for version_id in version_ids {
        for root in blob_roots {
            let src = if from_trash {
                get_trash_root(root, fragment).join("blob").join(version_id)
            } else {
                path_manager.get_shard_dir_on_disk(root, fragment, version_id)
            };
            if !fs::try_exists(&src).await.unwrap_or(false) {
                continue;
            }

            let dst = if from_trash {
                path_manager.get_shard_dir_on_disk(root, fragment, version_id)
            } else {
                get_trash_root(root, fragment).join("blob").join(version_id)
            };

            let _ = move_dir_or_copy(&src, &dst).await;
        }
    }
}

async fn remove_blob_versions(
    path_manager: &ObjectPathManager,
    blob_roots: &[std::path::PathBuf],
    fragment: &str,
    version_ids: &[String],
) {
    for version_id in version_ids {
        for disk in blob_roots {
            let live_shard = path_manager.get_shard_dir_on_disk(disk, fragment, version_id);
            let _ = fs::remove_dir_all(&live_shard).await;

            let trash_shard = get_trash_root(disk, fragment).join("blob").join(version_id);
            let _ = fs::remove_dir_all(&trash_shard).await;
        }
    }
}

async fn mark_accelerators_stale(fragment: &str, live: bool, trash: bool) {
    if live {
        if let Err(e) = mark_fragment_accelerator_stale(fragment, false).await {
            warn!(
                "Failed to mark live accelerator stale for fragment {}: {}",
                fragment, e
            );
        }
    }

    if trash {
        if let Err(e) = mark_fragment_accelerator_stale(fragment, true).await {
            warn!(
                "Failed to mark trash accelerator stale for fragment {}: {}",
                fragment, e
            );
        }
    }
}

fn bulk_object_concurrency_limit() -> usize {
    std::thread::available_parallelism()
        .map(|n| (n.get() * 2).clamp(4, 32))
        .unwrap_or(8)
}

#[derive(Debug, Deserialize)]
pub struct BulkObjectKeysRequest {
    pub object_keys: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct BulkObjectFailure {
    pub object_key: String,
    pub error: String,
}

#[derive(Debug, Serialize)]
pub struct BulkObjectOperationResponse {
    pub requested: usize,
    pub unique: usize,
    pub succeeded: usize,
    pub not_found: usize,
    pub failed: Vec<BulkObjectFailure>,
}

async fn delete_object_core(fragment: &str, object_key: &str) -> Result<usize, AppError> {
    let path_manager = ObjectPathManager::new();
    let versions_found = list_object_versions_any(fragment, object_key).await?;

    if versions_found.is_empty() {
        return Err(AppError::NotFound);
    }

    let blob_roots = path_manager.get_blob_roots();
    let moved_versions = match index::move_object_to_trash_fast(fragment, object_key).await {
        Ok(Some(version_ids)) => version_ids,
        Ok(None) => {
            let versions_found = list_object_versions_any(fragment, object_key).await?;
            if versions_found.is_empty() {
                return Err(AppError::NotFound);
            }
            let mut moved = Vec::new();
            for version_id in &versions_found {
                let _moved_meta = match index::move_object_version_to_trash_with_metadata(
                    fragment, object_key, version_id,
                )
                .await
                {
                    Ok(meta) => meta,
                    Err(e) => {
                        warn!(
                            "Partition trash move failed for {}/{}#{}: {}",
                            fragment, object_key, version_id, e
                        );
                        continue;
                    }
                };
                moved.push(version_id.clone());
            }
            moved
        }
        Err(e) => return Err(e),
    };

    for version_id in &moved_versions {
        let _ = index::remove_session(fragment, version_id, "soft-delete-object").await;
    }
    move_blob_versions_between_states(&path_manager, &blob_roots, fragment, &moved_versions, false)
        .await;
    mark_accelerators_stale(fragment, true, true).await;

    Ok(moved_versions.len())
}

async fn recover_object_core(fragment: &str, object_key: &str) -> Result<usize, AppError> {
    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    purge_delete_markers_for_key(fragment, object_key).await;

    let restored_versions = match index::restore_object_from_trash_fast(fragment, object_key).await
    {
        Ok(Some(version_ids)) => version_ids,
        Ok(None) => {
            let mut found_versions = list_trashed_object_versions_any(fragment, object_key).await?;
            if found_versions.is_empty() {
                return Err(AppError::NotFound);
            }
            found_versions.sort();
            found_versions.dedup();
            let mut restored = Vec::new();
            for version_id in &found_versions {
                if let Err(e) = index::restore_object_version_from_trash_with_metadata(
                    fragment, object_key, version_id,
                )
                .await
                {
                    warn!(
                        "Partition restore failed for {}/{}#{}: {}",
                        fragment, object_key, version_id, e
                    );
                    continue;
                }
                restored.push(version_id.clone());
            }
            restored
        }
        Err(e) => return Err(e),
    };

    move_blob_versions_between_states(
        &path_manager,
        &blob_roots,
        fragment,
        &restored_versions,
        true,
    )
    .await;
    mark_accelerators_stale(fragment, true, true).await;

    Ok(restored_versions.len())
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
    if let Ok(_meta) = index::read_trashed_object_version(&fragment, &object_key, &version_id).await
    {
        for root in &blob_roots {
            let trash_blob_dir = get_trash_root(root, &fragment)
                .join("blob")
                .join(&version_id);
            let _ = fs::remove_dir_all(&trash_blob_dir).await;
        }
        let _ = index::delete_trashed_object_version(&fragment, &object_key, &version_id).await;
        let _ = index::remove_session(&fragment, &version_id, "permanent-delete-version").await;
        mark_accelerators_stale(&fragment, false, true).await;
        state.emit(
            &fragment,
            StorageEventPayload::FileDeleted {
                fragment: fragment.clone(),
                object_key,
                version_id: Some(version_id),
            },
        );
        return Ok(StatusCode::NO_CONTENT);
    }

    let _moved_meta =
        index::move_object_version_to_trash_with_metadata(&fragment, &object_key, &version_id)
            .await?;

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

    let _ = index::remove_session(&fragment, &version_id, "soft-delete-version").await;
    mark_accelerators_stale(&fragment, true, true).await;

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

    let moved_count = delete_object_core(&fragment, &object_key).await?;

    info!(
        "Soft deleted {} versions for object {}/{}",
        moved_count, fragment, object_key
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

pub async fn delete_objects_bulk(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumJson(payload): AxumJson<BulkObjectKeysRequest>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }

    let fragment = claims.fragment.clone();
    let requested = payload.object_keys.len();
    let unique_keys: Vec<String> = payload
        .object_keys
        .into_iter()
        .filter(|key| !key.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    if unique_keys.is_empty() {
        return Err(AppError::BadRequest("object_keys must not be empty".into()));
    }

    let concurrency = bulk_object_concurrency_limit();
    let results = stream::iter(unique_keys.iter().cloned().map(|object_key| {
        let fragment = fragment.clone();
        async move {
            let result = delete_object_core(&fragment, &object_key).await;
            (object_key, result)
        }
    }))
    .buffer_unordered(concurrency)
    .collect::<Vec<_>>()
    .await;

    let mut succeeded = 0usize;
    let mut not_found = 0usize;
    let mut failed = Vec::new();

    for (object_key, result) in results {
        match result {
            Ok(_) => succeeded = succeeded.saturating_add(1),
            Err(AppError::NotFound) => not_found = not_found.saturating_add(1),
            Err(e) => failed.push(BulkObjectFailure {
                object_key,
                error: e.to_string(),
            }),
        }
    }

    if succeeded > 0 {
        state.emit(&fragment, StorageEventPayload::StateInvalidated);
    }

    Ok((
        StatusCode::OK,
        AxumJson(BulkObjectOperationResponse {
            requested,
            unique: unique_keys.len(),
            succeeded,
            not_found,
            failed,
        }),
    ))
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
    let index_roots = path_manager.get_all_index_roots();
    let blob_roots = path_manager.get_blob_roots();

    index::clear_fragment_trash_in_partitions(&fragment, None).await?;

    let mut trash_targets = BTreeSet::new();
    for root in &index_roots {
        trash_targets.insert(get_trash_root(root, &fragment));
    }
    for root in &blob_roots {
        trash_targets.insert(get_trash_root(root, &fragment));
    }
    let mut delete_tasks: Vec<Pin<Box<dyn Future<Output = ()> + Send>>> = Vec::new();

    for trash_root in trash_targets {
        delete_tasks.push(Box::pin(async move {
            if fs::try_exists(&trash_root).await.unwrap_or(false) {
                if let Err(e) = fs::remove_dir_all(&trash_root).await {
                    warn!("Failed to empty trash root {:?}: {}", trash_root, e);
                }
            }
        }));
    }

    futures_util::future::join_all(delete_tasks).await;

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
    let versions_to_delete = match index::delete_object_fast(&fragment, &object_key).await? {
        Some(version_ids) => version_ids,
        None => return Err(AppError::NotFound),
    };
    let blob_roots = path_manager.get_blob_roots();
    remove_blob_versions(&path_manager, &blob_roots, &fragment, &versions_to_delete).await;
    for version_id in &versions_to_delete {
        let _ = index::remove_session(&fragment, version_id, "permanent-delete").await;
    }
    mark_accelerators_stale(&fragment, true, true).await;

    info!(
        "Permanently deleted {} versions for {}/{}",
        versions_to_delete.len(),
        fragment,
        object_key
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
    purge_delete_markers_for_key(&fragment, &object_key).await;
    let _restored_meta =
        index::restore_object_version_from_trash_with_metadata(&fragment, &object_key, &version_id)
            .await?;

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

    mark_accelerators_stale(&fragment, true, true).await;

    info!(
        "Recovered object version: {}/{} (v: {})",
        fragment, object_key, version_id
    );

    if let Ok(meta) = index::read_object_version(&fragment, &object_key, &version_id).await {
        state.emit(&fragment, StorageEventPayload::FileUploaded(meta));
    }

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

    let restored_count = recover_object_core(&fragment, &object_key).await?;

    info!(
        "Recovered {} versions for object {}/{}",
        restored_count, fragment, object_key
    );

    state.emit(&fragment, StorageEventPayload::StateInvalidated);

    Ok(StatusCode::OK)
}

pub async fn recover_objects_bulk(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumJson(payload): AxumJson<BulkObjectKeysRequest>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden(
            "Write permission required to recover".into(),
        ));
    }

    let fragment = claims.fragment.clone();
    let requested = payload.object_keys.len();
    let unique_keys: Vec<String> = payload
        .object_keys
        .into_iter()
        .filter(|key| !key.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    if unique_keys.is_empty() {
        return Err(AppError::BadRequest("object_keys must not be empty".into()));
    }

    let concurrency = bulk_object_concurrency_limit();
    let results = stream::iter(unique_keys.iter().cloned().map(|object_key| {
        let fragment = fragment.clone();
        async move {
            let result = recover_object_core(&fragment, &object_key).await;
            (object_key, result)
        }
    }))
    .buffer_unordered(concurrency)
    .collect::<Vec<_>>()
    .await;

    let mut succeeded = 0usize;
    let mut not_found = 0usize;
    let mut failed = Vec::new();

    for (object_key, result) in results {
        match result {
            Ok(_) => succeeded = succeeded.saturating_add(1),
            Err(AppError::NotFound) => not_found = not_found.saturating_add(1),
            Err(e) => failed.push(BulkObjectFailure {
                object_key,
                error: e.to_string(),
            }),
        }
    }

    if succeeded > 0 {
        state.emit(&fragment, StorageEventPayload::StateInvalidated);
    }

    Ok((
        StatusCode::OK,
        AxumJson(BulkObjectOperationResponse {
            requested,
            unique: unique_keys.len(),
            succeeded,
            not_found,
            failed,
        }),
    ))
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

    let mut trashed_keys = BTreeSet::new();
    if let Ok(trashed_latest) = index::list_fragment_trash_objects(&fragment).await {
        for meta in trashed_latest {
            trashed_keys.insert(meta.object_key);
        }
    }

    if trashed_keys.is_empty() {
        return Ok(StatusCode::OK);
    }
    let concurrency = bulk_object_concurrency_limit();
    let recovered_count = stream::iter(trashed_keys.into_iter().map(|object_key| {
        let fragment = fragment.clone();
        async move {
            match recover_object_core(&fragment, &object_key).await {
                Ok(count) => count,
                Err(AppError::NotFound) => 0,
                Err(e) => {
                    warn!("Recover-all failed for {}/{}: {}", fragment, object_key, e);
                    0
                }
            }
        }
    }))
    .buffer_unordered(concurrency)
    .fold(
        0usize,
        |acc, count| async move { acc.saturating_add(count) },
    )
    .await;

    if recovered_count > 0 {
        mark_accelerators_stale(&fragment, true, true).await;
    }

    info!(
        "Recovered {} trashed item(s) for fragment {}",
        recovered_count, fragment
    );
    state.emit(&fragment, StorageEventPayload::StateInvalidated);

    Ok(StatusCode::OK)
}
