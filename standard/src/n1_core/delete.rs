use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::extract::{Path as AxumPath, State};
use axum::{http::StatusCode, response::IntoResponse};

use tokio::fs;
use tracing::{info, warn};
use walkdir::WalkDir;

use crate::enums::app_error_enum::AppError;
use crate::n1_core::disk_manager::{ObjectMetadata, ObjectPathManager};
use crate::n1_core::index;
use crate::n1_core::prefix_updates::{
    logical_now, synthetic_delete_marker, synthetic_live_entry, write_prefix_entry_update,
};
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
    let had_trash_before = !list_trashed_object_versions_any(&fragment, &object_key)
        .await?
        .is_empty();
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
        let trashed_after =
            index::list_trashed_object_versions_metadata(&fragment, &object_key).await?;
        let ts = logical_now();
        if let Some(latest_trash) = trashed_after.first() {
            let latest_entry = synthetic_live_entry(latest_trash, ts);
            let _ = write_prefix_entry_update(
                &fragment,
                &object_key,
                &latest_trash.version_id,
                latest_entry,
                0,
                true,
            )
            .await;
        } else {
            let tombstone_source = ObjectMetadata {
                object_key: object_key.clone(),
                fragment: fragment.clone(),
                version_id: version_id.clone(),
                upload_session_id: String::new(),
                owner_uid: String::new(),
                content_type: String::new(),
                total_size_bytes: 0,
                created_at: ts,
                parts: Vec::new(),
                is_delete_marker: true,
                stripe_size_bytes: 0,
            };
            let tombstone = synthetic_delete_marker(&tombstone_source, ts);
            let delta = if had_trash_before { -1 } else { 0 };
            let _ = write_prefix_entry_update(
                &fragment,
                &object_key,
                &version_id,
                tombstone,
                delta,
                true,
            )
            .await;
        }
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

    let moved_meta =
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
    let live_after = index::list_live_object_versions_metadata(&fragment, &object_key).await?;
    let trash_after = index::list_trashed_object_versions_metadata(&fragment, &object_key).await?;
    let ts = logical_now();

    let live_tombstone = synthetic_delete_marker(&moved_meta, ts);
    let live_child_delta = if live_after.is_empty() { -1 } else { 0 };
    let _ = write_prefix_entry_update(
        &fragment,
        &object_key,
        &moved_meta.version_id,
        live_tombstone,
        live_child_delta,
        false,
    )
    .await;
    if let Some(latest_live) = live_after.first() {
        let live_entry = synthetic_live_entry(latest_live, ts.saturating_add(1));
        let _ = write_prefix_entry_update(
            &fragment,
            &object_key,
            &latest_live.version_id,
            live_entry,
            0,
            false,
        )
        .await;
    }

    if let Some(latest_trash) = trash_after.first() {
        let trash_entry = synthetic_live_entry(latest_trash, ts.saturating_add(2));
        let _ = write_prefix_entry_update(
            &fragment,
            &object_key,
            &latest_trash.version_id,
            trash_entry,
            if had_trash_before { 0 } else { 1 },
            true,
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
    let versions_found = list_object_versions_any(&fragment, &object_key).await?;

    if versions_found.is_empty() {
        return Err(AppError::NotFound);
    }

    let had_trash_before = !list_trashed_object_versions_any(&fragment, &object_key)
        .await?
        .is_empty();
    let live_latest_before = index::read_latest_object(&fragment, &object_key).await.ok();
    let blob_roots = path_manager.get_blob_roots();
    let moved_versions = match index::move_object_to_trash_fast(&fragment, &object_key).await {
        Ok(Some(version_ids)) => version_ids,
        Ok(None) => {
            let versions_found = list_object_versions_any(&fragment, &object_key).await?;
            if versions_found.is_empty() {
                return Err(AppError::NotFound);
            }
            let mut moved = Vec::new();
            for version_id in &versions_found {
                let _moved_meta = match index::move_object_version_to_trash_with_metadata(
                    &fragment,
                    &object_key,
                    version_id,
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
        let _ = index::remove_session(&fragment, version_id, "soft-delete-object").await;
    }
    move_blob_versions_between_states(
        &path_manager,
        &blob_roots,
        &fragment,
        &moved_versions,
        false,
    )
    .await;
    let trash_after = index::list_trashed_object_versions_metadata(&fragment, &object_key).await?;
    let ts = logical_now();
    if let Some(anchor) = live_latest_before.as_ref().or(trash_after.first()) {
        let live_tombstone = synthetic_delete_marker(anchor, ts);
        let _ = write_prefix_entry_update(
            &fragment,
            &object_key,
            &anchor.version_id,
            live_tombstone,
            -1,
            false,
        )
        .await;
    }
    if let Some(latest_trash) = trash_after.first() {
        let trash_entry = synthetic_live_entry(latest_trash, ts.saturating_add(1));
        let _ = write_prefix_entry_update(
            &fragment,
            &object_key,
            &latest_trash.version_id,
            trash_entry,
            if had_trash_before { 0 } else { 1 },
            true,
        )
        .await;
    }

    info!(
        "Soft deleted {} versions for object {}/{}",
        moved_versions.len(),
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
    let live_latest_before = index::read_latest_object(&fragment, &object_key).await.ok();
    let trash_latest_before = index::list_trashed_object_versions_metadata(&fragment, &object_key)
        .await?
        .into_iter()
        .next();
    let versions_to_delete = match index::delete_object_fast(&fragment, &object_key).await? {
        Some(version_ids) => version_ids,
        None => return Err(AppError::NotFound),
    };
    let blob_roots = path_manager.get_blob_roots();
    remove_blob_versions(&path_manager, &blob_roots, &fragment, &versions_to_delete).await;
    for version_id in &versions_to_delete {
        let _ = index::remove_session(&fragment, version_id, "permanent-delete").await;
    }

    let ts = logical_now();
    if let Some(live_meta) = live_latest_before.as_ref() {
        let tombstone = synthetic_delete_marker(live_meta, ts);
        let _ = write_prefix_entry_update(
            &fragment,
            &object_key,
            &live_meta.version_id,
            tombstone,
            -1,
            false,
        )
        .await;
    }
    if let Some(trash_meta) = trash_latest_before.as_ref() {
        let tombstone = synthetic_delete_marker(trash_meta, ts.saturating_add(1));
        let _ = write_prefix_entry_update(
            &fragment,
            &object_key,
            &trash_meta.version_id,
            tombstone,
            -1,
            true,
        )
        .await;
    }

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
    let had_live_before = !list_object_versions_any(&fragment, &object_key)
        .await?
        .is_empty();
    let had_trash_before = !list_trashed_object_versions_any(&fragment, &object_key)
        .await?
        .is_empty();
    purge_delete_markers_for_key(&fragment, &object_key).await;
    let restored_meta =
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

    let live_after = index::list_live_object_versions_metadata(&fragment, &object_key).await?;
    let trash_after = index::list_trashed_object_versions_metadata(&fragment, &object_key).await?;
    let ts = logical_now();

    if let Some(latest_live) = live_after.first() {
        let live_entry = synthetic_live_entry(latest_live, ts.saturating_add(1));
        let _ = write_prefix_entry_update(
            &fragment,
            &object_key,
            &latest_live.version_id,
            live_entry,
            if had_live_before { 0 } else { 1 },
            false,
        )
        .await;
    }

    let trash_tombstone = synthetic_delete_marker(&restored_meta, ts);
    let trash_child_delta = if had_trash_before && trash_after.is_empty() {
        -1
    } else {
        0
    };
    let _ = write_prefix_entry_update(
        &fragment,
        &object_key,
        &restored_meta.version_id,
        trash_tombstone,
        trash_child_delta,
        true,
    )
    .await;
    if let Some(latest_trash) = trash_after.first() {
        let trash_entry = synthetic_live_entry(latest_trash, ts.saturating_add(2));
        let _ = write_prefix_entry_update(
            &fragment,
            &object_key,
            &latest_trash.version_id,
            trash_entry,
            0,
            true,
        )
        .await;
    }

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

    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    let had_live_before = !list_object_versions_any(&fragment, &object_key)
        .await?
        .is_empty();
    let had_trash_before = !list_trashed_object_versions_any(&fragment, &object_key)
        .await?
        .is_empty();
    purge_delete_markers_for_key(&fragment, &object_key).await;

    let restored_versions =
        match index::restore_object_from_trash_fast(&fragment, &object_key).await {
            Ok(Some(version_ids)) => version_ids,
            Ok(None) => {
                let mut found_versions =
                    list_trashed_object_versions_any(&fragment, &object_key).await?;
                if found_versions.is_empty() {
                    return Err(AppError::NotFound);
                }
                found_versions.sort();
                found_versions.dedup();
                let mut restored = Vec::new();
                for version_id in &found_versions {
                    if let Err(e) = index::restore_object_version_from_trash_with_metadata(
                        &fragment,
                        &object_key,
                        version_id,
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
        &fragment,
        &restored_versions,
        true,
    )
    .await;
    let live_after = index::list_live_object_versions_metadata(&fragment, &object_key).await?;
    let trash_after = index::list_trashed_object_versions_metadata(&fragment, &object_key).await?;
    let ts = logical_now();
    if let Some(latest_live) = live_after.first() {
        let live_entry = synthetic_live_entry(latest_live, ts.saturating_add(1));
        let _ = write_prefix_entry_update(
            &fragment,
            &object_key,
            &latest_live.version_id,
            live_entry,
            if had_live_before { 0 } else { 1 },
            false,
        )
        .await;
    }
    if let Some(restored_anchor) = live_after.first() {
        let trash_tombstone = synthetic_delete_marker(restored_anchor, ts);
        let trash_child_delta = if had_trash_before && trash_after.is_empty() {
            -1
        } else {
            0
        };
        let _ = write_prefix_entry_update(
            &fragment,
            &object_key,
            &restored_anchor.version_id,
            trash_tombstone,
            trash_child_delta,
            true,
        )
        .await;
    }
    if let Some(latest_trash) = trash_after.first() {
        let trash_entry = synthetic_live_entry(latest_trash, ts.saturating_add(2));
        let _ = write_prefix_entry_update(
            &fragment,
            &object_key,
            &latest_trash.version_id,
            trash_entry,
            0,
            true,
        )
        .await;
    }

    info!(
        "Recovered {} versions for object {}/{}",
        restored_versions.len(),
        fragment,
        object_key
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

    let mut trashed_keys = BTreeSet::new();
    if let Ok(trashed_latest) = index::list_fragment_trash_objects(&fragment).await {
        for meta in trashed_latest {
            trashed_keys.insert(meta.object_key);
        }
    }

    if trashed_keys.is_empty() {
        return Ok(StatusCode::OK);
    }
    let mut recovered_count = 0usize;

    for object_key in trashed_keys {
        let had_live_before = !list_object_versions_any(&fragment, &object_key)
            .await?
            .is_empty();
        let had_trash_before = !list_trashed_object_versions_any(&fragment, &object_key)
            .await?
            .is_empty();
        purge_delete_markers_for_key(&fragment, &object_key).await;

        let restored_versions =
            match index::restore_object_from_trash_fast(&fragment, &object_key).await {
                Ok(Some(version_ids)) => version_ids,
                Ok(None) => {
                    let Ok(metas) =
                        index::list_trashed_object_versions_metadata(&fragment, &object_key).await
                    else {
                        continue;
                    };
                    if metas.is_empty() {
                        continue;
                    }

                    let mut restored = Vec::new();
                    for meta in metas {
                        if let Err(e) = index::restore_object_version_from_trash_with_metadata(
                            &fragment,
                            &object_key,
                            &meta.version_id,
                        )
                        .await
                        {
                            warn!(
                                "Partition restore failed during recover-all for {}/{}#{}: {}",
                                fragment, object_key, meta.version_id, e
                            );
                            continue;
                        }
                        restored.push(meta.version_id);
                    }
                    restored
                }
                Err(e) => {
                    warn!(
                        "Fast recover-all restore failed for {}/{}: {}",
                        fragment, object_key, e
                    );
                    continue;
                }
            };

        if restored_versions.is_empty() {
            continue;
        }

        move_blob_versions_between_states(
            &path_manager,
            &blob_roots,
            &fragment,
            &restored_versions,
            true,
        )
        .await;
        let live_after =
            match index::list_live_object_versions_metadata(&fragment, &object_key).await {
                Ok(metas) => metas,
                Err(_) => continue,
            };
        let trash_after =
            match index::list_trashed_object_versions_metadata(&fragment, &object_key).await {
                Ok(metas) => metas,
                Err(_) => continue,
            };
        let ts = logical_now();
        if let Some(latest_live) = live_after.first() {
            let live_entry = synthetic_live_entry(latest_live, ts.saturating_add(1));
            let _ = write_prefix_entry_update(
                &fragment,
                &object_key,
                &latest_live.version_id,
                live_entry,
                if had_live_before { 0 } else { 1 },
                false,
            )
            .await;
            let trash_tombstone = synthetic_delete_marker(latest_live, ts);
            let trash_child_delta = if had_trash_before && trash_after.is_empty() {
                -1
            } else {
                0
            };
            let _ = write_prefix_entry_update(
                &fragment,
                &object_key,
                &latest_live.version_id,
                trash_tombstone,
                trash_child_delta,
                true,
            )
            .await;
        }
        if let Some(latest_trash) = trash_after.first() {
            let trash_entry = synthetic_live_entry(latest_trash, ts.saturating_add(2));
            let _ = write_prefix_entry_update(
                &fragment,
                &object_key,
                &latest_trash.version_id,
                trash_entry,
                0,
                true,
            )
            .await;
        }
        recovered_count = recovered_count.saturating_add(restored_versions.len());
    }

    info!(
        "Recovered {} trashed item(s) for fragment {}",
        recovered_count, fragment
    );
    state.emit(&fragment, StorageEventPayload::StateInvalidated);

    Ok(StatusCode::OK)
}
