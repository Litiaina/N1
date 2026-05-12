use std::collections::HashSet;
use std::sync::Arc;

use axum::extract::State;
use axum::{Json as AxumJson, http::StatusCode, response::IntoResponse};
use chrono::Utc;
use tokio::fs;
use tracing::{info, warn};

use crate::core::constants::EDGE_BLOB_DIR;
use crate::core::disk_manager::{ObjectPathManager, RenameObjectRequest};
use crate::core::index;
use crate::enums::app_error_enum::AppError;
use crate::namespace::models::NamespaceEntryKind;
use crate::namespace::runtime::global_namespace_runtime;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::ws_structs::{NamespaceChangeEvent, StorageEventPayload};

#[cfg(unix)]
pub async fn sync_dir(path: &std::path::Path) -> Result<(), AppError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        use std::fs::OpenOptions;
        use std::os::unix::fs::OpenOptionsExt;

        let f = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY)
            .open(&path)
            .map_err(|e| AppError::Io(e, format!("open dir for sync: {:?}", path)))?;

        f.sync_all()
            .map_err(|e| AppError::Io(e, format!("sync dir: {:?}", path)))?;

        Ok::<(), AppError>(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("sync dir task: {}", e)))?
}

#[cfg(windows)]
pub async fn sync_dir(path: &std::path::Path) -> Result<(), AppError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        use std::fs::OpenOptions;
        use std::os::windows::fs::OpenOptionsExt;

        const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x02000000;

        let result = OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(&path);

        match result {
            Ok(file) => {
                let _ = file.sync_all();
            }
            Err(e) => {
                warn!(
                    "Windows: Failed to open directory for sync (non-fatal): {} - {}",
                    path.display(),
                    e
                );
            }
        }

        Ok::<(), AppError>(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("sync dir task: {}", e)))?
}

#[cfg(not(any(unix, windows)))]
pub async fn sync_dir(_path: &std::path::Path) -> Result<(), AppError> {
    Ok(())
}

fn remap_shard_absolute_path_for_fragment_move(
    path_manager: &ObjectPathManager,
    blob_roots: &[std::path::PathBuf],
    source_fragment: &str,
    destination_fragment: &str,
    version_id: &str,
    current_path: &str,
    file_name: &str,
) -> String {
    for root in blob_roots {
        let src_dir = path_manager.get_shard_dir_on_disk(root, source_fragment, version_id);
        let src_dir_s = src_dir.to_string_lossy();
        if current_path.starts_with(src_dir_s.as_ref()) {
            return path_manager
                .get_shard_dir_on_disk(root, destination_fragment, version_id)
                .join(file_name)
                .to_string_lossy()
                .to_string();
        }
    }

    let unix_from = format!("/{}/{}/{}/", source_fragment, EDGE_BLOB_DIR, version_id);
    let unix_to = format!(
        "/{}/{}/{}/",
        destination_fragment, EDGE_BLOB_DIR, version_id
    );
    if current_path.contains(&unix_from) {
        return current_path.replacen(&unix_from, &unix_to, 1);
    }

    let win_from = format!("\\{}\\{}\\{}\\", source_fragment, EDGE_BLOB_DIR, version_id);
    let win_to = format!(
        "\\{}\\{}\\{}\\",
        destination_fragment, EDGE_BLOB_DIR, version_id
    );
    if current_path.contains(&win_from) {
        return current_path.replacen(&win_from, &win_to, 1);
    }

    current_path.to_string()
}

async fn acquire_rename_locks(
    source_fragment: &str,
    source_key: &str,
    destination_fragment: &str,
    destination_key: &str,
) -> Result<Vec<index::ObjectOperationGuard>, AppError> {
    let mut lock_targets = vec![
        (
            source_fragment.to_string(),
            ObjectPathManager::canonical_object_key(source_key),
        ),
        (
            destination_fragment.to_string(),
            ObjectPathManager::canonical_object_key(destination_key),
        ),
    ];
    lock_targets.sort();
    lock_targets.dedup();

    let mut guards = Vec::with_capacity(lock_targets.len());
    for (fragment, object_key) in lock_targets {
        guards.push(index::acquire_object_operation_lock(&fragment, &object_key).await?);
    }
    Ok(guards)
}

pub async fn rename_object(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumJson(payload): AxumJson<RenameObjectRequest>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden("Modify permission required".into()));
    }

    if claims.fragment != payload.source_fragment {
        return Err(AppError::Forbidden(
            "Access denied to source fragment".into(),
        ));
    }

    if payload.source_fragment != payload.destination_fragment {
        if claims.fragment != payload.destination_fragment {
            return Err(AppError::Forbidden(
                "Access denied to destination fragment".into(),
            ));
        }
    }

    let path_manager = ObjectPathManager::new();
    if payload.source_fragment == payload.destination_fragment
        && payload.source_key == payload.destination_key
    {
        return Ok(StatusCode::OK);
    }

    let _rename_locks = acquire_rename_locks(
        &payload.source_fragment,
        &payload.source_key,
        &payload.destination_fragment,
        &payload.destination_key,
    )
    .await?;

    if payload.source_fragment == payload.destination_fragment {
        let summary = global_namespace_runtime()
            .rename(
                &payload.source_fragment,
                &payload.source_key,
                &payload.destination_key,
            )
            .await?;

        info!(
            "Renamed namespace path from {}/{} to {}/{}",
            payload.source_fragment,
            payload.source_key,
            payload.destination_fragment,
            payload.destination_key
        );

        state.emit(
            &payload.source_fragment,
            StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::from_summary(
                &payload.source_fragment,
                summary,
            )),
        );

        return Ok(StatusCode::OK);
    }

    let destination_versions = index::list_live_object_versions_metadata(
        &payload.destination_fragment,
        &payload.destination_key,
    )
    .await?;
    if destination_versions.iter().any(|m| !m.is_delete_marker) {
        return Err(AppError::Conflict(
            "Destination object already exists".into(),
        ));
    }

    let source_versions =
        index::list_live_object_versions_metadata(&payload.source_fragment, &payload.source_key)
            .await?;
    if source_versions.is_empty() {
        return Err(AppError::NotFound);
    }
    let source_versions_snapshot = source_versions.clone();
    let blob_roots = path_manager.get_blob_roots();
    let mut affected_versions: HashSet<String> = HashSet::new();
    let mut destination_versions_to_write = Vec::new();
    for source_meta in source_versions {
        if source_meta.is_delete_marker {
            continue;
        }
        let mut new_meta = source_meta.clone();
        new_meta.object_key = payload.destination_key.clone();
        new_meta.fragment = payload.destination_fragment.clone();
        new_meta.owner_uid = payload.destination_fragment.clone();
        new_meta.created_at = Utc::now().timestamp();

        if payload.source_fragment != payload.destination_fragment {
            for part in &mut new_meta.parts {
                for shard in &mut part.shards {
                    shard.absolute_path = remap_shard_absolute_path_for_fragment_move(
                        &path_manager,
                        &blob_roots,
                        &payload.source_fragment,
                        &payload.destination_fragment,
                        &new_meta.version_id,
                        &shard.absolute_path,
                        &shard.file_name,
                    );
                }
            }
        }

        destination_versions_to_write.push(new_meta.clone());
        affected_versions.insert(new_meta.version_id.clone());
    }

    if affected_versions.is_empty() {
        return Err(AppError::NotFound);
    }

    if payload.source_fragment == payload.destination_fragment {
        for version_id in &affected_versions {
            let _ = index::delete_object_version(
                &payload.source_fragment,
                &payload.source_key,
                version_id,
            )
            .await;
        }

        let mut inserted_destination_versions: Vec<String> = Vec::new();
        for new_meta in &destination_versions_to_write {
            if let Err(err) = index::upsert_object_version(new_meta).await {
                for version_id in &inserted_destination_versions {
                    let _ = index::delete_object_version(
                        &payload.destination_fragment,
                        &payload.destination_key,
                        version_id,
                    )
                    .await;
                }
                for source_meta in &source_versions_snapshot {
                    let _ = index::upsert_object_version(source_meta).await;
                }
                return Err(err);
            }
            inserted_destination_versions.push(new_meta.version_id.clone());
        }
    } else {
        for new_meta in &destination_versions_to_write {
            index::upsert_object_version(new_meta).await?;
        }
        for version_id in &affected_versions {
            let _ = index::delete_object_version(
                &payload.source_fragment,
                &payload.source_key,
                version_id,
            )
            .await;
        }

        for root in blob_roots {
            for version_id in &affected_versions {
                let src_blob_dir =
                    path_manager.get_shard_dir_on_disk(&root, &payload.source_fragment, version_id);

                let dst_blob_dir = path_manager.get_shard_dir_on_disk(
                    &root,
                    &payload.destination_fragment,
                    version_id,
                );

                if fs::try_exists(&src_blob_dir).await.unwrap_or(false) {
                    if let Some(parent) = dst_blob_dir.parent() {
                        let _ = fs::create_dir_all(parent).await;
                    }

                    if let Err(e) = fs::rename(&src_blob_dir, &dst_blob_dir).await {
                        warn!(
                            "Failed to rename blob dir {:?} -> {:?}: {}",
                            src_blob_dir, dst_blob_dir, e
                        );
                    } else if let Some(parent) = dst_blob_dir.parent() {
                        let _ = sync_dir(parent).await;
                    }
                }
            }
        }
    }

    info!(
        "Renamed object from {}/{} to {}/{}",
        payload.source_fragment,
        payload.source_key,
        payload.destination_fragment,
        payload.destination_key
    );

    state.emit(
        &payload.source_fragment,
        StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::renamed(
            &payload.source_fragment,
            &payload.source_key,
            &payload.destination_key,
            NamespaceEntryKind::File,
            1,
            affected_versions.len(),
        )),
    );
    if payload.source_fragment != payload.destination_fragment {
        state.emit(
            &payload.destination_fragment,
            StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::renamed(
                &payload.destination_fragment,
                &payload.source_key,
                &payload.destination_key,
                NamespaceEntryKind::File,
                1,
                affected_versions.len(),
            )),
        );
    }

    Ok(StatusCode::OK)
}
