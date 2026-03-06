use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use axum::extract::State;
use axum::{Json as AxumJson, http::StatusCode, response::IntoResponse};
use chrono::Utc;
use tokio::fs;
use tracing::{info, warn};
use ulid::Ulid;

use crate::enums::app_error_enum::AppError;
use crate::n1_core::disk_manager::{
    ObjectMetadata, ObjectPathManager, RenameObjectRequest, compare_version_recency,
};
use crate::n1_core::index;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::ws_structs::StorageEventPayload;

#[cfg(unix)]
pub async fn sync_dir(path: &Path) -> Result<(), AppError> {
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
pub async fn sync_dir(path: &Path) -> Result<(), AppError> {
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
pub async fn sync_dir(_path: &Path) -> Result<(), AppError> {
    Ok(())
}

async fn write_delete_marker_for_key(
    _path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
    template: &ObjectMetadata,
) -> Result<ObjectMetadata, AppError> {
    let now = Utc::now().timestamp();
    let delete_version_id = Ulid::new().to_string();

    let mut dm = template.clone();
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

async fn delete_source_metadata_versions(
    _path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
    versions: &HashSet<String>,
) {
    for v in versions {
        let _ = index::delete_object_version(fragment, object_key, v).await;
    }
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
    let source_versions =
        index::list_live_object_versions_metadata(&payload.source_fragment, &payload.source_key)
            .await?;
    if source_versions.is_empty() {
        return Err(AppError::NotFound);
    }

    if !index::list_live_object_versions_metadata(
        &payload.destination_fragment,
        &payload.destination_key,
    )
    .await?
    .is_empty()
    {
        return Err(AppError::Conflict(
            "Destination object already exists".into(),
        ));
    }

    let mut renamed_versions = source_versions;
    renamed_versions.sort_by(|left, right| {
        compare_version_recency(
            left.created_at,
            &left.version_id,
            right.created_at,
            &right.version_id,
        )
    });

    let mut affected_versions = HashSet::<String>::new();
    let template_meta = renamed_versions.first().cloned();

    for (idx, source_meta) in renamed_versions.iter().enumerate() {
        let mut renamed = source_meta.clone();
        renamed.object_key = payload.destination_key.clone();
        renamed.fragment = payload.destination_fragment.clone();
        renamed.owner_uid = payload.destination_fragment.clone();
        renamed.created_at = Utc::now().timestamp() + idx as i64;

        index::upsert_object_version(&renamed).await?;
        affected_versions.insert(renamed.version_id.clone());
    }

    if payload.source_fragment != payload.destination_fragment {
        let blob_roots = path_manager.get_blob_roots();
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

    if let Some(tmpl) = template_meta.as_ref() {
        let _del_meta = write_delete_marker_for_key(
            &path_manager,
            &payload.source_fragment,
            &payload.source_key,
            tmpl,
        )
        .await?;
    }

    delete_source_metadata_versions(
        &path_manager,
        &payload.source_fragment,
        &payload.source_key,
        &affected_versions,
    )
    .await;

    info!(
        "Renamed object from {}/{} to {}/{}",
        payload.source_fragment,
        payload.source_key,
        payload.destination_fragment,
        payload.destination_key
    );

    state.emit(
        &payload.source_fragment,
        StorageEventPayload::FileRenamed {
            source_fragment: payload.source_fragment.clone(),
            source_key: payload.source_key.clone(),
            destination_fragment: payload.destination_fragment.clone(),
            destination_key: payload.destination_key.clone(),
        },
    );

    Ok(StatusCode::OK)
}
