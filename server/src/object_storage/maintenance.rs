use crate::enums::app_error_enum::AppError;
use crate::object_storage::quota_manager::set_storage_used;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{ObjectMetadata, ObjectPathManager};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{error, info};
use walkdir::WalkDir;

async fn perform_quota_sync(uid: &str) -> Result<u64, AppError> {
    let path_manager = ObjectPathManager::new();
    let user_dir = path_manager.get_user_dir(uid);

    if !user_dir.exists() {
        return Err(AppError::NotFound);
    }

    let uid_owned = Arc::new(uid.to_string());
    let user_dir_clone = user_dir.clone();
    let uid_for_task = uid_owned.clone();

    let calculated_usage = tokio::task::spawn_blocking(move || {
        let mut total_usage = 0u64;
        let mut orphans_removed = 0;

        if let Ok(fragments) = std::fs::read_dir(&user_dir_clone) {
            for frag_entry in fragments.filter_map(|e| e.ok()) {
                if !frag_entry.path().is_dir() {
                    continue;
                }

                let index_dir = frag_entry.path().join("index");
                let blobs_dir = frag_entry.path().join("blobs");
                let mut valid_versions = HashSet::new();

                if index_dir.exists() {
                    for entry in WalkDir::new(&index_dir).into_iter().filter_map(|e| e.ok()) {
                        if entry.file_type().is_file() {
                            let filename = entry.file_name().to_string_lossy();
                            if let Some((_, ver_ext)) = filename.split_once('#') {
                                if let Some(ver) = ver_ext.strip_suffix(".bson") {
                                    valid_versions.insert(ver.to_string());
                                    
                                    if let Ok(data) = std::fs::read(entry.path()) {
                                        if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                                            if !meta.is_delete_marker {
                                                total_usage += meta.total_size_bytes;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                if blobs_dir.exists() {
                    if let Ok(blob_entries) = std::fs::read_dir(&blobs_dir) {
                        for blob_entry in blob_entries.filter_map(|e| e.ok()) {
                            let path = blob_entry.path();
                            if !path.is_dir() { continue; }
                            
                            let version_id = blob_entry.file_name().to_string_lossy().to_string();

                            if !valid_versions.contains(&version_id) {
                                if let Err(e) = std::fs::remove_dir_all(&path) {
                                    error!("Sync: Failed to remove orphaned blob {}/{}: {}", uid_for_task, version_id, e);
                                } else {
                                    orphans_removed += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
        
        if orphans_removed > 0 {
            info!("Sync: Cleaned up {} orphaned upload sessions for user {}", orphans_removed, uid_for_task);
        }
        
        total_usage
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("Sync task failed: {}", e)))?;

    if let Err(e) = set_storage_used(&uid_owned, calculated_usage).await {
        error!("Failed to sync quota file for {}: {}", uid_owned, e);
        return Err(AppError::InternalServerError(
            "Failed to update quota file.".into(),
        ));
    }

    Ok(calculated_usage)
}

pub async fn sync_user_metadata(
    State(_state): State<Arc<AppState>>,
    claims: Claims,
) -> Result<impl IntoResponse, AppError> {
    let usage = perform_quota_sync(&claims.uid).await?;
    info!("Self-sync complete for user {}. New Usage: {} bytes", claims.uid, usage);
    
    Ok((StatusCode::OK, "Metadata synchronization and cleanup complete."))
}

#[allow(dead_code)]
pub async fn admin_sync_user_metadata(
    State(_state): State<Arc<AppState>>,
    _claims: Claims,
    Path(target_uid): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let usage = perform_quota_sync(&target_uid).await?;
    info!("Admin-sync complete for user {}. New Usage: {} bytes", target_uid, usage);
    
    Ok((StatusCode::OK, format!("Metadata synced for user {}.", target_uid)))
}
