use crate::enums::app_error_enum::AppError;
use crate::core::disk_manager::ObjectPathManager;
use crate::core::index_facade as index;
use crate::core::quota_manager::set_storage_used;
use crate::structs::auth_structs::Claims;
use axum::{http::StatusCode, response::IntoResponse};
use std::collections::HashSet;
use tracing::{error, info};

async fn perform_quota_sync(fragment: &str) -> Result<u64, AppError> {
    let path_manager = ObjectPathManager::new();

    let blob_roots = path_manager.get_blob_roots();
    let fragment_owned = fragment.to_string();
    let mut valid_versions = HashSet::new();
    let mut total_usage = 0u64;

    for trashed in [false, true] {
        let mut stream = index::stream_fragment_object_versions(fragment.to_string(), trashed);
        while let Some(next_meta) = stream.recv().await {
            let meta = next_meta?;
            if meta.is_delete_marker || !valid_versions.insert(meta.version_id.clone()) {
                continue;
            }
            total_usage += meta.total_size_bytes;
        }
    }

    let (_valid_versions, orphans_removed) =
        tokio::task::spawn_blocking(move || -> Result<(HashSet<String>, i32), AppError> {
            let mut orphans_count = 0;

            for root in blob_roots {
                let blob_root = root.join(&fragment_owned).join("blob");

                if !blob_root.exists() {
                    continue;
                }

                if let Ok(entries) = std::fs::read_dir(&blob_root) {
                    for entry in entries.filter_map(|e| e.ok()) {
                        let path = entry.path();
                        if !path.is_dir() {
                            continue;
                        }

                        let version_id = entry.file_name().to_string_lossy().to_string();

                        if !valid_versions.contains(&version_id) {
                            if let Err(e) = std::fs::remove_dir_all(&path) {
                                error!("Sync: Failed to remove orphaned blob {:?}: {}", path, e);
                            } else {
                                orphans_count += 1;
                            }
                        }
                    }
                }
            }

            Ok((valid_versions, orphans_count))
        })
        .await
        .map_err(|e| AppError::InternalServerError(format!("Sync task failed: {}", e)))??;

    if orphans_removed > 0 {
        info!(
            "Sync: Cleaned up {} orphaned upload sessions/shards for fragment {}",
            orphans_removed, fragment
        );
    }

    if let Err(e) = set_storage_used(fragment, total_usage).await {
        error!("Failed to update quota for {}: {}", fragment, e);
    }

    Ok(total_usage)
}

pub async fn sync_fragment_metadata(claims: Claims) -> Result<impl IntoResponse, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden(
            "Modify permissions required to sync quota".into(),
        ));
    }

    let usage = perform_quota_sync(&claims.fragment).await?;
    info!(
        "Self-sync complete for fragment {}. New Usage: {} bytes",
        claims.fragment, usage
    );

    Ok((
        StatusCode::OK,
        "Metadata synchronization and cleanup complete.",
    ))
}
