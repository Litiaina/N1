use crate::core::disk_manager::ObjectPathManager;
use crate::core::index;
use crate::enums::app_error_enum::AppError;
use crate::structs::auth_structs::Claims;
use axum::{http::StatusCode, response::IntoResponse};
use std::collections::HashSet;
use tracing::{error, info};

async fn perform_fragment_cleanup(fragment: &str) -> Result<(u64, i32), AppError> {
    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    let fragment_owned = fragment.to_string();
    let live_objects = index::list_fragment_live_objects(fragment).await?;
    let total_usage = live_objects
        .iter()
        .filter(|meta| !meta.is_delete_marker)
        .map(|meta| meta.total_size_bytes)
        .sum::<u64>();
    let valid_versions = live_objects
        .into_iter()
        .map(|meta| meta.version_id)
        .collect::<HashSet<_>>();

    let orphans_removed = tokio::task::spawn_blocking(move || -> Result<i32, AppError> {
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

        Ok(orphans_count)
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("Sync task failed: {}", e)))??;

    if orphans_removed > 0 {
        info!(
            "Sync: Cleaned up {} orphaned upload sessions/shards for fragment {}",
            orphans_removed, fragment
        );
    }

    Ok((total_usage, orphans_removed))
}

pub async fn sync_fragment_metadata(claims: Claims) -> Result<impl IntoResponse, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden(
            "Modify permissions required to clean fragment metadata".into(),
        ));
    }

    let (usage, orphans_removed) = perform_fragment_cleanup(&claims.fragment).await?;
    info!(
        "Fragment cleanup complete for {}. Usage: {} bytes, orphaned blobs removed: {}",
        claims.fragment, usage, orphans_removed
    );

    Ok((StatusCode::OK, "Fragment metadata cleanup complete."))
}
