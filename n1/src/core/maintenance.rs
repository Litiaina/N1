use crate::core::disk_manager::ObjectPathManager;
use crate::core::index;
use crate::core::index_store;
use crate::enums::app_error_enum::AppError;
use crate::settings::config::CONFIG;
use crate::structs::auth_structs::Claims;
use axum::{Json, http::StatusCode, response::IntoResponse};
use serde::Serialize;
use std::collections::HashSet;
use std::time::{Duration, SystemTime};
use tracing::{error, info};

async fn perform_fragment_cleanup(fragment: &str) -> Result<(u64, i32), AppError> {
    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    let fragment_owned = fragment.to_string();
    let now = SystemTime::now();
    let file_ttl = Duration::from_secs(CONFIG.garbage_collector.file_ttl_seconds);
    let live_objects = index::list_fragment_live_objects(fragment).await?;
    let active_sessions = index::list_active_sessions(fragment).await?;
    let total_usage = live_objects
        .iter()
        .filter(|meta| !meta.is_delete_marker)
        .map(|meta| meta.total_size_bytes)
        .sum::<u64>();
    let referenced_object_ids = index_store::referenced_object_ids(fragment)?;
    let active_session_versions = active_sessions
        .into_iter()
        .filter(|session| session.finalized_at.is_none() && session.aborted_at.is_none())
        .map(|session| session.manifest.version_id)
        .collect::<HashSet<_>>();

    let orphans_removed = tokio::task::spawn_blocking(move || -> Result<i32, AppError> {
        let mut orphans_count = 0;

        for root in blob_roots {
            let Ok(object_dirs) = index_store::local_object_dirs_under_root(&root, &fragment_owned)
            else {
                continue;
            };
            for (object_dir, object_id) in object_dirs {
                if referenced_object_ids.contains(&object_id) {
                    continue;
                }
                let has_active_session = std::fs::read_dir(&object_dir)
                    .ok()
                    .into_iter()
                    .flatten()
                    .filter_map(Result::ok)
                    .any(|entry| {
                        entry.path().is_dir()
                            && active_session_versions
                                .contains(&entry.file_name().to_string_lossy().to_string())
                    });
                if has_active_session {
                    continue;
                }
                let modified = std::fs::metadata(&object_dir)
                    .and_then(|metadata| metadata.modified())
                    .unwrap_or(now);
                if now.duration_since(modified).unwrap_or(Duration::ZERO) <= file_ttl {
                    continue;
                }
                if let Err(err) = std::fs::remove_dir_all(&object_dir) {
                    error!(
                        "Sync: Failed to remove orphaned object {:?}: {}",
                        object_dir, err
                    );
                } else {
                    orphans_count += 1;
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

#[derive(Serialize)]
struct FragmentCleanupResponse {
    fragment: String,
    usage_bytes: u64,
    orphan_blob_dirs_removed: i32,
    message: String,
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

    Ok((
        StatusCode::OK,
        Json(FragmentCleanupResponse {
            fragment: claims.fragment,
            usage_bytes: usage,
            orphan_blob_dirs_removed: orphans_removed,
            message: "Fragment metadata cleanup complete.".to_string(),
        }),
    ))
}
