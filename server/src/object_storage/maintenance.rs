use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::OBJECT_DATA_ROOT;
use crate::object_storage::quota_manager::set_storage_used;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{ObjectMetadata, ObjectPathManager};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use std::collections::{HashMap, HashSet};
use std::path::{Path as StdPath, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{error, info};
use walkdir::WalkDir;

const TRASH_DIR: &str = ".trash";

fn parse_version_from_meta_filename(name: &str) -> Option<&str> {
    if !name.ends_with(".bson") {
        return None;
    }
    let (_, ver_ext) = name.split_once('#')?;
    let ver = ver_ext.strip_suffix(".bson")?;
    if ver.is_empty() {
        return None;
    }
    Some(ver)
}

#[cfg(unix)]
fn sync_dir_std(path: &StdPath) {
    use std::fs::OpenOptions;
    use std::os::unix::fs::OpenOptionsExt;
    if let Ok(f) = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY)
        .open(path)
    {
        let _ = f.sync_all();
    }
}

#[cfg(windows)]
fn sync_dir_std(path: &StdPath) {
    use std::fs::OpenOptions;
    use std::os::windows::fs::OpenOptionsExt;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x02000000;
    if let Ok(f) = OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
    {
        let _ = f.sync_all();
    }
}

#[cfg(not(any(unix, windows)))]
fn sync_dir_std(_path: &StdPath) {}

fn remove_dir_strict(dir: &StdPath) -> bool {
    if !dir.exists() {
        return true;
    }
    let parent = match dir.parent() {
        Some(p) => p,
        None => return false,
    };
    let gc_dir = PathBuf::from(format!("{}.gcdir", dir.to_string_lossy()));
    if !gc_dir.exists() {
        if std::fs::rename(dir, &gc_dir).is_err() {
            return false;
        }
        sync_dir_std(parent);
    }
    let _ = std::fs::remove_dir_all(&gc_dir);
    sync_dir_std(parent);
    !dir.exists()
}

fn scan_index_dir(
    index_dir: &StdPath,
    best_meta: &mut HashMap<String, (SystemTime, PathBuf)>,
    valid_versions: &mut HashSet<String>,
) {
    if !index_dir.exists() {
        return;
    }

    for entry in WalkDir::new(index_dir).into_iter().filter_map(|e| e.ok()) {
        if !entry.file_type().is_file() {
            continue;
        }

        let filename = entry.file_name().to_string_lossy().to_string();
        if filename == "quota.bson"
            || filename == "reservation.bson"
            || filename.ends_with(".partmeta")
        {
            continue;
        }

        let Some(ver) = parse_version_from_meta_filename(&filename) else {
            continue;
        };

        let modified = entry
            .metadata()
            .ok()
            .and_then(|m| m.modified().ok())
            .unwrap_or(SystemTime::UNIX_EPOCH);

        valid_versions.insert(ver.to_string());

        match best_meta.get(ver) {
            None => {
                best_meta.insert(ver.to_string(), (modified, entry.path().to_path_buf()));
            }
            Some((cur_m, _)) => {
                if modified > *cur_m {
                    best_meta.insert(ver.to_string(), (modified, entry.path().to_path_buf()));
                }
            }
        }
    }
}

async fn perform_quota_sync(uid: &str) -> Result<u64, AppError> {
    let path_manager = ObjectPathManager::new();
    let user_index_dirs = path_manager.get_all_user_index_dirs(uid);
    let blob_roots = path_manager.get_blob_roots();
    let uid_owned = uid.to_string();

    let (total_usage, orphans_removed) =
        tokio::task::spawn_blocking(move || -> Result<(u64, i32), AppError> {
            let mut best_meta: HashMap<String, (SystemTime, PathBuf)> = HashMap::new();
            let mut valid_versions: HashSet<String> = HashSet::new();

            let reachable_index_dirs: Vec<PathBuf> = user_index_dirs
                .iter()
                .cloned()
                .filter(|d| d.exists())
                .collect();

            if reachable_index_dirs.is_empty() {
                return Err(AppError::NotFound);
            }

            for user_dir in &reachable_index_dirs {
                if let Ok(fragments) = std::fs::read_dir(user_dir) {
                    for frag_entry in fragments.filter_map(|e| e.ok()) {
                        let frag_path = frag_entry.path();
                        if !frag_path.is_dir() {
                            continue;
                        }

                        let active_index = frag_path.join("index");
                        scan_index_dir(&active_index, &mut best_meta, &mut valid_versions);

                        let trash_index = frag_path.join(TRASH_DIR).join("index");
                        scan_index_dir(&trash_index, &mut best_meta, &mut valid_versions);
                    }
                }
            }

            let mut total_usage = 0u64;

            for (_ver, (_mtime, path)) in best_meta {
                if let Ok(data) = std::fs::read(&path) {
                    if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                        if !meta.is_delete_marker {
                            total_usage = total_usage.saturating_add(meta.total_size_bytes);
                        }
                    }
                }
            }

            let mut orphans_count = 0;

            let mut cleanup_roots: Vec<PathBuf> = Vec::new();
            cleanup_roots.extend(reachable_index_dirs.into_iter());

            for root in blob_roots {
                cleanup_roots.push(root.join(OBJECT_DATA_ROOT).join(&uid_owned));
            }

            for user_root in cleanup_roots {
                if !user_root.exists() {
                    continue;
                }

                if let Ok(fragments) = std::fs::read_dir(&user_root) {
                    for frag_entry in fragments.filter_map(|e| e.ok()) {
                        if !frag_entry.path().is_dir() {
                            continue;
                        }

                        let blobs_dir = frag_entry.path().join("blobs");
                        if !blobs_dir.exists() {
                            continue;
                        }

                        if let Ok(blob_entries) = std::fs::read_dir(&blobs_dir) {
                            for blob_entry in blob_entries.filter_map(|e| e.ok()) {
                                let path = blob_entry.path();
                                if !path.is_dir() {
                                    continue;
                                }

                                let version_id =
                                    blob_entry.file_name().to_string_lossy().to_string();

                                if valid_versions.contains(&version_id) {
                                    continue;
                                }

                                let receipt = path.join("reservation.bson");
                                if !receipt.exists() {
                                    continue;
                                }

                                if remove_dir_strict(&path) {
                                    orphans_count += 1;
                                }
                            }
                        }
                    }
                }
            }

            Ok((total_usage, orphans_count))
        })
        .await
        .map_err(|e| AppError::InternalServerError(format!("Sync task failed: {}", e)))??;

    if orphans_removed > 0 {
        info!(
            "Sync: Cleaned up {} orphaned upload sessions/shards for user {}",
            orphans_removed, uid
        );
    }

    if let Err(e) = set_storage_used(uid, total_usage).await {
        error!("Failed to update quota for {}: {}", uid, e);
    }

    Ok(total_usage)
}

pub async fn sync_user_metadata(
    State(_state): State<Arc<AppState>>,
    claims: Claims,
) -> Result<impl IntoResponse, AppError> {
    let usage = perform_quota_sync(&claims.uid).await?;
    info!(
        "Self-sync complete for user {}. New Usage: {} bytes",
        claims.uid, usage
    );

    Ok((
        StatusCode::OK,
        "Metadata synchronization and cleanup complete.",
    ))
}

#[allow(dead_code)]
pub async fn admin_sync_user_metadata(
    State(_state): State<Arc<AppState>>,
    _claims: Claims,
    Path(target_uid): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let usage = perform_quota_sync(&target_uid).await?;
    info!(
        "Admin-sync complete for user {}. New Usage: {} bytes",
        target_uid, usage
    );

    Ok((
        StatusCode::OK,
        format!("Metadata synced for user {}.", target_uid),
    ))
}
