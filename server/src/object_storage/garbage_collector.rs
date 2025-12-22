use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::{OBJECT_DATA_ROOT, OBJECT_META_FILE};
use crate::settings::config::CONFIG;
use crate::structs::object_storage_structs::{OBJECT_STORAGE_ROOT, ObjectMetadata};
use mongodb::Client as MongoClient;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::time::sleep;
use tracing::{error, info};
use walkdir::WalkDir;

pub async fn run_lifecycle_manager(_db_client: Arc<MongoClient>) {
    let cleanup_interval = Duration::from_secs(CONFIG.garbage_collector.cleanup_interval_seconds);
    let file_ttl = Duration::from_secs(CONFIG.garbage_collector.file_ttl_seconds);

    loop {
        info!("⏳ Lifecycle Manager: Starting scan...");
        if let Err(e) = perform_lifecycle_scan().await {
            error!("Lifecycle Scan Failed: {}", e);
        }

        if let Err(e) = perform_filesystem_cleanup(file_ttl).await {
            error!("Filesystem Cleanup Failed: {}", e);
        }

        sleep(cleanup_interval).await;
    }
}

async fn perform_lifecycle_scan() -> Result<(), AppError> {
    let root_str = OBJECT_STORAGE_ROOT
        .get()
        .ok_or(AppError::InternalServerError("No root".into()))?;
    let storage_root = PathBuf::from(root_str).join(OBJECT_DATA_ROOT);

    tokio::task::spawn_blocking(move || -> Result<(), AppError> {
        if !storage_root.exists() {
            return Ok(());
        }

        for entry in WalkDir::new(&storage_root)
            .min_depth(3)
            .max_depth(3)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_dir() {
                if let Err(e) = process_object_lifecycle(entry.path()) {
                    error!("Error processing lifecycle for {:?}: {}", entry.path(), e);
                }
            }
        }
        Ok(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(e.to_string()))??;

    Ok(())
}

fn process_object_lifecycle(object_dir: &std::path::Path) -> Result<(), AppError> {
    let mut versions = Vec::new();
    let read_dir =
        std::fs::read_dir(object_dir).map_err(|e| AppError::Io(e, "read obj dir".into()))?;

    for entry in read_dir.flatten() {
        if entry.path().is_dir() {
            if let Some(name) = entry.file_name().to_str() {
                versions.push(name.to_string());
            }
        }
    }

    versions.sort();

    if versions.is_empty() {
        let _ = std::fs::remove_dir(object_dir);
        return Ok(());
    }

    let latest_version_id = versions.last().unwrap().clone();
    let retention_duration = Duration::from_secs(CONFIG.garbage_collector.retention_days * 86400);

    for version_id in &versions {
        let is_latest = version_id == &latest_version_id;
        let version_path = object_dir.join(version_id);
        let meta_path = version_path.join(OBJECT_META_FILE);

        if let Ok(metadata) = std::fs::metadata(&version_path) {
            if let Ok(modified) = metadata.modified() {
                if let Ok(age) = SystemTime::now().duration_since(modified) {
                    if age < retention_duration {
                        continue;
                    }
                }
            }
        }

        if let Ok(data) = std::fs::read(&meta_path) {
            if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                if !is_latest {
                    info!(
                        "♻️ Lifecycle: Deleting old version {}/{}",
                        meta.object_key, version_id
                    );
                    let _ = std::fs::remove_dir_all(&version_path);
                    continue;
                }

                if is_latest && meta.is_delete_marker {
                    info!(
                        "♻️ Lifecycle: Cleaning up expired delete marker {}/{}",
                        meta.object_key, version_id
                    );
                    let _ = std::fs::remove_dir_all(&version_path);
                }
            }
        }
    }

    Ok(())
}

async fn perform_filesystem_cleanup(ttl: Duration) -> Result<(), AppError> {
    let root_str = OBJECT_STORAGE_ROOT
        .get()
        .ok_or_else(|| AppError::InternalServerError("Storage root not initialized".to_string()))?;

    let storage_root = PathBuf::from(root_str).join(OBJECT_DATA_ROOT);

    tokio::task::spawn_blocking(move || -> Result<(), AppError> {
        if !storage_root.exists() {
            return Ok(());
        }

        for entry in WalkDir::new(&storage_root)
            .contents_first(true)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let path = entry.path();

            if path == storage_root {
                continue;
            }
            if !path.is_dir() {
                continue;
            }

            let mut has_subdirs = false;
            let mut has_files = false;
            let mut has_meta = false;

            if let Ok(read_dir) = fs::read_dir(path) {
                for child in read_dir.flatten() {
                    if let Ok(ft) = child.file_type() {
                        if ft.is_dir() {
                            has_subdirs = true;
                            break;
                        } else if ft.is_file() {
                            has_files = true;
                            if child.file_name() == OBJECT_META_FILE {
                                has_meta = true;
                            }
                        }
                    }
                }
            }

            if has_subdirs {
                continue;
            }

            if has_meta {
                continue;
            }

            if has_files {
                let metadata = fs::metadata(path).ok();
                let modified = metadata
                    .and_then(|m| m.modified().ok())
                    .unwrap_or(SystemTime::now());
                let age = SystemTime::now()
                    .duration_since(modified)
                    .unwrap_or(Duration::ZERO);

                if age > ttl {
                    info!(
                        "🗑️ Found stale incomplete upload: {:?} (Age: {:?}). Deleting...",
                        path, age
                    );
                    let _ = fs::remove_dir_all(path);
                }
                continue;
            }

            let _ = fs::remove_dir(path);
        }

        Ok(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("GC Task Error: {}", e)))??;

    Ok(())
}

pub async fn run_garbage_collector(_db_client: Arc<MongoClient>) {
    let file_ttl = Duration::from_secs(60);
    info!("🗑️ GC (One-Time) executing. TTL: {:?}", file_ttl);

    if let Err(e) = perform_filesystem_cleanup(file_ttl).await {
        error!("🗑️ GC One-Time run failed: {}", e);
    } else {
        info!("🗑️ GC (One-Time) completed successfully.");
    }
}
