use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::{OBJECT_DATA_ROOT, OBJECT_META_FILE};
use crate::settings::config::CONFIG;
use crate::structs::object_storage_structs::OBJECT_STORAGE_ROOT;
use mongodb::Client as MongoClient;
use walkdir::WalkDir;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::fs;
use tokio::time::sleep;
use tracing::{error, info};

pub async fn run_garbage_collector(_db_client: Arc<MongoClient>) {
    let file_ttl = Duration::from_secs(60);

    info!("🗑️ GC (One-Time) executing. TTL: {:?}", file_ttl);

    if let Err(e) = perform_filesystem_cleanup(file_ttl).await {
        error!("🗑️ GC One-Time run failed: {}", e);
    } else {
        info!("🗑️ GC (One-Time) completed successfully.");
    }
}

pub async fn interval_garbage_collector(_db_client: Arc<MongoClient>) {
    let cleanup_interval = Duration::from_secs(CONFIG.garbage_collector.cleanup_interval_seconds);
    let file_ttl = Duration::from_secs(CONFIG.garbage_collector.file_ttl_seconds);

    info!(
        "🗑️ GC (Interval) started. Interval: {:?}, TTL: {:?}",
        cleanup_interval, file_ttl
    );

    loop {
        sleep(cleanup_interval).await;
        if let Err(e) = perform_filesystem_cleanup(file_ttl).await {
            error!("🗑️ GC Cycle failed: {}", e);
        }
    }
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

        for entry in WalkDir::new(&storage_root).contents_first(true).into_iter().filter_map(|e| e.ok()) {
            let path = entry.path();
            
            if path == storage_root { continue; }
            if !path.is_dir() { continue; }

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
                let modified = metadata.and_then(|m| m.modified().ok()).unwrap_or(SystemTime::now());
                let age = SystemTime::now().duration_since(modified).unwrap_or(Duration::ZERO);

                if age > ttl {
                    info!("🗑️ Found stale incomplete upload: {:?} (Age: {:?}). Deleting...", path, age);
                    let _ = fs::remove_dir_all(path);
                }
                continue;
            }

            info!("🗑️ Removing empty directory: {:?}", path);
            let _ = fs::remove_dir(path);
        }

        Ok(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("GC Task Error: {}", e)))??;

    Ok(())
}
