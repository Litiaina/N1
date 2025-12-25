use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::{OBJECT_DATA_ROOT, OBJECT_META_FILE};
use crate::object_storage::quota_manager::update_quota;
use crate::settings::config::CONFIG;
use crate::structs::object_storage_structs::{OBJECT_STORAGE_ROOT, ObjectMetadata, ReservationReceipt};
use tokio::runtime::Handle;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tokio::time::sleep;
use tracing::{error, info, warn};
use walkdir::WalkDir;

pub async fn run_lifecycle_manager() {
    let cleanup_interval = Duration::from_secs(CONFIG.garbage_collector.cleanup_interval_seconds);
    let file_ttl = if CONFIG.garbage_collector.file_ttl_seconds > 0 {
        Duration::from_secs(CONFIG.garbage_collector.file_ttl_seconds)
    } else {
        Duration::from_secs(3600)
    };

    loop {
        info!("⏳ Lifecycle Manager: Starting scan...");
        
        if let Err(e) = perform_lifecycle_scan(file_ttl).await {
            error!("Lifecycle Scan Failed: {}", e);
        }

        if let Err(e) = perform_filesystem_cleanup(file_ttl).await {
            error!("Filesystem Cleanup Failed: {}", e);
        }

        sleep(cleanup_interval).await;
    }
}

pub async fn run_garbage_collector() {
    let file_ttl = Duration::from_secs(60);
    info!("🗑️ GC (One-Time) executing...");

    if let Err(e) = perform_lifecycle_scan(file_ttl).await {
        error!("🗑️ GC (One-Time) Lifecycle Scan failed: {}", e);
    }

    if let Err(e) = perform_filesystem_cleanup(file_ttl).await {
        error!("🗑️ GC (One-Time) Filesystem Cleanup failed: {}", e);
    } else {
        info!("🗑️ GC (One-Time) completed successfully.");
    }
}

fn get_refund_amount(path: &Path) -> u64 {
    let reservation_path = path.join("reservation.bson");
    if let Ok(data) = std::fs::read(&reservation_path) {
        if let Ok(receipt) = mongodb::bson::from_slice::<ReservationReceipt>(&data) {
            return receipt.amount;
        }
    }

    let mut total = 0;
    for entry in WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
        if entry.file_type().is_file() {
            total += entry.metadata().map(|m| m.len()).unwrap_or(0);
        }
    }
    total
}

fn extract_uid(path: &Path, storage_root: &Path) -> Option<String> {
    path.strip_prefix(storage_root)
        .ok()?
        .components()
        .next()?
        .as_os_str()
        .to_str()
        .map(|s| s.to_string())
}

fn refund_quota_async(uid: String, amount: u64) {
    if amount == 0 { return; }
    
    let handle = Handle::current();
    handle.spawn(async move {
        if let Err(e) = update_quota(&uid, -(amount as i64)).await {
            warn!("GC: Failed to refund quota for user {}: {}", uid, e);
        } else {
            info!("GC: Refunded {} bytes to user {}", amount, uid);
        }
    });
}

async fn perform_lifecycle_scan(ttl: Duration) -> Result<(), AppError> {
    let root_str = OBJECT_STORAGE_ROOT
        .get()
        .ok_or(AppError::InternalServerError("No root".into()))?;
    let storage_root = PathBuf::from(root_str).join(OBJECT_DATA_ROOT);

    tokio::task::spawn_blocking(move || -> Result<(), AppError> {
        if !storage_root.exists() {
            return Ok(());
        }

        let mut object_dirs = HashSet::new();

        for entry in WalkDir::new(&storage_root)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_file() && entry.file_name() == OBJECT_META_FILE {
                if let Some(version_dir) = entry.path().parent() {
                    if let Some(object_dir) = version_dir.parent() {
                        object_dirs.insert(object_dir.to_path_buf());
                    }
                }
            }
        }

        for object_dir in object_dirs {
            if let Err(e) = process_object_lifecycle(&object_dir, &storage_root, ttl) {
                error!("Error processing lifecycle for {:?}: {}", object_dir, e);
            }
        }
        
        Ok(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(e.to_string()))??;

    Ok(())
}

fn process_object_lifecycle(object_dir: &Path, storage_root: &Path, ttl: Duration) -> Result<(), AppError> {
    let read_dir = fs::read_dir(object_dir).map_err(|e| AppError::Io(e, "read obj dir".into()))?;
    
    let mut valid_versions = Vec::new();
    let mut incomplete_versions = Vec::new();

    for entry in read_dir.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Some(name) = entry.file_name().to_str() {
                let meta_path = path.join(OBJECT_META_FILE);
                if meta_path.exists() {
                    valid_versions.push(name.to_string());
                } else {
                    incomplete_versions.push(path.clone());
                }
            }
        }
    }

    for folder_path in incomplete_versions {
        if let Ok(metadata) = fs::metadata(&folder_path) {
            let modified = metadata.modified().unwrap_or(SystemTime::now());
            let age = SystemTime::now().duration_since(modified).unwrap_or(Duration::ZERO);
            
            if age > ttl {
                let refund_amount = get_refund_amount(&folder_path);
                
                info!("♻️ Lifecycle: Removing stale incomplete upload at {:?}", folder_path);
                if fs::remove_dir_all(&folder_path).is_ok() && refund_amount > 0 {
                    if let Some(uid) = extract_uid(object_dir, storage_root) {
                        refund_quota_async(uid, refund_amount);
                    }
                }
            }
        }
    }

    valid_versions.sort();

    if valid_versions.is_empty() {
        let _ = fs::remove_dir(object_dir);
        return Ok(());
    }

    let latest_version_id = valid_versions.last().unwrap().clone();
    let retention_duration = Duration::from_secs(CONFIG.garbage_collector.retention_days * 86400);

    for version_id in &valid_versions {
        let is_latest = version_id == &latest_version_id;
        let version_path = object_dir.join(version_id);
        let meta_path = version_path.join(OBJECT_META_FILE);

        if let Ok(metadata) = fs::metadata(&version_path) {
            if let Ok(modified) = metadata.modified() {
                if let Ok(age) = SystemTime::now().duration_since(modified) {
                    if age < retention_duration {
                        continue; 
                    }
                }
            }
        }

        if let Ok(data) = fs::read(&meta_path) {
            if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                if !is_latest {
                    let size = meta.total_size_bytes as i64;
                    info!("♻️ Lifecycle: Deleting old version {}/{}", meta.object_key, version_id);
                    if fs::remove_dir_all(&version_path).is_ok() {
                        if let Some(uid) = extract_uid(object_dir, storage_root) {
                            refund_quota_async(uid, size as u64);
                        }
                    }
                    continue;
                }

                if is_latest && meta.is_delete_marker {
                    info!("♻️ Lifecycle: Cleaning up expired delete marker {}/{}", meta.object_key, version_id);
                    let _ = fs::remove_dir_all(&version_path);
                }
            }
        }
    }

    let _ = fs::remove_dir(object_dir);
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

            if path == storage_root { continue; }
            if !path.is_dir() { continue; }

            if path.parent() == Some(&storage_root) {
                continue;
            }

            if path.join(OBJECT_META_FILE).exists() {
                continue;
            }

            let mut has_subdirs = false;
            let mut has_files = false;

            if let Ok(read_dir) = fs::read_dir(path) {
                for child in read_dir.flatten() {
                    if let Ok(ft) = child.file_type() {
                        if ft.is_dir() {
                            has_subdirs = true;
                            break; 
                        } else {
                            has_files = true;
                        }
                    }
                }
            }

            if has_subdirs { continue; }

            if has_files {
                let metadata = fs::metadata(path).ok();
                let modified = metadata.and_then(|m| m.modified().ok()).unwrap_or(SystemTime::now());
                let age = SystemTime::now().duration_since(modified).unwrap_or(Duration::ZERO);

                if age > ttl {
                    let refund_amount = get_refund_amount(path);
                    
                    info!("🗑️ GC: Found failed upload (no meta): {:?} (Age: {:?}, Size: {}). Deleting...", path, age, refund_amount);
                    
                    if fs::remove_dir_all(path).is_ok() && refund_amount > 0 {
                        if let Some(uid) = extract_uid(path, &storage_root) {
                            refund_quota_async(uid, refund_amount);
                        }
                    }
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
