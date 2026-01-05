use crate::enums::app_error_enum::AppError;
use crate::fs::cleanup_manager::start_cleanup;
use crate::fs::fs_constants::STORAGE_ROOT;
use crate::object_storage::constants::OBJECT_DATA_ROOT;
use crate::object_storage::quota_manager::update_quota;
use crate::routes::storage_routes::AppState;
use crate::settings::config::CONFIG;
use crate::structs::object_storage_structs::OBJECT_STORAGE_ROOT;
use crate::websocket::ws_structs::StorageEventPayload;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use tokio::runtime::Handle;
use std::collections::HashMap;
use std::{collections::HashSet, time::SystemTime};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};
use walkdir::WalkDir;

pub async fn run_lifecycle_manager(state: Arc<AppState>) {
    let cleanup_interval = Duration::from_secs(CONFIG.garbage_collector.cleanup_interval_seconds);
    
    loop {
        info!("GC & Lifecycle: Starting cycle...");
        if let Err(e) = perform_gc_scan(state.clone(), false).await {
             error!("GC Scan Failed: {}", e);
        }

        sleep(cleanup_interval).await;
    }
}

pub async fn run_garbage_collector(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    tokio::spawn(async move {
        if let Err(e) = perform_gc_scan(state.clone(), true).await {
        error!("Manual GC Failed: {}", e);
    } else {
        info!("Manual GC completed successfully.");
    }

    start_cleanup(
        OBJECT_STORAGE_ROOT
            .get()
            .unwrap_or(&STORAGE_ROOT.to_string()),
    ).await;
    });

    StatusCode::ACCEPTED
}

fn refund_quota_async(uid: String, amount: u64, state: Arc<AppState>) {
    if amount == 0 { return; }
    
    let handle = Handle::current();
    handle.spawn(async move {
        if let Err(e) = update_quota(&uid, -(amount as i64)).await {
            warn!("GC: Failed to refund quota for user {}: {}", uid, e);
        } else {
            info!("GC: Refunded {} bytes to user {}", amount, uid);
            state.emit(&uid, StorageEventPayload::StateInvalidated);
        }
    });
}

struct IndexEntry {
    version_id: String,
    path: PathBuf,
    modified: SystemTime,
}

async fn perform_gc_scan(state: Arc<AppState>, is_manual_run: bool) -> Result<(), AppError> {
    let root_str = OBJECT_STORAGE_ROOT
        .get()
        .ok_or(AppError::InternalServerError("No root".into()))?;
    let storage_root = PathBuf::from(root_str).join(OBJECT_DATA_ROOT);
    
    let retention_duration = Duration::from_secs(CONFIG.garbage_collector.retention_days * 86400);
    let file_ttl = Duration::from_secs(CONFIG.garbage_collector.file_ttl_seconds);

    tokio::task::spawn_blocking(move || -> Result<(), AppError> {
        if !storage_root.exists() {
            return Ok(());
        }

        for user_entry in fs::read_dir(&storage_root).map_err(|e| AppError::Io(e, "read storage root".into()))? {
            let user_entry = match user_entry { Ok(e) => e, Err(_) => continue };
            let uid = user_entry.file_name().to_string_lossy().to_string();
            let user_path = user_entry.path();
            
            if !user_path.is_dir() { continue; }

            for frag_entry in fs::read_dir(&user_path).map_err(|e| AppError::Io(e, "read user root".into()))? {
                let frag_entry = match frag_entry { Ok(e) => e, Err(_) => continue };
                let fragment_path = frag_entry.path();
                if !fragment_path.is_dir() { continue; }

                let index_root = fragment_path.join("index");
                let blobs_root = fragment_path.join("blobs");

                if !blobs_root.exists() && !index_root.exists() { continue; }

                let mut valid_versions = HashSet::new();
                let mut expired_versions = HashSet::new();
                
                if index_root.exists() {
                    let mut object_versions: HashMap<String, Vec<IndexEntry>> = HashMap::new();

                    for entry in WalkDir::new(&index_root).into_iter().filter_map(|e| e.ok()) {
                        if entry.file_type().is_file() {
                            let filename = entry.file_name().to_string_lossy();
                            if let Some((hash, ver_ext)) = filename.split_once('#') {
                                if let Some(ver) = ver_ext.strip_suffix(".bson") {
                                    let modified = entry.metadata().ok()
                                        .and_then(|m| m.modified().ok())
                                        .unwrap_or(SystemTime::now());

                                    object_versions.entry(hash.to_string()).or_default().push(IndexEntry {
                                        version_id: ver.to_string(),
                                        path: entry.path().to_path_buf(),
                                        modified,
                                    });
                                }
                            }
                        }
                    }

                    for (hash, mut versions) in object_versions {
                        versions.sort_by(|a, b| a.version_id.cmp(&b.version_id));

                        if let Some(latest) = versions.last() {
                            valid_versions.insert(latest.version_id.clone());

                            let count = versions.len();
                            if count > 1 {
                                for version in versions.iter().take(count - 1) {
                                    let age = SystemTime::now().duration_since(version.modified).unwrap_or(Duration::ZERO);
                                    
                                    if age > retention_duration {
                                        info!("♻️ Lifecycle: Expired version {}/{} (Age: {:?}). Deleting index entry...", hash, version.version_id, age);
                                        
                                        if let Err(e) = fs::remove_file(&version.path) {
                                            error!("Failed to delete expired index file: {}", e);
                                            valid_versions.insert(version.version_id.clone());
                                        } else {
                                            expired_versions.insert(version.version_id.clone());
                                        }
                                    } else {
                                        valid_versions.insert(version.version_id.clone());
                                    }
                                }
                            }
                        }
                    }
                }

                if blobs_root.exists() {
                    if let Ok(blob_entries) = fs::read_dir(&blobs_root) {
                        for blob_entry in blob_entries.flatten() {
                            let blob_path = blob_entry.path();
                            let version_id = blob_entry.file_name().to_string_lossy().to_string();

                            if !valid_versions.contains(&version_id) {
                                let should_skip_grace_period = is_manual_run || expired_versions.contains(&version_id);

                                if !should_skip_grace_period {
                                    if let Ok(meta) = fs::metadata(&blob_path) {
                                        if let Ok(modified) = meta.modified() {
                                            if let Ok(age) = std::time::SystemTime::now().duration_since(modified) {
                                                if age < file_ttl {
                                                    continue; 
                                                }
                                            }
                                        }
                                    }
                                }

                                let mut size_to_refund = 0;
                                let receipt_path = blob_path.join("reservation.bson");
                                if let Ok(data) = fs::read(&receipt_path) {
                                    if let Ok(receipt) = mongodb::bson::from_slice::<crate::structs::object_storage_structs::ReservationReceipt>(&data) {
                                        size_to_refund = receipt.amount;
                                    }
                                } else {
                                    for file in WalkDir::new(&blob_path).into_iter().filter_map(|e| e.ok()) {
                                        if file.file_type().is_file() {
                                            size_to_refund += file.metadata().map(|m| m.len()).unwrap_or(0);
                                        }
                                    }
                                }

                                info!("🗑️ GC: Deleting orphaned/expired blob version: {}/blobs/{} (Refunding {} bytes)", uid, version_id, size_to_refund);
                                
                                if fs::remove_dir_all(&blob_path).is_ok() {
                                    if size_to_refund > 0 {
                                         refund_quota_async(uid.clone(), size_to_refund, state.clone());
                                    }
                                }
                            }
                        }
                    }
                }

                if index_root.exists() {
                    for entry in WalkDir::new(&index_root).contents_first(true).into_iter().filter_map(|e| e.ok()) {
                        let path = entry.path();
                        if path == index_root { continue; }
                        
                        if entry.file_type().is_dir() {
                            let _ = fs::remove_dir(path);
                        }
                    }
                }
            }
        }
        
        Ok(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(e.to_string()))??;

    Ok(())
}
