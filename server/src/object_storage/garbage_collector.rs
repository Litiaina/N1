use crate::enums::app_error_enum::AppError;
use crate::fs::cleanup_manager::start_cleanup;
use crate::fs::fs_constants::STORAGE_ROOT;
use crate::object_storage::constants::OBJECT_DATA_ROOT;
use crate::object_storage::quota_manager::update_quota;
use crate::routes::storage_routes::AppState;
use crate::settings::config::CONFIG;
use crate::structs::object_storage_structs::{BLOB_STORE_DISKS, FS_STORAGE_ROOT, INDEX_STORE_DISKS};
use crate::websocket::ws_structs::StorageEventPayload;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use tokio::runtime::Handle;
use std::collections::{HashMap, HashSet};
use std::time::SystemTime;
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
        if let Err(err) = perform_gc_scan(state.clone(), false).await {
             error!("Scan failed: {}", err);
        }

        sleep(cleanup_interval).await;
    }
}

pub async fn run_garbage_collector(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    tokio::spawn(async move {

    if let Err(err) = perform_gc_scan(state.clone(), true).await {
        error!("Scan failed: {}", err);
    }

    start_cleanup(
        FS_STORAGE_ROOT
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
            warn!("Failed to refund quota for user {}: {}", uid, e);
        } else {
            info!("Refunded {} bytes to user {}", amount, uid);
            state.emit(&uid, StorageEventPayload::StateInvalidated);
        }
    });
}

struct IndexEntry {
    version_id: String,
    modified: SystemTime,
}

async fn perform_gc_scan(state: Arc<AppState>, is_manual_run: bool) -> Result<(), AppError> {
    let index_roots_config = INDEX_STORE_DISKS
        .get()
        .ok_or(AppError::InternalServerError("Index Store Config not initialized".into()))?;
    
    let blob_roots_config = BLOB_STORE_DISKS
        .get()
        .ok_or(AppError::InternalServerError("Blob Store Config not initialized".into()))?;

    let index_roots: Vec<PathBuf> = index_roots_config.iter().map(|s| PathBuf::from(s).join(OBJECT_DATA_ROOT)).collect();
    let blob_roots: Vec<PathBuf> = blob_roots_config.iter().map(|s| PathBuf::from(s).join(OBJECT_DATA_ROOT)).collect();
    
    let primary_index_root = index_roots.iter().find(|p| p.exists());

    if primary_index_root.is_none() {
        return Ok(());
    }
    let primary_index_root = primary_index_root.unwrap().clone();

    let retention_duration = Duration::from_secs(CONFIG.garbage_collector.retention_days * 86400);
    let file_ttl = Duration::from_secs(CONFIG.garbage_collector.file_ttl_seconds);

    tokio::task::spawn_blocking(move || -> Result<(), AppError> {
        for user_entry in fs::read_dir(&primary_index_root).map_err(|e| AppError::Io(e, "read primary root".into()))? {
            let user_entry = match user_entry { Ok(e) => e, Err(_) => continue };
            let uid = user_entry.file_name().to_string_lossy().to_string();
            let user_primary_path = user_entry.path();
            
            if !user_primary_path.is_dir() { continue; }

            for frag_entry in fs::read_dir(&user_primary_path).map_err(|e| AppError::Io(e, "read user root".into()))? {
                let frag_entry = match frag_entry { Ok(e) => e, Err(_) => continue };
                let fragment_name = frag_entry.file_name().to_string_lossy().to_string();
                let fragment_path = frag_entry.path();
                
                if !fragment_path.is_dir() { continue; }

                let index_dir = fragment_path.join("index");
                
                let mut valid_versions = HashSet::new();
                let mut expired_versions = HashSet::new();
                let mut refunded_versions = HashSet::new();

                if index_dir.exists() {
                    let mut object_versions: HashMap<String, Vec<IndexEntry>> = HashMap::new();

                    for entry in WalkDir::new(&index_dir).into_iter().filter_map(|e| e.ok()) {
                        if entry.file_type().is_file() {
                            let filename = entry.file_name().to_string_lossy();
                            if let Some((hash, ver_ext)) = filename.split_once('#') {
                                if let Some(ver) = ver_ext.strip_suffix(".bson") {
                                    let modified = entry.metadata().ok()
                                        .and_then(|m| m.modified().ok())
                                        .unwrap_or(SystemTime::now());

                                    object_versions.entry(hash.to_string()).or_default().push(IndexEntry {
                                        version_id: ver.to_string(),
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
                                        info!("Expired version {}/{} (Age: {:?}). Marking for deletion.", hash, version.version_id, age);
                                        expired_versions.insert(version.version_id.clone());
                                    } else {
                                        valid_versions.insert(version.version_id.clone());
                                    }
                                }
                            }
                        }
                    }
                }

                for root in &index_roots {
                    let user_frag_root = root.join(&uid).join(&fragment_name);
                    
                    let mirror_index = user_frag_root.join("index");
                    if mirror_index.exists() {
                         for entry in WalkDir::new(&mirror_index).into_iter().filter_map(|e| e.ok()) {
                            if entry.file_type().is_file() {
                                let filename = entry.file_name().to_string_lossy();
                                if let Some((_, ver_ext)) = filename.split_once('#') {
                                    if let Some(ver) = ver_ext.strip_suffix(".bson") {
                                        if expired_versions.contains(ver) {
                                            let _ = fs::remove_file(entry.path());
                                        }
                                    }
                                }
                            }
                         }
                    }

                    let mirror_blobs = user_frag_root.join("blobs");
                    if mirror_blobs.exists() {
                        if let Ok(entries) = fs::read_dir(&mirror_blobs) {
                            for entry in entries.filter_map(|e| e.ok()) {
                                let path = entry.path();
                                if !path.is_dir() { continue; }
                                let version_id = entry.file_name().to_string_lossy().to_string();

                                let mut attempt_refund = false;

                                let should_delete = if valid_versions.contains(&version_id) {
                                    true 
                                } else if expired_versions.contains(&version_id) {
                                    true
                                } else {
                                    if is_manual_run {
                                        true
                                    } else {
                                        let mut is_stale = false;
                                         if let Ok(meta) = fs::metadata(&path) {
                                            if let Ok(modified) = meta.modified() {
                                                if let Ok(age) = SystemTime::now().duration_since(modified) {
                                                    if age > file_ttl {
                                                        is_stale = true;
                                                        attempt_refund = true;
                                                    }
                                                }
                                            }
                                         }
                                         is_stale
                                    }
                                };

                                if should_delete {
                                    if attempt_refund && !refunded_versions.contains(&version_id) {
                                        let receipt_path = path.join("reservation.bson");
                                        if let Ok(data) = fs::read(&receipt_path) {
                                            if let Ok(receipt) = mongodb::bson::from_slice::<crate::structs::object_storage_structs::ReservationReceipt>(&data) {
                                                if receipt.amount > 0 {
                                                    refund_quota_async(uid.clone(), receipt.amount, state.clone());
                                                    refunded_versions.insert(version_id.clone());
                                                }
                                            }
                                        }
                                    }
                                    let _ = fs::remove_dir_all(&path);
                                }
                            }
                        }
                    }
                }

                for root in &blob_roots {
                    let user_frag_blobs = root.join(&uid).join(&fragment_name).join("blobs");
                    
                    if user_frag_blobs.exists() {
                         if let Ok(entries) = fs::read_dir(&user_frag_blobs) {
                            for entry in entries.filter_map(|e| e.ok()) {
                                let path = entry.path();
                                if !path.is_dir() { continue; }
                                let version_id = entry.file_name().to_string_lossy().to_string();

                                let should_delete = if valid_versions.contains(&version_id) {
                                    false
                                } else if expired_versions.contains(&version_id) {
                                    true
                                } else {
                                    if is_manual_run {
                                        true
                                    } else {
                                        let mut is_stale = false;
                                         if let Ok(meta) = fs::metadata(&path) {
                                            if let Ok(modified) = meta.modified() {
                                                if let Ok(age) = SystemTime::now().duration_since(modified) {
                                                    if age > file_ttl {
                                                        is_stale = true;
                                                    }
                                                }
                                            }
                                         }
                                         is_stale
                                    }
                                };

                                if should_delete {
                                    let _ = fs::remove_dir_all(&path);
                                }
                            }
                         }
                    }
                }
            }
        }
        Ok(())
    }).await.map_err(|e| AppError::InternalServerError(format!("GC Task Failed: {}", e)))??;

    Ok(())
}
