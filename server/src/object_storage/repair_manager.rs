use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::OBJECT_DATA_ROOT;
use crate::object_storage::recovery::{repair_entire_shard, verify_shard_integrity};
use crate::structs::object_storage_structs::INDEX_STORE_DISKS;
use crate::structs::object_storage_structs::{ObjectMetadata, ObjectPathManager, Quota};
use axum::response::sse::{Event, Sse};
use axum::{extract::Query, http::StatusCode, response::IntoResponse};
use chrono::Utc;
use futures_util::stream::{self, Stream};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};
use tokio::fs::{self, File};
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use walkdir::WalkDir;

#[derive(Debug, Clone, Serialize, Default)]
pub struct RepairJobStatus {
    pub is_running: bool,
    pub phase: String,
    pub total_scanned: u64,
    pub total_repaired: u64,
    pub corrupt_found: u64,
    pub current_user: String,
    pub last_repaired: Option<String>,
    pub last_error: Option<String>,

    pub start_timestamp: i64,
    pub duration_seconds: u64,
    pub scan_rate_per_sec: f64,
}

static REPAIR_STATUS: LazyLock<Arc<RwLock<RepairJobStatus>>> =
    LazyLock::new(|| Arc::new(RwLock::new(RepairJobStatus::default())));

static STOP_SIGNAL: LazyLock<AtomicBool> = LazyLock::new(|| AtomicBool::new(false));

pub async fn monitor_repair_job_api() -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = stream::unfold((), |()| async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let state = REPAIR_STATUS.read().await.clone();
        let event = Event::default()
            .json_data(state)
            .unwrap_or_else(|_| Event::default().data("serialization error"));
        Some((Ok(event), ()))
    });

    Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::default())
}

pub async fn trigger_repair_job_api(
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    {
        let status = REPAIR_STATUS.read().await;
        if status.is_running {
            return (StatusCode::CONFLICT, "Job already running").into_response();
        }
    }

    let dry_run = params.get("dry_run").map(|v| v == "true").unwrap_or(false);

    tokio::spawn(async move {
        if let Err(e) = run_repair_job(dry_run).await {
            error!("Manual Repair Job Failed: {}", e);
            let mut status = REPAIR_STATUS.write().await;
            status.is_running = false;
            status.phase = "Failed".to_string();
            status.last_error = Some(e.to_string());
        }
    });

    (StatusCode::ACCEPTED, "Storage repair job initiated.").into_response()
}

pub async fn stop_repair_job_api() -> impl IntoResponse {
    STOP_SIGNAL.store(true, Ordering::Relaxed);
    let mut status = REPAIR_STATUS.write().await;
    if status.is_running {
        status.phase = "Stopping...".to_string();
    }
    (StatusCode::OK, "Stop signal sent to repair manager.")
}

pub async fn run_repair_job(dry_run: bool) -> Result<(), AppError> {
    STOP_SIGNAL.store(false, Ordering::Relaxed);
    let start_time = Instant::now();

    {
        let mut status = REPAIR_STATUS.write().await;
        status.is_running = true;
        status.phase = "Initializing".to_string();
        status.total_scanned = 0;
        status.total_repaired = 0;
        status.corrupt_found = 0;
        status.last_error = None;
        status.start_timestamp = Utc::now().timestamp();
        status.duration_seconds = 0;
        status.scan_rate_per_sec = 0.0;
    }

    let path_manager = ObjectPathManager::new();
    let index_roots_config = INDEX_STORE_DISKS
        .get()
        .ok_or(AppError::InternalServerError(
            "Index Store Config not initialized".into(),
        ))?;
    let index_roots: Vec<PathBuf> = index_roots_config
        .iter()
        .map(|s| PathBuf::from(s).join(OBJECT_DATA_ROOT))
        .collect();
    let blob_roots = path_manager.get_blob_roots();
    let num_disks = blob_roots.len();
    let processed_versions = Arc::new(Mutex::new(HashSet::new()));
    let processed_users = Arc::new(Mutex::new(HashSet::new()));

    info!(
        "Starting Storage Repair Job (Scanning {} index roots)",
        index_roots.len()
    );

    for index_root in index_roots {
        if STOP_SIGNAL.load(Ordering::Relaxed) {
            break;
        }
        if !index_root.exists() {
            warn!("Index root unreachable, skipping scan: {:?}", index_root);
            continue;
        }

        {
            let mut status = REPAIR_STATUS.write().await;
            status.phase = format!("Scanning Root {:?}", index_root);
        }

        let mut user_entries = match fs::read_dir(&index_root).await {
            Ok(e) => e,
            Err(e) => {
                error!("Failed to read index root {:?}: {}", index_root, e);
                continue;
            }
        };

        while let Ok(Some(user_entry)) = user_entries.next_entry().await {
            if STOP_SIGNAL.load(Ordering::Relaxed) {
                break;
            }
            let user_path = user_entry.path();
            if !user_path.is_dir() {
                continue;
            }

            let uid = user_entry.file_name().to_string_lossy().to_string();
            {
                let mut status = REPAIR_STATUS.write().await;
                status.current_user = uid.clone();
            }

            let should_process_quota = {
                let mut p_users = processed_users.lock().unwrap();
                if p_users.contains(&uid) {
                    false
                } else {
                    p_users.insert(uid.clone());
                    true
                }
            };

            if should_process_quota {
                let quota_path = user_path.join("quota.bson");
                if let Ok(quota_data) = fs::read(&quota_path).await {
                    if let Err(e) = repair_user_quota(&uid, &quota_data, dry_run).await {
                        warn!(
                            "Warning: Failed to align quota for user {} (non-fatal): {}",
                            uid, e
                        );
                    }
                }
            }

            let mut frag_entries = match fs::read_dir(&user_path).await {
                Ok(e) => e,
                Err(_) => continue,
            };

            while let Ok(Some(frag_entry)) = frag_entries.next_entry().await {
                if STOP_SIGNAL.load(Ordering::Relaxed) {
                    break;
                }
                let index_dir = frag_entry.path().join("index");
                if !index_dir.exists() {
                    continue;
                }

                let (tx, mut rx) = tokio::sync::mpsc::channel(100);

                let index_dir_clone = index_dir.clone();
                tokio::task::spawn_blocking(move || {
                    let walker = WalkDir::new(index_dir_clone).into_iter();
                    for entry in walker.filter_map(|e| e.ok()) {
                        if entry.file_type().is_file()
                            && entry.path().extension().map_or(false, |ext| ext == "bson")
                        {
                            if tx.blocking_send(entry.path().to_owned()).is_err() {
                                break;
                            }
                        }
                    }
                });

                let blob_roots_clone = blob_roots.clone();
                let processed_arc = processed_versions.clone();

                let mut batch_scanned_count: u64 = 0;
                const STATUS_UPDATE_BATCH: u64 = 50;

                while let Some(meta_path) = rx.recv().await {
                    if STOP_SIGNAL.load(Ordering::Relaxed) {
                        break;
                    }
                    batch_scanned_count += 1;

                    if batch_scanned_count >= STATUS_UPDATE_BATCH {
                        let mut status = REPAIR_STATUS.write().await;
                        status.total_scanned += batch_scanned_count;
                        let elapsed = start_time.elapsed().as_secs_f64();
                        status.duration_seconds = elapsed as u64;
                        if elapsed > 0.0 {
                            status.scan_rate_per_sec = status.total_scanned as f64 / elapsed;
                        }
                        batch_scanned_count = 0;
                    }

                    if let Ok(data) = fs::read(&meta_path).await {
                        if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                            if meta.is_delete_marker {
                                continue;
                            }
                            {
                                let mut set = processed_arc.lock().unwrap();
                                if set.contains(&meta.version_id) {
                                    continue;
                                }
                                set.insert(meta.version_id.clone());
                            }

                            if let Err(e) = repair_metadata_mirrors(&meta, &data, dry_run).await {
                                warn!(
                                    "Warning: Metadata sync issues for {}/{}: {}",
                                    meta.fragment, meta.object_key, e
                                );
                            }

                            if let Err(e) =
                                process_object_repair(meta, &blob_roots_clone, num_disks, dry_run)
                                    .await
                            {
                                warn!("Object repair check skipped: {}", e);
                            }
                        }
                    }
                }

                if batch_scanned_count > 0 {
                    let mut status = REPAIR_STATUS.write().await;
                    status.total_scanned += batch_scanned_count;
                    let elapsed = start_time.elapsed().as_secs_f64();
                    status.duration_seconds = elapsed as u64;
                    if elapsed > 0.0 {
                        status.scan_rate_per_sec = status.total_scanned as f64 / elapsed;
                    }
                }
            }
        }
    }

    {
        let mut status = REPAIR_STATUS.write().await;
        status.is_running = false;
        status.phase = if STOP_SIGNAL.load(Ordering::Relaxed) {
            "Stopped by User".to_string()
        } else {
            "Completed".to_string()
        };
        status.duration_seconds = start_time.elapsed().as_secs();
    }

    info!("Storage Repair Job Ended.");
    Ok(())
}

async fn repair_user_quota(uid: &str, source_data: &[u8], dry_run: bool) -> Result<(), AppError> {
    if mongodb::bson::from_slice::<Quota>(source_data).is_err() {
        return Ok(());
    }
    let path_manager = ObjectPathManager::new();
    let all_user_dirs = path_manager.get_all_user_index_dirs(uid);

    for dir in all_user_dirs {
        let target_path = dir.join("quota.bson");
        let mut needs_repair = false;
        if !dir.exists() {
            if !dry_run {
                if let Err(_) = fs::create_dir_all(&dir).await {
                    continue;
                }
            }
        }

        match fs::read(&target_path).await {
            Ok(current_data) => {
                if current_data != source_data {
                    needs_repair = true;
                }
            }
            Err(_) => {
                needs_repair = true;
            }
        }

        if needs_repair && !dry_run {
            let tmp = target_path.with_extension("tmp");
            if let Ok(_) = fs::write(&tmp, source_data).await {
                let _ = fs::rename(&tmp, &target_path).await;
            }
        }
    }
    Ok(())
}

pub async fn repair_metadata_mirrors(
    meta: &ObjectMetadata,
    data: &[u8],
    dry_run: bool,
) -> Result<(), AppError> {
    let path_manager = ObjectPathManager::new();
    let all_paths = path_manager.get_all_metadata_paths(
        &meta.owner_uid,
        &meta.fragment,
        &meta.object_key,
        &meta.version_id,
    );

    for path in all_paths {
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                if !dry_run {
                    if let Err(_) = fs::create_dir_all(parent).await {
                        continue;
                    }
                }
            }
        }

        let mut needs_repair = false;
        match fs::read(&path).await {
            Ok(mirror_data) => {
                if mirror_data != data {
                    needs_repair = true;
                }
            }
            Err(_) => {
                needs_repair = true;
            }
        }

        if needs_repair && !dry_run {
            let tmp_path = path.with_extension("tmp");
            if let Ok(_) = fs::write(&tmp_path, data).await {
                let _ = fs::rename(&tmp_path, &path).await;
            }
        }
    }
    Ok(())
}

async fn process_object_repair(
    meta: ObjectMetadata,
    blob_roots: &[PathBuf],
    num_disks: usize,
    dry_run: bool,
) -> Result<(), AppError> {
    let path_manager = ObjectPathManager::new();
    let meta_arc = Arc::new(meta);

    for part in &meta_arc.parts {
        let part_arc = Arc::new(part.clone());
        let mut missing_shards = Vec::new();

        for shard in &part.shards {
            let disk_idx = (part.part_index as usize + shard.shard_index) % num_disks;
            if disk_idx >= blob_roots.len() {
                continue;
            }
            let disk_root = &blob_roots[disk_idx];
            if !disk_root.exists() {
                continue;
            }

            let shard_dir = path_manager.get_shard_dir_on_disk(
                disk_root,
                &meta_arc.owner_uid,
                &meta_arc.fragment,
                &meta_arc.version_id,
            );
            let shard_path = shard_dir.join(&shard.file_name);

            let mut is_healthy = false;
            if fs::try_exists(&shard_path).await.unwrap_or(false) {
                if let Ok(mut f) = File::open(&shard_path).await {
                    if verify_shard_integrity(&mut f, &shard.checksum).await {
                        is_healthy = true;
                    } else {
                        let mut status = REPAIR_STATUS.write().await;
                        status.corrupt_found += 1;
                    }
                }
            }
            if !is_healthy {
                missing_shards.push(shard.shard_index);
            }
        }

        if missing_shards.is_empty() || dry_run {
            continue;
        }

        for missing_idx in missing_shards {
            let part_for_task = part_arc.clone();
            if let Err(e) =
                repair_entire_shard(part_for_task, missing_idx, meta_arc.stripe_size_bytes).await
            {
                warn!("Shard repair failed (disk might be offline): {}", e);
            } else {
                let mut status = REPAIR_STATUS.write().await;
                status.total_repaired += 1;
                status.last_repaired =
                    Some(format!("{}/{}", meta_arc.fragment, meta_arc.object_key));
            }
        }
    }
    Ok(())
}
