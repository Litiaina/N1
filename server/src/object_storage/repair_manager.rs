use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::{OBJECT_DATA_ROOT, PART_META_SUFFIX, QUOTA_FILE};
use crate::object_storage::recovery::{repair_entire_shard, verify_shard_integrity};
use crate::routes::storage_routes::AppState;
use crate::structs::object_storage_structs::{
    INDEX_STORE_DISKS, ObjectMetadata, ObjectPathManager, Quota,
};
use crate::websocket::manager::WsConnectionManager;
use crate::websocket::ws_structs::StorageEventPayload;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;

use chrono::Utc;
use futures_util::{StreamExt, stream};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Instant, SystemTime};
use tokio::fs;
use tokio::fs::File;
use tokio::sync::{RwLock, Semaphore, mpsc};
use tracing::{error, info, warn};
use walkdir::WalkDir;

#[derive(Debug, Clone, Serialize, Default)]
pub struct RepairJobStatus {
    pub is_running: bool,
    pub phase: String,
    pub total_scanned: u64,
    pub total_repaired: u64,
    pub corrupt_found: u64,
    pub missing_meta_repaired: u64,
    pub divergent_meta_repaired: u64,
    pub current_user: String,
    pub last_repaired: Option<String>,
    pub last_error: Option<String>,
    pub last_disagreement: Option<String>,
    pub start_timestamp: i64,
    pub duration_seconds: u64,
    pub scan_rate_per_sec: f64,
}

static REPAIR_STATUS: LazyLock<Arc<RwLock<RepairJobStatus>>> =
    LazyLock::new(|| Arc::new(RwLock::new(RepairJobStatus::default())));

static STOP_SIGNAL: LazyLock<AtomicBool> = LazyLock::new(|| AtomicBool::new(false));

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RepairCheckMode {
    Availability,
    Scrub,
}

#[derive(Clone, Debug)]
pub struct RepairRunConfig {
    pub dry_run: bool,
    pub check_mode: RepairCheckMode,
    pub meta_concurrency: usize,
    pub shard_repair_concurrency: usize,
    pub meta_batch_size: usize,
}

#[derive(Default, Clone)]
struct BatchDelta {
    scanned: u64,
    repaired_shards: u64,
    corrupt_found: u64,
    missing_meta_repaired: u64,
    divergent_meta_repaired: u64,
    last_repaired: Option<String>,
    last_disagreement: Option<String>,
}

#[inline]
fn should_stop() -> bool {
    STOP_SIGNAL.load(Ordering::Relaxed)
}

#[inline]
fn clamp_usize(v: usize, lo: usize, hi: usize) -> usize {
    v.max(lo).min(hi)
}

fn parse_check_mode(params: &HashMap<String, String>) -> RepairCheckMode {
    match params.get("mode").map(|s| s.as_str()) {
        Some("scrub") | Some("strict") | Some("checksum") => RepairCheckMode::Scrub,
        _ => RepairCheckMode::Availability,
    }
}

pub async fn get_repair_status() -> RepairJobStatus {
    REPAIR_STATUS.read().await.clone()
}

pub async fn trigger_repair_job_api(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    {
        let status = REPAIR_STATUS.read().await;
        if status.is_running {
            return (StatusCode::CONFLICT, "Job already running").into_response();
        }
    }

    let cfg = RepairRunConfig {
        dry_run: params.get("dry_run").map(|v| v == "true").unwrap_or(false),
        check_mode: parse_check_mode(&params),
        meta_concurrency: clamp_usize(
            params
                .get("meta_concurrency")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(8),
            1,
            64,
        ),
        shard_repair_concurrency: clamp_usize(
            params
                .get("repair_concurrency")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(2),
            1,
            16,
        ),
        meta_batch_size: clamp_usize(
            params
                .get("batch")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(256),
            32,
            4096,
        ),
    };

    let ws_manager = Arc::new(state.ws_manager.clone());
    let ws_manager_clone = ws_manager.clone();

    tokio::spawn(async move {
        if let Err(e) = run_repair_job(cfg, ws_manager).await {
            error!("Manual Repair Job Failed: {}", e);
            let mut status = REPAIR_STATUS.write().await;
            status.is_running = false;
            status.phase = "Failed".to_string();
            status.last_error = Some(e.to_string());

            ws_manager_clone
                .broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }
    });

    (StatusCode::ACCEPTED, "Storage repair job initiated.").into_response()
}

pub async fn stop_repair_job_api(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    STOP_SIGNAL.store(true, Ordering::Relaxed);
    let mut status = REPAIR_STATUS.write().await;
    if status.is_running {
        status.phase = "Stopping...".to_string();
        state
            .ws_manager
            .broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
    }
    (StatusCode::OK, "Stop signal sent to repair manager.").into_response()
}

pub async fn run_repair_job(
    cfg: RepairRunConfig,
    ws_manager: Arc<WsConnectionManager>,
) -> Result<(), AppError> {
    STOP_SIGNAL.store(false, Ordering::Relaxed);
    let start_time = Instant::now();

    {
        let mut status = REPAIR_STATUS.write().await;
        status.is_running = true;
        status.phase = format!(
            "Initializing (mode={:?}, meta_conc={}, repair_conc={})",
            cfg.check_mode, cfg.meta_concurrency, cfg.shard_repair_concurrency
        );
        status.total_scanned = 0;
        status.total_repaired = 0;
        status.corrupt_found = 0;
        status.missing_meta_repaired = 0;
        status.divergent_meta_repaired = 0;
        status.current_user.clear();
        status.last_repaired = None;
        status.last_error = None;
        status.last_disagreement = None;
        status.start_timestamp = Utc::now().timestamp();
        status.duration_seconds = 0;
        status.scan_rate_per_sec = 0.0;

        ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
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

    info!(
        "Starting Storage Repair Job (index_roots={}, blob_disks={}, dry_run={}, mode={:?})",
        index_roots.len(),
        num_disks,
        cfg.dry_run,
        cfg.check_mode
    );

    let index_roots_arc = Arc::new(index_roots);
    let blob_roots_arc = Arc::new(blob_roots);
    let processed_versions = Arc::new(Mutex::new(HashSet::<String>::new()));
    let processed_users = Arc::new(Mutex::new(HashSet::<String>::new()));

    let shard_repair_sem = Arc::new(Semaphore::new(cfg.shard_repair_concurrency));

    for index_root in index_roots_arc.iter() {
        if should_stop() {
            break;
        }

        if !index_root.exists() {
            warn!("Index root unreachable, skipping scan: {:?}", index_root);
            continue;
        }

        {
            let mut status = REPAIR_STATUS.write().await;
            status.phase = format!("Scanning Root {:?}", index_root);
            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }

        let mut user_entries = match fs::read_dir(index_root).await {
            Ok(e) => e,
            Err(e) => {
                warn!("Failed to read index root {:?}: {}", index_root, e);
                continue;
            }
        };

        while let Ok(Some(user_entry)) = user_entries.next_entry().await {
            if should_stop() {
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
                let mut seen = processed_users.lock().unwrap();
                if seen.contains(&uid) {
                    false
                } else {
                    seen.insert(uid.clone());
                    true
                }
            };

            if should_process_quota {
                if let Err(e) = maybe_repair_quota(&path_manager, &uid, cfg.dry_run).await {
                    warn!(
                        "Warning: Failed to align quota for user {} (non-fatal): {}",
                        uid, e
                    );
                }
            }

            let mut frag_entries = match fs::read_dir(&user_path).await {
                Ok(e) => e,
                Err(_) => continue,
            };

            while let Ok(Some(frag_entry)) = frag_entries.next_entry().await {
                if should_stop() {
                    break;
                }

                let index_dir = frag_entry.path().join("index");
                if !index_dir.exists() {
                    continue;
                }

                let (tx, mut rx) = mpsc::channel::<PathBuf>(1024);
                let index_dir_clone = index_dir.clone();

                tokio::task::spawn_blocking(move || {
                    let walker = WalkDir::new(index_dir_clone).into_iter();
                    for entry in walker.filter_map(|e| e.ok()) {
                        if !entry.file_type().is_file() {
                            continue;
                        }

                        let p = entry.path();
                        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");

                        if name == QUOTA_FILE || name == "reservation.bson" {
                            continue;
                        }
                        if name.ends_with(PART_META_SUFFIX) {
                            continue;
                        }
                        if !name.ends_with(".bson") {
                            continue;
                        }

                        if tx.blocking_send(p.to_path_buf()).is_err() {
                            break;
                        }
                    }
                });

                let mut pending: Vec<PathBuf> = Vec::with_capacity(cfg.meta_batch_size);

                while let Some(meta_path) = rx.recv().await {
                    if should_stop() {
                        break;
                    }
                    pending.push(meta_path);
                    if pending.len() >= cfg.meta_batch_size {
                        apply_meta_batch(
                            &pending,
                            index_root,
                            index_roots_arc.clone(),
                            blob_roots_arc.clone(),
                            num_disks,
                            cfg.clone(),
                            processed_versions.clone(),
                            shard_repair_sem.clone(),
                            ws_manager.clone(),
                            start_time,
                        )
                        .await?;
                        pending.clear();
                    }
                }

                if !pending.is_empty() {
                    apply_meta_batch(
                        &pending,
                        index_root,
                        index_roots_arc.clone(),
                        blob_roots_arc.clone(),
                        num_disks,
                        cfg.clone(),
                        processed_versions.clone(),
                        shard_repair_sem.clone(),
                        ws_manager.clone(),
                        start_time,
                    )
                    .await?;
                    pending.clear();
                }
            }
        }
    }

    {
        let mut status = REPAIR_STATUS.write().await;
        status.is_running = false;
        status.phase = if should_stop() {
            "Stopped by User".to_string()
        } else if cfg.dry_run {
            "Finished scanning".to_string()
        } else {
            "Finished repair".to_string()
        };
        status.duration_seconds = start_time.elapsed().as_secs();
        ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
    }

    info!("Storage Repair Job Ended.");
    Ok(())
}

async fn apply_meta_batch(
    batch: &[PathBuf],
    current_index_root: &Path,
    index_roots: Arc<Vec<PathBuf>>,
    blob_roots: Arc<Vec<PathBuf>>,
    num_disks: usize,
    cfg: RepairRunConfig,
    processed_versions: Arc<Mutex<HashSet<String>>>,
    shard_repair_sem: Arc<Semaphore>,
    ws_manager: Arc<WsConnectionManager>,
    start_time: Instant,
) -> Result<(), AppError> {
    if batch.is_empty() {
        return Ok(());
    }

    let current_index_root = current_index_root.to_path_buf();

    let results = stream::iter(batch.iter().cloned())
        .map(|meta_path| {
            let index_roots = index_roots.clone();
            let blob_roots = blob_roots.clone();
            let processed_versions = processed_versions.clone();
            let shard_repair_sem = shard_repair_sem.clone();
            let cfg = cfg.clone();
            let current_index_root = current_index_root.clone();

            async move {
                process_one_meta_path(
                    meta_path,
                    &current_index_root,
                    &index_roots,
                    &blob_roots,
                    num_disks,
                    cfg,
                    processed_versions,
                    shard_repair_sem,
                )
                .await
            }
        })
        .buffer_unordered(cfg.meta_concurrency)
        .collect::<Vec<Result<BatchDelta, AppError>>>()
        .await;

    let mut agg = BatchDelta::default();

    for r in results {
        match r {
            Ok(d) => {
                agg.scanned += d.scanned;
                agg.repaired_shards += d.repaired_shards;
                agg.corrupt_found += d.corrupt_found;
                agg.missing_meta_repaired += d.missing_meta_repaired;
                agg.divergent_meta_repaired += d.divergent_meta_repaired;
                if d.last_repaired.is_some() {
                    agg.last_repaired = d.last_repaired;
                }
                if d.last_disagreement.is_some() {
                    agg.last_disagreement = d.last_disagreement;
                }
            }
            Err(e) => {
                warn!("Meta batch item failed (non-fatal): {}", e);
            }
        }
    }

    {
        let mut status = REPAIR_STATUS.write().await;
        status.total_scanned += agg.scanned;
        status.corrupt_found += agg.corrupt_found;
        status.missing_meta_repaired += agg.missing_meta_repaired;
        status.divergent_meta_repaired += agg.divergent_meta_repaired;
        status.total_repaired +=
            agg.missing_meta_repaired + agg.divergent_meta_repaired + agg.repaired_shards;

        if agg.last_repaired.is_some() {
            status.last_repaired = agg.last_repaired.clone();
        }
        if agg.last_disagreement.is_some() {
            status.last_disagreement = agg.last_disagreement.clone();
        }

        let elapsed = start_time.elapsed().as_secs_f64();
        status.duration_seconds = elapsed as u64;
        if elapsed > 0.0 {
            status.scan_rate_per_sec = status.total_scanned as f64 / elapsed;
        }

        ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
    }

    Ok(())
}

fn sha256_bytes(b: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b);
    let out = h.finalize();
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&out[..]);
    arr
}

async fn atomic_write(path: &Path, data: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent).await;
    }
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, data).await?;
    fs::rename(&tmp, path).await?;
    Ok(())
}

#[derive(Clone)]
struct ReplicaInfo {
    root: PathBuf,
    full_path: PathBuf,
    bytes: Option<Vec<u8>>,
    mtime: SystemTime,
    len: u64,
    sha256: Option<[u8; 32]>,
}

async fn read_replica(root: &Path, rel: &Path) -> ReplicaInfo {
    let full = root.join(rel);
    match fs::read(&full).await {
        Ok(bytes) => {
            let meta = fs::metadata(&full).await.ok();
            let mtime = meta
                .as_ref()
                .and_then(|m| m.modified().ok())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            let len = meta.as_ref().map(|m| m.len()).unwrap_or(bytes.len() as u64);
            let sha = Some(sha256_bytes(&bytes));
            ReplicaInfo {
                root: root.to_path_buf(),
                full_path: full,
                bytes: Some(bytes),
                mtime,
                len,
                sha256: sha,
            }
        }
        Err(_) => ReplicaInfo {
            root: root.to_path_buf(),
            full_path: full,
            bytes: None,
            mtime: SystemTime::UNIX_EPOCH,
            len: 0,
            sha256: None,
        },
    }
}

async fn reconcile_index_metadata_strict(
    rel: &Path,
    index_roots: &[PathBuf],
    dry_run: bool,
) -> Result<(Vec<u8>, ObjectMetadata, u64, u64, Option<String>), AppError> {
    if index_roots.is_empty() {
        return Err(AppError::InternalServerError(
            "No index roots configured".into(),
        ));
    }

    let mut replicas = Vec::with_capacity(index_roots.len());
    for r in index_roots {
        replicas.push(read_replica(r, rel).await);
    }

    let mut best_idx: Option<usize> = None;
    let mut best_meta: Option<ObjectMetadata> = None;

    for (idx, ri) in replicas.iter().enumerate() {
        let Some(bytes) = &ri.bytes else {
            continue;
        };
        if let Ok(m) = mongodb::bson::from_slice::<ObjectMetadata>(bytes) {
            match best_idx {
                None => {
                    best_idx = Some(idx);
                    best_meta = Some(m);
                }
                Some(cur) => {
                    let cur_ri = &replicas[cur];
                    let take = (ri.mtime > cur_ri.mtime)
                        || (ri.mtime == cur_ri.mtime && ri.len > cur_ri.len)
                        || (ri.mtime == cur_ri.mtime
                            && ri.len == cur_ri.len
                            && ri.sha256.unwrap_or([0u8; 32]) > cur_ri.sha256.unwrap_or([0u8; 32]));
                    if take {
                        best_idx = Some(idx);
                        best_meta = Some(m);
                    }
                }
            }
        }
    }

    let best_idx = best_idx.ok_or_else(|| {
        AppError::InternalServerError(format!(
            "All replicas unreadable/corrupt bson for {}",
            rel.display()
        ))
    })?;

    let canonical_bytes = replicas[best_idx].bytes.clone().ok_or_else(|| {
        AppError::InternalServerError(format!(
            "Canonical replica missing bytes for {}",
            rel.display()
        ))
    })?;
    let canonical_meta = best_meta.ok_or_else(|| {
        AppError::InternalServerError(format!(
            "Canonical replica missing parsed meta for {}",
            rel.display()
        ))
    })?;

    let canon_sha = sha256_bytes(&canonical_bytes);

    let mut missing_repairs = 0u64;
    let mut divergent_repairs = 0u64;

    let mut report_lines: Vec<String> = Vec::new();

    for ri in &replicas {
        match &ri.bytes {
            None => {
                report_lines.push(format!(
                    "MISSING root={:?} path={}",
                    ri.root,
                    ri.full_path.display()
                ));
                if !dry_run {
                    atomic_write(&ri.full_path, &canonical_bytes)
                        .await
                        .map_err(|e| AppError::Io(e, "repair missing index meta".into()))?;
                    missing_repairs += 1;
                }
            }
            Some(b) => {
                let sha = sha256_bytes(b);
                if sha != canon_sha {
                    report_lines.push(format!(
                        "DIVERGENT root={:?} path={} len={} canon_root={:?}",
                        ri.root,
                        ri.full_path.display(),
                        b.len(),
                        replicas[best_idx].root
                    ));
                    if !dry_run {
                        atomic_write(&ri.full_path, &canonical_bytes)
                            .await
                            .map_err(|e| AppError::Io(e, "repair divergent index meta".into()))?;
                        divergent_repairs += 1;
                    }
                }
            }
        }
    }

    let report = if report_lines.is_empty() {
        None
    } else {
        Some(format!(
            "INDEX DISAGREEMENT {}:\n{}",
            rel.display(),
            report_lines.join("\n")
        ))
    };

    Ok((
        canonical_bytes,
        canonical_meta,
        missing_repairs,
        divergent_repairs,
        report,
    ))
}

async fn process_one_meta_path(
    meta_path: PathBuf,
    current_index_root: &PathBuf,
    index_roots: &Vec<PathBuf>,
    blob_roots: &Vec<PathBuf>,
    num_disks: usize,
    cfg: RepairRunConfig,
    processed_versions: Arc<Mutex<HashSet<String>>>,
    shard_repair_sem: Arc<Semaphore>,
) -> Result<BatchDelta, AppError> {
    if should_stop() {
        return Ok(BatchDelta::default());
    }

    let mut delta = BatchDelta {
        scanned: 1,
        ..Default::default()
    };

    let rel = match meta_path.strip_prefix(current_index_root) {
        Ok(r) => r.to_path_buf(),
        Err(_) => {
            delta.corrupt_found += 1;
            return Ok(delta);
        }
    };

    let (canonical_data, canonical_meta, miss_rep, div_rep, dry_report) =
        reconcile_index_metadata_strict(&rel, index_roots, cfg.dry_run).await?;

    delta.missing_meta_repaired += miss_rep;
    delta.divergent_meta_repaired += div_rep;

    if let Some(r) = dry_report {
        delta.last_disagreement = Some(r.clone());
        if cfg.dry_run {
            warn!("{}", r);
        }
    }

    {
        let mut set = processed_versions.lock().unwrap();
        if set.contains(&canonical_meta.version_id) {
            return Ok(delta);
        }
        set.insert(canonical_meta.version_id.clone());
    }

    if let Err(e) = repair_metadata_mirrors(&canonical_meta, &canonical_data, cfg.dry_run).await {
        warn!(
            "Warning: Metadata mirror sync issues for {}/{} (non-fatal): {}",
            canonical_meta.fragment, canonical_meta.object_key, e
        );
    }

    let (repaired, corrupt, last_rep) =
        process_object_repair_fast(canonical_meta, blob_roots, num_disks, cfg, shard_repair_sem)
            .await?;

    delta.repaired_shards += repaired as u64;
    delta.corrupt_found += corrupt as u64;
    delta.last_repaired = last_rep;

    Ok(delta)
}

async fn maybe_repair_quota(
    path_manager: &ObjectPathManager,
    uid: &str,
    dry_run: bool,
) -> Result<(), AppError> {
    let all_user_index_dirs = path_manager.get_all_user_index_dirs(uid);

    let mut best_quota_path: Option<PathBuf> = None;
    let mut max_mtime = SystemTime::UNIX_EPOCH;

    for mirror_dir in &all_user_index_dirs {
        let qp = mirror_dir.join(QUOTA_FILE);
        if let Ok(meta) = fs::metadata(&qp).await {
            if let Ok(mtime) = meta.modified() {
                if mtime > max_mtime {
                    max_mtime = mtime;
                    best_quota_path = Some(qp);
                }
            }
        }
    }

    if let Some(source_path) = best_quota_path {
        if let Ok(quota_data) = fs::read(&source_path).await {
            repair_user_quota(uid, &quota_data, dry_run).await?;
        }
    }

    Ok(())
}

async fn repair_user_quota(uid: &str, source_data: &[u8], dry_run: bool) -> Result<(), AppError> {
    if mongodb::bson::from_slice::<Quota>(source_data).is_err() {
        return Ok(());
    }

    let path_manager = ObjectPathManager::new();
    let all_user_dirs = path_manager.get_all_user_index_dirs(uid);

    for dir in all_user_dirs {
        let target_path = dir.join(QUOTA_FILE);

        if !dir.exists() {
            if dry_run {
                continue;
            }
            if fs::create_dir_all(&dir).await.is_err() {
                continue;
            }
        }

        let needs_repair = match fs::read(&target_path).await {
            Ok(current_data) => current_data != source_data,
            Err(_) => true,
        };

        if needs_repair && !dry_run {
            let tmp = target_path.with_extension("tmp");
            if fs::write(&tmp, source_data).await.is_ok() {
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
            if !fs::try_exists(parent).await.unwrap_or(false) {
                if dry_run {
                    continue;
                }
                if fs::create_dir_all(parent).await.is_err() {
                    continue;
                }
            }
        }

        let needs_repair = match fs::read(&path).await {
            Ok(mirror_data) => mirror_data != data,
            Err(_) => true,
        };

        if needs_repair && !dry_run {
            let tmp_path = path.with_extension("tmp");
            if fs::write(&tmp_path, data).await.is_ok() {
                let _ = fs::rename(&tmp_path, &path).await;
            }
        }
    }

    Ok(())
}

async fn process_object_repair_fast(
    meta: ObjectMetadata,
    blob_roots: &Vec<PathBuf>,
    num_disks: usize,
    cfg: RepairRunConfig,
    shard_repair_sem: Arc<Semaphore>,
) -> Result<(usize, usize, Option<String>), AppError> {
    if meta.parts.is_empty() || num_disks == 0 {
        return Ok((0, 0, None));
    }

    let path_manager = ObjectPathManager::new();
    let meta_arc = Arc::new(meta);

    let mut repaired_total = 0usize;
    let mut corrupt_found = 0usize;
    let mut last_repaired: Option<String> = None;

    for part in &meta_arc.parts {
        if should_stop() {
            break;
        }

        let part_arc = Arc::new(part.clone());
        let mut missing: Vec<usize> = Vec::new();

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

            let mut healthy = false;

            match fs::metadata(&shard_path).await {
                Ok(m) => {
                    if cfg.check_mode == RepairCheckMode::Availability {
                        healthy = m.len() == shard.size_bytes;
                        if !healthy {
                            corrupt_found += 1;
                        }
                    } else {
                        if let Ok(mut f) = File::open(&shard_path).await {
                            healthy = verify_shard_integrity(&mut f, &shard.checksum).await;
                            if !healthy {
                                corrupt_found += 1;
                            }
                        }
                    }
                }
                Err(_) => {
                    healthy = false;
                }
            }

            if !healthy {
                missing.push(shard.shard_index);
            }
        }

        if missing.is_empty() || cfg.dry_run {
            continue;
        }

        let repairs = stream::iter(missing.into_iter())
            .map(|missing_idx| {
                let part_for_task = part_arc.clone();
                let sem = shard_repair_sem.clone();
                let block_size = meta_arc.stripe_size_bytes;

                async move {
                    let _permit = sem.acquire().await.map_err(|_| {
                        AppError::InternalServerError("Repair semaphore closed".into())
                    })?;

                    repair_entire_shard(part_for_task, missing_idx, block_size).await
                }
            })
            .buffer_unordered(cfg.shard_repair_concurrency)
            .collect::<Vec<Result<(), AppError>>>()
            .await;

        for r in repairs {
            match r {
                Ok(_) => {
                    repaired_total += 1;
                    last_repaired = Some(format!("{}/{}", meta_arc.fragment, meta_arc.object_key));
                }
                Err(e) => {
                    warn!("Shard repair failed (non-fatal): {}", e);
                }
            }
        }
    }

    Ok((repaired_total, corrupt_found, last_repaired))
}
