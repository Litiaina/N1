use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::{METADATA_EXT, PARITY_SUFFIX, QUOTA_FILE, SHARD_SUFFIX};
use crate::object_storage::disk_manager::{
    INDEX_STORE_DISKS, ObjectMetadata, ObjectPathManager, PartMetadata, Quota,
};
use crate::object_storage::recovery::{repair_entire_shard, verify_shard_integrity};
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::manager::WsConnectionManager;
use crate::websocket::ws_structs::StorageEventPayload;

use axum::extract::State;
use axum::{Json, extract::Query, http::StatusCode, response::IntoResponse};
use chrono::Utc;
use futures_util::{StreamExt, stream};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Instant, SystemTime};
use tokio::fs::{self, File};
use tokio::sync::RwLock;
use tokio::sync::Semaphore;
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
    pub last_disagreement: Option<String>,

    pub current_fragment: String,
    pub last_repaired: Option<String>,
    pub last_error: Option<String>,

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

#[inline]
fn strip_tmp_attempt_suffix(s: &str) -> &str {
    if let Some(pos) = s.find(".tmp.") {
        &s[..pos]
    } else {
        s
    }
}

#[inline]
fn shard_ext(is_parity: bool) -> &'static str {
    if is_parity {
        PARITY_SUFFIX
    } else {
        SHARD_SUFFIX
    }
}

#[inline]
fn stable_shard_file_name(part_index: u64, shard_index: usize, is_parity: bool) -> String {
    format!(
        "part_{}_shard_{}{}",
        part_index,
        shard_index,
        shard_ext(is_parity)
    )
}

fn expected_shard_path(
    path_manager: &ObjectPathManager,
    blob_roots: &[PathBuf],
    fragment: &str,
    version_id: &str,
    part_index: u64,
    shard_index: usize,
    is_parity: bool,
) -> Option<PathBuf> {
    let num_disks = blob_roots.len();
    if num_disks == 0 {
        return None;
    }
    let disk_idx = (part_index as usize + shard_index) % num_disks;
    if disk_idx >= blob_roots.len() {
        return None;
    }
    let disk_root = &blob_roots[disk_idx];
    let shard_dir = path_manager.get_shard_dir_on_disk(disk_root, fragment, version_id);
    let name = stable_shard_file_name(part_index, shard_index, is_parity);
    Some(shard_dir.join(name))
}

async fn normalize_part_paths_for_repair(
    path_manager: &ObjectPathManager,
    blob_roots: &[PathBuf],
    fragment: &str,
    version_id: &str,
    mut part: PartMetadata,
) -> PartMetadata {
    for shard in part.shards.iter_mut() {
        let stable_name =
            stable_shard_file_name(part.part_index, shard.shard_index, shard.is_parity);
        shard.file_name = stable_name;

        let abs = shard.absolute_path.clone();
        let abs_norm = strip_tmp_attempt_suffix(abs.as_str()).to_string();

        let expected = expected_shard_path(
            path_manager,
            blob_roots,
            fragment,
            version_id,
            part.part_index,
            shard.shard_index,
            shard.is_parity,
        );

        let mut chosen: Option<PathBuf> = None;

        if !abs_norm.is_empty() {
            let p = PathBuf::from(&abs_norm);
            if fs::try_exists(&p).await.unwrap_or(false) {
                chosen = Some(p);
            }
        }

        if chosen.is_none() {
            if let Some(exp) = expected.as_ref() {
                if fs::try_exists(exp).await.unwrap_or(false) {
                    chosen = Some(exp.clone());
                }
            }
        }

        if chosen.is_none() {
            if let Some(exp) = expected {
                chosen = Some(exp);
            } else if !abs_norm.is_empty() {
                chosen = Some(PathBuf::from(abs_norm));
            }
        }

        if let Some(p) = chosen {
            shard.absolute_path = p.to_string_lossy().to_string();
        }
    }

    part
}

pub async fn get_repair_status() -> RepairJobStatus {
    REPAIR_STATUS.read().await.clone()
}

pub async fn get_repair_status_api(claims: Claims) -> Result<Json<RepairJobStatus>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let status = REPAIR_STATUS.read().await.clone();
    Ok(Json(status))
}

pub async fn trigger_repair_job_api(
    claims: Claims,
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write || !claims.modify || !claims.delete {
        return Err(AppError::Forbidden(
            "Write, Modify and Delete permission required".into(),
        ));
    }

    {
        let status = REPAIR_STATUS.read().await;
        if status.is_running {
            return Err(AppError::Conflict(
                "A repair job is already running.".into(),
            ));
        }
    }

    let cfg = RepairRunConfig {
        dry_run: params.get("dry_run").map(|v| v == "true").unwrap_or(false),
        check_mode: parse_check_mode(&params),
        meta_concurrency: clamp_usize(
            params
                .get("meta_concurrency")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(16),
            1,
            128,
        ),
        shard_repair_concurrency: clamp_usize(
            params
                .get("repair_concurrency")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(4),
            1,
            64,
        ),
        meta_batch_size: clamp_usize(
            params
                .get("batch")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(512),
            32,
            8192,
        ),
    };

    let ws_manager = Arc::new(state.ws_manager.clone());
    let ws_manager_clone = ws_manager.clone();

    tokio::spawn(async move {
        if let Err(e) = run_repair_job(cfg, ws_manager.clone()).await {
            error!("Manual Repair Job Failed: {}", e);
            let mut status = REPAIR_STATUS.write().await;
            status.is_running = false;
            status.phase = "Failed".to_string();
            status.last_error = Some(e.to_string());
            ws_manager_clone
                .broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }
    });

    Ok((
        StatusCode::ACCEPTED,
        "Storage repair job initiated. Check logs or poll /status/repair.",
    )
        .into_response())
}

pub async fn stop_repair_job_api(
    claims: Claims,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write || !claims.modify || !claims.delete {
        return Err(AppError::Forbidden(
            "Write, Modify and Delete permission required".into(),
        ));
    }

    STOP_SIGNAL.store(true, Ordering::Relaxed);
    let mut status = REPAIR_STATUS.write().await;
    if status.is_running {
        status.phase = "Stopping...".to_string();
        state
            .ws_manager
            .broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
    }
    Ok((StatusCode::OK, "Stop signal sent to repair manager."))
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
            "Initializing (mode={:?}, meta_conc={}, repair_conc={}, batch={}, dry_run={})",
            cfg.check_mode,
            cfg.meta_concurrency,
            cfg.shard_repair_concurrency,
            cfg.meta_batch_size,
            cfg.dry_run
        );
        status.total_scanned = 0;
        status.total_repaired = 0;
        status.corrupt_found = 0;
        status.missing_meta_repaired = 0;
        status.divergent_meta_repaired = 0;
        status.last_disagreement = None;
        status.current_fragment.clear();
        status.last_repaired = None;
        status.last_error = None;
        status.start_timestamp = Utc::now().timestamp();
        status.duration_seconds = 0;
        status.scan_rate_per_sec = 0.0;

        ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
    }

    let path_manager = ObjectPathManager::new();
    let _index_roots_config = INDEX_STORE_DISKS
        .get()
        .ok_or(AppError::InternalServerError(
            "Index Store Config not initialized".into(),
        ))?;

    let index_roots = path_manager.get_all_index_roots();
    let blob_roots = path_manager.get_blob_roots();
    let num_disks = blob_roots.len();

    let processed_versions = Arc::new(Mutex::new(HashSet::<String>::new()));
    let processed_fragments = Arc::new(Mutex::new(HashSet::<String>::new()));

    let shard_repair_sem = Arc::new(Semaphore::new(cfg.shard_repair_concurrency));

    info!(
        "Starting Storage Repair Job (Scanning {} index roots). mode={:?}, dry_run={}",
        index_roots.len(),
        cfg.check_mode,
        cfg.dry_run
    );

    for index_root in index_roots {
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

        let mut frag_entries = match fs::read_dir(&index_root).await {
            Ok(e) => e,
            Err(e) => {
                error!("Failed to read index root {:?}: {}", index_root, e);
                continue;
            }
        };

        while let Ok(Some(frag_entry)) = frag_entries.next_entry().await {
            if should_stop() {
                break;
            }
            let frag_path = frag_entry.path();
            if !frag_path.is_dir() {
                continue;
            }

            let fragment_hash = frag_entry.file_name().to_string_lossy().to_string();
            if fragment_hash.starts_with('.') {
                continue;
            }

            {
                let mut status = REPAIR_STATUS.write().await;
                status.current_fragment = fragment_hash.clone();
            }

            let should_process_quota = {
                let mut seen = processed_fragments.lock().unwrap();
                if seen.contains(&fragment_hash) {
                    false
                } else {
                    seen.insert(fragment_hash.clone());
                    true
                }
            };

            if should_process_quota {
                let quota_path = frag_path.join(QUOTA_FILE);
                if let Ok(quota_data) = fs::read(&quota_path).await {
                    if let Err(e) =
                        repair_fragment_quota(&fragment_hash, &quota_data, cfg.dry_run).await
                    {
                        warn!(
                            "Warning: Failed to align quota for fragment {} (non-fatal): {}",
                            fragment_hash, e
                        );
                    }
                }
            }

            let index_dir = frag_path.join("index");
            if !index_dir.exists() {
                continue;
            }

            let (tx, mut rx) = tokio::sync::mpsc::channel::<PathBuf>(1024);
            let index_dir_clone = index_dir.clone();
            tokio::task::spawn_blocking(move || {
                let walker = WalkDir::new(index_dir_clone).into_iter();
                for entry in walker.filter_map(|e| e.ok()) {
                    if entry.file_type().is_file()
                        && entry.path().to_string_lossy().ends_with(METADATA_EXT)
                    {
                        if tx.blocking_send(entry.path().to_owned()).is_err() {
                            break;
                        }
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
                        &blob_roots,
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
                    &blob_roots,
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

    {
        let mut status = REPAIR_STATUS.write().await;
        status.is_running = false;
        status.phase = if should_stop() {
            "Stopped by User".to_string()
        } else {
            "Completed".to_string()
        };
        status.duration_seconds = start_time.elapsed().as_secs();
        ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));

        info!(
            "Repair Job Ended. Scanned: {}, Repaired: {}, Corrupt: {}, MissingMeta: {}, DivergentMeta: {}",
            status.total_scanned,
            status.total_repaired,
            status.corrupt_found,
            status.missing_meta_repaired,
            status.divergent_meta_repaired
        );
    }

    Ok(())
}

async fn apply_meta_batch(
    batch: &[PathBuf],
    blob_roots: &[PathBuf],
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

    let results = stream::iter(batch.iter().cloned())
        .map(|meta_path| {
            let blob_roots = blob_roots.to_vec();
            let processed_versions = processed_versions.clone();
            let shard_repair_sem = shard_repair_sem.clone();
            let cfg = cfg.clone();

            async move {
                process_one_meta_path(
                    meta_path,
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
            agg.repaired_shards + agg.missing_meta_repaired + agg.divergent_meta_repaired;

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
    full_path: PathBuf,
    bytes: Option<Vec<u8>>,
    mtime: SystemTime,
    len: u64,
    sha256: Option<[u8; 32]>,
    parsed_ok: bool,
}

async fn read_replica(full: &Path) -> ReplicaInfo {
    match fs::read(full).await {
        Ok(bytes) => {
            let meta = fs::metadata(full).await.ok();
            let mtime = meta
                .as_ref()
                .and_then(|m| m.modified().ok())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            let len = meta.as_ref().map(|m| m.len()).unwrap_or(bytes.len() as u64);
            let sha = Some(sha256_bytes(&bytes));
            let parsed_ok = rmp_serde::from_slice::<ObjectMetadata>(&bytes).is_ok();
            ReplicaInfo {
                full_path: full.to_path_buf(),
                bytes: Some(bytes),
                mtime,
                len,
                sha256: sha,
                parsed_ok,
            }
        }
        Err(_) => ReplicaInfo {
            full_path: full.to_path_buf(),
            bytes: None,
            mtime: SystemTime::UNIX_EPOCH,
            len: 0,
            sha256: None,
            parsed_ok: false,
        },
    }
}

async fn reconcile_object_metadata_strict(
    fragment: &str,
    object_key: &str,
    version_id: &str,
    dry_run: bool,
) -> Result<(Vec<u8>, ObjectMetadata, u64, u64, Option<String>), AppError> {
    let path_manager = ObjectPathManager::new();
    let all_paths = path_manager.get_all_metadata_paths(fragment, object_key, version_id);

    if all_paths.is_empty() {
        return Err(AppError::InternalServerError(
            "No metadata paths configured".into(),
        ));
    }

    let mut replicas: Vec<ReplicaInfo> = Vec::with_capacity(all_paths.len());
    for p in &all_paths {
        replicas.push(read_replica(p).await);
    }

    let mut best_idx: Option<usize> = None;
    let mut best_meta: Option<ObjectMetadata> = None;

    for (idx, ri) in replicas.iter().enumerate() {
        if !ri.parsed_ok {
            continue;
        }
        let Some(bytes) = &ri.bytes else {
            continue;
        };
        let Ok(meta) = rmp_serde::from_slice::<ObjectMetadata>(bytes) else {
            continue;
        };

        match best_idx {
            None => {
                best_idx = Some(idx);
                best_meta = Some(meta);
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
                    best_meta = Some(meta);
                }
            }
        }
    }

    let best_idx = best_idx.ok_or_else(|| {
        AppError::InternalServerError(format!(
            "All replicas unreadable/corrupt msgpack for {}/{}/{}",
            fragment, object_key, version_id
        ))
    })?;

    let canonical_bytes = replicas[best_idx]
        .bytes
        .clone()
        .ok_or_else(|| AppError::InternalServerError("Canonical replica missing bytes".into()))?;

    let canonical_meta = best_meta
        .ok_or_else(|| AppError::InternalServerError("Canonical replica missing meta".into()))?;
    let canon_sha = sha256_bytes(&canonical_bytes);

    let mut missing_repairs = 0u64;
    let mut divergent_repairs = 0u64;
    let mut report_lines: Vec<String> = Vec::new();

    for ri in &replicas {
        match &ri.bytes {
            None => {
                report_lines.push(format!("MISSING path={}", ri.full_path.display()));
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
                        "DIVERGENT path={} len={} canon_path={}",
                        ri.full_path.display(),
                        b.len(),
                        replicas[best_idx].full_path.display()
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
            "INDEX DISAGREEMENT {}/{}/{}:\n{}",
            fragment,
            object_key,
            version_id,
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
    blob_roots: &Vec<PathBuf>,
    num_disks: usize,
    cfg: RepairRunConfig,
    processed_versions: Arc<Mutex<HashSet<String>>>,
    shard_repair_sem: Arc<Semaphore>,
) -> Result<BatchDelta, AppError> {
    let mut delta = BatchDelta::default();
    if should_stop() {
        return Ok(delta);
    }

    delta.scanned = 1;

    let data = match fs::read(&meta_path).await {
        Ok(d) => d,
        Err(_) => return Ok(delta),
    };

    let meta = match rmp_serde::from_slice::<ObjectMetadata>(&data) {
        Ok(m) => m,
        Err(_) => {
            delta.corrupt_found += 1;
            return Ok(delta);
        }
    };

    if meta.is_delete_marker {
        return Ok(delta);
    }

    {
        let mut set = processed_versions.lock().unwrap();
        if set.contains(&meta.version_id) {
            return Ok(delta);
        }
        set.insert(meta.version_id.clone());
    }

    let (_canonical_data, canonical_meta, miss_rep, div_rep, report) =
        reconcile_object_metadata_strict(
            &meta.fragment,
            &meta.object_key,
            &meta.version_id,
            cfg.dry_run,
        )
        .await?;

    delta.missing_meta_repaired += miss_rep;
    delta.divergent_meta_repaired += div_rep;
    if let Some(r) = report {
        delta.last_disagreement = Some(r.clone());
        if cfg.dry_run {
            warn!("{}", r);
        }
    }

    let (repaired, corrupt, last_rep) =
        process_object_repair(canonical_meta, blob_roots, num_disks, cfg, shard_repair_sem).await?;

    delta.repaired_shards += repaired as u64;
    delta.corrupt_found += corrupt as u64;
    delta.last_repaired = last_rep;

    Ok(delta)
}

async fn repair_fragment_quota(
    fragment_hash: &str,
    source_data: &[u8],
    dry_run: bool,
) -> Result<(), AppError> {
    if rmp_serde::from_slice::<Quota>(source_data).is_err() {
        return Ok(());
    }
    let path_manager = ObjectPathManager::new();
    let all_quota_paths = path_manager.get_all_quota_paths(fragment_hash);

    for target_path in all_quota_paths {
        let mut needs_repair = false;

        if let Some(parent) = target_path.parent() {
            if !parent.exists() {
                if !dry_run {
                    if fs::create_dir_all(parent).await.is_err() {
                        continue;
                    }
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
            if fs::write(&tmp, source_data).await.is_ok() {
                let _ = fs::rename(&tmp, &target_path).await;
                info!("Repaired quota file at {:?}", target_path);
            }
        }
    }
    Ok(())
}

async fn process_object_repair(
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

        let part_fixed = normalize_part_paths_for_repair(
            &path_manager,
            blob_roots,
            &meta_arc.fragment,
            &meta_arc.version_id,
            part.clone(),
        )
        .await;

        let part_arc = Arc::new(part_fixed);
        let mut missing_shards: Vec<usize> = Vec::new();

        for shard in &part_arc.shards {
            let shard_path = PathBuf::from(strip_tmp_attempt_suffix(&shard.absolute_path));

            let mut healthy = false;

            match fs::metadata(&shard_path).await {
                Ok(m) => {
                    if cfg.check_mode == RepairCheckMode::Availability {
                        healthy = m.len() == shard.size_bytes;
                        if !healthy {
                            corrupt_found += 1;
                            warn!("Size mismatch shard (availability): {:?}", shard_path);
                        }
                    } else {
                        if let Ok(mut f) = File::open(&shard_path).await {
                            healthy = verify_shard_integrity(&mut f, &shard.checksum).await;
                            if !healthy {
                                corrupt_found += 1;
                                warn!("Corrupt shard found: {:?}", shard_path);
                            }
                        }
                    }
                }
                Err(_) => {
                    healthy = false;
                }
            }

            if !healthy {
                missing_shards.push(shard.shard_index);
            }
        }

        if missing_shards.is_empty() || cfg.dry_run {
            continue;
        }

        let repairs = stream::iter(missing_shards.into_iter())
            .map(|missing_idx| {
                let part_for_task = part_arc.clone();
                let sem = shard_repair_sem.clone();
                let stripe = meta_arc.stripe_size_bytes;

                async move {
                    let _permit = sem.acquire().await.map_err(|_| {
                        AppError::InternalServerError("Repair semaphore closed".into())
                    })?;
                    repair_entire_shard(part_for_task, missing_idx, stripe).await
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
                    warn!("Shard repair failed (disk might be offline): {}", e);
                }
            }
        }
    }

    Ok((repaired_total, corrupt_found, last_repaired))
}
