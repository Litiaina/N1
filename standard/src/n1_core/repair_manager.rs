use crate::enums::app_error_enum::AppError;
use crate::n1_core::constants::{
    FRAGMENT_METADATA_FILE, METADATA_EXT, PARITY_SUFFIX, PART_META_SUFFIX, RESERVATION_FILE,
    SESSION_MANIFEST_FILE, SHARD_SUFFIX,
};
use crate::n1_core::disk_manager::{
    INDEX_STORE_DISKS, ObjectMetadata, ObjectPathManager, PartMetadata,
};
use crate::n1_core::operation_gate::{MaintenanceAccessMode, acquire_maintenance_mode};
use crate::n1_core::recovery::{repair_entire_shard, verify_shard_integrity};
use crate::n1_core::upload::UploadSessionManifest;
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
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant, SystemTime};
use tokio::fs::{self, File};
use tokio::sync::{RwLock, Semaphore};
use tracing::{error, info, warn};
use ulid::Ulid;
use walkdir::WalkDir;

#[derive(Debug, Clone, Serialize, Default)]
pub struct RepairJobStatus {
    pub is_running: bool,
    pub phase: String,
    pub fast_mode: bool,

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
    Checksum,
    Strict,
    Scrub,
}

impl RepairCheckMode {
    fn maintenance_access_mode(self) -> MaintenanceAccessMode {
        match self {
            Self::Availability => MaintenanceAccessMode::Availability,
            Self::Checksum | Self::Strict => MaintenanceAccessMode::ReadOnly,
            Self::Scrub => MaintenanceAccessMode::Quiesced,
        }
    }

    fn verifies_full_integrity(self) -> bool {
        !matches!(self, Self::Availability)
    }
}

#[derive(Clone, Debug)]
pub struct RepairRunConfig {
    pub dry_run: bool,
    pub target_fragment: Option<String>,
    pub check_mode: RepairCheckMode,
    pub meta_concurrency: usize,
    pub shard_repair_concurrency: usize,
    pub meta_batch_size: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct RepairRunOptions {
    pub fast_mode: bool,
    pub access_mode: MaintenanceAccessMode,
}

impl Default for RepairRunOptions {
    fn default() -> Self {
        Self {
            fast_mode: false,
            access_mode: MaintenanceAccessMode::Availability,
        }
    }
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
    let normalized_mode = params
        .get("mode")
        .or_else(|| params.get("check_mode"))
        .or_else(|| params.get("execution_mode"))
        .map(|s| s.trim().to_ascii_lowercase());

    match normalized_mode.as_deref() {
        Some("scrub") | Some("quiesced") | Some("offline") => return RepairCheckMode::Scrub,
        Some("strict") | Some("readonly") | Some("read_only") => return RepairCheckMode::Strict,
        Some("checksum") => return RepairCheckMode::Checksum,
        _ => {}
    }

    let normalized_access_mode = params
        .get("access_mode")
        .map(|s| s.trim().to_ascii_lowercase());
    match normalized_access_mode.as_deref() {
        Some("scrub") | Some("quiesced") | Some("offline") => RepairCheckMode::Scrub,
        Some("strict") | Some("readonly") | Some("read_only") => RepairCheckMode::Strict,
        _ => RepairCheckMode::Availability,
    }
}

fn parse_bool_param(params: &HashMap<String, String>, key: &str, default: bool) -> bool {
    match params.get(key).map(|s| s.as_str()) {
        Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("YES") | Some("on")
        | Some("ON") => true,
        Some("0") | Some("false") | Some("FALSE") | Some("no") | Some("NO") | Some("off")
        | Some("OFF") => false,
        Some(_) => default,
        None => default,
    }
}

fn detect_total_ram_gib() -> usize {
    let content = match std::fs::read_to_string("/proc/meminfo") {
        Ok(v) => v,
        Err(_) => return 8,
    };

    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            if let Some(kib) = rest
                .split_whitespace()
                .next()
                .and_then(|v| v.parse::<u64>().ok())
            {
                return ((kib / (1024 * 1024)) as usize).max(1);
            }
        }
    }

    8
}

fn repair_limits_for_ram_gib(ram_gib: usize) -> (usize, usize, usize) {
    match ram_gib {
        0..=4 => (8, 2, 256),
        5..=8 => (16, 4, 512),
        9..=16 => (24, 8, 1024),
        17..=32 => (32, 12, 2048),
        _ => (64, 16, 4096),
    }
}

fn repair_defaults_for_runtime(
    check_mode: RepairCheckMode,
    fast_mode: bool,
    ram_gib: usize,
    cpu_cores: usize,
) -> (usize, usize, usize) {
    let (mut meta, mut shard, mut batch) = match check_mode {
        RepairCheckMode::Availability => {
            let meta = if fast_mode {
                cpu_cores.clamp(2, 6)
            } else {
                (cpu_cores / 2).clamp(1, 4)
            };
            let shard = if fast_mode { 2 } else { 1 };
            let batch = if fast_mode { 192 } else { 128 };
            (meta, shard, batch)
        }
        RepairCheckMode::Checksum | RepairCheckMode::Strict => {
            let meta = if fast_mode {
                cpu_cores.saturating_mul(2).clamp(4, 16)
            } else {
                cpu_cores.clamp(2, 10)
            };
            let shard = if fast_mode {
                cpu_cores.clamp(2, 8)
            } else {
                (cpu_cores / 2).clamp(1, 4)
            };
            let batch = if fast_mode { 768 } else { 384 };
            (meta, shard, batch)
        }
        RepairCheckMode::Scrub => {
            let meta = if fast_mode {
                cpu_cores.saturating_mul(3).clamp(6, 24)
            } else {
                cpu_cores.saturating_mul(2).clamp(4, 16)
            };
            let shard = if fast_mode {
                cpu_cores.saturating_mul(2).clamp(2, 12)
            } else {
                cpu_cores.clamp(2, 6)
            };
            let batch = if fast_mode { 1024 } else { 512 };
            (meta, shard, batch)
        }
    };

    match ram_gib {
        0..=2 => {
            meta = meta.min(2);
            shard = shard.min(1);
            batch = batch.min(64);
        }
        3..=4 => {
            meta = meta.min(4);
            shard = shard.min(1);
            batch = batch.min(128);
        }
        5..=8 => {
            meta = meta.min(6);
            shard = shard.min(2);
            batch = batch.min(192);
        }
        9..=16 => {
            meta = meta.min(8);
            shard = shard.min(3);
            batch = batch.min(256);
        }
        17..=32 => {
            meta = meta.min(12);
            shard = shard.min(4);
            batch = batch.min(384);
        }
        _ => {}
    }

    let (max_meta, max_shard, max_batch) = repair_limits_for_ram_gib(ram_gib);
    (
        clamp_usize(meta, 1, max_meta),
        clamp_usize(shard, 1, max_shard),
        clamp_usize(batch, 32, max_batch),
    )
}

fn normalize_repair_cfg_for_layout(
    mut cfg: RepairRunConfig,
    blob_disks: usize,
    index_roots: usize,
) -> RepairRunConfig {
    let blob_width = blob_disks.max(1);
    let index_width = index_roots.max(1);
    let io_width = blob_width.min(index_width.saturating_mul(2)).max(1);

    let (meta_cap, shard_cap) = match cfg.check_mode {
        RepairCheckMode::Availability => (io_width.clamp(1, 4), blob_width.clamp(1, 2)),
        RepairCheckMode::Checksum | RepairCheckMode::Strict => (
            io_width.saturating_mul(2).clamp(2, 16),
            blob_width.saturating_mul(2).clamp(1, 8),
        ),
        RepairCheckMode::Scrub => (
            io_width.saturating_mul(4).clamp(4, 24),
            blob_width.saturating_mul(4).clamp(1, 12),
        ),
    };

    cfg.meta_concurrency = cfg.meta_concurrency.min(meta_cap).max(1);
    cfg.shard_repair_concurrency = cfg.shard_repair_concurrency.min(shard_cap).max(1);
    cfg
}

fn availability_pacing_delay(check_mode: RepairCheckMode, fast_mode: bool) -> Option<Duration> {
    if check_mode != RepairCheckMode::Availability {
        return None;
    }

    Some(if fast_mode {
        Duration::from_millis(1)
    } else {
        Duration::from_millis(3)
    })
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
    Ok(Json(REPAIR_STATUS.read().await.clone()))
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

    let fast_mode = parse_bool_param(&params, "fast_mode", false);
    let read_only_fast = parse_bool_param(&params, "read_only_fast", false);
    let ram_gib = detect_total_ram_gib();
    let cpu_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let check_mode = parse_check_mode(&params);
    let (default_meta_conc, default_repair_conc, default_batch) =
        repair_defaults_for_runtime(check_mode, fast_mode, ram_gib, cpu_cores);
    let (max_meta_conc, max_repair_conc, max_batch_size) = repair_limits_for_ram_gib(ram_gib);
    let mut access_mode = check_mode.maintenance_access_mode();
    if read_only_fast && access_mode == MaintenanceAccessMode::Availability {
        access_mode = MaintenanceAccessMode::ReadOnly;
    }

    let cfg = RepairRunConfig {
        dry_run: parse_bool_param(&params, "dry_run", false),
        target_fragment: params
            .get("fragment")
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty()),
        check_mode,
        meta_concurrency: clamp_usize(
            params
                .get("meta_concurrency")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(default_meta_conc),
            1,
            max_meta_conc,
        ),
        shard_repair_concurrency: clamp_usize(
            params
                .get("repair_concurrency")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(default_repair_conc),
            1,
            max_repair_conc,
        ),
        meta_batch_size: clamp_usize(
            params
                .get("batch")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(default_batch),
            32,
            max_batch_size,
        ),
    };
    info!(
        "Repair tuning: ram_gib={} cpu_cores={} fast_mode={} access_mode={:?} mode={:?} meta_conc={} repair_conc={} batch={} (caps: meta<= {}, repair<= {}, batch<= {})",
        ram_gib,
        cpu_cores,
        fast_mode,
        access_mode,
        cfg.check_mode,
        cfg.meta_concurrency,
        cfg.shard_repair_concurrency,
        cfg.meta_batch_size,
        max_meta_conc,
        max_repair_conc,
        max_batch_size
    );
    let run_opts = RepairRunOptions {
        fast_mode,
        access_mode,
    };

    let ws_manager = Arc::new(state.ws_manager.clone());
    let ws_manager_clone = ws_manager.clone();

    tokio::spawn(async move {
        if let Err(e) = run_repair_job_with_options(cfg, run_opts, ws_manager.clone()).await {
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

async fn list_all_fragments(index_roots: &[PathBuf]) -> Vec<String> {
    let roots = index_roots.to_vec();
    tokio::task::spawn_blocking(move || {
        let mut out = HashSet::new();
        for root in roots {
            if !root.exists() {
                continue;
            }
            if let Ok(rd) = std::fs::read_dir(&root) {
                for ent in rd.filter_map(|e| e.ok()) {
                    if ent.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                        if let Some(name) = ent.file_name().to_str() {
                            if !name.starts_with('.') {
                                out.insert(name.to_string());
                            }
                        }
                    }
                }
            }
        }
        let mut v: Vec<String> = out.into_iter().collect();
        v.sort();
        v
    })
    .await
    .unwrap_or_default()
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
    let parent = path.parent().unwrap_or(Path::new("."));
    let _ = fs::create_dir_all(parent).await;
    let tmp = parent.join(format!(
        ".{}.tmp.{}",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("file"),
        Ulid::new().to_string()
    ));
    fs::write(&tmp, data).await?;
    fs::rename(&tmp, path).await?;
    Ok(())
}

#[derive(Clone)]
struct RawReplica {
    bytes: Option<Vec<u8>>,
    mtime: SystemTime,
    len: u64,
    sha256: Option<[u8; 32]>,
    valid: bool,
}

async fn read_raw_replica<F>(full: &Path, valid_fn: &F) -> RawReplica
where
    F: Fn(&[u8]) -> bool + Send + Sync,
{
    match fs::read(full).await {
        Ok(bytes) => {
            let meta = fs::metadata(full).await.ok();
            let mtime = meta
                .as_ref()
                .and_then(|m| m.modified().ok())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            let len = meta.as_ref().map(|m| m.len()).unwrap_or(bytes.len() as u64);
            let sha = Some(sha256_bytes(&bytes));
            let valid = valid_fn(&bytes);

            RawReplica {
                bytes: Some(bytes),
                mtime,
                len,
                sha256: sha,
                valid,
            }
        }
        Err(_) => RawReplica {
            bytes: None,
            mtime: SystemTime::UNIX_EPOCH,
            len: 0,
            sha256: None,
            valid: false,
        },
    }
}

fn choose_best_raw_replica(replicas: &[RawReplica]) -> Option<usize> {
    let mut best: Option<usize> = None;
    for (i, r) in replicas.iter().enumerate() {
        if !r.valid {
            continue;
        }
        let take = match best {
            None => true,
            Some(cur) => {
                let c = &replicas[cur];
                (r.mtime > c.mtime)
                    || (r.mtime == c.mtime && r.len > c.len)
                    || (r.mtime == c.mtime
                        && r.len == c.len
                        && r.sha256.unwrap_or([0u8; 32]) > c.sha256.unwrap_or([0u8; 32]))
            }
        };
        if take {
            best = Some(i);
        }
    }
    best
}

async fn replicate_bytes_strict(
    paths: &[PathBuf],
    canonical: &[u8],
    dry_run: bool,
) -> Result<(u64, u64), AppError> {
    let canon_sha = sha256_bytes(canonical);
    let mut missing = 0u64;
    let mut divergent = 0u64;

    for p in paths {
        match fs::read(p).await {
            Ok(b) => {
                if sha256_bytes(&b) != canon_sha {
                    divergent += 1;
                    if !dry_run {
                        atomic_write(p, canonical)
                            .await
                            .map_err(|e| AppError::Io(e, "repair mirror file".into()))?;
                    }
                }
            }
            Err(_) => {
                missing += 1;
                if !dry_run {
                    atomic_write(p, canonical)
                        .await
                        .map_err(|e| AppError::Io(e, "repair mirror file".into()))?;
                }
            }
        }
    }

    Ok((missing, divergent))
}

async fn repair_fragment_metadata_mirrors(
    index_roots: &[PathBuf],
    fragment: &str,
    dry_run: bool,
) -> Result<(u64, u64, u64), AppError> {
    let paths: Vec<PathBuf> = index_roots
        .iter()
        .map(|r| r.join(fragment).join(FRAGMENT_METADATA_FILE))
        .collect();

    let valid = |b: &[u8]| -> bool { rmp_serde::from_slice::<serde_json::Value>(b).is_ok() };

    let mut reps = Vec::with_capacity(paths.len());
    for p in &paths {
        reps.push(read_raw_replica(p, &valid).await);
    }

    let Some(best) = choose_best_raw_replica(&reps) else {
        let any_exists = reps.iter().any(|r| r.bytes.is_some());
        let corrupt = if any_exists { 1 } else { 0 };
        return Ok((0, 0, corrupt));
    };

    let canon = reps[best].bytes.clone().unwrap();
    let (m, d) = replicate_bytes_strict(&paths, &canon, dry_run).await?;
    Ok((m, d, 0))
}

async fn list_all_session_versions(index_roots: &[PathBuf], fragment: &str) -> Vec<String> {
    let mut out = HashSet::<String>::new();

    for root in index_roots {
        let partitions_base = root.join(fragment).join("index").join("partitions");
        if !fs::try_exists(&partitions_base).await.unwrap_or(false) {
            continue;
        }

        let mut partitions = match fs::read_dir(&partitions_base).await {
            Ok(r) => r,
            Err(_) => continue,
        };

        while let Ok(Some(part)) = partitions.next_entry().await {
            if !part
                .file_type()
                .await
                .map(|ft| ft.is_dir())
                .unwrap_or(false)
            {
                continue;
            }

            let sessions_dir = part.path().join("sessions");
            if !fs::try_exists(&sessions_dir).await.unwrap_or(false) {
                continue;
            }

            let mut rd = match fs::read_dir(&sessions_dir).await {
                Ok(r) => r,
                Err(_) => continue,
            };
            while let Ok(Some(ent)) = rd.next_entry().await {
                let name = ent.file_name().to_string_lossy().to_string();
                if ent.file_type().await.map(|ft| ft.is_dir()).unwrap_or(false) {
                    if !name.is_empty() {
                        out.insert(name);
                    }
                    continue;
                }
                if let Some(vid) = name.strip_suffix(".session.msgpack") {
                    if !vid.is_empty() {
                        out.insert(vid.to_string());
                    }
                }
            }
        }
    }

    let mut v: Vec<String> = out.into_iter().collect();
    v.sort();
    v
}

async fn list_part_meta_names(session_dirs: &[PathBuf]) -> HashSet<String> {
    let mut out = HashSet::<String>::new();

    for d in session_dirs {
        if !fs::try_exists(d).await.unwrap_or(false) {
            continue;
        }

        let mut rd = match fs::read_dir(d).await {
            Ok(r) => r,
            Err(_) => continue,
        };

        while let Ok(Some(ent)) = rd.next_entry().await {
            let name = ent.file_name().to_string_lossy().to_string();
            if name.ends_with(PART_META_SUFFIX) && name.starts_with("part_") {
                out.insert(name);
            }
        }
    }

    out
}

async fn repair_fragment_sessions_strict(
    fragment: &str,
    index_roots: &[PathBuf],
    dry_run: bool,
) -> Result<(u64, u64, u64), AppError> {
    let pm = ObjectPathManager::new();
    let versions = list_all_session_versions(index_roots, fragment).await;

    let mut miss = 0u64;
    let mut div = 0u64;
    let mut corrupt = 0u64;

    for version_id in versions {
        if should_stop() {
            break;
        }

        let session_dirs = pm.get_all_session_dirs(fragment, &version_id);
        for d in &session_dirs {
            let _ = fs::create_dir_all(d).await;
        }

        let manifest_paths: Vec<PathBuf> = session_dirs
            .iter()
            .map(|d| d.join(SESSION_MANIFEST_FILE))
            .collect();

        let manifest_valid = |b: &[u8]| -> bool {
            if let Ok(m) = rmp_serde::from_slice::<UploadSessionManifest>(b) {
                m.fragment == fragment && m.version_id == version_id
            } else {
                false
            }
        };

        let mut reps = Vec::with_capacity(manifest_paths.len());
        for p in &manifest_paths {
            reps.push(read_raw_replica(p, &manifest_valid).await);
        }

        let Some(best) = choose_best_raw_replica(&reps) else {
            let any_exists = reps.iter().any(|r| r.bytes.is_some());
            if any_exists {
                corrupt += 1;
            }
            continue;
        };

        let canon_manifest = reps[best].bytes.clone().unwrap();
        let (m1, d1) = replicate_bytes_strict(&manifest_paths, &canon_manifest, dry_run).await?;
        miss += m1;
        div += d1;

        let receipt_paths: Vec<PathBuf> = session_dirs
            .iter()
            .map(|d| d.join(RESERVATION_FILE))
            .collect();
        let receipt_valid = |b: &[u8]| -> bool {
            rmp_serde::from_slice::<crate::n1_core::disk_manager::ReservationReceipt>(b).is_ok()
        };

        let mut reps = Vec::with_capacity(receipt_paths.len());
        for p in &receipt_paths {
            reps.push(read_raw_replica(p, &receipt_valid).await);
        }

        if let Some(best) = choose_best_raw_replica(&reps) {
            let canon_receipt = reps[best].bytes.clone().unwrap();
            let (m2, d2) = replicate_bytes_strict(&receipt_paths, &canon_receipt, dry_run).await?;
            miss += m2;
            div += d2;
        }

        let part_names = list_part_meta_names(&session_dirs).await;
        for name in part_names {
            if should_stop() {
                break;
            }

            let paths: Vec<PathBuf> = session_dirs.iter().map(|d| d.join(&name)).collect();
            let part_valid =
                |b: &[u8]| -> bool { rmp_serde::from_slice::<PartMetadata>(b).is_ok() };

            let mut reps = Vec::with_capacity(paths.len());
            for p in &paths {
                reps.push(read_raw_replica(p, &part_valid).await);
            }

            let Some(best) = choose_best_raw_replica(&reps) else {
                let any_exists = reps.iter().any(|r| r.bytes.is_some());
                if any_exists {
                    corrupt += 1;
                }
                continue;
            };

            let canon_part = reps[best].bytes.clone().unwrap();
            let (m3, d3) = replicate_bytes_strict(&paths, &canon_part, dry_run).await?;
            miss += m3;
            div += d3;
        }
    }

    Ok((miss, div, corrupt))
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

async fn repair_fragment_legacy_object_metadata_mirrors(
    index_roots: &[PathBuf],
    fragment: &str,
    dry_run: bool,
) -> Result<(u64, u64, u64), AppError> {
    let roots = index_roots.to_vec();
    let fragment_owned = fragment.to_string();
    let rel_paths: Vec<String> = tokio::task::spawn_blocking(move || {
        let mut out: HashSet<String> = HashSet::new();
        for root in roots {
            let frag_root = root.join(&fragment_owned);
            let index_dir = frag_root.join("index");
            if !index_dir.exists() {
                continue;
            }
            let walker = WalkDir::new(index_dir).into_iter();
            for entry in walker.filter_map(|e| e.ok()) {
                if !entry.file_type().is_file() {
                    continue;
                }
                let p = entry.path();
                if !should_scan_object_metadata_file(p) {
                    continue;
                }
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                // Legacy object metadata filename: {key_hash}#{version}.msgpack
                if !name.contains('#') {
                    continue;
                }
                let rel = match p.strip_prefix(&frag_root) {
                    Ok(v) => v.to_string_lossy().replace('\\', "/"),
                    Err(_) => continue,
                };
                out.insert(rel);
            }
        }
        let mut v: Vec<String> = out.into_iter().collect();
        v.sort();
        v
    })
    .await
    .unwrap_or_default();

    let valid = |b: &[u8]| -> bool { rmp_serde::from_slice::<ObjectMetadata>(b).is_ok() };

    let mut missing = 0u64;
    let mut divergent = 0u64;
    let mut corrupt = 0u64;

    for rel in rel_paths {
        let paths: Vec<PathBuf> = index_roots
            .iter()
            .map(|root| root.join(fragment).join(&rel))
            .collect();

        let mut reps = Vec::with_capacity(paths.len());
        for p in &paths {
            reps.push(read_raw_replica(p, &valid).await);
        }

        let Some(best) = choose_best_raw_replica(&reps) else {
            let any_exists = reps.iter().any(|r| r.bytes.is_some());
            if any_exists {
                corrupt += 1;
            }
            continue;
        };

        let canon = reps[best].bytes.clone().unwrap_or_default();
        if canon.is_empty() {
            continue;
        }
        let (m, d) = replicate_bytes_strict(&paths, &canon, dry_run).await?;
        missing += m;
        divergent += d;
    }

    Ok((missing, divergent, corrupt))
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
                    if !cfg.check_mode.verifies_full_integrity() {
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

        let mut repairs = stream::iter(missing_shards.into_iter())
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
            .buffer_unordered(cfg.shard_repair_concurrency);

        while let Some(r) = repairs.next().await {
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

fn should_scan_object_metadata_file(path: &Path) -> bool {
    let name = match path.file_name().and_then(|n| n.to_str()) {
        Some(s) => s,
        None => return false,
    };

    if !name.ends_with(METADATA_EXT) {
        return false;
    }

    if name.ends_with(PART_META_SUFFIX) {
        return false;
    }

    if name == RESERVATION_FILE || name == SESSION_MANIFEST_FILE {
        return false;
    }

    if name == "latest.msgpack" {
        return false;
    }

    let p = path.to_string_lossy();
    if p.contains("/sessions/") || p.contains("\\sessions\\") {
        return false;
    }

    true
}

async fn fragment_has_legacy_object_metadata(index_roots: &[PathBuf], fragment: &str) -> bool {
    let roots = index_roots.to_vec();
    let frag = fragment.to_string();
    tokio::task::spawn_blocking(move || {
        for root in roots {
            let index_dir = root.join(&frag).join("index");
            if !index_dir.exists() {
                continue;
            }
            for entry in WalkDir::new(index_dir).into_iter().filter_map(|e| e.ok()) {
                if !entry.file_type().is_file() {
                    continue;
                }
                let p = entry.path();
                if !should_scan_object_metadata_file(p) {
                    continue;
                }
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name.contains('#') {
                    return true;
                }
            }
        }
        false
    })
    .await
    .unwrap_or(false)
}

fn object_version_identity_from_meta_path(path: &Path) -> Option<(String, String)> {
    let filename = path.file_name()?.to_str()?;
    if !filename.ends_with(METADATA_EXT) {
        return None;
    }

    // Legacy layout:
    // index/{h1}/{h2}/{h3}/{h4}/{sha256(object_key)}#{version}.msgpack
    if let Some(hash_sep) = filename.find('#') {
        let key_hash = &filename[..hash_sep];
        let version_with_ext = &filename[hash_sep + 1..];
        if key_hash.len() == 64
            && version_with_ext.len() > METADATA_EXT.len()
            && version_with_ext.ends_with(METADATA_EXT)
        {
            let version_id = &version_with_ext[..version_with_ext.len() - METADATA_EXT.len()];
            return Some((key_hash.to_string(), version_id.to_string()));
        }
    }

    // Partitioned layout:
    // index/partitions/{pid}/{objects|trash}/{hh}/{key_hash}/{version}.msgpack
    let mut rev = path.components().rev();
    let file_component = rev.next()?.as_os_str().to_str()?;
    let key_hash = rev.next()?.as_os_str().to_str()?;
    let _hh = rev.next()?.as_os_str().to_str()?;
    let kind = rev.next()?.as_os_str().to_str()?;
    let partitions = rev.next()?.as_os_str().to_str()?;
    let index = rev.next()?.as_os_str().to_str()?;

    if index != "index" || partitions != "partitions" {
        return None;
    }
    if kind != "objects" && kind != "trash" {
        return None;
    }
    if key_hash.len() != 64 {
        return None;
    }
    if file_component == "latest.msgpack" || !file_component.ends_with(METADATA_EXT) {
        return None;
    }

    let version_id = &file_component[..file_component.len() - METADATA_EXT.len()];
    if version_id.is_empty() {
        return None;
    }
    Some((key_hash.to_string(), version_id.to_string()))
}

#[cfg(test)]
pub async fn run_repair_job(
    cfg: RepairRunConfig,
    ws_manager: Arc<WsConnectionManager>,
) -> Result<(), AppError> {
    let run_opts = RepairRunOptions {
        fast_mode: false,
        access_mode: cfg.check_mode.maintenance_access_mode(),
    };
    run_repair_job_with_options(cfg, run_opts, ws_manager).await
}

pub async fn run_repair_job_with_options(
    cfg: RepairRunConfig,
    opts: RepairRunOptions,
    ws_manager: Arc<WsConnectionManager>,
) -> Result<(), AppError> {
    STOP_SIGNAL.store(false, Ordering::Relaxed);
    let start_time = Instant::now();
    let effective_access_mode = opts
        .access_mode
        .max(cfg.check_mode.maintenance_access_mode());
    if effective_access_mode != opts.access_mode {
        info!(
            "Repair mode upgraded maintenance gate from {:?} to {:?} based on check_mode={:?}",
            opts.access_mode, effective_access_mode, cfg.check_mode
        );
    }
    let _write_gate = acquire_maintenance_mode(effective_access_mode);
    let batch_pacing = availability_pacing_delay(cfg.check_mode, opts.fast_mode);

    {
        let mut status = REPAIR_STATUS.write().await;
        status.is_running = true;
        status.fast_mode = opts.fast_mode;
        status.phase = format!(
            "Initializing (access_mode={:?}, mode={:?}, meta_conc={}, repair_conc={}, batch={}, dry_run={}, fast_mode={})",
            effective_access_mode,
            cfg.check_mode,
            cfg.meta_concurrency,
            cfg.shard_repair_concurrency,
            cfg.meta_batch_size,
            cfg.dry_run,
            opts.fast_mode
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

    let index_roots: Vec<PathBuf> = path_manager.get_all_index_roots().into_iter().collect();
    let blob_roots = Arc::new(path_manager.get_blob_roots());
    let num_disks = blob_roots.len();
    let cfg = normalize_repair_cfg_for_layout(cfg, num_disks, index_roots.len());
    info!(
        "Repair runtime caps after layout normalization: mode={:?} meta_conc={} repair_conc={} batch={} blob_disks={} index_roots={}",
        cfg.check_mode,
        cfg.meta_concurrency,
        cfg.shard_repair_concurrency,
        cfg.meta_batch_size,
        num_disks,
        index_roots.len()
    );

    let shard_repair_sem = Arc::new(Semaphore::new(cfg.shard_repair_concurrency));
    let fragments = match cfg.target_fragment.as_ref() {
        Some(fragment) => vec![fragment.clone()],
        None => list_all_fragments(&index_roots).await,
    };

    info!(
        "Starting Storage Repair Job (scope={}, fragments={}, index_roots={}). mode={:?}, dry_run={}",
        if cfg.target_fragment.is_some() {
            "single-fragment"
        } else {
            "all-fragments"
        },
        fragments.len(),
        index_roots.len(),
        cfg.check_mode,
        cfg.dry_run
    );

    for fragment_hash in fragments {
        if should_stop() {
            break;
        }

        {
            let mut status = REPAIR_STATUS.write().await;
            status.current_fragment = fragment_hash.clone();
            status.phase = format!("Repairing Fragment {}", fragment_hash);
            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }

        {
            let mut status = REPAIR_STATUS.write().await;
            status.phase = format!("Repairing fragment metadata {}", fragment_hash);
            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }

        let (fmiss, fdiv, fcorrupt) =
            repair_fragment_metadata_mirrors(&index_roots, &fragment_hash, cfg.dry_run).await?;

        {
            let mut status = REPAIR_STATUS.write().await;
            status.missing_meta_repaired += fmiss;
            status.divergent_meta_repaired += fdiv;
            status.corrupt_found += fcorrupt;
            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }

        {
            let mut status = REPAIR_STATUS.write().await;
            status.phase = format!("Repairing upload sessions {}", fragment_hash);
            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }

        let (smiss, sdiv, scorrupt) =
            repair_fragment_sessions_strict(&fragment_hash, &index_roots, cfg.dry_run).await?;

        {
            let mut status = REPAIR_STATUS.write().await;
            status.missing_meta_repaired += smiss;
            status.divergent_meta_repaired += sdiv;
            status.corrupt_found += scorrupt;
            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }

        let mut scan_index_dirs: Vec<PathBuf> = Vec::new();
        for root in &index_roots {
            let idx = root.join(&fragment_hash).join("index");
            let partitions = idx.join("partitions");
            if fs::try_exists(&partitions).await.unwrap_or(false) {
                scan_index_dirs.push(partitions);
            } else if fs::try_exists(&idx).await.unwrap_or(false) {
                scan_index_dirs.push(idx);
            }
        }
        if scan_index_dirs.is_empty() {
            warn!(
                "No readable index dirs for fragment {}, skipping",
                fragment_hash
            );
            continue;
        }

        if fragment_has_legacy_object_metadata(&index_roots, &fragment_hash).await {
            {
                let mut status = REPAIR_STATUS.write().await;
                status.phase = format!(
                    "Reconciling legacy object metadata mirrors {}",
                    fragment_hash
                );
                ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
            }

            let (omiss, odiv, ocorrupt) = repair_fragment_legacy_object_metadata_mirrors(
                &index_roots,
                &fragment_hash,
                cfg.dry_run,
            )
            .await?;

            {
                let mut status = REPAIR_STATUS.write().await;
                status.missing_meta_repaired += omiss;
                status.divergent_meta_repaired += odiv;
                status.corrupt_found += ocorrupt;
                ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
            }
        }

        let (tx, mut rx) = tokio::sync::mpsc::channel::<PathBuf>(1024);
        let scan_dirs_clone = scan_index_dirs.clone();

        tokio::task::spawn_blocking(move || {
            for scan_dir in scan_dirs_clone {
                let walker = WalkDir::new(scan_dir).into_iter();
                for entry in walker.filter_map(|e| e.ok()) {
                    if !entry.file_type().is_file() {
                        continue;
                    }
                    let p = entry.path();
                    if should_scan_object_metadata_file(p) {
                        if tx.blocking_send(p.to_owned()).is_err() {
                            return;
                        }
                    }
                }
            }
        });

        let mut pending: Vec<PathBuf> = Vec::with_capacity(cfg.meta_batch_size);
        let mut fragment_touched = false;

        while let Some(meta_path) = rx.recv().await {
            if should_stop() {
                break;
            }

            pending.push(meta_path);

            if pending.len() >= cfg.meta_batch_size {
                let d = apply_meta_batch(
                    &pending,
                    blob_roots.clone(),
                    num_disks,
                    cfg.clone(),
                    shard_repair_sem.clone(),
                    batch_pacing,
                    ws_manager.clone(),
                    start_time,
                )
                .await?;

                if d.repaired_shards > 0
                    || d.missing_meta_repaired > 0
                    || d.divergent_meta_repaired > 0
                {
                    fragment_touched = true;
                }

                pending.clear();
            }
        }

        if !pending.is_empty() {
            let d = apply_meta_batch(
                &pending,
                blob_roots.clone(),
                num_disks,
                cfg.clone(),
                shard_repair_sem.clone(),
                batch_pacing,
                ws_manager.clone(),
                start_time,
            )
            .await?;

            if d.repaired_shards > 0 || d.missing_meta_repaired > 0 || d.divergent_meta_repaired > 0
            {
                fragment_touched = true;
            }

            pending.clear();
        }

        let _ = fragment_touched;
    }

    {
        let mut status = REPAIR_STATUS.write().await;
        status.is_running = false;
        status.fast_mode = opts.fast_mode;
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
    blob_roots: Arc<Vec<PathBuf>>,
    num_disks: usize,
    cfg: RepairRunConfig,
    shard_repair_sem: Arc<Semaphore>,
    batch_pacing: Option<Duration>,
    ws_manager: Arc<WsConnectionManager>,
    start_time: Instant,
) -> Result<BatchDelta, AppError> {
    if batch.is_empty() {
        return Ok(BatchDelta::default());
    }

    // Multiple index mirrors (and legacy + partitioned metadata paths) can surface
    // the same object-version many times in one batch. Dedupe first to avoid
    // concurrent reconcile races on the same metadata tuple.
    let mut seen_object_versions: HashSet<String> = HashSet::with_capacity(batch.len());
    let mut deduped_batch: Vec<PathBuf> = Vec::with_capacity(batch.len());
    for meta_path in batch {
        if let Some((key_hash, version_id)) = object_version_identity_from_meta_path(meta_path) {
            let dedupe_key = format!("{}#{}", key_hash, version_id);
            if !seen_object_versions.insert(dedupe_key) {
                continue;
            }
        }
        deduped_batch.push(meta_path.clone());
    }

    let mut stream = stream::iter(deduped_batch.into_iter())
        .map(|meta_path| {
            let blob_roots = blob_roots.clone();
            let shard_repair_sem = shard_repair_sem.clone();
            let cfg = cfg.clone();

            async move {
                process_one_meta_path(meta_path, blob_roots, num_disks, cfg, shard_repair_sem).await
            }
        })
        .buffer_unordered(cfg.meta_concurrency);

    let mut batch_agg = BatchDelta::default();
    let mut pending_update = BatchDelta::default();
    let mut last_update = Instant::now();
    let mut processed_since_pause: usize = 0;

    while let Some(res) = stream.next().await {
        let delta = match res {
            Ok(d) => d,
            Err(e) => {
                warn!("Meta batch item failed (non-fatal): {}", e);
                continue;
            }
        };

        batch_agg.scanned += delta.scanned;
        batch_agg.repaired_shards += delta.repaired_shards;
        batch_agg.corrupt_found += delta.corrupt_found;
        batch_agg.missing_meta_repaired += delta.missing_meta_repaired;
        batch_agg.divergent_meta_repaired += delta.divergent_meta_repaired;
        if delta.last_repaired.is_some() {
            batch_agg.last_repaired = delta.last_repaired.clone();
        }
        if delta.last_disagreement.is_some() {
            batch_agg.last_disagreement = delta.last_disagreement.clone();
        }

        pending_update.scanned += delta.scanned;
        pending_update.repaired_shards += delta.repaired_shards;
        pending_update.corrupt_found += delta.corrupt_found;
        pending_update.missing_meta_repaired += delta.missing_meta_repaired;
        pending_update.divergent_meta_repaired += delta.divergent_meta_repaired;
        if delta.last_repaired.is_some() {
            pending_update.last_repaired = delta.last_repaired.clone();
        }
        if delta.last_disagreement.is_some() {
            pending_update.last_disagreement = delta.last_disagreement.clone();
        }

        if let Some(delay) = batch_pacing {
            processed_since_pause += 1;
            if processed_since_pause >= 32 {
                processed_since_pause = 0;
                tokio::time::sleep(delay).await;
            }
        }

        let significant_change = delta.repaired_shards > 0
            || delta.missing_meta_repaired > 0
            || delta.divergent_meta_repaired > 0
            || delta.corrupt_found > 0;

        if significant_change
            || pending_update.scanned >= 10
            || last_update.elapsed().as_millis() > 200
        {
            let mut status = REPAIR_STATUS.write().await;
            status.total_scanned += pending_update.scanned;
            status.total_repaired += pending_update.repaired_shards
                + pending_update.missing_meta_repaired
                + pending_update.divergent_meta_repaired;
            status.corrupt_found += pending_update.corrupt_found;
            status.missing_meta_repaired += pending_update.missing_meta_repaired;
            status.divergent_meta_repaired += pending_update.divergent_meta_repaired;

            if let Some(s) = &pending_update.last_repaired {
                status.last_repaired = Some(s.clone());
            }
            if let Some(s) = &pending_update.last_disagreement {
                status.last_disagreement = Some(s.clone());
            }

            let elapsed = start_time.elapsed().as_secs_f64();
            status.duration_seconds = elapsed as u64;
            if elapsed > 0.0 {
                status.scan_rate_per_sec = status.total_scanned as f64 / elapsed;
            }

            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));

            pending_update = BatchDelta::default();
            last_update = Instant::now();
        }
    }

    if pending_update.scanned > 0 {
        let mut status = REPAIR_STATUS.write().await;
        status.total_scanned += pending_update.scanned;
        status.total_repaired += pending_update.repaired_shards
            + pending_update.missing_meta_repaired
            + pending_update.divergent_meta_repaired;
        status.corrupt_found += pending_update.corrupt_found;
        status.missing_meta_repaired += pending_update.missing_meta_repaired;
        status.divergent_meta_repaired += pending_update.divergent_meta_repaired;

        if let Some(s) = &pending_update.last_repaired {
            status.last_repaired = Some(s.clone());
        }
        if let Some(s) = &pending_update.last_disagreement {
            status.last_disagreement = Some(s.clone());
        }

        let elapsed = start_time.elapsed().as_secs_f64();
        status.duration_seconds = elapsed as u64;
        if elapsed > 0.0 {
            status.scan_rate_per_sec = status.total_scanned as f64 / elapsed;
        }

        ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
    }

    Ok(batch_agg)
}

async fn process_one_meta_path(
    meta_path: PathBuf,
    blob_roots: Arc<Vec<PathBuf>>,
    num_disks: usize,
    cfg: RepairRunConfig,
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

    if canonical_meta.is_delete_marker {
        return Ok(delta);
    }

    let (repaired, corrupt, last_rep) = process_object_repair(
        canonical_meta,
        &blob_roots,
        num_disks,
        cfg,
        shard_repair_sem,
    )
    .await?;

    delta.repaired_shards += repaired as u64;
    delta.corrupt_found += corrupt as u64;
    delta.last_repaired = last_rep;

    Ok(delta)
}

#[cfg(test)]
mod tests {
    use super::{RepairCheckMode, parse_check_mode};
    use std::collections::HashMap;

    #[test]
    fn parse_check_mode_is_case_insensitive_and_accepts_aliases() {
        let mut params = HashMap::new();
        params.insert("mode".to_string(), "ScRuB".to_string());
        assert_eq!(parse_check_mode(&params), RepairCheckMode::Scrub);

        params.insert("mode".to_string(), "READ_ONLY".to_string());
        assert_eq!(parse_check_mode(&params), RepairCheckMode::Strict);

        params.insert("mode".to_string(), "checksum".to_string());
        assert_eq!(parse_check_mode(&params), RepairCheckMode::Checksum);
    }

    #[test]
    fn parse_check_mode_falls_back_to_access_mode_when_mode_absent() {
        let mut params = HashMap::new();
        params.insert("access_mode".to_string(), "quiesced".to_string());
        assert_eq!(parse_check_mode(&params), RepairCheckMode::Scrub);

        params.insert("access_mode".to_string(), "readonly".to_string());
        assert_eq!(parse_check_mode(&params), RepairCheckMode::Strict);
    }
}
