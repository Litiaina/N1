use crate::enums::app_error_enum::AppError;
use crate::n1_core::blob_store::{
    BlobDiskHandle, allocate_raw_direct_io, commit_staged_raw_shard, copy_raw_extent,
    free_raw_address, open_raw_direct_io, raw_blob_debug_path, raw_stage_path_for_shard,
    resolve_blob_disk_path, stable_shard_file_name, verify_shard_checksum, write_raw_gc_markers,
};
use crate::n1_core::constants::{GC_SUFFIX, QUOTA_FILE};
use crate::n1_core::disk_manager::{
    ObjectMetadata, ObjectPathManager, PartMetadata, Quota, ShardInfo,
    configured_index_store_disks, default_stripe_size,
};
use crate::n1_core::ec::ErasureStreamWriter;
use crate::n1_core::fragment_metadata::{read_fragment_metadata, write_fragment_metadata};
use crate::n1_core::index;
use crate::n1_core::operation_gate::acquire_read_only_fast_mode;
use crate::n1_core::prefix_rebuilder::run_prefix_rebuild_job;
use crate::n1_core::recovery::{ShardFileCache, ShardRepairContext, read_or_recover_chunk_into};
use crate::n1_core::upload::{
    DATA_SHARDS_ATOMIC, DirectWriter, PARITY_SHARDS_ATOMIC, STRIPE_SIZE_ATOMIC, create_direct_file,
    sync_dir,
};
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::manager::WsConnectionManager;
use crate::websocket::ws_structs::StorageEventPayload;

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;

use chrono::Utc;
use crc32fast::Hasher as Crc32Hasher;
use futures_util::stream::{FuturesUnordered, StreamExt};
use serde::Serialize;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant, SystemTime};

use tokio::fs as tokio_fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{error, info, warn};
use ulid::Ulid;

#[derive(Debug, Clone, Serialize, Default)]
pub struct RedistributionJobStatus {
    pub is_running: bool,
    pub phase: String,
    pub fast_mode: bool,
    pub durability_mode: String,

    pub total_fragments_scanned: u64,
    pub total_objects_scanned: u64,
    pub total_objects_need_redistribution: u64,
    pub total_objects_migrated: u64,
    pub total_errors: u64,

    pub current_fragment: String,
    pub last_object: Option<String>,
    pub last_error: Option<String>,

    pub start_timestamp: i64,
    pub duration_seconds: u64,
    pub scan_rate_per_sec: f64,
}

static REDIST_STATUS: LazyLock<Arc<tokio::sync::RwLock<RedistributionJobStatus>>> =
    LazyLock::new(|| Arc::new(tokio::sync::RwLock::new(RedistributionJobStatus::default())));

static STOP_SIGNAL: LazyLock<AtomicBool> = LazyLock::new(|| AtomicBool::new(false));

#[derive(Clone, Copy, Debug)]
pub enum RedistributionDurabilityMode {
    Auto,
    Strict,
    Batched,
}

impl RedistributionDurabilityMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Strict => "strict",
            Self::Batched => "batched",
        }
    }
}

#[derive(Clone, Debug)]
pub struct RedistributionRunOptions {
    pub fast_mode: bool,
    pub durability_mode: RedistributionDurabilityMode,
    pub object_concurrency: usize,
    pub status_batch: u64,
    pub fragment_filter: Option<String>,
}

fn default_object_concurrency(fast_mode: bool) -> usize {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let ram_gib = detect_total_ram_gib();
    let ram_cap = if fast_mode {
        match ram_gib {
            0..=2 => 2,
            3..=4 => 4,
            5..=8 => 8,
            9..=16 => 12,
            17..=32 => 16,
            33..=64 => 24,
            _ => 32,
        }
    } else {
        match ram_gib {
            0..=2 => 2,
            3..=4 => 4,
            5..=8 => 6,
            9..=16 => 8,
            17..=32 => 12,
            33..=64 => 16,
            _ => 24,
        }
    };

    let cpu_default = if fast_mode {
        (cpus.saturating_mul(2)).clamp(4, 64)
    } else {
        cpus.clamp(2, 16)
    };

    cpu_default.min(ram_cap).max(2)
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

fn redistribution_limits_for_ram_gib(ram_gib: usize) -> (usize, u64) {
    match ram_gib {
        0..=4 => (8, 512),
        5..=8 => (16, 1024),
        9..=16 => (32, 2048),
        17..=32 => (48, 4096),
        33..=64 => (64, 8192),
        _ => (128, 8192),
    }
}

fn default_status_batch(fast_mode: bool) -> u64 {
    let ram_gib = detect_total_ram_gib();
    if fast_mode {
        match ram_gib {
            0..=4 => 32,
            5..=8 => 64,
            9..=32 => 128,
            _ => 256,
        }
    } else {
        match ram_gib {
            0..=4 => 16,
            5..=8 => 32,
            9..=32 => 64,
            _ => 128,
        }
    }
}

impl Default for RedistributionRunOptions {
    fn default() -> Self {
        Self {
            fast_mode: false,
            durability_mode: RedistributionDurabilityMode::Auto,
            object_concurrency: default_object_concurrency(false),
            status_batch: default_status_batch(false),
            fragment_filter: None,
        }
    }
}

impl RedistributionRunOptions {
    #[inline]
    fn strict_durability(&self) -> bool {
        match self.durability_mode {
            RedistributionDurabilityMode::Auto => !self.fast_mode,
            RedistributionDurabilityMode::Strict => true,
            RedistributionDurabilityMode::Batched => false,
        }
    }

    #[inline]
    fn effective_durability_mode(&self) -> &'static str {
        if self.strict_durability() {
            "strict"
        } else {
            "batched"
        }
    }
}

#[inline]
fn clamp_usize(v: usize, lo: usize, hi: usize) -> usize {
    v.max(lo).min(hi)
}

#[inline]
fn clamp_u64(v: u64, lo: u64, hi: u64) -> u64 {
    v.max(lo).min(hi)
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

fn parse_durability_mode_param(
    params: &HashMap<String, String>,
    fast_mode: bool,
) -> RedistributionDurabilityMode {
    if let Some(v) = params
        .get("durability")
        .or_else(|| params.get("durability_mode"))
    {
        let normalized = v.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "strict" | "sync" | "durable" => return RedistributionDurabilityMode::Strict,
            "batched" | "async" | "fast" | "relaxed" => {
                return RedistributionDurabilityMode::Batched;
            }
            "auto" => return RedistributionDurabilityMode::Auto,
            _ => {}
        }
    }

    if params.contains_key("strict_durability") {
        if parse_bool_param(params, "strict_durability", !fast_mode) {
            RedistributionDurabilityMode::Strict
        } else {
            RedistributionDurabilityMode::Batched
        }
    } else {
        RedistributionDurabilityMode::Auto
    }
}

struct StatusDelta {
    scanned: u64,
    needs: u64,
    migrated: u64,
    errors: u64,
    last_object: Option<String>,
    last_error: Option<String>,
}

async fn flush_status_delta(
    ws_manager: &Arc<WsConnectionManager>,
    start_time: Instant,
    delta: &mut StatusDelta,
) {
    if delta.scanned == 0
        && delta.needs == 0
        && delta.migrated == 0
        && delta.errors == 0
        && delta.last_object.is_none()
        && delta.last_error.is_none()
    {
        return;
    }

    let mut st = REDIST_STATUS.write().await;
    st.total_objects_scanned += delta.scanned;
    st.total_objects_need_redistribution += delta.needs;
    st.total_objects_migrated += delta.migrated;
    st.total_errors += delta.errors;
    if let Some(last) = delta.last_object.take() {
        st.last_object = Some(last);
    }
    if let Some(last_err) = delta.last_error.take() {
        st.last_error = Some(last_err);
    }
    let elapsed = start_time.elapsed().as_secs_f64();
    st.duration_seconds = elapsed as u64;
    if elapsed > 0.0 {
        st.scan_rate_per_sec = st.total_objects_scanned as f64 / elapsed;
    }
    ws_manager.broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));

    delta.scanned = 0;
    delta.needs = 0;
    delta.migrated = 0;
    delta.errors = 0;
}

#[inline]
fn should_stop() -> bool {
    STOP_SIGNAL.load(Ordering::Relaxed)
}

pub async fn get_redist_status() -> RedistributionJobStatus {
    REDIST_STATUS.read().await.clone()
}

pub async fn get_redist_status_api(
    claims: Claims,
) -> Result<Json<RedistributionJobStatus>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    Ok(Json(REDIST_STATUS.read().await.clone()))
}

pub async fn trigger_redistribution_job_api(
    claims: Claims,
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write || !claims.modify || !claims.delete {
        return Err(AppError::Forbidden(
            "Write, Modify and Delete permissions required".into(),
        ));
    }

    {
        let st = REDIST_STATUS.read().await;
        if st.is_running {
            return Err(AppError::Conflict("Job already running".into()));
        }
    }

    let dry_run = parse_bool_param(&params, "dry_run", false);
    let fast_mode = parse_bool_param(&params, "fast_mode", false)
        || parse_bool_param(&params, "read_only_fast", false);
    let ram_gib = detect_total_ram_gib();
    let cpu_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let (max_object_concurrency, max_status_batch) = redistribution_limits_for_ram_gib(ram_gib);
    let durability_mode = parse_durability_mode_param(&params, fast_mode);
    let object_concurrency = clamp_usize(
        params
            .get("object_concurrency")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or_else(|| default_object_concurrency(fast_mode)),
        1,
        max_object_concurrency,
    );
    let status_batch = clamp_u64(
        params
            .get("status_batch")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or_else(|| default_status_batch(fast_mode)),
        1,
        max_status_batch,
    );
    info!(
        "Redistribution tuning: ram_gib={} cpu_cores={} fast_mode={} object_concurrency={} status_batch={} (caps: object_concurrency<= {}, status_batch<= {})",
        ram_gib,
        cpu_cores,
        fast_mode,
        object_concurrency,
        status_batch,
        max_object_concurrency,
        max_status_batch
    );
    let run_opts = RedistributionRunOptions {
        fast_mode,
        durability_mode,
        object_concurrency,
        status_batch,
        fragment_filter: params
            .get("fragment")
            .or_else(|| params.get("fragment_hash"))
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .map(|v| v.to_string()),
    };

    let ws_manager = Arc::new(state.ws_manager.clone());
    let ws_manager_clone = ws_manager.clone();

    tokio::spawn(async move {
        if let Err(e) = run_redistribution_job_with_options(dry_run, run_opts, ws_manager).await {
            error!("Redistribution job failed: {}", e);
            let mut st = REDIST_STATUS.write().await;
            st.is_running = false;
            st.phase = "Failed".to_string();
            st.last_error = Some(e.to_string());
            ws_manager_clone
                .broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));
        }
    });

    Ok((
        StatusCode::ACCEPTED,
        if dry_run {
            "Redistribution initiated (dry run)."
        } else {
            "Redistribution initiated."
        },
    )
        .into_response())
}

pub async fn stop_redistribution_job_api(
    claims: Claims,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write || !claims.modify || !claims.delete {
        return Err(AppError::Forbidden(
            "Write, Modify and Delete permissions required".into(),
        ));
    }

    STOP_SIGNAL.store(true, Ordering::Relaxed);

    let mut st = REDIST_STATUS.write().await;
    if st.is_running {
        st.phase = "Stopping...".to_string();
        state
            .ws_manager
            .broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));
    }

    Ok((StatusCode::OK, "Stop signal sent.").into_response())
}

async fn replicate_quota_best_effort(fragment_hash: &str, index_roots: &[PathBuf], dry_run: bool) {
    let mut best_bytes: Option<Vec<u8>> = None;
    let mut best_mtime: SystemTime = SystemTime::UNIX_EPOCH;
    let mut best_len: u64 = 0;

    for r in index_roots {
        let p = r.join(fragment_hash).join(QUOTA_FILE);
        let bytes = match tokio_fs::read(&p).await {
            Ok(b) => b,
            Err(_) => continue,
        };
        if rmp_serde::from_slice::<Quota>(&bytes).is_err() {
            continue;
        }
        let meta = tokio_fs::metadata(&p).await.ok();
        let mtime = meta
            .as_ref()
            .and_then(|m| m.modified().ok())
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let len = meta.as_ref().map(|m| m.len()).unwrap_or(bytes.len() as u64);

        let take =
            best_bytes.is_none() || (mtime > best_mtime) || (mtime == best_mtime && len > best_len);
        if take {
            best_bytes = Some(bytes);
            best_mtime = mtime;
            best_len = len;
        }
    }

    let Some(canon) = best_bytes else {
        return;
    };

    for r in index_roots {
        let p = r.join(fragment_hash).join(QUOTA_FILE);
        let same = match tokio_fs::read(&p).await {
            Ok(cur) => cur == canon,
            Err(_) => false,
        };
        if same || dry_run {
            continue;
        }
        if let Err(e) = durable_atomic_write_bytes(&p, &canon).await {
            warn!(
                "Failed to mirror quota file during redistribution {}: {}",
                p.display(),
                e
            );
        }
    }
}

fn get_all_metadata_paths_custom(
    _index_roots: &[PathBuf],
    fragment_hash: &str,
    object_key: &str,
    version_id: &str,
    is_trash: bool,
) -> Vec<PathBuf> {
    index::get_object_version_paths(fragment_hash, object_key, version_id, is_trash)
        .unwrap_or_default()
}

async fn reachable_roots(roots: &[PathBuf]) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for r in roots {
        if tokio_fs::try_exists(r).await.unwrap_or(false) {
            out.push(r.clone());
        }
    }
    out
}

async fn replicate_fragment_metadata_best_effort(fragment_hash: &str) {
    if let Ok(meta) = read_fragment_metadata(fragment_hash).await {
        let _ = write_fragment_metadata(&meta).await;
    }
}

async fn durable_atomic_write_bytes(path: &Path, bytes: &[u8]) -> Result<(), AppError> {
    durable_atomic_write_bytes_with_mode(path, bytes, true).await
}

async fn durable_atomic_write_bytes_with_mode(
    path: &Path,
    bytes: &[u8],
    strict_durability: bool,
) -> Result<(), AppError> {
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError(format!("missing parent for {}", path.display()))
    })?;

    tokio_fs::create_dir_all(parent)
        .await
        .map_err(|e| AppError::Io(e, format!("mkdir {}", parent.display())))?;

    let file_name = path
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or("file")
        .to_string();

    let tmp = parent.join(format!(".{}.tmp.{}", file_name, Ulid::new().to_string()));

    let mut f = tokio_fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .await
        .map_err(|e| AppError::Io(e, format!("create tmp {}", tmp.display())))?;

    f.write_all(bytes)
        .await
        .map_err(|e| AppError::Io(e, format!("write tmp {}", tmp.display())))?;

    if strict_durability {
        let _ = f.sync_all().await;
    }
    drop(f);

    tokio_fs::rename(&tmp, path)
        .await
        .map_err(|e| AppError::Io(e, format!("rename {} -> {}", tmp.display(), path.display())))?;

    if strict_durability {
        let _ = sync_dir(parent).await;
    }
    Ok(())
}

#[cfg(unix)]
async fn stage_old_shard_gc_with_symlink(old_abs: &str) -> Result<(), AppError> {
    let old_path = PathBuf::from(old_abs);
    if !tokio_fs::try_exists(&old_path).await.unwrap_or(false) {
        return Ok(());
    }

    let gc_path = PathBuf::from(format!("{}{}", old_abs, GC_SUFFIX));
    if tokio_fs::try_exists(&gc_path).await.unwrap_or(false) {
        return Ok(());
    }

    let parent = old_path
        .parent()
        .ok_or_else(|| AppError::InternalServerError("missing shard parent".into()))?
        .to_path_buf();

    tokio_fs::rename(&old_path, &gc_path)
        .await
        .map_err(|e| AppError::Io(e, format!("rename shard -> gc {}", old_path.display())))?;

    let target_name = gc_path
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or("")
        .to_string();

    let old_path_clone = old_path.clone();
    let parent_clone = parent.clone();

    let symlink_res = tokio::task::spawn_blocking(move || {
        let target = PathBuf::from(target_name);
        std::os::unix::fs::symlink(target, &old_path_clone)
    })
    .await
    .map_err(|_| AppError::InternalServerError("symlink join failed".into()))?;

    if let Err(e) = symlink_res {
        let _ = tokio_fs::rename(&gc_path, &old_path).await;
        return Err(AppError::Io(e, "create gc symlink".into()));
    }

    let _ = sync_dir(&parent_clone).await;
    Ok(())
}

#[cfg(not(unix))]
async fn stage_old_shard_gc_with_symlink(_old_abs: &str) -> Result<(), AppError> {
    Ok(())
}

async fn list_all_fragments(index_roots: &[PathBuf]) -> Vec<String> {
    let roots = index_roots.to_vec();
    tokio::task::spawn_blocking(move || {
        let mut out = HashSet::<String>::new();
        for root in roots {
            if !root.exists() {
                continue;
            }
            let rd = match std::fs::read_dir(&root) {
                Ok(r) => r,
                Err(_) => continue,
            };
            for ent in rd.filter_map(|e| e.ok()) {
                if ent.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    if let Some(name) = ent.file_name().to_str() {
                        out.insert(name.to_string());
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

struct LiveLayout {
    blob_cfg: Vec<PathBuf>,
    index_cfg: Vec<PathBuf>,
    live_blob: Arc<Vec<PathBuf>>,
    live_index: Arc<Vec<PathBuf>>,
    last_refresh: Instant,
}

impl LiveLayout {
    async fn new(blob_cfg: Vec<PathBuf>, index_cfg: Vec<PathBuf>) -> Self {
        let live_blob = Arc::new(reachable_roots(&blob_cfg).await);
        let live_index = Arc::new(reachable_roots(&index_cfg).await);
        Self {
            blob_cfg,
            index_cfg,
            live_blob,
            live_index,
            last_refresh: Instant::now(),
        }
    }

    async fn refresh_if_needed(&mut self) {
        if self.last_refresh.elapsed() < Duration::from_secs(2) {
            return;
        }
        self.live_blob = Arc::new(reachable_roots(&self.blob_cfg).await);
        self.live_index = Arc::new(reachable_roots(&self.index_cfg).await);
        self.last_refresh = Instant::now();
    }
}

#[cfg(target_os = "linux")]
#[cfg(test)]
pub(crate) async fn run_redistribution_job(
    dry_run: bool,
    ws_manager: Arc<WsConnectionManager>,
) -> Result<(), AppError> {
    run_redistribution_job_with_options(dry_run, RedistributionRunOptions::default(), ws_manager)
        .await
}

pub async fn run_redistribution_job_with_options(
    dry_run: bool,
    opts: RedistributionRunOptions,
    ws_manager: Arc<WsConnectionManager>,
) -> Result<(), AppError> {
    STOP_SIGNAL.store(false, Ordering::Relaxed);
    let start_time = Instant::now();
    let _write_gate = acquire_read_only_fast_mode(opts.fast_mode);
    let strict_durability = opts.strict_durability();
    let effective_durability = opts.effective_durability_mode();
    let scope_label = opts.fragment_filter.as_deref().unwrap_or("all-fragments");

    {
        let mut st = REDIST_STATUS.write().await;
        *st = RedistributionJobStatus {
            is_running: true,
            phase: format!(
                "Initializing (scope={}, fast_mode={}, durability={}=>{}, object_concurrency={}, status_batch={}, dry_run={})",
                scope_label,
                opts.fast_mode,
                opts.durability_mode.as_str(),
                effective_durability,
                opts.object_concurrency,
                opts.status_batch,
                dry_run
            ),
            fast_mode: opts.fast_mode,
            durability_mode: effective_durability.to_string(),
            start_timestamp: Utc::now().timestamp(),
            ..Default::default()
        };
        ws_manager.broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));
    }

    let target_data_shards = DATA_SHARDS_ATOMIC
        .get()
        .ok_or(AppError::InternalServerError("DATA_SHARDS missing".into()))?
        .load(Ordering::Relaxed) as usize;

    let target_parity_shards = PARITY_SHARDS_ATOMIC
        .get()
        .ok_or(AppError::InternalServerError(
            "PARITY_SHARDS missing".into(),
        ))?
        .load(Ordering::Relaxed) as usize;

    let target_stripe_size = STRIPE_SIZE_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or_else(default_stripe_size);

    let pm = ObjectPathManager::new();
    let blob_roots_cfg = pm.get_blob_roots();

    let index_roots_cfg: Vec<PathBuf> = configured_index_store_disks()?
        .iter()
        .map(PathBuf::from)
        .collect();

    let mut layout = LiveLayout::new(blob_roots_cfg.clone(), index_roots_cfg.clone()).await;

    if layout.live_blob.is_empty() {
        return Err(AppError::InternalServerError(
            "No reachable blob disks".into(),
        ));
    }
    if layout.live_index.is_empty() {
        return Err(AppError::InternalServerError(
            "No reachable index roots".into(),
        ));
    }

    info!(
        "Starting Redistribution Job (scope={}, Active & Trash). target={}D+{}P stripe={} bytes blob_cfg={} index_cfg={} dry_run={} fast_mode={} durability={}=>{} object_concurrency={} status_batch={}",
        scope_label,
        target_data_shards,
        target_parity_shards,
        target_stripe_size,
        blob_roots_cfg.len(),
        index_roots_cfg.len(),
        dry_run,
        opts.fast_mode,
        opts.durability_mode.as_str(),
        effective_durability,
        opts.object_concurrency,
        opts.status_batch
    );

    let mut touched_fragments: HashSet<String> = HashSet::new();
    let mut fragments = list_all_fragments(layout.live_index.as_ref()).await;
    if let Some(fragment_filter) = opts.fragment_filter.as_deref() {
        if !fragments.iter().any(|fragment| fragment == fragment_filter) {
            warn!("Fragment not found for redistribution: {}", fragment_filter);
            return Err(AppError::NotFound);
        }
        fragments.retain(|fragment| fragment == fragment_filter);
    }

    {
        let mut st = REDIST_STATUS.write().await;
        st.phase = format!(
            "Preparing (scope={}, fragments={})",
            scope_label,
            fragments.len()
        );
        ws_manager.broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));
    }

    let object_concurrency = opts.object_concurrency.max(1);
    let status_batch = opts.status_batch.max(1);

    for fragment_hash in fragments {
        if should_stop() {
            break;
        }

        layout.refresh_if_needed().await;
        let live_blob = layout.live_blob.clone();
        let live_index = layout.live_index.clone();

        if live_blob.is_empty() || live_index.is_empty() {
            continue;
        }

        replicate_fragment_metadata_best_effort(&fragment_hash).await;
        replicate_quota_best_effort(&fragment_hash, live_index.as_ref(), dry_run).await;

        {
            let mut st = REDIST_STATUS.write().await;
            st.current_fragment = fragment_hash.clone();
            st.total_fragments_scanned += 1;
            st.phase = format!("Scanning Fragment {}", fragment_hash);
            ws_manager.broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));
        }

        for is_trash in [false, true] {
            if should_stop() {
                break;
            }

            let mut delta = StatusDelta {
                scanned: 0,
                needs: 0,
                migrated: 0,
                errors: 0,
                last_object: None,
                last_error: None,
            };
            let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();

            let metas = match index::list_fragment_object_versions(&fragment_hash, is_trash).await {
                Ok(metas) => metas,
                Err(e) => {
                    warn!(
                        "Redistribution metadata scan failed for fragment {} trash={}: {}",
                        fragment_hash, is_trash, e
                    );
                    continue;
                }
            };

            for meta in metas {
                if should_stop() {
                    break;
                }

                if meta.is_delete_marker {
                    continue;
                }

                delta.scanned += 1;
                if delta.scanned >= status_batch {
                    layout.refresh_if_needed().await;
                    flush_status_delta(&ws_manager, start_time, &mut delta).await;
                }

                let needs = check_needs_redistribution(
                    &meta,
                    target_data_shards,
                    target_parity_shards,
                    target_stripe_size,
                    live_blob.len(),
                    &live_blob,
                    &live_index,
                    is_trash,
                )
                .await;

                if !needs {
                    continue;
                }

                let object_label = format!(
                    "{}/{} (v:{}) [Trash:{}]",
                    meta.fragment, meta.object_key, meta.version_id, is_trash
                );
                delta.needs += 1;
                delta.last_object = Some(object_label.clone());

                if dry_run {
                    delta.migrated += 1;
                    if delta.needs + delta.migrated + delta.errors >= status_batch {
                        flush_status_delta(&ws_manager, start_time, &mut delta).await;
                    }
                    continue;
                }

                let live_blob_for_task = live_blob.clone();
                let live_index_for_task = live_index.clone();
                let task_strict_durability = strict_durability;
                in_flight.push(async move {
                    let mut meta = meta;
                    let fragment = meta.fragment.clone();
                    let key = meta.object_key.clone();
                    let version = meta.version_id.clone();

                    match redistribute_object_streaming(
                        &mut meta,
                        target_data_shards,
                        target_parity_shards,
                        target_stripe_size,
                        &live_blob_for_task,
                        &live_index_for_task,
                        is_trash,
                        task_strict_durability,
                    )
                    .await
                    {
                        Ok(_) => Ok((fragment, key, version, is_trash)),
                        Err(e) => Err((fragment, key, version, is_trash, e.to_string())),
                    }
                });

                while in_flight.len() >= object_concurrency {
                    if let Some(res) = in_flight.next().await {
                        match res {
                            Ok((fragment, key, version, task_is_trash)) => {
                                touched_fragments.insert(fragment.clone());
                                delta.migrated += 1;
                                delta.last_object = Some(format!(
                                    "{}/{} (v:{}) [Trash:{}]",
                                    fragment, key, version, task_is_trash
                                ));
                            }
                            Err((fragment, key, version, task_is_trash, err)) => {
                                warn!(
                                    "Redistribute failed for {}/{} (v:{}) [Trash:{}]: {}",
                                    fragment, key, version, task_is_trash, err
                                );
                                delta.errors += 1;
                                delta.last_error = Some(err);
                            }
                        }
                    }
                    if delta.needs + delta.migrated + delta.errors >= status_batch {
                        flush_status_delta(&ws_manager, start_time, &mut delta).await;
                    }
                }
            }

            while let Some(res) = in_flight.next().await {
                match res {
                    Ok((fragment, key, version, task_is_trash)) => {
                        touched_fragments.insert(fragment.clone());
                        delta.migrated += 1;
                        delta.last_object = Some(format!(
                            "{}/{} (v:{}) [Trash:{}]",
                            fragment, key, version, task_is_trash
                        ));
                    }
                    Err((fragment, key, version, task_is_trash, err)) => {
                        warn!(
                            "Redistribute failed for {}/{} (v:{}) [Trash:{}]: {}",
                            fragment, key, version, task_is_trash, err
                        );
                        delta.errors += 1;
                        delta.last_error = Some(err);
                    }
                }
                if delta.needs + delta.migrated + delta.errors >= status_batch {
                    flush_status_delta(&ws_manager, start_time, &mut delta).await;
                }
            }

            flush_status_delta(&ws_manager, start_time, &mut delta).await;
        }
    }

    if !dry_run && !touched_fragments.is_empty() && !should_stop() {
        {
            let mut st = REDIST_STATUS.write().await;
            st.phase = format!(
                "Rebuilding Prefix Index (fragments={})",
                touched_fragments.len()
            );
            ws_manager.broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));
        }

        let mut frags: Vec<String> = touched_fragments.into_iter().collect();
        frags.sort();

        for frag in frags {
            if should_stop() {
                break;
            }
            let _ = run_prefix_rebuild_job(Some(frag), ws_manager.clone()).await;
        }
    }

    {
        let mut st = REDIST_STATUS.write().await;
        st.is_running = false;
        st.fast_mode = opts.fast_mode;
        st.durability_mode = effective_durability.to_string();
        st.phase = if should_stop() {
            "Stopped by User".to_string()
        } else {
            "Completed".to_string()
        };
        st.duration_seconds = start_time.elapsed().as_secs();
        ws_manager.broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));
    }

    info!("Redistribution Job Ended.");
    Ok(())
}

async fn check_needs_redistribution(
    meta: &ObjectMetadata,
    target_data: usize,
    target_parity: usize,
    target_stripe_size: usize,
    target_num_disks: usize,
    target_blob_roots: &[PathBuf],
    target_index_roots: &[PathBuf],
    is_trash: bool,
) -> bool {
    if meta.parts.is_empty() {
        return false;
    }

    if meta.stripe_size_bytes != target_stripe_size {
        return true;
    }

    let part0 = &meta.parts[0];
    let cur_data = part0.shards.iter().filter(|s| !s.is_parity).count();
    let cur_parity = part0.shards.iter().filter(|s| s.is_parity).count();

    if cur_data != target_data || cur_parity != target_parity {
        return true;
    }

    if target_num_disks == 0 {
        return true;
    }

    for part in &meta.parts {
        for shard in &part.shards {
            let expected = (part.part_index as usize + shard.shard_index) % target_num_disks;
            if expected >= target_blob_roots.len() {
                return true;
            }

            match resolve_blob_disk_path(target_blob_roots[expected].as_path()) {
                Ok(BlobDiskHandle::Raw { device_id, .. }) => {
                    let Some(raw) = shard.raw_blob_address.as_ref() else {
                        return true;
                    };
                    if raw.device_id != device_id {
                        return true;
                    }
                }
                Err(_) => return true,
            }
        }
    }

    let expected_metadata_paths = get_all_metadata_paths_custom(
        target_index_roots,
        &meta.fragment,
        &meta.object_key,
        &meta.version_id,
        is_trash,
    );

    for path in expected_metadata_paths {
        if !tokio_fs::try_exists(&path).await.unwrap_or(false) {
            return true;
        }
    }

    false
}

async fn cleanup_tmp_files(files: &[PathBuf]) {
    for p in files {
        let _ = tokio_fs::remove_file(p).await;
    }
}

async fn crc32_file_hex(path: &Path) -> Result<String, AppError> {
    let mut f = tokio_fs::File::open(path)
        .await
        .map_err(|e| AppError::Io(e, format!("open for crc32 {}", path.display())))?;

    let mut hasher = Crc32Hasher::new();
    let mut buf = vec![0u8; 256 * 1024];

    loop {
        let n = f
            .read(&mut buf)
            .await
            .map_err(|e| AppError::Io(e, format!("read crc32 {}", path.display())))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    Ok(format!("{:08x}", hasher.finalize()))
}

async fn verify_tmp_shard(
    tmp_path: &Path,
    expected_len: u64,
    expected_crc32: Option<&str>,
) -> Result<(), AppError> {
    let len_ok = tokio_fs::metadata(tmp_path)
        .await
        .map(|m| m.len() == expected_len)
        .unwrap_or(false);

    if !len_ok {
        return Err(AppError::InternalServerError(format!(
            "Shard length mismatch for {}",
            tmp_path.display()
        )));
    }

    if let Some(exp) = expected_crc32.filter(|s| !s.is_empty()) {
        let got = crc32_file_hex(tmp_path).await?;
        if !got.eq_ignore_ascii_case(exp) {
            return Err(AppError::InternalServerError(format!(
                "Shard checksum mismatch for {} (expected {}, got {})",
                tmp_path.display(),
                exp,
                got
            )));
        }
    }

    Ok(())
}

async fn redistribute_object_streaming(
    meta: &mut ObjectMetadata,
    target_data_shards: usize,
    target_parity_shards: usize,
    target_stripe_size: usize,
    target_blob_roots: &[PathBuf],
    target_index_roots: &[PathBuf],
    is_trash: bool,
    strict_durability: bool,
) -> Result<(), AppError> {
    if should_stop() {
        return Err(AppError::InternalServerError("Stopped".into()));
    }

    let start_time = Instant::now();
    let num_disks = target_blob_roots.len();
    let total_shards = target_data_shards + target_parity_shards;

    if num_disks < target_data_shards {
        return Err(AppError::InsufficientStorage);
    }

    let old_block_size = meta.stripe_size_bytes;
    let new_block_size = target_stripe_size;
    let verify_tmp_crc = strict_durability;
    let repair_context = ShardRepairContext::new(
        meta.fragment.clone(),
        meta.object_key.clone(),
        meta.version_id.clone(),
        is_trash,
    );

    let mut file_cache: ShardFileCache = ShardFileCache::new();

    let old_parts = std::mem::take(&mut meta.parts);
    let mut new_parts: Vec<PartMetadata> = Vec::with_capacity(old_parts.len());
    let mut old_shards_for_gc: Vec<String> = Vec::new();
    let mut old_raw_shards_for_gc = Vec::new();

    for old_part in old_parts {
        if should_stop() {
            return Err(AppError::InternalServerError("Stopped".into()));
        }

        let old_part_arc = Arc::new(old_part);

        let old_data_count = old_part_arc.shards.iter().filter(|s| !s.is_parity).count() as u64;
        if old_data_count == 0 {
            return Err(AppError::InternalServerError("invalid old EC".into()));
        }

        let old_stripe_width = old_block_size as u64 * old_data_count;

        let new_data_width = (new_block_size * target_data_shards) as u64;
        if new_data_width == 0 {
            return Err(AppError::InternalServerError(
                "invalid target stripe".into(),
            ));
        }

        let new_stripe_count = (old_part_arc.size_bytes + new_data_width - 1) / new_data_width;
        let final_shard_len = new_stripe_count * new_block_size as u64;

        let can_fast_copy_raw = old_block_size == new_block_size
            && old_part_arc.shards.len() == total_shards
            && old_part_arc
                .shards
                .iter()
                .all(|s| s.raw_blob_address.is_some())
            && old_part_arc
                .shards
                .iter()
                .all(|s| s.size_bytes == final_shard_len);

        if can_fast_copy_raw {
            let mut copied_shards: Vec<ShardInfo> = Vec::with_capacity(total_shards);
            let mut allocated_raw = Vec::with_capacity(total_shards);
            for shard_idx in 0..total_shards {
                if should_stop() {
                    for raw in &allocated_raw {
                        let _ = free_raw_address(raw).await;
                    }
                    return Err(AppError::InternalServerError("Stopped".into()));
                }

                let old_shard = old_part_arc
                    .shards
                    .iter()
                    .find(|s| s.shard_index == shard_idx)
                    .ok_or_else(|| {
                        AppError::InternalServerError(format!(
                            "missing raw shard {} during redistribution",
                            shard_idx
                        ))
                    })?;

                if !verify_shard_checksum(old_shard).await? {
                    for raw in &allocated_raw {
                        let _ = free_raw_address(raw).await;
                    }
                    return Err(AppError::InternalServerError(format!(
                        "raw shard {} failed checksum verification during redistribution",
                        old_shard.file_name
                    )));
                }

                let is_parity = shard_idx >= target_data_shards;
                let disk_idx = (old_part_arc.part_index as usize + shard_idx) % num_disks;
                let (raw_addr, target_io) = allocate_raw_direct_io(disk_idx, final_shard_len)?;
                allocated_raw.push(raw_addr.clone());

                let source_io = open_raw_direct_io(old_shard)?;
                if let Err(err) = copy_raw_extent(
                    source_io,
                    target_io,
                    final_shard_len,
                    new_block_size.max(1024 * 1024),
                    true,
                )
                .await
                {
                    let _ = free_raw_address(&raw_addr).await;
                    for raw in &allocated_raw {
                        if raw != &raw_addr {
                            let _ = free_raw_address(raw).await;
                        }
                    }
                    return Err(err);
                }

                copied_shards.push(ShardInfo {
                    shard_index: shard_idx,
                    file_name: stable_shard_file_name(
                        old_part_arc.part_index,
                        shard_idx,
                        is_parity,
                    ),
                    absolute_path: raw_blob_debug_path(&raw_addr),
                    raw_blob_address: Some(raw_addr),
                    size_bytes: final_shard_len,
                    checksum: old_shard.checksum.clone(),
                    is_parity,
                });
            }

            let new_part = PartMetadata {
                part_index: old_part_arc.part_index,
                size_bytes: old_part_arc.size_bytes,
                created_at: old_part_arc.created_at,
                shards: copied_shards.clone(),
                checksum: old_part_arc.checksum.clone(),
            };
            new_parts.push(new_part);

            let new_paths: HashSet<String> = copied_shards
                .iter()
                .map(|s| s.absolute_path.clone())
                .collect();
            for old_shard in &old_part_arc.shards {
                if !new_paths.contains(&old_shard.absolute_path) {
                    if let Some(raw) = old_shard.raw_blob_address.clone() {
                        old_raw_shards_for_gc.push(raw);
                    } else {
                        old_shards_for_gc.push(old_shard.absolute_path.clone());
                    }
                }
            }
            continue;
        }

        let mut writers: Vec<Option<DirectWriter>> = Vec::with_capacity(total_shards);
        let mut commit_list: Vec<(PathBuf, PathBuf, PathBuf, String, bool, usize)> = Vec::new();
        let mut new_shards: Vec<ShardInfo> = Vec::with_capacity(total_shards);
        let mut created_tmps: Vec<PathBuf> = Vec::new();

        for shard_idx in 0..total_shards {
            if should_stop() {
                cleanup_tmp_files(&created_tmps).await;
                return Err(AppError::InternalServerError("Stopped".into()));
            }

            let is_parity = shard_idx >= target_data_shards;
            let file_name = stable_shard_file_name(old_part_arc.part_index, shard_idx, is_parity);

            let disk_idx = (old_part_arc.part_index as usize + shard_idx) % num_disks;
            let disk_root = &target_blob_roots[disk_idx];

            if !tokio_fs::try_exists(disk_root).await.unwrap_or(false) {
                cleanup_tmp_files(&created_tmps).await;
                return Err(AppError::InternalServerError(
                    "blob disk unreachable".into(),
                ));
            }

            let tmp_path = match resolve_blob_disk_path(disk_root.as_path())? {
                BlobDiskHandle::Raw { .. } => {
                    raw_stage_path_for_shard(
                        disk_idx,
                        meta.fragment.as_str(),
                        meta.version_id.as_str(),
                        Ulid::new().to_string().as_str(),
                        file_name.as_str(),
                    )
                    .await?
                }
            };
            created_tmps.push(tmp_path.clone());

            let std_file = create_direct_file(&tmp_path).await.map_err(|e| {
                AppError::InternalServerError(format!(
                    "create shard tmp {}: {}",
                    tmp_path.display(),
                    e
                ))
            })?;

            let file_clone = std_file
                .try_clone()
                .map_err(|e| AppError::Io(e, "clone direct file".into()))?;

            let len = final_shard_len;
            tokio::task::spawn_blocking(move || file_clone.set_len(len))
                .await
                .map_err(|e| AppError::InternalServerError(format!("set_len join: {}", e)))?
                .map_err(|e| AppError::Io(e, "set_len shard".into()))?;

            writers.push(Some(DirectWriter::new(std_file)));
            commit_list.push((
                tmp_path,
                PathBuf::new(),
                PathBuf::new(),
                file_name.clone(),
                true,
                disk_idx,
            ));

            new_shards.push(ShardInfo {
                shard_index: shard_idx,
                file_name: file_name.clone(),
                absolute_path: String::new(),
                raw_blob_address: None,
                size_bytes: final_shard_len,
                checksum: String::new(),
                is_parity,
            });
        }

        let mut stream_writer = ErasureStreamWriter::new(
            writers,
            target_data_shards,
            target_parity_shards,
            final_shard_len as usize,
            false,
            new_block_size,
        )?;

        let mut pos = 0u64;
        let mut batch: Vec<u8> = Vec::with_capacity(new_block_size);
        let mut scratch = Vec::new();

        while pos < old_part_arc.size_bytes {
            if should_stop() {
                cleanup_tmp_files(&created_tmps).await;
                return Err(AppError::InternalServerError("Stopped".into()));
            }

            let stripe_index = pos / old_stripe_width;
            let offset_in_stripe = pos % old_stripe_width;

            let shard_index = (offset_in_stripe / old_block_size as u64) as usize;
            let offset_in_block = offset_in_stripe % old_block_size as u64;
            let file_offset = (stripe_index * old_block_size as u64) + offset_in_block;

            let to_read = ((old_block_size as u64 - offset_in_block)
                .min(old_part_arc.size_bytes - pos)) as usize;
            if to_read == 0 {
                break;
            }

            read_or_recover_chunk_into(
                &mut file_cache,
                Path::new(""),
                old_part_arc.clone(),
                shard_index,
                file_offset,
                to_read,
                old_block_size,
                Some(repair_context.clone()),
                &mut scratch,
            )
            .await?;

            batch.extend_from_slice(&scratch);

            if batch.len() >= new_block_size || (pos + to_read as u64) >= old_part_arc.size_bytes {
                stream_writer.write(&batch).await?;
                batch.clear();
            }

            pos += to_read as u64;
        }

        stream_writer.finalize().await?;

        let sums = stream_writer.get_shard_checksums();
        let mut sum_map: HashMap<usize, String> = HashMap::new();
        for (i, s) in sums.into_iter().enumerate() {
            sum_map.insert(i, s);
        }
        let writers = stream_writer.into_writers();

        if strict_durability {
            for w in writers.into_iter().flatten() {
                if let Err(e) = w.sync_data().await {
                    cleanup_tmp_files(&created_tmps).await;
                    return Err(AppError::Io(e, "fsync redistribution shard".into()));
                }
            }
        }

        for s in new_shards.iter_mut() {
            if let Some(v) = sum_map.get(&s.shard_index) {
                s.checksum = v.clone();
            }
        }
        for ((tmp, _final_p, _parent, _fname, is_raw_target, disk_idx), shard_meta) in
            commit_list.iter().zip(new_shards.iter_mut())
        {
            if should_stop() {
                cleanup_tmp_files(&created_tmps).await;
                return Err(AppError::InternalServerError("Stopped".into()));
            }

            let expected_crc = verify_tmp_crc.then_some(shard_meta.checksum.as_str());

            verify_tmp_shard(tmp.as_path(), final_shard_len, expected_crc).await?;

            if *is_raw_target {
                let raw_addr =
                    commit_staged_raw_shard(*disk_idx, tmp.as_path(), final_shard_len).await?;
                shard_meta.raw_blob_address = Some(raw_addr.clone());
                shard_meta.absolute_path = raw_blob_debug_path(&raw_addr);
            }
        }

        let new_part = PartMetadata {
            part_index: old_part_arc.part_index,
            size_bytes: old_part_arc.size_bytes,
            created_at: old_part_arc.created_at,
            shards: new_shards.clone(),
            checksum: old_part_arc.checksum.clone(),
        };
        new_parts.push(new_part);

        let new_paths: HashSet<String> = new_parts
            .last()
            .unwrap()
            .shards
            .iter()
            .map(|s| s.absolute_path.clone())
            .collect();

        for old_shard in &old_part_arc.shards {
            if !new_paths.contains(&old_shard.absolute_path) {
                if let Some(raw) = old_shard.raw_blob_address.clone() {
                    old_raw_shards_for_gc.push(raw);
                } else {
                    old_shards_for_gc.push(old_shard.absolute_path.clone());
                }
            }
        }

        file_cache.clear();
    }

    meta.parts = new_parts;
    meta.stripe_size_bytes = new_block_size;

    let bytes = rmp_serde::to_vec(&*meta).map_err(|e| {
        AppError::InternalServerError(format!("Failed to serialize metadata: {}", e))
    })?;

    let meta_paths = get_all_metadata_paths_custom(
        target_index_roots,
        &meta.fragment,
        &meta.object_key,
        &meta.version_id,
        is_trash,
    );

    let mut any_meta_ok = false;
    let mut meta_sync_dirs: HashSet<PathBuf> = HashSet::new();
    for p in meta_paths {
        if should_stop() {
            return Err(AppError::InternalServerError("Stopped".into()));
        }
        match durable_atomic_write_bytes_with_mode(&p, &bytes, strict_durability).await {
            Ok(_) => {
                any_meta_ok = true;
                if !strict_durability {
                    if let Some(parent) = p.parent() {
                        meta_sync_dirs.insert(parent.to_path_buf());
                    }
                }
            }
            Err(e) => {
                warn!(
                    "Failed to write redistributed metadata replica {}: {}",
                    p.display(),
                    e
                );
            }
        }
    }

    if !strict_durability {
        for d in meta_sync_dirs {
            let _ = sync_dir(&d).await;
        }
    }

    if !any_meta_ok {
        return Err(AppError::InternalServerError(
            "failed to write metadata to any reachable index root".into(),
        ));
    }

    if is_trash {
        index::upsert_trashed_object_version(meta).await?;
    } else {
        index::upsert_object_version(meta).await?;
    }

    for old in old_shards_for_gc {
        if should_stop() {
            return Err(AppError::InternalServerError("Stopped".into()));
        }
        let _ = stage_old_shard_gc_with_symlink(&old).await;
    }

    if !old_raw_shards_for_gc.is_empty() {
        let mut deduped = HashMap::new();
        for raw in old_raw_shards_for_gc {
            deduped
                .entry((
                    raw.device_id.clone(),
                    raw.allocation_id,
                    raw.offset_bytes,
                    raw.size_bytes,
                ))
                .or_insert(raw);
        }
        let values: Vec<_> = deduped.into_values().collect();
        write_raw_gc_markers(target_index_roots, &meta.fragment, &values).await?;
    }

    info!(
        "Redistributed {}/{} (v:{}) [Trash:{}] in {:.2?}.",
        meta.fragment,
        meta.object_key,
        meta.version_id,
        is_trash,
        start_time.elapsed()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        RedistributionDurabilityMode, RedistributionRunOptions, parse_durability_mode_param,
    };
    use std::collections::HashMap;

    #[test]
    fn durability_auto_resolves_from_fast_mode() {
        let opts_slow = RedistributionRunOptions {
            fast_mode: false,
            durability_mode: RedistributionDurabilityMode::Auto,
            ..Default::default()
        };
        assert!(opts_slow.strict_durability());
        assert_eq!(opts_slow.effective_durability_mode(), "strict");

        let opts_fast = RedistributionRunOptions {
            fast_mode: true,
            durability_mode: RedistributionDurabilityMode::Auto,
            ..Default::default()
        };
        assert!(!opts_fast.strict_durability());
        assert_eq!(opts_fast.effective_durability_mode(), "batched");
    }

    #[test]
    fn durability_explicit_override_wins_over_fast_mode() {
        let strict = RedistributionRunOptions {
            fast_mode: true,
            durability_mode: RedistributionDurabilityMode::Strict,
            ..Default::default()
        };
        assert!(strict.strict_durability());
        assert_eq!(strict.effective_durability_mode(), "strict");

        let batched = RedistributionRunOptions {
            fast_mode: false,
            durability_mode: RedistributionDurabilityMode::Batched,
            ..Default::default()
        };
        assert!(!batched.strict_durability());
        assert_eq!(batched.effective_durability_mode(), "batched");
    }

    #[test]
    fn parse_durability_mode_from_string_param() {
        let mut params = HashMap::new();
        params.insert("durability".to_string(), "strict".to_string());
        assert!(matches!(
            parse_durability_mode_param(&params, true),
            RedistributionDurabilityMode::Strict
        ));

        params.insert("durability".to_string(), "BATCHED".to_string());
        assert!(matches!(
            parse_durability_mode_param(&params, false),
            RedistributionDurabilityMode::Batched
        ));

        params.insert("durability".to_string(), "auto".to_string());
        assert!(matches!(
            parse_durability_mode_param(&params, false),
            RedistributionDurabilityMode::Auto
        ));
    }

    #[test]
    fn parse_durability_mode_from_bool_fallback() {
        let mut params = HashMap::new();
        params.insert("strict_durability".to_string(), "true".to_string());
        assert!(matches!(
            parse_durability_mode_param(&params, true),
            RedistributionDurabilityMode::Strict
        ));

        params.insert("strict_durability".to_string(), "false".to_string());
        assert!(matches!(
            parse_durability_mode_param(&params, false),
            RedistributionDurabilityMode::Batched
        ));
    }
}
