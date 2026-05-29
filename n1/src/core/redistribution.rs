use crate::core::constants::{
    FRAGMENT_METADATA_FILE, GC_SUFFIX, N1_BLOB_DIR, PARITY_SUFFIX, REDIST_TMP_SUFFIX, SHARD_SUFFIX,
};
use crate::core::disk_manager::{
    INDEX_STORE_DISKS, ObjectMetadata, ObjectPathManager, PartMetadata, ShardInfo,
    default_stripe_size,
};
use crate::core::ec::ErasureStreamWriter;
use crate::core::fragment_identity::fragment_root_on;
use crate::core::fragment_metadata::{
    fragment_index_partition_count, read_fragment_metadata, write_fragment_metadata,
};
use crate::core::index;
use crate::core::index_store;
use crate::core::io_scheduler::{DiskIoClass, acquire_disk_io};
use crate::core::operation_gate::{MaintenanceAccessMode, acquire_maintenance_mode};
use crate::core::quota_manager::validate_fragment_project_headroom_for_redistribution;
use crate::core::recovery::read_or_recover_chunk;
use crate::core::upload::{
    DATA_SHARDS_ATOMIC, DirectWriter, PARITY_SHARDS_ATOMIC, STRIPE_SIZE_ATOMIC, create_direct_file,
    sync_dir,
};
use crate::enums::app_error_enum::AppError;
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
use serde::{Deserialize, Serialize};

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant, SystemTime};

use tokio::fs as tokio_fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{error, info, warn};
use ulid::Ulid;
use walkdir::WalkDir;

#[derive(Debug, Clone, Serialize, Default)]
pub struct RedistributionJobStatus {
    pub is_running: bool,
    pub phase: String,
    pub fast_mode: bool,
    pub durability_mode: String,
    pub scope: String,

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

#[derive(Debug, Clone, Serialize, Default)]
pub struct RedistributionIntentDebtSnapshot {
    pub pending_intents: u64,
    pub failed_intents: u64,
    pub fragments_with_pending_intents: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_pending_intent_age_seconds: Option<u64>,
}

static REDIST_STATUS: LazyLock<Arc<tokio::sync::RwLock<RedistributionJobStatus>>> =
    LazyLock::new(|| Arc::new(tokio::sync::RwLock::new(RedistributionJobStatus::default())));

static STOP_SIGNAL: LazyLock<AtomicBool> = LazyLock::new(|| AtomicBool::new(false));
const REDIST_INTENT_SUFFIX: &str = ".redist.intent.msgpack";
const ORPHAN_BLOB_GRACE: Duration = Duration::from_secs(5);

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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RedistributionExecutionMode {
    #[default]
    Availability,
    Strict,
    Scrub,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RedistributionScope {
    #[default]
    All,
    Blob,
    Index,
}

impl RedistributionScope {
    fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Blob => "blob",
            Self::Index => "index",
        }
    }

    fn from_str(raw: &str) -> Self {
        match raw {
            "blob" | "blob_only" => Self::Blob,
            "index" | "index_only" => Self::Index,
            _ => Self::All,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RedistributionIntent {
    tx_id: String,
    fragment: String,
    object_key: String,
    version_id: String,
    is_trash: bool,
    scope: String,
    phase: String,
    created_at_unix_ms: i64,
    updated_at_unix_ms: i64,
}

impl RedistributionIntent {
    fn scope_mode(&self) -> RedistributionScope {
        RedistributionScope::from_str(&self.scope)
    }
}

impl RedistributionExecutionMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Availability => "availability",
            Self::Strict => "strict",
            Self::Scrub => "scrub",
        }
    }

    fn maintenance_access_mode(self) -> MaintenanceAccessMode {
        match self {
            Self::Availability => MaintenanceAccessMode::Availability,
            Self::Strict => MaintenanceAccessMode::ReadOnly,
            Self::Scrub => MaintenanceAccessMode::Quiesced,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RedistributionRunOptions {
    pub fast_mode: bool,
    pub execution_mode: RedistributionExecutionMode,
    pub scope: RedistributionScope,
    pub target_fragment: Option<String>,
    pub durability_mode: RedistributionDurabilityMode,
    pub object_concurrency: usize,
    pub status_batch: u64,
}

fn default_object_concurrency(
    execution_mode: RedistributionExecutionMode,
    fast_mode: bool,
) -> usize {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let ram_gib = detect_total_ram_gib();
    let ram_cap = match execution_mode {
        RedistributionExecutionMode::Availability => match ram_gib {
            0..=2 => 1,
            3..=4 => 2,
            5..=8 => 3,
            9..=16 => 4,
            17..=32 => 6,
            33..=64 => 8,
            _ => 10,
        },
        RedistributionExecutionMode::Strict => {
            if fast_mode {
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
            }
        }
        RedistributionExecutionMode::Scrub => {
            if fast_mode {
                match ram_gib {
                    0..=2 => 2,
                    3..=4 => 4,
                    5..=8 => 10,
                    9..=16 => 16,
                    17..=32 => 24,
                    33..=64 => 32,
                    _ => 48,
                }
            } else {
                match ram_gib {
                    0..=2 => 2,
                    3..=4 => 4,
                    5..=8 => 8,
                    9..=16 => 12,
                    17..=32 => 16,
                    33..=64 => 24,
                    _ => 32,
                }
            }
        }
    };

    let cpu_default = match execution_mode {
        RedistributionExecutionMode::Availability => {
            if fast_mode {
                (cpus / 2).clamp(1, 4)
            } else {
                (cpus / 3).clamp(1, 3)
            }
        }
        RedistributionExecutionMode::Strict => {
            if fast_mode {
                (cpus.saturating_mul(2)).clamp(4, 48)
            } else {
                cpus.clamp(2, 16)
            }
        }
        RedistributionExecutionMode::Scrub => {
            if fast_mode {
                (cpus.saturating_mul(3)).clamp(6, 64)
            } else {
                (cpus.saturating_mul(2)).clamp(4, 32)
            }
        }
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

fn default_status_batch(execution_mode: RedistributionExecutionMode, fast_mode: bool) -> u64 {
    let ram_gib = detect_total_ram_gib();
    match execution_mode {
        RedistributionExecutionMode::Availability => match ram_gib {
            0..=4 => 8,
            5..=8 => 16,
            9..=32 => 32,
            _ => 64,
        },
        RedistributionExecutionMode::Strict => {
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
        RedistributionExecutionMode::Scrub => {
            if fast_mode {
                match ram_gib {
                    0..=4 => 64,
                    5..=8 => 128,
                    9..=32 => 256,
                    _ => 512,
                }
            } else {
                match ram_gib {
                    0..=4 => 32,
                    5..=8 => 64,
                    9..=32 => 128,
                    _ => 256,
                }
            }
        }
    }
}

impl Default for RedistributionRunOptions {
    fn default() -> Self {
        Self {
            fast_mode: false,
            execution_mode: RedistributionExecutionMode::Availability,
            scope: RedistributionScope::All,
            target_fragment: None,
            durability_mode: RedistributionDurabilityMode::Auto,
            object_concurrency: default_object_concurrency(
                RedistributionExecutionMode::Availability,
                false,
            ),
            status_batch: default_status_batch(RedistributionExecutionMode::Availability, false),
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

fn normalize_redistribution_opts_for_layout(
    mut opts: RedistributionRunOptions,
    live_blob_disks: usize,
    live_index_roots: usize,
) -> RedistributionRunOptions {
    let blob_width = live_blob_disks.max(1);
    let index_width = live_index_roots.max(1);
    let io_width = blob_width.min(index_width.saturating_mul(2)).max(1);

    let object_cap = match opts.execution_mode {
        RedistributionExecutionMode::Availability => io_width.clamp(1, 4),
        RedistributionExecutionMode::Strict => io_width.saturating_mul(4).clamp(4, 32),
        RedistributionExecutionMode::Scrub => io_width.saturating_mul(8).clamp(8, 64),
    };

    opts.object_concurrency = opts.object_concurrency.min(object_cap).max(1);
    opts
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
            "batched" | "async" | "fast" | "relaxed" | "availability" => {
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

fn parse_execution_mode_param(params: &HashMap<String, String>) -> RedistributionExecutionMode {
    let mode = params
        .get("mode")
        .or_else(|| params.get("check_mode"))
        .or_else(|| params.get("execution_mode"))
        .map(|v| v.trim().to_ascii_lowercase());
    match mode.as_deref() {
        Some("strict") | Some("readonly") | Some("read_only") => {
            return RedistributionExecutionMode::Strict;
        }
        Some("scrub") | Some("quiesced") | Some("offline") => {
            return RedistributionExecutionMode::Scrub;
        }
        Some("checksum") => return RedistributionExecutionMode::Strict,
        _ => {}
    }

    let access_mode = params
        .get("access_mode")
        .map(|v| v.trim().to_ascii_lowercase());
    match access_mode.as_deref() {
        Some("strict") | Some("readonly") | Some("read_only") => {
            RedistributionExecutionMode::Strict
        }
        Some("scrub") | Some("quiesced") | Some("offline") => RedistributionExecutionMode::Scrub,
        _ => RedistributionExecutionMode::Availability,
    }
}

fn parse_scope_param(params: &HashMap<String, String>) -> RedistributionScope {
    let scope = params
        .get("scope")
        .or_else(|| params.get("redistribution_scope"))
        .map(|v| v.trim().to_ascii_lowercase());
    match scope.as_deref() {
        Some("blob") | Some("blob_only") => RedistributionScope::Blob,
        Some("index") | Some("index_only") => RedistributionScope::Index,
        _ => RedistributionScope::All,
    }
}

fn redistribution_pacing_delay(
    execution_mode: RedistributionExecutionMode,
    fast_mode: bool,
) -> Option<Duration> {
    if execution_mode != RedistributionExecutionMode::Availability {
        return None;
    }

    Some(if fast_mode {
        Duration::from_millis(1)
    } else {
        Duration::from_millis(3)
    })
}

struct StatusDelta {
    scanned: u64,
    needs: u64,
    migrated: u64,
    errors: u64,
    last_object: Option<String>,
    last_error: Option<String>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RedistributionNeeds {
    pub blob: bool,
    pub index: bool,
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

#[inline]
fn is_stopped_error(err: &str) -> bool {
    err.contains("Stopped")
}

#[inline]
fn is_deferrable_owner_unavailability(err: &AppError) -> bool {
    matches!(err, AppError::ServiceUnavailable(message) if message.contains("authoritative owner index root unavailable"))
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
    let fast_mode = parse_bool_param(&params, "fast_mode", false);
    let read_only_fast = parse_bool_param(&params, "read_only_fast", false);
    let ram_gib = detect_total_ram_gib();
    let cpu_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let (max_object_concurrency, max_status_batch) = redistribution_limits_for_ram_gib(ram_gib);
    let durability_mode = parse_durability_mode_param(&params, fast_mode);
    let scope = parse_scope_param(&params);
    let target_fragment = params
        .get("fragment")
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let mut execution_mode = parse_execution_mode_param(&params);
    if read_only_fast && execution_mode == RedistributionExecutionMode::Availability {
        execution_mode = RedistributionExecutionMode::Strict;
    }
    let object_concurrency = clamp_usize(
        params
            .get("object_concurrency")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or_else(|| default_object_concurrency(execution_mode, fast_mode)),
        1,
        max_object_concurrency,
    );
    let status_batch = clamp_u64(
        params
            .get("status_batch")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or_else(|| default_status_batch(execution_mode, fast_mode)),
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
        execution_mode,
        scope,
        target_fragment,
        durability_mode,
        object_concurrency,
        status_batch,
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

fn get_shard_dir_custom(
    disk_root: &Path,
    fragment_hash: &str,
    version_id: &str,
    is_trash: bool,
) -> PathBuf {
    let _ = is_trash;
    fragment_root_on(disk_root, fragment_hash)
        .join(N1_BLOB_DIR)
        .join(version_id)
}

fn get_partition_intent_paths_custom(
    index_roots: &[PathBuf],
    fragment_hash: &str,
    object_key: &str,
) -> Vec<PathBuf> {
    use sha2::{Digest, Sha256};

    if index_roots.is_empty() {
        return Vec::new();
    }

    let partition_count = fragment_index_partition_count(fragment_hash).unwrap_or(1);
    let canonical_key = ObjectPathManager::canonical_object_key(object_key);
    let mut object_hasher = Sha256::new();
    for part in ["object", fragment_hash, canonical_key.as_str()] {
        object_hasher.update(part.as_bytes());
        object_hasher.update([0u8]);
    }
    let object_hash: [u8; 32] = object_hasher.finalize().into();
    let partition_id =
        (u16::from(object_hash[0]) << 8 | u16::from(object_hash[1])) % partition_count;

    let mut route_hasher = Sha256::new();
    for part in ["fragment", fragment_hash] {
        route_hasher.update(part.as_bytes());
        route_hasher.update([0u8]);
    }
    let route_hash: [u8; 32] = route_hasher.finalize().into();
    let fragment_owner =
        usize::from(u16::from(route_hash[0]) << 8 | u16::from(route_hash[1])) % index_roots.len();
    let owner_idx = (fragment_owner + usize::from(partition_id)) % index_roots.len();
    let mut selected: Vec<&PathBuf> = vec![&index_roots[owner_idx]];
    for (idx, root) in index_roots.iter().enumerate() {
        if idx != owner_idx {
            selected.push(root);
        }
    }

    selected
        .into_iter()
        .map(|root| {
            fragment_root_on(root, fragment_hash)
                .join(".intents")
                .join(format!("partition_{partition_id:04x}.n1"))
        })
        .collect()
}

fn get_all_intent_paths_custom(
    index_roots: &[PathBuf],
    fragment_hash: &str,
    object_key: &str,
    version_id: &str,
    is_trash: bool,
) -> Vec<PathBuf> {
    get_partition_intent_paths_custom(index_roots, fragment_hash, object_key)
        .into_iter()
        .map(|db_path| {
            let partition_stem = db_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or("partition");
            let object_hash = ObjectPathManager::object_key_hash(object_key);
            db_path.with_file_name(format!(
                "{partition_stem}.{object_hash}.{version_id}.{is_trash}{}",
                REDIST_INTENT_SUFFIX
            ))
        })
        .collect()
}

fn object_index_dir_for_root(
    root: &Path,
    fragment_hash: &str,
    object_key: &str,
    _is_trash: bool,
) -> PathBuf {
    let object_id = object_key
        .trim()
        .trim_start_matches('/')
        .strip_prefix("@o_")
        .unwrap_or_else(|| object_key.trim().trim_start_matches('/'));
    index_store::live_object_dir(root, fragment_hash, object_id).unwrap_or_else(|_| {
        fragment_root_on(root, fragment_hash)
            .join(".objects")
            .join(".invalid-object-id")
    })
}

async fn object_index_dir_has_ndx(path: &Path) -> bool {
    let Ok(mut entries) = tokio_fs::read_dir(path).await else {
        return false;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name();
        if name.to_string_lossy().ends_with(".ndx") {
            return true;
        }
    }
    false
}

fn intent_dedupe_key(fragment: &str, object_key: &str, version_id: &str, is_trash: bool) -> String {
    format!("{fragment}\u{1f}{object_key}\u{1f}{version_id}\u{1f}{is_trash}")
}

async fn write_intent_state(
    intent_paths: &[PathBuf],
    intent: &RedistributionIntent,
    strict_durability: bool,
) -> Result<(), AppError> {
    if intent_paths.is_empty() {
        return Err(AppError::InternalServerError(
            "no intent paths resolved for redistribution object".into(),
        ));
    }
    let payload = rmp_serde::to_vec(intent).map_err(|e| {
        AppError::InternalServerError(format!("serialize redistribution intent: {}", e))
    })?;
    let mut wrote_any = false;
    for path in intent_paths {
        if let Some(parent) = path.parent() {
            let _ = tokio_fs::create_dir_all(parent).await;
        }
        match durable_atomic_write_bytes_with_mode(path, &payload, strict_durability).await {
            Ok(_) => wrote_any = true,
            Err(e) => warn!(
                "Failed to write redistribution intent {}: {}",
                path.display(),
                e
            ),
        }
    }

    if !wrote_any {
        return Err(AppError::InternalServerError(
            "failed to persist redistribution intent on any reachable index root".into(),
        ));
    }
    Ok(())
}

async fn remove_intent_paths_best_effort(intent_paths: &[PathBuf]) {
    for path in intent_paths {
        let _ = tokio_fs::remove_file(path).await;
    }
}

pub async fn collect_redistribution_intent_debt(
    index_roots: &[PathBuf],
    target_fragment: Option<String>,
) -> RedistributionIntentDebtSnapshot {
    if index_roots.is_empty() {
        return RedistributionIntentDebtSnapshot::default();
    }

    let roots = index_roots.to_vec();
    tokio::task::spawn_blocking(move || {
        let now_ms = Utc::now().timestamp_millis();
        let mut grouped: HashMap<String, RedistributionIntent> = HashMap::new();
        let mut fragments = HashSet::new();

        for root in roots {
            let direct_intent_dir = root.join(".intents");
            if target_fragment.is_none() && direct_intent_dir.exists() {
                for entry in WalkDir::new(direct_intent_dir)
                    .into_iter()
                    .filter_map(|e| e.ok())
                {
                    if !entry.file_type().is_file() {
                        continue;
                    }
                    let name = entry.file_name().to_string_lossy();
                    if !name.ends_with(REDIST_INTENT_SUFFIX) {
                        continue;
                    }
                    let Ok(bytes) = std::fs::read(entry.path()) else {
                        continue;
                    };
                    let Ok(intent) = rmp_serde::from_slice::<RedistributionIntent>(&bytes) else {
                        continue;
                    };
                    let key = intent_dedupe_key(
                        &intent.fragment,
                        &intent.object_key,
                        &intent.version_id,
                        intent.is_trash,
                    );
                    grouped
                        .entry(key)
                        .and_modify(|current| {
                            if intent.updated_at_unix_ms > current.updated_at_unix_ms {
                                *current = intent.clone();
                            }
                        })
                        .or_insert(intent);
                }
                continue;
            }

            if !root.exists() {
                continue;
            }
            let scan_root = if let Some(fragment) = target_fragment.as_ref() {
                let scoped = fragment_root_on(&root, fragment).join(".intents");
                if !scoped.exists() {
                    continue;
                }
                scoped
            } else {
                root
            };
            for entry in WalkDir::new(scan_root).into_iter().filter_map(|e| e.ok()) {
                if !entry.file_type().is_file() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy();
                if !name.ends_with(REDIST_INTENT_SUFFIX) {
                    continue;
                }
                let Ok(bytes) = std::fs::read(entry.path()) else {
                    continue;
                };
                let Ok(intent) = rmp_serde::from_slice::<RedistributionIntent>(&bytes) else {
                    continue;
                };
                if target_fragment
                    .as_ref()
                    .is_some_and(|target| intent.fragment != *target)
                {
                    continue;
                }
                let key = intent_dedupe_key(
                    &intent.fragment,
                    &intent.object_key,
                    &intent.version_id,
                    intent.is_trash,
                );
                grouped
                    .entry(key)
                    .and_modify(|current| {
                        if intent.updated_at_unix_ms > current.updated_at_unix_ms {
                            *current = intent.clone();
                        }
                    })
                    .or_insert(intent);
            }
        }

        let mut snapshot = RedistributionIntentDebtSnapshot::default();
        let mut oldest_created_ms: Option<i64> = None;
        for intent in grouped.values() {
            snapshot.pending_intents = snapshot.pending_intents.saturating_add(1);
            if intent.phase.eq_ignore_ascii_case("failed") {
                snapshot.failed_intents = snapshot.failed_intents.saturating_add(1);
            }
            fragments.insert(intent.fragment.clone());
            oldest_created_ms = Some(match oldest_created_ms {
                Some(current) => current.min(intent.created_at_unix_ms),
                None => intent.created_at_unix_ms,
            });
        }
        snapshot.fragments_with_pending_intents = fragments.len() as u64;
        snapshot.oldest_pending_intent_age_seconds = oldest_created_ms.and_then(|created_at| {
            if created_at <= 0 || now_ms <= created_at {
                None
            } else {
                Some(((now_ms - created_at) / 1000) as u64)
            }
        });
        snapshot
    })
    .await
    .unwrap_or_default()
}

async fn replay_pending_intents_for_fragment(
    fragment_hash: &str,
    live_index: &[PathBuf],
    live_blob: &[PathBuf],
    target_data_shards: usize,
    target_parity_shards: usize,
    target_stripe_size: usize,
    strict_durability: bool,
) -> Result<(), AppError> {
    let mut intent_files = Vec::<PathBuf>::new();
    for root in live_index {
        let intent_dir = fragment_root_on(root, fragment_hash).join(".intents");
        if !intent_dir.exists() {
            continue;
        }
        for entry in WalkDir::new(intent_dir).into_iter().filter_map(|e| e.ok()) {
            if !entry.file_type().is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy();
            if name.ends_with(REDIST_INTENT_SUFFIX) {
                intent_files.push(entry.path().to_path_buf());
            }
        }
    }
    if intent_files.is_empty() {
        return Ok(());
    }

    let mut grouped: HashMap<String, (RedistributionIntent, Vec<PathBuf>)> = HashMap::new();
    for path in intent_files {
        let bytes = match tokio_fs::read(&path).await {
            Ok(v) => v,
            Err(_) => continue,
        };
        let intent = match rmp_serde::from_slice::<RedistributionIntent>(&bytes) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let key = intent_dedupe_key(
            &intent.fragment,
            &intent.object_key,
            &intent.version_id,
            intent.is_trash,
        );
        grouped
            .entry(key)
            .and_modify(|(_, paths)| paths.push(path.clone()))
            .or_insert_with(|| (intent, vec![path]));
    }

    let mut first_err: Option<AppError> = None;
    for (_, (intent, mut found_paths)) in grouped {
        if should_stop() {
            break;
        }
        let mut all_intent_paths = get_all_intent_paths_custom(
            live_index,
            &intent.fragment,
            &intent.object_key,
            &intent.version_id,
            intent.is_trash,
        );
        all_intent_paths.append(&mut found_paths);
        all_intent_paths.sort();
        all_intent_paths.dedup();

        let mut meta = if intent.is_trash {
            match index::read_trashed_object_version(
                &intent.fragment,
                &intent.object_key,
                &intent.version_id,
            )
            .await
            {
                Ok(v) => v,
                Err(_) => {
                    remove_intent_paths_best_effort(&all_intent_paths).await;
                    continue;
                }
            }
        } else {
            match index::read_object_version(
                &intent.fragment,
                &intent.object_key,
                &intent.version_id,
            )
            .await
            {
                Ok(v) => v,
                Err(_) => {
                    remove_intent_paths_best_effort(&all_intent_paths).await;
                    continue;
                }
            }
        };

        let scope = intent.scope_mode();
        if scope != RedistributionScope::Index && live_blob.is_empty() {
            continue;
        }
        let needs = check_needs_redistribution(
            &meta,
            scope,
            target_data_shards,
            target_parity_shards,
            target_stripe_size,
            live_blob.len(),
            live_blob,
            live_index,
            intent.is_trash,
        )
        .await;

        if !needs.blob && !needs.index {
            remove_intent_paths_best_effort(&all_intent_paths).await;
            continue;
        }

        let resume_result = match scope {
            RedistributionScope::Index => {
                if !needs.index {
                    Ok(())
                } else {
                    redistribute_object_index_only(
                        &meta,
                        live_index,
                        intent.is_trash,
                        strict_durability,
                    )
                    .await
                }
            }
            RedistributionScope::Blob => {
                if !needs.blob {
                    Ok(())
                } else {
                    redistribute_object_streaming(
                        &mut meta,
                        target_data_shards,
                        target_parity_shards,
                        target_stripe_size,
                        live_blob,
                        live_index,
                        intent.is_trash,
                        strict_durability,
                    )
                    .await
                }
            }
            RedistributionScope::All => {
                if needs.blob {
                    redistribute_object_streaming(
                        &mut meta,
                        target_data_shards,
                        target_parity_shards,
                        target_stripe_size,
                        live_blob,
                        live_index,
                        intent.is_trash,
                        strict_durability,
                    )
                    .await
                } else if needs.index {
                    redistribute_object_index_only(
                        &meta,
                        live_index,
                        intent.is_trash,
                        strict_durability,
                    )
                    .await
                } else {
                    Ok(())
                }
            }
        };

        match resume_result {
            Ok(_) => remove_intent_paths_best_effort(&all_intent_paths).await,
            Err(e) => {
                if should_stop() || is_stopped_error(&e.to_string()) {
                    break;
                }
                warn!(
                    "Failed replaying redistribution intent for {}/{} (v:{}) [Trash:{}]: {}",
                    intent.fragment, intent.object_key, intent.version_id, intent.is_trash, e
                );
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
    }

    if let Some(err) = first_err {
        return Err(err);
    }

    Ok(())
}

pub async fn replay_pending_intents_for_fragment_now(
    fragment_hash: &str,
    strict_durability: bool,
) -> Result<(), AppError> {
    let index_cfg = INDEX_STORE_DISKS
        .get()
        .ok_or(AppError::InternalServerError(
            "index roots not configured".to_string(),
        ))?
        .iter()
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    let live_index = reachable_roots(&index_cfg).await;
    if live_index.is_empty() {
        return Ok(());
    }

    let live_blob = reachable_roots(&ObjectPathManager::new().get_blob_roots()).await;
    let target_data_shards = DATA_SHARDS_ATOMIC
        .get()
        .map(|value| value.load(Ordering::Relaxed))
        .unwrap_or(1)
        .max(1);
    let target_parity_shards = PARITY_SHARDS_ATOMIC
        .get()
        .map(|value| value.load(Ordering::Relaxed))
        .unwrap_or(0);
    let target_stripe_size = STRIPE_SIZE_ATOMIC
        .get()
        .map(|value| value.load(Ordering::Relaxed))
        .unwrap_or_else(default_stripe_size);

    replay_pending_intents_for_fragment(
        fragment_hash,
        &live_index,
        &live_blob,
        target_data_shards,
        target_parity_shards,
        target_stripe_size,
        strict_durability,
    )
    .await
}

async fn reachable_roots(roots: &[PathBuf]) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for r in roots {
        if let Ok(meta) = tokio_fs::metadata(r).await {
            if meta.is_dir() {
                out.push(r.clone());
            }
        }
    }
    out
}

async fn replicate_fragment_metadata_best_effort(fragment_hash: &str) {
    if let Ok(meta) = read_fragment_metadata(fragment_hash).await {
        let _ = write_fragment_metadata(&meta).await;
    }
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

fn dedupe_id_from_metadata(meta: &ObjectMetadata, is_trash: bool) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    meta.fragment.hash(&mut h);
    meta.object_key.hash(&mut h);
    meta.version_id.hash(&mut h);
    is_trash.hash(&mut h);
    h.finish()
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
                        let metadata_path = ent.path().join(FRAGMENT_METADATA_FILE);
                        if metadata_path.is_file() {
                            out.insert(name.to_string());
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

async fn fragment_has_blob_payload(fragment: &str, blob_roots: &[PathBuf]) -> bool {
    let fragment = fragment.to_string();
    let roots = blob_roots.to_vec();
    let active_session_ids = index::list_active_sessions(&fragment)
        .await
        .map(|sessions| {
            sessions
                .into_iter()
                .filter(|session| session.finalized_at.is_none() && session.aborted_at.is_none())
                .map(|session| session.manifest.version_id)
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();
    let now = SystemTime::now();
    tokio::task::spawn_blocking(move || {
        for root in roots {
            let blob_dir = fragment_root_on(&root, &fragment).join(N1_BLOB_DIR);
            let Ok(entries) = std::fs::read_dir(&blob_dir) else {
                continue;
            };
            for entry in entries.filter_map(|entry| entry.ok()) {
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }
                let version_id = entry.file_name().to_string_lossy().to_string();
                if active_session_ids.contains(&version_id) {
                    continue;
                }
                let modified = std::fs::metadata(&path)
                    .and_then(|meta| meta.modified())
                    .unwrap_or(now);
                if now.duration_since(modified).unwrap_or(Duration::ZERO) <= ORPHAN_BLOB_GRACE {
                    continue;
                }
                if std::fs::read_dir(&path)
                    .ok()
                    .and_then(|mut parts| parts.next())
                    .is_some()
                {
                    return true;
                }
            }
        }
        false
    })
    .await
    .unwrap_or(false)
}

async fn fragment_has_indexed_payload(
    fragment: &str,
    active_index_roots: &[PathBuf],
) -> Result<bool, AppError> {
    let fragment_owned = fragment.to_string();
    let roots = active_index_roots.to_vec();
    let has_object_index = tokio::task::spawn_blocking(move || {
        index_store::fragment_has_object_index_records(&fragment_owned, &roots)
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("N1 object index scan join: {e}")))?;
    if has_object_index {
        return Ok(true);
    }

    let live = index::scan_fragment_live_metadata_page(fragment, None, None, 1, None, true).await?;
    if !live.objects.is_empty() {
        return Ok(true);
    }

    let trash =
        index::scan_fragment_trash_metadata_page(fragment, None, None, 1, None, true).await?;
    Ok(!trash.objects.is_empty())
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
fn rename_exchange(a: &Path, b: &Path) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let a_c = CString::new(a.as_os_str().as_bytes()).unwrap();
    let b_c = CString::new(b.as_os_str().as_bytes()).unwrap();

    let rc = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            a_c.as_ptr(),
            libc::AT_FDCWD,
            b_c.as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    };

    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

async fn publish_tmp_overwrite(
    tmp: &Path,
    final_p: &Path,
    parent: &Path,
    file_name: &str,
    sync_parent_immediately: bool,
) -> Result<(), AppError> {
    let final_exists = tokio_fs::try_exists(final_p).await.unwrap_or(false);

    if !final_exists {
        tokio_fs::rename(tmp, final_p)
            .await
            .map_err(|e| AppError::Io(e, "commit rename".into()))?;
        if sync_parent_immediately {
            let _ = sync_dir(parent).await;
        }
        return Ok(());
    }

    #[cfg(target_os = "linux")]
    {
        let a = tmp.to_path_buf();
        let b = final_p.to_path_buf();

        let ex = tokio::task::spawn_blocking(move || rename_exchange(&a, &b))
            .await
            .map_err(|e| AppError::InternalServerError(format!("rename_exchange join: {}", e)))?;

        if ex.is_ok() {
            let gc_name = format!(
                "{}.preexisting.{}{}",
                file_name,
                Ulid::new().to_string(),
                GC_SUFFIX
            );
            let gc_path = parent.join(gc_name);
            let _ = tokio_fs::rename(tmp, &gc_path).await;
            if sync_parent_immediately {
                let _ = sync_dir(parent).await;
            }
            return Ok(());
        }
    }

    let gc_name = format!(
        "{}.preexisting.{}{}",
        file_name,
        Ulid::new().to_string(),
        GC_SUFFIX
    );
    let gc_path = parent.join(gc_name);

    tokio_fs::rename(final_p, &gc_path)
        .await
        .map_err(|e| AppError::Io(e, "rename final->gc".into()))?;

    tokio_fs::rename(tmp, final_p)
        .await
        .map_err(|e| AppError::Io(e, "rename tmp->final".into()))?;

    if sync_parent_immediately {
        let _ = sync_dir(parent).await;
    }
    Ok(())
}

#[cfg(test)]
pub async fn run_redistribution_job(
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
    let _write_gate = acquire_maintenance_mode(opts.execution_mode.maintenance_access_mode());
    let _io_guard = acquire_disk_io(
        DiskIoClass::BackgroundRedistribution,
        opts.target_fragment.as_deref(),
        "redistribution job",
    )
    .await?;
    let strict_durability = opts.strict_durability();
    let effective_durability = opts.effective_durability_mode();
    let pacing_delay = redistribution_pacing_delay(opts.execution_mode, opts.fast_mode);

    {
        let mut st = REDIST_STATUS.write().await;
        *st = RedistributionJobStatus {
            is_running: true,
            phase: format!(
                "Initializing (mode={}, scope={}, fast_mode={}, durability={}=>{}, object_concurrency={}, status_batch={}, dry_run={})",
                opts.execution_mode.as_str(),
                opts.scope.as_str(),
                opts.fast_mode,
                opts.durability_mode.as_str(),
                effective_durability,
                opts.object_concurrency,
                opts.status_batch,
                dry_run
            ),
            fast_mode: opts.fast_mode,
            durability_mode: effective_durability.to_string(),
            scope: opts.scope.as_str().to_string(),
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

    let index_roots_cfg: Vec<PathBuf> = INDEX_STORE_DISKS
        .get()
        .ok_or(AppError::InternalServerError(
            "Index Store Config not initialized".into(),
        ))?
        .iter()
        .map(PathBuf::from)
        .collect();

    let mut layout = LiveLayout::new(blob_roots_cfg.clone(), index_roots_cfg.clone()).await;

    if opts.scope != RedistributionScope::Index && layout.live_blob.is_empty() {
        return Err(AppError::InternalServerError(
            "No reachable blob disks".into(),
        ));
    }
    if layout.live_index.is_empty() {
        return Err(AppError::InternalServerError(
            "No reachable index roots".into(),
        ));
    }
    let opts = normalize_redistribution_opts_for_layout(
        opts,
        layout.live_blob.len(),
        layout.live_index.len(),
    );

    info!(
        "Starting Redistribution Job (Active & Trash). mode={} scope={} target={}D+{}P stripe={} bytes blob_cfg={} index_cfg={} dry_run={} fast_mode={} durability={}=>{} object_concurrency={} status_batch={}",
        opts.execution_mode.as_str(),
        opts.scope.as_str(),
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
    info!(
        "Redistribution runtime caps after layout normalization: mode={} object_concurrency={} live_blob_disks={} live_index_roots={}",
        opts.execution_mode.as_str(),
        opts.object_concurrency,
        layout.live_blob.len(),
        layout.live_index.len()
    );

    let mut touched_fragments: HashSet<String> = HashSet::new();
    let mut scanned_fragments: HashSet<String> = HashSet::new();
    let mut total_task_errors: u64 = 0;
    let mut last_task_error: Option<String> = None;
    let fragments = match opts.target_fragment.as_ref() {
        Some(fragment) => vec![fragment.clone()],
        None => list_all_fragments(layout.live_index.as_ref()).await,
    };

    {
        let mut st = REDIST_STATUS.write().await;
        st.phase = format!(
            "Preparing (fragments={}, scope={})",
            fragments.len(),
            opts.scope.as_str()
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

        if live_index.is_empty()
            || (opts.scope != RedistributionScope::Index && live_blob.is_empty())
        {
            continue;
        }

        replicate_fragment_metadata_best_effort(&fragment_hash).await;
        if !dry_run && opts.scope != RedistributionScope::Index {
            validate_fragment_project_headroom_for_redistribution(
                &fragment_hash,
                live_blob.as_ref(),
            )
            .await?;
        }
        if !dry_run {
            if let Err(err) = replay_pending_intents_for_fragment(
                &fragment_hash,
                live_index.as_ref(),
                live_blob.as_ref(),
                target_data_shards,
                target_parity_shards,
                target_stripe_size,
                strict_durability,
            )
            .await
            {
                warn!(
                    "Failed replaying pending redistribution intents for fragment {} before scan: {}",
                    fragment_hash, err
                );
            }
        }

        {
            let mut st = REDIST_STATUS.write().await;
            st.current_fragment = fragment_hash.clone();
            st.total_fragments_scanned += 1;
            st.phase = format!("Scanning Fragment {}", fragment_hash);
            ws_manager.broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));
        }
        scanned_fragments.insert(fragment_hash.clone());

        let has_blob_payload = fragment_has_blob_payload(&fragment_hash, live_blob.as_ref()).await;
        let has_indexed_payload =
            fragment_has_indexed_payload(&fragment_hash, live_index.as_ref()).await?;
        if has_blob_payload && !has_indexed_payload {
            info!(
                "Fragment {} has headless blob payload with no live/trash index; leaving it for GC",
                fragment_hash
            );
            continue;
        }

        let mut seen_meta_ids: HashSet<u64> = HashSet::new();

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
            let mut continuation_token = None;
            loop {
                if should_stop() {
                    break;
                }

                let page = if is_trash {
                    index::scan_fragment_trash_metadata_page(
                        &fragment_hash,
                        None,
                        None,
                        usize::try_from(status_batch.max(1)).unwrap_or(usize::MAX),
                        continuation_token.clone(),
                        true,
                    )
                    .await?
                } else {
                    index::scan_fragment_live_metadata_page(
                        &fragment_hash,
                        None,
                        None,
                        usize::try_from(status_batch.max(1)).unwrap_or(usize::MAX),
                        continuation_token.clone(),
                        true,
                    )
                    .await?
                };

                for meta in page.objects {
                    let is_trash_flag = is_trash;
                    let id = dedupe_id_from_metadata(&meta, is_trash_flag);
                    if !seen_meta_ids.insert(id) {
                        continue;
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
                        opts.scope,
                        target_data_shards,
                        target_parity_shards,
                        target_stripe_size,
                        live_blob.len(),
                        &live_blob,
                        &live_index,
                        is_trash_flag,
                    )
                    .await;

                    if !needs.blob && !needs.index {
                        continue;
                    }

                    let object_label = format!(
                        "{}/{} (v:{}) [Trash:{}]",
                        meta.fragment, meta.object_key, meta.version_id, is_trash_flag
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
                    let task_scope = opts.scope;
                    let task_needs = needs;
                    in_flight.push(async move {
                        let mut meta = meta;
                        let fragment = meta.fragment.clone();
                        let key = meta.object_key.clone();
                        let version = meta.version_id.clone();
                        let intent_paths = get_all_intent_paths_custom(
                            &live_index_for_task,
                            &fragment,
                            &key,
                            &version,
                            is_trash_flag,
                        );
                        let tx_id = Ulid::new().to_string();
                        let created_ms = Utc::now().timestamp_millis();
                        let mut intent = RedistributionIntent {
                            tx_id,
                            fragment: fragment.clone(),
                            object_key: key.clone(),
                            version_id: version.clone(),
                            is_trash: is_trash_flag,
                            scope: task_scope.as_str().to_string(),
                            phase: "prepare".to_string(),
                            created_at_unix_ms: created_ms,
                            updated_at_unix_ms: created_ms,
                        };

                        if let Err(e) =
                            write_intent_state(&intent_paths, &intent, task_strict_durability).await
                        {
                            return Err((
                                fragment,
                                key,
                                version,
                                is_trash_flag,
                                format!("failed to persist redistribution intent: {}", e),
                            ));
                        }
                        intent.phase = "running".to_string();
                        intent.updated_at_unix_ms = Utc::now().timestamp_millis();
                        if let Err(e) =
                            write_intent_state(&intent_paths, &intent, task_strict_durability).await
                        {
                            return Err((
                                fragment,
                                key,
                                version,
                                is_trash_flag,
                                format!("failed to update redistribution intent: {}", e),
                            ));
                        }

                        let result = match task_scope {
                            RedistributionScope::Index => {
                                if !task_needs.index {
                                    Ok(())
                                } else {
                                    redistribute_object_index_only(
                                        &meta,
                                        &live_index_for_task,
                                        is_trash_flag,
                                        task_strict_durability,
                                    )
                                    .await
                                }
                            }
                            RedistributionScope::Blob => {
                                if !task_needs.blob {
                                    Ok(())
                                } else {
                                    redistribute_object_streaming(
                                        &mut meta,
                                        target_data_shards,
                                        target_parity_shards,
                                        target_stripe_size,
                                        &live_blob_for_task,
                                        &live_index_for_task,
                                        is_trash_flag,
                                        task_strict_durability,
                                    )
                                    .await
                                }
                            }
                            RedistributionScope::All => {
                                if task_needs.blob {
                                    redistribute_object_streaming(
                                        &mut meta,
                                        target_data_shards,
                                        target_parity_shards,
                                        target_stripe_size,
                                        &live_blob_for_task,
                                        &live_index_for_task,
                                        is_trash_flag,
                                        task_strict_durability,
                                    )
                                    .await
                                } else if task_needs.index {
                                    redistribute_object_index_only(
                                        &meta,
                                        &live_index_for_task,
                                        is_trash_flag,
                                        task_strict_durability,
                                    )
                                    .await
                                } else {
                                    Ok(())
                                }
                            }
                        };

                        match result {
                            Ok(_) => {
                                remove_intent_paths_best_effort(&intent_paths).await;
                                Ok(Some((fragment, key, version, is_trash_flag)))
                            }
                            Err(e) if is_deferrable_owner_unavailability(&e) => {
                                intent.phase = "deferred".to_string();
                                intent.updated_at_unix_ms = Utc::now().timestamp_millis();
                                let _ = write_intent_state(
                                    &intent_paths,
                                    &intent,
                                    task_strict_durability,
                                )
                                .await;
                                Ok(None)
                            }
                            Err(e) => {
                                let err_text = e.to_string();
                                if !(should_stop() || is_stopped_error(&err_text)) {
                                    intent.phase = "failed".to_string();
                                    intent.updated_at_unix_ms = Utc::now().timestamp_millis();
                                    let _ = write_intent_state(
                                        &intent_paths,
                                        &intent,
                                        task_strict_durability,
                                    )
                                    .await;
                                }
                                Err((fragment, key, version, is_trash_flag, err_text))
                            }
                        }
                    });

                    while in_flight.len() >= object_concurrency {
                        if let Some(res) = in_flight.next().await {
                            match res {
                                Ok(Some((fragment, key, version, task_is_trash))) => {
                                    touched_fragments.insert(fragment.clone());
                                    delta.migrated += 1;
                                    delta.last_object = Some(format!(
                                        "{}/{} (v:{}) [Trash:{}]",
                                        fragment, key, version, task_is_trash
                                    ));
                                }
                                Ok(None) => {}
                                Err((fragment, key, version, task_is_trash, err)) => {
                                    if should_stop() || is_stopped_error(&err) {
                                        continue;
                                    }
                                    warn!(
                                        "Redistribute failed for {}/{} (v:{}) [Trash:{}]: {}",
                                        fragment, key, version, task_is_trash, err
                                    );
                                    total_task_errors = total_task_errors.saturating_add(1);
                                    delta.errors += 1;
                                    last_task_error = Some(err.clone());
                                    delta.last_error = Some(err);
                                }
                            }
                            if let Some(delay) = pacing_delay {
                                tokio::time::sleep(delay).await;
                            }
                        }
                        if delta.needs + delta.migrated + delta.errors >= status_batch {
                            flush_status_delta(&ws_manager, start_time, &mut delta).await;
                        }
                    }
                }

                match page.continuation_token {
                    Some(token) => continuation_token = Some(token),
                    None => break,
                }
            }

            while let Some(res) = in_flight.next().await {
                match res {
                    Ok(Some((fragment, key, version, task_is_trash))) => {
                        touched_fragments.insert(fragment.clone());
                        delta.migrated += 1;
                        delta.last_object = Some(format!(
                            "{}/{} (v:{}) [Trash:{}]",
                            fragment, key, version, task_is_trash
                        ));
                    }
                    Ok(None) => {}
                    Err((fragment, key, version, task_is_trash, err)) => {
                        if should_stop() || is_stopped_error(&err) {
                            continue;
                        }
                        warn!(
                            "Redistribute failed for {}/{} (v:{}) [Trash:{}]: {}",
                            fragment, key, version, task_is_trash, err
                        );
                        total_task_errors = total_task_errors.saturating_add(1);
                        delta.errors += 1;
                        last_task_error = Some(err.clone());
                        delta.last_error = Some(err);
                    }
                }
                if let Some(delay) = pacing_delay {
                    tokio::time::sleep(delay).await;
                }
                if delta.needs + delta.migrated + delta.errors >= status_batch {
                    flush_status_delta(&ws_manager, start_time, &mut delta).await;
                }
            }

            flush_status_delta(&ws_manager, start_time, &mut delta).await;
        }
    }

    if !dry_run {
        for fragment in &touched_fragments {
            if let Err(err) = index::sync_fragment_partition_replicas(fragment).await {
                warn!(
                    "Failed to resynchronize N1 index replicas for fragment {}: {}",
                    fragment, err
                );
            }
        }
        for fragment in &scanned_fragments {
            if let Err(err) = index_store::cleanup_stale_index_plane_on_blob_only_roots(fragment) {
                warn!(
                    "Failed to clean stale N1 index plane from blob-only roots for fragment {}: {}",
                    fragment, err
                );
            }
        }
    }

    {
        let mut st = REDIST_STATUS.write().await;
        st.is_running = false;
        st.fast_mode = opts.fast_mode;
        st.durability_mode = effective_durability.to_string();
        st.phase = if should_stop() {
            "Stopped by User".to_string()
        } else if total_task_errors > 0 {
            "Completed with Errors".to_string()
        } else {
            "Completed".to_string()
        };
        if total_task_errors > 0 {
            st.total_errors = st.total_errors.max(total_task_errors);
            st.last_error = last_task_error.clone();
        }
        st.duration_seconds = start_time.elapsed().as_secs();
        ws_manager.broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));
    }

    info!("Redistribution Job Ended.");
    if !should_stop() && total_task_errors > 0 {
        let message = match last_task_error {
            Some(last) => format!(
                "Redistribution finished with {} error(s); last error: {}",
                total_task_errors, last
            ),
            None => format!(
                "Redistribution finished with {} error(s)",
                total_task_errors
            ),
        };
        return Err(AppError::InternalServerError(message));
    }

    Ok(())
}

pub(crate) async fn check_needs_redistribution(
    meta: &ObjectMetadata,
    scope: RedistributionScope,
    target_data: usize,
    target_parity: usize,
    _target_stripe_size: usize,
    target_num_disks: usize,
    target_blob_roots: &[PathBuf],
    target_index_roots: &[PathBuf],
    is_trash: bool,
) -> RedistributionNeeds {
    let check_blob = scope != RedistributionScope::Index;
    let check_index = scope != RedistributionScope::Blob;
    let mut needs = RedistributionNeeds::default();

    if check_blob {
        if meta.parts.is_empty() {
            return needs;
        }

        let part0 = &meta.parts[0];
        let cur_data = part0.shards.iter().filter(|s| !s.is_parity).count();
        let cur_parity = part0.shards.iter().filter(|s| s.is_parity).count();

        if cur_data != target_data || cur_parity != target_parity {
            needs.blob = true;
        }

        if target_num_disks == 0 {
            needs.blob = true;
        }

        if !needs.blob {
            for part in &meta.parts {
                for shard in &part.shards {
                    let expected =
                        (part.part_index as usize + shard.shard_index) % target_num_disks;
                    if expected >= target_blob_roots.len() {
                        needs.blob = true;
                        break;
                    }

                    let target_disk_path = target_blob_roots[expected].to_string_lossy();
                    if !shard.absolute_path.starts_with(target_disk_path.as_ref()) {
                        needs.blob = true;
                        break;
                    }

                    if !tokio_fs::try_exists(Path::new(&shard.absolute_path))
                        .await
                        .unwrap_or(false)
                    {
                        needs.blob = true;
                        break;
                    }

                    let in_trash_path = shard.absolute_path.contains(".trash");
                    if is_trash != in_trash_path {
                        needs.blob = true;
                        break;
                    }
                }
                if needs.blob {
                    break;
                }
            }
        }
    }

    if check_index {
        for root in target_index_roots {
            let path = object_index_dir_for_root(root, &meta.fragment, &meta.object_key, is_trash);
            if !object_index_dir_has_ndx(&path).await {
                needs.index = true;
                break;
            }
        }
    }

    needs
}

async fn cleanup_tmp_files(files: &[PathBuf]) {
    for p in files {
        let _ = tokio_fs::remove_file(p).await;
    }
}

async fn write_redistributed_metadata(
    meta: &ObjectMetadata,
    _target_index_roots: &[PathBuf],
    is_trash: bool,
    _strict_durability: bool,
) -> Result<(), AppError> {
    if is_trash {
        index::publish_trashed_object_version_for_maintenance(meta).await
    } else {
        index::upsert_object_version(meta).await
    }
}

async fn redistribute_object_index_only(
    meta: &ObjectMetadata,
    target_index_roots: &[PathBuf],
    is_trash: bool,
    strict_durability: bool,
) -> Result<(), AppError> {
    let _object_lock =
        index::acquire_object_operation_lock(&meta.fragment, &meta.object_key).await?;
    if should_stop() {
        return Err(AppError::InternalServerError("Stopped".into()));
    }

    let still_present = if is_trash {
        index::read_trashed_object_version(&meta.fragment, &meta.object_key, &meta.version_id)
            .await
            .is_ok()
    } else {
        index::read_object_version(&meta.fragment, &meta.object_key, &meta.version_id)
            .await
            .is_ok()
    };
    if !still_present {
        return Ok(());
    }

    write_redistributed_metadata(meta, target_index_roots, is_trash, strict_durability).await?;

    info!(
        "Redistributed index metadata {}/{} (v:{}) [Trash:{}].",
        meta.fragment, meta.object_key, meta.version_id, is_trash
    );
    Ok(())
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

async fn ensure_authoritative_partition_owner_writable(
    fragment: &str,
    object_key: &str,
) -> Result<(), AppError> {
    index::ensure_object_owner_partition_available(fragment, object_key).await
}

async fn redistribute_object_streaming(
    meta: &mut ObjectMetadata,
    target_data_shards: usize,
    target_parity_shards: usize,
    _target_stripe_size: usize,
    target_blob_roots: &[PathBuf],
    _target_index_roots: &[PathBuf],
    is_trash: bool,
    strict_durability: bool,
) -> Result<(), AppError> {
    let _object_lock =
        index::acquire_object_operation_lock(&meta.fragment, &meta.object_key).await?;
    if should_stop() {
        return Err(AppError::InternalServerError("Stopped".into()));
    }
    ensure_authoritative_partition_owner_writable(&meta.fragment, &meta.object_key).await?;

    let still_present = if is_trash {
        index::read_trashed_object_version(&meta.fragment, &meta.object_key, &meta.version_id)
            .await
            .is_ok()
    } else {
        index::read_object_version(&meta.fragment, &meta.object_key, &meta.version_id)
            .await
            .is_ok()
    };
    if !still_present {
        return Ok(());
    }

    let start_time = Instant::now();
    let num_disks = target_blob_roots.len();
    let total_shards = target_data_shards + target_parity_shards;

    if num_disks < total_shards {
        return Err(AppError::InsufficientStorage);
    }

    let old_block_size = meta.stripe_size_bytes;
    let new_block_size = old_block_size;
    let verify_tmp_crc = strict_durability;

    let mut file_cache: HashMap<String, tokio::fs::File> = HashMap::new();

    let old_parts = std::mem::take(&mut meta.parts);
    let mut new_parts: Vec<PartMetadata> = Vec::with_capacity(old_parts.len());
    let mut old_shards_for_gc: Vec<String> = Vec::new();

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

        let mut writers: Vec<Option<DirectWriter>> = Vec::with_capacity(total_shards);
        let mut commit_list: Vec<(PathBuf, PathBuf, PathBuf, String)> = Vec::new();
        let mut new_shards: Vec<ShardInfo> = Vec::with_capacity(total_shards);
        let mut created_tmps: Vec<PathBuf> = Vec::new();

        for shard_idx in 0..total_shards {
            if should_stop() {
                cleanup_tmp_files(&created_tmps).await;
                return Err(AppError::InternalServerError("Stopped".into()));
            }

            let is_parity = shard_idx >= target_data_shards;
            let ext = if is_parity {
                PARITY_SUFFIX
            } else {
                SHARD_SUFFIX
            };

            let file_name = format!(
                "part_{}_shard_{}{}",
                old_part_arc.part_index, shard_idx, ext
            );

            let disk_idx = (old_part_arc.part_index as usize + shard_idx) % num_disks;
            let disk_root = &target_blob_roots[disk_idx];

            if !tokio_fs::try_exists(disk_root).await.unwrap_or(false) {
                cleanup_tmp_files(&created_tmps).await;
                return Err(AppError::InternalServerError(
                    "blob disk unreachable".into(),
                ));
            }

            let shard_dir =
                get_shard_dir_custom(disk_root, &meta.fragment, &meta.version_id, is_trash);
            let _ = tokio_fs::create_dir_all(&shard_dir).await;

            let final_path = shard_dir.join(&file_name);

            let tmp_path = shard_dir.join(format!(
                "{}{}{}",
                file_name,
                REDIST_TMP_SUFFIX,
                Ulid::new().to_string()
            ));
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
                final_path.clone(),
                shard_dir.clone(),
                file_name.clone(),
            ));

            new_shards.push(ShardInfo {
                shard_index: shard_idx,
                file_name: file_name.clone(),
                absolute_path: final_path.to_string_lossy().to_string(),
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

            let chunk = match read_or_recover_chunk(
                &mut file_cache,
                Path::new(""),
                old_part_arc.clone(),
                shard_index,
                file_offset,
                to_read,
                old_block_size,
            )
            .await
            {
                Ok(chunk) => chunk,
                Err(err) => {
                    let still_present = if is_trash {
                        index::read_trashed_object_version(
                            &meta.fragment,
                            &meta.object_key,
                            &meta.version_id,
                        )
                        .await
                        .is_ok()
                    } else {
                        index::read_object_version(
                            &meta.fragment,
                            &meta.object_key,
                            &meta.version_id,
                        )
                        .await
                        .is_ok()
                    };
                    if !still_present {
                        cleanup_tmp_files(&created_tmps).await;
                        return Ok(());
                    }
                    return Err(err);
                }
            };

            batch.extend_from_slice(&chunk);

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
        let checksum_by_name: HashMap<&str, &str> = new_shards
            .iter()
            .map(|s| (s.file_name.as_str(), s.checksum.as_str()))
            .collect();
        let mut batched_sync_dirs: HashSet<PathBuf> = HashSet::new();

        for (tmp, final_p, parent, fname) in &commit_list {
            if should_stop() {
                cleanup_tmp_files(&created_tmps).await;
                return Err(AppError::InternalServerError("Stopped".into()));
            }

            let expected_crc = if verify_tmp_crc {
                checksum_by_name.get(fname.as_str()).copied()
            } else {
                None
            };

            verify_tmp_shard(tmp.as_path(), final_shard_len, expected_crc).await?;

            publish_tmp_overwrite(
                tmp.as_path(),
                final_p.as_path(),
                parent.as_path(),
                fname,
                strict_durability,
            )
            .await?;

            if !strict_durability {
                batched_sync_dirs.insert(parent.clone());
            }
        }

        if !strict_durability {
            for d in batched_sync_dirs {
                let _ = sync_dir(&d).await;
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
                old_shards_for_gc.push(old_shard.absolute_path.clone());
            }
        }

        file_cache.clear();
    }

    meta.parts = new_parts;
    meta.stripe_size_bytes = new_block_size;

    let still_present = if is_trash {
        index::read_trashed_object_version(&meta.fragment, &meta.object_key, &meta.version_id)
            .await
            .is_ok()
    } else {
        index::read_object_version(&meta.fragment, &meta.object_key, &meta.version_id)
            .await
            .is_ok()
    };

    if !still_present {
        for part in &meta.parts {
            for shard in &part.shards {
                let p = PathBuf::from(&shard.absolute_path);
                let _ = tokio_fs::remove_file(&p).await;
            }
        }
        return Ok(());
    }

    write_redistributed_metadata(meta, _target_index_roots, is_trash, strict_durability).await?;

    for old in old_shards_for_gc {
        if should_stop() {
            return Err(AppError::InternalServerError("Stopped".into()));
        }
        let _ = stage_old_shard_gc_with_symlink(&old).await;
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
        REDIST_INTENT_SUFFIX, RedistributionDurabilityMode, RedistributionExecutionMode,
        RedistributionIntent, RedistributionRunOptions, RedistributionScope,
        collect_redistribution_intent_debt, default_object_concurrency, default_status_batch,
        list_all_fragments, parse_durability_mode_param, parse_execution_mode_param,
        parse_scope_param, reachable_roots,
    };
    use crate::core::constants::FRAGMENT_METADATA_FILE;
    use crate::core::fragment_identity::fragment_root_on;
    use std::collections::HashMap;
    use std::fs;
    use ulid::Ulid;

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

    #[test]
    fn parse_execution_mode_defaults_to_availability() {
        let mut params = HashMap::new();
        assert_eq!(
            parse_execution_mode_param(&params),
            RedistributionExecutionMode::Availability
        );

        params.insert("mode".to_string(), "StRiCt".to_string());
        assert_eq!(
            parse_execution_mode_param(&params),
            RedistributionExecutionMode::Strict
        );

        params.insert("mode".to_string(), "scrub".to_string());
        assert_eq!(
            parse_execution_mode_param(&params),
            RedistributionExecutionMode::Scrub
        );

        params.clear();
        params.insert("access_mode".to_string(), "read_only".to_string());
        assert_eq!(
            parse_execution_mode_param(&params),
            RedistributionExecutionMode::Strict
        );

        params.insert("access_mode".to_string(), "quiesced".to_string());
        assert_eq!(
            parse_execution_mode_param(&params),
            RedistributionExecutionMode::Scrub
        );
    }

    #[test]
    fn execution_modes_scale_default_concurrency_and_batch() {
        let availability =
            default_object_concurrency(RedistributionExecutionMode::Availability, true);
        let strict = default_object_concurrency(RedistributionExecutionMode::Strict, true);
        let scrub = default_object_concurrency(RedistributionExecutionMode::Scrub, true);
        assert!(availability <= strict);
        assert!(strict <= scrub);

        let availability_batch =
            default_status_batch(RedistributionExecutionMode::Availability, true);
        let strict_batch = default_status_batch(RedistributionExecutionMode::Strict, true);
        let scrub_batch = default_status_batch(RedistributionExecutionMode::Scrub, true);
        assert!(availability_batch <= strict_batch);
        assert!(strict_batch <= scrub_batch);
    }

    #[test]
    fn parse_scope_defaults_and_aliases() {
        let mut params = HashMap::new();
        assert_eq!(parse_scope_param(&params), RedistributionScope::All);

        params.insert("scope".to_string(), "blob".to_string());
        assert_eq!(parse_scope_param(&params), RedistributionScope::Blob);

        params.insert("scope".to_string(), "index_only".to_string());
        assert_eq!(parse_scope_param(&params), RedistributionScope::Index);
    }

    #[tokio::test]
    async fn list_all_fragments_ignores_filesystem_trash_directories() {
        let base = std::env::temp_dir().join(format!("n1-redist-fragments-{}", Ulid::new()));
        let index_root = base.join("index-root");
        fs::create_dir_all(&index_root).expect("create index root");

        let real_fragment = "real-fragment";
        let real_fragment_dir = index_root.join(real_fragment);
        fs::create_dir_all(&real_fragment_dir).expect("create fragment dir");
        fs::write(real_fragment_dir.join(FRAGMENT_METADATA_FILE), b"fragment")
            .expect("write fragment metadata marker");

        fs::create_dir_all(index_root.join(".Trash-1000")).expect("create desktop trash dir");
        fs::create_dir_all(index_root.join("lost+found")).expect("create unrelated dir");

        let fragments = list_all_fragments(&[index_root]).await;
        assert_eq!(fragments, vec![real_fragment.to_string()]);

        let _ = fs::remove_dir_all(base);
    }

    #[tokio::test]
    async fn reachable_roots_ignores_blocker_files() {
        let base = std::env::temp_dir().join(format!("n1-redist-roots-{}", Ulid::new()));
        let live_root = base.join("live-root");
        let blocked_root = base.join("blocked-root");

        fs::create_dir_all(&live_root).expect("create live root");
        fs::create_dir_all(&base).expect("create base");
        fs::write(&blocked_root, b"down").expect("create blocker file");

        let live = reachable_roots(&[live_root.clone(), blocked_root.clone()]).await;
        assert_eq!(live, vec![live_root]);

        let _ = fs::remove_dir_all(base);
    }

    #[tokio::test]
    async fn redistribution_intent_debt_can_scope_to_one_fragment() {
        let base = std::env::temp_dir().join(format!("n1-redist-intents-{}", Ulid::new()));
        let index_root = base.join("index-root");
        let fragment_a = "fragment-a";
        let fragment_b = "fragment-b";
        let now_ms = chrono::Utc::now().timestamp_millis();

        let write_intent = |fragment: &str, object_key: &str, phase: &str| {
            let intent_dir = fragment_root_on(&index_root, fragment).join(".intents");
            fs::create_dir_all(&intent_dir).expect("create intent dir");
            let intent = RedistributionIntent {
                tx_id: Ulid::new().to_string(),
                fragment: fragment.to_string(),
                object_key: object_key.to_string(),
                version_id: Ulid::new().to_string(),
                is_trash: false,
                scope: "all".to_string(),
                phase: phase.to_string(),
                created_at_unix_ms: now_ms.saturating_sub(1000),
                updated_at_unix_ms: now_ms,
            };
            let payload = rmp_serde::to_vec(&intent).expect("serialize intent");
            fs::write(
                intent_dir.join(format!("{}{}", intent.tx_id, REDIST_INTENT_SUFFIX)),
                payload,
            )
            .expect("write intent");
        };

        write_intent(fragment_a, "a.txt", "pending");
        write_intent(fragment_b, "b.txt", "failed");

        let scoped = collect_redistribution_intent_debt(
            std::slice::from_ref(&index_root),
            Some(fragment_a.to_string()),
        )
        .await;
        assert_eq!(scoped.pending_intents, 1);
        assert_eq!(scoped.failed_intents, 0);
        assert_eq!(scoped.fragments_with_pending_intents, 1);

        let global = collect_redistribution_intent_debt(&[index_root], None).await;
        assert_eq!(global.pending_intents, 2);
        assert_eq!(global.failed_intents, 1);
        assert_eq!(global.fragments_with_pending_intents, 2);

        let _ = fs::remove_dir_all(base);
    }
}
