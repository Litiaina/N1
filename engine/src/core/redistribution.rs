use crate::core::constants::{
    FRAGMENT_METADATA_FILE, GC_SUFFIX, PARITY_SUFFIX, REDIST_TMP_SUFFIX, SHARD_SUFFIX,
};
use crate::core::disk_manager::{
    INDEX_STORE_DISKS, ObjectMetadata, ObjectPathManager, PartMetadata, ShardInfo,
    default_stripe_size,
};
use crate::core::ec::ErasureStreamWriter;
use crate::core::fragment_identity::{
    fragment_root_on, list_fragment_aliases, stable_fragment_id_for_root,
};
use crate::core::fragment_metadata::{fragment_index_partition_count, resolve_stable_fragment_id};
use crate::core::index;
use crate::core::index_store;
use crate::core::io_scheduler::{DiskIoClass, acquire_disk_io};
use crate::core::linux_io;
use crate::core::operation_gate::ensure_user_mutations_allowed;
use crate::core::operation_gate::{MaintenanceAccessMode, acquire_maintenance_mode};
use crate::core::recovery::read_or_recover_chunk;
use crate::core::root_identity::configured_root_identity_is_current;
use crate::core::upload::{
    DATA_SHARDS_ATOMIC, DirectWriter, PARITY_SHARDS_ATOMIC, STRIPE_SIZE_ATOMIC, create_direct_file,
    sync_dir, target_stripe_size_for_metadata,
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
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use tokio::fs as tokio_fs;
use tokio::io::AsyncReadExt;
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
    Object,
    Namespace,
}

impl RedistributionScope {
    fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Object => "object",
            Self::Namespace => "namespace",
        }
    }

    fn from_str(raw: &str) -> Self {
        match raw {
            "object" => Self::Object,
            "namespace" => Self::Namespace,
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
    pub target_object: Option<String>,
    pub target_version: Option<String>,
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
            target_object: None,
            target_version: None,
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
        Some("object") => RedistributionScope::Object,
        Some("namespace") => RedistributionScope::Namespace,
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
    pub payload: bool,
    pub metadata: bool,
}

impl RedistributionNeeds {
    fn object(self) -> bool {
        self.payload || self.metadata
    }
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
    ensure_user_mutations_allowed()?;
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
    let target_fragment = match params
        .get("fragment")
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
    {
        Some(fragment) => Some(resolve_stable_fragment_id(&fragment).await?),
        None => None,
    };
    let target_object = params
        .get("object_key")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let target_version = params
        .get("version_id")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if target_object.is_some() && target_fragment.is_none() {
        return Err(AppError::BadRequest(
            "object redistribution requires a fragment".into(),
        ));
    }
    if target_object.is_some() && scope == RedistributionScope::Namespace {
        return Err(AppError::BadRequest(
            "object redistribution cannot use namespace scope".into(),
        ));
    }
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
        target_object,
        target_version,
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

mod execution;
mod intent;

#[cfg(test)]
pub use execution::run_redistribution_job;
pub use execution::run_redistribution_job_with_options;
pub use intent::{collect_redistribution_intent_debt, replay_pending_intents_for_fragment_now};

pub(crate) use execution::check_needs_redistribution;

#[cfg(test)]
use intent::{list_all_fragments, reachable_roots};

#[cfg(test)]
mod tests;
