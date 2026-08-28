use crate::core::constants::{FRAGMENT_METADATA_FILE, PARITY_SUFFIX, SHARD_SUFFIX};
use crate::core::disk_manager::{
    INDEX_STORE_DISKS, ObjectMetadata, ObjectPathManager, PartMetadata,
};
use crate::core::fragment_identity::{
    fragment_root_on, list_fragment_aliases, stable_fragment_id_for_root,
};
use crate::core::fragment_metadata::resolve_stable_fragment_id;
use crate::core::index;
use crate::core::index_store;
use crate::core::io_scheduler::{DiskIoClass, acquire_disk_io, try_acquire_disk_io};
use crate::core::linux_io;
use crate::core::operation_gate::{MaintenanceAccessMode, acquire_maintenance_mode};
use crate::core::recovery::{repair_entire_shard, verify_shard_integrity};
use crate::core::redistribution::replay_pending_intents_for_fragment_now;
use crate::core::root_identity::configured_root_identity_is_current;
use crate::core::upload::{
    DATA_SHARDS_ATOMIC, PARITY_SHARDS_ATOMIC, STRIPE_SIZE_ATOMIC, target_stripe_size_for_metadata,
};
use crate::enums::app_error_enum::AppError;
use crate::namespace::replica::repair_namespace_replicas;
use crate::namespace::runtime::global_namespace_runtime;
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
use std::sync::{Arc, LazyLock, Mutex as StdMutex};
use std::time::{Duration, Instant, SystemTime};
use tokio::fs;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, OwnedSemaphorePermit, RwLock, Semaphore};
use tracing::{error, info, warn};
use ulid::Ulid;

#[derive(Debug, Clone, Serialize, Default)]
pub struct RepairJobStatus {
    pub is_running: bool,
    pub phase: String,
    pub fast_mode: bool,

    pub total_scanned: u64,
    pub total_repaired: u64,
    pub corrupt_found: u64,
    pub failed_repairs: u64,

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

#[derive(Debug, Clone, Serialize)]
pub struct ObjectRepairStatus {
    pub object_key: String,
    pub version_id: String,
    pub status: String,
    pub total_parts: u64,
    pub total_shards: u64,
    pub healthy_shards: u64,
    pub missing_shards: u64,
    pub corrupt_shards: u64,
    pub repairable: bool,
    pub repaired_shards: u64,
    pub failed_repairs: u64,
    pub layout: ObjectLayoutStatus,
}

#[derive(Debug, Clone, Serialize)]
pub struct ObjectLayoutStatus {
    pub recorded_data_shards: usize,
    pub recorded_parity_shards: usize,
    pub current_data_shards: usize,
    pub current_parity_shards: usize,
    pub recorded_stripe_size_bytes: usize,
    pub current_stripe_size_bytes: usize,
    pub configured_blob_roots: usize,
    pub layout_matches_current: bool,
    pub placement_matches_current: bool,
    pub state: String,
    pub redistributable: bool,
    pub mismatch_reasons: Vec<String>,
}

static REPAIR_STATUS: LazyLock<Arc<RwLock<RepairJobStatus>>> =
    LazyLock::new(|| Arc::new(RwLock::new(RepairJobStatus::default())));

static STOP_SIGNAL: LazyLock<AtomicBool> = LazyLock::new(|| AtomicBool::new(false));
static REPAIR_RUNNING: LazyLock<AtomicBool> = LazyLock::new(|| AtomicBool::new(false));

struct RepairRunningGuard;

impl Drop for RepairRunningGuard {
    fn drop(&mut self) {
        REPAIR_RUNNING.store(false, Ordering::Release);
    }
}

fn acquire_repair_running() -> Result<RepairRunningGuard, AppError> {
    REPAIR_RUNNING
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .map(|_| RepairRunningGuard)
        .map_err(|_| AppError::Conflict("A repair job is already running.".into()))
}
static OBJECT_STATUS_GATE: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(64)));
static OBJECT_REPAIR_GATE: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(8)));
static OBJECT_STATUS_FRAGMENT_GATES: LazyLock<StdMutex<HashMap<String, Arc<Semaphore>>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));
static OBJECT_REPAIR_FRAGMENT_GATES: LazyLock<StdMutex<HashMap<String, Arc<Semaphore>>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));
static OBJECT_MAINTENANCE_LOCKS: LazyLock<StdMutex<HashMap<String, Arc<AsyncMutex<()>>>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

const OBJECT_STATUS_PER_FRAGMENT_LIMIT: usize = 16;
const OBJECT_REPAIR_PER_FRAGMENT_LIMIT: usize = 2;

struct ObjectMaintenancePermit {
    _global: OwnedSemaphorePermit,
    _fragment: OwnedSemaphorePermit,
    object_lock_key: Option<String>,
    object_guard: Option<OwnedMutexGuard<()>>,
}

impl Drop for ObjectMaintenancePermit {
    fn drop(&mut self) {
        drop(self.object_guard.take());
        let Some(key) = self.object_lock_key.as_ref() else {
            return;
        };
        let Ok(mut locks) = OBJECT_MAINTENANCE_LOCKS.lock() else {
            return;
        };
        if let Some(lock) = locks.get(key) {
            if Arc::strong_count(lock) <= 1 {
                locks.remove(key);
            }
        }
    }
}

#[derive(Clone, Copy)]
enum ObjectMaintenanceKind {
    Status,
    Repair,
}

impl ObjectMaintenanceKind {
    fn label(self) -> &'static str {
        match self {
            Self::Status => "object status",
            Self::Repair => "object repair",
        }
    }

    fn global_gate(self) -> Arc<Semaphore> {
        match self {
            Self::Status => OBJECT_STATUS_GATE.clone(),
            Self::Repair => OBJECT_REPAIR_GATE.clone(),
        }
    }

    fn fragment_gate(self, fragment: &str) -> Result<Arc<Semaphore>, AppError> {
        let (map, limit) = match self {
            Self::Status => (
                &*OBJECT_STATUS_FRAGMENT_GATES,
                OBJECT_STATUS_PER_FRAGMENT_LIMIT,
            ),
            Self::Repair => (
                &*OBJECT_REPAIR_FRAGMENT_GATES,
                OBJECT_REPAIR_PER_FRAGMENT_LIMIT,
            ),
        };
        let mut gates = map.lock().map_err(|_| {
            AppError::InternalServerError("object maintenance gate lock poisoned".into())
        })?;
        Ok(gates
            .entry(fragment.to_string())
            .or_insert_with(|| Arc::new(Semaphore::new(limit)))
            .clone())
    }
}

fn object_maintenance_lock(
    fragment: &str,
    object_id: &str,
) -> Result<Arc<AsyncMutex<()>>, AppError> {
    let key = format!("{fragment}\0{object_id}");
    let mut locks = OBJECT_MAINTENANCE_LOCKS.lock().map_err(|_| {
        AppError::InternalServerError("object maintenance lock map poisoned".into())
    })?;
    Ok(locks
        .entry(key)
        .or_insert_with(|| Arc::new(AsyncMutex::new(())))
        .clone())
}

async fn acquire_object_maintenance_permit(
    meta: &ObjectMetadata,
    kind: ObjectMaintenanceKind,
    lock_object: bool,
) -> Result<ObjectMaintenancePermit, AppError> {
    let global = kind.global_gate().try_acquire_owned().map_err(|_| {
        AppError::ServiceUnavailable(format!(
            "{} path is saturated globally; retry shortly",
            kind.label()
        ))
    })?;
    let fragment_gate = kind.fragment_gate(&meta.fragment)?;
    let fragment = fragment_gate.try_acquire_owned().map_err(|_| {
        AppError::ServiceUnavailable(format!(
            "{} path is saturated for fragment {}; retry shortly",
            kind.label(),
            meta.fragment
        ))
    })?;

    let (object_lock_key, object) = if lock_object {
        let key = format!("{}\0{}", meta.fragment, meta.object_key);
        let lock = object_maintenance_lock(&meta.fragment, &meta.object_key)?;
        let guard = lock.try_lock_owned().map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "{} is already running for this object; retry shortly",
                kind.label()
            ))
        })?;
        (Some(key), Some(guard))
    } else {
        (None, None)
    };

    Ok(ObjectMaintenancePermit {
        _global: global,
        _fragment: fragment,
        object_lock_key,
        object_guard: object,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RepairCheckMode {
    Availability,
    Checksum,
    Strict,
    Scrub,
}

impl RepairCheckMode {
    /// Operator-requested default. A manual checksum intentionally protects
    /// the namespace with a read-only gate unless the caller selected a
    /// stronger mode.
    pub(crate) fn default_maintenance_access_mode(self) -> MaintenanceAccessMode {
        match self {
            Self::Availability => MaintenanceAccessMode::Availability,
            Self::Checksum | Self::Strict => MaintenanceAccessMode::ReadOnly,
            Self::Scrub => MaintenanceAccessMode::Quiesced,
        }
    }

    /// Lowest safe gate for the repair algorithm itself. Online checksum is
    /// safe for autonomous maintenance because objects are immutable and each
    /// repair re-reads the current version while holding N1's object operation
    /// lock. Strict and scrub retain their stronger isolation contracts.
    pub(crate) fn minimum_maintenance_access_mode(self) -> MaintenanceAccessMode {
        match self {
            Self::Availability | Self::Checksum => MaintenanceAccessMode::Availability,
            Self::Strict => MaintenanceAccessMode::ReadOnly,
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
    pub replay_pending_redistribution: bool,
}

impl Default for RepairRunOptions {
    fn default() -> Self {
        Self {
            fast_mode: false,
            access_mode: MaintenanceAccessMode::Availability,
            replay_pending_redistribution: true,
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
    failed_repairs: u64,
    last_repaired: Option<String>,
    last_disagreement: Option<String>,
    last_failure: Option<String>,
}

#[derive(Clone, Copy)]
enum MetadataCatalog {
    Live,
    Trash,
}

fn ensure_no_unresolved_repairs(
    unresolved_repairs: u64,
    last_failure: Option<&str>,
) -> Result<(), AppError> {
    if unresolved_repairs == 0 {
        return Ok(());
    }
    Err(AppError::ServiceUnavailable(format!(
        "N1 repair scan completed with {} unresolved shard or metadata repair(s): {}",
        unresolved_repairs,
        last_failure.unwrap_or("repair could not complete")
    )))
}

#[inline]
fn should_stop() -> bool {
    STOP_SIGNAL.load(Ordering::Relaxed)
}

#[inline]
fn clamp_usize(v: usize, lo: usize, hi: usize) -> usize {
    v.max(lo).min(hi)
}

pub(crate) fn parse_check_mode(params: &HashMap<String, String>) -> RepairCheckMode {
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

    match check_mode {
        RepairCheckMode::Availability => match ram_gib {
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
        },
        RepairCheckMode::Checksum | RepairCheckMode::Strict => match ram_gib {
            0..=2 => {
                meta = meta.min(4);
                shard = shard.min(1);
                batch = batch.min(128);
            }
            3..=4 => {
                meta = meta.min(8);
                shard = shard.min(2);
                batch = batch.min(256);
            }
            5..=8 => {
                meta = meta.min(12);
                shard = shard.min(4);
                batch = batch.min(512);
            }
            9..=16 => {
                meta = meta.min(16);
                shard = shard.min(6);
                batch = batch.min(768);
            }
            17..=32 => {
                meta = meta.min(24);
                shard = shard.min(8);
                batch = batch.min(1024);
            }
            _ => {}
        },
        RepairCheckMode::Scrub => match ram_gib {
            0..=2 => {
                meta = meta.min(4);
                shard = shard.min(1);
                batch = batch.min(128);
            }
            3..=4 => {
                meta = meta.min(8);
                shard = shard.min(2);
                batch = batch.min(256);
            }
            5..=8 => {
                meta = meta.min(16);
                shard = shard.min(4);
                batch = batch.min(768);
            }
            9..=16 => {
                meta = meta.min(24);
                shard = shard.min(8);
                batch = batch.min(1024);
            }
            17..=32 => {
                meta = meta.min(32);
                shard = shard.min(12);
                batch = batch.min(2048);
            }
            _ => {}
        },
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
            io_width.saturating_mul(4).clamp(4, 32),
            blob_width.saturating_mul(4).clamp(2, 16),
        ),
        RepairCheckMode::Scrub => (
            io_width.saturating_mul(8).clamp(8, 64),
            blob_width.saturating_mul(8).clamp(4, 32),
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

async fn normalize_part_paths_for_repair(mut part: PartMetadata) -> PartMetadata {
    for shard in part.shards.iter_mut() {
        let stable_name =
            stable_shard_file_name(part.part_index, shard.shard_index, shard.is_parity);
        shard.file_name = stable_name;

        shard.absolute_path = strip_tmp_attempt_suffix(&shard.absolute_path).to_string();
    }

    part
}

async fn inspect_object_parts(
    meta: &ObjectMetadata,
    check_mode: RepairCheckMode,
) -> Result<(ObjectRepairStatus, Vec<PartMetadata>, Vec<Vec<usize>>), AppError> {
    let path_manager = ObjectPathManager::new();
    let active_blob_roots = path_manager.get_blob_roots();
    let layout = object_layout_status(meta, &path_manager);
    let mut report = ObjectRepairStatus {
        object_key: meta.object_key.clone(),
        version_id: meta.version_id.clone(),
        status: "healthy".to_string(),
        total_parts: meta.parts.len() as u64,
        total_shards: 0,
        healthy_shards: 0,
        missing_shards: 0,
        corrupt_shards: 0,
        repairable: true,
        repaired_shards: 0,
        failed_repairs: 0,
        layout,
    };

    let mut normalized_parts = Vec::with_capacity(meta.parts.len());
    let mut repair_targets = Vec::with_capacity(meta.parts.len());
    let mut any_degraded = false;
    let mut any_critical = false;
    let mut any_lost = false;

    for part in &meta.parts {
        let normalized = normalize_part_paths_for_repair(part.clone()).await;
        let data_shards = normalized
            .shards
            .iter()
            .filter(|shard| !shard.is_parity)
            .count()
            .max(1);
        let mut healthy_in_part = 0usize;
        let mut bad_indexes = Vec::new();

        for shard in &normalized.shards {
            report.total_shards += 1;
            let shard_path = PathBuf::from(strip_tmp_attempt_suffix(&shard.absolute_path));
            if !active_blob_roots
                .iter()
                .any(|root| shard_path.starts_with(root))
            {
                report.missing_shards += 1;
                bad_indexes.push(shard.shard_index);
                continue;
            }
            match fs::metadata(&shard_path).await {
                Ok(metadata) => {
                    if metadata.len() != shard.size_bytes {
                        report.corrupt_shards += 1;
                        bad_indexes.push(shard.shard_index);
                    } else if check_mode.verifies_full_integrity() {
                        match linux_io::open_read(&shard_path).await {
                            Ok(file) => {
                                if verify_shard_integrity(&file, &shard.checksum).await {
                                    report.healthy_shards += 1;
                                    healthy_in_part += 1;
                                } else {
                                    report.corrupt_shards += 1;
                                    bad_indexes.push(shard.shard_index);
                                }
                            }
                            Err(_) => {
                                report.missing_shards += 1;
                                bad_indexes.push(shard.shard_index);
                            }
                        }
                    } else {
                        report.healthy_shards += 1;
                        healthy_in_part += 1;
                    }
                }
                Err(_) => {
                    report.missing_shards += 1;
                    bad_indexes.push(shard.shard_index);
                }
            }
        }

        if !bad_indexes.is_empty() {
            any_degraded = true;
        }
        if healthy_in_part == 0 {
            any_lost = true;
        } else if healthy_in_part < data_shards {
            any_critical = true;
        }
        if healthy_in_part < data_shards {
            report.repairable = false;
        }

        normalized_parts.push(normalized);
        repair_targets.push(bad_indexes);
    }

    report.status = if any_lost {
        "lost".to_string()
    } else if any_critical {
        "critical".to_string()
    } else if any_degraded {
        "degraded".to_string()
    } else {
        "healthy".to_string()
    };

    Ok((report, normalized_parts, repair_targets))
}

fn object_layout_status(
    meta: &ObjectMetadata,
    path_manager: &ObjectPathManager,
) -> ObjectLayoutStatus {
    let blob_roots = path_manager.get_blob_roots();
    let current_data = DATA_SHARDS_ATOMIC
        .get()
        .map(|value| value.load(Ordering::Relaxed))
        .unwrap_or_else(|| usize::from(meta.data_shards));
    let current_parity = PARITY_SHARDS_ATOMIC
        .get()
        .map(|value| value.load(Ordering::Relaxed))
        .unwrap_or_else(|| usize::from(meta.parity_shards));
    let configured_stripe_ceiling = STRIPE_SIZE_ATOMIC
        .get()
        .map(|value| value.load(Ordering::Relaxed))
        .unwrap_or(meta.stripe_size_bytes);
    let current_stripe = target_stripe_size_for_metadata(meta, configured_stripe_ceiling);
    let recorded_data = usize::from(meta.data_shards);
    let recorded_parity = usize::from(meta.parity_shards);
    let current_total = current_data.saturating_add(current_parity);

    let mut mismatch_reasons = Vec::new();
    if recorded_data != current_data || recorded_parity != current_parity {
        mismatch_reasons.push("ec_layout".to_string());
    }
    if meta.stripe_size_bytes != current_stripe {
        mismatch_reasons.push("stripe_size".to_string());
    }

    let current_placement = path_manager
        .select_blob_roots_for_object(
            &meta.fragment,
            &meta.object_key,
            &meta.version_id,
            current_total,
        )
        .ok();
    let placement_matches_current = current_placement.as_ref().is_some_and(|roots| {
        meta.parts.iter().all(|part| {
            part.shards.iter().all(|shard| {
                let Some(root) = roots.get(shard.shard_index) else {
                    return false;
                };
                let expected = path_manager
                    .get_shard_dir_on_disk(root, &meta.fragment, &meta.object_key, &meta.version_id)
                    .join(&shard.file_name);
                Path::new(&shard.absolute_path) == expected
            })
        })
    });
    if !placement_matches_current {
        mismatch_reasons.push("placement".to_string());
    }

    let layout_matches_current = recorded_data == current_data
        && recorded_parity == current_parity
        && meta.stripe_size_bytes == current_stripe;
    let current = layout_matches_current && placement_matches_current;

    ObjectLayoutStatus {
        recorded_data_shards: recorded_data,
        recorded_parity_shards: recorded_parity,
        current_data_shards: current_data,
        current_parity_shards: current_parity,
        recorded_stripe_size_bytes: meta.stripe_size_bytes,
        current_stripe_size_bytes: current_stripe,
        configured_blob_roots: blob_roots.len(),
        layout_matches_current,
        placement_matches_current,
        state: if current {
            "current_layout".to_string()
        } else {
            "recorded_layout".to_string()
        },
        redistributable: !current
            && current_data > 0
            && current_total > 0
            && blob_roots.len() >= current_total,
        mismatch_reasons,
    }
}

pub async fn inspect_object_status(
    meta: &ObjectMetadata,
    check_mode: RepairCheckMode,
) -> Result<ObjectRepairStatus, AppError> {
    let _permit = acquire_object_maintenance_permit(
        meta,
        ObjectMaintenanceKind::Status,
        matches!(check_mode, RepairCheckMode::Scrub),
    )
    .await?;
    let io = try_acquire_disk_io(DiskIoClass::ForegroundRead, Some(&meta.fragment))?;
    let result = inspect_object_parts(meta, check_mode)
        .await
        .map(|(report, _, _)| report);
    if result.is_ok() {
        io.complete();
    }
    result
}

pub async fn repair_object(
    meta: &ObjectMetadata,
    check_mode: RepairCheckMode,
) -> Result<ObjectRepairStatus, AppError> {
    let _permit =
        acquire_object_maintenance_permit(meta, ObjectMaintenanceKind::Repair, true).await?;
    let io = try_acquire_disk_io(DiskIoClass::BackgroundRepair, Some(&meta.fragment))?;
    let (mut report, normalized_parts, repair_targets) =
        inspect_object_parts(meta, check_mode).await?;
    if report.status == "healthy" || !report.repairable {
        io.complete();
        return Ok(report);
    }

    for (part, bad_indexes) in normalized_parts.into_iter().zip(repair_targets.into_iter()) {
        let part = Arc::new(part);
        for missing_idx in bad_indexes {
            match repair_entire_shard(part.clone(), missing_idx, meta.stripe_size_bytes).await {
                Ok(()) => {
                    report.repaired_shards += 1;
                    if let Some(shard) = part.shards.get(missing_idx) {
                        io.record_progress(shard.size_bytes);
                    } else {
                        io.heartbeat();
                    }
                }
                Err(err) => {
                    report.failed_repairs += 1;
                    warn!(
                        "Object-level repair failed for {}/{} shard {}: {}",
                        meta.fragment, meta.object_key, missing_idx, err
                    );
                }
            }
        }
    }

    let (mut after, _, _) = inspect_object_parts(meta, check_mode).await?;
    after.repaired_shards = report.repaired_shards;
    after.failed_repairs = report.failed_repairs;
    io.complete();
    Ok(after)
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
    let mut access_mode = check_mode.default_maintenance_access_mode();
    if read_only_fast && access_mode == MaintenanceAccessMode::Availability {
        access_mode = MaintenanceAccessMode::ReadOnly;
    }

    let target_fragment = match params
        .get("fragment")
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
    {
        Some(fragment) => Some(resolve_stable_fragment_id(&fragment).await?),
        None => None,
    };

    let cfg = RepairRunConfig {
        dry_run: parse_bool_param(&params, "dry_run", false),
        target_fragment,
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
        replay_pending_redistribution: true,
    };

    let ws_manager = Arc::new(state.ws_manager.clone());
    let ws_manager_clone = ws_manager.clone();
    let running_guard = acquire_repair_running()?;
    {
        let mut status = REPAIR_STATUS.write().await;
        status.is_running = true;
        status.fast_mode = fast_mode;
        status.phase = "Queued".to_string();
        status.last_error = None;
        status.start_timestamp = Utc::now().timestamp();
        ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
    }

    tokio::spawn(async move {
        if let Err(e) =
            run_repair_job_reserved(cfg, run_opts, ws_manager.clone(), running_guard).await
        {
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

mod execution;
mod object_repair;

#[cfg(test)]
pub use execution::run_repair_job;
use execution::run_repair_job_reserved;
pub use execution::run_repair_job_with_options;

#[cfg(test)]
use object_repair::{list_all_fragments, reachable_dir_roots};

#[cfg(test)]
mod tests;
