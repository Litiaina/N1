use crate::core::constants::{FRAGMENT_METADATA_FILE, PARITY_SUFFIX, SHARD_SUFFIX};
use crate::core::disk_manager::{ObjectMetadata, ObjectPathManager, PartMetadata, STORAGE_DISKS};
use crate::core::fragment_identity::{
    list_fragment_aliases, physical_fragment_root_on, stable_fragment_id_for_root,
};
use crate::core::fragment_metadata::resolve_stable_fragment_id;
use crate::core::index;
use crate::core::io_scheduler::{DiskIoClass, acquire_disk_io, try_acquire_disk_io};
use crate::core::operation_gate::{MaintenanceAccessMode, acquire_maintenance_mode};
use crate::core::recovery::{repair_entire_shard, verify_shard_integrity};
use crate::core::redistribution::replay_pending_intents_for_fragment_now;
use crate::distribution::download::{
    ShardProbeResult, part_has_remote_shards, shard_is_remote, shard_probe,
};
use crate::distribution::repair::repair_cluster_object_shards;
use crate::distribution::status::inspect_cluster_object_status;
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
use tokio::fs::{self, File};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, OwnedSemaphorePermit, RwLock, Semaphore};
use tracing::{error, info, warn};
use ulid::Ulid;

mod api;
mod object;
mod replicas;
mod runner;

pub use api::*;
pub use object::*;
use replicas::*;
pub use runner::*;

#[derive(Debug, Clone, Serialize, Default)]
pub struct RepairJobStatus {
    pub is_running: bool,
    pub phase: String,
    pub fast_mode: bool,

    pub total_scanned: u64,
    pub total_repaired: u64,
    pub total_failed: u64,
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

#[derive(Debug, Clone, Serialize)]
pub struct ObjectLayoutStatus {
    pub recorded_membership_epoch: u64,
    pub current_membership_epoch: u64,
    pub recorded_data_shards: u16,
    pub recorded_parity_shards: u16,
    pub current_data_shards: usize,
    pub current_parity_shards: usize,
    pub recorded_total_shards: usize,
    pub current_total_shards: usize,
    pub active_write_nodes: usize,
    pub layout_matches_current: bool,
    pub placement_matches_current: bool,
    pub state: String,
    pub redistributable: bool,
    pub mismatch_reasons: Vec<String>,
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
    pub unavailable_shards: u64,
    pub repairable: bool,
    pub repaired_shards: u64,
    pub failed_repairs: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub layout: Option<ObjectLayoutStatus>,
}

static REPAIR_STATUS: LazyLock<Arc<RwLock<RepairJobStatus>>> =
    LazyLock::new(|| Arc::new(RwLock::new(RepairJobStatus::default())));

static STOP_SIGNAL: LazyLock<AtomicBool> = LazyLock::new(|| AtomicBool::new(false));
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
    fn maintenance_access_mode(self) -> MaintenanceAccessMode {
        match self {
            Self::Availability => MaintenanceAccessMode::Availability,
            Self::Checksum | Self::Strict => MaintenanceAccessMode::ReadOnly,
            Self::Scrub => MaintenanceAccessMode::Quiesced,
        }
    }

    pub(crate) fn verifies_full_integrity(self) -> bool {
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
    failed_repairs: u64,
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

pub(crate) fn parse_check_mode(params: &HashMap<String, String>) -> RepairCheckMode {
    let normalized_mode = params
        .get("mode")
        .or_else(|| params.get("check_mode"))
        .or_else(|| params.get("check"))
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

fn expected_shard_path(
    path_manager: &ObjectPathManager,
    blob_roots: &[PathBuf],
    fragment: &str,
    object_id: &str,
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
    let shard_dir = path_manager.get_shard_dir_on_disk(disk_root, fragment, object_id, version_id);
    let name = stable_shard_file_name(part_index, shard_index, is_parity);
    Some(shard_dir.join(name))
}

async fn normalize_part_paths_for_repair(
    path_manager: &ObjectPathManager,
    blob_roots: &[PathBuf],
    fragment: &str,
    object_id: &str,
    version_id: &str,
    mut part: PartMetadata,
) -> PartMetadata {
    for shard in part.shards.iter_mut() {
        let stable_name =
            stable_shard_file_name(part.part_index, shard.shard_index, shard.is_parity);
        shard.file_name = stable_name;

        let abs = shard.absolute_path.clone();
        let abs_norm = strip_tmp_attempt_suffix(abs.as_str()).to_string();
        if shard_is_remote(shard) {
            continue;
        }

        let expected = expected_shard_path(
            path_manager,
            blob_roots,
            fragment,
            object_id,
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

#[cfg(test)]
#[path = "repair_manager/tests.rs"]
mod tests;
