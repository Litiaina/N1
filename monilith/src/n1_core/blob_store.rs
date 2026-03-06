use crate::enums::app_error_enum::AppError;
use crate::n1_core::constants::{PARITY_SUFFIX, SHARD_SUFFIX};
use crate::n1_core::disk_manager::{
    ObjectMetadata, RawBlobAddress, ShardInfo, configured_blob_store_disks,
    configured_fs_storage_root, configured_index_store_disks,
};
use block_manager::{
    BlockManager, BlockManagerError, DirectExtentIo, FormatOptions, RawExtentAddress,
};
use chrono::Utc;
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use tokio::fs as tokio_fs;
use tokio::sync::Notify;
use tracing::{info, warn};
use walkdir::WalkDir;

#[cfg(unix)]
use std::os::unix::fs::FileTypeExt;

#[derive(Clone)]
pub enum BlobDiskHandle {
    Raw {
        device_id: String,
        manager: Arc<RwLock<BlockManager>>,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RawBlobPathKind {
    BlockDevice,
    LoopbackFile,
}

#[derive(Debug, Clone, Serialize)]
pub struct RawBlobDeviceStatus {
    pub path: String,
    pub canonical_path: String,
    pub kind: RawBlobPathKind,
    pub status: String,
    pub direct_io: bool,
    pub mounted: bool,
    pub mount_points: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path_warning: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detected_signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_size_bytes: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allocation_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub journal_cursor: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub used_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub available_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RawBlobDiskState {
    Managed,
    Unformatted,
    ForeignSignature,
    DirtyUnknown,
}

#[derive(Debug, Clone)]
struct RawBlobDiskInspection {
    path: PathBuf,
    canonical_path: PathBuf,
    kind: RawBlobPathKind,
    mounted: bool,
    mount_points: Vec<String>,
    path_warning: Option<String>,
    total_bytes: u64,
    state: RawBlobDiskState,
    detected_signature: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawGcMarker {
    pub fragment: String,
    pub address: RawBlobAddress,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RawDeferredFreeMarker {
    pub address: RawBlobAddress,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RawDeviceMapTable {
    pub topology_epoch: u64,
    pub updated_at: i64,
    #[serde(default)]
    pub entries: Vec<RawDeviceMapEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RawDeviceMapEntry {
    pub device_id: String,
    pub canonical_path: String,
    pub kind: RawBlobPathKind,
    pub state: String,
    pub first_seen_at: i64,
    pub last_seen_at: i64,
    pub last_seen_epoch: u64,
}

static RAW_MANAGER_CACHE: LazyLock<Mutex<HashMap<PathBuf, Arc<RwLock<BlockManager>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static RAW_MANAGER_BY_DEVICE_ID: LazyLock<Mutex<HashMap<String, Arc<RwLock<BlockManager>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static RAW_TOPOLOGY_EPOCH: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(1));
static RAW_TOPOLOGY_MAP_CACHE: LazyLock<Mutex<Option<RawDeviceMapTable>>> =
    LazyLock::new(|| Mutex::new(None));
static RAW_TOPOLOGY_LAST_REFRESH_SECS: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));
static RAW_DEFERRED_FREE_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);
static RAW_DEFERRED_FREE_WORKER_STARTED: LazyLock<AtomicBool> =
    LazyLock::new(|| AtomicBool::new(false));

const MISSING_RAW_DEVICE_ERROR_PREFIX: &str = "No raw blob device found for device id ";
const RAW_DEVICE_MAP_DIR: &str = ".n1";
const RAW_DEVICE_MAP_FILE: &str = "raw_device_map.msgpack";
const RAW_DEFERRED_FREE_DIR: &str = ".raw_free_defer";
const RAW_DEFERRED_FREE_BATCH_SIZE: usize = 512;
const RAW_DEFERRED_FREE_THRESHOLD: usize = 64;
const RAW_TOPOLOGY_REFRESH_INTERVAL_SECS: u64 = 5;
const RAW_DEFERRED_FREE_PRESSURE_MAX_BATCHES: usize = 8;

fn register_raw_manager_device(manager: &Arc<RwLock<BlockManager>>) -> Result<String, AppError> {
    let device_id = manager
        .read()
        .map_err(|_| AppError::InternalServerError("Raw blob manager lock poisoned".into()))?
        .header()
        .device_id
        .to_string();
    RAW_MANAGER_BY_DEVICE_ID
        .lock()
        .map_err(|_| {
            AppError::InternalServerError("Raw manager device index lock poisoned".into())
        })?
        .insert(device_id.clone(), Arc::clone(manager));
    Ok(device_id)
}

fn clear_raw_manager_device_index() {
    if let Ok(mut guard) = RAW_MANAGER_BY_DEVICE_ID.lock() {
        guard.clear();
    }
}

fn unregister_raw_manager_device(device_id: &str) {
    if let Ok(mut guard) = RAW_MANAGER_BY_DEVICE_ID.lock() {
        guard.remove(device_id);
    }
}

#[cfg(test)]
pub(crate) fn clear_raw_runtime_caches_for_tests() {
    if let Ok(mut guard) = RAW_MANAGER_CACHE.lock() {
        guard.clear();
    }
    clear_raw_manager_device_index();
    if let Ok(mut guard) = RAW_TOPOLOGY_MAP_CACHE.lock() {
        *guard = None;
    }
    RAW_TOPOLOGY_EPOCH.store(1, Ordering::Relaxed);
    RAW_TOPOLOGY_LAST_REFRESH_SECS.store(0, Ordering::Relaxed);
}

pub fn current_raw_topology_epoch() -> u64 {
    RAW_TOPOLOGY_EPOCH.load(Ordering::Relaxed).max(1)
}

fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn raw_device_map_path(index_root: &Path) -> PathBuf {
    index_root
        .join(RAW_DEVICE_MAP_DIR)
        .join(RAW_DEVICE_MAP_FILE)
}

fn read_raw_device_map_any(index_roots: &[String]) -> Option<RawDeviceMapTable> {
    for root in index_roots {
        let path = raw_device_map_path(Path::new(root));
        let Ok(bytes) = std::fs::read(&path) else {
            continue;
        };
        let Ok(table) = rmp_serde::from_slice::<RawDeviceMapTable>(&bytes) else {
            continue;
        };
        return Some(table);
    }
    None
}

fn write_raw_device_map_all(
    index_roots: &[String],
    table: &RawDeviceMapTable,
) -> Result<(), AppError> {
    let bytes = rmp_serde::to_vec(table)
        .map_err(|e| AppError::InternalServerError(format!("serialize raw device map: {}", e)))?;
    for root in index_roots {
        let final_path = raw_device_map_path(Path::new(root));
        let parent = final_path.parent().ok_or_else(|| {
            AppError::InternalServerError("invalid raw device map parent path".into())
        })?;
        std::fs::create_dir_all(parent).map_err(|e| {
            AppError::Io(e, format!("create raw device map dir {}", parent.display()))
        })?;
        let tmp_path = final_path.with_extension(format!(
            "tmp.{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        std::fs::write(&tmp_path, &bytes)
            .map_err(|e| AppError::Io(e, format!("write raw device map {}", tmp_path.display())))?;
        std::fs::rename(&tmp_path, &final_path).map_err(|e| {
            AppError::Io(
                e,
                format!(
                    "rename raw device map {} -> {}",
                    tmp_path.display(),
                    final_path.display()
                ),
            )
        })?;
    }
    Ok(())
}

fn sync_raw_device_map(force: bool) -> Result<u64, AppError> {
    let now_secs = now_unix_secs();
    if !force {
        let last = RAW_TOPOLOGY_LAST_REFRESH_SECS.load(Ordering::Relaxed);
        if now_secs.saturating_sub(last) < RAW_TOPOLOGY_REFRESH_INTERVAL_SECS {
            return Ok(current_raw_topology_epoch());
        }
    }

    let index_roots = configured_index_store_disks()?;
    if index_roots.is_empty() {
        return Ok(current_raw_topology_epoch());
    }

    let mut existing = {
        RAW_TOPOLOGY_MAP_CACHE
            .lock()
            .map_err(|_| AppError::InternalServerError("raw topology cache lock poisoned".into()))?
            .clone()
    };
    if existing.is_none() {
        existing = read_raw_device_map_any(&index_roots);
    }

    let statuses = collect_raw_blob_device_statuses()?;
    let now_ts = Utc::now().timestamp();
    let online_entries = statuses
        .iter()
        .filter(|status| status.status == "managed")
        .filter_map(|status| {
            let device_id = status.device_id.clone()?;
            Some(RawDeviceMapEntry {
                device_id,
                canonical_path: status.canonical_path.clone(),
                kind: status.kind,
                state: "online".to_string(),
                first_seen_at: now_ts,
                last_seen_at: now_ts,
                last_seen_epoch: 1,
            })
        })
        .collect::<Vec<_>>();

    let mut prev_online = HashSet::<String>::new();
    let mut epoch = existing
        .as_ref()
        .map(|t| t.topology_epoch)
        .unwrap_or(1)
        .max(1);
    if let Some(prev) = existing.as_ref() {
        for entry in &prev.entries {
            if entry.state == "online" {
                prev_online.insert(entry.device_id.clone());
            }
        }
    }
    let next_online = online_entries
        .iter()
        .map(|entry| entry.device_id.clone())
        .collect::<HashSet<_>>();
    if existing.is_some() && prev_online != next_online {
        epoch = epoch.saturating_add(1);
    }

    let mut merged = HashMap::<String, RawDeviceMapEntry>::new();
    if let Some(prev) = existing {
        for entry in prev.entries {
            merged.insert(entry.device_id.clone(), entry);
        }
    }

    for mut entry in online_entries {
        if let Some(prev) = merged.get(&entry.device_id) {
            entry.first_seen_at = prev.first_seen_at;
        }
        entry.last_seen_at = now_ts;
        entry.last_seen_epoch = epoch;
        entry.state = "online".to_string();
        merged.insert(entry.device_id.clone(), entry);
    }

    for entry in merged.values_mut() {
        if !next_online.contains(&entry.device_id) {
            entry.state = "missing".to_string();
        }
    }

    let mut entries = merged.into_values().collect::<Vec<_>>();
    entries.sort_by(|a, b| a.device_id.cmp(&b.device_id));
    let table = RawDeviceMapTable {
        topology_epoch: epoch,
        updated_at: now_ts,
        entries,
    };

    write_raw_device_map_all(&index_roots, &table)?;
    RAW_TOPOLOGY_EPOCH.store(epoch, Ordering::Relaxed);
    RAW_TOPOLOGY_LAST_REFRESH_SECS.store(now_secs, Ordering::Relaxed);
    if let Ok(mut cache) = RAW_TOPOLOGY_MAP_CACHE.lock() {
        *cache = Some(table);
    }
    Ok(epoch)
}

fn sync_raw_device_map_best_effort(force: bool) {
    if let Err(e) = sync_raw_device_map(force) {
        warn!("Raw topology/device map sync skipped: {}", e);
    }
}

pub fn stable_shard_file_name(part_index: u64, shard_index: usize, is_parity: bool) -> String {
    let ext = if is_parity {
        PARITY_SUFFIX
    } else {
        SHARD_SUFFIX
    };
    format!("part_{}_shard_{}{}", part_index, shard_index, ext)
}

pub fn raw_blob_address_from_extent(extent: &RawExtentAddress) -> RawBlobAddress {
    RawBlobAddress {
        topology_epoch: current_raw_topology_epoch(),
        device_id: extent.device_id.to_string(),
        allocation_id: extent.allocation_id,
        offset_bytes: extent.offset_bytes,
        size_bytes: extent.size_bytes,
    }
}

pub fn raw_blob_debug_path(raw: &RawBlobAddress) -> String {
    format!("raw://{}/{}", raw.device_id, raw.allocation_id)
}

pub fn configured_blob_disk_count() -> Result<usize, AppError> {
    let disks = configured_blob_store_disks()?;
    if disks.is_empty() {
        return Err(AppError::InternalServerError(
            "No blob disks configured".into(),
        ));
    }
    Ok(disks.len())
}

pub fn validate_configured_raw_blob_disks() -> Result<Vec<RawBlobDeviceStatus>, AppError> {
    let statuses = collect_raw_blob_device_statuses()?;
    sync_raw_device_map_best_effort(false);
    let usable = statuses
        .iter()
        .filter(|status| matches!(status.status.as_str(), "managed" | "unformatted"))
        .count();
    if usable == 0 {
        return Err(AppError::InternalServerError(
            "No usable raw blob disks available from blob_store_disks".into(),
        ));
    }
    Ok(statuses)
}

pub fn raw_blob_device_statuses() -> Result<Vec<RawBlobDeviceStatus>, AppError> {
    let statuses = collect_raw_blob_device_statuses()?;
    sync_raw_device_map_best_effort(false);
    Ok(statuses)
}

pub fn format_raw_blob_device(path: &Path, force: bool) -> Result<RawBlobDeviceStatus, AppError> {
    let canonical = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let inspection = inspect_raw_blob_device(&canonical, false)?;

    if inspection.mounted {
        return Err(AppError::BadRequest(format!(
            "blob device {} is mounted at {}; raw blob disks must stay unmounted",
            inspection.path.display(),
            inspection.mount_points.join(", ")
        )));
    }

    match inspection.state {
        RawBlobDiskState::Unformatted => {}
        RawBlobDiskState::Managed
        | RawBlobDiskState::ForeignSignature
        | RawBlobDiskState::DirtyUnknown
            if force => {}
        RawBlobDiskState::Managed => {
            return Err(AppError::BadRequest(format!(
                "blob device {} is already N1-formatted; rerun with --force to reinitialize it",
                inspection.path.display()
            )));
        }
        RawBlobDiskState::ForeignSignature => {
            return Err(AppError::BadRequest(format!(
                "blob device {} contains foreign on-disk data ({}); rerun with --force only if you intend to destroy it",
                inspection.path.display(),
                inspection
                    .detected_signature
                    .as_deref()
                    .unwrap_or("unknown signature")
            )));
        }
        RawBlobDiskState::DirtyUnknown => {
            return Err(AppError::BadRequest(format!(
                "blob device {} contains unknown non-zero data; rerun with --force only if you intend to destroy it",
                inspection.path.display()
            )));
        }
    }

    RAW_MANAGER_CACHE
        .lock()
        .map_err(|_| AppError::InternalServerError("Raw manager cache lock poisoned".into()))?
        .remove(&inspection.canonical_path);
    clear_raw_manager_device_index();

    BlockManager::format(
        &inspection.canonical_path,
        FormatOptions {
            journal_blocks: recommended_raw_journal_blocks(inspection.total_bytes),
            total_bytes: Some(inspection.total_bytes),
            ..Default::default()
        },
    )
    .map_err(block_manager_error_to_app)?;

    let refreshed = inspect_raw_blob_device(&inspection.canonical_path, false)?;
    sync_raw_device_map_best_effort(true);
    Ok(status_from_inspection(refreshed))
}

pub fn resolve_blob_disk(disk_idx: usize) -> Result<BlobDiskHandle, AppError> {
    let disks = configured_blob_store_disks()?;
    if disks.is_empty() {
        return Err(AppError::InternalServerError(
            "No blob disks configured".into(),
        ));
    }
    if disk_idx >= disks.len() {
        return Err(AppError::InternalServerError(format!(
            "Blob disk index {} out of bounds",
            disk_idx
        )));
    }

    let mut first_err: Option<AppError> = None;
    for offset in 0..disks.len() {
        let idx = (disk_idx + offset) % disks.len();
        match resolve_blob_disk_from_path(Path::new(&disks[idx])) {
            Ok(handle) => return Ok(handle),
            Err(err) => {
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
        }
    }

    Err(first_err.unwrap_or_else(|| {
        AppError::InternalServerError("No usable raw blob disks configured".into())
    }))
}

fn resolve_blob_disk_from_path(path: &Path) -> Result<BlobDiskHandle, AppError> {
    let inspection = inspect_raw_blob_device(path, true)?;
    let manager = open_or_format_raw_manager(&inspection.canonical_path)?;
    let device_id = register_raw_manager_device(&manager)?;
    sync_raw_device_map_best_effort(false);
    Ok(BlobDiskHandle::Raw { device_id, manager })
}

pub fn resolve_blob_disk_path(path: &Path) -> Result<BlobDiskHandle, AppError> {
    let disks = configured_blob_store_disks()?;
    let wanted = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    for raw in &disks {
        let candidate = PathBuf::from(raw);
        let canonical = fs::canonicalize(&candidate).unwrap_or(candidate.clone());
        if canonical == wanted {
            return resolve_blob_disk_from_path(&candidate);
        }
    }
    Err(AppError::InternalServerError(format!(
        "configured blob disk not found for {}",
        path.display()
    )))
}

fn collect_raw_blob_device_statuses() -> Result<Vec<RawBlobDeviceStatus>, AppError> {
    let disks = configured_blob_store_disks()?;
    let mut out = Vec::with_capacity(disks.len());
    for disk in &disks {
        let path = Path::new(disk);
        match inspect_raw_blob_device(path, false) {
            Ok(inspection) => out.push(status_from_inspection(inspection)),
            Err(err) => out.push(status_from_inspection_error(path, &err)),
        }
    }
    Ok(out)
}

fn status_from_inspection_error(path: &Path, err: &AppError) -> RawBlobDeviceStatus {
    let kind = raw_blob_path_kind_hint(path);
    let canonical = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let status = match err {
        AppError::BadRequest(_) => "misconfigured",
        AppError::Io(_, _) => "unavailable",
        _ => "error",
    }
    .to_string();

    RawBlobDeviceStatus {
        path: path.to_string_lossy().to_string(),
        canonical_path: canonical.to_string_lossy().to_string(),
        kind,
        status,
        direct_io: true,
        mounted: false,
        mount_points: Vec::new(),
        path_warning: raw_path_warning(path, kind),
        detected_signature: None,
        device_id: None,
        block_size_bytes: None,
        allocation_count: None,
        journal_cursor: None,
        total_bytes: None,
        used_bytes: None,
        available_bytes: None,
        error: Some(err.to_string()),
    }
}

fn raw_blob_path_kind_hint(path: &Path) -> RawBlobPathKind {
    if path.to_string_lossy().starts_with("/dev/") {
        RawBlobPathKind::BlockDevice
    } else {
        RawBlobPathKind::LoopbackFile
    }
}

pub async fn raw_stage_path_for_shard(
    disk_idx: usize,
    fragment_hash: &str,
    version_id: &str,
    attempt_id: &str,
    stable_name: &str,
) -> Result<PathBuf, AppError> {
    let root = configured_fs_storage_root()
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| std::env::temp_dir().join("n1-raw-stage"));
    let dir = root
        .join(".n1-raw-stage")
        .join(fragment_hash)
        .join(version_id)
        .join(attempt_id)
        .join(format!("disk_{}", disk_idx));
    tokio_fs::create_dir_all(&dir)
        .await
        .map_err(|e| AppError::Io(e, format!("create raw stage dir {}", dir.display())))?;
    Ok(dir.join(stable_name))
}

pub async fn commit_staged_raw_shard(
    disk_idx: usize,
    staged_path: &Path,
    size_bytes: u64,
) -> Result<RawBlobAddress, AppError> {
    let staged_path = staged_path.to_path_buf();
    let staged_for_task = staged_path.clone();
    let handle = tokio::task::spawn_blocking(move || -> Result<RawBlobAddress, AppError> {
        let BlobDiskHandle::Raw { manager, .. } = resolve_blob_disk(disk_idx)?;

        let mut src = File::open(&staged_for_task).map_err(|e| {
            AppError::Io(
                e,
                format!("open staged shard {}", staged_for_task.display()),
            )
        })?;
        let alloc = {
            let mut mgr = manager.write().map_err(|_| {
                AppError::InternalServerError("Raw blob manager lock poisoned".into())
            })?;
            match mgr.allocate_bytes(size_bytes) {
                Ok(handle) => handle,
                Err(BlockManagerError::NoSpace { .. }) => {
                    drop(mgr);
                    let drained = drain_deferred_raw_frees_under_pressure_blocking();
                    if drained > 0 {
                        info!(
                            "Raw allocator pressure reclaim freed {} deferred extent(s) before staged commit retry",
                            drained
                        );
                    }
                    let mut mgr_retry = manager.write().map_err(|_| {
                        AppError::InternalServerError("Raw blob manager lock poisoned".into())
                    })?;
                    mgr_retry
                        .allocate_bytes(size_bytes)
                        .map_err(block_manager_error_to_app)?
                }
                Err(err) => return Err(block_manager_error_to_app(err)),
            }
        };
        let mut mgr = manager
            .write()
            .map_err(|_| AppError::InternalServerError("Raw blob manager lock poisoned".into()))?;
        let address = match mgr.raw_extent_address(alloc, size_bytes) {
            Ok(v) => v,
            Err(e) => {
                let _ = mgr.free(alloc.allocation_id);
                return Err(block_manager_error_to_app(e));
            }
        };

        let mut buf = vec![0u8; 1024 * 1024];
        let mut offset = 0u64;
        loop {
            let read = src
                .read(&mut buf)
                .map_err(|e| AppError::Io(e, "read staged shard".into()))?;
            if read == 0 {
                break;
            }
            if let Err(e) = mgr.write_address(&address, offset, &buf[..read]) {
                let _ = mgr.free(alloc.allocation_id);
                return Err(block_manager_error_to_app(e));
            }
            offset = offset.saturating_add(read as u64);
        }

        if offset != size_bytes {
            let _ = mgr.free(alloc.allocation_id);
            return Err(AppError::InternalServerError(format!(
                "staged shard size mismatch: copied {} bytes but expected {}",
                offset, size_bytes
            )));
        }

        mgr.flush().map_err(block_manager_error_to_app)?;
        Ok(raw_blob_address_from_extent(&address))
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("raw shard commit task failed: {}", e)))??;

    let _ = tokio_fs::remove_file(staged_path).await;
    Ok(handle)
}

pub fn open_raw_direct_io(shard: &ShardInfo) -> Result<DirectExtentIo, AppError> {
    let raw = shard.raw_blob_address.as_ref().ok_or_else(|| {
        AppError::InternalServerError("Shard is not backed by raw blob storage".into())
    })?;
    open_raw_direct_io_for_address(raw)
}

pub fn open_raw_direct_io_for_address(raw: &RawBlobAddress) -> Result<DirectExtentIo, AppError> {
    let current_epoch = current_raw_topology_epoch();
    if raw.topology_epoch.max(1) != current_epoch {
        info!(
            "Raw address epoch {} differs from current topology epoch {} for device {} allocation {}; continuing with mapped device lookup",
            raw.topology_epoch.max(1),
            current_epoch,
            raw.device_id,
            raw.allocation_id
        );
    }
    let manager = raw_manager_by_device_id(raw.device_id.as_str())?;
    let mgr = manager
        .read()
        .map_err(|_| AppError::InternalServerError("Raw blob manager lock poisoned".into()))?;
    let address = RawExtentAddress {
        device_id: mgr.header().device_id,
        allocation_id: raw.allocation_id,
        offset_bytes: raw.offset_bytes,
        size_bytes: raw.size_bytes,
    };
    mgr.direct_extent_io(&address)
        .map_err(block_manager_error_to_app)
}

pub fn allocate_raw_direct_io(
    disk_idx: usize,
    size_bytes: u64,
) -> Result<(RawBlobAddress, DirectExtentIo), AppError> {
    let BlobDiskHandle::Raw { manager, .. } = resolve_blob_disk(disk_idx)?;
    let alloc = {
        let mut mgr = manager
            .write()
            .map_err(|_| AppError::InternalServerError("Raw blob manager lock poisoned".into()))?;
        match mgr.allocate_bytes(size_bytes) {
            Ok(handle) => handle,
            Err(BlockManagerError::NoSpace { .. }) => {
                drop(mgr);
                let drained = drain_deferred_raw_frees_under_pressure_blocking();
                if drained > 0 {
                    info!(
                        "Raw allocator pressure reclaim freed {} deferred extent(s) before allocation retry",
                        drained
                    );
                }
                let mut mgr_retry = manager.write().map_err(|_| {
                    AppError::InternalServerError("Raw blob manager lock poisoned".into())
                })?;
                mgr_retry
                    .allocate_bytes(size_bytes)
                    .map_err(block_manager_error_to_app)?
            }
            Err(err) => return Err(block_manager_error_to_app(err)),
        }
    };
    let mut mgr = manager
        .write()
        .map_err(|_| AppError::InternalServerError("Raw blob manager lock poisoned".into()))?;
    let address = match mgr.raw_extent_address(alloc, size_bytes) {
        Ok(v) => v,
        Err(e) => {
            let _ = mgr.free(alloc.allocation_id);
            return Err(block_manager_error_to_app(e));
        }
    };
    let io = match mgr.direct_extent_io(&address) {
        Ok(v) => v,
        Err(e) => {
            let _ = mgr.free(alloc.allocation_id);
            return Err(block_manager_error_to_app(e));
        }
    };
    Ok((raw_blob_address_from_extent(&address), io))
}

pub async fn copy_raw_extent(
    src: DirectExtentIo,
    dst: DirectExtentIo,
    size_bytes: u64,
    chunk_size: usize,
    flush: bool,
) -> Result<(), AppError> {
    let chunk_size = chunk_size.max(4096);
    tokio::task::spawn_blocking(move || -> Result<(), AppError> {
        let mut buf = vec![0u8; chunk_size];
        let mut offset = 0u64;
        while offset < size_bytes {
            let take = size_bytes.saturating_sub(offset).min(chunk_size as u64) as usize;
            src.read_into(offset, &mut buf[..take])
                .map_err(block_manager_error_to_app)?;
            dst.write(offset, &buf[..take])
                .map_err(block_manager_error_to_app)?;
            offset = offset.saturating_add(take as u64);
        }
        if flush {
            dst.flush_data().map_err(block_manager_error_to_app)?;
        }
        Ok(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("raw extent copy task failed: {}", e)))?
}

pub async fn write_shard_range(
    shard: &ShardInfo,
    offset: u64,
    data: &[u8],
) -> Result<(), AppError> {
    if let Some(raw) = shard.raw_blob_address.as_ref() {
        let raw = raw.clone();
        let payload = data.to_vec();
        return tokio::task::spawn_blocking(move || {
            let manager = raw_manager_by_device_id(raw.device_id.as_str())?;
            let mgr = manager.read().map_err(|_| {
                AppError::InternalServerError("Raw blob manager lock poisoned".into())
            })?;
            let address = RawExtentAddress {
                device_id: mgr.header().device_id,
                allocation_id: raw.allocation_id,
                offset_bytes: raw.offset_bytes,
                size_bytes: raw.size_bytes,
            };
            mgr.write_address(&address, offset, &payload)
                .map_err(block_manager_error_to_app)?;
            mgr.flush().map_err(block_manager_error_to_app)
        })
        .await
        .map_err(|e| AppError::InternalServerError(format!("raw write task failed: {}", e)))?;
    }

    let path = PathBuf::from(shard.absolute_path.as_str());
    let payload = data.to_vec();
    tokio::task::spawn_blocking(move || -> Result<(), AppError> {
        use std::io::Write;
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(|e| AppError::Io(e, format!("open shard {}", path.display())))?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| AppError::Io(e, format!("seek shard {}", path.display())))?;
        file.write_all(&payload)
            .map_err(|e| AppError::Io(e, format!("write shard {}", path.display())))?;
        file.sync_data()
            .map_err(|e| AppError::Io(e, format!("sync shard {}", path.display())))?;
        Ok(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("filesystem write task failed: {}", e)))?
}

pub async fn verify_shard_checksum(shard: &ShardInfo) -> Result<bool, AppError> {
    if shard.checksum.is_empty() {
        return Ok(true);
    }
    let expected = u32::from_str_radix(shard.checksum.as_str(), 16)
        .map_err(|_| AppError::InternalServerError("invalid shard checksum".into()))?;

    let shard_clone = shard.clone();
    tokio::task::spawn_blocking(move || -> Result<bool, AppError> {
        let mut hasher = Hasher::new();
        let mut offset = 0u64;
        let mut remaining = shard_clone.size_bytes;
        while remaining > 0 {
            let take = remaining.min(1024 * 1024) as usize;
            let chunk = if let Some(raw) = shard_clone.raw_blob_address.as_ref() {
                let manager = raw_manager_by_device_id(raw.device_id.as_str())?;
                let mgr = manager.read().map_err(|_| {
                    AppError::InternalServerError("Raw blob manager lock poisoned".into())
                })?;
                let address = RawExtentAddress {
                    device_id: mgr.header().device_id,
                    allocation_id: raw.allocation_id,
                    offset_bytes: raw.offset_bytes,
                    size_bytes: raw.size_bytes,
                };
                mgr.read_address(&address, offset, take)
                    .map_err(block_manager_error_to_app)?
            } else {
                let mut file = File::open(&shard_clone.absolute_path).map_err(|e| {
                    AppError::Io(e, format!("open shard {}", shard_clone.absolute_path))
                })?;
                file.seek(SeekFrom::Start(offset))
                    .map_err(|e| AppError::Io(e, "seek shard for checksum".into()))?;
                let mut buf = vec![0u8; take];
                file.read_exact(&mut buf)
                    .map_err(|e| AppError::Io(e, "read shard for checksum".into()))?;
                buf
            };
            hasher.update(&chunk);
            offset = offset.saturating_add(take as u64);
            remaining = remaining.saturating_sub(take as u64);
        }
        Ok(hasher.finalize() == expected)
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("checksum task failed: {}", e)))?
}

pub async fn free_raw_shards_for_metadata(metadata: &ObjectMetadata) -> Result<(), AppError> {
    let mut seen: Vec<RawBlobAddress> = Vec::new();
    for part in &metadata.parts {
        for shard in &part.shards {
            if let Some(raw) = shard.raw_blob_address.as_ref() {
                if !seen.iter().any(|existing| existing == raw) {
                    seen.push(raw.clone());
                }
            }
        }
    }
    free_raw_addresses(&seen).await
}

pub async fn free_raw_shards_for_metadatas(metadatas: &[ObjectMetadata]) -> Result<(), AppError> {
    let mut addrs = Vec::<RawBlobAddress>::new();
    let mut seen = HashSet::<(u64, String, u64, u64, u64)>::new();

    for metadata in metadatas {
        for part in &metadata.parts {
            for shard in &part.shards {
                let Some(raw) = shard.raw_blob_address.as_ref() else {
                    continue;
                };
                let key = (
                    raw.topology_epoch,
                    raw.device_id.clone(),
                    raw.allocation_id,
                    raw.offset_bytes,
                    raw.size_bytes,
                );
                if seen.insert(key) {
                    addrs.push(raw.clone());
                }
            }
        }
    }

    free_raw_addresses(&addrs).await
}

fn raw_address_identity_key(raw: &RawBlobAddress) -> (u64, String, u64, u64, u64) {
    (
        raw.topology_epoch.max(1),
        raw.device_id.clone(),
        raw.allocation_id,
        raw.offset_bytes,
        raw.size_bytes,
    )
}

fn raw_deferred_marker_file_name(raw: &RawBlobAddress) -> String {
    let sanitized_device = raw
        .device_id
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect::<String>();
    format!(
        "e{}-{}-{}-{}-{}.msgpack",
        raw.topology_epoch.max(1),
        sanitized_device,
        raw.allocation_id,
        raw.offset_bytes,
        raw.size_bytes
    )
}

fn raw_deferred_marker_path(index_root: &Path, raw: &RawBlobAddress) -> PathBuf {
    index_root
        .join(RAW_DEVICE_MAP_DIR)
        .join(RAW_DEFERRED_FREE_DIR)
        .join(raw_deferred_marker_file_name(raw))
}

async fn write_deferred_raw_free_markers(addresses: &[RawBlobAddress]) -> Result<(), AppError> {
    if addresses.is_empty() {
        return Ok(());
    }
    let index_roots = configured_index_store_disks()?;
    if index_roots.is_empty() {
        return Err(AppError::InternalServerError(
            "No index roots configured for deferred raw free markers".into(),
        ));
    }

    let mut unique = HashSet::<(u64, String, u64, u64, u64)>::new();
    for raw in addresses {
        let key = raw_address_identity_key(raw);
        if !unique.insert(key) {
            continue;
        }
        let marker = RawDeferredFreeMarker {
            address: raw.clone(),
            created_at: Utc::now().timestamp(),
        };
        let bytes = rmp_serde::to_vec(&marker).map_err(|e| {
            AppError::InternalServerError(format!("serialize deferred raw free marker: {}", e))
        })?;
        for root in &index_roots {
            let path = raw_deferred_marker_path(Path::new(root), raw);
            if let Some(parent) = path.parent() {
                tokio_fs::create_dir_all(parent).await.map_err(|e| {
                    AppError::Io(
                        e,
                        format!("create deferred raw free dir {}", parent.display()),
                    )
                })?;
            }
            tokio_fs::write(&path, &bytes).await.map_err(|e| {
                AppError::Io(
                    e,
                    format!("write deferred raw free marker {}", path.display()),
                )
            })?;
        }
    }
    Ok(())
}

async fn collect_deferred_raw_free_markers(
    batch_limit: usize,
) -> Result<Vec<(RawBlobAddress, Vec<PathBuf>)>, AppError> {
    let index_roots = configured_index_store_disks()?;
    if index_roots.is_empty() {
        return Ok(Vec::new());
    }
    let roots = index_roots
        .into_iter()
        .map(PathBuf::from)
        .collect::<Vec<PathBuf>>();
    tokio::task::spawn_blocking(move || {
        let mut markers =
            HashMap::<(u64, String, u64, u64, u64), (RawBlobAddress, Vec<PathBuf>)>::new();
        for root in roots {
            let dir = root.join(RAW_DEVICE_MAP_DIR).join(RAW_DEFERRED_FREE_DIR);
            if !dir.exists() {
                continue;
            }
            for entry in WalkDir::new(&dir).into_iter().filter_map(|e| e.ok()) {
                if !entry.file_type().is_file() {
                    continue;
                }
                let Ok(bytes) = std::fs::read(entry.path()) else {
                    continue;
                };
                let Ok(marker) = rmp_serde::from_slice::<RawDeferredFreeMarker>(&bytes) else {
                    continue;
                };
                let key = raw_address_identity_key(&marker.address);
                let path = entry.path().to_path_buf();
                markers
                    .entry(key)
                    .and_modify(|(_, paths)| paths.push(path.clone()))
                    .or_insert((marker.address, vec![path]));
                if markers.len() >= batch_limit {
                    break;
                }
            }
            if markers.len() >= batch_limit {
                break;
            }
        }
        Ok::<_, AppError>(markers.into_values().collect::<Vec<_>>())
    })
    .await
    .map_err(|e| {
        AppError::InternalServerError(format!(
            "collect deferred raw free markers task failed: {}",
            e
        ))
    })?
}

async fn remove_deferred_marker_paths(paths: &[PathBuf]) {
    for path in paths {
        let _ = tokio_fs::remove_file(path).await;
    }
}

fn collect_deferred_raw_free_markers_blocking(
    batch_limit: usize,
) -> Result<Vec<(RawBlobAddress, Vec<PathBuf>)>, AppError> {
    let index_roots = configured_index_store_disks()?;
    if index_roots.is_empty() {
        return Ok(Vec::new());
    }
    let mut markers =
        HashMap::<(u64, String, u64, u64, u64), (RawBlobAddress, Vec<PathBuf>)>::new();
    for root in index_roots {
        let dir = PathBuf::from(root)
            .join(RAW_DEVICE_MAP_DIR)
            .join(RAW_DEFERRED_FREE_DIR);
        if !dir.exists() {
            continue;
        }
        for entry in WalkDir::new(&dir).into_iter().filter_map(|e| e.ok()) {
            if !entry.file_type().is_file() {
                continue;
            }
            let Ok(bytes) = std::fs::read(entry.path()) else {
                continue;
            };
            let Ok(marker) = rmp_serde::from_slice::<RawDeferredFreeMarker>(&bytes) else {
                continue;
            };
            let key = raw_address_identity_key(&marker.address);
            let path = entry.path().to_path_buf();
            markers
                .entry(key)
                .and_modify(|(_, paths)| paths.push(path.clone()))
                .or_insert((marker.address, vec![path]));
            if markers.len() >= batch_limit {
                break;
            }
        }
        if markers.len() >= batch_limit {
            break;
        }
    }
    Ok(markers.into_values().collect::<Vec<_>>())
}

fn remove_deferred_marker_paths_blocking(paths: &[PathBuf]) {
    for path in paths {
        let _ = std::fs::remove_file(path);
    }
}

fn free_raw_addresses_immediate_blocking(addresses: &[RawBlobAddress]) -> Result<(), AppError> {
    if addresses.is_empty() {
        return Ok(());
    }

    let mut grouped = HashMap::<String, Vec<RawBlobAddress>>::new();
    let mut seen = HashSet::<(u64, String, u64, u64, u64)>::new();
    for raw in addresses {
        let key = raw_address_identity_key(raw);
        if seen.insert(key) {
            grouped
                .entry(raw.device_id.clone())
                .or_default()
                .push(raw.clone());
        }
    }

    for (device_id, addresses) in grouped {
        let manager = match raw_manager_by_device_id(device_id.as_str()) {
            Ok(manager) => manager,
            Err(err) if is_missing_raw_device_mapping_error(&err) => continue,
            Err(err) => return Err(err),
        };
        let mut mgr = manager
            .write()
            .map_err(|_| AppError::InternalServerError("Raw blob manager lock poisoned".into()))?;

        let manager_device_id = mgr.header().device_id;
        let mut raw_extents = Vec::<RawExtentAddress>::with_capacity(addresses.len());
        for raw in addresses {
            raw_extents.push(RawExtentAddress {
                device_id: manager_device_id,
                allocation_id: raw.allocation_id,
                offset_bytes: raw.offset_bytes,
                size_bytes: raw.size_bytes,
            });
        }

        let freed = match mgr.free_addresses(&raw_extents) {
            Ok(freed) => freed,
            Err(BlockManagerError::UnknownAllocation(_)) => {
                let mut recovered_freed = 0usize;
                for address in &raw_extents {
                    match mgr.free_address(address) {
                        Ok(()) => {
                            recovered_freed = recovered_freed.saturating_add(1);
                        }
                        Err(BlockManagerError::UnknownAllocation(_)) => {}
                        Err(err) => return Err(block_manager_error_to_app(err)),
                    }
                }
                recovered_freed
            }
            Err(err) => return Err(block_manager_error_to_app(err)),
        };
        if freed > 0 {
            mgr.flush_meta().map_err(block_manager_error_to_app)?;
        }
    }
    Ok(())
}

fn apply_deferred_raw_frees_once_blocking(batch_limit: usize) -> Result<usize, AppError> {
    let markers = collect_deferred_raw_free_markers_blocking(batch_limit)?;
    if markers.is_empty() {
        return Ok(0);
    }

    let mut freed = 0usize;
    for (raw, marker_paths) in markers {
        let free_result = free_raw_addresses_immediate_blocking(std::slice::from_ref(&raw));
        match free_result {
            Ok(()) => {
                remove_deferred_marker_paths_blocking(&marker_paths);
                freed = freed.saturating_add(1);
            }
            Err(err) if is_missing_raw_device_mapping_error(&err) => {}
            Err(err) => {
                warn!(
                    "Deferred raw free (blocking pressure path) failed for {}:{}:{}:{} (epoch={}): {}",
                    raw.device_id,
                    raw.allocation_id,
                    raw.offset_bytes,
                    raw.size_bytes,
                    raw.topology_epoch.max(1),
                    err
                );
            }
        }
    }
    Ok(freed)
}

fn drain_deferred_raw_frees_under_pressure_blocking() -> usize {
    let mut total_freed = 0usize;
    for _ in 0..RAW_DEFERRED_FREE_PRESSURE_MAX_BATCHES {
        match apply_deferred_raw_frees_once_blocking(RAW_DEFERRED_FREE_BATCH_SIZE) {
            Ok(0) => break,
            Ok(freed) => {
                total_freed = total_freed.saturating_add(freed);
            }
            Err(err) => {
                warn!("Deferred raw free pressure-drain aborted: {}", err);
                break;
            }
        }
    }
    total_freed
}

async fn free_raw_addresses_immediate(addresses: &[RawBlobAddress]) -> Result<(), AppError> {
    if addresses.is_empty() {
        return Ok(());
    }

    let mut grouped = HashMap::<String, Vec<RawBlobAddress>>::new();
    let mut seen = HashSet::<(u64, String, u64, u64, u64)>::new();
    for raw in addresses {
        let key = raw_address_identity_key(raw);
        if seen.insert(key) {
            grouped
                .entry(raw.device_id.clone())
                .or_default()
                .push(raw.clone());
        }
    }

    for (device_id, addresses) in grouped {
        let result = tokio::task::spawn_blocking(move || {
            let manager = match raw_manager_by_device_id(device_id.as_str()) {
                Ok(manager) => manager,
                Err(err) if is_missing_raw_device_mapping_error(&err) => return Ok(()),
                Err(err) => return Err(err),
            };
            let mut mgr = manager.write().map_err(|_| {
                AppError::InternalServerError("Raw blob manager lock poisoned".into())
            })?;

            let manager_device_id = mgr.header().device_id;
            let mut raw_extents = Vec::<RawExtentAddress>::with_capacity(addresses.len());
            for raw in addresses {
                raw_extents.push(RawExtentAddress {
                    device_id: manager_device_id,
                    allocation_id: raw.allocation_id,
                    offset_bytes: raw.offset_bytes,
                    size_bytes: raw.size_bytes,
                });
            }

            let freed = match mgr.free_addresses(&raw_extents) {
                Ok(freed) => freed,
                Err(BlockManagerError::UnknownAllocation(_)) => {
                    // Batch free should stay idempotent across retries/crash windows where
                    // some extents were already freed before index metadata was cleared.
                    let mut recovered_freed = 0usize;
                    for address in &raw_extents {
                        match mgr.free_address(address) {
                            Ok(()) => {
                                recovered_freed = recovered_freed.saturating_add(1);
                            }
                            Err(BlockManagerError::UnknownAllocation(_)) => {
                                // Already freed; treat as success for cleanup workflows.
                            }
                            Err(err) => return Err(block_manager_error_to_app(err)),
                        }
                    }
                    recovered_freed
                }
                Err(err) => return Err(block_manager_error_to_app(err)),
            };
            if freed > 0 {
                mgr.flush_meta().map_err(block_manager_error_to_app)?;
            }
            Ok::<(), AppError>(())
        })
        .await
        .map_err(|e| AppError::InternalServerError(format!("raw batch free task failed: {}", e)))?;

        if let Err(err) = result {
            return Err(err);
        }
    }

    Ok(())
}

async fn apply_deferred_raw_frees_once(batch_limit: usize) -> Result<usize, AppError> {
    let markers = collect_deferred_raw_free_markers(batch_limit).await?;
    if markers.is_empty() {
        return Ok(0);
    }

    let mut freed = 0usize;
    for (raw, marker_paths) in markers {
        let free_result = free_raw_addresses_immediate(std::slice::from_ref(&raw)).await;
        match free_result {
            Ok(()) => {
                remove_deferred_marker_paths(&marker_paths).await;
                freed = freed.saturating_add(1);
            }
            Err(err) if is_missing_raw_device_mapping_error(&err) => {
                // Leave marker for future topology epochs/device reattachment.
            }
            Err(err) => {
                warn!(
                    "Deferred raw free apply failed for {}:{}:{}:{} (epoch={}): {}",
                    raw.device_id,
                    raw.allocation_id,
                    raw.offset_bytes,
                    raw.size_bytes,
                    raw.topology_epoch.max(1),
                    err
                );
            }
        }
    }

    Ok(freed)
}

fn ensure_deferred_raw_free_worker_started() {
    if RAW_DEFERRED_FREE_WORKER_STARTED
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
        .is_err()
    {
        return;
    }

    tokio::spawn(async {
        loop {
            RAW_DEFERRED_FREE_NOTIFY.notified().await;
            loop {
                match apply_deferred_raw_frees_once(RAW_DEFERRED_FREE_BATCH_SIZE).await {
                    Ok(0) => break,
                    Ok(_) => continue,
                    Err(err) => {
                        warn!("Deferred raw free worker paused: {}", err);
                        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                        break;
                    }
                }
            }
        }
    });
}

pub async fn free_raw_addresses(addresses: &[RawBlobAddress]) -> Result<(), AppError> {
    if addresses.is_empty() {
        return Ok(());
    }

    if addresses.len() < RAW_DEFERRED_FREE_THRESHOLD {
        return free_raw_addresses_immediate(addresses).await;
    }

    write_deferred_raw_free_markers(addresses).await?;
    ensure_deferred_raw_free_worker_started();
    RAW_DEFERRED_FREE_NOTIFY.notify_one();
    info!(
        "Deferred {} raw extent free request(s) for background apply",
        addresses.len()
    );
    Ok(())
}

pub async fn flush_raw_metadata_for_addresses(
    addresses: &[RawBlobAddress],
) -> Result<(), AppError> {
    let mut device_ids = std::collections::HashSet::<String>::new();
    for raw in addresses {
        device_ids.insert(raw.device_id.clone());
    }

    for device_id in device_ids {
        tokio::task::spawn_blocking(move || {
            let manager = raw_manager_by_device_id(device_id.as_str())?;
            let mut mgr = manager.write().map_err(|_| {
                AppError::InternalServerError("Raw blob manager lock poisoned".into())
            })?;
            mgr.flush_meta().map_err(block_manager_error_to_app)
        })
        .await
        .map_err(|e| {
            AppError::InternalServerError(format!("raw metadata flush task failed: {}", e))
        })??;
    }

    Ok(())
}

pub fn metadata_contains_raw_shards(metadata: &ObjectMetadata) -> bool {
    metadata
        .parts
        .iter()
        .flat_map(|part| part.shards.iter())
        .any(|shard| shard.raw_blob_address.is_some())
}

pub async fn free_raw_address(raw: &RawBlobAddress) -> Result<(), AppError> {
    free_raw_addresses(std::slice::from_ref(raw)).await
}

pub async fn raw_blob_address_exists(raw: &RawBlobAddress) -> Result<bool, AppError> {
    let manager = match raw_manager_by_device_id(raw.device_id.as_str()) {
        Ok(manager) => manager,
        Err(err) if is_missing_raw_device_mapping_error(&err) => return Ok(false),
        Err(err) => return Err(err),
    };
    let address = RawExtentAddress {
        device_id: manager
            .read()
            .map_err(|_| AppError::InternalServerError("raw manager lock poisoned".into()))?
            .header()
            .device_id,
        allocation_id: raw.allocation_id,
        offset_bytes: raw.offset_bytes,
        size_bytes: raw.size_bytes,
    };

    tokio::task::spawn_blocking(move || {
        let mgr = manager
            .read()
            .map_err(|_| AppError::InternalServerError("raw manager lock poisoned".into()))?;
        match mgr.read_address(&address, 0, 0) {
            Ok(_) => Ok(true),
            Err(BlockManagerError::UnknownAllocation(_)) => Ok(false),
            Err(err) => Err(block_manager_error_to_app(err)),
        }
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("raw exists task failed: {}", e)))?
}

pub async fn write_raw_gc_markers(
    index_roots: &[PathBuf],
    fragment: &str,
    addresses: &[RawBlobAddress],
) -> Result<(), AppError> {
    if addresses.is_empty() {
        return Ok(());
    }

    let created_at = chrono::Utc::now().timestamp();
    for raw in addresses {
        let marker = RawGcMarker {
            fragment: fragment.to_string(),
            address: raw.clone(),
            created_at,
        };
        let bytes = rmp_serde::to_vec(&marker).map_err(|e| {
            AppError::InternalServerError(format!("serialize raw gc marker: {}", e))
        })?;

        for root in index_roots {
            let path = raw_gc_marker_path(root, fragment, raw);
            if let Some(parent) = path.parent() {
                tokio_fs::create_dir_all(parent).await.map_err(|e| {
                    AppError::Io(e, format!("create raw gc dir {}", parent.display()))
                })?;
            }
            tokio_fs::write(&path, &bytes)
                .await
                .map_err(|e| AppError::Io(e, format!("write raw gc marker {}", path.display())))?;
        }
    }

    Ok(())
}

pub fn raw_gc_marker_path(index_root: &Path, fragment: &str, raw: &RawBlobAddress) -> PathBuf {
    index_root
        .join(fragment)
        .join("index")
        .join(".raw_gc")
        .join(raw_gc_marker_file_name(raw))
}

pub fn raw_gc_marker_file_name(raw: &RawBlobAddress) -> String {
    let sanitized_device = raw
        .device_id
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect::<String>();
    format!(
        "e{}-{}-{}-{}-{}.msgpack",
        raw.topology_epoch.max(1),
        sanitized_device,
        raw.allocation_id,
        raw.offset_bytes,
        raw.size_bytes
    )
}

fn raw_manager_by_device_id(device_id: &str) -> Result<Arc<RwLock<BlockManager>>, AppError> {
    sync_raw_device_map_best_effort(false);
    if let Some(manager) = RAW_MANAGER_BY_DEVICE_ID
        .lock()
        .map_err(|_| {
            AppError::InternalServerError("Raw manager device index lock poisoned".into())
        })?
        .get(device_id)
        .cloned()
    {
        return Ok(manager);
    }

    let disk_count = configured_blob_disk_count()?;
    let mut first_resolve_err: Option<AppError> = None;
    let mut resolved_any = false;
    for idx in 0..disk_count {
        let BlobDiskHandle::Raw {
            device_id: raw_device_id,
            manager,
            ..
        } = match resolve_blob_disk(idx) {
            Ok(handle) => handle,
            Err(err) => {
                if first_resolve_err.is_none() {
                    first_resolve_err = Some(err);
                }
                continue;
            }
        };
        resolved_any = true;
        RAW_MANAGER_BY_DEVICE_ID
            .lock()
            .map_err(|_| {
                AppError::InternalServerError("Raw manager device index lock poisoned".into())
            })?
            .insert(raw_device_id.clone(), manager.clone());
        if raw_device_id == device_id {
            return Ok(manager);
        }
    }
    if !resolved_any {
        return Err(first_resolve_err.unwrap_or_else(|| {
            AppError::InternalServerError("No usable raw blob disks configured".into())
        }));
    }
    sync_raw_device_map_best_effort(true);
    Err(AppError::InternalServerError(format!(
        "{}{}",
        MISSING_RAW_DEVICE_ERROR_PREFIX, device_id
    )))
}

pub fn is_missing_raw_device_mapping_error(err: &AppError) -> bool {
    matches!(
        err,
        AppError::InternalServerError(message)
            if message.starts_with(MISSING_RAW_DEVICE_ERROR_PREFIX)
    )
}

fn open_or_format_raw_manager(path: &Path) -> Result<Arc<RwLock<BlockManager>>, AppError> {
    let canonical = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let inspection = inspect_raw_blob_device(&canonical, true)?;
    if let Some(existing) = {
        let cache = RAW_MANAGER_CACHE
            .lock()
            .map_err(|_| AppError::InternalServerError("Raw manager cache lock poisoned".into()))?;
        cache.get(&inspection.canonical_path).cloned()
    } {
        let cached_device_id = existing
            .read()
            .map_err(|_| AppError::InternalServerError("Raw blob manager lock poisoned".into()))?
            .header()
            .device_id
            .to_string();
        let cache_is_current = if inspection.state == RawBlobDiskState::Managed {
            match BlockManager::open(&inspection.canonical_path) {
                Ok(on_disk) => on_disk.header().device_id.to_string() == cached_device_id,
                Err(BlockManagerError::InvalidDevice(_)) => false,
                Err(err) => return Err(block_manager_error_to_app(err)),
            }
        } else {
            false
        };

        if cache_is_current {
            let _ = register_raw_manager_device(&existing)?;
            return Ok(existing);
        }

        RAW_MANAGER_CACHE
            .lock()
            .map_err(|_| AppError::InternalServerError("Raw manager cache lock poisoned".into()))?
            .remove(&inspection.canonical_path);
        unregister_raw_manager_device(cached_device_id.as_str());
    }

    let manager = match BlockManager::open(&inspection.canonical_path) {
        Ok(mgr) => {
            let recommended = recommended_raw_journal_blocks(inspection.total_bytes);
            if mgr.header().journal_block_count < recommended {
                warn!(
                    "raw blob device {} was formatted with a small journal (current_blocks={}, recommended_blocks={}); high-part-count uploads can exhaust the allocation journal until the device is reinitialized",
                    inspection.canonical_path.display(),
                    mgr.header().journal_block_count,
                    recommended
                );
            }
            mgr
        }
        Err(BlockManagerError::InvalidDevice(_)) => {
            if inspection.state != RawBlobDiskState::Unformatted {
                return Err(raw_blob_validation_error(&inspection));
            }
            BlockManager::format(
                &inspection.canonical_path,
                FormatOptions {
                    journal_blocks: recommended_raw_journal_blocks(inspection.total_bytes),
                    total_bytes: Some(inspection.total_bytes),
                    ..Default::default()
                },
            )
            .map_err(block_manager_error_to_app)?
        }
        Err(err) => return Err(block_manager_error_to_app(err)),
    };

    let manager = Arc::new(RwLock::new(manager));
    RAW_MANAGER_CACHE
        .lock()
        .map_err(|_| AppError::InternalServerError("Raw manager cache lock poisoned".into()))?
        .insert(inspection.canonical_path, manager.clone());
    let _ = register_raw_manager_device(&manager)?;
    Ok(manager)
}

fn recommended_raw_journal_blocks(total_bytes: u64) -> u64 {
    const BLOCK_SIZE: u64 = 4096;
    const MIN_JOURNAL_BYTES: u64 = 16 * 1024 * 1024;
    const MAX_JOURNAL_BYTES: u64 = 256 * 1024 * 1024;

    let target_bytes = (total_bytes / 32_768).clamp(MIN_JOURNAL_BYTES, MAX_JOURNAL_BYTES);
    let mut blocks = target_bytes.div_ceil(BLOCK_SIZE).max(128);

    let total_blocks = total_bytes / BLOCK_SIZE;
    let hard_cap = total_blocks.saturating_sub(8).max(1);
    if blocks > hard_cap {
        blocks = hard_cap;
    }
    blocks
}

fn inspect_raw_blob_device(path: &Path, strict: bool) -> Result<RawBlobDiskInspection, AppError> {
    let meta = fs::metadata(path)
        .map_err(|e| AppError::Io(e, format!("stat raw blob path {}", path.display())))?;
    if meta.is_dir() {
        return Err(AppError::BadRequest(format!(
            "blob_store_disks entry {} is a mounted directory; N1 blob disks must be raw block devices or loopback files",
            path.display()
        )));
    }

    let kind = raw_blob_path_kind(&meta, path)?;
    let canonical_path = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let mount_points = mount_points_for_path(path, &canonical_path);
    let mounted = !mount_points.is_empty();
    let path_warning = raw_path_warning(path, kind);
    let total_bytes = probe_raw_device_size_bytes(&canonical_path)?;

    if mounted && strict {
        return Err(AppError::BadRequest(format!(
            "blob device {} is mounted at {}; raw blob disks must stay unmounted",
            path.display(),
            mount_points.join(", ")
        )));
    }

    match BlockManager::open(&canonical_path) {
        Ok(_) => Ok(RawBlobDiskInspection {
            path: path.to_path_buf(),
            canonical_path,
            kind,
            mounted,
            mount_points,
            path_warning,
            total_bytes,
            state: RawBlobDiskState::Managed,
            detected_signature: Some("n1_block_manager".to_string()),
        }),
        Err(BlockManagerError::InvalidDevice(_)) => {
            let detected_signature = detect_foreign_signature(&canonical_path, total_bytes)?;
            let looks_empty = looks_zeroed_or_empty(&canonical_path, total_bytes)?;
            let state = if let Some(_) = detected_signature {
                RawBlobDiskState::ForeignSignature
            } else if looks_empty {
                RawBlobDiskState::Unformatted
            } else {
                RawBlobDiskState::DirtyUnknown
            };

            let inspection = RawBlobDiskInspection {
                path: path.to_path_buf(),
                canonical_path,
                kind,
                mounted,
                mount_points,
                path_warning,
                total_bytes,
                state,
                detected_signature,
            };
            if strict && inspection.state != RawBlobDiskState::Unformatted {
                return Err(raw_blob_validation_error(&inspection));
            }
            Ok(inspection)
        }
        Err(err) => Err(block_manager_error_to_app(err)),
    }
}

fn raw_blob_validation_error(inspection: &RawBlobDiskInspection) -> AppError {
    match inspection.state {
        RawBlobDiskState::Managed => AppError::InternalServerError(format!(
            "unexpected validation state for managed blob device {}",
            inspection.path.display()
        )),
        RawBlobDiskState::Unformatted => AppError::InternalServerError(format!(
            "unexpected validation state for unformatted blob device {}",
            inspection.path.display()
        )),
        RawBlobDiskState::ForeignSignature => AppError::BadRequest(format!(
            "blob device {} contains foreign on-disk data ({}) and is not safe for N1 to format",
            inspection.path.display(),
            inspection
                .detected_signature
                .as_deref()
                .unwrap_or("unknown signature")
        )),
        RawBlobDiskState::DirtyUnknown => AppError::BadRequest(format!(
            "blob device {} is neither zeroed nor N1-formatted; refusing to claim unknown data",
            inspection.path.display()
        )),
    }
}

fn status_from_inspection(inspection: RawBlobDiskInspection) -> RawBlobDeviceStatus {
    match BlockManager::open(&inspection.canonical_path) {
        Ok(mgr) => RawBlobDeviceStatus {
            path: inspection.path.to_string_lossy().to_string(),
            canonical_path: inspection.canonical_path.to_string_lossy().to_string(),
            kind: inspection.kind,
            status: if inspection.mounted {
                "misconfigured".to_string()
            } else {
                "managed".to_string()
            },
            direct_io: true,
            mounted: inspection.mounted,
            mount_points: inspection.mount_points,
            path_warning: inspection.path_warning,
            detected_signature: inspection.detected_signature,
            device_id: Some(mgr.header().device_id.to_string()),
            block_size_bytes: Some(mgr.header().block_size),
            allocation_count: Some(mgr.allocation_count()),
            journal_cursor: Some(mgr.journal_cursor()),
            total_bytes: Some(inspection.total_bytes),
            used_bytes: Some(mgr.used_bytes()),
            available_bytes: Some(mgr.available_bytes()),
            error: None,
        },
        Err(BlockManagerError::InvalidDevice(_)) => {
            let detected_signature = inspection.detected_signature.clone();
            RawBlobDeviceStatus {
                path: inspection.path.to_string_lossy().to_string(),
                canonical_path: inspection.canonical_path.to_string_lossy().to_string(),
                kind: inspection.kind,
                status: match inspection.state {
                    RawBlobDiskState::Managed => "managed".to_string(),
                    RawBlobDiskState::Unformatted => "unformatted".to_string(),
                    RawBlobDiskState::ForeignSignature | RawBlobDiskState::DirtyUnknown => {
                        "misconfigured".to_string()
                    }
                },
                direct_io: true,
                mounted: inspection.mounted,
                mount_points: inspection.mount_points,
                path_warning: inspection.path_warning,
                detected_signature,
                device_id: None,
                block_size_bytes: None,
                allocation_count: None,
                journal_cursor: None,
                total_bytes: Some(inspection.total_bytes),
                used_bytes: None,
                available_bytes: None,
                error: if inspection.mounted {
                    Some("blob device is mounted".to_string())
                } else {
                    match inspection.state {
                        RawBlobDiskState::Unformatted => None,
                        RawBlobDiskState::ForeignSignature => Some(format!(
                            "foreign signature detected: {}",
                            inspection
                                .detected_signature
                                .clone()
                                .unwrap_or_else(|| "unknown".to_string())
                        )),
                        RawBlobDiskState::DirtyUnknown => {
                            Some("device contains unknown non-zero data".to_string())
                        }
                        RawBlobDiskState::Managed => None,
                    }
                },
            }
        }
        Err(err) => RawBlobDeviceStatus {
            path: inspection.path.to_string_lossy().to_string(),
            canonical_path: inspection.canonical_path.to_string_lossy().to_string(),
            kind: inspection.kind,
            status: "error".to_string(),
            direct_io: true,
            mounted: inspection.mounted,
            mount_points: inspection.mount_points,
            path_warning: inspection.path_warning,
            detected_signature: inspection.detected_signature,
            device_id: None,
            block_size_bytes: None,
            allocation_count: None,
            journal_cursor: None,
            total_bytes: Some(inspection.total_bytes),
            used_bytes: None,
            available_bytes: None,
            error: Some(err.to_string()),
        },
    }
}

fn raw_blob_path_kind(meta: &fs::Metadata, path: &Path) -> Result<RawBlobPathKind, AppError> {
    #[cfg(unix)]
    if meta.file_type().is_block_device() {
        return Ok(RawBlobPathKind::BlockDevice);
    }
    if meta.is_file() {
        return Ok(RawBlobPathKind::LoopbackFile);
    }
    Err(AppError::BadRequest(format!(
        "blob_store_disks entry {} must be a raw block device or regular loopback file",
        path.display()
    )))
}

fn raw_path_warning(path: &Path, kind: RawBlobPathKind) -> Option<String> {
    if kind == RawBlobPathKind::BlockDevice {
        let rendered = path.to_string_lossy();
        if rendered.starts_with("/dev/sd")
            || rendered.starts_with("/dev/vd")
            || rendered.starts_with("/dev/nvme")
        {
            return Some(
                "prefer /dev/disk/by-id/* for stable blob device naming across reboots".to_string(),
            );
        }
    }
    None
}

fn mount_points_for_path(original: &Path, canonical: &Path) -> Vec<String> {
    let Ok(contents) = fs::read_to_string("/proc/self/mountinfo") else {
        return Vec::new();
    };
    let original_str = original.to_string_lossy().to_string();
    let canonical_str = canonical.to_string_lossy().to_string();
    parse_mountinfo_sources(contents.as_str())
        .into_iter()
        .filter_map(|(source, mount_point)| {
            let source_path = PathBuf::from(&source);
            let source_canonical = fs::canonicalize(&source_path).unwrap_or(source_path);
            let source_canonical_str = source_canonical.to_string_lossy().to_string();
            if source == original_str
                || source == canonical_str
                || source_canonical_str == original_str
                || source_canonical_str == canonical_str
            {
                Some(mount_point)
            } else {
                None
            }
        })
        .collect()
}

fn parse_mountinfo_sources(contents: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for line in contents.lines() {
        let Some((left, right)) = line.split_once(" - ") else {
            continue;
        };
        let mut left_parts = left.split_whitespace();
        let _mount_id = left_parts.next();
        let _parent_id = left_parts.next();
        let _major_minor = left_parts.next();
        let _root = left_parts.next();
        let mount_point = match left_parts.next() {
            Some(v) => v.to_string(),
            None => continue,
        };
        let mut right_parts = right.split_whitespace();
        let _fstype = right_parts.next();
        let source = match right_parts.next() {
            Some(v) => v.to_string(),
            None => continue,
        };
        out.push((source, mount_point));
    }
    out
}

fn detect_foreign_signature(path: &Path, total_bytes: u64) -> Result<Option<String>, AppError> {
    let mut file = File::open(path)
        .map_err(|e| AppError::Io(e, format!("open raw blob device {}", path.display())))?;
    let probe_len = (128 * 1024).min(total_bytes as usize);
    let mut prefix = vec![0u8; probe_len];
    file.read_exact(&mut prefix)
        .map_err(|e| AppError::Io(e, format!("read raw blob header {}", path.display())))?;

    if prefix.get(0..16) == Some(block_manager_superblock_magic().as_slice()) {
        return Ok(Some("n1_block_manager".to_string()));
    }
    if prefix.get(0..4) == Some(b"XFSB".as_slice()) {
        return Ok(Some("xfs".to_string()));
    }
    if prefix.get(0..6) == Some(b"LUKS\xba\xbe".as_slice()) {
        return Ok(Some("luks".to_string()));
    }
    if prefix.get(3..11) == Some(b"NTFS    ".as_slice()) {
        return Ok(Some("ntfs".to_string()));
    }
    if prefix.get(3..11) == Some(b"EXFAT   ".as_slice()) {
        return Ok(Some("exfat".to_string()));
    }
    if prefix.get(1080..1082) == Some([0x53, 0xEF].as_slice()) {
        return Ok(Some("ext4".to_string()));
    }
    if prefix.get(512..520) == Some(b"EFI PART".as_slice()) {
        return Ok(Some("gpt".to_string()));
    }
    if prefix.len() > 512
        && prefix[510] == 0x55
        && prefix[511] == 0xAA
        && prefix.iter().take(440).any(|b| *b != 0)
    {
        return Ok(Some("mbr".to_string()));
    }
    if prefix.len() > 0x10048 && prefix.get(0x10040..0x10048) == Some(b"_BHRfS_M".as_slice()) {
        return Ok(Some("btrfs".to_string()));
    }

    Ok(None)
}

fn block_manager_superblock_magic() -> [u8; 16] {
    *b"LITIAINA-BM-V1\0\0"
}

fn looks_zeroed_or_empty(path: &Path, total_bytes: u64) -> Result<bool, AppError> {
    if total_bytes == 0 {
        return Ok(true);
    }
    let mut file = File::open(path)
        .map_err(|e| AppError::Io(e, format!("open raw blob device {}", path.display())))?;
    let sample = (64 * 1024).min(total_bytes as usize);
    let mut buf = vec![0u8; sample];
    file.read_exact(&mut buf)
        .map_err(|e| AppError::Io(e, format!("read raw blob device {}", path.display())))?;
    if buf.iter().any(|b| *b != 0) {
        return Ok(false);
    }

    if total_bytes > sample as u64 {
        file.seek(SeekFrom::Start(total_bytes.saturating_sub(sample as u64)))
            .map_err(|e| AppError::Io(e, format!("seek raw blob device {}", path.display())))?;
        file.read_exact(&mut buf).map_err(|e| {
            AppError::Io(e, format!("tail read raw blob device {}", path.display()))
        })?;
        if buf.iter().any(|b| *b != 0) {
            return Ok(false);
        }
    }

    Ok(true)
}

fn probe_raw_device_size_bytes(path: &Path) -> Result<u64, AppError> {
    let meta = fs::metadata(path)
        .map_err(|e| AppError::Io(e, format!("stat raw blob device {}", path.display())))?;
    if meta.len() > 0 {
        return Ok(meta.len());
    }

    #[cfg(target_os = "linux")]
    {
        let file = File::open(path)
            .map_err(|e| AppError::Io(e, format!("open raw blob device {}", path.display())))?;
        let file_type = meta.file_type();
        if file_type.is_block_device() {
            let mut size: u64 = 0;
            let rc = unsafe { libc::ioctl(file.as_raw_fd(), BLKGETSIZE64_IOCTL, &mut size) };
            if rc == 0 && size > 0 {
                return Ok(size);
            }
            return Err(AppError::InternalServerError(format!(
                "failed to query block size for {}",
                path.display()
            )));
        }
    }

    Err(AppError::InternalServerError(format!(
        "raw blob device {} has no detectable size",
        path.display()
    )))
}

fn block_manager_error_to_app(err: BlockManagerError) -> AppError {
    AppError::InternalServerError(format!("block manager: {}", err))
}

#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;

#[cfg(target_os = "linux")]
const BLKGETSIZE64_IOCTL: libc::c_ulong = 0x8008_1272;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::n1_core::disk_manager::replace_storage_roots_for_tests;
    use std::fs::OpenOptions;
    use std::os::unix::fs::FileExt;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn clear_raw_manager_caches_for_tests() {
        clear_raw_runtime_caches_for_tests();
    }

    fn temp_loopback(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "n1-blob-store-{}-{}-{}",
            std::process::id(),
            unique,
            name
        ))
    }

    #[test]
    fn stable_shard_file_name_uses_expected_suffixes() {
        assert_eq!(stable_shard_file_name(7, 1, false), "part_7_shard_1.shard");
        assert_eq!(stable_shard_file_name(7, 2, true), "part_7_shard_2.parity");
    }

    #[test]
    fn raw_blob_debug_path_uses_scheme() {
        let addr = RawBlobAddress {
            topology_epoch: 1,
            device_id: "dev-1".into(),
            allocation_id: 42,
            offset_bytes: 4096,
            size_bytes: 8192,
        };
        assert_eq!(raw_blob_debug_path(&addr), "raw://dev-1/42");
    }

    #[test]
    fn probe_raw_device_size_uses_regular_file_length() {
        let path = temp_loopback("size");
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&path)
            .expect("create loopback file");
        file.set_len(8 * 1024 * 1024).expect("size loopback");
        drop(file);

        let size = probe_raw_device_size_bytes(&path).expect("probe size");
        assert_eq!(size, 8 * 1024 * 1024);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn parse_mountinfo_sources_extracts_source_and_mountpoint() {
        let parsed = parse_mountinfo_sources(
            "52 38 8:17 / /mnt/blob rw,relatime - ext4 /dev/sdb1 rw\n\
             53 38 8:18 / /mnt/index rw,relatime - xfs /dev/disk/by-id/idx rw",
        );
        assert_eq!(
            parsed,
            vec![
                ("/dev/sdb1".to_string(), "/mnt/blob".to_string()),
                ("/dev/disk/by-id/idx".to_string(), "/mnt/index".to_string()),
            ]
        );
    }

    #[test]
    fn detect_foreign_signature_recognizes_ext4_magic() {
        let path = temp_loopback("ext4-magic");
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&path)
            .expect("create loopback file");
        file.set_len(4 * 1024 * 1024).expect("size loopback");
        file.write_at(&[0x53, 0xEF], 1080)
            .expect("write ext4 magic");
        file.sync_all().expect("sync loopback");
        drop(file);

        let signature = detect_foreign_signature(&path, 4 * 1024 * 1024).expect("signature");
        assert_eq!(signature.as_deref(), Some("ext4"));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn zeroed_loopback_is_treated_as_unformatted() {
        let path = temp_loopback("zeroed");
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&path)
            .expect("create loopback file");
        file.set_len(4 * 1024 * 1024).expect("size loopback");
        drop(file);

        let inspection = inspect_raw_blob_device(&path, false).expect("inspect");
        assert_eq!(inspection.state, RawBlobDiskState::Unformatted);
        assert_eq!(inspection.kind, RawBlobPathKind::LoopbackFile);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn dirty_unknown_loopback_is_rejected() {
        let path = temp_loopback("dirty");
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&path)
            .expect("create loopback file");
        file.set_len(4 * 1024 * 1024).expect("size loopback");
        file.write_at(b"opaque-random-data", 0)
            .expect("write dirty bytes");
        file.sync_all().expect("sync loopback");
        drop(file);

        let err = inspect_raw_blob_device(&path, true).expect_err("dirty device rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("neither zeroed nor N1-formatted"),
            "unexpected error: {}",
            msg
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn open_or_format_raw_manager_initializes_unformatted_loopback() {
        let path = temp_loopback("format");
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&path)
            .expect("create loopback file");
        file.set_len(4 * 1024 * 1024).expect("size loopback");
        drop(file);

        let manager = open_or_format_raw_manager(&path).expect("format manager");
        let header = manager.read().expect("lock manager").header().clone();
        assert_eq!(header.block_size, 4096);
        assert!(header.total_blocks > header.data_start_block);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn open_or_format_raw_manager_reinitializes_after_path_recreate() {
        clear_raw_manager_caches_for_tests();
        let path = temp_loopback("recreate");
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&path)
            .expect("create loopback file");
        file.set_len(8 * 1024 * 1024).expect("size loopback");
        drop(file);

        let manager_1 = open_or_format_raw_manager(&path).expect("first format manager");
        let device_1 = manager_1
            .read()
            .expect("lock manager")
            .header()
            .device_id
            .to_string();

        std::fs::remove_file(&path).expect("remove initial loopback");
        let file_recreated = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&path)
            .expect("recreate loopback file");
        file_recreated
            .set_len(8 * 1024 * 1024)
            .expect("size recreated loopback");
        drop(file_recreated);

        let manager_2 = open_or_format_raw_manager(&path).expect("reinitialize manager");
        let device_2 = manager_2
            .read()
            .expect("lock manager")
            .header()
            .device_id
            .to_string();
        assert_ne!(
            device_1, device_2,
            "manager cache should be invalidated when the underlying path is recreated"
        );

        let _ = std::fs::remove_file(path);
        clear_raw_manager_caches_for_tests();
    }

    #[test]
    fn managed_loopback_reports_observability_fields() {
        let path = temp_loopback("status");
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&path)
            .expect("create loopback file");
        file.set_len(4 * 1024 * 1024).expect("size loopback");
        drop(file);

        let _ = open_or_format_raw_manager(&path).expect("format manager");
        let status =
            status_from_inspection(inspect_raw_blob_device(&path, false).expect("inspect"));
        assert_eq!(status.status, "managed");
        assert_eq!(status.kind, RawBlobPathKind::LoopbackFile);
        assert_eq!(status.direct_io, true);
        assert!(status.device_id.is_some());
        assert!(status.total_bytes.unwrap_or(0) > 0);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn recommended_raw_journal_blocks_scales_for_large_devices() {
        assert_eq!(recommended_raw_journal_blocks(4 * 1024 * 1024), 1016);
        assert_eq!(
            recommended_raw_journal_blocks(1 * 1024 * 1024 * 1024 * 1024),
            8192
        );
        assert_eq!(
            recommended_raw_journal_blocks(8 * 1024 * 1024 * 1024 * 1024),
            65536
        );
    }

    #[test]
    fn format_raw_blob_device_initializes_unformatted_loopback() {
        let path = temp_loopback("format-cli");
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&path)
            .expect("create loopback file");
        file.set_len(64 * 1024 * 1024).expect("size loopback");
        drop(file);

        let status = format_raw_blob_device(&path, false).expect("format loopback");
        assert_eq!(status.status, "managed");
        assert!(status.device_id.is_some());
        assert_eq!(
            status.block_size_bytes,
            Some(4096),
            "raw formatter should keep 4 KiB block size"
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn format_raw_blob_device_requires_force_for_dirty_unknown() {
        let path = temp_loopback("format-force");
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&path)
            .expect("create loopback file");
        file.set_len(4 * 1024 * 1024).expect("size loopback");
        file.write_at(&[1, 2, 3, 4], 0).expect("dirty bytes");
        file.sync_all().expect("sync dirty loopback");
        drop(file);

        let err = format_raw_blob_device(&path, false).expect_err("force required");
        assert!(
            err.to_string().contains("rerun with --force"),
            "unexpected error: {}",
            err
        );

        let status = format_raw_blob_device(&path, true).expect("forced format");
        assert_eq!(status.status, "managed");

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn raw_blob_device_statuses_reports_missing_path_without_failing() {
        let index_root = temp_loopback("index-root");
        std::fs::create_dir_all(&index_root).expect("create index root");
        let fs_root = temp_loopback("fs-root");
        std::fs::create_dir_all(&fs_root).expect("create fs root");

        let healthy = temp_loopback("healthy-status");
        let healthy_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&healthy)
            .expect("create healthy loopback");
        healthy_file.set_len(4 * 1024 * 1024).expect("size healthy");
        drop(healthy_file);

        let missing = temp_loopback("missing-status");
        replace_storage_roots_for_tests(
            vec![index_root.to_string_lossy().to_string()],
            vec![
                healthy.to_string_lossy().to_string(),
                missing.to_string_lossy().to_string(),
            ],
            fs_root.to_string_lossy().to_string(),
        );

        let statuses = raw_blob_device_statuses().expect("status probe should not hard fail");
        assert_eq!(statuses.len(), 2);
        assert!(
            statuses
                .iter()
                .any(|s| s.path == missing.to_string_lossy() && s.status == "unavailable"),
            "missing disk path should be reported as unavailable"
        );
        validate_configured_raw_blob_disks().expect("one healthy disk should keep validation ok");

        let _ = std::fs::remove_file(healthy);
        let _ = std::fs::remove_dir_all(index_root);
        let _ = std::fs::remove_dir_all(fs_root);
    }

    #[test]
    fn resolve_blob_disk_falls_back_to_next_usable_path() {
        clear_raw_manager_caches_for_tests();
        let index_root = temp_loopback("index-fallback");
        std::fs::create_dir_all(&index_root).expect("create index root");
        let fs_root = temp_loopback("fs-fallback");
        std::fs::create_dir_all(&fs_root).expect("create fs root");

        let missing = temp_loopback("missing-fallback");
        let healthy = temp_loopback("healthy-fallback");
        let healthy_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&healthy)
            .expect("create healthy loopback");
        healthy_file.set_len(8 * 1024 * 1024).expect("size healthy");
        drop(healthy_file);

        replace_storage_roots_for_tests(
            vec![index_root.to_string_lossy().to_string()],
            vec![
                missing.to_string_lossy().to_string(),
                healthy.to_string_lossy().to_string(),
            ],
            fs_root.to_string_lossy().to_string(),
        );

        let handle = resolve_blob_disk(0).expect("resolver should fall back to healthy disk");
        let BlobDiskHandle::Raw { device_id, .. } = handle;
        assert!(
            !device_id.is_empty(),
            "resolved fallback disk should expose a device id"
        );

        let _ = std::fs::remove_file(healthy);
        let _ = std::fs::remove_dir_all(index_root);
        let _ = std::fs::remove_dir_all(fs_root);
        clear_raw_manager_caches_for_tests();
    }

    #[test]
    fn raw_manager_lookup_by_device_id_uses_cached_index() {
        clear_raw_manager_caches_for_tests();
        let index_root = temp_loopback("index-device-cache");
        std::fs::create_dir_all(&index_root).expect("create index root");
        let fs_root = temp_loopback("fs-device-cache");
        std::fs::create_dir_all(&fs_root).expect("create fs root");

        let healthy = temp_loopback("healthy-device-cache");
        let healthy_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&healthy)
            .expect("create healthy loopback");
        healthy_file.set_len(8 * 1024 * 1024).expect("size healthy");
        drop(healthy_file);

        replace_storage_roots_for_tests(
            vec![index_root.to_string_lossy().to_string()],
            vec![healthy.to_string_lossy().to_string()],
            fs_root.to_string_lossy().to_string(),
        );

        let BlobDiskHandle::Raw { device_id, manager } =
            resolve_blob_disk(0).expect("resolve healthy raw disk");
        let by_id = raw_manager_by_device_id(&device_id).expect("lookup manager by device id");
        assert!(
            Arc::ptr_eq(&manager, &by_id),
            "device-id lookup should return the same cached manager"
        );

        let _ = std::fs::remove_file(healthy);
        let _ = std::fs::remove_dir_all(index_root);
        let _ = std::fs::remove_dir_all(fs_root);
        clear_raw_manager_caches_for_tests();
    }
}
