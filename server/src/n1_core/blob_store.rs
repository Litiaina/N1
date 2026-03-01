use crate::enums::app_error_enum::AppError;
use crate::n1_core::constants::{PARITY_SUFFIX, SHARD_SUFFIX};
use crate::n1_core::disk_manager::{
    ObjectMetadata, RawBlobAddress, ShardInfo, configured_blob_store_disks,
    configured_fs_storage_root,
};
use block_manager::{BlockManager, BlockManagerError, FormatOptions, RawExtentAddress};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};
use tokio::fs as tokio_fs;

#[cfg(unix)]
use std::os::unix::fs::FileTypeExt;

#[derive(Clone)]
pub enum BlobDiskHandle {
    Raw {
        device_id: String,
        manager: Arc<Mutex<BlockManager>>,
    },
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
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

static RAW_MANAGER_CACHE: LazyLock<Mutex<HashMap<PathBuf, Arc<Mutex<BlockManager>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

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
    let disks = configured_blob_store_disks()?;
    let mut out = Vec::with_capacity(disks.len());
    for disk in &disks {
        out.push(inspect_raw_blob_device(Path::new(disk), false)?);
    }
    Ok(out.into_iter().map(status_from_inspection).collect())
}

pub fn raw_blob_device_statuses() -> Result<Vec<RawBlobDeviceStatus>, AppError> {
    validate_configured_raw_blob_disks()
}

pub fn resolve_blob_disk(disk_idx: usize) -> Result<BlobDiskHandle, AppError> {
    let disks = configured_blob_store_disks()?;
    let raw_path = disks.get(disk_idx).ok_or_else(|| {
        AppError::InternalServerError(format!("Blob disk index {} out of bounds", disk_idx))
    })?;
    let path = PathBuf::from(raw_path);
    let inspection = inspect_raw_blob_device(&path, true)?;
    let manager = open_or_format_raw_manager(&inspection.canonical_path)?;
    let device_id = manager
        .lock()
        .map_err(|_| AppError::InternalServerError("Raw blob manager lock poisoned".into()))?
        .header()
        .device_id
        .to_string();
    Ok(BlobDiskHandle::Raw { device_id, manager })
}

pub fn resolve_blob_disk_path(path: &Path) -> Result<BlobDiskHandle, AppError> {
    let disks = configured_blob_store_disks()?;
    let wanted = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    for raw in &disks {
        let candidate = PathBuf::from(raw);
        let canonical = fs::canonicalize(&candidate).unwrap_or(candidate.clone());
        if canonical == wanted {
            let inspection = inspect_raw_blob_device(&candidate, true)?;
            let manager = open_or_format_raw_manager(&inspection.canonical_path)?;
            let device_id = manager
                .lock()
                .map_err(|_| {
                    AppError::InternalServerError("Raw blob manager lock poisoned".into())
                })?
                .header()
                .device_id
                .to_string();
            return Ok(BlobDiskHandle::Raw { device_id, manager });
        }
    }
    Err(AppError::InternalServerError(format!(
        "configured blob disk not found for {}",
        path.display()
    )))
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
        let mut mgr = manager
            .lock()
            .map_err(|_| AppError::InternalServerError("Raw blob manager lock poisoned".into()))?;

        let alloc = mgr
            .allocate_bytes(size_bytes)
            .map_err(block_manager_error_to_app)?;
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

pub async fn read_shard_range(
    shard: &ShardInfo,
    offset: u64,
    len: usize,
) -> Result<Vec<u8>, AppError> {
    if let Some(raw) = shard.raw_blob_address.as_ref() {
        let raw = raw.clone();
        return tokio::task::spawn_blocking(move || {
            let manager = raw_manager_by_device_id(raw.device_id.as_str())?;
            let mgr = manager.lock().map_err(|_| {
                AppError::InternalServerError("Raw blob manager lock poisoned".into())
            })?;
            let address = RawExtentAddress {
                device_id: mgr.header().device_id,
                allocation_id: raw.allocation_id,
                offset_bytes: raw.offset_bytes,
                size_bytes: raw.size_bytes,
            };
            mgr.read_address(&address, offset, len)
                .map_err(block_manager_error_to_app)
        })
        .await
        .map_err(|e| AppError::InternalServerError(format!("raw read task failed: {}", e)))?;
    }

    let path = PathBuf::from(shard.absolute_path.as_str());
    let mut file = tokio_fs::File::open(&path)
        .await
        .map_err(|e| AppError::Io(e, format!("open shard {}", path.display())))?;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    file.seek(SeekFrom::Start(offset))
        .await
        .map_err(|e| AppError::Io(e, format!("seek shard {}", path.display())))?;
    let mut out = vec![0u8; len];
    file.read_exact(&mut out)
        .await
        .map_err(|e| AppError::Io(e, format!("read shard {}", path.display())))?;
    Ok(out)
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
            let mgr = manager.lock().map_err(|_| {
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
                let mgr = manager.lock().map_err(|_| {
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
    for raw in seen {
        free_raw_address(&raw).await?;
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
    let raw = raw.clone();
    tokio::task::spawn_blocking(move || {
        let manager = raw_manager_by_device_id(raw.device_id.as_str())?;
        let mut mgr = manager
            .lock()
            .map_err(|_| AppError::InternalServerError("Raw blob manager lock poisoned".into()))?;
        let address = RawExtentAddress {
            device_id: mgr.header().device_id,
            allocation_id: raw.allocation_id,
            offset_bytes: raw.offset_bytes,
            size_bytes: raw.size_bytes,
        };
        mgr.free_address(&address)
            .map_err(block_manager_error_to_app)?;
        mgr.flush().map_err(block_manager_error_to_app)
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("raw free task failed: {}", e)))?
}

pub async fn raw_blob_address_exists(raw: &RawBlobAddress) -> Result<bool, AppError> {
    let manager = raw_manager_by_device_id(raw.device_id.as_str())?;
    let address = RawExtentAddress {
        device_id: manager
            .lock()
            .map_err(|_| AppError::InternalServerError("raw manager lock poisoned".into()))?
            .header()
            .device_id,
        allocation_id: raw.allocation_id,
        offset_bytes: raw.offset_bytes,
        size_bytes: raw.size_bytes,
    };

    tokio::task::spawn_blocking(move || {
        let mgr = manager
            .lock()
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
        "{}-{}-{}-{}.msgpack",
        sanitized_device, raw.allocation_id, raw.offset_bytes, raw.size_bytes
    )
}

fn raw_manager_by_device_id(device_id: &str) -> Result<Arc<Mutex<BlockManager>>, AppError> {
    let disk_count = configured_blob_disk_count()?;
    for idx in 0..disk_count {
        let BlobDiskHandle::Raw {
            device_id: raw_device_id,
            manager,
            ..
        } = resolve_blob_disk(idx)?;
        if raw_device_id == device_id {
            return Ok(manager);
        }
    }
    Err(AppError::InternalServerError(format!(
        "No raw blob device found for device id {}",
        device_id
    )))
}

fn open_or_format_raw_manager(path: &Path) -> Result<Arc<Mutex<BlockManager>>, AppError> {
    let canonical = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let inspection = inspect_raw_blob_device(&canonical, true)?;
    if let Some(existing) = RAW_MANAGER_CACHE
        .lock()
        .map_err(|_| AppError::InternalServerError("Raw manager cache lock poisoned".into()))?
        .get(&inspection.canonical_path)
        .cloned()
    {
        return Ok(existing);
    }

    let manager = match BlockManager::open(&inspection.canonical_path) {
        Ok(mgr) => mgr,
        Err(BlockManagerError::InvalidDevice(_)) => {
            if inspection.state != RawBlobDiskState::Unformatted {
                return Err(raw_blob_validation_error(&inspection));
            }
            BlockManager::format(
                &inspection.canonical_path,
                FormatOptions {
                    total_bytes: Some(inspection.total_bytes),
                    ..Default::default()
                },
            )
            .map_err(block_manager_error_to_app)?
        }
        Err(err) => return Err(block_manager_error_to_app(err)),
    };

    let manager = Arc::new(Mutex::new(manager));
    RAW_MANAGER_CACHE
        .lock()
        .map_err(|_| AppError::InternalServerError("Raw manager cache lock poisoned".into()))?
        .insert(inspection.canonical_path, manager.clone());
    Ok(manager)
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
    use std::fs::OpenOptions;
    use std::os::unix::fs::FileExt;
    use std::time::{SystemTime, UNIX_EPOCH};

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
        let header = manager.lock().expect("lock manager").header().clone();
        assert_eq!(header.block_size, 4096);
        assert!(header.total_blocks > header.data_start_block);

        let _ = std::fs::remove_file(path);
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
}
