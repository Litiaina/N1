use crate::enums::app_error_enum::AppError;
use crate::n1_core::constants::{PARITY_SUFFIX, SHARD_SUFFIX};
use crate::n1_core::disk_manager::{
    ObjectMetadata, RawBlobAddress, ShardInfo, configured_blob_store_disks, configured_fs_storage_root,
};
#[cfg(test)]
use crate::n1_core::disk_manager::ObjectPathManager;
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
    Filesystem {
        root: PathBuf,
    },
    Raw {
        device_id: String,
        manager: Arc<Mutex<BlockManager>>,
    },
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

#[cfg(test)]
pub fn filesystem_shard_address(
    path_manager: &ObjectPathManager,
    disk_root: &PathBuf,
    fragment_hash: &str,
    version_id: &str,
    part_index: u64,
    shard_index: usize,
    is_parity: bool,
) -> PathBuf {
    let shard_dir = path_manager.get_shard_dir_on_disk(disk_root, fragment_hash, version_id);
    shard_dir.join(stable_shard_file_name(part_index, shard_index, is_parity))
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

pub fn configured_filesystem_blob_roots() -> Result<Vec<PathBuf>, AppError> {
    let disks = configured_blob_store_disks()?;
    let mut out = Vec::new();
    for disk in &disks {
        let path = PathBuf::from(disk);
        if fs::metadata(&path).map(|m| m.is_dir()).unwrap_or(false) {
            out.push(path);
        }
    }
    Ok(out)
}

pub fn resolve_blob_disk(disk_idx: usize) -> Result<BlobDiskHandle, AppError> {
    let disks = configured_blob_store_disks()?;
    let raw_path = disks.get(disk_idx).ok_or_else(|| {
        AppError::InternalServerError(format!("Blob disk index {} out of bounds", disk_idx))
    })?;
    let path = PathBuf::from(raw_path);
    let meta = fs::metadata(&path)
        .map_err(|e| AppError::Io(e, format!("stat blob store path {}", path.display())))?;

    if meta.is_dir() {
        return Ok(BlobDiskHandle::Filesystem { root: path });
    }

    let manager = open_or_format_raw_manager(&path)?;
    let device_id = manager
        .lock()
        .map_err(|_| AppError::InternalServerError("Raw blob manager lock poisoned".into()))?
        .header()
        .device_id
        .to_string();
    Ok(BlobDiskHandle::Raw { device_id, manager })
}

pub fn raw_backend_present() -> Result<bool, AppError> {
    Ok((0..configured_blob_disk_count()?)
        .any(|idx| matches!(resolve_blob_disk(idx), Ok(BlobDiskHandle::Raw { .. }))))
}

pub fn resolve_blob_disk_path(path: &Path) -> Result<BlobDiskHandle, AppError> {
    let disks = configured_blob_store_disks()?;
    let wanted = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    for raw in &disks {
        let candidate = PathBuf::from(raw);
        let canonical = fs::canonicalize(&candidate).unwrap_or(candidate.clone());
        if canonical == wanted {
            let meta = fs::metadata(&candidate).map_err(|e| {
                AppError::Io(e, format!("stat blob store path {}", candidate.display()))
            })?;
            if meta.is_dir() {
                return Ok(BlobDiskHandle::Filesystem { root: candidate });
            }
            let manager = open_or_format_raw_manager(&candidate)?;
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
        let BlobDiskHandle::Raw { manager, .. } = resolve_blob_disk(disk_idx)? else {
            return Err(AppError::InternalServerError(format!(
                "blob disk {} is not a raw device",
                disk_idx
            )));
        };

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
        if let BlobDiskHandle::Raw {
            device_id: raw_device_id,
            manager,
            ..
        } = resolve_blob_disk(idx)?
        {
            if raw_device_id == device_id {
                return Ok(manager);
            }
        }
    }
    Err(AppError::InternalServerError(format!(
        "No raw blob device found for device id {}",
        device_id
    )))
}

fn open_or_format_raw_manager(path: &Path) -> Result<Arc<Mutex<BlockManager>>, AppError> {
    let canonical = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    if let Some(existing) = RAW_MANAGER_CACHE
        .lock()
        .map_err(|_| AppError::InternalServerError("Raw manager cache lock poisoned".into()))?
        .get(&canonical)
        .cloned()
    {
        return Ok(existing);
    }

    let manager = match BlockManager::open(&canonical) {
        Ok(mgr) => mgr,
        Err(BlockManagerError::InvalidDevice(_)) => {
            let total_bytes = probe_raw_device_size_bytes(&canonical)?;
            BlockManager::format(
                &canonical,
                FormatOptions {
                    total_bytes: Some(total_bytes),
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
        .insert(canonical, manager.clone());
    Ok(manager)
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
    use crate::n1_core::disk_manager::ObjectPathManager;
    use std::fs::OpenOptions;
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
    fn filesystem_shard_address_builds_expected_path() {
        let pm = ObjectPathManager::new();
        let disk = PathBuf::from("/disk0");
        let addr = filesystem_shard_address(&pm, &disk, "frag", "ver", 3, 1, false);
        assert_eq!(
            addr,
            PathBuf::from("/disk0/frag/blob/ver/part_3_shard_1.shard")
        );
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
}
