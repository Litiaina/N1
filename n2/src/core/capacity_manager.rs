use crate::core::constants::N2_BLOB_DIR;
use crate::core::disk_manager::ObjectPathManager;
use crate::core::fragment_identity::fragment_root_on;
use crate::enums::app_error_enum::AppError;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StorageUsage {
    pub capacity_allocated: u64,
    pub capacity_used: u64,
    pub capacity_available: u64,
    pub filesystem_count: usize,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct FragmentBlobUsage {
    pub live_bytes: u64,
    pub trash_bytes: u64,
}

impl FragmentBlobUsage {
    pub fn used_bytes(self) -> u64 {
        self.live_bytes.saturating_add(self.trash_bytes)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageRootUsage {
    pub path: String,
    pub role: String,
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
    pub usage_percent: f64,
}

#[cfg(unix)]
fn statvfs_totals_sync(path: &Path) -> Result<(u64, u64, u64), AppError> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::MetadataExt;

    let meta = std::fs::metadata(path)
        .map_err(|err| AppError::Io(err, format!("capacity metadata {}", path.display())))?;
    let device_id = meta.dev();
    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| AppError::BadRequest("path contains NUL".into()))?;

    let mut vfs: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c_path.as_ptr(), &mut vfs) } != 0 {
        return Err(AppError::Io(
            std::io::Error::last_os_error(),
            format!("statvfs {}", path.display()),
        ));
    }

    let total = (vfs.f_frsize as u64).saturating_mul(vfs.f_blocks as u64);
    let avail = (vfs.f_frsize as u64).saturating_mul(vfs.f_bavail as u64);
    Ok((device_id, total, avail))
}

#[cfg(not(unix))]
fn statvfs_totals_sync(_path: &Path) -> Result<(u64, u64, u64), AppError> {
    Ok((0, 0, 0))
}

#[cfg(unix)]
fn file_storage_bytes(metadata: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;

    metadata.blocks().saturating_mul(512)
}

#[cfg(not(unix))]
fn file_storage_bytes(metadata: &std::fs::Metadata) -> u64 {
    metadata.len()
}

fn unique_capacity_probe_paths() -> Vec<PathBuf> {
    let pm = ObjectPathManager::new();
    let mut out = Vec::new();
    let mut seen = HashSet::new();

    for path in pm
        .get_blob_roots()
        .into_iter()
        .chain(pm.get_all_index_roots())
    {
        if seen.insert(path.clone()) {
            out.push(path);
        }
    }

    out
}

fn sum_file_tree_bytes(path: &Path) -> Result<u64, AppError> {
    let mut total = 0u64;
    let mut stack = vec![path.to_path_buf()];
    while let Some(current) = stack.pop() {
        let entries = match std::fs::read_dir(&current) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(AppError::Io(err, format!("read_dir {}", current.display()))),
        };
        for entry in entries {
            let entry = entry
                .map_err(|err| AppError::Io(err, format!("read_dir {}", current.display())))?;
            let metadata = entry
                .metadata()
                .map_err(|err| AppError::Io(err, format!("metadata {}", entry.path().display())))?;
            if metadata.is_dir() {
                stack.push(entry.path());
            } else if metadata.is_file() {
                total = total.saturating_add(file_storage_bytes(&metadata));
            }
        }
    }
    Ok(total)
}

async fn compute_fragment_blob_usage(fragment: &str) -> Result<FragmentBlobUsage, AppError> {
    let fragment = fragment.to_string();
    let roots = ObjectPathManager::new().get_blob_roots();
    tokio::task::spawn_blocking(move || {
        let mut usage = FragmentBlobUsage::default();
        let mut seen_roots = HashSet::<PathBuf>::new();
        for root in roots {
            if !seen_roots.insert(root.clone()) {
                continue;
            }
            usage.live_bytes = usage.live_bytes.saturating_add(sum_file_tree_bytes(
                &fragment_root_on(&root, &fragment).join(N2_BLOB_DIR),
            )?);
            usage.trash_bytes = usage.trash_bytes.saturating_add(sum_file_tree_bytes(
                &fragment_root_on(&root, &fragment)
                    .join(".trash")
                    .join(N2_BLOB_DIR),
            )?);
        }
        Ok::<FragmentBlobUsage, AppError>(usage)
    })
    .await
    .map_err(|err| AppError::InternalServerError(format!("fragment usage task: {err}")))?
}

fn final_shard_len_for_part(part_size_bytes: u64, data_shards: usize, block_size: usize) -> u64 {
    let stripe_data_width = block_size as u64 * data_shards.max(1) as u64;
    let stripe_count = if stripe_data_width == 0 {
        0
    } else {
        part_size_bytes.div_ceil(stripe_data_width)
    };
    stripe_count * block_size as u64
}

fn placement_required_bytes_by_root(
    root_count: usize,
    start_part_index: u64,
    part_sizes: &[u64],
    data_shards: usize,
    parity_shards: usize,
    block_size: usize,
) -> Vec<u64> {
    let mut required = vec![0u64; root_count];
    if root_count == 0 {
        return required;
    }

    let total_shards = data_shards.saturating_add(parity_shards);
    for (offset, part_size_bytes) in part_sizes.iter().copied().enumerate() {
        let part_index = start_part_index.saturating_add(offset as u64);
        let shard_len = final_shard_len_for_part(part_size_bytes, data_shards, block_size);
        for shard_index in 0..total_shards {
            let root_idx = (part_index as usize + shard_index) % root_count;
            required[root_idx] = required[root_idx].saturating_add(shard_len);
        }
    }

    required
}

fn human_bytes_for_error(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut idx = 0usize;
    while value >= 1024.0 && idx < UNITS.len() - 1 {
        value /= 1024.0;
        idx += 1;
    }
    if idx == 0 {
        format!("{bytes} {}", UNITS[idx])
    } else {
        format!("{value:.2} {}", UNITS[idx])
    }
}

async fn compute_box_usage() -> Result<StorageUsage, AppError> {
    let paths = unique_capacity_probe_paths();
    tokio::task::spawn_blocking(move || {
        let mut seen_devices = HashSet::<u64>::new();
        let mut total_bytes = 0u64;
        let mut available_bytes = 0u64;

        for path in paths {
            if !path.exists() {
                continue;
            }
            let (device_id, total, avail) = statvfs_totals_sync(&path)?;
            if seen_devices.insert(device_id) {
                total_bytes = total_bytes.saturating_add(total);
                available_bytes = available_bytes.saturating_add(avail);
            }
        }

        Ok::<StorageUsage, AppError>(StorageUsage {
            capacity_allocated: total_bytes,
            capacity_used: total_bytes.saturating_sub(available_bytes),
            capacity_available: available_bytes,
            filesystem_count: seen_devices.len(),
        })
    })
    .await
    .map_err(|err| AppError::InternalServerError(format!("capacity stat task: {err}")))?
}

pub async fn get_storage_usage() -> Result<StorageUsage, AppError> {
    compute_box_usage().await
}

pub async fn get_fragment_blob_usage(fragment: &str) -> Result<FragmentBlobUsage, AppError> {
    compute_fragment_blob_usage(fragment).await
}

pub async fn get_storage_root_usage() -> Result<Vec<StorageRootUsage>, AppError> {
    let pm = ObjectPathManager::new();
    let mut roots = Vec::<(PathBuf, String)>::new();
    roots.extend(
        pm.get_all_index_roots()
            .into_iter()
            .map(|path| (path, "index".to_string())),
    );
    roots.extend(
        pm.get_blob_roots()
            .into_iter()
            .map(|path| (path, "blob".to_string())),
    );

    tokio::task::spawn_blocking(move || {
        let mut out = Vec::new();
        for (path, role) in roots {
            if !path.exists() {
                continue;
            }
            let (_device_id, total_bytes, available_bytes) = statvfs_totals_sync(&path)?;
            let used_bytes = total_bytes.saturating_sub(available_bytes);
            let usage_percent = if total_bytes > 0 {
                (used_bytes as f64 / total_bytes as f64) * 100.0
            } else {
                0.0
            };
            out.push(StorageRootUsage {
                path: path.display().to_string(),
                role,
                total_bytes,
                used_bytes,
                available_bytes,
                usage_percent,
            });
        }
        Ok::<Vec<StorageRootUsage>, AppError>(out)
    })
    .await
    .map_err(|err| AppError::InternalServerError(format!("storage root usage task: {err}")))?
}

pub async fn ensure_capacity_available(required_bytes: u64) -> Result<(), AppError> {
    let usage = compute_box_usage().await?;
    if required_bytes > usage.capacity_available {
        return Err(AppError::InsufficientStorage);
    }
    Ok(())
}

pub async fn ensure_blob_placement_capacity_available_for_parts(
    start_part_index: u64,
    part_sizes: &[u64],
    data_shards: usize,
    parity_shards: usize,
    block_size: usize,
) -> Result<(), AppError> {
    let roots = ObjectPathManager::new().get_blob_roots();
    if roots.is_empty() {
        return Err(AppError::InternalServerError(
            "No blob storage disks configured".into(),
        ));
    }
    if data_shards == 0 {
        return Err(AppError::InternalServerError(
            "data_shards must be greater than zero".into(),
        ));
    }
    let total_shards = data_shards.saturating_add(parity_shards);
    if roots.len() < total_shards {
        return Err(AppError::ServiceUnavailable(format!(
            "Configured blob roots ({}) cannot satisfy target layout {}D+{}P requiring {} distinct roots",
            roots.len(),
            data_shards,
            parity_shards,
            total_shards
        )));
    }

    let required_by_root = placement_required_bytes_by_root(
        roots.len(),
        start_part_index,
        part_sizes,
        data_shards,
        parity_shards,
        block_size,
    );
    let required_roots: Vec<(PathBuf, u64)> = roots
        .into_iter()
        .zip(required_by_root)
        .filter(|(_, required)| *required > 0)
        .collect();

    tokio::task::spawn_blocking(move || {
        let mut seen_devices = HashSet::<u64>::new();
        let mut device_availability = HashMap::<u64, (PathBuf, u64, u64)>::new();
        let mut device_required = HashMap::<u64, u64>::new();

        for (path, required_bytes) in required_roots {
            let meta = std::fs::metadata(&path).map_err(|err| {
                AppError::ServiceUnavailable(format!(
                    "Target blob disk is unavailable: {} ({})",
                    path.display(),
                    err
                ))
            })?;
            if !meta.is_dir() {
                return Err(AppError::ServiceUnavailable(format!(
                    "Target blob disk root is not a directory: {}",
                    path.display()
                )));
            }

            let (device_id, total_bytes, available_bytes) = statvfs_totals_sync(&path)?;
            if seen_devices.insert(device_id) {
                device_availability.insert(device_id, (path.clone(), total_bytes, available_bytes));
            }
            *device_required.entry(device_id).or_insert(0) = device_required
                .get(&device_id)
                .copied()
                .unwrap_or(0)
                .saturating_add(required_bytes);
        }

        for (device_id, required_bytes) in device_required {
            let Some((path, _total_bytes, available_bytes)) = device_availability.get(&device_id)
            else {
                return Err(AppError::InternalServerError(
                    "blob device availability map missing device".into(),
                ));
            };
            if required_bytes > *available_bytes {
                return Err(AppError::ServiceUnavailable(format!(
                    "Placement-limiting blob disk {} has {} free but needs {} for this upload",
                    path.display(),
                    human_bytes_for_error(*available_bytes),
                    human_bytes_for_error(required_bytes)
                )));
            }
        }

        Ok::<(), AppError>(())
    })
    .await
    .map_err(|err| AppError::InternalServerError(format!("placement capacity task: {err}")))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placement_required_bytes_spreads_shards_by_part_and_root() {
        let required = placement_required_bytes_by_root(4, 0, &[1024, 1024], 2, 1, 512);
        assert_eq!(required, vec![512, 1024, 1024, 512]);
    }
}
