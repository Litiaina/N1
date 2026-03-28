use crate::core::disk_manager::ObjectPathManager;
use crate::enums::app_error_enum::AppError;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy)]
pub struct StorageUsage {
    pub capacity_allocated: u64,
    pub capacity_used: u64,
    pub capacity_available: u64,
    pub filesystem_count: usize,
}

#[cfg(unix)]
fn statvfs_totals_sync(path: &Path) -> Result<(u64, u64, u64), AppError> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::MetadataExt;

    let meta = std::fs::metadata(path)
        .map_err(|e| AppError::Io(e, format!("quota metadata {}", path.display())))?;
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

fn unique_capacity_probe_paths() -> Vec<PathBuf> {
    let pm = ObjectPathManager::new();
    let mut out = Vec::<PathBuf>::new();
    let mut seen = HashSet::<PathBuf>::new();

    for path in pm
        .get_blob_roots()
        .into_iter()
        .chain(pm.get_all_index_roots().into_iter())
    {
        if seen.insert(path.clone()) {
            out.push(path);
        }
    }

    out
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
    .map_err(|e| AppError::InternalServerError(format!("capacity stat task: {}", e)))?
}

// Test helper for isolated workflow setup.
#[cfg(test)]
pub async fn init_quota_file(fragment: &str, _allocated: u64) -> Result<(), AppError> {
    let _ = fragment;
    Ok(())
}

pub async fn get_storage_usage() -> Result<StorageUsage, AppError> {
    compute_box_usage().await
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
        .zip(required_by_root.into_iter())
        .filter(|(_, required)| *required > 0)
        .collect();

    tokio::task::spawn_blocking(move || {
        let mut seen_devices = HashSet::<u64>::new();
        let mut device_availability = std::collections::HashMap::<u64, (PathBuf, u64, u64)>::new();
        let mut device_required = std::collections::HashMap::<u64, u64>::new();

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
    .map_err(|e| AppError::InternalServerError(format!("placement capacity task: {}", e)))?
}

#[cfg(test)]
mod tests {
    use super::placement_required_bytes_by_root;

    #[test]
    fn placement_requirements_shift_with_part_index() {
        let req0 = placement_required_bytes_by_root(3, 0, &[4096], 2, 1, 4096);
        let req1 = placement_required_bytes_by_root(3, 1, &[4096], 2, 1, 4096);
        assert_eq!(req0, vec![4096, 4096, 4096]);
        assert_eq!(req1, vec![4096, 4096, 4096]);
    }

    #[test]
    fn placement_requirements_accumulate_when_roots_are_fewer_than_shards() {
        let req = placement_required_bytes_by_root(2, 0, &[4096], 2, 1, 4096);
        assert_eq!(req, vec![8192, 4096]);
    }

    #[test]
    fn placement_requirements_cover_multiple_parts() {
        let req = placement_required_bytes_by_root(3, 0, &[4096, 4096], 2, 1, 4096);
        assert_eq!(req, vec![8192, 8192, 8192]);
    }
}
