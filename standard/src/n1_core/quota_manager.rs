use crate::enums::app_error_enum::AppError;
use crate::n1_core::disk_manager::ObjectPathManager;
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

// Legacy helper retained only for older tests/helpers during the transition.
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
