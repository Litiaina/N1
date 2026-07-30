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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageReadiness {
    pub readable: bool,
    pub writable: bool,
    pub root_count: usize,
    pub readable_roots: usize,
    pub writable_roots: usize,
    pub minimum_available_bytes: Option<u64>,
    pub issues: Vec<String>,
}

const MIN_STORAGE_WRITE_HEADROOM_BYTES: u64 = 8 * 1024 * 1024;

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

#[cfg(unix)]
fn path_accessible(path: &Path, mode: libc::c_int) -> bool {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    CString::new(path.as_os_str().as_bytes())
        .ok()
        .is_some_and(|raw| unsafe { libc::access(raw.as_ptr(), mode) } == 0)
}

#[cfg(not(unix))]
fn path_accessible(path: &Path, _mode: libc::c_int) -> bool {
    path.is_dir()
}

fn classify_storage_readiness(roots: &[PathBuf]) -> StorageReadiness {
    let mut readable_roots = 0usize;
    let mut writable_roots = 0usize;
    let mut minimum_available_bytes = None::<u64>;
    let mut issues = Vec::new();

    for root in roots {
        match std::fs::metadata(root) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => {
                issues.push(format!("{} is not a directory", root.display()));
                continue;
            }
            Err(error) => {
                issues.push(format!("{} is unavailable: {error}", root.display()));
                continue;
            }
        }

        let readable = path_accessible(root, libc::R_OK | libc::X_OK);
        if readable {
            readable_roots = readable_roots.saturating_add(1);
        } else {
            issues.push(format!("{} is not readable", root.display()));
        }

        let available_bytes = match statvfs_totals_sync(root) {
            Ok((_, _, available_bytes)) => {
                minimum_available_bytes = Some(
                    minimum_available_bytes
                        .map(|current| current.min(available_bytes))
                        .unwrap_or(available_bytes),
                );
                available_bytes
            }
            Err(error) => {
                issues.push(format!(
                    "{} capacity is unavailable: {error}",
                    root.display()
                ));
                continue;
            }
        };
        let writable = path_accessible(root, libc::W_OK | libc::X_OK)
            && available_bytes >= MIN_STORAGE_WRITE_HEADROOM_BYTES;
        if writable {
            writable_roots = writable_roots.saturating_add(1);
        } else {
            issues.push(format!(
                "{} is not write-ready ({} bytes available; {} required headroom)",
                root.display(),
                available_bytes,
                MIN_STORAGE_WRITE_HEADROOM_BYTES
            ));
        }
    }

    StorageReadiness {
        // A node is a placement participant, not a single disk. Losing one
        // configured root must remain a degraded node-level condition while
        // any other root can still serve I/O.
        readable: readable_roots > 0,
        writable: writable_roots > 0,
        root_count: roots.len(),
        readable_roots,
        writable_roots,
        minimum_available_bytes,
        issues,
    }
}

pub async fn get_storage_readiness() -> Result<StorageReadiness, AppError> {
    let roots = ObjectPathManager::new().get_storage_roots();
    tokio::task::spawn_blocking(move || classify_storage_readiness(&roots))
        .await
        .map_err(|error| AppError::InternalServerError(format!("storage readiness task: {error}")))
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
        .get_storage_roots()
        .into_iter()
        .chain(pm.get_storage_roots())
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
    let roots = ObjectPathManager::new().get_storage_roots();
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
        pm.get_storage_roots()
            .into_iter()
            .map(|path| (path, "index".to_string())),
    );
    roots.extend(
        pm.get_storage_roots()
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
    let roots = ObjectPathManager::new().get_storage_roots();
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
            "Configured storage devices ({}) cannot satisfy target layout {}D+{}P requiring {} distinct devices",
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

    #[test]
    fn storage_readiness_rejects_missing_and_non_directory_roots() {
        let base = std::env::temp_dir().join(format!(
            "n2-storage-readiness-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        std::fs::create_dir_all(&base).unwrap();
        let file = base.join("not-a-root");
        std::fs::write(&file, b"not a directory").unwrap();
        let missing = base.join("missing");

        let readiness = classify_storage_readiness(&[file, missing]);
        assert!(!readiness.readable);
        assert!(!readiness.writable);
        assert_eq!(readiness.root_count, 2);
        assert_eq!(readiness.readable_roots, 0);
        assert_eq!(readiness.writable_roots, 0);
        assert_eq!(readiness.issues.len(), 2);

        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn storage_readiness_accepts_an_accessible_root_with_headroom() {
        let root = std::env::temp_dir().join(format!(
            "n2-storage-readiness-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        std::fs::create_dir_all(&root).unwrap();

        let readiness = classify_storage_readiness(std::slice::from_ref(&root));
        assert!(readiness.readable);
        assert!(readiness.writable);
        assert_eq!(readiness.readable_roots, 1);
        assert_eq!(readiness.writable_roots, 1);
        assert!(
            readiness.minimum_available_bytes.unwrap_or_default()
                >= MIN_STORAGE_WRITE_HEADROOM_BYTES
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn storage_readiness_keeps_node_available_when_one_root_is_lost() {
        let base = std::env::temp_dir().join(format!(
            "n2-storage-readiness-degraded-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let healthy = base.join("healthy");
        let missing = base.join("missing");
        std::fs::create_dir_all(&healthy).unwrap();

        let readiness = classify_storage_readiness(&[healthy, missing]);
        assert!(readiness.readable);
        assert!(readiness.writable);
        assert_eq!(readiness.root_count, 2);
        assert_eq!(readiness.readable_roots, 1);
        assert_eq!(readiness.writable_roots, 1);
        assert!(!readiness.issues.is_empty());

        std::fs::remove_dir_all(base).unwrap();
    }
}
