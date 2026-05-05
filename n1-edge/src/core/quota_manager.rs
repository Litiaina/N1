use crate::core::disk_manager::ObjectPathManager;
use crate::core::edge_index_store;
use crate::enums::app_error_enum::AppError;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy)]
pub struct StorageUsage {
    pub capacity_allocated: u64,
    pub capacity_used: u64,
    pub capacity_available: u64,
    pub filesystem_count: usize,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct FragmentBlobUsage {
    pub live_bytes: u64,
    pub trash_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FragmentQuotaBackend {
    Disabled,
    XfsProject,
}

impl FragmentQuotaBackend {
    pub fn parse(value: Option<&str>) -> Result<Self, AppError> {
        let Some(value) = value else {
            return Ok(Self::default_backend());
        };
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "xfs_project" | "xfs-project" | "xfs" | "project" | "required" => {
                Ok(Self::XfsProject)
            }
            "disabled" | "off" | "none" | "report_only" | "report-only" => Ok(Self::Disabled),
            other => Err(AppError::BadRequest(format!(
                "unsupported quota_config.fragment_quota_backend value: {other}"
            ))),
        }
    }

    fn default_backend() -> Self {
        #[cfg(test)]
        {
            Self::Disabled
        }
        #[cfg(not(test))]
        {
            Self::XfsProject
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::XfsProject => "xfs_project",
        }
    }

    pub fn public_name(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::XfsProject => "filesystem_project",
        }
    }
}

static FRAGMENT_QUOTA_BACKEND: OnceLock<FragmentQuotaBackend> = OnceLock::new();
const QUOTA_PERMISSION_PROBE_PROJECT_ID: u32 = 9_999;

pub fn configure_fragment_quota_backend(backend: FragmentQuotaBackend) -> Result<(), AppError> {
    FRAGMENT_QUOTA_BACKEND.set(backend).map_err(|_| {
        AppError::InternalServerError("fragment quota backend configured more than once".into())
    })
}

pub fn fragment_quota_backend() -> FragmentQuotaBackend {
    *FRAGMENT_QUOTA_BACKEND.get_or_init(FragmentQuotaBackend::default_backend)
}

pub fn fragment_quota_enforcement_enabled() -> bool {
    fragment_quota_backend() == FragmentQuotaBackend::XfsProject
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MountInfo {
    mount_point: PathBuf,
    fs_type: String,
    options: Vec<String>,
    super_options: Vec<String>,
}

fn decode_mountinfo_path(value: &str) -> PathBuf {
    let mut out = Vec::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut idx = 0usize;
    while idx < bytes.len() {
        if bytes[idx] == b'\\' && idx + 3 < bytes.len() {
            let octal = &value[idx + 1..idx + 4];
            if let Ok(decoded) = u8::from_str_radix(octal, 8) {
                out.push(decoded);
                idx += 4;
                continue;
            }
        }
        out.push(bytes[idx]);
        idx += 1;
    }
    PathBuf::from(String::from_utf8_lossy(&out).into_owned())
}

fn parse_mountinfo_line(line: &str) -> Option<MountInfo> {
    let (left, right) = line.split_once(" - ")?;
    let left_fields = left.split_whitespace().collect::<Vec<_>>();
    let right_fields = right.split_whitespace().collect::<Vec<_>>();
    if left_fields.len() < 6 || right_fields.len() < 3 {
        return None;
    }
    Some(MountInfo {
        mount_point: decode_mountinfo_path(left_fields[4]),
        fs_type: right_fields[0].to_string(),
        options: left_fields[5].split(',').map(str::to_string).collect(),
        super_options: right_fields[2].split(',').map(str::to_string).collect(),
    })
}

fn read_mountinfo() -> Result<Vec<MountInfo>, AppError> {
    let text = std::fs::read_to_string("/proc/self/mountinfo")
        .map_err(|err| AppError::Io(err, "read /proc/self/mountinfo".into()))?;
    Ok(text.lines().filter_map(parse_mountinfo_line).collect())
}

fn find_mount_for_path<'a>(mounts: &'a [MountInfo], path: &Path) -> Option<&'a MountInfo> {
    mounts
        .iter()
        .filter(|mount| path.starts_with(&mount.mount_point))
        .max_by_key(|mount| mount.mount_point.components().count())
}

fn mount_has_project_quota(mount: &MountInfo) -> bool {
    let has_project = mount
        .options
        .iter()
        .chain(mount.super_options.iter())
        .any(|option| matches!(option.as_str(), "prjquota" | "pquota"));
    let disabled = mount
        .options
        .iter()
        .chain(mount.super_options.iter())
        .any(|option| option == "noquota");
    has_project && !disabled
}

fn canonical_existing_path(path: &Path) -> Result<PathBuf, AppError> {
    std::fs::canonicalize(path)
        .map_err(|err| AppError::Io(err, format!("canonicalize {}", path.display())))
}

fn xfs_quota_bin() -> String {
    if let Ok(value) = std::env::var("N1_XFS_QUOTA_BIN") {
        return value;
    }
    if Path::new("/usr/sbin/xfs_quota").exists() {
        return "/usr/sbin/xfs_quota".to_string();
    }
    "xfs_quota".to_string()
}

fn run_xfs_quota_command(mount_point: &Path, command: &str) -> Result<String, AppError> {
    let output = Command::new(xfs_quota_bin())
        .arg("-x")
        .arg("-c")
        .arg(command)
        .arg(mount_point)
        .output()
        .map_err(|err| AppError::Io(err, "run xfs_quota".into()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if stderr.is_empty() { stdout } else { stderr };
        let detail = if detail.contains("Operation not permitted")
            || detail.contains("Permission denied")
        {
            format!(
                "{detail}; strict fragment quota requires the N1 service to set filesystem project quota limits. Run N1 with the required filesystem quota privileges, or set N1_XFS_QUOTA_BIN to a root-owned quota helper for N1."
            )
        } else {
            detail
        };
        return Err(AppError::ServiceUnavailable(format!(
            "N1 filesystem quota command failed on {}: {} ({})",
            mount_point.display(),
            command,
            detail
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn xfs_quota_path_arg(path: &Path) -> Result<String, AppError> {
    let raw = path.to_str().ok_or_else(|| {
        AppError::ServiceUnavailable(format!(
            "strict fragment quota path {} is not valid UTF-8",
            path.display()
        ))
    })?;
    if raw
        .chars()
        .any(|ch| ch.is_whitespace() || ch.is_control() || matches!(ch, '"' | '\'' | '\\' | ';'))
    {
        return Err(AppError::ServiceUnavailable(format!(
            "strict fragment quota path {} contains whitespace or quoting characters; use simple mount/blob paths such as /mnt/disk-00",
            path.display()
        )));
    }
    Ok(raw.to_string())
}

fn quota_limit_kib(bytes: u64) -> u64 {
    bytes.div_ceil(1024).max(1)
}

fn unix_timestamp_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}

fn xfs_project_check_has_mismatch(output: &str) -> bool {
    output.lines().map(str::trim).any(|line| {
        !line.is_empty()
            && !line.starts_with("Checking project ")
            && !(line.starts_with("Processed ") && line.contains(" paths for project "))
    })
}

fn validate_xfs_project_quota_root(
    root: &Path,
    mounts: &[MountInfo],
) -> Result<MountInfo, AppError> {
    let canonical = canonical_existing_path(root)?;
    let mount = find_mount_for_path(mounts, &canonical)
        .cloned()
        .ok_or_else(|| {
            AppError::ServiceUnavailable(format!(
                "blob root {} is not covered by a mounted filesystem",
                root.display()
            ))
        })?;
    if mount.fs_type != "xfs" {
        return Err(AppError::ServiceUnavailable(format!(
            "blob root {} is on filesystem type '{}' but strict N1 fragment quota requires a project-quota-capable filesystem",
            root.display(),
            mount.fs_type
        )));
    }
    if !mount_has_project_quota(&mount) {
        return Err(AppError::ServiceUnavailable(format!(
            "blob root {} is on mount {} but filesystem project quota is not enabled; enable project quotas for this mount",
            root.display(),
            mount.mount_point.display()
        )));
    }
    Ok(mount)
}

fn unique_blob_quota_mounts(
    roots: &[PathBuf],
    mounts: &[MountInfo],
) -> Result<Vec<MountInfo>, AppError> {
    let mut by_mount = HashMap::<PathBuf, MountInfo>::new();
    for root in roots {
        let mount = validate_xfs_project_quota_root(root, mounts)?;
        by_mount.entry(mount.mount_point.clone()).or_insert(mount);
    }
    Ok(by_mount.into_values().collect())
}

fn validate_xfs_project_quota_backend_sync() -> Result<(), AppError> {
    let roots = ObjectPathManager::new().get_blob_roots();
    if roots.is_empty() {
        return Err(AppError::ServiceUnavailable(
            "strict fragment quota requires at least one blob root".into(),
        ));
    }
    let mounts = read_mountinfo()?;
    let unique_mounts = unique_blob_quota_mounts(&roots, &mounts)?;
    for mount in unique_mounts {
        run_xfs_quota_command(&mount.mount_point, "state -p")?;
    }
    for root in roots {
        let mount = validate_xfs_project_quota_root(&root, &mounts)?;
        probe_xfs_project_quota_permissions(&root, &mount)?;
    }
    Ok(())
}

fn probe_xfs_project_quota_permissions(root: &Path, mount: &MountInfo) -> Result<(), AppError> {
    let probe_name = format!(
        ".n1-quota-probe-{}-{}",
        std::process::id(),
        unix_timestamp_nanos()
    );
    let probe_dir = root.join(probe_name);
    let result = (|| {
        std::fs::create_dir(&probe_dir).map_err(|err| {
            AppError::ServiceUnavailable(format!(
                "strict fragment quota cannot create probe directory {}: {}",
                probe_dir.display(),
                err
            ))
        })?;
        let probe_root = canonical_existing_path(&probe_dir)?;
        let probe_root_arg = xfs_quota_path_arg(&probe_root)?;
        let check = run_xfs_quota_command(
            &mount.mount_point,
            &format!("project -c -d 0 -p {probe_root_arg} {QUOTA_PERMISSION_PROBE_PROJECT_ID}"),
        )?;
        if xfs_project_check_has_mismatch(&check) {
            run_xfs_quota_command(
                &mount.mount_point,
                &format!("project -s -d 0 -p {probe_root_arg} {QUOTA_PERMISSION_PROBE_PROJECT_ID}"),
            )?;
        }
        run_xfs_quota_command(
            &mount.mount_point,
            &format!("limit -p bhard=1k {QUOTA_PERMISSION_PROBE_PROJECT_ID}"),
        )?;
        Ok(())
    })();
    let _ = std::fs::remove_dir(&probe_dir);
    result
}

pub async fn validate_fragment_quota_backend() -> Result<(), AppError> {
    if !fragment_quota_enforcement_enabled() {
        return Ok(());
    }
    tokio::task::spawn_blocking(validate_xfs_project_quota_backend_sync)
        .await
        .map_err(|err| AppError::InternalServerError(format!("quota backend task: {err}")))?
}

fn apply_xfs_project_quota_sync(
    fragment: &str,
    quota_bytes: u64,
    project_id: u32,
) -> Result<(), AppError> {
    if quota_bytes == 0 {
        return Err(AppError::ServiceUnavailable(format!(
            "fragment {fragment} has zero quota; strict project quota cannot be applied"
        )));
    }
    let roots = ObjectPathManager::new().get_blob_roots();
    if roots.is_empty() {
        return Err(AppError::ServiceUnavailable(
            "strict fragment quota requires at least one blob root".into(),
        ));
    }
    let mounts = read_mountinfo()?;
    let unique_mount_count = unique_blob_quota_mounts(&roots, &mounts)?.len().max(1);
    let per_mount_quota_bytes = quota_bytes.div_ceil(unique_mount_count as u64).max(1);
    let hard_limit = quota_limit_kib(per_mount_quota_bytes);

    for root in roots {
        std::fs::create_dir_all(root.join(fragment)).map_err(|err| {
            AppError::Io(
                err,
                format!(
                    "create fragment quota root {}",
                    root.join(fragment).display()
                ),
            )
        })?;
        let mount = validate_xfs_project_quota_root(&root, &mounts)?;
        let fragment_root = canonical_existing_path(&root.join(fragment))?;
        let fragment_root_arg = xfs_quota_path_arg(&fragment_root)?;
        let root_check = run_xfs_quota_command(
            &mount.mount_point,
            &format!("project -c -d 0 -p {fragment_root_arg} {project_id}"),
        )?;
        if xfs_project_check_has_mismatch(&root_check) {
            run_xfs_quota_command(
                &mount.mount_point,
                &format!("project -s -p {fragment_root_arg} {project_id}"),
            )?;
        }
        run_xfs_quota_command(
            &mount.mount_point,
            &format!("limit -p bhard={hard_limit}k {project_id}"),
        )?;
        let check = run_xfs_quota_command(
            &mount.mount_point,
            &format!("project -c -d 0 -p {fragment_root_arg} {project_id}"),
        )?;
        if xfs_project_check_has_mismatch(&check) {
            return Err(AppError::ServiceUnavailable(format!(
                "N1 filesystem quota verification failed for {}: {}",
                fragment_root.display(),
                check.trim()
            )));
        }
    }
    Ok(())
}

pub async fn apply_fragment_project_quota(
    fragment: &str,
    quota_bytes: u64,
    project_id: u32,
) -> Result<(), AppError> {
    if !fragment_quota_enforcement_enabled() {
        return Ok(());
    }
    let fragment = fragment.to_string();
    tokio::task::spawn_blocking(move || {
        apply_xfs_project_quota_sync(&fragment, quota_bytes, project_id)
    })
    .await
    .map_err(|err| AppError::InternalServerError(format!("quota apply task: {err}")))?
}

pub async fn apply_all_fragment_project_quotas() -> Result<(), AppError> {
    if !fragment_quota_enforcement_enabled() {
        return Ok(());
    }
    validate_fragment_quota_backend().await?;
    let specs = tokio::task::spawn_blocking(edge_index_store::active_fragment_quota_specs)
        .await
        .map_err(|err| AppError::InternalServerError(format!("quota spec task: {err}")))??;
    for spec in specs {
        let fragment = spec.fragment.clone();
        let project_id = tokio::task::spawn_blocking(move || {
            edge_index_store::ensure_fragment_quota_project_id(&fragment)
        })
        .await
        .map_err(|err| AppError::InternalServerError(format!("quota project id task: {err}")))??;
        apply_fragment_project_quota(&spec.fragment, spec.quota_bytes, project_id).await?;
    }
    Ok(())
}

impl FragmentBlobUsage {
    pub fn used_bytes(self) -> u64 {
        self.live_bytes.saturating_add(self.trash_bytes)
    }
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

#[cfg(unix)]
fn file_storage_bytes(metadata: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;

    metadata.blocks().saturating_mul(512)
}

#[cfg(not(unix))]
fn file_storage_bytes(metadata: &std::fs::Metadata) -> u64 {
    metadata.len()
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
            usage.live_bytes = usage
                .live_bytes
                .saturating_add(sum_file_tree_bytes(&root.join(&fragment).join("blob"))?);
            usage.trash_bytes = usage.trash_bytes.saturating_add(sum_file_tree_bytes(
                &root.join(&fragment).join(".trash").join("blob"),
            )?);
        }
        Ok::<FragmentBlobUsage, AppError>(usage)
    })
    .await
    .map_err(|err| AppError::InternalServerError(format!("fragment blob usage task: {err}")))?
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

pub async fn get_fragment_blob_usage(fragment: &str) -> Result<FragmentBlobUsage, AppError> {
    compute_fragment_blob_usage(fragment).await
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
    use super::{
        FragmentQuotaBackend, find_mount_for_path, mount_has_project_quota, parse_mountinfo_line,
        placement_required_bytes_by_root, quota_limit_kib, xfs_project_check_has_mismatch,
        xfs_quota_path_arg,
    };
    use std::path::Path;

    #[test]
    fn quota_backend_parser_defaults_to_disabled_in_tests() {
        assert_eq!(
            FragmentQuotaBackend::parse(None).expect("default backend"),
            FragmentQuotaBackend::Disabled
        );
        assert_eq!(
            FragmentQuotaBackend::parse(Some("xfs_project")).expect("xfs backend"),
            FragmentQuotaBackend::XfsProject
        );
        assert_eq!(
            FragmentQuotaBackend::parse(Some("disabled")).expect("disabled backend"),
            FragmentQuotaBackend::Disabled
        );
    }

    #[test]
    fn mountinfo_parser_detects_xfs_project_quota() {
        let line = "42 21 8:1 / /mnt/disk-00 rw,relatime,prjquota - xfs /dev/sda1 rw,attr2,inode64,prjquota";
        let mount = parse_mountinfo_line(line).expect("mountinfo line");
        assert_eq!(mount.mount_point, Path::new("/mnt/disk-00"));
        assert_eq!(mount.fs_type, "xfs");
        assert!(mount_has_project_quota(&mount));
    }

    #[test]
    fn mountinfo_parser_rejects_noquota_for_strict_quota() {
        let line =
            "42 21 8:1 / /mnt/disk-00 rw,relatime,noquota - xfs /dev/sda1 rw,attr2,inode64,noquota";
        let mount = parse_mountinfo_line(line).expect("mountinfo line");
        assert!(!mount_has_project_quota(&mount));
    }

    #[test]
    fn mount_lookup_prefers_deepest_mount() {
        let parent = parse_mountinfo_line("1 0 8:1 / /mnt rw,relatime - xfs /dev/sda1 rw,prjquota")
            .expect("parent mount");
        let child =
            parse_mountinfo_line("2 1 8:2 / /mnt/disk-00 rw,relatime - xfs /dev/sdb1 rw,prjquota")
                .expect("child mount");
        let mounts = vec![parent, child];
        let selected = find_mount_for_path(&mounts, Path::new("/mnt/disk-00/frag/blob"))
            .expect("selected mount");
        assert_eq!(selected.mount_point, Path::new("/mnt/disk-00"));
    }

    #[test]
    fn quota_limit_rounds_up_to_kib() {
        assert_eq!(quota_limit_kib(1), 1);
        assert_eq!(quota_limit_kib(1024), 1);
        assert_eq!(quota_limit_kib(1025), 2);
    }

    #[test]
    fn xfs_quota_path_arg_rejects_paths_xfs_quota_cannot_parse_safely() {
        assert_eq!(
            xfs_quota_path_arg(Path::new("/mnt/disk-00/fragment")).expect("simple path"),
            "/mnt/disk-00/fragment"
        );
        assert!(xfs_quota_path_arg(Path::new("/mnt/disk 00/fragment")).is_err());
        assert!(xfs_quota_path_arg(Path::new("/mnt/disk-00/frag;ment")).is_err());
    }

    #[test]
    fn xfs_project_check_ignores_success_status_lines() {
        let clean = "\
Checking project 10000 (path /mnt/disk-00/frag)...\n\
Processed 1 (/etc/projects and cmdline) paths for project 10000 with recursion depth limited (0).\n";
        assert!(!xfs_project_check_has_mismatch(clean));

        let dirty = "\
Checking project 10000 (path /mnt/disk-00/frag)...\n\
/mnt/disk-00/frag - project identifier is not set (inode=0, tree=10000)\n\
Processed 1 (/etc/projects and cmdline) paths for project 10000 with recursion depth limited (0).\n";
        assert!(xfs_project_check_has_mismatch(dirty));
    }

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
