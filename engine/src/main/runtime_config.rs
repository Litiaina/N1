use super::*;

pub(super) fn detect_cpu_cores() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

#[derive(Clone, Copy)]
pub(super) struct RuntimeThreadConfig {
    pub(super) worker_threads: usize,
    pub(super) max_blocking_threads: usize,
    pub(super) thread_stack_bytes: usize,
}

pub(super) fn parse_env_usize(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
}

pub(super) fn runtime_thread_config(cpu_cores: usize) -> RuntimeThreadConfig {
    let worker_threads = parse_env_usize("N2_RUNTIME_WORKER_THREADS")
        .unwrap_or_else(|| cpu_cores.clamp(2, 16))
        .clamp(1, 64);
    let max_blocking_threads = parse_env_usize("N2_RUNTIME_MAX_BLOCKING_THREADS")
        .unwrap_or_else(|| cpu_cores.saturating_mul(4).clamp(16, 32))
        .clamp(8, 128);
    let thread_stack_bytes = parse_env_usize("N2_RUNTIME_THREAD_STACK_BYTES")
        .unwrap_or(1024 * 1024)
        .clamp(512 * 1024, 8 * 1024 * 1024);

    RuntimeThreadConfig {
        worker_threads,
        max_blocking_threads,
        thread_stack_bytes,
    }
}

pub(super) fn align_up_4k(v: usize) -> usize {
    ((v.max(4096) + 4095) / 4096) * 4096
}

pub(super) fn ram_gib(ram_bytes: Option<u64>) -> u64 {
    ram_bytes
        .map(|b| b / (1024 * 1024 * 1024))
        .unwrap_or(0)
        .max(1)
}

pub(super) fn adaptive_max_concurrent_io(
    cpu_cores: usize,
    blob_disks: usize,
    ram_bytes: Option<u64>,
) -> usize {
    let core_cap = (cpu_cores.saturating_mul(2)).clamp(1, 64);
    let disk_cap = match blob_disks {
        0 | 1 => 4,
        2 => 6,
        3 => 8,
        4 => 12,
        _ => blob_disks.saturating_mul(3),
    }
    .clamp(1, 64);
    let ram_cap = match ram_gib(ram_bytes) {
        0..=4 => 2,
        5..=8 => 4,
        9..=16 => 8,
        17..=32 => 20,
        _ => 24,
    };
    core_cap.min(disk_cap).min(ram_cap).clamp(1, 64)
}

pub(super) fn adaptive_writer_buffer_size(
    cpu_cores: usize,
    ram_bytes: Option<u64>,
    blob_disks: usize,
    total_shards: usize,
) -> usize {
    let mut bytes = match ram_gib(ram_bytes) {
        0..=4 => 256 * 1024,
        5..=8 => 512 * 1024,
        9..=64 => 1024 * 1024,
        _ => 2 * 1024 * 1024,
    };

    if cpu_cores <= 2 {
        bytes = bytes.min(256 * 1024);
    } else if cpu_cores <= 4 {
        bytes = bytes.min(512 * 1024);
    } else if cpu_cores <= 8 {
        bytes = bytes.min(1024 * 1024);
    }

    if total_shards >= 8 {
        bytes = bytes.min(256 * 1024);
    } else if total_shards >= 6 {
        bytes = bytes.min(512 * 1024);
    } else if total_shards >= 4 {
        bytes = bytes.min(768 * 1024);
    }

    if blob_disks <= 1 {
        bytes = bytes.min(256 * 1024);
    } else if blob_disks == 2 {
        bytes = bytes.min(512 * 1024);
    }

    align_up_4k(bytes.clamp(64 * 1024, 4 * 1024 * 1024))
}

pub(super) fn detect_total_ram_bytes() -> Option<u64> {
    let content = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kib = rest
                .split_whitespace()
                .next()
                .and_then(|v| v.parse::<u64>().ok())?;
            return Some(kib.saturating_mul(1024));
        }
    }
    None
}

pub(super) fn adaptive_upload_memory_budget_bytes(
    cpu_cores: usize,
    ram_bytes: Option<u64>,
    blob_disks: usize,
    total_shards: usize,
) -> usize {
    let mut budget: usize = match ram_gib(ram_bytes) {
        0..=4 => 32usize * 1024 * 1024,
        5..=8 => 48usize * 1024 * 1024,
        9..=16 => 64usize * 1024 * 1024,
        17..=32 => 80usize * 1024 * 1024,
        _ => 96usize * 1024 * 1024,
    };

    if cpu_cores >= 24 {
        budget = budget.saturating_add(16usize * 1024 * 1024);
    }
    if total_shards >= 4 {
        budget = budget.saturating_add(16usize * 1024 * 1024);
    }

    if blob_disks <= 1 {
        budget = budget.min(64usize * 1024 * 1024);
    } else if blob_disks == 2 {
        budget = budget.min(96usize * 1024 * 1024);
    }

    budget.clamp(32usize * 1024 * 1024, 128usize * 1024 * 1024)
}

pub(super) fn raise_open_file_limit_for_storage() {
    let mut limit = std::mem::MaybeUninit::<libc::rlimit>::uninit();
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) };
    if rc != 0 {
        tracing::warn!("Open-file limit: unable to inspect process limit");
        return;
    }

    let mut limit = unsafe { limit.assume_init() };
    if limit.rlim_cur == libc::RLIM_INFINITY {
        return;
    }

    let desired = if limit.rlim_max == libc::RLIM_INFINITY {
        1_048_576 as libc::rlim_t
    } else {
        limit.rlim_max
    };
    if desired <= limit.rlim_cur {
        return;
    }

    let previous = limit.rlim_cur;
    limit.rlim_cur = desired;
    let rc = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) };
    if rc == 0 {
        tracing::info!(
            "Open-file limit: raised soft limit from {} to {} for N2 storage index",
            previous,
            desired
        );
    } else {
        tracing::warn!(
            "Open-file limit: unable to raise soft limit from {} to {}; N2 index directory concurrency may be capped",
            previous,
            desired
        );
    }
}

pub(super) fn estimate_upload_part_working_set_bytes(
    data_shards: usize,
    parity_shards: usize,
    writer_buffer_size: usize,
    stripe_block_size: usize,
) -> usize {
    let total_shards = data_shards.saturating_add(parity_shards).max(1);
    let writer_bytes = total_shards.saturating_mul(writer_buffer_size.max(4096));
    let ec_ingest = data_shards
        .max(1)
        .saturating_mul(stripe_block_size.max(4096));
    let framing_overhead = 512 * 1024usize;
    writer_bytes
        .saturating_add(ec_ingest.saturating_mul(2))
        .saturating_add(framing_overhead)
        .max(1)
}

pub(super) fn adaptive_stream_chunk_size(cpu_cores: usize, ram_bytes: Option<u64>) -> usize {
    let bytes = if cpu_cores <= 2 || ram_gib(ram_bytes) <= 4 {
        128 * 1024
    } else if cpu_cores <= 4 || ram_gib(ram_bytes) <= 8 {
        256 * 1024
    } else if cpu_cores <= 8 || ram_gib(ram_bytes) <= 16 {
        512 * 1024
    } else {
        1024 * 1024
    };
    align_up_4k(bytes)
}

pub(super) fn adaptive_stream_readahead_bytes(chunk_size: usize, ram_bytes: Option<u64>) -> usize {
    let mul = match ram_gib(ram_bytes) {
        0..=4 => 2,
        5..=8 => 2,
        9..=16 => 3,
        17..=32 => 4,
        _ => 6,
    };
    align_up_4k((chunk_size.saturating_mul(mul)).clamp(64 * 1024, 8 * 1024 * 1024))
}

pub(super) fn adaptive_stream_memory_budget_bytes(ram_bytes: Option<u64>) -> usize {
    match ram_gib(ram_bytes) {
        0..=4 => 32 * 1024 * 1024,
        5..=8 => 48 * 1024 * 1024,
        9..=16 => 64 * 1024 * 1024,
        17..=32 => 80 * 1024 * 1024,
        _ => 96 * 1024 * 1024,
    }
}

pub(super) fn adaptive_stream_max_concurrency(
    cpu_cores: usize,
    blob_disks: usize,
    ram_bytes: Option<u64>,
    chunk_size: usize,
    readahead_bytes: usize,
) -> usize {
    let core_cap = (cpu_cores.saturating_mul(8)).clamp(8, 256);
    let disk_cap = (blob_disks.max(1).saturating_mul(32)).clamp(8, 256);
    let per_stream = chunk_size
        .saturating_add(readahead_bytes)
        .saturating_add(64 * 1024)
        .max(1);
    let mem_cap = (adaptive_stream_memory_budget_bytes(ram_bytes) / per_stream).clamp(4, 256);
    core_cap.min(disk_cap).min(mem_cap).clamp(4, 256)
}

pub(super) fn normalize_fs_backed_disk_paths(role: &str, paths: Vec<String>) -> Vec<String> {
    let mut normalized = Vec::with_capacity(paths.len());
    for raw in paths {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }

        let candidate = PathBuf::from(trimmed);
        let meta = match std::fs::symlink_metadata(&candidate) {
            Ok(meta) => meta,
            Err(error) => {
                tracing::warn!(
                    "Skipping unavailable {role} entry '{trimmed}' during degraded startup: {error}"
                );
                continue;
            }
        };
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileTypeExt;
            let ft = meta.file_type();
            if ft.is_block_device() || ft.is_char_device() {
                tracing::warn!(
                    "Skipping {role} entry '{trimmed}': N2 requires a mounted directory, not a device"
                );
                continue;
            }
        }
        if !meta.is_dir() {
            tracing::warn!(
                "Skipping unavailable {role} entry '{trimmed}': path is not a directory"
            );
            continue;
        }

        let resolved = std::fs::canonicalize(&candidate).unwrap_or(candidate);
        normalized.push(resolved.to_string_lossy().to_string());
    }

    if normalized.is_empty() {
        panic!(
            "At least one valid directory must be configured in {}",
            role
        );
    }
    normalized
}

#[cfg(unix)]
pub(super) fn detect_unique_storage_devices(paths: &[String]) -> usize {
    use std::os::unix::fs::MetadataExt;

    let mut dev_ids: HashSet<u64> = HashSet::new();
    let mut fallback_paths: HashSet<PathBuf> = HashSet::new();

    for raw in paths {
        let p = PathBuf::from(raw);
        let resolved = std::fs::canonicalize(&p).unwrap_or(p);
        fallback_paths.insert(resolved.clone());

        if let Ok(meta) = std::fs::metadata(&resolved) {
            dev_ids.insert(meta.dev());
        }
    }

    if !dev_ids.is_empty() {
        dev_ids.len().max(1)
    } else {
        fallback_paths.len().max(1)
    }
}

#[cfg(not(unix))]
pub(super) fn detect_unique_storage_devices(paths: &[String]) -> usize {
    let mut unique: HashSet<PathBuf> = HashSet::new();
    for raw in paths {
        let p = PathBuf::from(raw);
        let resolved = std::fs::canonicalize(&p).unwrap_or(p);
        unique.insert(resolved);
    }
    unique.len().max(1)
}

pub(super) fn adaptive_concurrency_interval_ms(cpu_cores: usize, ram_bytes: Option<u64>) -> usize {
    let _ = (cpu_cores, ram_bytes);
    60_000
}

pub(super) fn adaptive_monitor_interval_seconds(cpu_cores: usize, ram_bytes: Option<u64>) -> usize {
    let _ = (cpu_cores, ram_bytes);
    300
}

pub(super) fn adaptive_monitor_deep_scan_interval_seconds(
    cpu_cores: usize,
    ram_bytes: Option<u64>,
) -> usize {
    let _ = (cpu_cores, ram_bytes);
    3600
}

pub(super) fn parse_usize_or_auto(raw: Option<&str>, key: &str, auto_value: usize) -> usize {
    match raw.map(str::trim) {
        None => auto_value,
        Some(v) if v.is_empty() => auto_value,
        Some(v) if v.eq_ignore_ascii_case("auto") => auto_value,
        Some(v) => match v.parse::<usize>() {
            Ok(0) => auto_value,
            Ok(parsed) => parsed,
            Err(_) => panic!("{key} must be a positive usize or 'auto'"),
        },
    }
}

pub(super) fn parse_bool_or_default(raw: Option<&str>, default_value: bool) -> bool {
    match raw.map(str::trim) {
        None => default_value,
        Some(v) if v.is_empty() => default_value,
        Some(v) if matches!(v, "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON") => true,
        Some(v) if matches!(v, "0" | "false" | "FALSE" | "no" | "NO" | "off" | "OFF") => false,
        Some(_) => default_value,
    }
}

pub(super) fn is_auto_or_empty(raw: Option<&str>) -> bool {
    match raw.map(str::trim) {
        None => true,
        Some(v) if v.is_empty() => true,
        Some(v) if v.eq_ignore_ascii_case("auto") => true,
        Some(_) => false,
    }
}
