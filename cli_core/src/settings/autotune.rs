//! Auto sizing retained from N1 commit 061a40a, engine/src/main.rs.
pub(super) fn align_up_4k(v: usize) -> usize {
    v.max(4096).div_ceil(4096) * 4096
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
