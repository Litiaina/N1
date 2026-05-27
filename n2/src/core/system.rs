use serde::Serialize;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs;

#[derive(Debug, Serialize, Clone, Default)]
pub struct SystemMetrics {
    pub cpu_usage_percent: f64,
    pub cpu_cores: u64,
    pub ram_total_bytes: u64,
    pub ram_used_bytes: u64,
    pub ram_usage_percent: f64,
    pub load_avg_1m: f64,
    pub uptime_seconds: u64,
    pub process_resident_bytes: u64,
    pub process_virtual_bytes: u64,
    pub process_open_fds: Option<u64>,
}

static PREV_TOTAL: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));
static PREV_IDLE: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));

pub async fn get_system_metrics() -> SystemMetrics {
    let (cpu_usage, _) = calculate_cpu_usage().await;
    let (ram_total, ram_available) = get_memory_info().await;
    let load_avg = get_load_avg().await;
    let uptime_seconds = get_uptime_seconds().await;
    let (process_resident_bytes, process_virtual_bytes) = get_process_memory_info().await;
    let process_open_fds = get_process_open_fd_count().await;

    let ram_used = ram_total.saturating_sub(ram_available);
    let ram_percent = if ram_total > 0 {
        (ram_used as f64 / ram_total as f64) * 100.0
    } else {
        0.0
    };

    SystemMetrics {
        cpu_usage_percent: cpu_usage,
        cpu_cores: std::thread::available_parallelism()
            .map(|count| count.get() as u64)
            .unwrap_or(1),
        ram_total_bytes: ram_total,
        ram_used_bytes: ram_used,
        ram_usage_percent: ram_percent,
        load_avg_1m: load_avg,
        uptime_seconds,
        process_resident_bytes,
        process_virtual_bytes,
        process_open_fds,
    }
}

async fn calculate_cpu_usage() -> (f64, (u64, u64)) {
    if let Ok(content) = fs::read_to_string("/proc/stat").await {
        if let Some(line) = content.lines().next() {
            if line.starts_with("cpu ") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 5 {
                    let user: u64 = parts[1].parse().unwrap_or(0);
                    let nice: u64 = parts[2].parse().unwrap_or(0);
                    let system: u64 = parts[3].parse().unwrap_or(0);
                    let idle: u64 = parts[4].parse().unwrap_or(0);
                    let iowait: u64 = parts.get(5).and_then(|s| s.parse().ok()).unwrap_or(0);
                    let irq: u64 = parts.get(6).and_then(|s| s.parse().ok()).unwrap_or(0);
                    let softirq: u64 = parts.get(7).and_then(|s| s.parse().ok()).unwrap_or(0);

                    let total = user + nice + system + idle + iowait + irq + softirq;

                    let prev_total = PREV_TOTAL.load(Ordering::Relaxed);
                    let prev_idle = PREV_IDLE.load(Ordering::Relaxed);

                    let total_delta = total.saturating_sub(prev_total);
                    let idle_delta = idle.saturating_sub(prev_idle);

                    let usage = if total_delta > 0 {
                        100.0 * (1.0 - (idle_delta as f64 / total_delta as f64))
                    } else {
                        0.0
                    };

                    PREV_TOTAL.store(total, Ordering::Relaxed);
                    PREV_IDLE.store(idle, Ordering::Relaxed);

                    return (usage, (total, idle));
                }
            }
        }
    }
    (0.0, (0, 0))
}

async fn get_memory_info() -> (u64, u64) {
    let mut total = 0;
    let mut available = 0;

    if let Ok(content) = fs::read_to_string("/proc/meminfo").await {
        for line in content.lines() {
            if line.starts_with("MemTotal:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    total = parts[1].parse::<u64>().unwrap_or(0) * 1024;
                }
            }
            if line.starts_with("MemAvailable:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    available = parts[1].parse::<u64>().unwrap_or(0) * 1024;
                }
            }
        }
    }
    (total, available)
}

async fn get_load_avg() -> f64 {
    if let Ok(content) = fs::read_to_string("/proc/loadavg").await {
        let parts: Vec<&str> = content.split_whitespace().collect();
        if !parts.is_empty() {
            return parts[0].parse().unwrap_or(0.0);
        }
    }
    0.0
}

async fn get_uptime_seconds() -> u64 {
    if let Ok(content) = fs::read_to_string("/proc/uptime").await {
        if let Some(first) = content.split_whitespace().next() {
            return first
                .split('.')
                .next()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0);
        }
    }
    0
}

async fn get_process_memory_info() -> (u64, u64) {
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    let page_size = if page_size > 0 {
        page_size as u64
    } else {
        4096
    };

    if let Ok(content) = fs::read_to_string("/proc/self/statm").await {
        let mut parts = content.split_whitespace();
        let virtual_pages = parts
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        let resident_pages = parts
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        return (
            resident_pages.saturating_mul(page_size),
            virtual_pages.saturating_mul(page_size),
        );
    }

    (0, 0)
}

async fn get_process_open_fd_count() -> Option<u64> {
    let mut dir = fs::read_dir("/proc/self/fd").await.ok()?;
    let mut count = 0u64;
    loop {
        match dir.next_entry().await {
            Ok(Some(_)) => count = count.saturating_add(1),
            Ok(None) => return Some(count),
            Err(_) => return None,
        }
    }
}
