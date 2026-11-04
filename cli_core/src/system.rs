use serde::Serialize;
use std::sync::LazyLock;
use tokio::fs;
use tokio::sync::Mutex;

/// Read-only filesystem counters. No file creation, directory walk or payload
/// read is used to measure capacity. Object callers pass their pinned root FD.
#[derive(Debug, Serialize, Clone)]
pub struct FilesystemMetrics {
    pub device_id: u64,
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
    pub total_inodes: Option<u64>,
    pub available_inodes: Option<u64>,
    pub read_only: bool,
}
pub fn filesystem_metrics(file: &std::fs::File) -> std::io::Result<FilesystemMetrics> {
    use std::os::{fd::AsRawFd, unix::fs::MetadataExt};
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: file owns a live descriptor and stat is valid writable storage.
    if unsafe { libc::fstatvfs(file.as_raw_fd(), &mut stat) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let unit = stat.f_frsize;
    let total = unit.saturating_mul(stat.f_blocks);
    Ok(FilesystemMetrics {
        device_id: file.metadata()?.dev(),
        total_bytes: total,
        used_bytes: total.saturating_sub(unit.saturating_mul(stat.f_bfree)),
        available_bytes: unit.saturating_mul(stat.f_bavail),
        total_inodes: (stat.f_files != 0).then_some(stat.f_files),
        available_inodes: (stat.f_files != 0).then_some(stat.f_favail),
        read_only: stat.f_flag & libc::ST_RDONLY != 0,
    })
}
pub fn filesystem_at(path: &std::path::Path) -> std::io::Result<FilesystemMetrics> {
    use std::os::unix::fs::OpenOptionsExt;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    filesystem_metrics(&file)
}
pub async fn node_identity() -> serde_json::Value {
    let host = fs::read_to_string("/proc/sys/kernel/hostname").await.ok();
    serde_json::json!({"hostname":host.map(|s| s.trim().to_owned()),"pid":std::process::id()})
}

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

#[derive(Clone, Copy, Debug, PartialEq)]
struct CpuTimes {
    busy: u64,
    idle: u64,
    iowait: u64,
}

impl CpuTimes {
    fn parse(line: &str) -> Option<Self> {
        let mut fields = line.split_whitespace();
        if fields.next()? != "cpu" {
            return None;
        }

        // guest/guest_nice are already included in user/nice. Count the first
        // eight counters only, including steal but not double-counting guests.
        let mut counters = [0u64; 8];
        for (index, counter) in counters.iter_mut().enumerate() {
            match fields.next() {
                Some(value) => *counter = value.parse().ok()?,
                None if index < 4 => return None,
                None => break,
            }
        }
        let busy = [0, 1, 2, 5, 6, 7]
            .into_iter()
            .try_fold(0u64, |sum, index| sum.checked_add(counters[index]))?;
        Some(Self {
            busy,
            idle: counters[3],
            iowait: counters[4],
        })
    }

    fn usage_since(self, previous: Self) -> Option<f64> {
        let busy = self.busy.checked_sub(previous.busy)?;
        let idle = self.idle.checked_sub(previous.idle)?;
        // Linux documents that iowait can decrease. It is not CPU execution;
        // a decreasing counter contributes no waiting ticks to this sample.
        let iowait = self.iowait.saturating_sub(previous.iowait);
        let total = busy.checked_add(idle)?.checked_add(iowait)?;
        (total != 0).then(|| 100.0 * busy as f64 / total as f64)
    }
}

#[derive(Default)]
struct CpuUsageSampler {
    previous: Option<CpuTimes>,
    usage: f64,
}

impl CpuUsageSampler {
    fn observe(&mut self, current: CpuTimes) -> f64 {
        if let Some(previous) = self.previous {
            if let Some(usage) = current.usage_since(previous) {
                self.usage = usage;
            } else if current != previous {
                // Reset or invalid counters must not manufacture saturation.
                // Keep an unchanged (sub-jiffy) sample at its last value.
                self.usage = 0.0;
            }
        }
        self.previous = Some(current);
        self.usage
    }
}

// Metrics and adaptive-control callers share one coherent sample. Serialize
// the read too, so concurrent requests cannot publish older counters later.
static CPU_USAGE_SAMPLER: LazyLock<Mutex<CpuUsageSampler>> =
    LazyLock::new(|| Mutex::new(CpuUsageSampler::default()));

pub async fn get_system_metrics() -> SystemMetrics {
    let cpu_usage = calculate_cpu_usage().await;
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

async fn calculate_cpu_usage() -> f64 {
    let mut sampler = CPU_USAGE_SAMPLER.lock().await;
    if let Ok(content) = fs::read_to_string("/proc/stat").await
        && let Some(current) = content.lines().next().and_then(CpuTimes::parse)
    {
        return sampler.observe(current);
    }
    // A failed read must not reset the last valid baseline or fake idle CPU.
    sampler.usage
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
    if let Ok(content) = fs::read_to_string("/proc/uptime").await
        && let Some(first) = content.split_whitespace().next()
    {
        return first
            .split('.')
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
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

#[cfg(test)]
mod tests {
    use super::{CpuTimes, CpuUsageSampler};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    fn times(line: &str) -> CpuTimes {
        CpuTimes::parse(line).expect("valid CPU fixture")
    }

    fn assert_percent(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 0.000_001,
            "{actual} != {expected}"
        );
    }

    #[test]
    fn cpu_iowait_is_not_compute_usage() {
        let previous = times("cpu 100 20 30 1000 200 4 6 0");
        let current = times("cpu 102 20 33 1010 285 4 6 0");
        assert_percent(current.usage_since(previous).unwrap(), 5.0);

        let busy = times("cpu 190 20 40 1000 200 4 6 0");
        assert_percent(busy.usage_since(previous).unwrap(), 100.0);

        let idle = times("cpu 100 20 30 1010 290 4 6 0");
        assert_percent(idle.usage_since(previous).unwrap(), 0.0);
    }

    #[test]
    fn cpu_accounts_for_steal_without_double_counting_guests() {
        assert_eq!(
            times("cpu 100 20 30 1000 200 4 6 40 80 10"),
            CpuTimes {
                busy: 200,
                idle: 1000,
                iowait: 200,
            }
        );
        let previous = times("cpu 0 0 0 0 0 0 0 0");
        let current = times("cpu 10 0 0 80 0 0 0 10 10 0");
        assert_percent(current.usage_since(previous).unwrap(), 20.0);
    }

    #[test]
    fn cpu_parser_rejects_invalid_counters_and_overflow() {
        for line in [
            "",
            "cpu0 1 2 3 4",
            "cpu 1 2 3",
            "cpu 1 nope 3 4",
            "cpu 1 2 3 4 nope",
            "cpu 18446744073709551615 1 0 0",
            "cpu 18446744073709551616 0 0 0",
        ] {
            assert!(CpuTimes::parse(line).is_none(), "accepted: {line}");
        }
        assert_eq!(times("cpu 1 2 3 4").busy, 6);
    }

    #[test]
    fn decreasing_iowait_does_not_reset_or_manufacture_cpu_pressure() {
        let previous = times("cpu 100 0 0 1000 100");
        let current = times("cpu 110 0 0 1090 90");
        assert_percent(current.usage_since(previous).unwrap(), 10.0);
    }

    #[test]
    fn cpu_counter_resets_reseed_instead_of_reporting_saturation() {
        let mut sampler = CpuUsageSampler::default();
        assert_percent(sampler.observe(times("cpu 100 0 0 1000 100")), 0.0);
        assert_percent(sampler.observe(times("cpu 190 0 0 1010 100")), 90.0);
        assert_percent(sampler.observe(times("cpu 1 0 0 10 1")), 0.0);
        assert_percent(sampler.observe(times("cpu 11 0 0 100 1")), 10.0);
        // An idle-counter reset must also reseed, even if busy kept growing.
        assert_percent(sampler.observe(times("cpu 20 0 0 1 1")), 0.0);
    }

    #[test]
    fn cpu_sub_jiffy_reads_preserve_the_last_valid_usage() {
        let mut sampler = CpuUsageSampler::default();
        let previous = times("cpu 100 0 0 1000 100");
        let current = times("cpu 150 0 0 1050 100");
        assert_percent(sampler.observe(previous), 0.0);
        assert_percent(sampler.observe(current), 50.0);
        assert_percent(sampler.observe(current), 50.0);
    }

    #[tokio::test]
    async fn concurrent_samplers_update_one_coherent_counter_pair() {
        let sampler = Arc::new(Mutex::new(CpuUsageSampler::default()));
        sampler.lock().await.observe(times("cpu 0 0 0 0 0"));
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..32 {
            let sampler = Arc::clone(&sampler);
            tasks.spawn(async move {
                let mut sampler = sampler.lock().await;
                let previous = sampler.previous.unwrap();
                tokio::task::yield_now().await;
                sampler.observe(CpuTimes {
                    busy: previous.busy + 1,
                    idle: previous.idle + 9,
                    iowait: previous.iowait,
                })
            });
        }
        while let Some(result) = tasks.join_next().await {
            assert_percent(result.expect("sampling task"), 10.0);
        }
        assert_eq!(sampler.lock().await.previous.unwrap().busy, 32);
    }
}
