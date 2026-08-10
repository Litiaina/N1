use serde::Serialize;
use std::sync::LazyLock;
use std::time::Instant;

static STARTED: LazyLock<Instant> = LazyLock::new(Instant::now);

#[derive(Clone, Debug, Serialize)]
pub struct ProcessResourceSnapshot {
    pub pid: u32,
    pub uptime_seconds: u64,
    pub rss_bytes: u64,
    pub open_fds: Option<u64>,
    pub threads: Option<u64>,
}

#[cfg(target_os = "linux")]
fn read_linux_status() -> (u64, Option<u64>) {
    let Ok(text) = std::fs::read_to_string("/proc/self/status") else {
        return (0, None);
    };
    let mut rss_bytes = 0u64;
    let mut threads = None;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb = rest
                .split_whitespace()
                .next()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0);
            rss_bytes = kb.saturating_mul(1024);
        } else if let Some(rest) = line.strip_prefix("Threads:") {
            threads = rest
                .split_whitespace()
                .next()
                .and_then(|value| value.parse::<u64>().ok());
        }
    }
    (rss_bytes, threads)
}

#[cfg(not(target_os = "linux"))]
fn read_linux_status() -> (u64, Option<u64>) {
    (0, None)
}

#[cfg(target_os = "linux")]
fn open_fd_count() -> Option<u64> {
    std::fs::read_dir("/proc/self/fd")
        .ok()
        .map(|entries| entries.count() as u64)
}

#[cfg(not(target_os = "linux"))]
fn open_fd_count() -> Option<u64> {
    None
}

pub fn process_resource_snapshot() -> ProcessResourceSnapshot {
    let (rss_bytes, threads) = read_linux_status();
    ProcessResourceSnapshot {
        pid: std::process::id(),
        uptime_seconds: STARTED.elapsed().as_secs(),
        rss_bytes,
        open_fds: open_fd_count(),
        threads,
    }
}
