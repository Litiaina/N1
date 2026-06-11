use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Debug)]
pub struct ProgressTracker {
    started_ms: AtomicU64,
    last_progress_ms: AtomicU64,
    bytes: AtomicU64,
}

impl ProgressTracker {
    pub fn new() -> Self {
        let now = now_ms();
        Self {
            started_ms: AtomicU64::new(now),
            last_progress_ms: AtomicU64::new(now),
            bytes: AtomicU64::new(0),
        }
    }

    pub fn record(&self, bytes: u64) {
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.heartbeat();
    }

    pub fn heartbeat(&self) {
        self.last_progress_ms.store(now_ms(), Ordering::Relaxed);
    }

    pub fn snapshot(&self, stale_after: Duration) -> StallSnapshot {
        let now = now_ms();
        let started_ms = self.started_ms.load(Ordering::Relaxed);
        let last_progress_ms = self.last_progress_ms.load(Ordering::Relaxed);
        let idle_ms = now.saturating_sub(last_progress_ms);
        StallSnapshot {
            bytes: self.bytes.load(Ordering::Relaxed),
            age_ms: now.saturating_sub(started_ms),
            idle_ms,
            stalled: idle_ms >= stale_after.as_millis() as u64,
        }
    }
}

impl Default for ProgressTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct StallSnapshot {
    pub bytes: u64,
    pub age_ms: u64,
    pub idle_ms: u64,
    pub stalled: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_tracks_bytes_and_heartbeat() {
        let tracker = ProgressTracker::new();
        tracker.record(7);
        tracker.heartbeat();
        let snapshot = tracker.snapshot(Duration::from_secs(60));
        assert_eq!(snapshot.bytes, 7);
        assert!(!snapshot.stalled);
    }
}
