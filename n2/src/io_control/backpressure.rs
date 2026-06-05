use crate::enums::app_error_enum::AppError;
use crate::io_control::stall::{ProgressTracker, StallSnapshot};
use once_cell::sync::Lazy;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

static PEERS: Lazy<Mutex<HashMap<String, Arc<PeerIoState>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

fn env_usize(key: &str, default_value: usize, min_value: usize, max_value: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(default_value)
        .clamp(min_value, max_value)
}

fn peer_write_limit() -> usize {
    env_usize("N2_PEER_MAX_ACTIVE_WRITES", 64, 1, 4096)
}

fn peer_read_limit() -> usize {
    env_usize("N2_PEER_MAX_ACTIVE_READS", 128, 1, 4096)
}

fn stale_after() -> Duration {
    let secs = env_usize("N2_PEER_STALL_SECS", 20, 2, 600) as u64;
    Duration::from_secs(secs)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PeerOperation {
    Read,
    Write,
}

#[derive(Debug)]
pub struct PeerIoState {
    node_id: String,
    active_reads: AtomicUsize,
    active_writes: AtomicUsize,
    completed_reads: AtomicU64,
    completed_writes: AtomicU64,
    failed_reads: AtomicU64,
    failed_writes: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    stall_events: AtomicU64,
    progress: ProgressTracker,
}

impl PeerIoState {
    fn new(node_id: String) -> Self {
        Self {
            node_id,
            active_reads: AtomicUsize::new(0),
            active_writes: AtomicUsize::new(0),
            completed_reads: AtomicU64::new(0),
            completed_writes: AtomicU64::new(0),
            failed_reads: AtomicU64::new(0),
            failed_writes: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            stall_events: AtomicU64::new(0),
            progress: ProgressTracker::new(),
        }
    }

    fn snapshot(&self) -> PeerIoSnapshot {
        let stall = self.progress.snapshot(stale_after());
        let active_reads = self.active_reads.load(Ordering::Relaxed);
        let active_writes = self.active_writes.load(Ordering::Relaxed);
        let read_limit = peer_read_limit();
        let write_limit = peer_write_limit();
        let read_pressure_percent = pressure_percent(active_reads, read_limit);
        let write_pressure_percent = pressure_percent(active_writes, write_limit);
        PeerIoSnapshot {
            node_id: self.node_id.clone(),
            active_reads,
            active_writes,
            read_limit,
            write_limit,
            read_pressure_percent,
            write_pressure_percent,
            pressure_state: pressure_state(
                read_pressure_percent,
                write_pressure_percent,
                stall.stalled,
            ),
            completed_reads: self.completed_reads.load(Ordering::Relaxed),
            completed_writes: self.completed_writes.load(Ordering::Relaxed),
            failed_reads: self.failed_reads.load(Ordering::Relaxed),
            failed_writes: self.failed_writes.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            stall_events: self.stall_events.load(Ordering::Relaxed),
            stalled: stall.stalled,
            stall,
        }
    }
}

fn pressure_percent(active: usize, limit: usize) -> f64 {
    if limit == 0 {
        return 0.0;
    }
    ((active as f64 / limit as f64) * 100.0).min(100.0)
}

fn pressure_state(read_percent: f64, write_percent: f64, stalled: bool) -> String {
    if stalled {
        return "stalled".to_string();
    }
    let pressure = read_percent.max(write_percent);
    if pressure >= 95.0 {
        "saturated".to_string()
    } else if pressure >= 75.0 {
        "pressured".to_string()
    } else if pressure > 0.0 {
        "normal".to_string()
    } else {
        "idle".to_string()
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct PeerIoSnapshot {
    pub node_id: String,
    pub active_reads: usize,
    pub active_writes: usize,
    pub read_limit: usize,
    pub write_limit: usize,
    pub read_pressure_percent: f64,
    pub write_pressure_percent: f64,
    pub pressure_state: String,
    pub completed_reads: u64,
    pub completed_writes: u64,
    pub failed_reads: u64,
    pub failed_writes: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub stall_events: u64,
    pub stalled: bool,
    pub stall: StallSnapshot,
}

pub struct PeerIoPermit {
    state: Arc<PeerIoState>,
    operation: PeerOperation,
    completed: bool,
}

impl PeerIoPermit {
    pub fn record_progress(&self, bytes: u64) {
        record_progress(&self.state, self.operation, bytes);
    }

    pub fn complete(mut self) {
        match self.operation {
            PeerOperation::Read => self.state.completed_reads.fetch_add(1, Ordering::Relaxed),
            PeerOperation::Write => self.state.completed_writes.fetch_add(1, Ordering::Relaxed),
        };
        self.completed = true;
    }
}

impl Drop for PeerIoPermit {
    fn drop(&mut self) {
        match self.operation {
            PeerOperation::Read => {
                self.state.active_reads.fetch_sub(1, Ordering::Relaxed);
                if !self.completed {
                    self.state.failed_reads.fetch_add(1, Ordering::Relaxed);
                }
            }
            PeerOperation::Write => {
                self.state.active_writes.fetch_sub(1, Ordering::Relaxed);
                if !self.completed {
                    self.state.failed_writes.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

pub fn acquire_peer_operation(
    node_id: &str,
    operation: PeerOperation,
) -> Result<PeerIoPermit, AppError> {
    let limit = match operation {
        PeerOperation::Read => peer_read_limit(),
        PeerOperation::Write => peer_write_limit(),
    };
    acquire_peer_operation_with_limit(node_id, operation, limit)
}

fn acquire_peer_operation_with_limit(
    node_id: &str,
    operation: PeerOperation,
    limit: usize,
) -> Result<PeerIoPermit, AppError> {
    let state = peer_state(node_id)?;
    let (active, label) = match operation {
        PeerOperation::Read => (&state.active_reads, "read peer shard operation"),
        PeerOperation::Write => (&state.active_writes, "write peer shard operation"),
    };

    loop {
        let current = active.load(Ordering::Relaxed);
        if current >= limit {
            return Err(AppError::ServiceUnavailable(format!(
                "N2 peer {} is saturated for {} (active={}, limit={})",
                node_id, label, current, limit
            )));
        }
        if active
            .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            return Ok(PeerIoPermit {
                state,
                operation,
                completed: false,
            });
        }
    }
}

pub fn record_peer_progress(node_id: &str, operation: PeerOperation, bytes: u64) {
    if let Ok(state) = peer_state(node_id) {
        record_progress(&state, operation, bytes);
    }
}

pub fn record_peer_failure(node_id: &str, operation: PeerOperation) {
    if let Ok(state) = peer_state(node_id) {
        match operation {
            PeerOperation::Read => state.failed_reads.fetch_add(1, Ordering::Relaxed),
            PeerOperation::Write => state.failed_writes.fetch_add(1, Ordering::Relaxed),
        };
    }
}

fn record_progress(state: &PeerIoState, operation: PeerOperation, bytes: u64) {
    if bytes == 0 {
        return;
    }
    match operation {
        PeerOperation::Read => state.bytes_read.fetch_add(bytes, Ordering::Relaxed),
        PeerOperation::Write => state.bytes_written.fetch_add(bytes, Ordering::Relaxed),
    };
    state.progress.record(bytes);
}

pub fn peer_io_snapshot() -> Vec<PeerIoSnapshot> {
    let Ok(peers) = PEERS.lock() else {
        return Vec::new();
    };
    let mut out = peers
        .values()
        .map(|state| state.snapshot())
        .collect::<Vec<_>>();
    out.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    out
}

fn peer_state(node_id: &str) -> Result<Arc<PeerIoState>, AppError> {
    let node_id = node_id.trim();
    if node_id.is_empty() {
        return Err(AppError::InternalServerError(
            "N2 peer operation missing node id".into(),
        ));
    }
    let mut peers = PEERS
        .lock()
        .map_err(|_| AppError::InternalServerError("N2 peer IO registry poisoned".into()))?;
    Ok(peers
        .entry(node_id.to_string())
        .or_insert_with(|| Arc::new(PeerIoState::new(node_id.to_string())))
        .clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_pressure_rejects_after_configured_limit() {
        let node = format!("test-node-{}", ulid::Ulid::new());
        let permit = acquire_peer_operation_with_limit(&node, PeerOperation::Write, 1)
            .expect("first permit");
        let rejected = acquire_peer_operation_with_limit(&node, PeerOperation::Write, 1);
        assert!(rejected.is_err());
        drop(permit);
        acquire_peer_operation_with_limit(&node, PeerOperation::Write, 1).expect("permit released");
    }
}
