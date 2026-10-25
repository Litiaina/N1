use crate::distribution::repair::{ReadRepairSnapshot, read_repair_snapshot};
use crate::io_control::backpressure::{PeerIoSnapshot, peer_io_snapshot};
use crate::io_control::capacity_cache::{CapacityCacheSnapshot, capacity_cache_snapshot};
use crate::io_control::job::{IoJobRegistrySnapshot, io_job_registry_snapshot};
use crate::io_control::latency::{IoLatencySnapshot, io_latency_snapshot};
use crate::io_control::resource::{ProcessResourceSnapshot, process_resource_snapshot};
use crate::io_control::scrub::{ScrubControlSnapshot, scrub_control_snapshot};
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};

static READ_RECOVER_EVENTS: AtomicU64 = AtomicU64::new(0);
static READ_RECOVER_BYTES: AtomicU64 = AtomicU64::new(0);
static READ_RECOVER_MISSING_SHARDS: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Serialize)]
pub struct IoControlSnapshot {
    pub peer_io: Vec<PeerIoSnapshot>,
    pub jobs: IoJobRegistrySnapshot,
    pub capacity_cache: CapacityCacheSnapshot,
    pub latency: IoLatencySnapshot,
    pub process: ProcessResourceSnapshot,
    pub scrub: ScrubControlSnapshot,
    pub read_recover: ReadRecoverSnapshot,
    pub read_repair: ReadRepairSnapshot,
    pub peer_count: usize,
    pub active_peer_reads: usize,
    pub active_peer_writes: usize,
    pub stalled_peers: usize,
    pub pressured_peers: usize,
    pub saturated_peers: usize,
    pub total_peer_bytes_read: u64,
    pub total_peer_bytes_written: u64,
    pub total_peer_read_failures: u64,
    pub total_peer_write_failures: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ReadRecoverSnapshot {
    pub events: u64,
    pub bytes: u64,
    pub missing_shards_observed: u64,
}

pub fn record_read_recover(bytes: u64, missing_shards: u64) {
    READ_RECOVER_EVENTS.fetch_add(1, Ordering::Relaxed);
    READ_RECOVER_BYTES.fetch_add(bytes, Ordering::Relaxed);
    READ_RECOVER_MISSING_SHARDS.fetch_add(missing_shards, Ordering::Relaxed);
}

fn read_recover_snapshot() -> ReadRecoverSnapshot {
    ReadRecoverSnapshot {
        events: READ_RECOVER_EVENTS.load(Ordering::Relaxed),
        bytes: READ_RECOVER_BYTES.load(Ordering::Relaxed),
        missing_shards_observed: READ_RECOVER_MISSING_SHARDS.load(Ordering::Relaxed),
    }
}

pub fn io_control_snapshot() -> IoControlSnapshot {
    let peer_io = peer_io_snapshot();
    IoControlSnapshot {
        peer_count: peer_io.len(),
        active_peer_reads: peer_io.iter().map(|peer| peer.active_reads).sum(),
        active_peer_writes: peer_io.iter().map(|peer| peer.active_writes).sum(),
        stalled_peers: peer_io.iter().filter(|peer| peer.stalled).count(),
        pressured_peers: peer_io
            .iter()
            .filter(|peer| peer.pressure_state == "pressured")
            .count(),
        saturated_peers: peer_io
            .iter()
            .filter(|peer| peer.pressure_state == "saturated")
            .count(),
        total_peer_bytes_read: peer_io.iter().map(|peer| peer.bytes_read).sum(),
        total_peer_bytes_written: peer_io.iter().map(|peer| peer.bytes_written).sum(),
        total_peer_read_failures: peer_io.iter().map(|peer| peer.failed_reads).sum(),
        total_peer_write_failures: peer_io.iter().map(|peer| peer.failed_writes).sum(),
        jobs: io_job_registry_snapshot(),
        capacity_cache: capacity_cache_snapshot(),
        latency: io_latency_snapshot(),
        process: process_resource_snapshot(),
        scrub: scrub_control_snapshot(),
        read_recover: read_recover_snapshot(),
        read_repair: read_repair_snapshot(),
        peer_io,
    }
}
