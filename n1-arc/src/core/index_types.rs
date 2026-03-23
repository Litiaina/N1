use crate::core::disk_manager::{PartMetadata, ReservationReceipt};
use crate::core::upload::UploadSessionManifest;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexReplicaWriteMode {
    BackgroundBestEffort,
    SyncBestEffort,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSessionState {
    pub manifest: UploadSessionManifest,
    #[serde(default)]
    pub reservation: Option<ReservationReceipt>,
    #[serde(default)]
    pub parts: BTreeMap<u64, PartMetadata>,
    #[serde(default)]
    pub last_activity_at: i64,
    #[serde(default)]
    pub finalized_at: Option<i64>,
    #[serde(default)]
    pub aborted_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionPartReceipt {
    pub part_index: u64,
    pub size_bytes: u64,
    pub created_at: i64,
    pub checksum: Option<u32>,
    pub shard_checksums: Vec<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemovedSessionReceipt {
    pub state: IndexSessionState,
    #[serde(default)]
    pub session_part_receipts: BTreeMap<u64, SessionPartReceipt>,
    pub removed_at: i64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct UploadRuntimeStats {
    pub active_fragments: u64,
    pub active_sessions: u64,
    pub reserved_bytes: u64,
    pub committed_parts: u64,
    pub committed_bytes: u64,
    pub oldest_session_age_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct IndexRuntimeStats {
    pub loaded_partition_runtimes: u64,
    pub wal_appenders: u64,
    pub prefix_projection_fragments_pending: u64,
}
