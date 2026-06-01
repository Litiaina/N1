use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct DiskStatus {
    pub path: String,
    pub role: String,
    pub status: String,
    #[serde(default)]
    pub total_bytes: Option<u64>,
    #[serde(default)]
    pub used_bytes: Option<u64>,
    #[serde(default)]
    pub available_bytes: Option<u64>,
    #[serde(default)]
    pub usage_percent: Option<f64>,
    #[serde(default)]
    pub latency_ms: Option<u64>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ConsistencyStats {
    #[serde(default, alias = "total_partition_dbs_scanned")]
    pub total_objects_scanned: u64,
    #[serde(default, alias = "index_partition_health")]
    pub index_mirror_health: String,
    #[serde(default, alias = "partition_db_counts")]
    pub mirror_counts: Option<HashMap<String, u64>>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct SystemMetrics {
    pub cpu_usage_percent: f64,
    #[serde(default)]
    pub cpu_cores: u64,
    pub ram_total_bytes: u64,
    pub ram_used_bytes: u64,
    pub ram_usage_percent: f64,
    pub load_avg_1m: f64,
    #[serde(default)]
    pub uptime_seconds: u64,
    #[serde(default)]
    pub process_resident_bytes: u64,
    #[serde(default)]
    pub process_virtual_bytes: u64,
    #[serde(default)]
    pub process_open_fds: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct UploadRuntimeStats {
    pub active_fragments: u64,
    pub active_sessions: u64,
    pub reserved_bytes: u64,
    pub committed_parts: u64,
    pub committed_bytes: u64,
    #[serde(default)]
    pub stale_sessions: u64,
    #[serde(default)]
    pub stale_reserved_bytes: u64,
    #[serde(default)]
    pub oldest_session_age_seconds: Option<u64>,
    #[serde(default)]
    pub oldest_stale_session_age_seconds: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct IndexRuntimeStats {
    #[serde(default)]
    pub loaded_partition_runtimes: u64,
    #[serde(default)]
    pub wal_appenders: u64,
    #[serde(default)]
    pub prefix_projection_fragments_pending: u64,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct StoragePlaneStats {
    pub index_roots_total: u64,
    pub index_roots_online: u64,
    pub blob_devices_total: u64,
    pub blob_devices_online: u64,
    pub raw_total_bytes: u64,
    pub raw_used_bytes: u64,
    pub raw_available_bytes: u64,
    #[serde(default)]
    pub heterogeneous_blob_capacity: bool,
    #[serde(default)]
    pub blob_capacity_imbalance_ratio: Option<f64>,
    #[serde(default)]
    pub placement_limiter_path: Option<String>,
    #[serde(default)]
    pub placement_limiter_available_bytes: Option<u64>,
    #[serde(default)]
    pub placement_limiter_usage_percent: Option<f64>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct MaintenanceRuntimeStats {
    #[serde(default)]
    pub access_mode: String,
    pub repair_running: bool,
    pub repair_phase: String,
    pub redistribution_running: bool,
    pub redistribution_phase: String,
    pub gc_running: bool,
    pub gc_phase: String,
    #[serde(default)]
    pub pending_redistribution_intents: u64,
    #[serde(default)]
    pub failed_redistribution_intents: u64,
    #[serde(default)]
    pub fragments_with_pending_redistribution: u64,
    #[serde(default)]
    pub oldest_pending_redistribution_intent_age_seconds: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ActiveSessionInfo {
    pub session_id: String,
    pub version_id: String,
    #[serde(default)]
    pub object_key: Option<String>,
    pub created_at: i64,
    pub reserved_size: u64,
    #[serde(default)]
    pub total_size_bytes: Option<u64>,
    #[serde(default)]
    pub expected_parts: Option<u64>,
    #[serde(default)]
    pub committed_parts: u64,
    #[serde(default)]
    pub committed_bytes: u64,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ListUploadsResponse {
    pub sessions: Vec<ActiveSessionInfo>,
    pub total: usize,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct UploadStatusResponse {
    pub uploaded_parts: Vec<u64>,
    #[serde(default)]
    pub committed_parts: u64,
    #[serde(default)]
    pub committed_bytes: u64,
    #[serde(default)]
    pub expected_parts: Option<u64>,
    #[serde(default)]
    pub total_size_bytes: Option<u64>,
    #[serde(default)]
    pub created_at: Option<i64>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub version_id: Option<String>,
    #[serde(default)]
    pub object_key: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct HealthResponse {
    #[serde(default)]
    pub sampled_at: i64,
    pub status: String,
    #[serde(default)]
    pub disks: Vec<DiskStatus>,
    #[serde(default)]
    pub consistency: ConsistencyStats,
    #[serde(default)]
    pub system: SystemMetrics,
    #[serde(default)]
    pub uploads: UploadRuntimeStats,
    #[serde(default)]
    pub index_runtime: IndexRuntimeStats,
    #[serde(default)]
    pub storage: StoragePlaneStats,
    #[serde(default)]
    pub maintenance: MaintenanceRuntimeStats,
    pub summary: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RepairJobStatus {
    pub is_running: bool,
    pub phase: String,
    pub total_scanned: u64,
    pub total_repaired: u64,
    pub corrupt_found: u64,
    pub missing_meta_repaired: u64,
    pub divergent_meta_repaired: u64,
    pub current_fragment: String,
    pub duration_seconds: u64,
    pub scan_rate_per_sec: f64,
    pub last_repaired: Option<String>,
    pub last_disagreement: Option<String>,
    pub last_error: Option<String>,
    pub start_timestamp: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RedistributionJobStatus {
    pub is_running: bool,
    pub phase: String,
    pub total_fragments_scanned: u64,
    pub total_objects_scanned: u64,
    pub total_objects_need_redistribution: u64,
    pub total_objects_migrated: u64,
    pub total_errors: u64,
    pub current_fragment: String,
    pub duration_seconds: u64,
    pub scan_rate_per_sec: f64,
    pub last_object: Option<String>,
    pub last_error: Option<String>,
    pub start_timestamp: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GcJobStatus {
    pub is_running: bool,
    pub phase: String,
    pub is_manual_run: bool,
    pub total_fragments: u64,
    pub total_fragments_scanned: u64,
    pub total_live_versions_seen: u64,
    pub total_trash_versions_expired: u64,
    pub total_sessions_deleted: u64,
    pub total_orphan_blob_dirs_deleted: u64,
    pub total_refund_bytes: u64,
    pub total_errors: u64,
    pub current_fragment: String,
    pub last_error: Option<String>,
    pub duration_seconds: u64,
    pub start_timestamp: i64,
}
