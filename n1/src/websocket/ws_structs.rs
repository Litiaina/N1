use serde::{Deserialize, Serialize};

use crate::core::{
    disk_manager::ObjectMetadata, redistribution::RedistributionJobStatus,
    repair_manager::RepairJobStatus,
};

#[derive(Deserialize)]
pub struct WsAuthParams {
    pub token: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "op", content = "d")]
pub enum ServerMessage {
    #[serde(rename = "HELLO")]
    Hello { connection_id: String },

    #[serde(rename = "PONG")]
    Pong,

    #[serde(rename = "EVENT")]
    Event(StorageEvent),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op", content = "d")]
pub enum ClientMessage {
    #[serde(rename = "PING")]
    Ping,
}

#[derive(Debug, Clone, Serialize)]
pub struct SmbTransferProgressPayload {
    pub transfer_id: String,
    pub fragment: String,
    pub object_key: String,
    pub phase: String,
    pub state: String,
    pub ingress_bytes: u64,
    pub expected_bytes: Option<u64>,
    pub started_at_ms: u64,
    pub updated_at_ms: u64,
    pub completed_at_ms: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", content = "data")]
pub enum StorageEventPayload {
    #[serde(rename = "FILE_UPLOADED")]
    FileUploaded(ObjectMetadata),

    #[serde(rename = "FILE_DELETED")]
    FileDeleted {
        fragment: String,
        object_key: String,
        version_id: Option<String>,
    },

    #[serde(rename = "FILE_RENAMED")]
    FileRenamed {
        source_fragment: String,
        source_key: String,
        destination_fragment: String,
        destination_key: String,
    },

    #[serde(rename = "FRAGMENT_DELETED")]
    FragmentDeleted { fragment: String },

    #[serde(rename = "SYSTEM_MESSAGE")]
    SystemMessage { message: String, severity: String },

    #[serde(rename = "STATE_INVALIDATED")]
    StateInvalidated,

    #[serde(rename = "MONITORING_UPDATE")]
    MonitoringUpdate {
        system: serde_json::Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        uploads: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        namespace_runtime: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        maintenance: Option<serde_json::Value>,
        repair_job: Option<serde_json::Value>,
        redist_job: Option<serde_json::Value>,
        shadow_index_status: String,
        shadow_index_healthy: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        sampled_at: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        summary: Option<String>,
    },

    #[serde(rename = "REDISTRIBUTION_UPDATE")]
    RedistributionUpdate(RedistributionJobStatus),

    #[serde(rename = "REPAIR_UPDATE")]
    RepairUpdate(RepairJobStatus),
}

#[derive(Debug, Clone, Serialize)]
pub struct StorageEvent {
    pub timestamp_ms: u64,
    #[serde(flatten)]
    pub payload: StorageEventPayload,
}

#[derive(serde::Deserialize)]
pub struct BroadcastReq {
    pub message: String,
    pub severity: String,
}
