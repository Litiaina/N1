use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
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
