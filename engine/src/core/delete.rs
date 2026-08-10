use serde::{Deserialize, Serialize};

/// Request shared by the soft-delete and recovery batch API surfaces.
///
/// N1 3.0 intentionally exposes no permanent-delete batch request. Trash
/// reclamation belongs exclusively to the configured TTL lifecycle.
#[derive(Debug, Deserialize)]
pub struct BulkObjectKeysRequest {
    pub object_keys: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct BulkObjectFailure {
    pub object_key: String,
    pub error: String,
}

#[derive(Debug, Serialize)]
pub struct BulkObjectOperationResponse {
    pub requested: usize,
    pub unique: usize,
    pub succeeded: usize,
    pub not_found: usize,
    pub failed: Vec<BulkObjectFailure>,
}
