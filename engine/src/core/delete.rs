use serde::{Deserialize, Serialize};

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
