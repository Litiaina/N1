use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct InitiateUploadParams {
    pub content_type: Option<String>,
    pub total_size_bytes: u64,
    pub total_chunks: u64,
}

#[derive(Serialize)]
pub struct InitiateUploadResponse {
    pub file_id: String,
}

#[derive(Serialize)]
pub struct UploadStatusResponse {
    pub total_size_bytes: u64,
    pub uploaded_chunks: Vec<u64>,
    pub total_chunks: u64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TempUploadMetadata {
    pub file_id: String,
    pub fragment: String,
    pub object_key: String,
    pub uid: String,
    pub content_type: Option<String>,
    pub total_size_bytes: u64,
    pub total_chunks: u64,
    pub creation_timestamp: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ObjectMetadata {
    #[serde(rename = "_id")]
    pub id: String,
    pub fragment: String,
    pub key: String,
    pub version_id: String,
    pub is_latest: bool,
    pub size_bytes: u64,
    pub content_type: String,
    pub owner_uid: String,
    pub created_at: mongodb::bson::DateTime,
    pub last_modified: mongodb::bson::DateTime,
    pub etag: String,
    pub physical_location: String,
}
