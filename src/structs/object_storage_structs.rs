use crate::enums::upload_status_enum::UploadStatus;
use crate::fs::fs_constants::{CHUNK_PREFIX, OBJECT_METADATA_FILENAME, TEMP_UPLOAD_ROOT};
use crate::object_storage::constants::OBJECT_STORAGE_ROOT;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub struct ObjectPathManager<'a> {
    pub file_id: &'a str,
}

impl<'a> ObjectPathManager<'a> {
    pub fn new(file_id: &'a str) -> Self {
        Self { file_id }
    }

    pub fn temp_upload_dir(&self) -> PathBuf {
        PathBuf::from(OBJECT_STORAGE_ROOT)
            .join(TEMP_UPLOAD_ROOT)
            .join(self.file_id)
    }

    pub fn chunk_path(&self, index: u64) -> PathBuf {
        self.temp_upload_dir()
            .join(format!("{}{}", CHUNK_PREFIX, index))
    }

    pub fn metadata_path(&self) -> PathBuf {
        self.temp_upload_dir().join(OBJECT_METADATA_FILENAME)
    }
}

#[derive(Serialize)]
pub struct CapacityUsageResponse {
    pub capacity_used: u64,
}

#[derive(Deserialize, Debug)]
pub struct TotalStorageUsage {
    pub total_usage: u64,
}

#[derive(Deserialize, Debug)]
pub struct UserAuthDetails {
    pub allocated_storage: u64,
}

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

#[derive(Deserialize)]
pub struct RenameObjectRequest {
    pub source_fragment: String,
    pub source_key: String,
    pub destination_fragment: String,
    pub destination_key: String,
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
    pub status: UploadStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
#[serde(rename_all = "lowercase")]
pub enum SignedUrlDisposition {
    #[default]
    View,
    Stream,
    Download,
}

#[derive(Deserialize)]
pub struct CreateSignedUrlRequest {
    pub fragment: String,
    pub object_key: String,
    pub expires_in: u64,
    #[serde(default)]
    pub disposition: SignedUrlDisposition,
}

#[derive(Serialize)]
pub struct SignedUrlResponse {
    pub url: String,
    pub expires_at: String,
}

#[derive(Deserialize)]
pub struct DeleteByPrefixRequest {
    pub fragment: String,
    pub prefix: String,
}
