use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::OnceLock;

pub static OBJECT_STORAGE_ROOT: OnceLock<String> = OnceLock::new();

pub struct ObjectPathManager {
    pub root: PathBuf,
}

impl ObjectPathManager {
    pub fn new() -> Self {
        let root = OBJECT_STORAGE_ROOT
            .get()
            .expect("Config not initialized!")
            .clone();
        Self {
            root: PathBuf::from(root),
        }
    }

    pub fn get_object_dir(&self, uid: &str, fragment: &str, key: &str) -> PathBuf {
        self.root
            .join("storage")
            .join(uid)
            .join(fragment)
            .join(key)
    }

    pub fn get_version_dir(&self, uid: &str, fragment: &str, key: &str, version_id: &str) -> PathBuf {
        self.get_object_dir(uid, fragment, key).join(version_id)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PartMetadata {
    pub part_index: u64,
    pub size_bytes: u64,
    pub file_name: String,
    pub created_at: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ObjectMetadata {
    pub object_key: String,
    pub fragment: String,
    pub version_id: String,
    pub upload_session_id: String,
    pub owner_uid: String,
    pub content_type: String,
    pub total_size_bytes: u64,
    pub created_at: i64,
    pub parts: Vec<PartMetadata>,
}

#[derive(Deserialize)]
pub struct InitiateUploadParams {
    pub content_type: Option<String>,
    pub total_size_bytes: u64,
}

#[derive(Serialize)]
pub struct InitiateUploadResponse {
    pub session_id: String,
    pub version_id: String,
}

#[derive(Serialize)]
pub struct CapacityUsageResponse {
    pub capacity_used: u64,
}

#[derive(Deserialize, Debug)]
pub struct UserAuthDetails {
    pub allocated_storage: u64,
    #[serde(default)] 
    pub storage_used: u64, 
}

#[derive(Deserialize)]
pub struct RenameObjectRequest {
    pub source_fragment: String,
    pub source_key: String,
    pub destination_fragment: String,
    pub destination_key: String,
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
pub struct ObjectKeyQuery {
    pub object_key: String,
}

#[derive(Serialize)]
pub struct UploadStatusResponse {
    pub uploaded_parts: Vec<u64>,
}

#[derive(Deserialize)]
pub struct ChunkQuery {
    pub object_key: String,
    pub offset: u64,
    pub chunk_size: usize,
}

#[derive(Deserialize)]
pub struct UploadPartQuery {
    pub object_key: String,
    pub buffer_size: Option<usize>,
}