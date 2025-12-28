use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::OnceLock;

use crate::object_storage::constants::OBJECT_DATA_ROOT;

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

    pub fn get_user_dir(&self, uid: &str) -> PathBuf {
        self.root
            .join(OBJECT_DATA_ROOT)
            .join(uid)
    }

    pub fn get_blob_dir(&self, uid: &str, fragment: &str, version_id: &str) -> PathBuf {
        self.get_user_dir(uid)
            .join(fragment)
            .join("blobs")
            .join(version_id)
    }

    pub fn get_index_dir(&self, uid: &str, fragment: &str) -> PathBuf {
        self.get_user_dir(uid)
            .join(fragment)
            .join("index")
    }

    pub fn get_metadata_path(&self, uid: &str, fragment: &str, object_key: &str, version_id: &str) -> PathBuf {
        let mut hasher = Sha256::new();
        hasher.update(object_key.as_bytes());
        let hash = hex::encode(hasher.finalize());

        let shard1 = &hash[0..2];
        let shard2 = &hash[2..4];
        
        let filename = format!("{}#{}.bson", hash, version_id);
        
        self.get_index_dir(uid, fragment)
            .join(shard1)
            .join(shard2)
            .join(filename)
    }
}

#[derive(Serialize, Deserialize)]
pub struct ReservationReceipt {
    pub amount: u64,
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
    #[serde(default)] 
    pub is_delete_marker: bool, 
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
    pub version_id: Option<String>,
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

#[derive(Deserialize)]
pub struct RenameFragmentQuery {
    pub fragment_to_rename: String,
    pub new_name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Quota {
    pub allocated_storage: u64,
    pub storage_used: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UserRecord {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<mongodb::bson::oid::ObjectId>,
    pub uid: String,
    pub email: String,
    #[serde(rename = "password")] 
    pub password_hash: String, 
    pub allocated_storage: u64,
    #[serde(default)]
    pub created_at: i64,
}
