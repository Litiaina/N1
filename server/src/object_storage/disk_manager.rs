use crate::object_storage::constants::{
    METADATA_EXT, PREFIX_INDEX_DIR, PREFIX_INDEX_NAMESPACE, PREFIX_INDEX_VERSION,
    PREFIX_SEGMENT_EXT, QUOTA_FILE,
};
use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::OnceLock;

pub fn default_format() -> String {
    "zip".to_string()
}

pub static INDEX_STORE_DISKS: OnceLock<Vec<String>> = OnceLock::new();
pub static BLOB_STORE_DISKS: OnceLock<Vec<String>> = OnceLock::new();
pub static FS_STORAGE_ROOT: OnceLock<String> = OnceLock::new();

pub fn default_stripe_size() -> usize {
    256 * 1024
}
pub struct ObjectPathManager;

impl ObjectPathManager {
    pub fn new() -> Self {
        Self
    }

    pub fn get_all_index_roots(&self) -> Vec<PathBuf> {
        let roots = INDEX_STORE_DISKS
            .get()
            .expect("Index Store Config not initialized!");
        roots.iter().map(PathBuf::from).collect()
    }

    pub fn get_blob_roots(&self) -> Vec<PathBuf> {
        let roots = BLOB_STORE_DISKS
            .get()
            .expect("Blob Store Config not initialized!");
        roots.iter().map(PathBuf::from).collect()
    }

    pub fn get_all_index_dirs(&self, fragment_hash: &str) -> Vec<PathBuf> {
        self.get_all_index_roots()
            .iter()
            .map(|root| root.join(fragment_hash).join("index"))
            .collect()
    }

    pub fn get_all_session_dirs(&self, fragment_hash: &str, version_id: &str) -> Vec<PathBuf> {
        self.get_all_index_roots()
            .iter()
            .map(|root| {
                root.join(fragment_hash)
                    .join("index")
                    .join("sessions")
                    .join(version_id)
            })
            .collect()
    }

    pub fn object_key_hash(object_key: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(object_key.as_bytes());
        hex::encode(hasher.finalize())
    }

    pub fn canonical_prefix(prefix: &str) -> String {
        prefix.trim_start_matches('/').to_string()
    }

    pub fn prefix_hash(prefix: &str) -> String {
        let p = Self::canonical_prefix(prefix);
        let mut hasher = Sha256::new();
        hasher.update(p.as_bytes());
        hex::encode(hasher.finalize())
    }

    pub fn get_all_metadata_paths(
        &self,
        fragment_hash: &str,
        object_key: &str,
        version_id: &str,
    ) -> Vec<PathBuf> {
        let key_hash = Self::object_key_hash(object_key);

        let shard1 = &key_hash[0..2];
        let shard2 = &key_hash[2..4];
        let shard3 = &key_hash[4..6];
        let shard4 = &key_hash[6..8];
        let filename = format!("{}#{}{}", key_hash, version_id, METADATA_EXT);

        self.get_all_index_roots()
            .iter()
            .map(|root| {
                root.join(fragment_hash)
                    .join("index")
                    .join(shard1)
                    .join(shard2)
                    .join(shard3)
                    .join(shard4)
                    .join(&filename)
            })
            .collect()
    }

    pub fn get_all_prefix_bucket_dirs(&self, fragment_hash: &str, prefix: &str) -> Vec<PathBuf> {
        let ph = Self::prefix_hash(prefix);
        let p1 = &ph[0..2];
        let p2 = &ph[2..4];

        self.get_all_index_roots()
            .iter()
            .map(|root| {
                root.join(fragment_hash)
                    .join("index")
                    .join(PREFIX_INDEX_DIR)
                    .join(PREFIX_INDEX_VERSION)
                    .join(PREFIX_INDEX_NAMESPACE)
                    .join(p1)
                    .join(p2)
                    .join(&ph)
            })
            .collect()
    }

    pub fn get_all_prefix_segment_paths(
        &self,
        fragment_hash: &str,
        prefix: &str,
        segment_id: &str,
    ) -> Vec<PathBuf> {
        let file = format!("seg_{}{}", segment_id, PREFIX_SEGMENT_EXT);
        self.get_all_prefix_bucket_dirs(fragment_hash, prefix)
            .into_iter()
            .map(|d| d.join(&file))
            .collect()
    }

    pub fn get_all_prefix_compacted_paths(
        &self,
        fragment_hash: &str,
        prefix: &str,
    ) -> Vec<PathBuf> {
        self.get_all_prefix_bucket_dirs(fragment_hash, prefix)
            .into_iter()
            .map(|d| d.join(format!("compacted{}", PREFIX_SEGMENT_EXT)))
            .collect()
    }

    pub fn get_shard_dir_on_disk(
        &self,
        disk_root: &PathBuf,
        fragment_hash: &str,
        version_id: &str,
    ) -> PathBuf {
        disk_root.join(fragment_hash).join("blob").join(version_id)
    }

    pub fn get_all_quota_paths(&self, fragment_hash: &str) -> Vec<PathBuf> {
        self.get_all_index_roots()
            .iter()
            .map(|root| root.join(fragment_hash).join(QUOTA_FILE))
            .collect()
    }
}

#[derive(Debug, Serialize)]
pub struct ListObjectsResult {
    pub objects: Vec<ObjectMetadata>,
    #[serde(default)]
    pub common_prefixes: Vec<String>,
    pub continuation_token: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ReservationReceipt {
    pub amount: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ShardInfo {
    pub shard_index: usize,
    pub file_name: String,
    pub absolute_path: String,
    pub size_bytes: u64,
    pub checksum: String,
    pub is_parity: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PartMetadata {
    pub part_index: u64,
    pub size_bytes: u64,
    pub created_at: i64,
    pub shards: Vec<ShardInfo>,
    #[serde(default)]
    pub checksum: Option<String>,
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
    #[serde(default = "default_stripe_size")]
    pub stripe_size_bytes: usize,
}

#[derive(Deserialize)]
pub struct ObjectSummary {
    pub object_key: String,
    pub fragment: String,
    pub version_id: String,
    pub upload_session_id: String,
    pub owner_uid: String,
    pub content_type: String,
    pub total_size_bytes: u64,
    pub created_at: i64,
    #[serde(rename = "parts")]
    pub _parts_ignored: IgnoredAny,
    #[serde(default)]
    pub is_delete_marker: bool,
    #[serde(default = "default_stripe_size")]
    pub stripe_size_bytes: usize,
}

impl From<ObjectSummary> for ObjectMetadata {
    fn from(summary: ObjectSummary) -> Self {
        ObjectMetadata {
            object_key: summary.object_key,
            fragment: summary.fragment,
            version_id: summary.version_id,
            upload_session_id: summary.upload_session_id,
            owner_uid: summary.owner_uid,
            content_type: summary.content_type,
            total_size_bytes: summary.total_size_bytes,
            created_at: summary.created_at,
            parts: Vec::new(),
            is_delete_marker: summary.is_delete_marker,
            stripe_size_bytes: summary.stripe_size_bytes,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PrefixIndexEntry {
    pub object_key: String,
    pub version_id: String,
    pub created_at: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PrefixIndexSegment {
    pub prefix: String,
    pub entries: Vec<PrefixIndexEntry>,
}

#[derive(Deserialize)]
pub struct InitiateUploadParams {
    pub content_type: Option<String>,
    pub total_size_bytes: u64,
    pub expected_parts: u64,
}

#[derive(Serialize)]
pub struct InitiateUploadResponse {
    pub session_id: String,
    pub version_id: String,
}

#[derive(Serialize)]
pub struct CapacityUsageResponse {
    pub capacity_used: u64,
    pub capacity_allocated: u64,
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
    pub checksum: Option<String>,
    #[serde(default)]
    pub durability: Option<String>,
    #[serde(default)]
    pub strict_durability: Option<bool>,
    #[serde(default)]
    pub io_gate: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Quota {
    pub allocated_storage: u64,
    pub storage_used: u64,
}

#[derive(serde::Deserialize)]
pub struct DownloadArchiveRequest {
    #[serde(default)]
    pub keys: Vec<String>,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub delimiter: Option<String>,
    #[serde(default)]
    pub max_keys: Option<usize>,
    #[serde(default)]
    pub continuation_token: Option<String>,
    #[serde(default = "default_format")]
    pub format: String,
    #[serde(default)]
    pub filename: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct FinalizeIncompleteResponse {
    pub uploaded_parts: Vec<u64>,
    pub missing_parts: Vec<u64>,
    pub expected_parts: u64,
}
