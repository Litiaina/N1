use crate::core::constants::N2_BLOB_DIR;
use crate::enums::app_error_enum::AppError;
use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::OnceLock;

const INTENT_PARTITION_PREFIX: &str = "partition_";

pub fn default_format() -> String {
    "zip".to_string()
}

pub fn default_tar_format() -> String {
    "tar".to_string()
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

    fn route_hash(parts: &[&str]) -> [u8; 32] {
        let mut hasher = Sha256::new();
        for part in parts {
            hasher.update(part.as_bytes());
            hasher.update([0u8]);
        }
        hasher.finalize().into()
    }

    fn fragment_owner_index(fragment_hash: &str, root_count: usize) -> usize {
        if root_count <= 1 {
            return 0;
        }
        let hash = Self::route_hash(&["fragment", fragment_hash]);
        usize::from(u16::from(hash[0]) << 8 | u16::from(hash[1])) % root_count
    }

    fn partition_id(fragment_hash: &str, namespace: &str, key: &str) -> Result<u16, AppError> {
        let partition_count =
            crate::core::fragment_metadata::fragment_index_partition_count(fragment_hash)?;
        let hash = Self::route_hash(&[namespace, fragment_hash, key]);
        Ok((u16::from(hash[0]) << 8 | u16::from(hash[1])) % partition_count)
    }

    fn select_partition_roots(&self, fragment_hash: &str, partition_id: u16) -> Vec<PathBuf> {
        let roots = self.get_all_index_roots();
        if roots.is_empty() {
            return Vec::new();
        }

        let owner_idx = (Self::fragment_owner_index(fragment_hash, roots.len())
            + usize::from(partition_id))
            % roots.len();
        let mut selected = vec![roots[owner_idx].clone()];
        for (idx, root) in roots.into_iter().enumerate() {
            if idx != owner_idx {
                selected.push(root);
            }
        }
        selected
    }

    fn intent_partition_path(root: &PathBuf, fragment_hash: &str, partition_id: u16) -> PathBuf {
        root.join(fragment_hash).join(".intents").join(format!(
            "{}{:04x}.n2",
            INTENT_PARTITION_PREFIX, partition_id
        ))
    }

    pub fn get_all_session_intent_partition_paths(
        &self,
        fragment_hash: &str,
        version_id: &str,
    ) -> Result<Vec<PathBuf>, AppError> {
        let partition_id = Self::partition_id(fragment_hash, "session", version_id)?;
        Ok(self
            .select_partition_roots(fragment_hash, partition_id)
            .iter()
            .map(|root| Self::intent_partition_path(root, fragment_hash, partition_id))
            .collect())
    }

    pub fn object_key_hash(object_key: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(object_key.as_bytes());
        hex::encode(hasher.finalize())
    }

    pub fn canonical_object_key(object_key: &str) -> String {
        object_key.trim_start_matches('/').to_string()
    }

    pub fn get_all_object_intent_partition_paths(
        &self,
        fragment_hash: &str,
        object_key: &str,
    ) -> Result<Vec<PathBuf>, AppError> {
        let object_key = Self::canonical_object_key(object_key);
        let partition_id = Self::partition_id(fragment_hash, "object", &object_key)?;
        Ok(self
            .select_partition_roots(fragment_hash, partition_id)
            .iter()
            .map(|root| Self::intent_partition_path(root, fragment_hash, partition_id))
            .collect())
    }

    pub fn get_shard_dir_on_disk(
        &self,
        disk_root: &PathBuf,
        fragment_hash: &str,
        version_id: &str,
    ) -> PathBuf {
        disk_root
            .join(fragment_hash)
            .join(N2_BLOB_DIR)
            .join(version_id)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IndexListEntry {
    pub record: String,
    pub kind: String,
    pub size_bytes: u64,
    pub created_at_unix_nanos: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub packed_ext_u64: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_ulid: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListObjectsResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<IndexListEntry>,
    pub objects: Vec<ObjectMetadata>,
    #[serde(default)]
    pub common_prefixes: Vec<String>,
    pub continuation_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_continuation_token: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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

impl ObjectMetadata {
    pub fn strip_internal_layout(&mut self) {
        self.parts.clear();
    }

    pub fn without_internal_layout(mut self) -> Self {
        self.strip_internal_layout();
        self
    }
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

#[derive(Deserialize)]
pub struct InitiateUploadParams {
    pub content_type: Option<String>,
    pub total_size_bytes: u64,
    pub expected_parts: u64,
    #[serde(default)]
    pub part_size_bytes: Option<u64>,
}

#[derive(Serialize, Deserialize)]
pub struct InitiateUploadResponse {
    pub session_id: String,
    pub version_id: String,
}

#[derive(Serialize)]
pub struct CapacityUsageResponse {
    pub capacity_used: u64,
    pub capacity_allocated: u64,
    #[serde(default)]
    pub capacity_available: u64,
    #[serde(default)]
    pub filesystem_count: usize,
}

#[derive(Deserialize)]
pub struct RenameObjectRequest {
    pub source_fragment: String,
    pub source_key: String,
    pub destination_fragment: String,
    pub destination_key: String,
}

#[derive(Deserialize)]
pub struct ObjectKeyQuery {
    pub object_key: String,
    pub version_id: Option<String>,
    #[serde(default)]
    pub filename: Option<String>,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct UploadStatusResponse {
    pub uploaded_parts: Vec<u64>,
    #[serde(default)]
    pub committed_parts: u64,
    #[serde(default)]
    pub committed_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_parts: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub physical_object_key: Option<String>,
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

#[derive(serde::Deserialize)]
pub struct DownloadArchiveRequest {
    #[serde(default)]
    pub keys: Vec<String>,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub prefixes: Vec<String>,
    #[serde(default = "default_format")]
    pub format: String,
    #[serde(default)]
    pub filename: Option<String>,
    #[serde(default)]
    pub export_all: bool,
    #[serde(default)]
    pub include_internal: bool,
}

#[derive(serde::Deserialize)]
pub struct DownloadArchiveManifestQuery {
    #[serde(default = "default_tar_format")]
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
