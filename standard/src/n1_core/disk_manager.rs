use crate::n1_core::constants::{
    METADATA_EXT, PREFIX_DELIMITER_COMPACTED_FILE, PREFIX_DELIMITER_SEGMENT_PREFIX,
    PREFIX_INDEX_DIR, PREFIX_INDEX_NAMESPACE, PREFIX_INDEX_VERSION, PREFIX_SEGMENT_EXT,
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

    fn route_hash(parts: &[&str]) -> [u8; 32] {
        let mut hasher = Sha256::new();
        for part in parts {
            hasher.update(part.as_bytes());
            hasher.update([0u8]);
        }
        hasher.finalize().into()
    }

    fn partition_id_from_hash(hash: [u8; 32]) -> u16 {
        (u16::from(hash[0]) << 8 | u16::from(hash[1])) % 256
    }

    fn select_partition_roots(&self, partition_id: u16) -> Vec<PathBuf> {
        let roots = self.get_all_index_roots();
        if roots.is_empty() {
            return Vec::new();
        }
        let owner_idx = usize::from(partition_id) % roots.len();
        let mut selected = vec![roots[owner_idx].clone()];
        if roots.len() > 1 {
            let replica_offset = 1 + ((usize::from(partition_id >> 8)) % (roots.len() - 1));
            let replica_idx = (owner_idx + replica_offset) % roots.len();
            if replica_idx != owner_idx {
                selected.push(roots[replica_idx].clone());
            }
        }
        selected.sort_by(|a, b| a.to_string_lossy().cmp(&b.to_string_lossy()));
        selected
    }

    fn object_partition_id(fragment_hash: &str, object_key: &str) -> u16 {
        let canonical = Self::canonical_prefix(object_key);
        Self::partition_id_from_hash(Self::route_hash(&["object", fragment_hash, &canonical]))
    }

    fn session_partition_id(fragment_hash: &str, version_id: &str) -> u16 {
        Self::partition_id_from_hash(Self::route_hash(&["session", fragment_hash, version_id]))
    }

    fn partition_dir(root: &PathBuf, fragment_hash: &str, partition_id: u16) -> PathBuf {
        root.join(fragment_hash)
            .join("index")
            .join("partitions")
            .join(format!("{:04x}", partition_id))
    }

    pub fn get_all_session_dirs(&self, fragment_hash: &str, version_id: &str) -> Vec<PathBuf> {
        let partition_id = Self::session_partition_id(fragment_hash, version_id);
        self.select_partition_roots(partition_id)
            .iter()
            .map(|root| {
                Self::partition_dir(root, fragment_hash, partition_id)
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
        let partition_id = Self::object_partition_id(fragment_hash, object_key);
        let filename = format!("{}{}", version_id, METADATA_EXT);
        self.select_partition_roots(partition_id)
            .iter()
            .map(|root| {
                Self::partition_dir(root, fragment_hash, partition_id)
                    .join("objects")
                    .join(&key_hash[0..2])
                    .join(&key_hash)
                    .join(&filename)
            })
            .collect()
    }

    #[allow(dead_code)]
    pub fn get_all_trashed_metadata_paths(
        &self,
        fragment_hash: &str,
        object_key: &str,
        version_id: &str,
    ) -> Vec<PathBuf> {
        let key_hash = Self::object_key_hash(object_key);
        let partition_id = Self::object_partition_id(fragment_hash, object_key);
        let filename = format!("{}{}", version_id, METADATA_EXT);
        self.select_partition_roots(partition_id)
            .iter()
            .map(|root| {
                Self::partition_dir(root, fragment_hash, partition_id)
                    .join("trash")
                    .join(&key_hash[0..2])
                    .join(&key_hash)
                    .join(&filename)
            })
            .collect()
    }

    pub fn get_all_prefix_bucket_dirs(&self, fragment_hash: &str, prefix: &str) -> Vec<PathBuf> {
        self.get_all_prefix_bucket_dirs_scoped(fragment_hash, prefix, false)
    }

    pub fn get_all_trash_prefix_bucket_dirs(
        &self,
        fragment_hash: &str,
        prefix: &str,
    ) -> Vec<PathBuf> {
        self.get_all_prefix_bucket_dirs_scoped(fragment_hash, prefix, true)
    }

    fn get_all_prefix_bucket_dirs_scoped(
        &self,
        fragment_hash: &str,
        prefix: &str,
        trashed: bool,
    ) -> Vec<PathBuf> {
        let ph = Self::prefix_hash(prefix);
        let p1 = &ph[0..2];
        let p2 = &ph[2..4];

        self.get_all_index_roots()
            .iter()
            .map(|root| {
                let base = if trashed {
                    root.join(fragment_hash).join(".trash").join("index")
                } else {
                    root.join(fragment_hash).join("index")
                };
                base.join(PREFIX_INDEX_DIR)
                    .join(PREFIX_INDEX_VERSION)
                    .join(PREFIX_INDEX_NAMESPACE)
                    .join(p1)
                    .join(p2)
                    .join(&ph)
            })
            .collect()
    }

    pub fn get_all_prefix_compacted_paths(
        &self,
        fragment_hash: &str,
        prefix: &str,
    ) -> Vec<PathBuf> {
        self.get_all_prefix_compacted_paths_scoped(fragment_hash, prefix, false)
    }

    pub fn get_all_trash_prefix_compacted_paths(
        &self,
        fragment_hash: &str,
        prefix: &str,
    ) -> Vec<PathBuf> {
        self.get_all_prefix_compacted_paths_scoped(fragment_hash, prefix, true)
    }

    fn get_all_prefix_compacted_paths_scoped(
        &self,
        fragment_hash: &str,
        prefix: &str,
        trashed: bool,
    ) -> Vec<PathBuf> {
        self.get_all_prefix_bucket_dirs_scoped(fragment_hash, prefix, trashed)
            .into_iter()
            .map(|d| d.join(format!("compacted{}", PREFIX_SEGMENT_EXT)))
            .collect()
    }

    pub fn get_all_prefix_delimiter_segment_paths(
        &self,
        fragment_hash: &str,
        prefix: &str,
        segment_id: &str,
    ) -> Vec<PathBuf> {
        self.get_all_prefix_delimiter_segment_paths_scoped(fragment_hash, prefix, segment_id, false)
    }

    pub fn get_all_trash_prefix_delimiter_segment_paths(
        &self,
        fragment_hash: &str,
        prefix: &str,
        segment_id: &str,
    ) -> Vec<PathBuf> {
        self.get_all_prefix_delimiter_segment_paths_scoped(fragment_hash, prefix, segment_id, true)
    }

    fn get_all_prefix_delimiter_segment_paths_scoped(
        &self,
        fragment_hash: &str,
        prefix: &str,
        segment_id: &str,
        trashed: bool,
    ) -> Vec<PathBuf> {
        let file = format!(
            "{}{}{}",
            PREFIX_DELIMITER_SEGMENT_PREFIX, segment_id, PREFIX_SEGMENT_EXT
        );
        self.get_all_prefix_bucket_dirs_scoped(fragment_hash, prefix, trashed)
            .into_iter()
            .map(|d| d.join(&file))
            .collect()
    }

    pub fn get_all_prefix_delimiter_compacted_paths(
        &self,
        fragment_hash: &str,
        prefix: &str,
    ) -> Vec<PathBuf> {
        self.get_all_prefix_delimiter_compacted_paths_scoped(fragment_hash, prefix, false)
    }

    pub fn get_all_trash_prefix_delimiter_compacted_paths(
        &self,
        fragment_hash: &str,
        prefix: &str,
    ) -> Vec<PathBuf> {
        self.get_all_prefix_delimiter_compacted_paths_scoped(fragment_hash, prefix, true)
    }

    fn get_all_prefix_delimiter_compacted_paths_scoped(
        &self,
        fragment_hash: &str,
        prefix: &str,
        trashed: bool,
    ) -> Vec<PathBuf> {
        self.get_all_prefix_bucket_dirs_scoped(fragment_hash, prefix, trashed)
            .into_iter()
            .map(|d| d.join(PREFIX_DELIMITER_COMPACTED_FILE))
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
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListObjectsResult {
    pub objects: Vec<ObjectMetadata>,
    #[serde(default)]
    pub common_prefixes: Vec<String>,
    pub continuation_token: Option<String>,
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
    #[serde(default)]
    pub is_delete_marker: bool,
    #[serde(default)]
    pub has_inline_summary: bool,
    #[serde(default)]
    pub upload_session_id: String,
    #[serde(default)]
    pub owner_uid: String,
    #[serde(default)]
    pub content_type: String,
    #[serde(default)]
    pub total_size_bytes: u64,
    #[serde(default = "default_stripe_size")]
    pub stripe_size_bytes: usize,
}

impl PrefixIndexEntry {
    pub fn from_metadata(metadata: &ObjectMetadata) -> Self {
        Self {
            object_key: metadata.object_key.clone(),
            version_id: metadata.version_id.clone(),
            created_at: metadata.created_at,
            is_delete_marker: metadata.is_delete_marker,
            has_inline_summary: true,
            upload_session_id: metadata.upload_session_id.clone(),
            owner_uid: metadata.owner_uid.clone(),
            content_type: metadata.content_type.clone(),
            total_size_bytes: metadata.total_size_bytes,
            stripe_size_bytes: metadata.stripe_size_bytes,
        }
    }

    pub fn from_summary(summary: &ObjectSummary) -> Self {
        Self {
            object_key: summary.object_key.clone(),
            version_id: summary.version_id.clone(),
            created_at: summary.created_at,
            is_delete_marker: summary.is_delete_marker,
            has_inline_summary: true,
            upload_session_id: summary.upload_session_id.clone(),
            owner_uid: summary.owner_uid.clone(),
            content_type: summary.content_type.clone(),
            total_size_bytes: summary.total_size_bytes,
            stripe_size_bytes: summary.stripe_size_bytes,
        }
    }

    pub fn to_object_metadata(&self, fragment: &str) -> Option<ObjectMetadata> {
        if !self.has_inline_summary {
            return None;
        }

        Some(ObjectMetadata {
            object_key: self.object_key.clone(),
            fragment: fragment.to_string(),
            version_id: self.version_id.clone(),
            upload_session_id: self.upload_session_id.clone(),
            owner_uid: self.owner_uid.clone(),
            content_type: self.content_type.clone(),
            total_size_bytes: self.total_size_bytes,
            created_at: self.created_at,
            parts: Vec::new(),
            is_delete_marker: self.is_delete_marker,
            stripe_size_bytes: self.stripe_size_bytes,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PrefixIndexSegment {
    pub prefix: String,
    pub entries: Vec<PrefixIndexEntry>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PrefixDelimiterChildDelta {
    pub common_prefix: String,
    pub delta: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PrefixDelimiterChildEntry {
    pub common_prefix: String,
    pub live_count: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PrefixDelimiterSegment {
    pub prefix: String,
    #[serde(default)]
    pub object_entries: Vec<PrefixIndexEntry>,
    #[serde(default)]
    pub child_deltas: Vec<PrefixDelimiterChildDelta>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PrefixDelimiterCompactedSegment {
    pub prefix: String,
    #[serde(default)]
    pub object_entries: Vec<PrefixIndexEntry>,
    #[serde(default)]
    pub child_entries: Vec<PrefixDelimiterChildEntry>,
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
