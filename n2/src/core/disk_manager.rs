use crate::core::checksum::Sha256Digest;
use crate::core::constants::N2_BLOB_DIR;
use crate::core::fragment_identity;
use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::OnceLock;

pub fn default_format() -> String {
    "zip".to_string()
}

pub fn default_tar_format() -> String {
    "tar".to_string()
}

pub static STORAGE_DISKS: OnceLock<Vec<String>> = OnceLock::new();
pub static FS_STORAGE_ROOT: OnceLock<String> = OnceLock::new();

pub fn default_stripe_size() -> usize {
    256 * 1024
}
pub struct ObjectPathManager;

impl ObjectPathManager {
    pub fn new() -> Self {
        Self
    }

    pub fn get_storage_roots(&self) -> Vec<PathBuf> {
        let roots = STORAGE_DISKS
            .get()
            .expect("N2 storage devices are not initialized");
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

    fn namespace_partition_index(fragment_hash: &str, path: &str, root_count: usize) -> usize {
        if root_count <= 1 {
            return 0;
        }
        let hash = Self::route_hash(&["namespace", fragment_hash, path]);
        usize::from(u16::from(hash[0]) << 8 | u16::from(hash[1])) % root_count
    }

    pub fn resolve_stable_fragment_id(&self, fragment_hash: &str) -> String {
        let roots = STORAGE_DISKS
            .get()
            .map(|roots| roots.iter().map(PathBuf::from).collect::<Vec<_>>())
            .unwrap_or_default();
        for root in roots {
            if let Some(stable) =
                fragment_identity::stable_fragment_id_for_root(&root, fragment_hash)
            {
                return stable;
            }
        }
        fragment_hash.to_string()
    }

    pub fn get_namespace_roots(&self, fragment_hash: &str) -> Vec<PathBuf> {
        self.get_namespace_roots_for_path(fragment_hash, "")
    }

    pub fn get_namespace_roots_for_path(
        &self,
        fragment_hash: &str,
        logical_path: &str,
    ) -> Vec<PathBuf> {
        let roots = self.get_storage_roots();
        if roots.is_empty() {
            return Vec::new();
        }
        vec![
            roots[Self::namespace_partition_index(fragment_hash, logical_path, roots.len())]
                .clone(),
        ]
    }

    pub fn get_all_namespace_roots(&self, _fragment_hash: &str) -> Vec<PathBuf> {
        self.get_storage_roots()
    }

    pub fn get_session_root(&self, fragment_hash: &str, version_id: &str) -> Option<PathBuf> {
        let roots = self.get_storage_roots();
        if roots.is_empty() {
            return None;
        }
        let hash = Self::route_hash(&["session", fragment_hash, version_id]);
        let idx = usize::from(u16::from(hash[0]) << 8 | u16::from(hash[1])) % roots.len();
        roots.get(idx).cloned()
    }

    pub fn object_key_hash(object_key: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(object_key.as_bytes());
        hex::encode(hasher.finalize())
    }

    pub fn canonical_object_key(object_key: &str) -> String {
        object_key.trim_start_matches('/').to_string()
    }

    pub fn object_fanout_segments(object_id: &str) -> [String; 4] {
        let hash = Sha256::digest(object_id.as_bytes());
        [
            format!("{:02x}", hash[0]),
            format!("{:02x}", hash[1]),
            format!("{:02x}", hash[2]),
            format!("{:02x}", hash[3]),
        ]
    }

    pub fn get_shard_dir_on_disk(
        &self,
        disk_root: &PathBuf,
        fragment_hash: &str,
        object_id: &str,
        version_id: &str,
    ) -> PathBuf {
        let stable_fragment_id = self.resolve_stable_fragment_id(fragment_hash);
        let fanout = Self::object_fanout_segments(object_id);
        fragment_identity::physical_fragment_root_on(disk_root, &stable_fragment_id)
            .join(N2_BLOB_DIR)
            .join(&fanout[0])
            .join(&fanout[1])
            .join(&fanout[2])
            .join(&fanout[3])
            .join(format!("@o_{object_id}"))
            .join(version_id)
    }

    pub fn get_object_dir_on_disk(
        &self,
        disk_root: &PathBuf,
        fragment_hash: &str,
        object_id: &str,
    ) -> PathBuf {
        let stable_fragment_id = self.resolve_stable_fragment_id(fragment_hash);
        let fanout = Self::object_fanout_segments(object_id);
        fragment_identity::physical_fragment_root_on(disk_root, &stable_fragment_id)
            .join(N2_BLOB_DIR)
            .join(&fanout[0])
            .join(&fanout[1])
            .join(&fanout[2])
            .join(&fanout[3])
            .join(format!("@o_{object_id}"))
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub block_checksums: Vec<Sha256Digest>,
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    pub checksum_block_size: usize,
    pub is_parity: bool,
}

fn is_zero_usize(value: &usize) -> bool {
    *value == 0
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
    #[serde(default)]
    pub packed_ext_u64: u64,
    pub fragment: String,
    pub version_id: String,
    pub upload_session_id: String,
    pub owner_uid: String,
    pub content_type: String,
    pub total_size_bytes: u64,
    pub created_at: i64,
    pub pool_id: String,
    pub pool_generation: u64,
    pub membership_epoch: u64,
    pub data_shards: u16,
    pub parity_shards: u16,
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
    #[serde(default)]
    pub packed_ext_u64: u64,
    pub fragment: String,
    pub version_id: String,
    pub upload_session_id: String,
    pub owner_uid: String,
    pub content_type: String,
    pub total_size_bytes: u64,
    pub created_at: i64,
    pub pool_id: String,
    pub pool_generation: u64,
    pub membership_epoch: u64,
    pub data_shards: u16,
    pub parity_shards: u16,
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
            packed_ext_u64: summary.packed_ext_u64,
            fragment: summary.fragment,
            version_id: summary.version_id,
            upload_session_id: summary.upload_session_id,
            owner_uid: summary.owner_uid,
            content_type: summary.content_type,
            total_size_bytes: summary.total_size_bytes,
            created_at: summary.created_at,
            pool_id: summary.pool_id,
            pool_generation: summary.pool_generation,
            membership_epoch: summary.membership_epoch,
            data_shards: summary.data_shards,
            parity_shards: summary.parity_shards,
            parts: Vec::new(),
            is_delete_marker: summary.is_delete_marker,
            stripe_size_bytes: summary.stripe_size_bytes,
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct InitiateUploadParams {
    pub content_type: Option<String>,
    pub total_size_bytes: u64,
    pub expected_parts: u64,
    #[serde(default)]
    pub part_size_bytes: Option<u64>,
    #[serde(skip)]
    pub packed_ext_u64: u64,
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

#[derive(Clone, Deserialize)]
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
