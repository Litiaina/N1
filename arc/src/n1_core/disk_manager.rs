#[cfg(test)]
use crate::n1_core::constants::QUOTA_FILE;
use crate::n1_core::constants::{
    PREFIX_DELIMITER_COMPACTED_FILE, PREFIX_INDEX_DIR, PREFIX_INDEX_NAMESPACE,
    PREFIX_INDEX_VERSION, PREFIX_SEGMENT_EXT,
};
#[cfg(test)]
use once_cell::sync::Lazy;
use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::path::PathBuf;
use std::sync::{OnceLock, RwLock};
#[cfg(test)]
use tokio::sync::Mutex;
use ulid::Ulid;

pub fn default_format() -> String {
    "zip".to_string()
}

pub static INDEX_STORE_DISKS: OnceLock<RwLock<Vec<String>>> = OnceLock::new();
pub static BLOB_STORE_DISKS: OnceLock<RwLock<Vec<String>>> = OnceLock::new();
pub static FS_STORAGE_ROOT: OnceLock<RwLock<String>> = OnceLock::new();
#[cfg(test)]
#[allow(dead_code)]
pub static STORAGE_TEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
fn rwlock_poisoned(label: &str) -> crate::enums::app_error_enum::AppError {
    crate::enums::app_error_enum::AppError::InternalServerError(format!(
        "{} config lock poisoned",
        label
    ))
}

pub fn configured_index_store_disks() -> Result<Vec<String>, crate::enums::app_error_enum::AppError>
{
    let roots = INDEX_STORE_DISKS
        .get()
        .ok_or_else(|| {
            crate::enums::app_error_enum::AppError::InternalServerError(
                "Index Store Config not initialized!".into(),
            )
        })?
        .read()
        .map_err(|_| rwlock_poisoned("index"))?
        .clone();
    Ok(roots)
}

pub fn configured_blob_store_disks() -> Result<Vec<String>, crate::enums::app_error_enum::AppError>
{
    let roots = BLOB_STORE_DISKS
        .get()
        .ok_or_else(|| {
            crate::enums::app_error_enum::AppError::InternalServerError(
                "Blob Store Config not initialized!".into(),
            )
        })?
        .read()
        .map_err(|_| rwlock_poisoned("blob"))?
        .clone();
    Ok(roots)
}

pub fn configured_fs_storage_root() -> Option<String> {
    FS_STORAGE_ROOT
        .get()
        .and_then(|lock| lock.read().ok().map(|value| value.clone()))
}

pub fn initialize_storage_roots(
    index_roots: Vec<String>,
    blob_roots: Vec<String>,
    fs_root: String,
) -> Result<(), String> {
    INDEX_STORE_DISKS
        .set(RwLock::new(index_roots))
        .map_err(|_| "Double initialization of INDEX_STORE_DISKS!".to_string())?;
    BLOB_STORE_DISKS
        .set(RwLock::new(blob_roots))
        .map_err(|_| "Double initialization of BLOB_STORE_DISKS!".to_string())?;
    FS_STORAGE_ROOT
        .set(RwLock::new(fs_root))
        .map_err(|_| "Double initialization of STORAGE_ROOT!".to_string())?;
    Ok(())
}

#[cfg(test)]
pub fn replace_storage_roots_for_tests(
    index_roots: Vec<String>,
    blob_roots: Vec<String>,
    fs_root: String,
) {
    if let Some(lock) = INDEX_STORE_DISKS.get() {
        *lock.write().expect("write INDEX_STORE_DISKS") = index_roots;
    } else {
        INDEX_STORE_DISKS
            .set(RwLock::new(index_roots))
            .expect("set INDEX_STORE_DISKS");
    }

    if let Some(lock) = BLOB_STORE_DISKS.get() {
        *lock.write().expect("write BLOB_STORE_DISKS") = blob_roots;
    } else {
        BLOB_STORE_DISKS
            .set(RwLock::new(blob_roots))
            .expect("set BLOB_STORE_DISKS");
    }

    if let Some(lock) = FS_STORAGE_ROOT.get() {
        *lock.write().expect("write FS_STORAGE_ROOT") = fs_root;
    } else {
        FS_STORAGE_ROOT
            .set(RwLock::new(fs_root))
            .expect("set FS_STORAGE_ROOT");
    }

    crate::n1_core::blob_store::clear_raw_runtime_caches_for_tests();
}

pub fn default_stripe_size() -> usize {
    256 * 1024
}

pub struct ObjectPathManager;

impl ObjectPathManager {
    pub fn new() -> Self {
        Self
    }

    pub fn get_all_index_roots(&self) -> Vec<PathBuf> {
        configured_index_store_disks()
            .expect("Index Store Config not initialized!")
            .into_iter()
            .map(PathBuf::from)
            .collect()
    }

    pub fn get_blob_roots(&self) -> Vec<PathBuf> {
        configured_blob_store_disks()
            .expect("Blob Store Config not initialized!")
            .into_iter()
            .map(PathBuf::from)
            .collect()
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

    pub fn get_all_prefix_delimiter_compacted_paths(
        &self,
        fragment_hash: &str,
        prefix: &str,
    ) -> Vec<PathBuf> {
        self.get_all_prefix_bucket_dirs(fragment_hash, prefix)
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

    #[cfg(test)]
    pub fn get_all_quota_paths(&self, fragment_hash: &str) -> Vec<PathBuf> {
        self.get_all_index_roots()
            .iter()
            .map(|root| root.join(fragment_hash).join(QUOTA_FILE))
            .collect()
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RawBlobAddress {
    #[serde(default = "default_raw_topology_epoch")]
    pub topology_epoch: u64,
    pub device_id: String,
    pub allocation_id: u64,
    #[serde(default = "default_raw_extent_generation")]
    pub extent_gen: u32,
    pub offset_bytes: u64,
    pub size_bytes: u64,
}

fn default_raw_topology_epoch() -> u64 {
    1
}

fn default_raw_extent_generation() -> u32 {
    1
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ShardInfo {
    pub shard_index: usize,
    pub file_name: String,
    pub absolute_path: String,
    #[serde(default)]
    pub raw_blob_address: Option<RawBlobAddress>,
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

impl From<&ObjectMetadata> for ObjectSummary {
    fn from(metadata: &ObjectMetadata) -> Self {
        ObjectSummary {
            object_key: metadata.object_key.clone(),
            fragment: metadata.fragment.clone(),
            version_id: metadata.version_id.clone(),
            upload_session_id: metadata.upload_session_id.clone(),
            owner_uid: metadata.owner_uid.clone(),
            content_type: metadata.content_type.clone(),
            total_size_bytes: metadata.total_size_bytes,
            created_at: metadata.created_at,
            _parts_ignored: IgnoredAny,
            is_delete_marker: metadata.is_delete_marker,
            stripe_size_bytes: metadata.stripe_size_bytes,
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

// Version ids are ULIDs, so they carry millisecond ordering when second-level
// created_at timestamps are too coarse to disambiguate back-to-back uploads.
pub fn compare_version_id_recency(left: &str, right: &str) -> Ordering {
    match (Ulid::from_string(left).ok(), Ulid::from_string(right).ok()) {
        (Some(left_ulid), Some(right_ulid)) => left_ulid.cmp(&right_ulid).then(left.cmp(right)),
        _ => left.cmp(right),
    }
}

pub fn compare_version_recency(
    left_created_at: i64,
    left_version_id: &str,
    right_created_at: i64,
    right_version_id: &str,
) -> Ordering {
    left_created_at
        .cmp(&right_created_at)
        .then_with(|| compare_version_id_recency(left_version_id, right_version_id))
}

pub fn version_recency_is_newer(
    candidate_created_at: i64,
    candidate_version_id: &str,
    current_created_at: i64,
    current_version_id: &str,
) -> bool {
    compare_version_recency(
        candidate_created_at,
        candidate_version_id,
        current_created_at,
        current_version_id,
    ) == Ordering::Greater
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PrefixIndexSegment {
    pub prefix: String,
    pub entries: Vec<PrefixIndexEntry>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PrefixDelimiterChildEntry {
    pub common_prefix: String,
    pub live_count: u64,
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
    #[serde(default)]
    pub part_size_bytes: Option<u64>,
    #[serde(default)]
    pub part_sizes_bytes: Option<Vec<u64>>,
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
    pub committed_parts: u64,
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
    pub object_key: Option<String>,
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
