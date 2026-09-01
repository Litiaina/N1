use crate::core::fragment_identity;
use crate::core::root_identity::{
    configured_root_identity_is_current, root_identity_enforcement_initialized,
};
use crate::enums::app_error_enum::AppError;
use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
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
        roots
            .iter()
            .map(PathBuf::from)
            .filter(|root| {
                !root_identity_enforcement_initialized()
                    || configured_root_identity_is_current(root)
            })
            .collect()
    }

    pub fn get_blob_roots(&self) -> Vec<PathBuf> {
        let roots = BLOB_STORE_DISKS
            .get()
            .expect("Blob Store Config not initialized!");
        roots
            .iter()
            .map(PathBuf::from)
            .filter(|root| {
                !root_identity_enforcement_initialized()
                    || configured_root_identity_is_current(root)
            })
            .collect()
    }

    pub fn select_blob_roots_for_object(
        &self,
        fragment: &str,
        object_key: &str,
        version_id: &str,
        shard_count: usize,
    ) -> Result<Vec<PathBuf>, AppError> {
        if shard_count == 0 {
            return Err(AppError::InternalServerError(
                "object EC set must contain at least one shard".into(),
            ));
        }

        let roots = self.get_blob_roots();
        if roots.len() < shard_count {
            return Err(AppError::ServiceUnavailable(format!(
                "Configured blob roots ({}) cannot satisfy this object's {}-shard EC set",
                roots.len(),
                shard_count
            )));
        }

        let ranked = self.rank_blob_roots_for_object(fragment, object_key, version_id);
        let mut selected = Vec::with_capacity(shard_count);
        let mut failure_domains = HashSet::new();
        for root in ranked {
            let failure_domain = blob_root_failure_domain(&root);
            if failure_domains.insert(failure_domain) {
                selected.push(root);
                if selected.len() == shard_count {
                    break;
                }
            }
        }
        if selected.len() != shard_count {
            return Err(AppError::ServiceUnavailable(format!(
                "Configured blob roots do not provide {} distinct failure domains required by this object's EC set",
                shard_count
            )));
        }
        Ok(selected)
    }

    pub fn rank_blob_roots_for_object(
        &self,
        fragment: &str,
        object_key: &str,
        version_id: &str,
    ) -> Vec<PathBuf> {
        let mut ranked: Vec<([u8; 32], PathBuf)> = self
            .get_blob_roots()
            .into_iter()
            .map(|root| {
                let root_label = root.to_string_lossy();
                (
                    Self::route_hash(&[
                        "object-ec-set",
                        fragment,
                        object_key,
                        version_id,
                        root_label.as_ref(),
                    ]),
                    root,
                )
            })
            .collect();
        ranked.sort_by(|(score_a, root_a), (score_b, root_b)| {
            score_b.cmp(score_a).then_with(|| root_a.cmp(root_b))
        });
        ranked.into_iter().map(|(_, root)| root).collect()
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
        fragment_identity::fragment_root_on(root, fragment_hash)
            .join(".intents")
            .join(format!(
                "{}{:04x}.n1",
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

    pub fn resolve_stable_fragment_id(&self, fragment_hash: &str) -> String {
        for root in self.get_all_index_roots() {
            if let Some(stable) =
                fragment_identity::stable_fragment_id_for_root(&root, fragment_hash)
            {
                return stable;
            }
        }
        fragment_hash
            .strip_prefix(fragment_identity::PHYSICAL_FRAGMENT_PREFIX)
            .unwrap_or(fragment_hash)
            .to_string()
    }

    pub fn object_fanout_segments(object_id: &str) -> [String; 2] {
        let hash = Sha256::digest(object_id.as_bytes());
        [format!("{:02x}", hash[0]), format!("{:02x}", hash[1])]
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
            .join(".objects")
            .join(&fanout[0])
            .join(&fanout[1])
            .join(format!("@o_{object_id}"))
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
        object_id: &str,
        version_id: &str,
    ) -> PathBuf {
        self.get_object_dir_on_disk(disk_root, fragment_hash, object_id)
            .join(version_id)
    }
}

#[cfg(unix)]
fn blob_root_failure_domain(root: &PathBuf) -> String {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(root)
        .map(|metadata| format!("dev:{}", metadata.dev()))
        .unwrap_or_else(|_| format!("path:{}", root.display()))
}

#[cfg(not(unix))]
fn blob_root_failure_domain(root: &PathBuf) -> String {
    format!("path:{}", root.display())
}

#[cfg(test)]
mod placement_tests {
    use sha2::{Digest, Sha256};
    use std::path::PathBuf;

    fn rank(
        roots: &[PathBuf],
        fragment: &str,
        object_key: &str,
        version_id: &str,
        count: usize,
    ) -> Vec<PathBuf> {
        let mut ranked: Vec<([u8; 32], PathBuf)> = roots
            .iter()
            .cloned()
            .map(|root| {
                let mut hasher = Sha256::new();
                for part in [
                    "object-ec-set",
                    fragment,
                    object_key,
                    version_id,
                    root.to_string_lossy().as_ref(),
                ] {
                    hasher.update(part.as_bytes());
                    hasher.update([0u8]);
                }
                (hasher.finalize().into(), root)
            })
            .collect();
        ranked.sort_by(|(score_a, root_a), (score_b, root_b)| {
            score_b.cmp(score_a).then_with(|| root_a.cmp(root_b))
        });
        ranked
            .into_iter()
            .take(count)
            .map(|(_, root)| root)
            .collect()
    }

    #[test]
    fn object_ec_set_selection_is_deterministic_and_bounded() {
        let roots = (0..8)
            .map(|idx| PathBuf::from(format!("/disk-{idx}")))
            .collect::<Vec<_>>();
        let first = rank(&roots, "fragment", "object", "version", 6);
        let second = rank(&roots, "fragment", "object", "version", 6);
        assert_eq!(first, second);
        assert_eq!(first.len(), 6);
        let unique = first.iter().collect::<std::collections::HashSet<_>>();
        assert_eq!(unique.len(), 6);
    }

    #[test]
    fn different_object_versions_spread_across_expanded_pool() {
        let roots = (0..8)
            .map(|idx| PathBuf::from(format!("/disk-{idx}")))
            .collect::<Vec<_>>();
        let mut used = std::collections::HashSet::new();
        for version in 0..64 {
            used.extend(rank(
                &roots,
                "fragment",
                "object",
                &format!("version-{version}"),
                6,
            ));
        }
        assert_eq!(used.len(), roots.len());
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
    pub data_shards: u16,
    pub parity_shards: u16,
    pub placement_generation: u64,
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
            fragment: summary.fragment,
            version_id: summary.version_id,
            upload_session_id: summary.upload_session_id,
            owner_uid: summary.owner_uid,
            content_type: summary.content_type,
            total_size_bytes: summary.total_size_bytes,
            created_at: summary.created_at,
            data_shards: summary.data_shards,
            parity_shards: summary.parity_shards,
            placement_generation: 0,
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DownloadArchiveEntry {
    pub object_key: String,
    pub archive_path: String,
    #[serde(default)]
    pub version_id: Option<String>,
    #[serde(default)]
    pub offset_bytes: Option<u64>,
    #[serde(default)]
    pub length_bytes: Option<u64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DownloadArchiveRequest {
    #[serde(default)]
    pub keys: Vec<String>,
    #[serde(default)]
    pub entries: Vec<DownloadArchiveEntry>,
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
