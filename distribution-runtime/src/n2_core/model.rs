use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CrossParams {
    pub kc: u8,
    pub mc: u8,
    pub kd: u8,
    pub md: u8,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ObjectIdentity {
    pub uid: String,
    pub fragment: String,
    pub object_key: String,
    pub version_id: String,
}

impl ObjectIdentity {
    pub fn placement_key(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            self.uid, self.fragment, self.object_key, self.version_id
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct NodeDescriptor {
    pub node_id: String,
    pub endpoint: String,
    pub epoch: u64,
    pub active: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterMembership {
    pub epoch: u64,
    pub nodes: Vec<NodeDescriptor>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ResolvePlacementQuery {
    pub uid: Option<String>,
    pub version_id: Option<String>,
    pub kc: Option<u8>,
    pub mc: Option<u8>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct UpsertMembershipRequest {
    pub epoch: Option<u64>,
    pub nodes: Vec<NodeDescriptor>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResolvePlacementResponse {
    pub epoch: u64,
    pub object: ObjectIdentity,
    pub kc: u8,
    pub mc: u8,
    pub nc: usize,
    pub write_quorum: u8,
    pub targets: Vec<NodeDescriptor>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PutObjectQuery {
    pub uid: Option<String>,
    pub version_id: Option<String>,
    pub expected_epoch: Option<u64>,
    pub kc: Option<u8>,
    pub mc: Option<u8>,
    pub kd: Option<u8>,
    pub md: Option<u8>,
    pub stripe_block_size: Option<usize>,
    pub content_type: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct GetObjectQuery {
    pub version_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ListN2ObjectsQuery {
    pub prefix: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ListN2VersionsQuery {
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DeleteN2ObjectQuery {
    pub version_id: Option<String>,
    pub expected_epoch: Option<u64>,
    pub purge_shards: Option<bool>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct RenameN2ObjectRequest {
    pub fragment: String,
    pub source_object_key: String,
    pub destination_object_key: String,
    pub expected_epoch: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ReconcileFragmentQuery {
    pub prefix: Option<String>,
    pub max_objects: Option<usize>,
    pub dry_run: Option<bool>,
    pub expected_epoch: Option<u64>,
    pub cleanup_orphans: Option<bool>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ClusterViewQuery {
    pub expected_epoch: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct N2ClusterViewResponse {
    pub node_id: String,
    pub node_endpoint: String,
    pub membership_epoch: u64,
    pub expected_epoch: Option<u64>,
    pub total_nodes: usize,
    pub active_nodes: usize,
    pub local_node_present: bool,
    pub local_node_active: bool,
    pub write_ready: bool,
    pub reasons: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct InternalStoreShardQuery {
    pub uid: Option<String>,
    pub object_key: Option<String>,
    pub kd: u8,
    pub md: u8,
    pub stripe_block_size: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct N2DiskShardRef {
    pub disk_shard_index: usize,
    pub absolute_path: String,
    pub size_bytes: u64,
    pub checksum: String,
    pub is_parity: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct N2ClusterShardRef {
    pub cluster_shard_index: usize,
    pub node_id: String,
    pub endpoint: String,
    pub size_bytes: u64,
    pub checksum: String,
    pub disk_shards: Vec<N2DiskShardRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct N2ObjectMap {
    pub epoch: u64,
    pub object: ObjectIdentity,
    pub params: CrossParams,
    pub stripe_block_size: usize,
    pub total_size_bytes: u64,
    pub content_type: String,
    pub write_quorum: u8,
    pub created_at: i64,
    pub committed_at: i64,
    pub shards: Vec<N2ClusterShardRef>,
}

#[derive(Clone, Debug, Serialize)]
pub struct N2PutObjectResponse {
    pub epoch: u64,
    pub version_id: String,
    pub total_size_bytes: u64,
    pub committed_shards: usize,
    pub required_quorum: u8,
    pub object: N2ObjectMap,
}

#[derive(Clone, Debug, Serialize)]
pub struct N2ObjectSummary {
    pub object_key: String,
    pub latest_version_id: String,
    pub epoch: u64,
    pub total_size_bytes: u64,
    pub content_type: String,
    pub committed_at: i64,
    pub shard_count: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct N2ListObjectsResponse {
    pub fragment: String,
    pub prefix: Option<String>,
    pub limit: usize,
    pub objects: Vec<N2ObjectSummary>,
}

#[derive(Clone, Debug, Serialize)]
pub struct N2ObjectVersionSummary {
    pub version_id: String,
    pub epoch: u64,
    pub total_size_bytes: u64,
    pub content_type: String,
    pub committed_at: i64,
    pub shard_count: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct N2ListObjectVersionsResponse {
    pub fragment: String,
    pub object_key: String,
    pub versions: Vec<N2ObjectVersionSummary>,
}

#[derive(Clone, Debug, Serialize)]
pub struct N2DeleteObjectResponse {
    pub fragment: String,
    pub object_key: String,
    pub deleted_versions: usize,
    pub deleted_map_files: usize,
    pub attempted_shard_deletes: usize,
    pub deleted_shard_paths: usize,
    pub shard_delete_failures: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct N2RenameObjectResponse {
    pub fragment: String,
    pub source_object_key: String,
    pub destination_object_key: String,
    pub moved_versions: usize,
    pub deleted_old_map_files: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct N2ReconcileObjectResult {
    pub object_key: String,
    pub version_id: String,
    pub old_epoch: u64,
    pub target_epoch: u64,
    pub planned_shards: usize,
    pub repaired_shards: usize,
    pub cleaned_orphans: usize,
    pub cleanup_failures: usize,
    pub status: String,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct N2ReconcileResponse {
    pub fragment: String,
    pub prefix: Option<String>,
    pub dry_run: bool,
    pub scanned_objects: usize,
    pub updated_objects: usize,
    pub failed_objects: usize,
    pub results: Vec<N2ReconcileObjectResult>,
}
