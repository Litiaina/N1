use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CrossParams {
    pub kc: u8,
    pub mc: u8,
    pub kd: u8,
    pub md: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClusterCommitMode {
    Quorum,
    StrictFullPlacement,
}

impl ClusterCommitMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Quorum => "quorum",
            Self::StrictFullPlacement => "strict_full_placement",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ObjectIdentity {
    pub uid: String,
    pub fragment: String,
    pub object_id: String,
    pub object_key: String,
    pub version_id: String,
}

impl ObjectIdentity {
    pub fn placement_key(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            self.uid, self.fragment, self.object_id, self.version_id
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
    pub limit: Option<usize>,
    pub continuation: Option<String>,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
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
    pub cluster_fingerprint: String,
    pub expected_epoch: Option<u64>,
    pub total_nodes: usize,
    pub active_nodes: usize,
    pub local_node_present: bool,
    pub local_node_active: bool,
    pub governance_term: u64,
    pub governance_commit_index: u64,
    pub governance_leader_id: Option<String>,
    pub governance_leader_endpoint: Option<String>,
    pub governance_pending_index: Option<u64>,
    pub governance_healthy: bool,
    pub write_ready: bool,
    pub reasons: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum N2HealthLevel {
    Healthy,
    Degraded,
    Critical,
    Lost,
}

impl N2HealthLevel {
    pub fn worst(self, other: Self) -> Self {
        if self.rank() >= other.rank() {
            self
        } else {
            other
        }
    }

    fn rank(self) -> u8 {
        match self {
            Self::Healthy => 0,
            Self::Degraded => 1,
            Self::Critical => 2,
            Self::Lost => 3,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ClusterHealthQuery {
    pub expected_epoch: Option<u64>,
    pub deep: Option<bool>,
    pub max_fragments: Option<usize>,
    pub max_objects_per_fragment: Option<usize>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct FragmentHealthQuery {
    pub max_objects: Option<usize>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ObjectHealthQuery {
    pub version_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct RepairN2ObjectQuery {
    pub version_id: Option<String>,
    pub expected_epoch: Option<u64>,
    pub dry_run: Option<bool>,
    pub cleanup_orphans: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct N2DiskRootHealth {
    pub kind: String,
    pub path: String,
    pub exists: bool,
    pub readable: bool,
    pub writable: bool,
    pub total_bytes: Option<u64>,
    pub available_bytes: Option<u64>,
    pub used_bytes: Option<u64>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct N2NodeHealthResponse {
    pub node_id: String,
    pub endpoint: String,
    pub active: bool,
    pub local: bool,
    pub reachable: bool,
    pub level: N2HealthLevel,
    pub observed_at_unix: i64,
    pub rss_bytes: Option<u64>,
    pub index_roots: Vec<N2DiskRootHealth>,
    pub blob_roots: Vec<N2DiskRootHealth>,
    pub reasons: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct N2ClusterShardHealth {
    pub cluster_shard_index: usize,
    pub node_id: String,
    pub endpoint: String,
    pub expected_node_id: Option<String>,
    pub expected_endpoint: Option<String>,
    pub reachable: bool,
    pub present: bool,
    pub stale_placement: bool,
    pub size_bytes: Option<u64>,
    pub expected_size_bytes: Option<u64>,
    pub checksum: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct N2ObjectHealthResponse {
    pub fragment: String,
    pub object_key: String,
    pub object_id: String,
    pub version_id: String,
    pub epoch: u64,
    pub target_epoch: Option<u64>,
    pub level: N2HealthLevel,
    pub readable: bool,
    pub repairable: bool,
    pub needs_repair: bool,
    pub needs_redistribution: bool,
    pub required_shards: usize,
    pub expected_shards: usize,
    pub available_shards: usize,
    pub missing_shards: usize,
    pub stale_shards: usize,
    pub total_size_bytes: u64,
    pub shards: Vec<N2ClusterShardHealth>,
    pub reasons: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct N2ObjectHealthSummary {
    pub object_key: String,
    pub version_id: String,
    pub level: N2HealthLevel,
    pub readable: bool,
    pub repairable: bool,
    pub needs_repair: bool,
    pub needs_redistribution: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct N2FragmentHealthResponse {
    pub fragment: String,
    pub level: N2HealthLevel,
    pub max_objects: usize,
    pub scanned_objects: usize,
    pub healthy_objects: usize,
    pub degraded_objects: usize,
    pub critical_objects: usize,
    pub lost_objects: usize,
    pub repairable_objects: usize,
    pub needs_repair_objects: usize,
    pub failed_objects: usize,
    pub objects: Vec<N2ObjectHealthSummary>,
    pub reasons: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct N2ClusterHealthResponse {
    pub level: N2HealthLevel,
    pub view: N2ClusterViewResponse,
    pub expected_shards: usize,
    pub required_shards: usize,
    pub reachable_active_nodes: usize,
    pub unreachable_active_nodes: usize,
    pub storage_ready_nodes: usize,
    pub nodes: Vec<N2NodeHealthResponse>,
    pub deep: bool,
    pub fragments: Vec<N2FragmentHealthResponse>,
    pub reasons: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct N2RepairObjectResponse {
    pub before: N2ObjectHealthResponse,
    pub reconcile: N2ReconcileObjectResult,
    pub after: Option<N2ObjectHealthResponse>,
}

#[derive(Clone, Debug, Serialize)]
pub struct N2GovernanceStateResponse {
    pub current_term: u64,
    pub commit_index: u64,
    pub last_applied_membership_epoch: u64,
    pub leader_id: Option<String>,
    pub leader_endpoint: Option<String>,
    pub pending_index: Option<u64>,
    pub is_leader: bool,
    pub leader_lease_valid: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GovernanceMembershipEntry {
    pub index: u64,
    pub term: u64,
    pub membership: ClusterMembership,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GovernanceVoteRequest {
    pub term: u64,
    pub candidate_id: String,
    pub candidate_endpoint: String,
    pub last_commit_index: u64,
    pub last_applied_membership_epoch: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GovernanceVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
    pub leader_id: Option<String>,
    pub leader_endpoint: Option<String>,
    pub commit_index: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GovernanceAppendRequest {
    pub term: u64,
    pub leader_id: String,
    pub leader_endpoint: String,
    pub prev_commit_index: u64,
    pub leader_commit_index: u64,
    pub entry: Option<GovernanceMembershipEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GovernanceAppendResponse {
    pub term: u64,
    pub accepted: bool,
    pub commit_index: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GovernanceAbortRequest {
    pub term: u64,
    pub leader_id: String,
    pub pending_index: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GovernanceAbortResponse {
    pub term: u64,
    pub cleared: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct InternalStoreShardQuery {
    pub uid: Option<String>,
    pub object_id: Option<String>,
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
pub struct N2ObjectRecord {
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
    pub commit_mode: ClusterCommitMode,
    pub planned_shards: usize,
    pub committed_shards: usize,
    pub required_quorum: u8,
    pub required_commit_shards: usize,
    pub object: N2ObjectRecord,
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
    pub delimiter: Option<String>,
    pub limit: usize,
    pub continuation: Option<String>,
    pub has_more: bool,
    pub common_prefixes: Vec<String>,
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
    pub deleted_metadata_files: usize,
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
    pub metadata_records_rewritten: usize,
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
    pub dry_run: bool,
    pub scanned_objects: usize,
    pub updated_objects: usize,
    pub failed_objects: usize,
    pub results: Vec<N2ReconcileObjectResult>,
}
