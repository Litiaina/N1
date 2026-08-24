use crate::distribution::layout::ClusterLayoutPolicy;
use crate::enums::app_error_enum::AppError;
use arc_swap::ArcSwap;
use chrono::Utc;
use futures_util::future::join_all;
use ini::Ini;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, OnceLock};
use tokio::net::TcpStream;
use tokio::time::{Duration, timeout};
use ulid::Ulid;

mod reachability;
mod runtime;

pub use reachability::*;
pub use runtime::*;

const CLUSTER_NODE_PROBE_TIMEOUT: Duration = Duration::from_millis(1_500);
const MUTATION_AUTHORITY_PROBE_ATTEMPTS: usize = 2;
pub const N2_MEMBERSHIP_FORMAT_VERSION: u32 = 5;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageRoot {
    /// Deterministic preferred local route recorded in immutable object
    /// placement. The node may transparently satisfy I/O from another
    /// enrolled root when this root is unavailable or full.
    pub id: String,
    /// Stable device/root identity. It remains unchanged when rebound to a
    /// logical root. If omitted, it is identical to `id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_id: Option<String>,
    pub path: String,
}

pub fn parse_storage_roots(raw: &str) -> Result<Vec<StorageRoot>, AppError> {
    let mut roots = Vec::new();
    let mut ids = BTreeSet::new();
    let mut paths = BTreeSet::new();
    for entry in raw
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let (id, path) = entry.split_once('=').ok_or_else(|| {
            AppError::BadRequest(format!(
                "storage_roots entry '{entry}' must use ROOT_ID=/absolute/path"
            ))
        })?;
        let root = StorageRoot {
            id: id.trim().to_string(),
            physical_id: None,
            path: path.trim().to_string(),
        };
        root.validate()?;
        if !ids.insert(root.id.clone()) {
            return Err(AppError::BadRequest(format!(
                "storage root ID '{}' is configured more than once",
                root.id
            )));
        }
        if !paths.insert(root.path.clone()) {
            return Err(AppError::BadRequest(format!(
                "storage path '{}' is configured more than once",
                root.path
            )));
        }
        roots.push(root);
    }
    if roots.is_empty() {
        return Err(AppError::BadRequest(
            "N2 3.4 requires at least one storage_roots entry".into(),
        ));
    }
    Ok(roots)
}

impl StorageRoot {
    pub fn physical_id(&self) -> &str {
        self.physical_id.as_deref().unwrap_or(&self.id)
    }

    pub fn validate(&self) -> Result<(), AppError> {
        if self.id.trim().is_empty() {
            return Err(AppError::BadRequest(
                "storage root ID cannot be empty".into(),
            ));
        }
        if self.id.contains('/')
            || self.id.contains('=')
            || self.id.chars().any(char::is_whitespace)
        {
            return Err(AppError::BadRequest(format!(
                "storage root ID '{}' contains an invalid character",
                self.id
            )));
        }
        if self.physical_id().trim().is_empty()
            || self.physical_id().contains('/')
            || self.physical_id().contains('=')
            || self.physical_id().chars().any(char::is_whitespace)
        {
            return Err(AppError::BadRequest(format!(
                "physical storage root ID '{}' contains an invalid character",
                self.physical_id()
            )));
        }
        if !Path::new(self.path.trim()).is_absolute() {
            return Err(AppError::BadRequest(format!(
                "storage root '{}' must use an absolute path",
                self.id
            )));
        }
        Ok(())
    }
}

/// The configured static EC for the local pool. Namespace durability is not
/// independently configurable: NCNX replicas are always parity + one.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterPolicy {
    pub object_data_shards: usize,
    pub object_parity_shards: usize,
}

impl ClusterPolicy {
    pub fn validate(&self) -> Result<(), AppError> {
        if self.object_data_shards == 0 || self.object_parity_shards == 0 {
            return Err(AppError::BadRequest(
                "N2 3.4 pool EC requires at least 1 data and 1 parity shard".into(),
            ));
        }
        Ok(())
    }

    pub fn object_width(&self) -> usize {
        self.object_data_shards
            .saturating_add(self.object_parity_shards)
    }

    pub fn namespace_replicas(&self) -> usize {
        self.object_parity_shards.saturating_add(1)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PoolState {
    Writable,
    Sealed,
}

impl Default for PoolState {
    fn default() -> Self {
        Self::Writable
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ServerPool {
    pub id: String,
    pub generation: u64,
    pub policy: ClusterPolicy,
    #[serde(default)]
    pub state: PoolState,
    #[serde(rename = "slot_ids")]
    pub node_ids: Vec<String>,
}

impl ServerPool {
    pub fn validate(&self) -> Result<(), AppError> {
        if self.id.trim().is_empty() || self.generation == 0 {
            return Err(AppError::BadRequest(
                "server pool requires a stable ID and positive generation".into(),
            ));
        }
        self.policy.validate()?;
        let distinct = self.node_ids.iter().collect::<BTreeSet<_>>();
        if distinct.len() != self.node_ids.len() {
            return Err(AppError::BadRequest(format!(
                "server pool '{}' contains duplicate node IDs",
                self.id
            )));
        }
        if self.node_ids.len() != self.policy.object_width() {
            return Err(AppError::BadRequest(format!(
                "server pool '{}' EC {}+{} requires exactly {} nodes, but records {}",
                self.id,
                self.policy.object_data_shards,
                self.policy.object_parity_shards,
                self.policy.object_width(),
                self.node_ids.len()
            )));
        }
        Ok(())
    }

    pub fn writable(&self) -> bool {
        self.state == PoolState::Writable
    }
}

static CLUSTER_PROBE_HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(CLUSTER_NODE_PROBE_TIMEOUT)
        .build()
        .expect("build N2 cluster probe HTTP client")
});

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeState {
    Online,
    Degraded,
    Offline,
    Draining,
    Maintenance,
}

impl NodeState {
    pub fn accepts_new_writes(&self) -> bool {
        matches!(self, Self::Online | Self::Degraded)
    }

    pub fn is_readable(&self) -> bool {
        matches!(self, Self::Online | Self::Degraded | Self::Draining)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterNode {
    /// Stable logical slot identity used by placement and immutable metadata.
    #[serde(rename = "slot_id")]
    pub id: String,
    /// Replaceable physical process or machine currently bound to this slot.
    pub instance_id: String,
    /// Operational epoch at which `instance_id` was bound to `id`.
    pub binding_epoch: u64,
    pub endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_endpoint: Option<String>,
    #[serde(default)]
    pub state: Option<NodeState>,
    #[serde(default = "default_weight")]
    pub weight: u32,
    pub storage_roots: Vec<StorageRoot>,
    #[serde(default = "default_joined_epoch")]
    pub joined_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub used_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_unix: Option<i64>,
}

fn default_weight() -> u32 {
    1
}

fn default_joined_epoch() -> u64 {
    1
}

impl ClusterNode {
    pub fn effective_state(&self) -> NodeState {
        self.state.clone().unwrap_or(NodeState::Online)
    }

    pub fn is_local(&self, node_id: &str) -> bool {
        self.instance_id == node_id
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReserveNode {
    pub instance_id: String,
    pub endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_endpoint: Option<String>,
    #[serde(default)]
    pub state: Option<NodeState>,
    pub storage_roots: Vec<StorageRoot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_unix: Option<i64>,
}

impl ReserveNode {
    pub fn effective_state(&self) -> NodeState {
        self.state.clone().unwrap_or(NodeState::Online)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetiredInstance {
    pub instance_id: String,
    pub former_slot_id: String,
    pub fenced_at_binding_epoch: u64,
    pub endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_endpoint: Option<String>,
    pub storage_roots: Vec<StorageRoot>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterMembership {
    pub format_version: u32,
    pub cluster_id: String,
    /// Topology epoch: changes only for complete server-pool contracts.
    pub epoch: u64,
    /// Binding epoch: changes only for logical-slot replacement.
    pub binding_epoch: u64,
    pub pools: Vec<ServerPool>,
    /// Active logical slots and their current physical bindings.
    #[serde(rename = "slot_bindings")]
    pub nodes: Vec<ClusterNode>,
    /// Unassigned physical instances. They never participate in placement.
    #[serde(default)]
    pub reserve_nodes: Vec<ReserveNode>,
    /// Fenced former bindings retained for split-brain rejection and audit.
    #[serde(default)]
    pub retired_instances: Vec<RetiredInstance>,
}

impl ClusterMembership {
    pub fn validate(&self) -> Result<(), AppError> {
        if self.format_version != N2_MEMBERSHIP_FORMAT_VERSION {
            return Err(AppError::BadRequest(format!(
                "membership format {} is not N2 3.4 format {}",
                self.format_version, N2_MEMBERSHIP_FORMAT_VERSION
            )));
        }
        if self.cluster_id.trim().is_empty() || self.epoch == 0 || self.binding_epoch == 0 {
            return Err(AppError::BadRequest(
                "membership requires a cluster_id, positive topology epoch, and positive binding epoch"
                    .into(),
            ));
        }
        if self.pools.is_empty() {
            return Err(AppError::BadRequest(
                "N2 3.4 membership requires at least one server pool".into(),
            ));
        }

        let mut node_ids = BTreeSet::new();
        let mut instance_ids = BTreeSet::new();
        let mut root_ids = BTreeSet::new();
        for node in &self.nodes {
            if node.id.trim().is_empty()
                || node.instance_id.trim().is_empty()
                || node.binding_epoch == 0
                || node.binding_epoch > self.binding_epoch
                || !node_ids.insert(node.id.clone())
            {
                return Err(AppError::BadRequest(format!(
                    "membership contains an invalid or duplicate logical slot ID '{}'.",
                    node.id
                )));
            }
            if !instance_ids.insert(node.instance_id.clone()) {
                return Err(AppError::BadRequest(format!(
                    "physical instance '{}' is bound more than once",
                    node.instance_id
                )));
            }
            if node.endpoint.trim().is_empty() || node.storage_roots.is_empty() {
                return Err(AppError::BadRequest(format!(
                    "logical slot '{}' requires an endpoint and at least one storage root",
                    node.id
                )));
            }
            for root in &node.storage_roots {
                root.validate()?;
                if !root_ids.insert(root.id.clone()) {
                    return Err(AppError::BadRequest(format!(
                        "storage root ID '{}' is assigned to more than one logical node",
                        root.id
                    )));
                }
            }
        }
        for reserve in &self.reserve_nodes {
            if reserve.instance_id.trim().is_empty()
                || reserve.endpoint.trim().is_empty()
                || reserve.storage_roots.is_empty()
                || !instance_ids.insert(reserve.instance_id.clone())
            {
                return Err(AppError::BadRequest(format!(
                    "reserve instance '{}' is invalid or assigned more than once",
                    reserve.instance_id
                )));
            }
            for root in &reserve.storage_roots {
                root.validate()?;
                if !root_ids.insert(root.id.clone()) {
                    return Err(AppError::BadRequest(format!(
                        "storage root ID '{}' is assigned more than once",
                        root.id
                    )));
                }
            }
        }
        for retired in &self.retired_instances {
            if retired.instance_id.trim().is_empty()
                || retired.former_slot_id.trim().is_empty()
                || retired.fenced_at_binding_epoch == 0
                || retired.fenced_at_binding_epoch > self.binding_epoch
                || !instance_ids.insert(retired.instance_id.clone())
            {
                return Err(AppError::BadRequest(format!(
                    "retired instance '{}' has an invalid or duplicate fence record",
                    retired.instance_id
                )));
            }
        }

        let mut pool_ids = BTreeSet::new();
        let mut generations = BTreeSet::new();
        let mut assigned_nodes = BTreeSet::new();
        for pool in &self.pools {
            pool.validate()?;
            if !pool_ids.insert(pool.id.clone()) || !generations.insert(pool.generation) {
                return Err(AppError::BadRequest(format!(
                    "server pool '{}' has a duplicate ID or generation",
                    pool.id
                )));
            }
            for node_id in &pool.node_ids {
                if !node_ids.contains(node_id) {
                    return Err(AppError::BadRequest(format!(
                        "server pool '{}' references unknown node '{}'",
                        pool.id, node_id
                    )));
                }
                if !assigned_nodes.insert(node_id.clone()) {
                    return Err(AppError::BadRequest(format!(
                        "node '{}' is assigned to more than one server pool",
                        node_id
                    )));
                }
            }
        }
        if assigned_nodes.len() != node_ids.len() {
            return Err(AppError::BadRequest(
                "every active N2 3.4 logical slot must belong to exactly one server pool".into(),
            ));
        }
        Ok(())
    }

    pub fn pool(&self, pool_id: &str) -> Option<&ServerPool> {
        self.pools.iter().find(|pool| pool.id == pool_id)
    }

    pub fn pool_for_node(&self, node_id: &str) -> Option<&ServerPool> {
        self.pools
            .iter()
            .find(|pool| pool.node_ids.iter().any(|candidate| candidate == node_id))
    }

    pub fn node_for_instance(&self, instance_id: &str) -> Option<&ClusterNode> {
        self.nodes
            .iter()
            .find(|node| node.instance_id == instance_id)
    }

    pub fn reserve(&self, instance_id: &str) -> Option<&ReserveNode> {
        self.reserve_nodes
            .iter()
            .find(|node| node.instance_id == instance_id)
    }

    pub fn is_fenced_instance(&self, instance_id: &str) -> bool {
        self.retired_instances
            .iter()
            .any(|node| node.instance_id == instance_id)
    }

    pub fn newest_writable_pool(&self) -> Option<&ServerPool> {
        self.pools
            .iter()
            .filter(|pool| pool.writable())
            .max_by_key(|pool| pool.generation)
    }

    pub fn nodes_for_pool(&self, pool_id: &str) -> Vec<ClusterNode> {
        let Some(pool) = self.pool(pool_id) else {
            return Vec::new();
        };
        let by_id = self
            .nodes
            .iter()
            .map(|node| (node.id.as_str(), node))
            .collect::<BTreeMap<_, _>>();
        pool.node_ids
            .iter()
            .filter_map(|id| by_id.get(id.as_str()).copied().cloned())
            .collect()
    }

    pub fn active_write_nodes_for_pool(&self, pool_id: &str) -> Vec<ClusterNode> {
        self.nodes_for_pool(pool_id)
            .into_iter()
            .filter(|node| node.effective_state().accepts_new_writes())
            .collect()
    }

    pub fn readable_nodes_for_pool(&self, pool_id: &str) -> Vec<ClusterNode> {
        self.nodes_for_pool(pool_id)
            .into_iter()
            .filter(|node| node.effective_state().is_readable())
            .collect()
    }

    pub fn active_newest_pool_nodes(&self) -> Vec<ClusterNode> {
        self.newest_writable_pool()
            .map(|pool| self.active_write_nodes_for_pool(&pool.id))
            .unwrap_or_default()
    }

    pub fn validate_minimum_topology(&self) -> Result<(), AppError> {
        for pool in &self.pools {
            let nodes = self.nodes_for_pool(&pool.id).len();
            if nodes != pool.policy.object_width() {
                return Err(AppError::ServiceUnavailable(format!(
                    "pool '{}' EC {}+{} requires exactly {} distinct nodes, membership has {}",
                    pool.id,
                    pool.policy.object_data_shards,
                    pool.policy.object_parity_shards,
                    pool.policy.object_width(),
                    nodes
                )));
            }
        }
        Ok(())
    }

    pub fn active_write_nodes(&self) -> Vec<ClusterNode> {
        self.nodes
            .iter()
            .filter(|node| node.effective_state().accepts_new_writes())
            .cloned()
            .collect()
    }

    pub fn readable_nodes(&self) -> Vec<ClusterNode> {
        self.nodes
            .iter()
            .filter(|node| node.effective_state().is_readable())
            .cloned()
            .collect()
    }
}

#[cfg(test)]
#[path = "membership/tests.rs"]
mod tests;
