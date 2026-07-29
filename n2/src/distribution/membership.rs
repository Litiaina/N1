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

const CLUSTER_NODE_PROBE_TIMEOUT: Duration = Duration::from_millis(1_500);
pub const N2_MEMBERSHIP_FORMAT_VERSION: u32 = 5;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageRoot {
    /// Logical root ID recorded in immutable object placement.
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
            "N2 3.3 requires at least one storage_roots entry".into(),
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
                "N2 3.3 pool EC requires at least 1 data and 1 parity shard".into(),
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
                "membership format {} is not N2 3.3 format {}",
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
                "N2 3.3 membership requires at least one server pool".into(),
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
                "every active N2 3.3 logical slot must belong to exactly one server pool".into(),
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

fn native_socket_addr(endpoint: &str) -> Option<String> {
    endpoint
        .trim()
        .strip_prefix("n2://")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

async fn native_endpoint_is_reachable(endpoint: &str) -> bool {
    let Some(addr) = native_socket_addr(endpoint) else {
        return false;
    };
    matches!(
        timeout(CLUSTER_NODE_PROBE_TIMEOUT, TcpStream::connect(addr)).await,
        Ok(Ok(_))
    )
}

async fn http_endpoint_is_reachable(endpoint: &str) -> bool {
    let url = format!("{}/noa/v1/ping", endpoint.trim_end_matches('/'));
    matches!(
        CLUSTER_PROBE_HTTP_CLIENT.get(url).send().await,
        Ok(response) if response.status().is_success()
    )
}

pub async fn cluster_node_is_reachable(local_node_id: &str, node: &ClusterNode) -> bool {
    if node.is_local(local_node_id) {
        return true;
    }
    if let Some(native_endpoint) = node
        .native_endpoint
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        return native_endpoint_is_reachable(native_endpoint).await;
    }
    http_endpoint_is_reachable(&node.endpoint).await
}

async fn live_filter_nodes(local_node_id: &str, candidates: Vec<ClusterNode>) -> Vec<ClusterNode> {
    let probes = candidates.into_iter().map(|node| {
        let local_node_id = local_node_id.to_string();
        async move {
            cluster_node_is_reachable(&local_node_id, &node)
                .await
                .then_some(node)
        }
    });
    join_all(probes).await.into_iter().flatten().collect()
}

pub async fn live_active_write_nodes(runtime: &ClusterRuntime) -> Vec<ClusterNode> {
    live_filter_nodes(
        &runtime.local_instance_id,
        runtime.membership.active_write_nodes(),
    )
    .await
}

pub async fn live_active_write_nodes_for_pool(
    runtime: &ClusterRuntime,
    pool_id: &str,
) -> Vec<ClusterNode> {
    live_filter_nodes(
        &runtime.local_instance_id,
        runtime.membership.active_write_nodes_for_pool(pool_id),
    )
    .await
}

pub async fn live_newest_pool_write_nodes(runtime: &ClusterRuntime) -> Vec<ClusterNode> {
    let Some(pool) = runtime.membership.newest_writable_pool() else {
        return Vec::new();
    };
    live_active_write_nodes_for_pool(runtime, &pool.id).await
}

pub async fn live_readable_nodes(runtime: &ClusterRuntime) -> Vec<ClusterNode> {
    live_filter_nodes(
        &runtime.local_instance_id,
        runtime.membership.readable_nodes(),
    )
    .await
}

#[derive(Clone, Debug)]
pub struct ClusterRuntime {
    /// Logical slot ID while active; physical instance ID while unassigned reserve.
    pub local_node_id: String,
    pub local_instance_id: String,
    pub local_is_reserve: bool,
    pub local_pool_id: String,
    pub local_storage_roots: Vec<StorageRoot>,
    pub membership_path: PathBuf,
    pub layout_policy: ClusterLayoutPolicy,
    pub membership: ClusterMembership,
    pub auto_rebalance: bool,
}

static CLUSTER_RUNTIME: OnceLock<ArcSwap<ClusterRuntime>> = OnceLock::new();

pub fn active_cluster_runtime() -> Result<Arc<ClusterRuntime>, AppError> {
    CLUSTER_RUNTIME
        .get()
        .map(ArcSwap::load_full)
        .ok_or_else(|| {
            AppError::InternalServerError("N2 cluster runtime is not initialized".into())
        })
}

pub fn try_active_cluster_runtime() -> Option<Arc<ClusterRuntime>> {
    CLUSTER_RUNTIME.get().map(ArcSwap::load_full)
}

pub fn publish_cluster_runtime(runtime: Arc<ClusterRuntime>) -> Result<(), AppError> {
    let current = CLUSTER_RUNTIME.get().ok_or_else(|| {
        AppError::InternalServerError("N2 cluster runtime is not initialized".into())
    })?;
    current.store(runtime);
    Ok(())
}

fn parse_usize(raw: Option<&str>, key: &str, default_value: usize) -> Result<usize, AppError> {
    match raw.map(str::trim) {
        None | Some("") => Ok(default_value),
        Some("auto") => Err(AppError::BadRequest(format!(
            "{key}=auto is not valid in N2 3.3; pool EC is static"
        ))),
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| AppError::BadRequest(format!("{key} must be a positive integer"))),
    }
}

fn parse_bool(raw: Option<&str>, default_value: bool) -> bool {
    match raw.map(str::trim) {
        None | Some("") => default_value,
        Some(value) if matches!(value, "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON") => true,
        Some(value) if matches!(value, "0" | "false" | "FALSE" | "no" | "NO" | "off" | "OFF") => {
            false
        }
        Some(_) => default_value,
    }
}

fn resolve_membership_path(raw: &str, fs_storage_path: &str) -> PathBuf {
    let path = PathBuf::from(raw.trim());
    if path.is_absolute() {
        path
    } else {
        PathBuf::from(fs_storage_path).join(path)
    }
}

fn physical_root_sets_match(configured: &[StorageRoot], bound: &[StorageRoot]) -> bool {
    let mut configured = configured
        .iter()
        .map(|root| (root.physical_id().to_string(), root.path.clone()))
        .collect::<Vec<_>>();
    let mut bound = bound
        .iter()
        .map(|root| (root.physical_id().to_string(), root.path.clone()))
        .collect::<Vec<_>>();
    configured.sort();
    bound.sort();
    configured == bound
}

fn resolve_startup_local_binding(
    membership: &ClusterMembership,
    instance_id: &str,
) -> Result<(String, String, bool, Vec<StorageRoot>), AppError> {
    if let Some(binding) = membership.node_for_instance(instance_id) {
        let pool = membership.pool_for_node(&binding.id).ok_or_else(|| {
            AppError::Conflict(format!(
                "local logical slot '{}' is not assigned to a server pool",
                binding.id
            ))
        })?;
        return Ok((
            binding.id.clone(),
            pool.id.clone(),
            false,
            binding.storage_roots.clone(),
        ));
    }
    if let Some(reserve) = membership.reserve(instance_id) {
        return Ok((
            instance_id.to_string(),
            String::new(),
            true,
            reserve.storage_roots.clone(),
        ));
    }
    Err(AppError::Conflict(format!(
        "physical instance '{instance_id}' has no active or reserve membership"
    )))
}

fn local_membership(
    node_id: &str,
    pool_id: &str,
    pool_generation: u64,
    endpoint: &str,
    native_endpoint: &str,
    storage_roots: &[StorageRoot],
    policy: ClusterPolicy,
) -> ClusterMembership {
    ClusterMembership {
        format_version: N2_MEMBERSHIP_FORMAT_VERSION,
        cluster_id: "n2-local".to_string(),
        epoch: 1,
        binding_epoch: 1,
        pools: vec![ServerPool {
            id: pool_id.to_string(),
            generation: pool_generation,
            policy,
            state: PoolState::Writable,
            node_ids: vec![node_id.to_string()],
        }],
        nodes: vec![ClusterNode {
            id: node_id.to_string(),
            instance_id: node_id.to_string(),
            binding_epoch: 1,
            endpoint: endpoint.to_string(),
            native_endpoint: Some(native_endpoint.to_string()),
            state: Some(NodeState::Online),
            weight: 1,
            storage_roots: storage_roots.to_vec(),
            joined_epoch: 1,
            capacity_bytes: None,
            used_bytes: None,
            last_seen_unix: Some(Utc::now().timestamp()),
        }],
        reserve_nodes: Vec::new(),
        retired_instances: Vec::new(),
    }
}

fn load_or_create_membership(
    path: &Path,
    node_id: &str,
    pool_id: &str,
    pool_generation: u64,
    endpoint: &str,
    native_endpoint: &str,
    storage_roots: &[StorageRoot],
    policy: ClusterPolicy,
) -> Result<ClusterMembership, AppError> {
    if path.exists() {
        let bytes = fs::read(path)
            .map_err(|e| AppError::Io(e, format!("read N2 membership file {}", path.display())))?;
        let mut membership: ClusterMembership = serde_json::from_slice(&bytes).map_err(|e| {
            AppError::InternalServerError(format!(
                "decode N2 3.3 membership file '{}': {e}",
                path.display()
            ))
        })?;
        membership.validate()?;
        let now = Utc::now().timestamp();
        if let Some(local_index) = membership
            .nodes
            .iter()
            .position(|node| node.instance_id == node_id)
        {
            let local_slot_id = membership.nodes[local_index].id.clone();
            let bound_pool = membership.pool_for_node(&local_slot_id).ok_or_else(|| {
                AppError::Conflict(format!(
                    "logical slot '{}' is not assigned to a server pool",
                    local_slot_id
                ))
            })?;
            if bound_pool.id != pool_id
                || bound_pool.generation != pool_generation
                || bound_pool.policy != policy
            {
                return Err(AppError::Conflict(format!(
                    "local slot contract {} generation {} {:?} does not match membership pool {} generation {} {:?}",
                    pool_id,
                    pool_generation,
                    policy,
                    bound_pool.id,
                    bound_pool.generation,
                    bound_pool.policy
                )));
            }
            let local = &mut membership.nodes[local_index];
            if !physical_root_sets_match(storage_roots, &local.storage_roots) {
                return Err(AppError::Conflict(format!(
                    "configured roots for physical instance '{}' do not match logical slot '{}'",
                    node_id, local.id
                )));
            }
            local.endpoint = endpoint.to_string();
            local.native_endpoint = Some(native_endpoint.to_string());
            local.state.get_or_insert(NodeState::Online);
            local.last_seen_unix = Some(now);
        } else if let Some(local) = membership
            .reserve_nodes
            .iter_mut()
            .find(|node| node.instance_id == node_id)
        {
            if !physical_root_sets_match(storage_roots, &local.storage_roots) {
                return Err(AppError::Conflict(format!(
                    "configured roots for reserve instance '{}' do not match membership",
                    node_id
                )));
            }
            local.endpoint = endpoint.to_string();
            local.native_endpoint = Some(native_endpoint.to_string());
            local.state.get_or_insert(NodeState::Online);
            local.last_seen_unix = Some(now);
        } else if membership.is_fenced_instance(node_id) {
            return Err(AppError::Conflict(format!(
                "physical instance '{}' is fenced and cannot rejoin without explicit reserve re-enrollment",
                node_id
            )));
        } else {
            return Err(AppError::Conflict(format!(
                "physical instance '{}' is absent from active slots and the reserve set",
                node_id
            )));
        }
        membership.validate()?;
        write_membership(path, &membership)?;
        return Ok(membership);
    }

    let membership = local_membership(
        node_id,
        pool_id,
        pool_generation,
        endpoint,
        native_endpoint,
        storage_roots,
        policy,
    );
    write_membership(path, &membership)?;
    Ok(membership)
}

pub fn write_membership(path: &Path, membership: &ClusterMembership) -> Result<(), AppError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            AppError::Io(
                e,
                format!("create N2 membership directory {}", parent.display()),
            )
        })?;
    }
    let bytes = serde_json::to_vec_pretty(membership)
        .map_err(|e| AppError::InternalServerError(format!("encode N2 membership file: {e}")))?;
    // Promotion/reconciliation tasks can publish the same local membership
    // concurrently. A process-only suffix lets one task rename another task's
    // temporary file. Keep each atomic publisher independent without adding a
    // lock to the membership fast path.
    let temp = path.with_extension(format!("tmp.{}.{}", std::process::id(), Ulid::new()));
    {
        use std::io::Write;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp)
            .map_err(|e| AppError::Io(e, format!("write N2 membership file {}", temp.display())))?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|e| AppError::Io(e, format!("sync N2 membership file {}", temp.display())))?;
    }
    fs::rename(&temp, path)
        .map_err(|e| AppError::Io(e, format!("publish N2 membership file {}", path.display())))?;
    if let Some(parent) = path.parent() {
        fs::File::open(parent)
            .and_then(|dir| dir.sync_all())
            .map_err(|e| {
                AppError::Io(
                    e,
                    format!("sync N2 membership directory {}", parent.display()),
                )
            })?;
    }
    Ok(())
}

pub fn init_cluster_runtime(
    config: &Ini,
    fs_storage_path: &str,
    local_storage_roots: &[StorageRoot],
    https_endpoint: &str,
    native_endpoint: &str,
) -> Result<Arc<ClusterRuntime>, AppError> {
    let section = config.section(Some("cluster_config"));
    if section.and_then(|s| s.get("node_role")).is_some() {
        return Err(AppError::BadRequest(
            "node_role was removed in N2 3.3; every server-pool node is unified".into(),
        ));
    }
    if section
        .and_then(|s| s.get("namespace_redundancy"))
        .is_some()
    {
        return Err(AppError::BadRequest(
            "namespace_redundancy was removed in N2 3.3; NCNX replicas equal parity + one".into(),
        ));
    }
    let node_id = section
        .and_then(|s| s.get("node_id"))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("node-1")
        .to_string();
    let pool_id = section
        .and_then(|s| s.get("pool_id"))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("pool-1")
        .to_string();
    let pool_generation = parse_usize(
        section.and_then(|s| s.get("pool_generation")),
        "pool_generation",
        1,
    )? as u64;
    let node_endpoint = section
        .and_then(|s| s.get("node_endpoint"))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(https_endpoint);
    let native_endpoint = section
        .and_then(|s| s.get("native_endpoint"))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(native_endpoint);
    let membership_file = section
        .and_then(|s| s.get("membership_file"))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("n2_membership.json");
    let membership_path = resolve_membership_path(membership_file, fs_storage_path);
    let policy = ClusterPolicy {
        object_data_shards: parse_usize(
            section.and_then(|s| s.get("object_data_shards")),
            "object_data_shards",
            1,
        )?,
        object_parity_shards: parse_usize(
            section.and_then(|s| s.get("object_parity_shards")),
            "object_parity_shards",
            1,
        )?,
    };
    policy.validate()?;
    if parse_bool(section.and_then(|s| s.get("auto_rebalance")), false) {
        return Err(AppError::BadRequest(
            "auto_rebalance=true is unsupported: N2 preserves exact object layouts".into(),
        ));
    }
    let membership = load_or_create_membership(
        &membership_path,
        &node_id,
        &pool_id,
        pool_generation,
        node_endpoint,
        native_endpoint,
        local_storage_roots,
        policy,
    )?;
    membership.validate_minimum_topology()?;
    let write_policy = membership
        .newest_writable_pool()
        .ok_or_else(|| {
            AppError::ServiceUnavailable("membership has no writable server pool".into())
        })?
        .policy
        .clone();
    let layout_policy = ClusterLayoutPolicy {
        data_shards: write_policy.object_data_shards,
        parity_shards: write_policy.object_parity_shards,
    };
    let (local_node_id, local_pool_id, local_is_reserve, runtime_storage_roots) =
        resolve_startup_local_binding(&membership, &node_id)?;
    let runtime = Arc::new(ClusterRuntime {
        local_node_id,
        local_instance_id: node_id,
        local_is_reserve,
        local_pool_id,
        // A promoted reserve keeps its physical device but inherits the
        // replaced slot's logical root IDs. Reload those recorded IDs after a
        // restart so peer placement validation remains identical to the live
        // promotion path.
        local_storage_roots: runtime_storage_roots,
        membership_path,
        layout_policy,
        membership,
        auto_rebalance: false,
    });
    CLUSTER_RUNTIME
        .set(ArcSwap::from(runtime.clone()))
        .map_err(|_| {
            AppError::InternalServerError("N2 cluster runtime initialized twice".into())
        })?;
    Ok(runtime)
}

pub fn commit_local_membership(
    membership: ClusterMembership,
) -> Result<Arc<ClusterRuntime>, AppError> {
    membership.validate()?;
    membership.validate_minimum_topology()?;
    let current = active_cluster_runtime()?;
    let (local_node_id, local_pool_id, local_is_reserve, local_storage_roots) =
        if let Some(binding) = membership.node_for_instance(&current.local_instance_id) {
            let pool = membership.pool_for_node(&binding.id).ok_or_else(|| {
                AppError::Conflict(format!(
                    "local logical slot '{}' is not assigned to a server pool",
                    binding.id
                ))
            })?;
            (
                binding.id.clone(),
                pool.id.clone(),
                false,
                binding.storage_roots.clone(),
            )
        } else if let Some(reserve) = membership.reserve(&current.local_instance_id) {
            (
                current.local_instance_id.clone(),
                String::new(),
                true,
                reserve.storage_roots.clone(),
            )
        } else if membership.is_fenced_instance(&current.local_instance_id) {
            (
                current.local_instance_id.clone(),
                String::new(),
                true,
                current.local_storage_roots.clone(),
            )
        } else {
            return Err(AppError::Conflict(format!(
                "local physical instance '{}' is absent from committed membership",
                current.local_instance_id
            )));
        };
    let write_policy = membership
        .newest_writable_pool()
        .ok_or_else(|| AppError::ServiceUnavailable("membership has no writable pool".into()))?
        .policy
        .clone();
    let next = Arc::new(ClusterRuntime {
        local_node_id,
        local_instance_id: current.local_instance_id.clone(),
        local_is_reserve,
        local_pool_id,
        local_storage_roots,
        membership_path: current.membership_path.clone(),
        layout_policy: ClusterLayoutPolicy {
            data_shards: write_policy.object_data_shards,
            parity_shards: write_policy.object_parity_shards,
        },
        membership,
        auto_rebalance: false,
    });
    write_membership(&next.membership_path, &next.membership)?;
    publish_cluster_runtime(next.clone())?;
    Ok(next)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str) -> ClusterNode {
        ClusterNode {
            id: id.into(),
            instance_id: id.into(),
            binding_epoch: 1,
            endpoint: format!("https://{id}:51001"),
            native_endpoint: Some(format!("n2://{id}:51002")),
            state: Some(NodeState::Online),
            weight: 1,
            storage_roots: vec![StorageRoot {
                id: format!("root-{id}"),
                physical_id: None,
                path: format!("/disk-{id}"),
            }],
            joined_epoch: 1,
            capacity_bytes: None,
            used_bytes: None,
            last_seen_unix: None,
        }
    }

    #[test]
    fn validates_unified_static_pool_and_derives_ncnx_replicas() {
        let policy = ClusterPolicy {
            object_data_shards: 2,
            object_parity_shards: 1,
        };
        let membership = ClusterMembership {
            format_version: N2_MEMBERSHIP_FORMAT_VERSION,
            cluster_id: "cluster-a".into(),
            epoch: 1,
            binding_epoch: 1,
            pools: vec![ServerPool {
                id: "pool-a".into(),
                generation: 1,
                policy: policy.clone(),
                state: PoolState::Writable,
                node_ids: vec!["a".into(), "b".into(), "c".into()],
            }],
            nodes: vec![node("a"), node("b"), node("c")],
            reserve_nodes: Vec::new(),
            retired_instances: Vec::new(),
        };
        membership.validate().expect("valid membership");
        assert_eq!(policy.namespace_replicas(), 2);
        assert_eq!(membership.newest_writable_pool().unwrap().id, "pool-a");
    }

    #[test]
    fn rejects_pool_membership_wider_than_its_static_ec_contract() {
        let pool = ServerPool {
            id: "pool-a".into(),
            generation: 1,
            policy: ClusterPolicy {
                object_data_shards: 2,
                object_parity_shards: 1,
            },
            state: PoolState::Writable,
            node_ids: vec!["a".into(), "b".into(), "c".into(), "d".into()],
        };
        assert!(pool.validate().is_err());
    }

    #[test]
    fn rejects_retired_membership_format() {
        let mut membership = ClusterMembership {
            format_version: N2_MEMBERSHIP_FORMAT_VERSION,
            cluster_id: "cluster-a".into(),
            epoch: 1,
            binding_epoch: 1,
            pools: vec![ServerPool {
                id: "pool-a".into(),
                generation: 1,
                policy: ClusterPolicy {
                    object_data_shards: 1,
                    object_parity_shards: 1,
                },
                state: PoolState::Writable,
                node_ids: vec!["a".into(), "b".into()],
            }],
            nodes: vec![node("a"), node("b")],
            reserve_nodes: Vec::new(),
            retired_instances: Vec::new(),
        };
        membership.validate().expect("format-v5 membership");
        membership.format_version = 4;
        assert!(membership.validate().is_err());
    }

    #[test]
    fn configured_physical_roots_match_a_promoted_logical_binding() {
        let configured = vec![StorageRoot {
            id: "reserve-device".into(),
            physical_id: None,
            path: "/disk-reserve".into(),
        }];
        let promoted = vec![StorageRoot {
            id: "logical-root-d".into(),
            physical_id: Some("reserve-device".into()),
            path: "/disk-reserve".into(),
        }];
        assert!(physical_root_sets_match(&configured, &promoted));

        let mut wrong_path = promoted.clone();
        wrong_path[0].path = "/other-device".into();
        assert!(!physical_root_sets_match(&configured, &wrong_path));
    }

    #[test]
    fn promoted_reserve_restart_restores_the_logical_slot_roots() {
        let promoted_root = StorageRoot {
            id: "logical-root-a".into(),
            physical_id: Some("reserve-device".into()),
            path: "/disk-reserve".into(),
        };
        let membership = ClusterMembership {
            format_version: N2_MEMBERSHIP_FORMAT_VERSION,
            cluster_id: "cluster-a".into(),
            epoch: 1,
            binding_epoch: 2,
            pools: vec![ServerPool {
                id: "pool-a".into(),
                generation: 1,
                policy: ClusterPolicy {
                    object_data_shards: 1,
                    object_parity_shards: 1,
                },
                state: PoolState::Writable,
                node_ids: vec!["slot-a".into(), "slot-b".into()],
            }],
            nodes: vec![
                ClusterNode {
                    id: "slot-a".into(),
                    instance_id: "reserve-instance".into(),
                    binding_epoch: 2,
                    endpoint: "https://reserve:51001".into(),
                    native_endpoint: Some("n2://reserve:51002".into()),
                    state: Some(NodeState::Online),
                    weight: 1,
                    storage_roots: vec![promoted_root.clone()],
                    joined_epoch: 1,
                    capacity_bytes: None,
                    used_bytes: None,
                    last_seen_unix: None,
                },
                node("slot-b"),
            ],
            reserve_nodes: Vec::new(),
            retired_instances: Vec::new(),
        };

        let (slot_id, pool_id, is_reserve, roots) =
            resolve_startup_local_binding(&membership, "reserve-instance")
                .expect("promoted reserve binding must resolve");
        assert_eq!(slot_id, "slot-a");
        assert_eq!(pool_id, "pool-a");
        assert!(!is_reserve);
        assert_eq!(roots, vec![promoted_root]);
    }

    #[test]
    fn rejects_node_assigned_to_two_pools() {
        let policy = ClusterPolicy {
            object_data_shards: 1,
            object_parity_shards: 1,
        };
        let membership = ClusterMembership {
            format_version: N2_MEMBERSHIP_FORMAT_VERSION,
            cluster_id: "cluster-a".into(),
            epoch: 2,
            binding_epoch: 1,
            pools: vec![
                ServerPool {
                    id: "a".into(),
                    generation: 1,
                    policy: policy.clone(),
                    state: PoolState::Sealed,
                    node_ids: vec!["n1".into(), "n2".into()],
                },
                ServerPool {
                    id: "b".into(),
                    generation: 2,
                    policy,
                    state: PoolState::Writable,
                    node_ids: vec!["n1".into(), "n3".into()],
                },
            ],
            nodes: vec![node("n1"), node("n2"), node("n3")],
            reserve_nodes: Vec::new(),
            retired_instances: Vec::new(),
        };
        assert!(membership.validate().is_err());
    }

    #[test]
    fn concurrent_membership_publishers_use_independent_atomic_files() {
        let root = std::env::temp_dir().join(format!("n2-membership-concurrency-{}", Ulid::new()));
        let path = Arc::new(root.join("n2_membership.json"));
        let policy = ClusterPolicy {
            object_data_shards: 1,
            object_parity_shards: 1,
        };
        let membership = ClusterMembership {
            format_version: N2_MEMBERSHIP_FORMAT_VERSION,
            cluster_id: "concurrent-cluster".into(),
            epoch: 1,
            binding_epoch: 1,
            pools: vec![ServerPool {
                id: "pool-a".into(),
                generation: 1,
                policy,
                state: PoolState::Writable,
                node_ids: vec!["a".into(), "b".into()],
            }],
            nodes: vec![node("a"), node("b")],
            reserve_nodes: Vec::new(),
            retired_instances: Vec::new(),
        };
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let writers = (0..8)
            .map(|_| {
                let path = path.clone();
                let membership = membership.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    write_membership(&path, &membership)
                })
            })
            .collect::<Vec<_>>();

        for writer in writers {
            writer
                .join()
                .expect("membership writer must not panic")
                .expect("concurrent membership publish must succeed");
        }

        let persisted: ClusterMembership =
            serde_json::from_slice(&fs::read(&*path).expect("read published membership"))
                .expect("decode published membership");
        persisted.validate().expect("published membership is valid");
        let leftovers = fs::read_dir(&root)
            .expect("read membership directory")
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name() != "n2_membership.json")
            .count();
        assert_eq!(leftovers, 0, "atomic membership temp files must not remain");
        fs::remove_dir_all(root).expect("remove membership test directory");
    }
}
