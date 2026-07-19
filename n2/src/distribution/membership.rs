use crate::distribution::layout::ClusterLayoutPolicy;
use crate::enums::app_error_enum::AppError;
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

const CLUSTER_NODE_PROBE_TIMEOUT: Duration = Duration::from_millis(1_500);
pub const N2_MEMBERSHIP_FORMAT_VERSION: u32 = 3;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageRoot {
    pub id: String,
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
            "N2 3.0 requires at least one storage_roots entry".into(),
        ));
    }
    Ok(roots)
}

impl StorageRoot {
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
                "N2 3.0 pool EC requires at least 1 data and 1 parity shard".into(),
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
        if self.node_ids.len() < self.policy.object_width() {
            return Err(AppError::BadRequest(format!(
                "server pool '{}' EC {}+{} requires at least {} nodes, but records {}",
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
    pub id: String,
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
        self.id == node_id
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterMembership {
    pub format_version: u32,
    pub cluster_id: String,
    pub epoch: u64,
    pub pools: Vec<ServerPool>,
    pub nodes: Vec<ClusterNode>,
}

impl ClusterMembership {
    pub fn validate(&self) -> Result<(), AppError> {
        if self.format_version != N2_MEMBERSHIP_FORMAT_VERSION {
            return Err(AppError::BadRequest(format!(
                "membership format {} is not N2 3.0 format {}",
                self.format_version, N2_MEMBERSHIP_FORMAT_VERSION
            )));
        }
        if self.cluster_id.trim().is_empty() || self.epoch == 0 {
            return Err(AppError::BadRequest(
                "membership requires a cluster_id and positive epoch".into(),
            ));
        }
        if self.pools.is_empty() {
            return Err(AppError::BadRequest(
                "N2 3.0 membership requires at least one server pool".into(),
            ));
        }

        let mut node_ids = BTreeSet::new();
        let mut root_ids = BTreeSet::new();
        for node in &self.nodes {
            if node.id.trim().is_empty() || !node_ids.insert(node.id.clone()) {
                return Err(AppError::BadRequest(format!(
                    "membership contains an empty or duplicate node ID '{}'.",
                    node.id
                )));
            }
            if node.endpoint.trim().is_empty() || node.storage_roots.is_empty() {
                return Err(AppError::BadRequest(format!(
                    "node '{}' requires an endpoint and at least one storage root",
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
                "every N2 3.0 node must belong to exactly one server pool".into(),
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
            if nodes < pool.policy.object_width() {
                return Err(AppError::ServiceUnavailable(format!(
                    "pool '{}' EC {}+{} requires {} distinct nodes, membership has {}",
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
        &runtime.local_node_id,
        runtime.membership.active_write_nodes(),
    )
    .await
}

pub async fn live_active_write_nodes_for_pool(
    runtime: &ClusterRuntime,
    pool_id: &str,
) -> Vec<ClusterNode> {
    live_filter_nodes(
        &runtime.local_node_id,
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
    live_filter_nodes(&runtime.local_node_id, runtime.membership.readable_nodes()).await
}

#[derive(Clone, Debug)]
pub struct ClusterRuntime {
    pub local_node_id: String,
    pub local_pool_id: String,
    pub local_storage_roots: Vec<StorageRoot>,
    pub membership_path: PathBuf,
    pub layout_policy: ClusterLayoutPolicy,
    pub membership: ClusterMembership,
    pub auto_rebalance: bool,
}

static CLUSTER_RUNTIME: OnceLock<Arc<ClusterRuntime>> = OnceLock::new();

pub fn active_cluster_runtime() -> Result<Arc<ClusterRuntime>, AppError> {
    CLUSTER_RUNTIME.get().cloned().ok_or_else(|| {
        AppError::InternalServerError("N2 cluster runtime is not initialized".into())
    })
}

pub fn try_active_cluster_runtime() -> Option<Arc<ClusterRuntime>> {
    CLUSTER_RUNTIME.get().cloned()
}

fn parse_usize(raw: Option<&str>, key: &str, default_value: usize) -> Result<usize, AppError> {
    match raw.map(str::trim) {
        None | Some("") => Ok(default_value),
        Some("auto") => Err(AppError::BadRequest(format!(
            "{key}=auto is not valid in N2 3.0; pool EC is static"
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
        pools: vec![ServerPool {
            id: pool_id.to_string(),
            generation: pool_generation,
            policy,
            state: PoolState::Writable,
            node_ids: vec![node_id.to_string()],
        }],
        nodes: vec![ClusterNode {
            id: node_id.to_string(),
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
                "decode N2 3.0 membership file '{}': {e}",
                path.display()
            ))
        })?;
        membership.validate()?;
        let pool = membership.pool(pool_id).ok_or_else(|| {
            AppError::Conflict(format!(
                "local pool '{}' is absent from membership; add the complete pool explicitly",
                pool_id
            ))
        })?;
        if pool.generation != pool_generation || pool.policy != policy {
            return Err(AppError::Conflict(format!(
                "local pool contract {} generation {} {:?} does not match membership generation {} {:?}",
                pool_id, pool_generation, policy, pool.generation, pool.policy
            )));
        }
        let now = Utc::now().timestamp();
        let local = membership
            .nodes
            .iter_mut()
            .find(|node| node.id == node_id)
            .ok_or_else(|| {
                AppError::Conflict(format!(
                    "node '{}' is absent from membership; membership expansion must be explicit",
                    node_id
                ))
            })?;
        if local.storage_roots != storage_roots {
            return Err(AppError::Conflict(format!(
                "configured roots for node '{}' do not match membership",
                node_id
            )));
        }
        local.endpoint = endpoint.to_string();
        local.native_endpoint = Some(native_endpoint.to_string());
        local.state.get_or_insert(NodeState::Online);
        local.last_seen_unix = Some(now);
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

fn write_membership(path: &Path, membership: &ClusterMembership) -> Result<(), AppError> {
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
    let temp = path.with_extension(format!("tmp.{}", std::process::id()));
    fs::write(&temp, bytes)
        .map_err(|e| AppError::Io(e, format!("write N2 membership file {}", temp.display())))?;
    fs::rename(&temp, path)
        .map_err(|e| AppError::Io(e, format!("publish N2 membership file {}", path.display())))
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
            "node_role was removed in N2 3.0; every server-pool node is unified".into(),
        ));
    }
    if section
        .and_then(|s| s.get("namespace_redundancy"))
        .is_some()
    {
        return Err(AppError::BadRequest(
            "namespace_redundancy was removed in N2 3.0; NCNX replicas equal parity + one".into(),
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

    let runtime = Arc::new(ClusterRuntime {
        local_node_id: node_id,
        local_pool_id: pool_id,
        local_storage_roots: local_storage_roots.to_vec(),
        membership_path,
        layout_policy,
        membership,
        auto_rebalance: false,
    });
    CLUSTER_RUNTIME.set(runtime.clone()).map_err(|_| {
        AppError::InternalServerError("N2 cluster runtime initialized twice".into())
    })?;
    Ok(runtime)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str) -> ClusterNode {
        ClusterNode {
            id: id.into(),
            endpoint: format!("https://{id}:51001"),
            native_endpoint: Some(format!("n2://{id}:51002")),
            state: Some(NodeState::Online),
            weight: 1,
            storage_roots: vec![StorageRoot {
                id: format!("root-{id}"),
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
            format_version: 3,
            cluster_id: "cluster-a".into(),
            epoch: 1,
            pools: vec![ServerPool {
                id: "pool-a".into(),
                generation: 1,
                policy: policy.clone(),
                state: PoolState::Writable,
                node_ids: vec!["a".into(), "b".into(), "c".into()],
            }],
            nodes: vec![node("a"), node("b"), node("c")],
        };
        membership.validate().expect("valid membership");
        assert_eq!(policy.namespace_replicas(), 2);
        assert_eq!(membership.newest_writable_pool().unwrap().id, "pool-a");
    }

    #[test]
    fn rejects_node_assigned_to_two_pools() {
        let policy = ClusterPolicy {
            object_data_shards: 1,
            object_parity_shards: 1,
        };
        let membership = ClusterMembership {
            format_version: 3,
            cluster_id: "cluster-a".into(),
            epoch: 2,
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
        };
        assert!(membership.validate().is_err());
    }
}
