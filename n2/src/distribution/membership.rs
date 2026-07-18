use crate::distribution::layout::{
    ClusterLayoutMode, ClusterLayoutPolicy, DEFAULT_AUTO_MAX_DATA_SHARDS,
    DEFAULT_AUTO_MAX_PARITY_SHARDS,
};
use crate::enums::app_error_enum::AppError;
use chrono::Utc;
use futures_util::future::join_all;
use ini::Ini;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, OnceLock};
use tokio::net::TcpStream;
use tokio::time::{Duration, timeout};

const CLUSTER_NODE_PROBE_TIMEOUT: Duration = Duration::from_millis(1_500);
pub const N2_MEMBERSHIP_FORMAT_VERSION: u32 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    Namespace,
    Blob,
}

impl NodeRole {
    pub fn parse(value: &str) -> Result<Self, AppError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "namespace" => Ok(Self::Namespace),
            "blob" => Ok(Self::Blob),
            other => Err(AppError::BadRequest(format!(
                "node_role must be namespace or blob, got '{other}'"
            ))),
        }
    }
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Namespace => f.write_str("namespace"),
            Self::Blob => f.write_str("blob"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageRoot {
    pub id: String,
    pub path: String,
}

pub fn parse_storage_roots(raw: &str) -> Result<Vec<StorageRoot>, AppError> {
    let mut roots = Vec::new();
    let mut ids = std::collections::BTreeSet::new();
    let mut paths = std::collections::BTreeSet::new();
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
            "N2 2.0 requires at least one storage_roots entry".into(),
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
        let path = Path::new(self.path.trim());
        if !path.is_absolute() {
            return Err(AppError::BadRequest(format!(
                "storage root '{}' must use an absolute path",
                self.id
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterPolicy {
    pub namespace_redundancy: usize,
    pub object_data_shards: usize,
    pub object_parity_shards: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NamespaceCatalogReplica {
    pub node_id: String,
    pub root_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NamespaceCatalogLayout {
    pub generation: u64,
    pub replicas: Vec<NamespaceCatalogReplica>,
}

impl ClusterPolicy {
    pub fn validate(&self) -> Result<(), AppError> {
        if self.namespace_redundancy < 2 {
            return Err(AppError::BadRequest(
                "N2 2.0 namespace_redundancy must be at least 2".into(),
            ));
        }
        if self.object_data_shards == 0 || self.object_parity_shards == 0 {
            return Err(AppError::BadRequest(
                "N2 2.0 object EC requires at least 1 data and 1 parity shard".into(),
            ));
        }
        Ok(())
    }

    pub fn object_width(&self) -> usize {
        self.object_data_shards
            .saturating_add(self.object_parity_shards)
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
    pub role: NodeRole,
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
    pub policy: ClusterPolicy,
    pub namespace_catalog: NamespaceCatalogLayout,
    pub nodes: Vec<ClusterNode>,
}

impl ClusterMembership {
    pub fn validate(&self) -> Result<(), AppError> {
        if self.format_version != N2_MEMBERSHIP_FORMAT_VERSION {
            return Err(AppError::BadRequest(format!(
                "membership format {} is not N2 2.0 format {}",
                self.format_version, N2_MEMBERSHIP_FORMAT_VERSION
            )));
        }
        if self.cluster_id.trim().is_empty() || self.epoch == 0 {
            return Err(AppError::BadRequest(
                "membership requires a cluster_id and a positive epoch".into(),
            ));
        }
        self.policy.validate()?;

        let mut node_ids = std::collections::BTreeSet::new();
        let mut root_ids = std::collections::BTreeSet::new();
        for node in &self.nodes {
            if node.id.trim().is_empty() || !node_ids.insert(node.id.clone()) {
                return Err(AppError::BadRequest(format!(
                    "membership contains an empty or duplicate node ID '{}'",
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
        if self.namespace_catalog.generation == 0
            || self.namespace_catalog.replicas.len() != self.policy.namespace_redundancy
        {
            return Err(AppError::BadRequest(format!(
                "namespace catalog requires exactly {} replicas at a positive layout generation",
                self.policy.namespace_redundancy
            )));
        }
        let mut catalog_nodes = std::collections::BTreeSet::new();
        for replica in &self.namespace_catalog.replicas {
            if !catalog_nodes.insert(replica.node_id.clone()) {
                return Err(AppError::BadRequest(
                    "namespace catalog replicas must use distinct namespace nodes".into(),
                ));
            }
            let node = self
                .nodes
                .iter()
                .find(|node| node.id == replica.node_id)
                .ok_or_else(|| {
                    AppError::BadRequest(format!(
                        "namespace catalog references unknown node '{}'",
                        replica.node_id
                    ))
                })?;
            if node.role != NodeRole::Namespace
                || !node
                    .storage_roots
                    .iter()
                    .any(|root| root.id == replica.root_id)
            {
                return Err(AppError::BadRequest(format!(
                    "namespace catalog replica '{}:{}' is not a valid namespace root",
                    replica.node_id, replica.root_id
                )));
            }
        }
        Ok(())
    }

    pub fn nodes_for_role(&self, role: NodeRole) -> Vec<ClusterNode> {
        self.nodes
            .iter()
            .filter(|node| node.role == role)
            .cloned()
            .collect()
    }

    pub fn active_write_nodes_for_role(&self, role: NodeRole) -> Vec<ClusterNode> {
        self.nodes
            .iter()
            .filter(|node| node.role == role && node.effective_state().accepts_new_writes())
            .cloned()
            .collect()
    }

    pub fn readable_nodes_for_role(&self, role: NodeRole) -> Vec<ClusterNode> {
        self.nodes
            .iter()
            .filter(|node| node.role == role && node.effective_state().is_readable())
            .cloned()
            .collect()
    }

    pub fn validate_minimum_topology(&self) -> Result<(), AppError> {
        let namespace_nodes = self.nodes_for_role(NodeRole::Namespace).len();
        let blob_nodes = self.nodes_for_role(NodeRole::Blob).len();
        if namespace_nodes < self.policy.namespace_redundancy {
            return Err(AppError::ServiceUnavailable(format!(
                "N2 namespace requires {} distinct namespace nodes, but membership has {}",
                self.policy.namespace_redundancy, namespace_nodes
            )));
        }
        if blob_nodes < self.policy.object_width() {
            return Err(AppError::ServiceUnavailable(format!(
                "N2 object EC {}+{} requires {} distinct blob nodes, but membership has {}",
                self.policy.object_data_shards,
                self.policy.object_parity_shards,
                self.policy.object_width(),
                blob_nodes
            )));
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
            if cluster_node_is_reachable(&local_node_id, &node).await {
                Some(node)
            } else {
                None
            }
        }
    });
    join_all(probes)
        .await
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
}

pub async fn live_active_write_nodes(runtime: &ClusterRuntime) -> Vec<ClusterNode> {
    live_filter_nodes(
        &runtime.local_node_id,
        runtime.membership.active_write_nodes(),
    )
    .await
}

pub async fn live_active_write_nodes_for_role(
    runtime: &ClusterRuntime,
    role: NodeRole,
) -> Vec<ClusterNode> {
    live_filter_nodes(
        &runtime.local_node_id,
        runtime.membership.active_write_nodes_for_role(role),
    )
    .await
}

pub async fn live_blob_write_nodes(runtime: &ClusterRuntime) -> Vec<ClusterNode> {
    live_active_write_nodes_for_role(runtime, NodeRole::Blob).await
}

pub async fn live_namespace_write_nodes(runtime: &ClusterRuntime) -> Vec<ClusterNode> {
    live_active_write_nodes_for_role(runtime, NodeRole::Namespace).await
}

pub async fn live_readable_nodes(runtime: &ClusterRuntime) -> Vec<ClusterNode> {
    live_filter_nodes(&runtime.local_node_id, runtime.membership.readable_nodes()).await
}

#[derive(Clone, Debug)]
pub struct ClusterRuntime {
    pub local_node_id: String,
    pub local_role: NodeRole,
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
        None | Some("") | Some("auto") => Ok(default_value),
        Some(value) => value.parse::<usize>().map_err(|_| {
            AppError::BadRequest(format!("{key} must be a non-negative integer or empty"))
        }),
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
    role: NodeRole,
    endpoint: &str,
    native_endpoint: &str,
    storage_roots: &[StorageRoot],
    policy: ClusterPolicy,
) -> ClusterMembership {
    let namespace_catalog = NamespaceCatalogLayout {
        generation: 1,
        replicas: if role == NodeRole::Namespace {
            storage_roots
                .first()
                .map(|root| NamespaceCatalogReplica {
                    node_id: node_id.to_string(),
                    root_id: root.id.clone(),
                })
                .into_iter()
                .collect()
        } else {
            Vec::new()
        },
    };
    ClusterMembership {
        format_version: N2_MEMBERSHIP_FORMAT_VERSION,
        cluster_id: "n2-local".to_string(),
        epoch: 1,
        policy,
        namespace_catalog,
        nodes: vec![ClusterNode {
            id: node_id.to_string(),
            role,
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
    role: NodeRole,
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
                "decode N2 membership file '{}': {e}",
                path.display()
            ))
        })?;
        membership.validate()?;
        if membership.policy != policy {
            return Err(AppError::Conflict(format!(
                "local cluster policy {:?} does not match membership policy {:?}",
                policy, membership.policy
            )));
        }
        let now = Utc::now().timestamp();
        if let Some(local) = membership.nodes.iter_mut().find(|node| node.id == node_id) {
            if local.role != role {
                return Err(AppError::Conflict(format!(
                    "node '{}' is recorded as role {}, not {}",
                    node_id, local.role, role
                )));
            }
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
        } else {
            let joined_epoch = membership.epoch.saturating_add(1);
            membership.nodes.push(ClusterNode {
                id: node_id.to_string(),
                role,
                endpoint: endpoint.to_string(),
                native_endpoint: Some(native_endpoint.to_string()),
                state: Some(NodeState::Online),
                weight: 1,
                storage_roots: storage_roots.to_vec(),
                joined_epoch,
                capacity_bytes: None,
                used_bytes: None,
                last_seen_unix: Some(now),
            });
            membership.epoch = joined_epoch;
        }
        membership.validate()?;
        write_membership(path, &membership)?;
        return Ok(membership);
    }

    let membership = local_membership(
        node_id,
        role,
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
    fs::write(path, bytes)
        .map_err(|e| AppError::Io(e, format!("write N2 membership file {}", path.display())))
}

pub fn init_cluster_runtime(
    config: &Ini,
    fs_storage_path: &str,
    local_storage_roots: &[StorageRoot],
    https_endpoint: &str,
    native_endpoint: &str,
) -> Result<Arc<ClusterRuntime>, AppError> {
    let section = config.section(Some("cluster_config"));
    let node_id = section
        .and_then(|s| s.get("node_id"))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("node-1")
        .to_string();
    let local_role = NodeRole::parse(
        section
            .and_then(|s| s.get("node_role"))
            .unwrap_or("namespace"),
    )?;
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
        namespace_redundancy: parse_usize(
            section.and_then(|s| s.get("namespace_redundancy")),
            "namespace_redundancy",
            2,
        )?,
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
    let layout_policy = ClusterLayoutPolicy {
        mode: ClusterLayoutMode::Fixed,
        fixed_data_shards: Some(policy.object_data_shards),
        fixed_parity_shards: Some(policy.object_parity_shards),
        auto_max_data_shards: DEFAULT_AUTO_MAX_DATA_SHARDS,
        auto_max_parity_shards: DEFAULT_AUTO_MAX_PARITY_SHARDS,
    };
    if parse_bool(section.and_then(|s| s.get("auto_rebalance")), false) {
        return Err(AppError::BadRequest(
            "auto_rebalance=true is not supported: N2 preserves recorded object layouts and only redistributes through an explicit operator request"
                .into(),
        ));
    }
    let auto_rebalance = false;
    let membership = load_or_create_membership(
        &membership_path,
        &node_id,
        local_role,
        node_endpoint,
        native_endpoint,
        local_storage_roots,
        policy,
    )?;
    membership.validate_minimum_topology()?;

    let runtime = Arc::new(ClusterRuntime {
        local_node_id: node_id,
        local_role,
        local_storage_roots: local_storage_roots.to_vec(),
        membership_path,
        layout_policy,
        membership,
        auto_rebalance,
    });
    CLUSTER_RUNTIME.set(runtime.clone()).map_err(|_| {
        AppError::InternalServerError("N2 cluster runtime initialized twice".into())
    })?;
    Ok(runtime)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn membership_file_is_created_with_local_node() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("n2-membership-{unique}"));
        let path = root.join("members.json");
        let membership = load_or_create_membership(
            &path,
            "node-a",
            NodeRole::Namespace,
            "https://127.0.0.1:51001",
            "n2://127.0.0.1:51002",
            &[StorageRoot {
                id: "root-a".into(),
                path: root.join("storage").display().to_string(),
            }],
            ClusterPolicy {
                namespace_redundancy: 2,
                object_data_shards: 1,
                object_parity_shards: 1,
            },
        )
        .expect("membership");
        assert_eq!(membership.nodes.len(), 1);
        assert_eq!(membership.nodes[0].id, "node-a");
        assert!(path.exists());
        let _ = fs::remove_dir_all(root);
    }
}
