use crate::distribution::layout::{ClusterLayoutMode, ClusterLayoutPolicy};
use crate::enums::app_error_enum::AppError;
use chrono::Utc;
use ini::Ini;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

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
    #[serde(default)]
    pub index_roots: Vec<String>,
    #[serde(default)]
    pub blob_roots: Vec<String>,
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
    pub cluster_id: String,
    pub epoch: u64,
    pub nodes: Vec<ClusterNode>,
}

impl ClusterMembership {
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

#[derive(Clone, Debug)]
pub struct ClusterRuntime {
    pub local_node_id: String,
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

fn parse_layout_mode(raw: Option<&str>) -> Result<ClusterLayoutMode, AppError> {
    match raw.unwrap_or("auto").trim().to_ascii_lowercase().as_str() {
        "auto" | "" => Ok(ClusterLayoutMode::Auto),
        "fixed" => Ok(ClusterLayoutMode::Fixed),
        other => Err(AppError::BadRequest(format!(
            "cluster_layout_mode must be auto or fixed, got {other}"
        ))),
    }
}

fn parse_optional_usize(raw: Option<&str>, key: &str) -> Result<Option<usize>, AppError> {
    match raw.map(str::trim) {
        None | Some("") | Some("auto") => Ok(None),
        Some(value) => value.parse::<usize>().map(Some).map_err(|_| {
            AppError::BadRequest(format!("{key} must be a positive integer or empty"))
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
    endpoint: &str,
    native_endpoint: &str,
    index_roots: &[String],
    blob_roots: &[String],
) -> ClusterMembership {
    ClusterMembership {
        cluster_id: "n2-local".to_string(),
        epoch: 1,
        nodes: vec![ClusterNode {
            id: node_id.to_string(),
            endpoint: endpoint.to_string(),
            native_endpoint: Some(native_endpoint.to_string()),
            state: Some(NodeState::Online),
            weight: 1,
            index_roots: index_roots.to_vec(),
            blob_roots: blob_roots.to_vec(),
            capacity_bytes: None,
            used_bytes: None,
            last_seen_unix: Some(Utc::now().timestamp()),
        }],
    }
}

fn load_or_create_membership(
    path: &Path,
    node_id: &str,
    endpoint: &str,
    native_endpoint: &str,
    index_roots: &[String],
    blob_roots: &[String],
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
        let now = Utc::now().timestamp();
        if let Some(local) = membership.nodes.iter_mut().find(|node| node.id == node_id) {
            local.endpoint = endpoint.to_string();
            local.native_endpoint = Some(native_endpoint.to_string());
            local.index_roots = index_roots.to_vec();
            local.blob_roots = blob_roots.to_vec();
            local.state.get_or_insert(NodeState::Online);
            local.last_seen_unix = Some(now);
        } else {
            membership.nodes.push(ClusterNode {
                id: node_id.to_string(),
                endpoint: endpoint.to_string(),
                native_endpoint: Some(native_endpoint.to_string()),
                state: Some(NodeState::Online),
                weight: 1,
                index_roots: index_roots.to_vec(),
                blob_roots: blob_roots.to_vec(),
                capacity_bytes: None,
                used_bytes: None,
                last_seen_unix: Some(now),
            });
            membership.epoch = membership.epoch.saturating_add(1);
        }
        write_membership(path, &membership)?;
        return Ok(membership);
    }

    let membership = local_membership(node_id, endpoint, native_endpoint, index_roots, blob_roots);
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
    local_index_roots: &[String],
    local_blob_roots: &[String],
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
    let layout_policy = ClusterLayoutPolicy {
        mode: parse_layout_mode(section.and_then(|s| s.get("cluster_layout_mode")))?,
        fixed_data_shards: parse_optional_usize(
            section.and_then(|s| s.get("fixed_data_shards")),
            "fixed_data_shards",
        )?,
        fixed_parity_shards: parse_optional_usize(
            section.and_then(|s| s.get("fixed_parity_shards")),
            "fixed_parity_shards",
        )?,
    };
    let auto_rebalance = parse_bool(section.and_then(|s| s.get("auto_rebalance")), false);
    let membership = load_or_create_membership(
        &membership_path,
        &node_id,
        node_endpoint,
        native_endpoint,
        local_index_roots,
        local_blob_roots,
    )?;

    let runtime = Arc::new(ClusterRuntime {
        local_node_id: node_id,
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
            "https://127.0.0.1:51001",
            "n2://127.0.0.1:51002",
            &[root.join("idx").display().to_string()],
            &[root.join("blob").display().to_string()],
        )
        .expect("membership");
        assert_eq!(membership.nodes.len(), 1);
        assert_eq!(membership.nodes[0].id, "node-a");
        assert!(path.exists());
        let _ = fs::remove_dir_all(root);
    }
}
