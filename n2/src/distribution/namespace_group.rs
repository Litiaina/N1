use crate::core::disk_manager::FS_STORAGE_ROOT;
use crate::distribution::membership::{ClusterNode, ClusterRuntime};
use crate::distribution::peer::{cluster_secret, peer_http_client};
use crate::enums::app_error_enum::AppError;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::time::{Duration, timeout};

const DEFAULT_NAMESPACE_GROUP_SIZE: usize = 3;
const DEFAULT_NAMESPACE_PARTITION_COUNT: u16 = 64;
const NAMESPACE_GROUP_DISCOVERY_TIMEOUT: Duration = Duration::from_millis(1_500);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NamespaceGroupAssignment {
    pub fragment: String,
    pub partition_id: u16,
    pub partition_count: u16,
    pub created_membership_epoch: u64,
    pub voter_node_ids: Vec<String>,
}

fn group_size() -> usize {
    std::env::var("N2_NAMESPACE_GROUP_SIZE")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_NAMESPACE_GROUP_SIZE)
        .clamp(1, 5)
}

fn group_root() -> Result<PathBuf, AppError> {
    let root = FS_STORAGE_ROOT.get().ok_or_else(|| {
        AppError::InternalServerError("N2 fs storage root is not initialized".into())
    })?;
    Ok(PathBuf::from(root).join(".n2").join("namespace-groups"))
}

pub fn namespace_partition_count() -> u16 {
    std::env::var("N2_NAMESPACE_PARTITION_COUNT")
        .ok()
        .and_then(|value| value.trim().parse::<u16>().ok())
        .unwrap_or(DEFAULT_NAMESPACE_PARTITION_COUNT)
        .clamp(1, 4096)
}

pub fn namespace_partition_for_path(fragment: &str, path: &str) -> u16 {
    let count = namespace_partition_count();
    let mut hasher = Sha256::new();
    hasher.update(b"n2-namespace-partition");
    hasher.update([0]);
    hasher.update(fragment.as_bytes());
    hasher.update([0]);
    hasher.update(path.trim_matches('/').as_bytes());
    let hash: [u8; 32] = hasher.finalize().into();
    u16::from_be_bytes([hash[0], hash[1]]) % count
}

fn group_path(fragment: &str, partition_id: u16) -> Result<PathBuf, AppError> {
    Ok(group_root()?
        .join(fragment)
        .join(format!("{partition_id:04}.json")))
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), AppError> {
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError(format!(
            "N2 namespace group path {} has no parent",
            path.display()
        ))
    })?;
    fs::create_dir_all(parent).map_err(|err| {
        AppError::Io(
            err,
            format!("create N2 namespace group directory {}", parent.display()),
        )
    })?;
    let tmp = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("group"),
        ulid::Ulid::new()
    ));
    fs::write(&tmp, bytes).map_err(|err| {
        AppError::Io(
            err,
            format!("write N2 namespace group temp {}", tmp.display()),
        )
    })?;
    fs::rename(&tmp, path).map_err(|err| {
        AppError::Io(
            err,
            format!("publish N2 namespace group {}", path.display()),
        )
    })
}

pub fn load_namespace_group(
    fragment: &str,
    partition_id: u16,
) -> Result<Option<NamespaceGroupAssignment>, AppError> {
    let path = group_path(fragment, partition_id)?;
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(AppError::Io(
                err,
                format!("read N2 namespace group {}", path.display()),
            ));
        }
    };
    let assignment = serde_json::from_slice::<NamespaceGroupAssignment>(&bytes).map_err(|err| {
        AppError::InternalServerError(format!(
            "decode N2 namespace group {}: {err}",
            path.display()
        ))
    })?;
    if assignment.fragment != fragment || assignment.partition_id != partition_id {
        return Err(AppError::InternalServerError(format!(
            "N2 namespace group {} belongs to fragment {} partition {}",
            path.display(),
            assignment.fragment,
            assignment.partition_id
        )));
    }
    Ok(Some(assignment))
}

pub fn persist_namespace_group(assignment: &NamespaceGroupAssignment) -> Result<(), AppError> {
    if assignment.voter_node_ids.is_empty() {
        return Err(AppError::BadRequest(
            "N2 namespace group must contain at least one voter".into(),
        ));
    }
    let mut unique = BTreeSet::new();
    if assignment
        .voter_node_ids
        .iter()
        .any(|node_id| node_id.trim().is_empty() || !unique.insert(node_id))
    {
        return Err(AppError::BadRequest(
            "N2 namespace group voter ids must be non-empty and unique".into(),
        ));
    }
    let bytes = serde_json::to_vec_pretty(assignment).map_err(|err| {
        AppError::InternalServerError(format!("encode N2 namespace group: {err}"))
    })?;
    write_atomic(
        &group_path(&assignment.fragment, assignment.partition_id)?,
        &bytes,
    )
}

fn assignment_score(fragment: &str, partition_id: u16, node_id: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"n2-namespace-group");
    hasher.update([0]);
    hasher.update(fragment.as_bytes());
    hasher.update([0]);
    hasher.update(partition_id.to_be_bytes());
    hasher.update([0]);
    hasher.update(node_id.as_bytes());
    hasher.finalize().into()
}

fn new_assignment(
    runtime: &ClusterRuntime,
    fragment: &str,
    partition_id: u16,
) -> Result<NamespaceGroupAssignment, AppError> {
    let mut candidates = runtime.membership.nodes.clone();
    if candidates.is_empty() {
        return Err(AppError::ServiceUnavailable(
            "N2 membership has no namespace voters".into(),
        ));
    }
    candidates.sort_by(|left, right| {
        assignment_score(fragment, partition_id, &right.id)
            .cmp(&assignment_score(fragment, partition_id, &left.id))
            .then(left.id.cmp(&right.id))
    });
    let voter_node_ids = candidates
        .into_iter()
        .take(group_size())
        .map(|node| node.id)
        .collect();
    Ok(NamespaceGroupAssignment {
        fragment: fragment.to_string(),
        partition_id,
        partition_count: namespace_partition_count(),
        created_membership_epoch: runtime.membership.epoch,
        voter_node_ids,
    })
}

fn group_url(endpoint: &str, fragment: &str, partition_id: u16) -> String {
    format!(
        "{}/n2/peer/namespace/group/{}/{}",
        endpoint.trim_end_matches('/'),
        utf8_percent_encode(fragment, NON_ALPHANUMERIC),
        partition_id
    )
}

async fn fetch_group(
    endpoint: &str,
    fragment: &str,
    partition_id: u16,
) -> Result<Option<NamespaceGroupAssignment>, AppError> {
    let client = peer_http_client()?;
    let mut request = client.get(group_url(endpoint, fragment, partition_id));
    if let Some(secret) = cluster_secret() {
        request = request.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(NAMESPACE_GROUP_DISCOVERY_TIMEOUT, request.send())
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "N2 namespace group discovery timed out at {endpoint}"
            ))
        })?
        .map_err(|err| {
            AppError::ServiceUnavailable(format!(
                "N2 namespace group discovery failed at {endpoint}: {err}"
            ))
        })?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "N2 namespace group discovery returned {} from {}",
            response.status(),
            endpoint
        )));
    }
    response
        .json::<NamespaceGroupAssignment>()
        .await
        .map(Some)
        .map_err(|err| {
            AppError::InternalServerError(format!(
                "decode N2 namespace group from {endpoint}: {err}"
            ))
        })
}

pub async fn resolve_namespace_group(
    runtime: &ClusterRuntime,
    fragment: &str,
    partition_id: u16,
) -> Result<NamespaceGroupAssignment, AppError> {
    if let Some(assignment) = load_namespace_group(fragment, partition_id)? {
        return Ok(assignment);
    }
    for node in runtime
        .membership
        .readable_nodes()
        .into_iter()
        .filter(|node| node.id != runtime.local_node_id)
    {
        if let Ok(Some(assignment)) = fetch_group(&node.endpoint, fragment, partition_id).await {
            persist_namespace_group(&assignment)?;
            return Ok(assignment);
        }
    }
    let assignment = new_assignment(runtime, fragment, partition_id)?;
    persist_namespace_group(&assignment)?;
    Ok(assignment)
}

pub fn assignment_nodes(
    runtime: &ClusterRuntime,
    assignment: &NamespaceGroupAssignment,
) -> Result<Vec<ClusterNode>, AppError> {
    let mut nodes = Vec::with_capacity(assignment.voter_node_ids.len());
    for node_id in &assignment.voter_node_ids {
        let node = runtime
            .membership
            .nodes
            .iter()
            .find(|node| &node.id == node_id)
            .cloned()
            .ok_or_else(|| {
                AppError::ServiceUnavailable(format!(
                    "N2 namespace voter {node_id} is absent from membership"
                ))
            })?;
        nodes.push(node);
    }
    Ok(nodes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::layout::{ClusterLayoutMode, ClusterLayoutPolicy};
    use crate::distribution::membership::{ClusterMembership, NodeState};

    fn runtime(node_count: usize) -> ClusterRuntime {
        ClusterRuntime {
            local_node_id: "node-0".into(),
            membership_path: "membership.json".into(),
            layout_policy: ClusterLayoutPolicy {
                mode: ClusterLayoutMode::Auto,
                fixed_data_shards: None,
                fixed_parity_shards: None,
            },
            membership: ClusterMembership {
                cluster_id: "test".into(),
                epoch: 9,
                nodes: (0..node_count)
                    .map(|idx| ClusterNode {
                        id: format!("node-{idx}"),
                        endpoint: format!("https://node-{idx}"),
                        native_endpoint: None,
                        state: Some(NodeState::Online),
                        weight: 1,
                        index_roots: Vec::new(),
                        blob_roots: Vec::new(),
                        capacity_bytes: None,
                        used_bytes: None,
                        last_seen_unix: None,
                    })
                    .collect(),
            },
            auto_rebalance: false,
        }
    }

    #[test]
    fn new_fragment_uses_three_stable_voters_by_default() {
        let left = new_assignment(&runtime(6), "fragment-a", 0).expect("assignment");
        let right = new_assignment(&runtime(6), "fragment-a", 0).expect("assignment");
        assert_eq!(left, right);
        assert_eq!(left.voter_node_ids.len(), 3);
    }
}
