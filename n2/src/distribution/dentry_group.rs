use crate::distribution::membership::{ClusterNode, ClusterRuntime};
use crate::enums::app_error_enum::AppError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const DEFAULT_NAMESPACE_REPLICAS: usize = 3;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DentryPartitionAssignment {
    pub virtual_partition: u64,
    pub membership_epoch: u64,
    pub voter_node_ids: Vec<String>,
}

pub fn configured_namespace_replicas() -> usize {
    std::env::var("N2_NAMESPACE_REPLICAS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_NAMESPACE_REPLICAS)
        .clamp(1, 7)
}

pub fn quorum_required(voters: usize) -> usize {
    (voters / 2).saturating_add(1).max(1)
}

pub fn guaranteed_reachable_voters(
    total_nodes: usize,
    reachable_nodes: usize,
    voters: usize,
) -> usize {
    voters.saturating_sub(total_nodes.saturating_sub(reachable_nodes))
}

pub fn assignment_for_partition(
    runtime: &ClusterRuntime,
    virtual_partition: u64,
) -> Result<DentryPartitionAssignment, AppError> {
    assignment_for_partition_at_epoch(runtime, virtual_partition, runtime.membership.epoch)
}

pub fn assignment_for_partition_at_epoch(
    runtime: &ClusterRuntime,
    virtual_partition: u64,
    placement_epoch: u64,
) -> Result<DentryPartitionAssignment, AppError> {
    if placement_epoch == 0 || placement_epoch > runtime.membership.epoch {
        return Err(AppError::Conflict(format!(
            "namespace placement epoch {placement_epoch} is not available at membership epoch {}",
            runtime.membership.epoch
        )));
    }
    // Placement membership is historical cluster truth, not liveness. Removing
    // offline voters from the hash would silently remap a partition and permit
    // stale or empty state to masquerade as the namespace.
    let mut nodes = runtime
        .membership
        .nodes
        .iter()
        .filter(|node| node.joined_epoch.max(1) <= placement_epoch)
        .cloned()
        .collect::<Vec<_>>();
    if nodes.is_empty() {
        return Err(AppError::ServiceUnavailable(format!(
            "membership has no namespace voters at placement epoch {placement_epoch}"
        )));
    }
    nodes.sort_by(|left, right| {
        placement_score(virtual_partition, right)
            .cmp(&placement_score(virtual_partition, left))
            .then(left.id.cmp(&right.id))
    });
    let voter_node_ids = nodes
        .into_iter()
        .take(configured_namespace_replicas())
        .map(|node| node.id)
        .collect::<Vec<_>>();
    Ok(DentryPartitionAssignment {
        virtual_partition,
        membership_epoch: placement_epoch,
        voter_node_ids,
    })
}

pub fn assignment_nodes(
    runtime: &ClusterRuntime,
    assignment: &DentryPartitionAssignment,
) -> Result<Vec<ClusterNode>, AppError> {
    assignment
        .voter_node_ids
        .iter()
        .map(|node_id| {
            runtime
                .membership
                .nodes
                .iter()
                .find(|node| &node.id == node_id)
                .cloned()
                .ok_or_else(|| {
                    AppError::ServiceUnavailable(format!(
                        "namespace voter '{node_id}' is absent from membership"
                    ))
                })
        })
        .collect()
}

fn placement_score(virtual_partition: u64, node: &ClusterNode) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"n2-dentry-partition-placement-v2");
    hasher.update([0]);
    hasher.update(virtual_partition.to_be_bytes());
    hasher.update([0]);
    hasher.update(node.id.as_bytes());
    hasher.update([0]);
    hasher.update(node.weight.max(1).to_be_bytes());
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::layout::{ClusterLayoutMode, ClusterLayoutPolicy};
    use crate::distribution::membership::{ClusterMembership, NodeState};
    use std::path::PathBuf;

    fn runtime(nodes: usize) -> ClusterRuntime {
        ClusterRuntime {
            local_node_id: "node-0".into(),
            membership_path: PathBuf::new(),
            membership: ClusterMembership {
                cluster_id: "cluster".into(),
                epoch: 7,
                nodes: (0..nodes)
                    .map(|index| ClusterNode {
                        id: format!("node-{index}"),
                        endpoint: format!("https://node-{index}"),
                        native_endpoint: None,
                        state: Some(NodeState::Online),
                        weight: 1,
                        storage_roots: vec![format!("/disk-{index}")],
                        joined_epoch: 1,
                        capacity_bytes: None,
                        used_bytes: None,
                        last_seen_unix: None,
                    })
                    .collect(),
            },
            layout_policy: ClusterLayoutPolicy {
                mode: ClusterLayoutMode::Auto,
                fixed_data_shards: None,
                fixed_parity_shards: None,
                auto_max_data_shards: 8,
                auto_max_parity_shards: 3,
            },
            auto_rebalance: false,
        }
    }

    #[test]
    fn assignment_is_deterministic_and_bounded() {
        let runtime = runtime(100);
        let first = assignment_for_partition(&runtime, 42).expect("assignment");
        let second = assignment_for_partition(&runtime, 42).expect("assignment");
        assert_eq!(first, second);
        assert_eq!(first.voter_node_ids.len(), configured_namespace_replicas());
        assert_eq!(quorum_required(first.voter_node_ids.len()), 2);
    }

    #[test]
    fn partitions_spread_across_voter_sets() {
        let runtime = runtime(12);
        let first = assignment_for_partition(&runtime, 1).expect("assignment");
        let second = assignment_for_partition(&runtime, 2).expect("assignment");
        assert_ne!(first.voter_node_ids, second.voter_node_ids);
    }

    #[test]
    fn reachable_voter_bound_is_conservative_across_partition_assignments() {
        assert_eq!(guaranteed_reachable_voters(4, 3, 3), 2);
        assert_eq!(guaranteed_reachable_voters(4, 2, 3), 1);
        assert_eq!(guaranteed_reachable_voters(3, 2, 3), 2);
    }

    #[test]
    fn expansion_does_not_remap_an_existing_placement_epoch() {
        let original = runtime(3);
        let before = assignment_for_partition_at_epoch(&original, 42, 7).expect("assignment");

        let mut expanded = original.clone();
        expanded.membership.epoch = 8;
        expanded.membership.nodes.push(ClusterNode {
            id: "node-3".into(),
            endpoint: "https://node-3".into(),
            native_endpoint: None,
            state: Some(NodeState::Online),
            weight: 1,
            storage_roots: vec!["/disk-3".into()],
            joined_epoch: 8,
            capacity_bytes: None,
            used_bytes: None,
            last_seen_unix: None,
        });

        let after = assignment_for_partition_at_epoch(&expanded, 42, 7).expect("old assignment");
        assert_eq!(before, after);
        assert!(!after.voter_node_ids.iter().any(|id| id == "node-3"));
    }
}
