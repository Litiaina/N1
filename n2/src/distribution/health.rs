use crate::core::operation_gate::cluster_safety_read_only_state;
use crate::distribution::layout::resolve_configured_layout;
use crate::distribution::membership::{
    ClusterNode, ClusterRuntime, NodeRole, NodeState, live_active_write_nodes, live_readable_nodes,
};
use crate::distribution::rebalance::{plan_rebalance, rebalance_status, repair_status};
use crate::enums::app_error_enum::AppError;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClusterHealthState {
    Healthy,
    Degraded,
    Critical,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterNodeHealth {
    pub node_id: String,
    pub role: NodeRole,
    pub state: NodeState,
    pub accepts_writes: bool,
    pub readable: bool,
    pub endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_endpoint: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterHealthSnapshot {
    pub state: ClusterHealthState,
    pub membership_epoch: u64,
    pub active_write_nodes: usize,
    pub readable_nodes: usize,
    pub data_shards: usize,
    pub parity_shards: usize,
    pub data_plane_readable: bool,
    pub data_plane_writable: bool,
    pub namespace_voters: usize,
    pub estimated_namespace_reachable_voters: usize,
    pub namespace_quorum_required: usize,
    pub namespace_quorum_writable: bool,
    pub effective_client_writable: bool,
    pub safety_read_only: bool,
    pub adaptive_layout: bool,
    pub manual_rebalance_available: bool,
    pub layout_drift_is_health_failure: bool,
    pub automatic_rebalance: bool,
    pub repair_running: bool,
    pub rebalance_running: bool,
    pub nodes: Vec<ClusterNodeHealth>,
    pub notes: Vec<String>,
}

pub async fn cluster_health_snapshot(
    runtime: &ClusterRuntime,
) -> Result<ClusterHealthSnapshot, AppError> {
    let active = live_active_write_nodes(runtime).await;
    let readable = live_readable_nodes(runtime).await;
    cluster_health_snapshot_with_live_nodes(runtime, active, readable)
}

pub fn cluster_health_snapshot_with_live_nodes(
    runtime: &ClusterRuntime,
    active: Vec<ClusterNode>,
    readable: Vec<ClusterNode>,
) -> Result<ClusterHealthSnapshot, AppError> {
    let configured_blob = runtime
        .membership
        .active_write_nodes_for_role(NodeRole::Blob);
    let active_blob = active
        .iter()
        .filter(|node| node.role == NodeRole::Blob)
        .cloned()
        .collect::<Vec<_>>();
    let readable_blob = readable
        .iter()
        .filter(|node| node.role == NodeRole::Blob)
        .cloned()
        .collect::<Vec<_>>();
    let readable_namespace = readable
        .iter()
        .filter(|node| node.role == NodeRole::Namespace)
        .count();
    let configured_layout =
        resolve_configured_layout(configured_blob.len(), &runtime.layout_policy)
            .map_err(AppError::BadRequest)?;
    let layout = configured_layout;
    let data_plane_readable = readable_blob.len() >= layout.data_shards;
    let data_plane_writable = active_blob.len() >= layout.total_shards();
    let configured_namespace_nodes = runtime.membership.nodes_for_role(NodeRole::Namespace).len();
    let namespace_voters = runtime.membership.policy.namespace_redundancy;
    let namespace_quorum_required = (namespace_voters / 2).saturating_add(1);
    let estimated_namespace_reachable_voters = readable_namespace.min(namespace_voters);
    let namespace_quorum_writable =
        estimated_namespace_reachable_voters >= namespace_quorum_required;
    let safety_state = cluster_safety_read_only_state();
    let safety_read_only = safety_state.is_some();
    let effective_client_writable =
        data_plane_writable && namespace_quorum_writable && !safety_read_only;
    let missing_blob_writable = configured_blob.len().saturating_sub(active_blob.len());
    let missing_namespace_readable = configured_namespace_nodes.saturating_sub(readable_namespace);
    let live_node_ids = readable
        .iter()
        .map(|node| node.id.clone())
        .collect::<HashSet<_>>();
    let state = if !data_plane_readable || safety_read_only {
        ClusterHealthState::Critical
    } else if !effective_client_writable {
        ClusterHealthState::Degraded
    } else if missing_blob_writable > 0 || missing_namespace_readable > 0 {
        ClusterHealthState::Degraded
    } else if data_plane_readable {
        ClusterHealthState::Healthy
    } else {
        ClusterHealthState::Critical
    };
    let rebalance_recommendation = plan_rebalance(runtime)
        .map(|plan| plan.recommendation)
        .unwrap_or_else(|error| format!("rebalance unavailable while topology is unsafe: {error}"));
    let repair_status = repair_status();
    let rebalance_status = rebalance_status();
    let mut notes = vec![rebalance_recommendation];
    if let Some(safety) = safety_state {
        notes.push(format!(
            "cluster safety read-only incident {}: {} ({})",
            safety.incident_id, safety.finding, safety.reason
        ));
    }
    if missing_blob_writable == 0 {
        notes.push(
            "layout drift is not health failure: new objects use the current node layout; old objects stay readable until manual rebalance"
                .to_string(),
        );
    }
    if repair_status.running {
        notes.push(format!(
            "cluster repair is running for fragment '{}'{}",
            repair_status.fragment,
            repair_status
                .last_object
                .as_deref()
                .map(|object| format!(" near object '{object}'"))
                .unwrap_or_default()
        ));
    }
    if rebalance_status.running {
        notes.push(format!(
            "cluster rebalance is running for fragment '{}'{}",
            rebalance_status.fragment,
            rebalance_status
                .last_object
                .as_deref()
                .map(|object| format!(" near object '{object}'"))
                .unwrap_or_default()
        ));
    }
    if missing_blob_writable > 0 {
        if namespace_quorum_writable {
            notes.push(format!(
                "{} configured blob node(s) are currently unreachable; namespace quorum remains writable but new object writes stop until the fixed {}+{} width is available",
                missing_blob_writable,
                configured_layout.data_shards,
                configured_layout.parity_shards
            ));
        } else {
            notes.push(format!(
                "{} configured blob node(s) are currently unreachable and namespace quorum is not writable; existing reconstructable objects may still be read",
                missing_blob_writable
            ));
        }
    }
    if !data_plane_readable {
        notes.push(format!(
            "cluster cannot read the current live layout: requires {} readable node(s), has {}",
            layout.data_shards,
            readable_blob.len()
        ));
    }
    if !namespace_quorum_writable {
        notes.push(format!(
            "namespace quorum is not writable: requires {} voter(s), has {} reachable voter estimate",
            namespace_quorum_required, estimated_namespace_reachable_voters
        ));
    }
    if data_plane_writable && !namespace_quorum_writable {
        notes.push(
            "data-plane placement can accept shards, but client writes still require namespace pointer quorum"
                .to_string(),
        );
    }

    Ok(ClusterHealthSnapshot {
        state,
        membership_epoch: runtime.membership.epoch,
        active_write_nodes: active.len(),
        readable_nodes: readable.len(),
        data_shards: layout.data_shards,
        parity_shards: layout.parity_shards,
        data_plane_readable,
        data_plane_writable,
        namespace_voters,
        estimated_namespace_reachable_voters,
        namespace_quorum_required,
        namespace_quorum_writable,
        effective_client_writable,
        safety_read_only,
        adaptive_layout: false,
        manual_rebalance_available: active_blob.len() >= layout.total_shards(),
        layout_drift_is_health_failure: false,
        automatic_rebalance: runtime.auto_rebalance,
        repair_running: repair_status.running,
        rebalance_running: rebalance_status.running,
        nodes: runtime
            .membership
            .nodes
            .iter()
            .map(|node| {
                let configured_state = node.effective_state();
                let state = if configured_state.is_readable()
                    && !node.is_local(&runtime.local_node_id)
                    && !live_node_ids.contains(&node.id)
                {
                    NodeState::Offline
                } else {
                    configured_state
                };
                ClusterNodeHealth {
                    node_id: node.id.clone(),
                    role: node.role,
                    accepts_writes: state.accepts_new_writes() && effective_client_writable,
                    readable: state.is_readable(),
                    state,
                    endpoint: node.endpoint.clone(),
                    native_endpoint: node.native_endpoint.clone(),
                }
            })
            .collect(),
        notes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::layout::{ClusterLayoutMode, ClusterLayoutPolicy};
    use crate::distribution::membership::{
        ClusterMembership, ClusterNode, ClusterPolicy, N2_MEMBERSHIP_FORMAT_VERSION,
        NamespaceCatalogLayout, NamespaceCatalogReplica, NodeRole,
    };
    use std::path::PathBuf;

    fn node(id: &str, role: NodeRole, state: NodeState) -> ClusterNode {
        ClusterNode {
            id: id.into(),
            role,
            endpoint: format!("https://{id}:50001"),
            native_endpoint: Some(format!("n2://{id}:50002")),
            state: Some(state),
            weight: 1,
            storage_roots: vec![],
            joined_epoch: 1,
            capacity_bytes: None,
            used_bytes: None,
            last_seen_unix: None,
        }
    }

    fn runtime(blob_nodes: Vec<ClusterNode>) -> ClusterRuntime {
        let mut nodes = vec![
            node("ns-a", NodeRole::Namespace, NodeState::Online),
            node("ns-b", NodeRole::Namespace, NodeState::Online),
        ];
        nodes.extend(blob_nodes);
        ClusterRuntime {
            local_node_id: "blob-a".into(),
            local_role: NodeRole::Blob,
            local_storage_roots: vec![],
            membership_path: PathBuf::from("membership.json"),
            layout_policy: ClusterLayoutPolicy {
                mode: ClusterLayoutMode::Fixed,
                fixed_data_shards: Some(1),
                fixed_parity_shards: Some(1),
                ..ClusterLayoutPolicy::default()
            },
            membership: ClusterMembership {
                format_version: N2_MEMBERSHIP_FORMAT_VERSION,
                cluster_id: "test".into(),
                epoch: 3,
                policy: ClusterPolicy {
                    namespace_redundancy: 2,
                    object_data_shards: 1,
                    object_parity_shards: 1,
                },
                namespace_catalog: NamespaceCatalogLayout {
                    generation: 3,
                    replicas: vec![
                        NamespaceCatalogReplica {
                            node_id: "ns-a".into(),
                            root_id: "root-ns-a".into(),
                        },
                        NamespaceCatalogReplica {
                            node_id: "ns-b".into(),
                            root_id: "root-ns-b".into(),
                        },
                    ],
                },
                nodes,
            },
            auto_rebalance: false,
        }
    }

    #[test]
    fn health_reflects_writable_layout_capacity() {
        let runtime = runtime(vec![
            node("blob-a", NodeRole::Blob, NodeState::Online),
            node("blob-b", NodeRole::Blob, NodeState::Online),
        ]);
        let snap = cluster_health_snapshot_with_live_nodes(
            &runtime,
            runtime.membership.active_write_nodes(),
            runtime.membership.readable_nodes(),
        )
        .unwrap();
        assert_eq!(snap.state, ClusterHealthState::Healthy);
        assert_eq!(snap.data_shards, 1);
        assert_eq!(snap.parity_shards, 1);
        assert!(!snap.adaptive_layout);
        assert!(snap.manual_rebalance_available);
        assert!(snap.data_plane_readable);
        assert!(snap.data_plane_writable);
        assert_eq!(snap.namespace_voters, 2);
        assert_eq!(snap.estimated_namespace_reachable_voters, 2);
        assert_eq!(snap.namespace_quorum_required, 2);
        assert!(snap.namespace_quorum_writable);
        assert!(snap.effective_client_writable);
        assert!(!snap.layout_drift_is_health_failure);
        assert_eq!(
            snap.nodes[0].native_endpoint.as_deref(),
            Some("n2://ns-a:50002")
        );
        assert!(!snap.repair_running);
        assert!(!snap.rebalance_running);
    }

    #[test]
    fn health_marks_unreachable_configured_node_as_degraded() {
        let runtime = runtime(vec![
            node("blob-a", NodeRole::Blob, NodeState::Online),
            node("blob-b", NodeRole::Blob, NodeState::Online),
            node("blob-c", NodeRole::Blob, NodeState::Online),
        ]);
        let live = runtime.membership.nodes[..4].to_vec();
        let snap = cluster_health_snapshot_with_live_nodes(&runtime, live.clone(), live).unwrap();
        assert_eq!(snap.state, ClusterHealthState::Degraded);
        assert_eq!(snap.active_write_nodes, 4);
        assert_eq!(snap.data_shards, 1);
        assert_eq!(snap.parity_shards, 1);
        assert!(snap.data_plane_writable);
        assert!(snap.namespace_quorum_writable);
        assert!(snap.effective_client_writable);
        assert_eq!(snap.nodes[4].state, NodeState::Offline);
    }

    #[test]
    fn fixed_two_plus_one_stops_writes_instead_of_shrinking_layout() {
        let mut runtime = runtime(vec![
            node("blob-a", NodeRole::Blob, NodeState::Online),
            node("blob-b", NodeRole::Blob, NodeState::Online),
            node("blob-c", NodeRole::Blob, NodeState::Online),
        ]);
        runtime.layout_policy = ClusterLayoutPolicy {
            mode: ClusterLayoutMode::Fixed,
            fixed_data_shards: Some(2),
            fixed_parity_shards: Some(1),
            ..ClusterLayoutPolicy::default()
        };
        runtime.membership.policy.object_data_shards = 2;
        let live = runtime.membership.nodes[..4].to_vec();

        let snap = cluster_health_snapshot_with_live_nodes(&runtime, live.clone(), live).unwrap();

        assert_eq!(snap.state, ClusterHealthState::Degraded);
        assert_eq!(snap.data_shards, 2);
        assert_eq!(snap.parity_shards, 1);
        assert!(snap.data_plane_readable);
        assert!(!snap.data_plane_writable);
        assert!(snap.namespace_quorum_writable);
        assert!(!snap.effective_client_writable);
        assert!(!snap.adaptive_layout);
    }

    #[test]
    fn wider_fixed_layout_is_healthy_only_at_full_width() {
        let mut runtime = runtime(vec![
            node("blob-a", NodeRole::Blob, NodeState::Online),
            node("blob-b", NodeRole::Blob, NodeState::Online),
            node("blob-c", NodeRole::Blob, NodeState::Online),
            node("blob-d", NodeRole::Blob, NodeState::Online),
        ]);
        runtime.layout_policy = ClusterLayoutPolicy {
            mode: ClusterLayoutMode::Fixed,
            fixed_data_shards: Some(3),
            fixed_parity_shards: Some(1),
            ..ClusterLayoutPolicy::default()
        };
        runtime.membership.policy.object_data_shards = 3;
        let live = runtime.membership.active_write_nodes();

        let snap = cluster_health_snapshot_with_live_nodes(&runtime, live.clone(), live).unwrap();

        assert_eq!(snap.data_shards, 3);
        assert_eq!(snap.parity_shards, 1);
        assert!(snap.data_plane_readable);
        assert!(snap.data_plane_writable);
        assert!(snap.effective_client_writable);
        assert!(!snap.adaptive_layout);
    }

    #[test]
    fn health_reports_namespace_quorum_blocking_writes() {
        let runtime = runtime(vec![
            node("blob-a", NodeRole::Blob, NodeState::Online),
            node("blob-b", NodeRole::Blob, NodeState::Online),
        ]);
        let active = runtime.membership.active_write_nodes();
        let readable = vec![
            runtime.membership.nodes[0].clone(),
            runtime.membership.nodes[2].clone(),
            runtime.membership.nodes[3].clone(),
        ];
        let snap = cluster_health_snapshot_with_live_nodes(&runtime, active, readable).unwrap();
        assert_eq!(snap.state, ClusterHealthState::Degraded);
        assert!(snap.data_plane_readable);
        assert!(snap.data_plane_writable);
        assert!(!snap.namespace_quorum_writable);
        assert!(!snap.effective_client_writable);
        assert!(snap.nodes.iter().all(|node| !node.accepts_writes));
        assert!(
            snap.notes
                .iter()
                .any(|note| note.contains("namespace quorum is not writable"))
        );
    }
}
