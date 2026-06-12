use crate::distribution::layout::ClusterLayoutMode;
use crate::distribution::layout::resolve_layout;
use crate::distribution::membership::{
    ClusterNode, ClusterRuntime, NodeState, live_active_write_nodes, live_readable_nodes,
};
use crate::distribution::namespace_group::{
    namespace_quorum_required, namespace_voter_count_for_membership,
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
    let configured_active = runtime.membership.active_write_nodes();
    let layout =
        resolve_layout(active.len(), &runtime.layout_policy).map_err(AppError::BadRequest)?;
    let data_plane_readable = readable.len() >= layout.data_shards;
    let data_plane_writable = active.len() >= layout.total_shards();
    let namespace_voters = namespace_voter_count_for_membership(runtime.membership.nodes.len());
    let namespace_quorum_required = namespace_quorum_required(namespace_voters);
    let estimated_namespace_reachable_voters = readable.len().min(namespace_voters);
    let namespace_quorum_writable =
        estimated_namespace_reachable_voters >= namespace_quorum_required;
    let effective_client_writable = data_plane_writable && namespace_quorum_writable;
    let missing_writable = configured_active.len().saturating_sub(active.len());
    let live_node_ids = readable
        .iter()
        .map(|node| node.id.clone())
        .collect::<HashSet<_>>();
    let state = if !data_plane_readable {
        ClusterHealthState::Critical
    } else if !effective_client_writable {
        ClusterHealthState::Degraded
    } else if missing_writable > 0 {
        ClusterHealthState::Degraded
    } else if data_plane_readable {
        ClusterHealthState::Healthy
    } else {
        ClusterHealthState::Critical
    };
    let rebalance = plan_rebalance(runtime)?;
    let repair_status = repair_status();
    let rebalance_status = rebalance_status();
    let mut notes = vec![rebalance.recommendation];
    if missing_writable == 0 {
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
    if missing_writable > 0 {
        if namespace_quorum_writable {
            notes.push(format!(
                "{} configured writable node(s) are currently unreachable; namespace quorum is still writable and new objects use the live data layout, while older objects keep their recorded layout",
                missing_writable
            ));
        } else {
            notes.push(format!(
                "{} configured writable node(s) are currently unreachable; object reads may still work, but namespace quorum is not writable so new client mutations are unavailable",
                missing_writable
            ));
        }
    }
    if !data_plane_readable {
        notes.push(format!(
            "cluster cannot read the current live layout: requires {} readable node(s), has {}",
            layout.data_shards,
            readable.len()
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
        adaptive_layout: matches!(runtime.layout_policy.mode, ClusterLayoutMode::Auto),
        manual_rebalance_available: active.len() > 1,
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
    use crate::distribution::membership::{ClusterMembership, ClusterNode};
    use std::path::PathBuf;

    fn runtime(nodes: Vec<ClusterNode>) -> ClusterRuntime {
        ClusterRuntime {
            local_node_id: "a".into(),
            membership_path: PathBuf::from("membership.json"),
            layout_policy: ClusterLayoutPolicy {
                mode: ClusterLayoutMode::Auto,
                fixed_data_shards: None,
                fixed_parity_shards: None,
                ..ClusterLayoutPolicy::default()
            },
            membership: ClusterMembership {
                cluster_id: "test".into(),
                epoch: 3,
                nodes,
            },
            auto_rebalance: false,
        }
    }

    fn node(id: &str, state: NodeState) -> ClusterNode {
        ClusterNode {
            id: id.into(),
            endpoint: format!("https://{id}:50001"),
            native_endpoint: Some(format!("n2://{id}:50002")),
            state: Some(state),
            weight: 1,
            index_roots: vec![],
            blob_roots: vec![],
            capacity_bytes: None,
            used_bytes: None,
            last_seen_unix: None,
        }
    }

    #[test]
    fn health_reflects_writable_layout_capacity() {
        let runtime = runtime(vec![
            node("a", NodeState::Online),
            node("b", NodeState::Online),
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
        assert!(snap.adaptive_layout);
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
            Some("n2://a:50002")
        );
        assert!(!snap.repair_running);
        assert!(!snap.rebalance_running);
    }

    #[test]
    fn health_marks_unreachable_configured_node_as_degraded() {
        let runtime = runtime(vec![
            node("a", NodeState::Online),
            node("b", NodeState::Online),
            node("c", NodeState::Online),
        ]);
        let live = vec![
            runtime.membership.nodes[0].clone(),
            runtime.membership.nodes[1].clone(),
        ];
        let snap = cluster_health_snapshot_with_live_nodes(&runtime, live.clone(), live).unwrap();
        assert_eq!(snap.state, ClusterHealthState::Degraded);
        assert_eq!(snap.active_write_nodes, 2);
        assert_eq!(snap.data_shards, 1);
        assert_eq!(snap.parity_shards, 1);
        assert!(snap.data_plane_writable);
        assert!(snap.namespace_quorum_writable);
        assert!(snap.effective_client_writable);
        assert_eq!(snap.nodes[2].state, NodeState::Offline);
    }

    #[test]
    fn health_reports_namespace_quorum_blocking_writes() {
        let runtime = runtime(vec![
            node("a", NodeState::Online),
            node("b", NodeState::Online),
        ]);
        let live = vec![runtime.membership.nodes[0].clone()];
        let snap = cluster_health_snapshot_with_live_nodes(&runtime, live.clone(), live).unwrap();
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
