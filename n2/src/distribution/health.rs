use crate::distribution::layout::ClusterLayoutMode;
use crate::distribution::layout::resolve_layout;
use crate::distribution::membership::{ClusterRuntime, NodeState};
use crate::distribution::rebalance::{plan_rebalance, rebalance_status, repair_status};
use crate::enums::app_error_enum::AppError;
use serde::{Deserialize, Serialize};

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
    pub adaptive_layout: bool,
    pub manual_rebalance_available: bool,
    pub layout_drift_is_health_failure: bool,
    pub automatic_rebalance: bool,
    pub repair_running: bool,
    pub rebalance_running: bool,
    pub nodes: Vec<ClusterNodeHealth>,
    pub notes: Vec<String>,
}

pub fn cluster_health_snapshot(
    runtime: &ClusterRuntime,
) -> Result<ClusterHealthSnapshot, AppError> {
    let active = runtime.membership.active_write_nodes();
    let readable = runtime.membership.readable_nodes();
    let layout =
        resolve_layout(active.len(), &runtime.layout_policy).map_err(AppError::BadRequest)?;
    let required = layout.total_shards();
    let state = if active.len() >= required {
        ClusterHealthState::Healthy
    } else if readable.len() >= layout.data_shards {
        ClusterHealthState::Degraded
    } else {
        ClusterHealthState::Critical
    };
    let rebalance = plan_rebalance(runtime)?;
    let repair_status = repair_status();
    let rebalance_status = rebalance_status();
    let mut notes = vec![rebalance.recommendation];
    if active.len() >= required {
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
    if active.len() < required {
        notes.push(format!(
            "cluster cannot accept this layout: requires {} writable node(s), has {}",
            required,
            active.len()
        ));
    }

    Ok(ClusterHealthSnapshot {
        state,
        membership_epoch: runtime.membership.epoch,
        active_write_nodes: active.len(),
        readable_nodes: readable.len(),
        data_shards: layout.data_shards,
        parity_shards: layout.parity_shards,
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
                let state = node.effective_state();
                ClusterNodeHealth {
                    node_id: node.id.clone(),
                    accepts_writes: state.accepts_new_writes(),
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
        let snap = cluster_health_snapshot(&runtime(vec![
            node("a", NodeState::Online),
            node("b", NodeState::Online),
        ]))
        .unwrap();
        assert_eq!(snap.state, ClusterHealthState::Healthy);
        assert_eq!(snap.data_shards, 1);
        assert_eq!(snap.parity_shards, 1);
        assert!(snap.adaptive_layout);
        assert!(snap.manual_rebalance_available);
        assert!(!snap.layout_drift_is_health_failure);
        assert_eq!(
            snap.nodes[0].native_endpoint.as_deref(),
            Some("n2://a:50002")
        );
        assert!(!snap.repair_running);
        assert!(!snap.rebalance_running);
    }
}
