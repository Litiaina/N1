use crate::core::operation_gate::cluster_safety_read_only_state;
use crate::distribution::layout::resolve_configured_layout;
use crate::distribution::membership::{
    ClusterNode, ClusterRuntime, NodeState, live_active_write_nodes, live_readable_nodes,
};
use crate::distribution::peer::{cluster_secret, peer_http_client};
use crate::distribution::rebalance::{plan_rebalance, rebalance_status, repair_status};
use crate::enums::app_error_enum::AppError;
use futures_util::future::join_all;
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
    pub instance_id: String,
    pub binding_epoch: u64,
    pub state: NodeState,
    pub accepts_writes: bool,
    pub readable: bool,
    pub storage_readable: bool,
    pub storage_writable: bool,
    pub storage_minimum_available_bytes: Option<u64>,
    pub storage_issues: Vec<String>,
    pub endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_endpoint: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReserveNodeHealth {
    pub instance_id: String,
    pub state: NodeState,
    pub endpoint: String,
    pub storage_roots: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterHealthSnapshot {
    pub state: ClusterHealthState,
    pub membership_epoch: u64,
    pub binding_epoch: u64,
    pub active_write_nodes: usize,
    pub readable_nodes: usize,
    pub data_shards: usize,
    pub parity_shards: usize,
    pub data_plane_readable: bool,
    pub data_plane_writable: bool,
    pub ncnx_replicas: usize,
    pub estimated_ncnx_replicas_reachable: usize,
    pub ncnx_replicas_required: usize,
    pub ncnx_strictly_writable: bool,
    pub effective_client_writable: bool,
    pub safety_read_only: bool,
    pub manual_rebalance_available: bool,
    pub layout_drift_is_health_failure: bool,
    pub automatic_rebalance: bool,
    pub repair_running: bool,
    pub rebalance_running: bool,
    pub historical_recovery_active: bool,
    pub nodes: Vec<ClusterNodeHealth>,
    pub reserve_nodes: Vec<ReserveNodeHealth>,
    pub notes: Vec<String>,
}

pub async fn cluster_health_snapshot(
    runtime: &ClusterRuntime,
) -> Result<ClusterHealthSnapshot, AppError> {
    let endpoint_active = live_active_write_nodes(runtime).await;
    let endpoint_readable = live_readable_nodes(runtime).await;
    let readiness = storage_readiness_by_node(runtime).await;
    let active = endpoint_active
        .into_iter()
        .filter(|node| {
            readiness
                .iter()
                .find(|(node_id, _)| node_id == &node.id)
                .is_some_and(|(_, status)| status.writable)
        })
        .collect();
    let readable = endpoint_readable
        .into_iter()
        .filter(|node| {
            readiness
                .iter()
                .find(|(node_id, _)| node_id == &node.id)
                .is_some_and(|(_, status)| status.readable)
        })
        .collect();
    cluster_health_snapshot_with_readiness(runtime, active, readable, &readiness)
}

pub fn cluster_health_snapshot_with_live_nodes(
    runtime: &ClusterRuntime,
    active: Vec<ClusterNode>,
    readable: Vec<ClusterNode>,
) -> Result<ClusterHealthSnapshot, AppError> {
    let readiness = runtime
        .membership
        .nodes
        .iter()
        .map(|node| {
            (
                node.id.clone(),
                crate::core::capacity_manager::StorageReadiness {
                    readable: readable.iter().any(|candidate| candidate.id == node.id),
                    writable: active.iter().any(|candidate| candidate.id == node.id),
                    root_count: node.storage_roots.len(),
                    readable_roots: node.storage_roots.len(),
                    writable_roots: node.storage_roots.len(),
                    minimum_available_bytes: None,
                    issues: Vec::new(),
                },
            )
        })
        .collect::<Vec<_>>();
    cluster_health_snapshot_with_readiness(runtime, active, readable, &readiness)
}

fn cluster_health_snapshot_with_readiness(
    runtime: &ClusterRuntime,
    active: Vec<ClusterNode>,
    readable: Vec<ClusterNode>,
    readiness: &[(String, crate::core::capacity_manager::StorageReadiness)],
) -> Result<ClusterHealthSnapshot, AppError> {
    let write_pool = runtime.membership.newest_writable_pool().ok_or_else(|| {
        AppError::ServiceUnavailable("cluster has no writable server pool".into())
    })?;
    let pool_nodes = write_pool.node_ids.iter().collect::<HashSet<_>>();
    let configured_blob = runtime
        .membership
        .active_write_nodes_for_pool(&write_pool.id);
    let active_blob = active
        .iter()
        .filter(|node| pool_nodes.contains(&node.id))
        .cloned()
        .collect::<Vec<_>>();
    let readable_blob = readable
        .iter()
        .filter(|node| pool_nodes.contains(&node.id))
        .cloned()
        .collect::<Vec<_>>();
    let readable_namespace = readable_blob.len();
    let configured_layout =
        resolve_configured_layout(configured_blob.len(), &runtime.layout_policy)
            .map_err(AppError::BadRequest)?;
    let layout = configured_layout;
    let data_plane_readable = readable_blob.len() >= layout.data_shards;
    let data_plane_writable = active_blob.len() >= layout.total_shards();
    let configured_namespace_nodes = runtime.membership.nodes_for_pool(&write_pool.id).len();
    let ncnx_replicas = write_pool.policy.namespace_replicas();
    let ncnx_replicas_required = ncnx_replicas;
    let estimated_ncnx_replicas_reachable = readable_namespace.min(ncnx_replicas);
    // NCNX placement is deterministic across the static pool membership.
    // Requiring every pool node here makes the cluster-wide writable signal
    // truthful for every possible key; an individual existing key may remain
    // readable with only its selected replicas.
    let ncnx_strictly_writable = readable_namespace == configured_namespace_nodes;
    let safety_state = cluster_safety_read_only_state();
    let safety_read_only = safety_state.is_some();
    let effective_client_writable =
        data_plane_writable && ncnx_strictly_writable && !safety_read_only;
    let missing_blob_writable = configured_blob.len().saturating_sub(active_blob.len());
    let missing_namespace_readable = configured_namespace_nodes.saturating_sub(readable_namespace);
    let historical_recovery_active = runtime
        .membership
        .nodes
        .iter()
        .any(|node| node.effective_state() == NodeState::Degraded);
    let live_node_ids = readable
        .iter()
        .map(|node| node.id.clone())
        .collect::<HashSet<_>>();
    let writable_node_ids = active
        .iter()
        .map(|node| node.id.clone())
        .collect::<HashSet<_>>();
    let state = if !data_plane_readable || safety_read_only {
        ClusterHealthState::Critical
    } else if !effective_client_writable {
        ClusterHealthState::Degraded
    } else if missing_blob_writable > 0
        || missing_namespace_readable > 0
        || historical_recovery_active
    {
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
    if historical_recovery_active {
        notes.push(
            "a replacement logical slot is serving new full-width writes while historical NCNX/NCDX/shard layouts heal in the background"
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
        if ncnx_strictly_writable {
            notes.push(format!(
                "{} configured pool node(s) are currently unreachable; NCNX placement remains readable for unaffected keys but new object writes stop until the fixed {}+{} width is available",
                missing_blob_writable,
                configured_layout.data_shards,
                configured_layout.parity_shards
            ));
        } else {
            notes.push(format!(
                "{} configured server-pool node(s) are currently unreachable and strict NCNX publication is unavailable; existing reconstructable objects may still be read",
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
    if !ncnx_strictly_writable {
        notes.push(format!(
            "namespace NCNX placement is not uniformly writable: {} of {} server-pool nodes are reachable ({} replicas per key)",
            readable_namespace, configured_namespace_nodes, ncnx_replicas_required
        ));
    }
    if data_plane_writable && !ncnx_strictly_writable {
        notes.push(
            "data-plane placement can accept shards, but client writes still require strict NCNX replica publication"
                .to_string(),
        );
    }

    Ok(ClusterHealthSnapshot {
        state,
        membership_epoch: runtime.membership.epoch,
        binding_epoch: runtime.membership.binding_epoch,
        active_write_nodes: active.len(),
        readable_nodes: readable.len(),
        data_shards: layout.data_shards,
        parity_shards: layout.parity_shards,
        data_plane_readable,
        data_plane_writable,
        ncnx_replicas,
        estimated_ncnx_replicas_reachable,
        ncnx_replicas_required,
        ncnx_strictly_writable,
        effective_client_writable,
        safety_read_only,
        manual_rebalance_available: active_blob.len() >= layout.total_shards(),
        layout_drift_is_health_failure: false,
        automatic_rebalance: runtime.auto_rebalance,
        repair_running: repair_status.running,
        rebalance_running: rebalance_status.running,
        historical_recovery_active,
        nodes: runtime
            .membership
            .nodes
            .iter()
            .map(|node| {
                let configured_state = node.effective_state();
                let storage = readiness
                    .iter()
                    .find(|(node_id, _)| node_id == &node.id)
                    .map(|(_, status)| status);
                let state = if configured_state.is_readable()
                    && !node.is_local(&runtime.local_instance_id)
                    && !live_node_ids.contains(&node.id)
                {
                    NodeState::Offline
                } else {
                    configured_state
                };
                ClusterNodeHealth {
                    node_id: node.id.clone(),
                    instance_id: node.instance_id.clone(),
                    binding_epoch: node.binding_epoch,
                    accepts_writes: state.accepts_new_writes()
                        && effective_client_writable
                        && writable_node_ids.contains(&node.id),
                    readable: state.is_readable(),
                    storage_readable: storage.is_some_and(|status| status.readable),
                    storage_writable: storage.is_some_and(|status| status.writable),
                    storage_minimum_available_bytes: storage
                        .and_then(|status| status.minimum_available_bytes),
                    storage_issues: storage
                        .map(|status| status.issues.clone())
                        .unwrap_or_else(|| vec!["storage readiness probe failed".to_string()]),
                    state,
                    endpoint: node.endpoint.clone(),
                    native_endpoint: node.native_endpoint.clone(),
                }
            })
            .collect(),
        reserve_nodes: runtime
            .membership
            .reserve_nodes
            .iter()
            .map(|node| ReserveNodeHealth {
                instance_id: node.instance_id.clone(),
                state: node.effective_state(),
                endpoint: node.endpoint.clone(),
                storage_roots: node.storage_roots.len(),
            })
            .collect(),
        notes,
    })
}

async fn storage_readiness_by_node(
    runtime: &ClusterRuntime,
) -> Vec<(String, crate::core::capacity_manager::StorageReadiness)> {
    let probes = runtime.membership.nodes.iter().cloned().map(|node| {
        let local_instance_id = runtime.local_instance_id.clone();
        async move {
            let result = async {
                if node.is_local(&local_instance_id) {
                    return crate::core::capacity_manager::get_storage_readiness().await;
                }
                let client = peer_http_client()?;
                let mut request = client.get(format!(
                    "{}/n2/peer/storage-readiness",
                    node.endpoint.trim_end_matches('/')
                ));
                if let Some(secret) = cluster_secret() {
                    request = request.header("x-n2-cluster-secret", secret);
                }
                let response = request
                    .timeout(std::time::Duration::from_secs(5))
                    .send()
                    .await
                    .map_err(|error| {
                        AppError::ServiceUnavailable(format!(
                            "storage readiness probe for '{}' failed: {error}",
                            node.id
                        ))
                    })?;
                if !response.status().is_success() {
                    return Err(AppError::ServiceUnavailable(format!(
                        "storage readiness probe for '{}' returned {}",
                        node.id,
                        response.status()
                    )));
                }
                response
                    .json::<crate::core::capacity_manager::StorageReadiness>()
                    .await
                    .map_err(|error| {
                        AppError::InternalServerError(format!(
                            "decode storage readiness for '{}': {error}",
                            node.id
                        ))
                    })
            }
            .await;
            (
                node.id,
                result.unwrap_or_else(|error| crate::core::capacity_manager::StorageReadiness {
                    readable: false,
                    writable: false,
                    root_count: node.storage_roots.len(),
                    readable_roots: 0,
                    writable_roots: 0,
                    minimum_available_bytes: None,
                    issues: vec![error.to_string()],
                }),
            )
        }
    });
    join_all(probes).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::layout::ClusterLayoutPolicy;
    use crate::distribution::membership::{
        ClusterMembership, ClusterNode, ClusterPolicy, N2_MEMBERSHIP_FORMAT_VERSION, PoolState,
        ServerPool,
    };
    use std::path::PathBuf;

    fn node(id: &str, state: NodeState) -> ClusterNode {
        ClusterNode {
            id: id.into(),
            instance_id: id.into(),
            binding_epoch: 1,
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
            node("ns-a", NodeState::Online),
            node("ns-b", NodeState::Online),
        ];
        nodes.extend(blob_nodes);
        ClusterRuntime {
            local_node_id: "blob-a".into(),
            local_instance_id: "blob-a".into(),
            local_is_reserve: false,
            local_pool_id: "pool-a".into(),
            local_storage_roots: vec![],
            membership_path: PathBuf::from("membership.json"),
            layout_policy: ClusterLayoutPolicy {
                data_shards: 1,
                parity_shards: 1,
            },
            membership: ClusterMembership {
                format_version: N2_MEMBERSHIP_FORMAT_VERSION,
                cluster_id: "test".into(),
                epoch: 3,
                binding_epoch: 1,
                pools: vec![ServerPool {
                    id: "pool-a".into(),
                    generation: 3,
                    policy: ClusterPolicy {
                        object_data_shards: 1,
                        object_parity_shards: 1,
                    },
                    state: PoolState::Writable,
                    node_ids: nodes.iter().map(|node| node.id.clone()).collect(),
                }],
                nodes,
                reserve_nodes: Vec::new(),
                retired_instances: Vec::new(),
            },
            auto_rebalance: false,
        }
    }

    #[test]
    fn health_reflects_writable_layout_capacity() {
        let runtime = runtime(vec![
            node("blob-a", NodeState::Online),
            node("blob-b", NodeState::Online),
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
        assert!(snap.manual_rebalance_available);
        assert!(snap.data_plane_readable);
        assert!(snap.data_plane_writable);
        assert_eq!(snap.ncnx_replicas, 2);
        assert_eq!(snap.estimated_ncnx_replicas_reachable, 2);
        assert_eq!(snap.ncnx_replicas_required, 2);
        assert!(snap.ncnx_strictly_writable);
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
            node("blob-a", NodeState::Online),
            node("blob-b", NodeState::Online),
            node("blob-c", NodeState::Online),
        ]);
        let live = runtime.membership.nodes[..4].to_vec();
        let snap = cluster_health_snapshot_with_live_nodes(&runtime, live.clone(), live).unwrap();
        assert_eq!(snap.state, ClusterHealthState::Degraded);
        assert_eq!(snap.active_write_nodes, 4);
        assert_eq!(snap.data_shards, 1);
        assert_eq!(snap.parity_shards, 1);
        assert!(snap.data_plane_writable);
        assert!(!snap.ncnx_strictly_writable);
        assert!(!snap.effective_client_writable);
        assert_eq!(snap.nodes[4].state, NodeState::Offline);
    }

    #[test]
    fn fixed_two_plus_one_stops_writes_below_unified_pool_width() {
        let mut runtime = runtime(vec![
            node("blob-a", NodeState::Online),
            node("blob-b", NodeState::Online),
            node("blob-c", NodeState::Online),
        ]);
        runtime.layout_policy = ClusterLayoutPolicy {
            data_shards: 2,
            parity_shards: 1,
        };
        runtime.membership.pools[0].policy.object_data_shards = 2;
        let live = runtime.membership.nodes[..2].to_vec();

        let snap = cluster_health_snapshot_with_live_nodes(&runtime, live.clone(), live).unwrap();

        assert_eq!(snap.state, ClusterHealthState::Degraded);
        assert_eq!(snap.data_shards, 2);
        assert_eq!(snap.parity_shards, 1);
        assert!(snap.data_plane_readable);
        assert!(!snap.data_plane_writable);
        assert!(!snap.ncnx_strictly_writable);
        assert!(!snap.effective_client_writable);
    }

    #[test]
    fn wider_fixed_layout_is_healthy_only_at_full_width() {
        let mut runtime = runtime(vec![
            node("blob-a", NodeState::Online),
            node("blob-b", NodeState::Online),
            node("blob-c", NodeState::Online),
            node("blob-d", NodeState::Online),
        ]);
        runtime.layout_policy = ClusterLayoutPolicy {
            data_shards: 3,
            parity_shards: 1,
        };
        runtime.membership.pools[0].policy.object_data_shards = 3;
        let live = runtime.membership.active_write_nodes();

        let snap = cluster_health_snapshot_with_live_nodes(&runtime, live.clone(), live).unwrap();

        assert_eq!(snap.data_shards, 3);
        assert_eq!(snap.parity_shards, 1);
        assert!(snap.data_plane_readable);
        assert!(snap.data_plane_writable);
        assert!(snap.effective_client_writable);
    }

    #[test]
    fn health_reports_strict_ncnx_blocking_writes() {
        let runtime = runtime(vec![
            node("blob-a", NodeState::Online),
            node("blob-b", NodeState::Online),
        ]);
        let active = runtime.membership.active_write_nodes();
        let readable = vec![runtime.membership.nodes[0].clone()];
        let snap = cluster_health_snapshot_with_live_nodes(&runtime, active, readable).unwrap();
        assert_eq!(snap.state, ClusterHealthState::Degraded);
        assert!(snap.data_plane_readable);
        assert!(snap.data_plane_writable);
        assert!(!snap.ncnx_strictly_writable);
        assert!(!snap.effective_client_writable);
        assert!(snap.nodes.iter().all(|node| !node.accepts_writes));
        assert!(
            snap.notes
                .iter()
                .any(|note| note.contains("namespace NCNX placement is not uniformly writable"))
        );
    }
}
