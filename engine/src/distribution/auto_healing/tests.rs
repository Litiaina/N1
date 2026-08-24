use super::*;
use crate::distribution::layout::ClusterLayoutPolicy;
use crate::distribution::membership::{
    ClusterMembership, ClusterPolicy, N2_MEMBERSHIP_FORMAT_VERSION, NodeState, PoolState,
    ServerPool, StorageRoot,
};

fn runtime() -> ClusterRuntime {
    ClusterRuntime {
        local_node_id: "node-a".into(),
        local_instance_id: "node-a".into(),
        local_is_reserve: false,
        local_pool_id: "pool-a".into(),
        local_storage_roots: vec![],
        membership_path: PathBuf::new(),
        layout_policy: ClusterLayoutPolicy::default(),
        membership: ClusterMembership {
            format_version: N2_MEMBERSHIP_FORMAT_VERSION,
            cluster_id: "healing-test".into(),
            epoch: 9,
            binding_epoch: 1,
            pools: vec![ServerPool {
                id: "pool-a".into(),
                generation: 9,
                policy: ClusterPolicy {
                    object_data_shards: 2,
                    object_parity_shards: 1,
                },
                state: PoolState::Writable,
                node_ids: ["node-a", "node-b", "node-c", "node-d"]
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
            }],
            nodes: ["node-a", "node-b", "node-c", "node-d"]
                .into_iter()
                .enumerate()
                .map(|(_index, id)| ClusterNode {
                    id: id.into(),
                    instance_id: id.into(),
                    binding_epoch: 1,
                    endpoint: format!("https://{id}"),
                    native_endpoint: None,
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
                })
                .collect(),
            reserve_nodes: Vec::new(),
            retired_instances: Vec::new(),
        },
        auto_rebalance: false,
    }
}

#[test]
fn fixed_layout_healing_requires_every_logical_slot_to_be_live() {
    assert!(full_topology_available(4, 4));
    assert!(!full_topology_available(4, 3));
    assert!(!full_topology_available(4, 2));
    assert!(!full_topology_available(0, 0));
}

#[test]
fn scheduled_overlap_is_not_a_healing_failure() {
    assert!(cycle_is_already_running(&AppError::Conflict(
        CYCLE_ALREADY_RUNNING.into()
    )));
    assert!(!cycle_is_already_running(&AppError::Conflict(
        "replicas disagree".into()
    )));
    assert!(!cycle_is_already_running(&AppError::ServiceUnavailable(
        "peer offline".into()
    )));
}

#[test]
fn healer_pool_and_fragment_owner_are_deterministic_and_bounded() {
    let runtime = runtime();
    let first = select_healer_pool(&runtime, &runtime.membership.nodes, 2);
    let second = select_healer_pool(&runtime, &runtime.membership.nodes, 2);
    assert_eq!(
        first.iter().map(|node| &node.id).collect::<Vec<_>>(),
        second.iter().map(|node| &node.id).collect::<Vec<_>>()
    );
    assert_eq!(first.len(), 2);
    assert_eq!(
        fragment_healer_owner(&runtime, "fragment-a", &first).map(|node| &node.id),
        fragment_healer_owner(&runtime, "fragment-a", &second).map(|node| &node.id)
    );
}

#[test]
fn automatic_pool_uses_half_of_live_nodes_for_ha() {
    let runtime = runtime();
    assert_eq!(
        select_healer_pool(&runtime, &runtime.membership.nodes, 0).len(),
        2
    );
    assert_eq!(
        select_healer_pool(&runtime, &runtime.membership.nodes[..3], 0).len(),
        2
    );
}

#[test]
fn lease_partition_is_stable_and_fragment_specific() {
    assert_eq!(
        healing_partition("fragment-a"),
        healing_partition("fragment-a")
    );
    assert_ne!(
        healing_partition("fragment-a"),
        healing_partition("fragment-b")
    );
}
