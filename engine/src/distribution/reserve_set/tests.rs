use super::*;
use crate::distribution::layout::ClusterEcLayout;
use crate::distribution::membership::{
    ClusterPolicy, N2_MEMBERSHIP_FORMAT_VERSION, PoolState, ServerPool,
};
use crate::distribution::placement::build_placement_plan;

fn root(id: &str, path: &str) -> StorageRoot {
    StorageRoot {
        id: id.into(),
        physical_id: None,
        path: path.into(),
    }
}

fn membership() -> ClusterMembership {
    ClusterMembership {
        format_version: N2_MEMBERSHIP_FORMAT_VERSION,
        cluster_id: "cluster".into(),
        epoch: 7,
        binding_epoch: 3,
        pools: vec![ServerPool {
            id: "pool-a".into(),
            generation: 7,
            policy: ClusterPolicy {
                object_data_shards: 2,
                object_parity_shards: 1,
            },
            state: PoolState::Writable,
            node_ids: vec!["a".into(), "b".into(), "c".into()],
        }],
        nodes: ["a", "b", "c"]
            .into_iter()
            .map(|id| ClusterNode {
                id: id.into(),
                instance_id: format!("instance-{id}"),
                binding_epoch: 1,
                endpoint: format!("https://{id}"),
                native_endpoint: Some(format!("n2://{id}:50002")),
                state: Some(NodeState::Online),
                weight: 1,
                storage_roots: vec![root(&format!("logical-{id}"), &format!("/disk/{id}"))],
                joined_epoch: 1,
                capacity_bytes: Some(1_000),
                used_bytes: Some(100),
                last_seen_unix: None,
            })
            .collect(),
        reserve_nodes: vec![ReserveNode {
            instance_id: "reserve-1".into(),
            endpoint: "https://reserve-1".into(),
            native_endpoint: Some("n2://reserve-1:50002".into()),
            state: Some(NodeState::Online),
            storage_roots: vec![root("physical-reserve", "/reserve/disk")],
            capacity_bytes: Some(1_000),
            last_seen_unix: None,
        }],
        retired_instances: Vec::new(),
    }
}

#[test]
fn promotion_preserves_logical_slot_and_topology() {
    let before = membership();
    let after = plan_reserve_promotion(&before, "b", "reserve-1").unwrap();
    assert_eq!(after.epoch, before.epoch);
    assert_eq!(after.binding_epoch, before.binding_epoch + 1);
    assert_eq!(after.pools, before.pools);
    let binding = after.nodes.iter().find(|node| node.id == "b").unwrap();
    assert_eq!(binding.instance_id, "reserve-1");
    assert_eq!(binding.storage_roots[0].id, "logical-b");
    assert_eq!(
        binding.storage_roots[0].physical_id.as_deref(),
        Some("physical-reserve")
    );
    assert_eq!(binding.storage_roots[0].path, "/reserve/disk");
    assert!(after.reserve_nodes.is_empty());
    assert_eq!(after.retired_instances[0].instance_id, "instance-b");
    validate_reserve_transition(&before, &after).unwrap();
}

#[test]
fn promotion_preserves_every_recorded_placement_coordinate() {
    let before = membership();
    let after = plan_reserve_promotion(&before, "b", "reserve-1").unwrap();
    let layout = ClusterEcLayout {
        data_shards: 2,
        parity_shards: 1,
    };
    let before_plan = build_placement_plan(
        "fragment",
        "videos/main.bin",
        "version",
        "pool-a",
        7,
        before.epoch,
        before.binding_epoch,
        layout,
        &before.active_write_nodes_for_pool("pool-a"),
    )
    .unwrap();
    let after_plan = build_placement_plan(
        "fragment",
        "videos/main.bin",
        "version",
        "pool-a",
        7,
        after.epoch,
        after.binding_epoch,
        layout,
        &after.active_write_nodes_for_pool("pool-a"),
    )
    .unwrap();
    let logical_before = before_plan
        .placements
        .iter()
        .map(|placement| {
            (
                placement.shard_index,
                placement.role.clone(),
                placement.node_id.clone(),
                placement.root_id.clone(),
            )
        })
        .collect::<Vec<_>>();
    let logical_after = after_plan
        .placements
        .iter()
        .map(|placement| {
            (
                placement.shard_index,
                placement.role.clone(),
                placement.node_id.clone(),
                placement.root_id.clone(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(logical_after, logical_before);
    let replacement = after_plan
        .placements
        .iter()
        .find(|placement| placement.node_id == "b")
        .unwrap();
    assert_eq!(replacement.instance_id, "reserve-1");
    assert_eq!(replacement.binding_epoch, after.binding_epoch);
}

#[test]
fn reserve_capacity_below_recorded_slot_usage_is_rejected() {
    let mut before = membership();
    before.reserve_nodes[0].capacity_bytes = Some(99);
    let error = plan_reserve_promotion(&before, "b", "reserve-1").unwrap_err();
    assert!(error.to_string().contains("below slot 'b' recorded usage"));
}

#[test]
fn activation_changes_only_recovery_state() {
    let promoted = plan_reserve_promotion(&membership(), "b", "reserve-1").unwrap();
    let activated = activation_membership(&promoted, "b").unwrap();
    assert_eq!(
        validate_activation_transition(&promoted, &activated).unwrap(),
        "b"
    );
    assert_eq!(promoted.epoch, activated.epoch);
    assert_eq!(promoted.binding_epoch, activated.binding_epoch);
    assert_eq!(promoted.pools, activated.pools);
    assert_eq!(
        serde_json::to_value(&promoted.retired_instances).unwrap(),
        serde_json::to_value(&activated.retired_instances).unwrap()
    );
    let mut invalid = activated.clone();
    invalid.nodes[0].endpoint.push_str("/changed");
    assert!(validate_activation_transition(&promoted, &invalid).is_err());
}

#[test]
fn membership_v5_serializes_explicit_slot_bindings() {
    let value = serde_json::to_value(membership()).unwrap();
    assert_eq!(value["format_version"], 5);
    assert!(value.get("slot_bindings").is_some());
    assert!(value.get("nodes").is_none());
    assert!(value["slot_bindings"][0].get("slot_id").is_some());
    assert!(value["slot_bindings"][0].get("id").is_none());
    assert!(value["pools"][0].get("slot_ids").is_some());
    assert!(value["pools"][0].get("node_ids").is_none());
}

#[test]
fn reserve_is_never_added_to_pool_or_placement_width() {
    let before = membership();
    assert_eq!(before.nodes_for_pool("pool-a").len(), 3);
    assert_eq!(before.active_write_nodes().len(), 3);
    assert_eq!(before.reserve_nodes.len(), 1);
    assert!(before.pool_for_node("reserve-1").is_none());
}

#[test]
fn incompatible_root_count_is_rejected() {
    let mut before = membership();
    before.reserve_nodes[0]
        .storage_roots
        .push(root("physical-reserve-2", "/reserve/disk-2"));
    assert!(plan_reserve_promotion(&before, "b", "reserve-1").is_err());
}
