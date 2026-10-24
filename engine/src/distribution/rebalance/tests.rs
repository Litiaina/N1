use super::*;
use crate::core::disk_manager::{PartMetadata, ShardInfo};
use crate::distribution::membership::{
    ClusterMembership, ClusterNode, ClusterPolicy, N2_MEMBERSHIP_FORMAT_VERSION, NodeState,
    PoolState, ServerPool, StorageRoot,
};
use crate::distribution::peer::{PeerShardTarget, encode_peer_shard_locator};

fn node(id: &str) -> ClusterNode {
    ClusterNode {
        id: id.into(),
        instance_id: id.into(),
        binding_epoch: 1,
        endpoint: format!("http://{id}"),
        native_endpoint: Some(format!("n2://{id}:50002")),
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
    }
}

fn runtime(epoch: u64) -> ClusterRuntime {
    ClusterRuntime {
        local_node_id: "a".into(),
        local_instance_id: "a".into(),
        local_is_reserve: false,
        local_pool_id: "pool-a".into(),
        local_storage_roots: vec![],
        membership_path: "membership.json".into(),
        layout_policy: crate::distribution::layout::ClusterLayoutPolicy::default(),
        membership: ClusterMembership {
            format_version: N2_MEMBERSHIP_FORMAT_VERSION,
            cluster_id: "test".into(),
            epoch,
            binding_epoch: 1,
            pools: vec![ServerPool {
                id: "pool-a".into(),
                generation: epoch,
                policy: ClusterPolicy {
                    object_data_shards: 2,
                    object_parity_shards: 1,
                },
                state: PoolState::Writable,
                node_ids: vec!["a".into(), "b".into(), "c".into()],
            }],
            nodes: vec![node("a"), node("b"), node("c")],
            reserve_nodes: Vec::new(),
            retired_instances: Vec::new(),
        },
        auto_rebalance: false,
    }
}

fn base_meta(epoch: u64, data_shards: u16, parity_shards: u16) -> ObjectMetadata {
    ObjectMetadata {
        object_key: "obj".into(),
        packed_ext_u64: 0,
        fragment: "frag".into(),
        version_id: "v".into(),
        upload_session_id: "s".into(),
        owner_uid: "u".into(),
        content_type: "application/octet-stream".into(),
        total_size_bytes: 1,
        created_at: 0,
        pool_id: "pool-a".into(),
        pool_generation: epoch,
        membership_epoch: epoch,
        data_shards,
        parity_shards,
        stripe_size_bytes: 4096,
        is_delete_marker: false,
        parts: vec![],
    }
}

fn meta_matching_runtime(runtime: &ClusterRuntime) -> ObjectMetadata {
    let active = runtime.membership.active_newest_pool_nodes();
    let layout = resolve_write_layout(active.len(), active.len(), &runtime.layout_policy).unwrap();
    let plan = build_placement_plan(
        "frag",
        "obj",
        "v",
        "pool-a",
        runtime.membership.pools[0].generation,
        runtime.membership.epoch,
        runtime.membership.binding_epoch,
        layout,
        &active,
    )
    .unwrap();
    let shards = plan
        .placements
        .iter()
        .map(|placement| {
            let is_parity = matches!(placement.role, ClusterShardRole::Parity);
            let absolute_path = if placement.node_id == runtime.local_node_id {
                format!("/tmp/part_0_shard_{}", placement.shard_index)
            } else {
                encode_peer_shard_locator(&PeerShardTarget {
                    pool_id: plan.pool_id.clone(),
                    pool_generation: plan.pool_generation,
                    node_id: placement.node_id.clone(),
                    instance_id: placement.instance_id.clone(),
                    binding_epoch: plan.binding_epoch,
                    root_id: placement.root_id.clone(),
                    endpoint: placement
                        .native_endpoint
                        .clone()
                        .unwrap_or_else(|| placement.endpoint.clone()),
                    fragment: "frag".into(),
                    object_id: "obj".into(),
                    version_id: "v".into(),
                    part_index: 0,
                    shard_index: placement.shard_index,
                    is_parity,
                    expected_len: 1,
                    deferred_durability: false,
                    repair: false,
                    expected_checksum: None,
                })
            };
            ShardInfo {
                shard_index: placement.shard_index,
                file_name: format!("part_0_shard_{}", placement.shard_index),
                absolute_path,
                size_bytes: 1,
                checksum: String::new(),
                block_checksums: Vec::new(),
                checksum_block_size: 0,
                is_parity,
            }
        })
        .collect::<Vec<_>>();
    let mut meta = base_meta(
        runtime.membership.epoch,
        layout.data_shards as u16,
        layout.parity_shards as u16,
    );
    meta.parts = vec![PartMetadata {
        part_index: 0,
        size_bytes: 1,
        created_at: 0,
        checksum: None,
        shards,
    }];
    meta
}

#[test]
fn layout_match_accepts_exact_current_epoch_shape_and_placement() {
    let runtime = runtime(4);
    let meta = meta_matching_runtime(&runtime);
    let status = object_layout_status(&meta, &runtime).unwrap();
    assert_eq!(status.state, "current_layout");
    assert!(status.layout_matches_current);
    assert!(status.placement_matches_current);
    assert!(metadata_matches_current_layout(&meta, &runtime));
}

#[test]
fn layout_match_rejects_older_recorded_epoch_even_when_shards_are_readable() {
    let runtime = runtime(4);
    let mut meta = meta_matching_runtime(&runtime);
    meta.membership_epoch = 2;
    let status = object_layout_status(&meta, &runtime).unwrap();
    assert_eq!(status.state, "older_recorded_layout");
    assert!(status.placement_matches_current);
    assert!(!status.layout_matches_current);
    assert!(!metadata_matches_current_layout(&meta, &runtime));
}

#[test]
fn layout_match_rejects_missing_remote_cluster_holders() {
    let runtime = runtime(1);
    let mut meta = base_meta(0, 1, 1);
    meta.parts = vec![PartMetadata {
        part_index: 0,
        size_bytes: 1,
        created_at: 0,
        checksum: None,
        shards: vec![ShardInfo {
            shard_index: 0,
            file_name: "s".into(),
            absolute_path: "/local".into(),
            size_bytes: 1,
            checksum: String::new(),
            block_checksums: Vec::new(),
            checksum_block_size: 0,
            is_parity: false,
        }],
    }];
    assert!(!metadata_matches_current_layout(&meta, &runtime));
}
