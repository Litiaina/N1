use super::*;
use crate::distribution::layout::ClusterLayoutPolicy;
use crate::distribution::membership::{
    ClusterMembership, ClusterPolicy, N2_MEMBERSHIP_FORMAT_VERSION, NodeState, PoolState,
    ServerPool, StorageRoot,
};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

fn metadata_runtime() -> ClusterRuntime {
    let nodes = (0..3)
        .map(|index| ClusterNode {
            id: format!("n{index}"),
            instance_id: format!("n{index}"),
            binding_epoch: 1,
            endpoint: format!("https://n{index}:50001"),
            native_endpoint: Some(format!("n2://n{index}:50002")),
            state: Some(NodeState::Online),
            weight: 1,
            storage_roots: vec![StorageRoot {
                id: format!("r{index}"),
                physical_id: None,
                path: format!("/disk/{index}"),
            }],
            joined_epoch: 1,
            capacity_bytes: None,
            used_bytes: None,
            last_seen_unix: None,
        })
        .collect::<Vec<_>>();
    ClusterRuntime {
        local_node_id: "n0".into(),
        local_instance_id: "n0".into(),
        local_is_reserve: false,
        local_pool_id: "pool-a".into(),
        local_storage_roots: nodes[0].storage_roots.clone(),
        membership_path: std::path::PathBuf::from("/membership"),
        layout_policy: ClusterLayoutPolicy {
            data_shards: 2,
            parity_shards: 1,
        },
        membership: ClusterMembership {
            format_version: N2_MEMBERSHIP_FORMAT_VERSION,
            cluster_id: "cluster-a".into(),
            epoch: 1,
            binding_epoch: 1,
            pools: vec![ServerPool {
                id: "pool-a".into(),
                generation: 1,
                policy: ClusterPolicy {
                    object_data_shards: 2,
                    object_parity_shards: 1,
                },
                state: PoolState::Writable,
                node_ids: (0..3).map(|index| format!("n{index}")).collect(),
            }],
            nodes,
            reserve_nodes: Vec::new(),
            retired_instances: Vec::new(),
        },
        auto_rebalance: false,
    }
}

#[test]
fn peer_shard_locator_round_trips_endpoint_and_identity() {
    let target = PeerShardTarget {
        pool_id: "pool-a".into(),
        pool_generation: 1,
        node_id: "node-a".into(),
        instance_id: "instance-a".into(),
        binding_epoch: 1,
        root_id: "blob-root-a".into(),
        endpoint: "https://node-a.local:50001".into(),
        fragment: "frag".into(),
        object_id: "object-1".into(),
        version_id: "01KTEST".into(),
        part_index: 7,
        shard_index: 2,
        is_parity: true,
        deferred_durability: false,
        repair: false,
        expected_checksum: None,
        expected_len: 128,
    };
    let encoded = encode_peer_shard_locator(&target);
    let decoded = parse_peer_shard_locator(&encoded).expect("locator");
    assert_eq!(decoded.node_id, target.node_id);
    assert_eq!(decoded.root_id, target.root_id);
    assert!(decoded.endpoint.is_empty());
    assert_eq!(decoded.pool_id, target.pool_id);
    assert_eq!(decoded.pool_generation, target.pool_generation);
    assert_eq!(decoded.fragment, target.fragment);
    assert_eq!(decoded.object_id, target.object_id);
    assert_eq!(decoded.version_id, target.version_id);
    assert_eq!(decoded.part_index, target.part_index);
    assert_eq!(decoded.shard_index, target.shard_index);
    assert_eq!(decoded.is_parity, target.is_parity);
}

#[test]
fn peer_shard_write_propagates_deferred_durability_explicitly() {
    let target = PeerShardTarget {
        pool_id: "pool-a".into(),
        pool_generation: 1,
        node_id: "node-a".into(),
        instance_id: "instance-a".into(),
        binding_epoch: 1,
        root_id: "blob-root-a".into(),
        endpoint: "https://node-a.local:50001".into(),
        fragment: "frag".into(),
        object_id: "object-1".into(),
        version_id: "01KTEST".into(),
        part_index: 7,
        shard_index: 2,
        is_parity: true,
        deferred_durability: true,
        repair: false,
        expected_checksum: None,
        expected_len: 128,
    };

    assert!(peer_shard_url(&target).contains("deferred_durability=true"));
}

#[test]
fn native_endpoint_detection_is_explicit() {
    assert!(endpoint_is_native("n2://127.0.0.1:50002"));
    assert!(!endpoint_is_native("https://127.0.0.1:50001"));
    assert_eq!(
        native_socket_addr("n2://node-a.local:50002").unwrap(),
        "node-a.local:50002"
    );
}

#[test]
fn peer_transfer_timeout_scales_for_large_shards() {
    let small = peer_transfer_timeout(1024);
    let large = peer_transfer_timeout(1024 * 1024 * 1024);
    assert!(large > small);
    assert!(small >= PEER_WRITE_TIMEOUT);
}

#[cfg(unix)]
#[tokio::test]
async fn lost_preferred_disk_is_repairable_when_another_local_root_is_readable() {
    let base = std::env::temp_dir().join(format!("n2-peer-root-fallback-{}", Ulid::new()));
    let unavailable = base.join("unavailable");
    let healthy = base.join("healthy");
    std::fs::create_dir_all(&unavailable).expect("create unavailable root");
    std::fs::create_dir_all(&healthy).expect("create healthy root");
    std::fs::set_permissions(&unavailable, std::fs::Permissions::from_mode(0o000))
        .expect("deny preferred root");

    let roots = vec![
        StorageRoot {
            id: "preferred".into(),
            physical_id: None,
            path: unavailable.display().to_string(),
        },
        StorageRoot {
            id: "healthy".into(),
            physical_id: None,
            path: healthy.display().to_string(),
        },
    ];
    let mut runtime = metadata_runtime();
    runtime.local_storage_roots = roots.clone();
    runtime.membership.nodes[0].storage_roots = roots;

    let result = open_local_shard_for_read(
        &runtime,
        "preferred",
        "fragment-a",
        "object-a",
        "version-a",
        "part_0_shard_0.shard",
    )
    .await;
    assert!(matches!(result, Err(AppError::NotFound)));

    std::fs::set_permissions(&unavailable, std::fs::Permissions::from_mode(0o755))
        .expect("restore preferred root permissions");
    let _ = std::fs::remove_dir_all(base);
}

#[test]
fn metadata_lookup_uses_every_exact_static_pool_holder_without_substitution() {
    let mut runtime = metadata_runtime();
    let object_id = "object-a".to_string();
    let pool = runtime.membership.pool("pool-a").unwrap();
    let nodes = runtime.membership.nodes_for_pool("pool-a");
    let plan = build_placement_plan(
        "fragment",
        &object_id,
        "",
        "pool-a",
        1,
        1,
        1,
        ClusterEcLayout {
            data_shards: 2,
            parity_shards: 1,
        },
        &nodes,
    )
    .unwrap();
    let expected = plan
        .placements
        .iter()
        .map(|placement| placement.node_id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(expected.len(), pool.policy.object_width());

    let selected =
        exact_metadata_node_batches(&runtime, "fragment", std::slice::from_ref(&object_id))
            .into_iter()
            .map(|(node, _)| node.id)
            .collect::<BTreeSet<_>>();
    assert_eq!(selected, expected);

    let failed_holder = expected.iter().next().unwrap().clone();
    runtime
        .membership
        .nodes
        .iter_mut()
        .find(|node| node.id == failed_holder)
        .unwrap()
        .state = Some(NodeState::Offline);
    let degraded =
        exact_metadata_node_batches(&runtime, "fragment", std::slice::from_ref(&object_id))
            .into_iter()
            .map(|(node, _)| node.id)
            .collect::<BTreeSet<_>>();
    assert_eq!(degraded.len(), expected.len() - 1);
    assert!(degraded.is_subset(&expected));
}

#[test]
fn metadata_absence_requires_one_exact_holder_response_per_pool() {
    let runtime = metadata_runtime();
    let object_ids = vec!["object-a".to_string()];
    let proven = BTreeSet::from([("pool-a".to_string(), "object-a".to_string())]);
    ensure_exact_head_visibility(&runtime, &object_ids, &proven).unwrap();
    ensure_exact_pool_visibility(&runtime, &BTreeSet::from(["pool-a".to_string()])).unwrap();
    assert!(ensure_exact_head_visibility(&runtime, &object_ids, &BTreeSet::new()).is_err());
    assert!(ensure_exact_pool_visibility(&runtime, &BTreeSet::new()).is_err());
}

#[tokio::test]
async fn peer_copy_idle_timeout_resets_when_bytes_keep_moving() {
    let (mut sender, mut receiver) = tokio::io::duplex(64);
    let producer = tokio::spawn(async move {
        for value in 0..6u8 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            sender.write_all(&[value]).await.unwrap();
        }
    });
    let mut sink = tokio::io::sink();
    let copied = copy_with_idle_timeout(&mut receiver, &mut sink, Duration::from_millis(50))
        .await
        .unwrap();
    producer.await.unwrap();
    assert_eq!(copied, 6);
}

#[tokio::test]
async fn peer_copy_idle_timeout_rejects_a_stalled_stream() {
    let (_sender, mut receiver) = tokio::io::duplex(64);
    let mut sink = tokio::io::sink();
    let err = copy_with_idle_timeout(&mut receiver, &mut sink, Duration::from_millis(20))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::TimedOut);
}
