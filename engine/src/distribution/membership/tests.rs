use super::*;

fn node(id: &str) -> ClusterNode {
    ClusterNode {
        id: id.into(),
        instance_id: id.into(),
        binding_epoch: 1,
        endpoint: format!("https://{id}:51001"),
        native_endpoint: Some(format!("n2://{id}:51002")),
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

#[test]
fn validates_unified_static_pool_and_derives_ncnx_replicas() {
    let policy = ClusterPolicy {
        object_data_shards: 2,
        object_parity_shards: 1,
    };
    let membership = ClusterMembership {
        format_version: N2_MEMBERSHIP_FORMAT_VERSION,
        cluster_id: "cluster-a".into(),
        epoch: 1,
        binding_epoch: 1,
        pools: vec![ServerPool {
            id: "pool-a".into(),
            generation: 1,
            policy: policy.clone(),
            state: PoolState::Writable,
            node_ids: vec!["a".into(), "b".into(), "c".into()],
        }],
        nodes: vec![node("a"), node("b"), node("c")],
        reserve_nodes: Vec::new(),
        retired_instances: Vec::new(),
    };
    membership.validate().expect("valid membership");
    assert_eq!(policy.namespace_replicas(), 2);
    assert_eq!(membership.newest_writable_pool().unwrap().id, "pool-a");
}

fn runtime_for_authority_test() -> ClusterRuntime {
    let membership = ClusterMembership {
        format_version: N2_MEMBERSHIP_FORMAT_VERSION,
        cluster_id: "cluster-authority".into(),
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
            node_ids: vec!["a".into(), "b".into(), "c".into()],
        }],
        nodes: vec![node("a"), node("b"), node("c")],
        reserve_nodes: Vec::new(),
        retired_instances: Vec::new(),
    };
    ClusterRuntime {
        local_node_id: "a".into(),
        local_instance_id: "a".into(),
        local_is_reserve: false,
        local_pool_id: "pool-a".into(),
        local_storage_roots: membership.nodes[0].storage_roots.clone(),
        membership_path: PathBuf::new(),
        layout_policy: ClusterLayoutPolicy {
            data_shards: 2,
            parity_shards: 1,
        },
        membership,
        auto_rebalance: false,
    }
}

#[test]
fn survivor_write_authority_requires_k_and_a_strict_pool_majority() {
    let runtime = runtime_for_authority_test();
    let full = runtime.membership.nodes_for_pool("pool-a");
    validate_pool_write_authority(&runtime, "pool-a", &full)
        .expect("complete exact pool has write authority");

    validate_pool_write_authority(&runtime, "pool-a", &full[..2])
        .expect("two of three slots satisfy K=2 and strict majority");

    let error = validate_pool_write_authority(&runtime, "pool-a", &full[..1])
        .expect_err("one of three slots has neither K nor majority authority");
    assert!(error.to_string().contains("strict-majority=2"));
    assert!(error.to_string().contains("missing: b, c"));
}

#[test]
fn tied_k_plus_m_layout_requires_one_more_than_k_for_authority() {
    let mut runtime = runtime_for_authority_test();
    runtime.membership.nodes.push(node("d"));
    runtime.membership.pools[0].node_ids.push("d".into());
    runtime.membership.pools[0].policy.object_parity_shards = 2;
    runtime.layout_policy.parity_shards = 2;
    let full = runtime.membership.nodes_for_pool("pool-a");

    let error = validate_pool_write_authority(&runtime, "pool-a", &full[..2])
        .expect_err("K=2 is a split tie in a four-slot pool");
    assert!(error.to_string().contains("strict-majority=3"));
    validate_pool_write_authority(&runtime, "pool-a", &full[..3])
        .expect("three of four slots form the only writable component");
}

#[test]
fn configured_offline_state_is_not_a_shortcut_to_write_authority() {
    let mut runtime = runtime_for_authority_test();
    runtime.membership.nodes[2].state = Some(NodeState::Offline);
    let reachable = runtime.membership.nodes[..2].to_vec();
    let error = validate_pool_write_authority(&runtime, "pool-a", &reachable)
        .expect_err("state change without binding fence must fail closed");
    assert!(
        error
            .to_string()
            .contains("complete the binding/fencing transition")
    );
}

#[test]
fn sealed_pool_retains_exact_namespace_mutation_authority_only() {
    let mut runtime = runtime_for_authority_test();
    runtime.membership.pools[0].state = PoolState::Sealed;
    let full = runtime.membership.nodes_for_pool("pool-a");

    validate_pool_mutation_authority(&runtime, "pool-a", &full)
        .expect("sealed historical pool remains namespace-mutable");
    let error = validate_pool_write_authority(&runtime, "pool-a", &full)
        .expect_err("sealed pool cannot accept a new object placement");
    assert!(error.to_string().contains("sealed"));
}

#[test]
fn rejects_pool_membership_wider_than_its_static_ec_contract() {
    let pool = ServerPool {
        id: "pool-a".into(),
        generation: 1,
        policy: ClusterPolicy {
            object_data_shards: 2,
            object_parity_shards: 1,
        },
        state: PoolState::Writable,
        node_ids: vec!["a".into(), "b".into(), "c".into(), "d".into()],
    };
    assert!(pool.validate().is_err());
}

#[test]
fn rejects_retired_membership_format() {
    let mut membership = ClusterMembership {
        format_version: N2_MEMBERSHIP_FORMAT_VERSION,
        cluster_id: "cluster-a".into(),
        epoch: 1,
        binding_epoch: 1,
        pools: vec![ServerPool {
            id: "pool-a".into(),
            generation: 1,
            policy: ClusterPolicy {
                object_data_shards: 1,
                object_parity_shards: 1,
            },
            state: PoolState::Writable,
            node_ids: vec!["a".into(), "b".into()],
        }],
        nodes: vec![node("a"), node("b")],
        reserve_nodes: Vec::new(),
        retired_instances: Vec::new(),
    };
    membership.validate().expect("format-v5 membership");
    membership.format_version = 4;
    assert!(membership.validate().is_err());
}

#[test]
fn configured_physical_roots_match_a_promoted_logical_binding() {
    let configured = vec![StorageRoot {
        id: "reserve-device".into(),
        physical_id: None,
        path: "/disk-reserve".into(),
    }];
    let promoted = vec![StorageRoot {
        id: "logical-root-d".into(),
        physical_id: Some("reserve-device".into()),
        path: "/disk-reserve".into(),
    }];
    assert!(physical_root_sets_match(&configured, &promoted));

    let mut wrong_path = promoted.clone();
    wrong_path[0].path = "/other-device".into();
    assert!(!physical_root_sets_match(&configured, &wrong_path));
}

#[test]
fn promoted_reserve_restart_restores_the_logical_slot_roots() {
    let promoted_root = StorageRoot {
        id: "logical-root-a".into(),
        physical_id: Some("reserve-device".into()),
        path: "/disk-reserve".into(),
    };
    let membership = ClusterMembership {
        format_version: N2_MEMBERSHIP_FORMAT_VERSION,
        cluster_id: "cluster-a".into(),
        epoch: 1,
        binding_epoch: 2,
        pools: vec![ServerPool {
            id: "pool-a".into(),
            generation: 1,
            policy: ClusterPolicy {
                object_data_shards: 1,
                object_parity_shards: 1,
            },
            state: PoolState::Writable,
            node_ids: vec!["slot-a".into(), "slot-b".into()],
        }],
        nodes: vec![
            ClusterNode {
                id: "slot-a".into(),
                instance_id: "reserve-instance".into(),
                binding_epoch: 2,
                endpoint: "https://reserve:51001".into(),
                native_endpoint: Some("n2://reserve:51002".into()),
                state: Some(NodeState::Online),
                weight: 1,
                storage_roots: vec![promoted_root.clone()],
                joined_epoch: 1,
                capacity_bytes: None,
                used_bytes: None,
                last_seen_unix: None,
            },
            node("slot-b"),
        ],
        reserve_nodes: Vec::new(),
        retired_instances: Vec::new(),
    };

    let (slot_id, pool_id, is_reserve, roots) =
        resolve_startup_local_binding(&membership, "reserve-instance")
            .expect("promoted reserve binding must resolve");
    assert_eq!(slot_id, "slot-a");
    assert_eq!(pool_id, "pool-a");
    assert!(!is_reserve);
    assert_eq!(roots, vec![promoted_root]);
}

#[test]
fn rejects_node_assigned_to_two_pools() {
    let policy = ClusterPolicy {
        object_data_shards: 1,
        object_parity_shards: 1,
    };
    let membership = ClusterMembership {
        format_version: N2_MEMBERSHIP_FORMAT_VERSION,
        cluster_id: "cluster-a".into(),
        epoch: 2,
        binding_epoch: 1,
        pools: vec![
            ServerPool {
                id: "a".into(),
                generation: 1,
                policy: policy.clone(),
                state: PoolState::Sealed,
                node_ids: vec!["n1".into(), "n2".into()],
            },
            ServerPool {
                id: "b".into(),
                generation: 2,
                policy,
                state: PoolState::Writable,
                node_ids: vec!["n1".into(), "n3".into()],
            },
        ],
        nodes: vec![node("n1"), node("n2"), node("n3")],
        reserve_nodes: Vec::new(),
        retired_instances: Vec::new(),
    };
    assert!(membership.validate().is_err());
}

#[test]
fn concurrent_membership_publishers_use_independent_atomic_files() {
    let root = std::env::temp_dir().join(format!("n2-membership-concurrency-{}", Ulid::new()));
    let path = Arc::new(root.join("n2_membership.json"));
    let policy = ClusterPolicy {
        object_data_shards: 1,
        object_parity_shards: 1,
    };
    let membership = ClusterMembership {
        format_version: N2_MEMBERSHIP_FORMAT_VERSION,
        cluster_id: "concurrent-cluster".into(),
        epoch: 1,
        binding_epoch: 1,
        pools: vec![ServerPool {
            id: "pool-a".into(),
            generation: 1,
            policy,
            state: PoolState::Writable,
            node_ids: vec!["a".into(), "b".into()],
        }],
        nodes: vec![node("a"), node("b")],
        reserve_nodes: Vec::new(),
        retired_instances: Vec::new(),
    };
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let writers = (0..8)
        .map(|_| {
            let path = path.clone();
            let membership = membership.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                write_membership(&path, &membership)
            })
        })
        .collect::<Vec<_>>();

    for writer in writers {
        writer
            .join()
            .expect("membership writer must not panic")
            .expect("concurrent membership publish must succeed");
    }

    let persisted: ClusterMembership =
        serde_json::from_slice(&fs::read(&*path).expect("read published membership"))
            .expect("decode published membership");
    persisted.validate().expect("published membership is valid");
    let leftovers = fs::read_dir(&root)
        .expect("read membership directory")
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name() != "n2_membership.json")
        .count();
    assert_eq!(leftovers, 0, "atomic membership temp files must not remain");
    fs::remove_dir_all(root).expect("remove membership test directory");
}
