use super::*;
use crate::distribution::membership::{
    ClusterMembership, ClusterPolicy, N2_MEMBERSHIP_FORMAT_VERSION, NodeState, PoolState,
    StorageRoot,
};
use ulid::Ulid;

fn runtime() -> ClusterRuntime {
    let policy = ClusterPolicy {
        object_data_shards: 4,
        object_parity_shards: 2,
    };
    let nodes = (0..6)
        .map(|index| ClusterNode {
            id: format!("n{index}"),
            instance_id: format!("n{index}"),
            binding_epoch: 1,
            endpoint: format!("https://n{index}:50001"),
            native_endpoint: None,
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
        membership_path: PathBuf::from("/membership"),
        layout_policy: crate::distribution::layout::ClusterLayoutPolicy {
            data_shards: 4,
            parity_shards: 2,
        },
        membership: ClusterMembership {
            format_version: N2_MEMBERSHIP_FORMAT_VERSION,
            cluster_id: "cluster-a".into(),
            epoch: 1,
            binding_epoch: 1,
            pools: vec![ServerPool {
                id: "pool-a".into(),
                generation: 1,
                policy,
                state: PoolState::Writable,
                node_ids: (0..6).map(|index| format!("n{index}")).collect(),
            }],
            nodes,
            reserve_nodes: Vec::new(),
            retired_instances: Vec::new(),
        },
        auto_rebalance: false,
    }
}

async fn own_local_request(runtime: &ClusterRuntime, request: &mut PeerNcnxRequest) {
    let grant = lease::run_local_lease(
        Arc::new(runtime.clone()),
        lease::PeerNcnxLeaseRequest {
            action: lease::PeerNcnxLeaseAction::Acquire,
            cluster_id: runtime.membership.cluster_id.clone(),
            pool_id: request.pool_id.clone(),
            pool_generation: request.pool_generation,
            target_slot_id: request.target_slot_id.clone(),
            target_instance_id: request.target_instance_id.clone(),
            binding_epoch: request.binding_epoch,
            fragment_id: request.fragment_id.clone(),
            owner_id: Ulid::new().to_string(),
            ttl_ms: lease::MAX_LEASE_TTL_MS,
            items: vec![lease::PeerNcnxLeaseKey {
                logical_key: request.logical_key.clone(),
                root_id: request.root_id.clone(),
                token: None,
            }],
        },
    )
    .await
    .unwrap();
    request.lease_token = Some(grant.items[0].token.clone());
}

#[test]
fn four_plus_two_derives_three_distinct_ncnx_holders() {
    let runtime = runtime();
    let pool = runtime.membership.pool("pool-a").unwrap();
    let layout = build_ncnx_layout(&runtime, pool, "fragment", "videos/a.mp4").unwrap();
    assert_eq!(layout.replicas.len(), 3);
    assert_eq!(
        layout
            .replicas
            .iter()
            .map(|replica| &replica.node_id)
            .collect::<BTreeSet<_>>()
            .len(),
        3
    );
}

#[test]
fn m_plus_one_ncnx_layout_intersects_every_k_survivor_set() {
    let runtime = runtime();
    let pool = runtime.membership.pool("pool-a").unwrap();
    let layout = build_ncnx_layout(&runtime, pool, "fragment", "videos/a.mp4").unwrap();
    let holders = layout
        .replicas
        .iter()
        .map(|replica| replica.node_id.as_str())
        .collect::<BTreeSet<_>>();

    // EC 4+2 permits any two failed pool nodes. R=M+1=3 guarantees that
    // no such failure set can remove every namespace pointer.
    for left in 0..6 {
        for right in (left + 1)..6 {
            let failed = [format!("n{left}"), format!("n{right}")];
            assert!(
                holders
                    .iter()
                    .any(|holder| !failed.iter().any(|node| node == *holder)),
                "all NCNX holders were lost for failures {failed:?}"
            );
        }
    }
}

#[test]
fn ncnx_placement_is_stable_and_key_specific() {
    let runtime = runtime();
    let pool = runtime.membership.pool("pool-a").unwrap();
    let first = build_ncnx_layout(&runtime, pool, "fragment", "videos/a.mp4").unwrap();
    let retry = build_ncnx_layout(&runtime, pool, "fragment", "videos/a.mp4").unwrap();
    let other = build_ncnx_layout(&runtime, pool, "fragment", "videos/b.mp4").unwrap();
    assert_eq!(first, retry);
    assert_eq!(
        first,
        build_ncnx_layout(&runtime, pool, "@f_fragment", "videos//a.mp4").unwrap()
    );
    assert_ne!(first.replicas, other.replicas);
}

#[test]
fn trash_expiration_matches_gc_boundary() {
    let locator = TrashLocator {
        logical_key: "videos/a.mp4".into(),
        deleted_at_unix_nanos: 100 * 1_000_000_000,
        delete_mutation_id: ulid::Ulid::new().to_string(),
    };
    assert!(!trash_is_expired_at(&locator, 109, 10));
    assert!(trash_is_expired_at(&locator, 110, 10));
    assert!(!trash_is_expired_at(&locator, 99, 10));
}

#[test]
fn local_canonical_listing_refills_past_noncanonical_physical_names() {
    let root = std::env::temp_dir().join(format!("n2-ncnx-refill-{}", Ulid::new()));
    std::fs::create_dir_all(&root).expect("create NCNX test root");
    let mut runtime = runtime();
    runtime.local_storage_roots[0].path = root.to_string_lossy().to_string();
    runtime.membership.nodes[0].storage_roots[0].path = root.to_string_lossy().to_string();
    let pool = runtime.membership.pool("pool-a").unwrap();

    let mut first_noncanonical = None;
    let mut later_canonical = None;
    for index in 0..20_000 {
        let key = format!("key-{index:05}");
        let layout = build_ncnx_layout(&runtime, pool, "fragment", &key).unwrap();
        let stored_here = layout
            .replicas
            .iter()
            .any(|replica| replica.node_id == "n0" && replica.root_id == "r0");
        if !stored_here {
            continue;
        }
        if first_noncanonical.is_none() && layout.canonical().unwrap().node_id != "n0" {
            first_noncanonical = Some(key);
        } else if first_noncanonical.is_some() && layout.canonical().unwrap().node_id == "n0" {
            later_canonical = Some(key);
            break;
        }
    }
    let first_noncanonical = first_noncanonical.expect("find noncanonical key");
    let later_canonical = later_canonical.expect("find later canonical key");
    assert!(first_noncanonical < later_canonical);

    let store = NcnxStore::new("r0", &root);
    for (index, key) in [&first_noncanonical, &later_canonical]
        .into_iter()
        .enumerate()
    {
        let record = NcnxRecord::new(
            "fragment",
            format!("object-{index}"),
            Ulid::new().to_string(),
            1,
        )
        .unwrap();
        store
            .create_live("fragment", key, &record.encode().unwrap())
            .unwrap();
    }
    let request = PeerNcnxRequest {
        action: PeerNcnxAction::ListLive,
        pool_id: "pool-a".into(),
        pool_generation: 1,
        target_slot_id: "n0".into(),
        target_instance_id: "n0".into(),
        binding_epoch: 1,
        fragment_id: "fragment".into(),
        logical_key: String::new(),
        root_id: String::new(),
        trash: None,
        bytes: None,
        after: None,
        limit: Some(1),
        lease_token: None,
    };
    let page = local_list(&runtime, &request).expect("list canonical page");
    assert_eq!(
        page.names,
        vec![PeerNcnxName {
            logical_key: later_canonical,
            trash: None,
        }]
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn replica_listing_tolerates_only_the_configured_namespace_failure_budget() {
    let name = PeerNcnxName {
        logical_key: "videos/a.mp4".into(),
        trash: None,
    };
    let pages = vec![
        NodeListPage {
            pool_id: "pool-a".into(),
            node_id: "n0".into(),
            allowed_failures: 2,
            result: Ok(vec![name.clone()]),
        },
        NodeListPage {
            pool_id: "pool-a".into(),
            node_id: "n1".into(),
            allowed_failures: 2,
            result: Ok(vec![name.clone()]),
        },
        NodeListPage {
            pool_id: "pool-a".into(),
            node_id: "n2".into(),
            allowed_failures: 2,
            result: Err(AppError::ServiceUnavailable("offline".into())),
        },
        NodeListPage {
            pool_id: "pool-a".into(),
            node_id: "n3".into(),
            allowed_failures: 2,
            result: Err(AppError::ServiceUnavailable("offline".into())),
        },
    ];
    // The same NCNX filename from two physical replicas is one logical
    // LIST result, and two failed pool nodes remain within M=2.
    assert_eq!(merge_node_pages(pages, 100, true).unwrap(), vec![name]);

    let pages = (0..3)
        .map(|index| NodeListPage {
            pool_id: "pool-a".into(),
            node_id: format!("n{index}"),
            allowed_failures: 2,
            result: Err(AppError::ServiceUnavailable("offline".into())),
        })
        .collect();
    assert!(merge_node_pages(pages, 100, true).is_err());
}

#[tokio::test]
async fn one_failed_local_disk_does_not_turn_the_node_into_a_failed_ncnx_replica() {
    let base = std::env::temp_dir().join(format!("n2-ncnx-local-disk-loss-{}", Ulid::new()));
    let failed = base.join("failed-root");
    let healthy = base.join("healthy-root");
    std::fs::create_dir_all(&base).expect("create test base");
    std::fs::write(&failed, b"not a directory").expect("create unusable root");
    std::fs::create_dir_all(&healthy).expect("create healthy root");

    let roots = vec![
        StorageRoot {
            id: "r0-failed".into(),
            physical_id: None,
            path: failed.display().to_string(),
        },
        StorageRoot {
            id: "r0-healthy".into(),
            physical_id: None,
            path: healthy.display().to_string(),
        },
    ];
    let mut runtime = runtime();
    runtime.local_storage_roots = roots.clone();
    runtime.membership.nodes[0].storage_roots = roots;
    let pool = runtime.membership.pool("pool-a").unwrap();
    let (logical_key, layout) = (0..20_000)
        .find_map(|index| {
            let logical_key = format!("disk-loss/key-{index}");
            let layout = build_ncnx_layout(&runtime, pool, "fragment", &logical_key).ok()?;
            layout
                .replicas
                .iter()
                .any(|replica| replica.node_id == runtime.local_node_id)
                .then_some((logical_key, layout))
        })
        .expect("find a key assigned to the local NCNX replica");
    let replica = layout
        .replicas
        .iter()
        .find(|replica| replica.node_id == runtime.local_node_id)
        .unwrap();
    let request = request_for_replica(
        PeerNcnxAction::ReadLive,
        &layout,
        replica,
        "fragment",
        &logical_key,
        None,
        None,
    );

    let response = run_local_apply(Arc::new(runtime.clone()), request.clone())
        .await
        .expect("the healthy disk proves this node's NCNX absence");
    assert!(response.bytes.is_none());

    runtime.local_storage_roots[1].path = failed.display().to_string();
    runtime.membership.nodes[0].storage_roots[1].path = failed.display().to_string();
    assert!(run_local_apply(Arc::new(runtime), request).await.is_err());

    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test]
async fn delayed_peer_repair_does_not_overwrite_batched_recreated_generation() {
    let root = std::env::temp_dir().join(format!("n2-stale-peer-repair-{}", Ulid::new()));
    std::fs::create_dir_all(&root).unwrap();
    let mut runtime = runtime();
    runtime.local_storage_roots[0].path = root.display().to_string();
    runtime.membership.nodes[0].storage_roots[0].path = root.display().to_string();
    crate::core::root_identity::initialize_local_root_identities(&runtime).unwrap();
    let pool = runtime.membership.pool("pool-a").unwrap();
    let (key, layout) = (0..20_000)
        .find_map(|index| {
            let key = format!("stale/key-{index:05}");
            let layout = build_ncnx_layout(&runtime, pool, "fragment", &key).ok()?;
            layout
                .replicas
                .iter()
                .any(|replica| replica.node_id == "n0")
                .then_some((key, layout))
        })
        .unwrap();
    let replica = layout
        .replicas
        .iter()
        .find(|replica| replica.node_id == "n0")
        .unwrap();
    let old = NcnxRecord::new("fragment", "object-a", Ulid::new().to_string(), 1)
        .unwrap()
        .encode()
        .unwrap();
    let new = NcnxRecord::new("fragment", "object-b", Ulid::new().to_string(), 2)
        .unwrap()
        .encode()
        .unwrap();
    let mut request = request_for_replica(
        PeerNcnxAction::CreateLive,
        &layout,
        replica,
        "fragment",
        &key,
        None,
        Some(&old),
    );
    own_local_request(&runtime, &mut request).await;
    run_local_apply(Arc::new(runtime.clone()), request.clone())
        .await
        .unwrap();
    let mut batch = PeerNcnxBatchRequest {
        action: PeerNcnxBatchAction::RemoveLive,
        pool_id: request.pool_id.clone(),
        pool_generation: request.pool_generation,
        target_slot_id: request.target_slot_id.clone(),
        target_instance_id: request.target_instance_id.clone(),
        binding_epoch: request.binding_epoch,
        fragment_id: request.fragment_id.clone(),
        root_id: request.root_id.clone(),
        items: vec![PeerNcnxBatchItem {
            logical_key: key.clone(),
            trash: None,
            bytes: ByteBuf::from(old.clone()),
            lease_token: request.lease_token.clone(),
        }],
    };
    let mut duplicate = batch.clone();
    let mut alias = duplicate.items[0].clone();
    alias.logical_key = key.replace('/', "//");
    duplicate.items.push(alias);
    assert!(matches!(
        run_local_batch(Arc::new(runtime.clone()), duplicate).await,
        Err(AppError::Conflict(_))
    ));
    let mut unchanged = request.clone();
    unchanged.action = PeerNcnxAction::ReadLive;
    assert_eq!(
        run_local_apply(Arc::new(runtime.clone()), unchanged)
            .await
            .unwrap()
            .bytes
            .unwrap()
            .as_ref(),
        old.as_slice()
    );
    assert_eq!(
        run_local_batch(Arc::new(runtime.clone()), batch.clone())
            .await
            .unwrap()
            .changed,
        1
    );
    batch.action = PeerNcnxBatchAction::CreateLive;
    batch.items[0].bytes = ByteBuf::from(new.clone());
    assert_eq!(
        run_local_batch(Arc::new(runtime.clone()), batch)
            .await
            .unwrap()
            .changed,
        1
    );

    let held = coordination::lock_keys("fragment", [key.as_str()])
        .await
        .unwrap();
    let mut stale = request.clone();
    stale.action = PeerNcnxAction::RepairLive;
    let repair_runtime = Arc::new(runtime.clone());
    let waiting = tokio::spawn(async move { run_local_apply(repair_runtime, stale).await });
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());
    drop(held);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .unwrap()
            .unwrap(),
        Err(AppError::Conflict(_))
    ));
    let mut read = request;
    read.action = PeerNcnxAction::ReadLive;
    read.bytes = None;
    assert_eq!(
        run_local_apply(Arc::new(runtime), read)
            .await
            .unwrap()
            .bytes
            .unwrap()
            .into_vec(),
        new
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn repair_checks_valid_generations_on_every_local_root_before_fallback_publication() {
    let base = std::env::temp_dir().join(format!("n2-repair-fallback-{}", Ulid::new()));
    let mut runtime = runtime();
    let roots = ["root-a", "root-b"]
        .map(|id| {
            let path = base.join(id);
            std::fs::create_dir_all(&path).unwrap();
            StorageRoot {
                id: id.into(),
                physical_id: None,
                path: path.display().to_string(),
            }
        })
        .to_vec();
    runtime.local_storage_roots = roots.clone();
    runtime.membership.nodes[0].storage_roots = roots;
    crate::core::root_identity::initialize_local_root_identities(&runtime).unwrap();
    let pool = runtime.membership.pool("pool-a").unwrap();
    let (key, layout) = (0..20_000)
        .find_map(|index| {
            let key = format!("fallback/key-{index}");
            let layout = build_ncnx_layout(&runtime, pool, "fragment", &key).ok()?;
            layout
                .replicas
                .iter()
                .any(|replica| replica.node_id == "n0")
                .then_some((key, layout))
        })
        .unwrap();
    let replica = layout
        .replicas
        .iter()
        .find(|replica| replica.node_id == "n0")
        .unwrap();
    let preferred = runtime
        .local_storage_roots
        .iter()
        .find(|root| root.id == replica.root_id)
        .unwrap();
    let fallback = runtime
        .local_storage_roots
        .iter()
        .find(|root| root.id != replica.root_id)
        .unwrap();
    let preferred_store = NcnxStore::new(&preferred.id, PathBuf::from(&preferred.path));
    let fallback_store = NcnxStore::new(&fallback.id, PathBuf::from(&fallback.path));
    let old = NcnxRecord::new("fragment", "object-a", Ulid::new().to_string(), 1)
        .unwrap()
        .encode()
        .unwrap();
    let new = NcnxRecord::new("fragment", "object-b", Ulid::new().to_string(), 2)
        .unwrap()
        .encode()
        .unwrap();
    fallback_store.create_live("fragment", &key, &new).unwrap();
    let mut request = request_for_replica(
        PeerNcnxAction::RepairLive,
        &layout,
        replica,
        "fragment",
        &key,
        None,
        Some(&old),
    );
    own_local_request(&runtime, &mut request).await;
    assert!(matches!(
        run_local_apply(Arc::new(runtime.clone()), request.clone()).await,
        Err(AppError::Conflict(_))
    ));
    assert!(
        preferred_store
            .read_live("fragment", &key)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        fallback_store
            .read_live("fragment", &key)
            .unwrap()
            .unwrap()
            .1,
        new
    );
    request.bytes = Some(ByteBuf::from(new.clone()));
    let response = run_local_apply(Arc::new(runtime.clone()), request.clone())
        .await
        .unwrap();
    assert!(!response.changed);
    assert_eq!(response.root_id, fallback.id);
    assert!(
        preferred_store
            .read_live("fragment", &key)
            .unwrap()
            .is_none()
    );
    // An inaccessible root is not evidence of absence. Hide the valid newer
    // generation behind a non-directory component without changing its bytes.
    let retained_root = base.join("retained-fallback");
    std::fs::rename(&fallback.path, &retained_root).unwrap();
    std::fs::write(&fallback.path, b"temporarily inaccessible storage root").unwrap();
    request.bytes = Some(ByteBuf::from(old));
    assert!(matches!(
        run_local_apply(Arc::new(runtime.clone()), request.clone()).await,
        Err(AppError::Io(_, _))
    ));
    assert!(
        preferred_store
            .read_live("fragment", &key)
            .unwrap()
            .is_none()
    );
    request.action = PeerNcnxAction::RepairTrash;
    request.trash = Some(TrashLocator {
        logical_key: key.clone(),
        deleted_at_unix_nanos: 100,
        delete_mutation_id: Ulid::new().to_string(),
    });
    assert!(matches!(
        run_local_apply(Arc::new(runtime.clone()), request.clone()).await,
        Err(AppError::Io(_, _))
    ));
    request.action = PeerNcnxAction::RemoveLive;
    request.trash = None;
    request.bytes = Some(ByteBuf::from(new.clone()));
    assert!(
        matches!(
            run_local_apply(Arc::new(runtime.clone()), request.clone()).await,
            Err(AppError::ServiceUnavailable(_))
        ),
        "one readable root cannot certify removal from an inaccessible fallback"
    );
    let batch = PeerNcnxBatchRequest {
        action: PeerNcnxBatchAction::RemoveLive,
        pool_id: request.pool_id.clone(),
        pool_generation: request.pool_generation,
        target_slot_id: request.target_slot_id.clone(),
        target_instance_id: request.target_instance_id.clone(),
        binding_epoch: request.binding_epoch,
        fragment_id: request.fragment_id.clone(),
        root_id: request.root_id.clone(),
        items: vec![PeerNcnxBatchItem {
            logical_key: key.clone(),
            trash: None,
            bytes: ByteBuf::from(new.clone()),
            lease_token: request.lease_token.clone(),
        }],
    };
    assert!(matches!(
        run_local_batch(Arc::new(runtime.clone()), batch).await,
        Err(AppError::ServiceUnavailable(_))
    ));
    assert_eq!(
        NcnxStore::new(&fallback.id, &retained_root)
            .read_live("fragment", &key)
            .unwrap()
            .unwrap()
            .1,
        new
    );
    let _ = std::fs::remove_dir_all(base);
}
