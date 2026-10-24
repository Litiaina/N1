use super::*;
use crate::distribution::membership::{
    ClusterMembership, ClusterPolicy, N2_MEMBERSHIP_FORMAT_VERSION, NodeState, PoolState,
    StorageRoot,
};

fn runtime() -> ClusterRuntime {
    let nodes = (0..2)
        .map(|index| ClusterNode {
            id: format!("n{index}"),
            instance_id: format!("instance-{index}"),
            binding_epoch: 1,
            endpoint: format!("http://127.0.0.1:{}", 51000 + index),
            native_endpoint: None,
            state: Some(NodeState::Online),
            weight: 1,
            storage_roots: vec![StorageRoot {
                id: format!("r{index}"),
                physical_id: Some(format!("physical-{index}")),
                path: format!("/lease-test/{index}"),
            }],
            joined_epoch: 1,
            capacity_bytes: None,
            used_bytes: None,
            last_seen_unix: None,
        })
        .collect::<Vec<_>>();
    ClusterRuntime {
        local_node_id: "n0".into(),
        local_instance_id: "instance-0".into(),
        local_is_reserve: false,
        local_pool_id: "pool-a".into(),
        local_storage_roots: nodes[0].storage_roots.clone(),
        membership_path: PathBuf::from("/lease-test/membership"),
        layout_policy: crate::distribution::layout::ClusterLayoutPolicy {
            data_shards: 1,
            parity_shards: 1,
        },
        membership: ClusterMembership {
            format_version: N2_MEMBERSHIP_FORMAT_VERSION,
            cluster_id: "lease-test-cluster".into(),
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
                node_ids: vec!["n0".into(), "n1".into()],
            }],
            nodes,
            reserve_nodes: Vec::new(),
            retired_instances: Vec::new(),
        },
        auto_rebalance: false,
    }
}

fn lease_request(fragment: &str, keys: &[&str]) -> PeerNcnxLeaseRequest {
    PeerNcnxLeaseRequest {
        action: PeerNcnxLeaseAction::Acquire,
        cluster_id: "lease-test-cluster".into(),
        pool_id: "pool-a".into(),
        pool_generation: 1,
        target_slot_id: "n0".into(),
        target_instance_id: "instance-0".into(),
        binding_epoch: 1,
        fragment_id: fragment.into(),
        owner_id: "attempt-a".into(),
        ttl_ms: 100,
        items: keys
            .iter()
            .map(|key| PeerNcnxLeaseKey {
                logical_key: (*key).into(),
                root_id: "r0".into(),
                token: None,
            })
            .collect(),
    }
}

fn existing_request(
    request: &PeerNcnxLeaseRequest,
    response: &PeerNcnxLeaseResponse,
    action: PeerNcnxLeaseAction,
) -> PeerNcnxLeaseRequest {
    let mut followup = request.clone();
    followup.action = action;
    for (item, grant) in followup.items.iter_mut().zip(&response.items) {
        item.token = Some(grant.token.clone());
    }
    followup
}

#[test]
fn expiry_release_restart_and_delayed_acquisition_never_restore_old_tokens() {
    let runtime = runtime();
    let request = lease_request("fragment", &["video/file"]);
    let keys = prepare(&runtime, &request).unwrap();
    let now = Instant::now();
    let mut registry = LeaseRegistry::new(8);
    let first = registry.apply_at(&request, &keys, now).unwrap();
    let old = &first.items[0].token;
    registry
        .validate_at(&keys[0].key, &keys[0].binding, old, now)
        .unwrap();
    let retry = registry
        .apply_at(&request, &keys, now + Duration::from_millis(50))
        .unwrap();
    assert_eq!(retry.items[0].token, *old);
    assert_eq!(
        retry.ttl_ms, 50,
        "acquisition retry cannot extend ownership"
    );
    let late = now + Duration::from_millis(101);
    assert!(
        registry
            .validate_at(&keys[0].key, &keys[0].binding, old, late)
            .is_err()
    );
    let renewal = existing_request(&request, &first, PeerNcnxLeaseAction::Renew);
    assert!(
        registry
            .apply_at(&renewal, &prepare(&runtime, &renewal).unwrap(), late)
            .is_err()
    );
    let successor = registry.apply_at(&request, &keys, late).unwrap();
    assert_ne!(
        successor.items[0].token, *old,
        "delayed acquisition receives a new unpredictable grant"
    );
    let stale_release = existing_request(&request, &first, PeerNcnxLeaseAction::Release);
    assert!(
        registry
            .apply_at(
                &stale_release,
                &prepare(&runtime, &stale_release).unwrap(),
                late
            )
            .is_err()
    );
    registry
        .validate_at(
            &keys[0].key,
            &keys[0].binding,
            &successor.items[0].token,
            late,
        )
        .unwrap();
    let release = existing_request(&request, &successor, PeerNcnxLeaseAction::Release);
    registry
        .apply_at(&release, &prepare(&runtime, &release).unwrap(), late)
        .unwrap();
    assert!(registry.grants.is_empty());
    assert!(
        registry
            .validate_at(
                &keys[0].key,
                &keys[0].binding,
                &successor.items[0].token,
                late
            )
            .is_err()
    );
    let after_release = registry.apply_at(&request, &keys, late).unwrap();
    assert_ne!(after_release.items[0].token, successor.items[0].token);
    let mut restarted = LeaseRegistry::new(8);
    let after_restart = restarted.apply_at(&request, &keys, late).unwrap();
    assert_ne!(after_release.boot_id, after_restart.boot_id);
    assert!(
        restarted
            .validate_at(
                &keys[0].key,
                &keys[0].binding,
                &after_release.items[0].token,
                late
            )
            .is_err()
    );
}

#[test]
fn exact_identity_aliases_forged_tokens_and_batch_validation_fail_closed() {
    let runtime = runtime();
    let request = lease_request("fragment", &["directory//file", "other"]);
    let keys = prepare(&runtime, &request).unwrap();
    assert_eq!(keys[0].key, ("@f_fragment".into(), "directory/file".into()));
    let mut alias = request.clone();
    alias.fragment_id = "@f_fragment".into();
    assert_eq!(prepare(&runtime, &alias).unwrap()[0].key, keys[0].key);
    let mut registry = LeaseRegistry::new(8);
    let now = Instant::now();
    let response = registry.apply_at(&request, &keys, now).unwrap();
    let token = &response.items[0].token;
    assert!(
        registry
            .validate_at(&keys[1].key, &keys[1].binding, token, now)
            .is_err()
    );
    for changed in 0..8 {
        let mut binding = keys[0].binding.clone();
        match changed {
            0 => binding.cluster_id.push('x'),
            1 => binding.pool_id.push('x'),
            2 => binding.pool_generation += 1,
            3 => binding.node_id.push('x'),
            4 => binding.instance_id.push('x'),
            5 => binding.binding_epoch += 1,
            6 => binding.physical_root_id.push('x'),
            _ => binding.root_path.push('x'),
        }
        assert!(
            registry
                .validate_at(&keys[0].key, &binding, token, now)
                .is_err()
        );
    }
    assert!(
        registry
            .validate_at(&keys[0].key, &keys[0].binding, "forged", now)
            .is_err()
    );
    let mut wrong_release = existing_request(&request, &response, PeerNcnxLeaseAction::Release);
    wrong_release.items[1].token = Some("forged".into());
    assert!(
        registry
            .apply_at(
                &wrong_release,
                &prepare(&runtime, &wrong_release).unwrap(),
                now
            )
            .is_err()
    );
    assert_eq!(
        registry.grants.len(),
        2,
        "group validation must finish before any release"
    );
    for changed in 0..9 {
        let mut invalid = request.clone();
        match changed {
            0 => invalid.cluster_id.push('x'),
            1 => invalid.pool_generation += 1,
            2 => invalid.target_slot_id.push('x'),
            3 => invalid.target_instance_id.push('x'),
            4 => invalid.binding_epoch += 1,
            5 => invalid.items[0].root_id.push('x'),
            6 => invalid.items[1].logical_key = "directory/file".into(),
            7 => invalid.items[0].token = Some("client-chosen".into()),
            _ => invalid.ttl_ms = MAX_LEASE_TTL_MS + 1,
        }
        assert!(prepare(&runtime, &invalid).is_err());
    }
    for fragment in [
        "",
        "..",
        "@f_",
        "fragment/nested",
        "fragment\\nested",
        "fragment\0nested",
    ] {
        let mut invalid = request.clone();
        invalid.fragment_id = fragment.into();
        assert!(prepare(&runtime, &invalid).is_err());
    }
    let mut oversized = request.clone();
    oversized.items = (0..=PEER_NCNX_BATCH_MAX)
        .map(|index| PeerNcnxLeaseKey {
            logical_key: index.to_string(),
            root_id: "r0".into(),
            token: None,
        })
        .collect();
    assert!(prepare(&runtime, &oversized).is_err());
}

#[test]
fn grant_capacity_is_bounded_and_expiration_reclaims_slots_without_tasks() {
    let runtime = runtime();
    let request = lease_request("fragment", &["first", "second"]);
    let keys = prepare(&runtime, &request).unwrap();
    let now = Instant::now();
    let mut registry = LeaseRegistry::new(2);
    registry.apply_at(&request, &keys, now).unwrap();
    let other = lease_request("other-fragment", &["third"]);
    let other_keys = prepare(&runtime, &other).unwrap();
    assert!(registry.apply_at(&other, &other_keys, now).is_err());
    assert_eq!(registry.grants.len(), 2);
    registry
        .apply_at(&other, &other_keys, now + Duration::from_millis(101))
        .unwrap();
    assert_eq!(registry.grants.len(), 1);
    assert!(registry.grants.contains_key(&other_keys[0].key));
}

#[tokio::test]
async fn admitted_writer_outlives_expiry_but_successor_waits_and_stale_write_is_fenced() {
    let runtime = Arc::new(runtime());
    let fragment = format!("lease-writer-{}", Ulid::new());
    let mut request = lease_request(&fragment, &["key"]);
    request.ttl_ms = 1000;
    let first = run_local_lease(runtime.clone(), request.clone())
        .await
        .unwrap();
    let old_token = first.items[0].token.clone();
    let admitted = coordination::lock_keys(&fragment, ["key"]).await.unwrap();
    validate_tokens(
        &runtime,
        "pool-a",
        1,
        &fragment,
        [("key", "r0", old_token.as_str())],
    )
    .unwrap();
    // Deterministically advance this grant's deadline rather than sleep the
    // process. The guarded filesystem operation has already been admitted.
    {
        let mut registry = LEASES.lock().unwrap();
        registry
            .grants
            .get_mut(&(physical_fragment_dir_name(&fragment), "key".into()))
            .unwrap()
            .expires = Instant::now();
    }
    request.owner_id = "successor".into();
    request.ttl_ms = 1000;
    let waiting_runtime = runtime.clone();
    let waiting_request = request.clone();
    let waiter =
        tokio::spawn(async move { run_local_lease(waiting_runtime, waiting_request).await });
    tokio::task::yield_now().await;
    assert!(
        !waiter.is_finished(),
        "expiry cannot bypass an admitted old worker's key guard"
    );
    let unrelated = lease_request(&fragment, &["unrelated"]);
    let unrelated_grant = tokio::time::timeout(
        Duration::from_secs(1),
        run_local_lease(runtime.clone(), unrelated.clone()),
    )
    .await
    .unwrap()
    .unwrap();
    drop(admitted);
    let successor = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_ne!(successor.items[0].token, old_token);
    assert!(
        validate_tokens(
            &runtime,
            "pool-a",
            1,
            &fragment,
            [("key", "r0", old_token.as_str())]
        )
        .is_err()
    );
    validate_tokens(
        &runtime,
        "pool-a",
        1,
        &fragment,
        [("key", "r0", successor.items[0].token.as_str())],
    )
    .unwrap();
    assert!(
        validate_tokens(
            &runtime,
            "pool-a",
            2,
            &fragment,
            [("key", "r0", successor.items[0].token.as_str())]
        )
        .is_err()
    );
    run_local_lease(
        runtime.clone(),
        existing_request(&request, &successor, PeerNcnxLeaseAction::Release),
    )
    .await
    .unwrap();
    run_local_lease(
        runtime,
        existing_request(&unrelated, &unrelated_grant, PeerNcnxLeaseAction::Release),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn lease_authentication_and_http_body_budget_fail_closed() {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    let mut headers = HeaderMap::new();
    assert!(matches!(
        authorize_configured_lease_peer(&headers, None),
        Err(AppError::ServiceUnavailable(_))
    ));
    assert!(matches!(
        authorize_configured_lease_peer(&headers, Some("test-secret")),
        Err(AppError::Forbidden(_))
    ));
    headers.insert("x-n2-cluster-secret", "different-secret".parse().unwrap());
    assert!(matches!(
        authorize_configured_lease_peer(&headers, Some("test-secret")),
        Err(AppError::Forbidden(_))
    ));
    headers.insert("x-n2-cluster-secret", "test-secret".parse().unwrap());
    authorize_configured_lease_peer(&headers, Some("test-secret")).unwrap();

    let state = Arc::new(AppState {
        ws_manager: crate::websocket::manager::WsConnectionManager::new(),
    });
    let response = build_lease_peer_routes()
        .with_state(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(PEER_NCNX_LEASE_PATH)
                .header("content-type", "application/json")
                .body(Body::from(vec![b' '; MAX_LEASE_BODY_BYTES + 1]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}
