use super::*;
use crate::distribution::layout::ClusterLayoutPolicy;
use crate::distribution::membership::{
    ClusterMembership, ClusterNode, ClusterPolicy, ClusterRuntime, N2_MEMBERSHIP_FORMAT_VERSION,
    NodeState, PoolState, ServerPool, StorageRoot,
};
use std::path::PathBuf;

fn manifest(
    total_size_bytes: u64,
    expected_parts: u64,
    part_size_bytes: u64,
) -> UploadSessionManifest {
    UploadSessionManifest {
        fragment: "fragment".into(),
        object_key: "video.bin".into(),
        packed_ext_u64: crate::core::index_store::packed_extension_key("video.bin"),
        version_id: "01TESTVERSION000000000000".into(),
        session_id: "01TESTSESSION000000000000".into(),
        total_size_bytes,
        expected_parts,
        part_size_bytes,
        content_type: "application/octet-stream".into(),
        created_at: 0,
        pool_id: "pool-a".into(),
        pool_generation: 1,
        stripe_size_bytes: 256 * 1024,
        membership_epoch: 0,
        data_shards: 1,
        parity_shards: 1,
    }
}

fn standard_test_session() -> Arc<StandardUploadSession> {
    Arc::new(StandardUploadSession {
        manifest: manifest(128, 2, 64),
        logical_path: "videos/video.bin".into(),
        lifecycle: AtomicU8::new(STANDARD_SESSION_OPEN),
        in_flight_parts: AtomicUsize::new(0),
        last_activity_at: AtomicI64::new(0),
        parts: DashMap::new(),
        finalized_metadata: OnceLock::new(),
    })
}

#[test]
fn standard_multipart_claims_distinct_parts_without_a_session_lock() {
    let session = standard_test_session();
    let first = match claim_standard_part(Arc::clone(&session), 0).unwrap() {
        StandardPartClaimResult::Claimed(claim) => claim,
        StandardPartClaimResult::AlreadyComplete => panic!("part was unexpectedly complete"),
    };
    let second = match claim_standard_part(Arc::clone(&session), 1).unwrap() {
        StandardPartClaimResult::Claimed(claim) => claim,
        StandardPartClaimResult::AlreadyComplete => panic!("part was unexpectedly complete"),
    };

    assert_eq!(session.in_flight_parts.load(Ordering::Acquire), 2);
    assert!(claim_standard_part(Arc::clone(&session), 0).is_err());

    drop(first);
    drop(second);
    assert_eq!(session.in_flight_parts.load(Ordering::Acquire), 0);
    assert!(matches!(
        claim_standard_part(session, 0),
        Ok(StandardPartClaimResult::Claimed(_))
    ));
}

#[test]
fn completed_standard_part_retry_is_idempotent() {
    let session = standard_test_session();
    let claim = match claim_standard_part(Arc::clone(&session), 0).unwrap() {
        StandardPartClaimResult::Claimed(claim) => claim,
        StandardPartClaimResult::AlreadyComplete => panic!("part was unexpectedly complete"),
    };
    claim
        .complete(PartMetadata {
            part_index: 0,
            size_bytes: 64,
            created_at: 1,
            shards: Vec::new(),
            checksum: Some("sha256:test".into()),
        })
        .unwrap();

    assert!(matches!(
        claim_standard_part(session, 0),
        Ok(StandardPartClaimResult::AlreadyComplete)
    ));
}

#[test]
fn standard_upload_reuse_requires_the_exact_upload_shape() {
    let session = standard_test_session();
    let exact = InitiateUploadParams {
        total_size_bytes: 128,
        expected_parts: 2,
        part_size_bytes: Some(64),
        content_type: Some("application/octet-stream".into()),
        packed_ext_u64: 0,
    };
    assert!(standard_request_matches(
        &session,
        "videos/video.bin",
        &exact
    ));

    let changed_size = InitiateUploadParams {
        total_size_bytes: 129,
        ..exact.clone()
    };
    assert!(!standard_request_matches(
        &session,
        "videos/video.bin",
        &changed_size
    ));
    assert!(!standard_request_matches(
        &session,
        "videos/other.bin",
        &exact
    ));
}

#[test]
fn finalized_standard_session_is_retained_for_ncnx_retry() {
    let session = standard_test_session();
    session
        .lifecycle
        .store(STANDARD_SESSION_FINALIZED, Ordering::Release);
    session.last_activity_at.store(0, Ordering::Release);
    assert!(!standard_session_is_expired(&session, i64::MAX, 1));
}

#[test]
fn multipart_expected_part_size_uses_manifest_part_size() {
    let m = manifest(1024 * 1024 * 1024, 16, 64 * 1024 * 1024);

    assert_eq!(expected_part_size(&m, 0), 64 * 1024 * 1024);
    assert_eq!(expected_part_size(&m, 14), 64 * 1024 * 1024);
    assert_eq!(expected_part_size(&m, 15), 64 * 1024 * 1024);
}

#[test]
fn multipart_expected_part_size_handles_short_final_part() {
    let m = manifest(130 * 1024 * 1024, 3, 64 * 1024 * 1024);

    assert_eq!(expected_part_size(&m, 0), 64 * 1024 * 1024);
    assert_eq!(expected_part_size(&m, 1), 64 * 1024 * 1024);
    assert_eq!(expected_part_size(&m, 2), 2 * 1024 * 1024);
}

#[test]
fn cluster_one_shot_rejects_large_objects() {
    assert!(validate_cluster_one_shot_size(CLUSTER_ONE_SHOT_MAX_BYTES).is_ok());
    assert!(validate_cluster_one_shot_size(CLUSTER_ONE_SHOT_MAX_BYTES + 1).is_err());
}

fn fixed_runtime() -> ClusterRuntime {
    ClusterRuntime {
        local_node_id: "a".into(),
        local_instance_id: "a".into(),
        local_is_reserve: false,
        local_pool_id: "pool-a".into(),
        local_storage_roots: vec![],
        membership_path: PathBuf::from("membership.json"),
        layout_policy: ClusterLayoutPolicy {
            data_shards: 2,
            parity_shards: 1,
        },
        membership: ClusterMembership {
            format_version: N2_MEMBERSHIP_FORMAT_VERSION,
            cluster_id: "test".into(),
            epoch: 7,
            binding_epoch: 1,
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
                    instance_id: id.into(),
                    binding_epoch: 1,
                    endpoint: format!("https://{id}:50001"),
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
                })
                .collect(),
            reserve_nodes: Vec::new(),
            retired_instances: Vec::new(),
        },
        auto_rebalance: false,
    }
}

#[test]
fn fixed_cluster_admits_full_and_k_threshold_survival_writes() {
    let runtime = fixed_runtime();
    let layout = resolve_survival_write_layout(3, 3, &runtime.layout_policy).unwrap();

    assert_eq!(layout.data_shards, 2);
    assert_eq!(layout.parity_shards, 1);
    assert!(validate_write_admission(&runtime, 3, 3, layout).is_ok());
    assert!(validate_write_admission(&runtime, 2, 2, layout).is_ok());
}

#[test]
fn offline_member_remains_part_of_the_static_layout_but_not_live_admission() {
    let mut runtime = fixed_runtime();
    runtime.membership.nodes[2].state = Some(NodeState::Offline);

    let layout = configured_layout(&runtime).unwrap();
    assert_eq!(layout.data_shards, 2);
    assert_eq!(layout.parity_shards, 1);
    assert_eq!(
        runtime.membership.active_newest_pool_nodes().len(),
        layout.data_shards
    );
    assert!(validate_write_admission(&runtime, 2, 2, layout).is_ok());
}

#[test]
fn fixed_cluster_rejects_a_blob_plane_below_k() {
    let runtime = fixed_runtime();
    assert!(resolve_survival_write_layout(2, 3, &runtime.layout_policy).is_ok());
    assert!(resolve_survival_write_layout(1, 3, &runtime.layout_policy).is_err());
}

#[test]
fn survival_admission_rejects_namespace_reachability_below_k() {
    let runtime = fixed_runtime();
    let layout = resolve_survival_write_layout(3, 3, &runtime.layout_policy).unwrap();
    assert!(validate_write_admission(&runtime, 1, 3, layout).is_err());
}

#[test]
fn pinned_one_plus_one_session_survives_node_return() {
    let layout = ClusterEcLayout {
        data_shards: 1,
        parity_shards: 1,
    };

    assert!(validate_pinned_upload_layout(layout, 2).is_ok());
    assert!(validate_pinned_upload_layout(layout, 3).is_ok());
}

#[test]
fn pinned_layout_validates_immutable_pool_width_not_live_reachability() {
    let layout = ClusterEcLayout {
        data_shards: 2,
        parity_shards: 1,
    };

    assert!(validate_pinned_upload_layout(layout, 2).is_err());
    assert!(validate_pinned_upload_layout(layout, 3).is_ok());
}
