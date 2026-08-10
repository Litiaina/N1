use crate::core::capacity_manager::storage_root_accepts_write;
use crate::core::root_identity::verify_local_root_identity;
use crate::distribution::membership::{ClusterRuntime, StorageRoot};
use crate::enums::app_error_enum::AppError;
use std::path::Path;

/// `root_id` is a deterministic preferred route, not a permanent disk
/// binding. The node is the EC failure domain and may satisfy the operation
/// from any currently enrolled local root.
pub(crate) fn ordered_local_roots(
    runtime: &ClusterRuntime,
    preferred_root_id: &str,
) -> Vec<StorageRoot> {
    let mut roots = runtime.local_storage_roots.clone();
    roots.sort_by(|left, right| {
        let left_preferred = left.id == preferred_root_id;
        let right_preferred = right.id == preferred_root_id;
        right_preferred
            .cmp(&left_preferred)
            .then_with(|| left.id.cmp(&right.id))
    });
    roots
}

pub(crate) fn writable_local_roots(
    runtime: &ClusterRuntime,
    preferred_root_id: &str,
    required_bytes: u64,
) -> Result<Vec<StorageRoot>, AppError> {
    let mut writable = Vec::new();
    let mut failures = Vec::new();
    for root in ordered_local_roots(runtime, preferred_root_id) {
        if let Err(error) = verify_local_root_identity(runtime, &root) {
            failures.push(format!("{}: {}", root.id, error));
            continue;
        }
        match storage_root_accepts_write(Path::new(&root.path), required_bytes) {
            Ok(true) => writable.push(root),
            Ok(false) => failures.push(format!("{}: not write-ready", root.id)),
            Err(error) => failures.push(format!("{}: {}", root.id, error)),
        }
    }
    if writable.is_empty() {
        Err(AppError::InsufficientStorage)
    } else {
        Ok(writable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::root_identity::initialize_local_root_identities;
    use crate::distribution::layout::ClusterLayoutPolicy;
    use crate::distribution::membership::{
        ClusterMembership, ClusterNode, ClusterPolicy, N2_MEMBERSHIP_FORMAT_VERSION, NodeState,
        PoolState, ServerPool,
    };
    use std::path::PathBuf;
    use ulid::Ulid;

    fn runtime(roots: Vec<StorageRoot>) -> ClusterRuntime {
        ClusterRuntime {
            local_node_id: "node-a".into(),
            local_instance_id: "node-a".into(),
            local_is_reserve: false,
            local_pool_id: "pool-a".into(),
            local_storage_roots: roots.clone(),
            membership_path: PathBuf::new(),
            layout_policy: ClusterLayoutPolicy {
                data_shards: 1,
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
                        object_data_shards: 1,
                        object_parity_shards: 1,
                    },
                    state: PoolState::Writable,
                    node_ids: vec!["node-a".into()],
                }],
                nodes: vec![ClusterNode {
                    id: "node-a".into(),
                    instance_id: "node-a".into(),
                    binding_epoch: 1,
                    endpoint: "https://127.0.0.1:1".into(),
                    native_endpoint: None,
                    state: Some(NodeState::Online),
                    weight: 1,
                    storage_roots: roots,
                    joined_epoch: 1,
                    capacity_bytes: None,
                    used_bytes: None,
                    last_seen_unix: None,
                }],
                reserve_nodes: Vec::new(),
                retired_instances: Vec::new(),
            },
            auto_rebalance: false,
        }
    }

    #[test]
    fn preferred_root_is_first_but_an_unknown_preference_keeps_all_roots() {
        let roots = vec![
            StorageRoot {
                id: "root-b".into(),
                physical_id: None,
                path: "/disk-b".into(),
            },
            StorageRoot {
                id: "root-a".into(),
                physical_id: None,
                path: "/disk-a".into(),
            },
        ];
        let runtime = runtime(roots);
        let preferred = ordered_local_roots(&runtime, "root-b");
        assert_eq!(preferred[0].id, "root-b");
        assert_eq!(preferred[1].id, "root-a");

        let replaced = ordered_local_roots(&runtime, "retired-root");
        assert_eq!(replaced.len(), 2);
        assert_eq!(replaced[0].id, "root-a");
        assert_eq!(replaced[1].id, "root-b");
    }

    #[test]
    fn lost_preferred_disk_falls_back_to_another_enrolled_disk() {
        let base = std::env::temp_dir().join(format!("n2-root-routing-{}", Ulid::new()));
        let lost = base.join("lost");
        let healthy = base.join("healthy");
        std::fs::create_dir_all(&lost).expect("create preferred root");
        std::fs::create_dir_all(&healthy).expect("create healthy root");
        let runtime = runtime(vec![
            StorageRoot {
                id: "root-lost".into(),
                physical_id: None,
                path: lost.display().to_string(),
            },
            StorageRoot {
                id: "root-healthy".into(),
                physical_id: None,
                path: healthy.display().to_string(),
            },
        ]);
        initialize_local_root_identities(&runtime).expect("enroll roots");
        std::fs::remove_dir_all(&lost).expect("remove preferred root");

        let candidates =
            writable_local_roots(&runtime, "root-lost", 4096).expect("fallback candidates");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].id, "root-healthy");

        let _ = std::fs::remove_dir_all(base);
    }
}
