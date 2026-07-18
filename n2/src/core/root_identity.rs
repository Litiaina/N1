use crate::distribution::membership::{ClusterRuntime, NodeRole, StorageRoot};
use crate::enums::app_error_enum::AppError;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

pub const ROOT_IDENTITY_FILE: &str = ".n2-root.ncdx";
const ROOT_IDENTITY_FORMAT: &str = "n2-root-v2";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RootIdentity {
    pub format: String,
    pub cluster_id: String,
    pub node_id: String,
    pub node_role: NodeRole,
    pub root_id: String,
    /// Immutable namespace durability contract for this cluster generation.
    pub namespace_redundancy: usize,
}

impl RootIdentity {
    fn expected(runtime: &ClusterRuntime, root: &StorageRoot) -> Self {
        Self {
            format: ROOT_IDENTITY_FORMAT.to_string(),
            cluster_id: runtime.membership.cluster_id.clone(),
            node_id: runtime.local_node_id.clone(),
            node_role: runtime.local_role,
            root_id: root.id.clone(),
            namespace_redundancy: runtime.membership.policy.namespace_redundancy,
        }
    }
}

fn identity_path(root: &StorageRoot) -> PathBuf {
    Path::new(&root.path).join(ROOT_IDENTITY_FILE)
}

fn read_identity(path: &Path) -> Result<RootIdentity, AppError> {
    let bytes = fs::read(path).map_err(|error| {
        AppError::Io(error, format!("read N2 root identity {}", path.display()))
    })?;
    let identity: RootIdentity = rmp_serde::from_slice(&bytes).map_err(|error| {
        AppError::InternalServerError(format!(
            "decode N2 root identity '{}': {error}",
            path.display()
        ))
    })?;
    if identity.format != ROOT_IDENTITY_FORMAT {
        return Err(AppError::Conflict(format!(
            "root '{}' uses unsupported identity format '{}'",
            path.display(),
            identity.format
        )));
    }
    Ok(identity)
}

fn root_is_empty(root: &Path) -> Result<bool, AppError> {
    let mut entries = fs::read_dir(root)
        .map_err(|error| AppError::Io(error, format!("inspect N2 root {}", root.display())))?;
    Ok(entries.next().is_none())
}

fn sync_directory(path: &Path) -> Result<(), AppError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| AppError::Io(error, format!("sync N2 root {}", path.display())))
}

fn enroll_root(runtime: &ClusterRuntime, root: &StorageRoot) -> Result<(), AppError> {
    root.validate()?;
    let root_path = Path::new(&root.path);
    let metadata = fs::metadata(root_path).map_err(|error| {
        AppError::Io(
            error,
            format!("inspect configured N2 root {}", root_path.display()),
        )
    })?;
    if !metadata.is_dir() {
        return Err(AppError::BadRequest(format!(
            "configured N2 root '{}' is not a directory",
            root_path.display()
        )));
    }

    let marker_path = identity_path(root);
    let expected = RootIdentity::expected(runtime, root);
    match read_identity(&marker_path) {
        Ok(actual) if actual == expected => return Ok(()),
        Ok(actual) => {
            return Err(AppError::Conflict(format!(
                "root '{}' belongs to cluster='{}' node='{}' role='{}' root_id='{}', not this logical node",
                root_path.display(),
                actual.cluster_id,
                actual.node_id,
                actual.node_role,
                actual.root_id
            )));
        }
        Err(AppError::Io(error, _)) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    if !root_is_empty(root_path)? {
        return Err(AppError::Conflict(format!(
            "unenrolled N2 2.0 root '{}' is not empty; use a new isolated directory",
            root_path.display()
        )));
    }

    let encoded = rmp_serde::to_vec_named(&expected).map_err(|error| {
        AppError::InternalServerError(format!("encode N2 root identity: {error}"))
    })?;
    match OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&marker_path)
    {
        Ok(mut file) => {
            file.write_all(&encoded).map_err(|error| {
                AppError::Io(
                    error,
                    format!("write N2 root identity {}", marker_path.display()),
                )
            })?;
            file.sync_all().map_err(|error| {
                AppError::Io(
                    error,
                    format!("sync N2 root identity {}", marker_path.display()),
                )
            })?;
            sync_directory(root_path)
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            let actual = read_identity(&marker_path)?;
            if actual == expected {
                Ok(())
            } else {
                Err(AppError::Conflict(format!(
                    "root '{}' was concurrently enrolled by another logical node",
                    root_path.display()
                )))
            }
        }
        Err(error) => Err(AppError::Io(
            error,
            format!("create N2 root identity {}", marker_path.display()),
        )),
    }
}

pub fn initialize_local_root_identities(runtime: &ClusterRuntime) -> Result<(), AppError> {
    for root in &runtime.local_storage_roots {
        enroll_root(runtime, root)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::layout::{ClusterLayoutMode, ClusterLayoutPolicy};
    use crate::distribution::membership::{
        ClusterMembership, ClusterNode, ClusterPolicy, N2_MEMBERSHIP_FORMAT_VERSION,
        NamespaceCatalogLayout, NamespaceCatalogReplica, NodeState,
    };
    use ulid::Ulid;

    fn runtime(root: &Path, node_id: &str) -> ClusterRuntime {
        let storage_root = StorageRoot {
            id: "ns-root-1".into(),
            path: root.display().to_string(),
        };
        let policy = ClusterPolicy {
            namespace_redundancy: 2,
            object_data_shards: 1,
            object_parity_shards: 1,
        };
        ClusterRuntime {
            local_node_id: node_id.into(),
            local_role: NodeRole::Namespace,
            local_storage_roots: vec![storage_root.clone()],
            membership_path: PathBuf::new(),
            layout_policy: ClusterLayoutPolicy {
                mode: ClusterLayoutMode::Fixed,
                fixed_data_shards: Some(1),
                fixed_parity_shards: Some(1),
                auto_max_data_shards: 8,
                auto_max_parity_shards: 3,
            },
            membership: ClusterMembership {
                format_version: N2_MEMBERSHIP_FORMAT_VERSION,
                cluster_id: "cluster-test".into(),
                epoch: 1,
                policy,
                namespace_catalog: NamespaceCatalogLayout {
                    generation: 1,
                    replicas: vec![NamespaceCatalogReplica {
                        node_id: node_id.into(),
                        root_id: "ns-root-1".into(),
                    }],
                },
                nodes: vec![ClusterNode {
                    id: node_id.into(),
                    role: NodeRole::Namespace,
                    endpoint: "https://127.0.0.1:1".into(),
                    native_endpoint: None,
                    state: Some(NodeState::Online),
                    weight: 1,
                    storage_roots: vec![storage_root],
                    joined_epoch: 1,
                    capacity_bytes: None,
                    used_bytes: None,
                    last_seen_unix: None,
                }],
            },
            auto_rebalance: false,
        }
    }

    #[test]
    fn root_identity_is_stable_and_rejects_rebinding() {
        let root = std::env::temp_dir().join(format!("n2-root-v2-{}", Ulid::new()));
        fs::create_dir_all(&root).expect("create root");
        let first = runtime(&root, "namespace-a");
        initialize_local_root_identities(&first).expect("enroll root");
        initialize_local_root_identities(&first).expect("repeat enrollment");

        let second = runtime(&root, "namespace-b");
        assert!(initialize_local_root_identities(&second).is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn non_empty_unenrolled_root_is_rejected() {
        let root = std::env::temp_dir().join(format!("n2-root-v2-dirty-{}", Ulid::new()));
        fs::create_dir_all(&root).expect("create root");
        fs::write(root.join("existing"), b"data").expect("seed root");
        assert!(initialize_local_root_identities(&runtime(&root, "namespace-a")).is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn enrolled_root_rejects_namespace_redundancy_change() {
        let root = std::env::temp_dir().join(format!("n2-root-v2-policy-{}", Ulid::new()));
        fs::create_dir_all(&root).expect("create root");
        let first = runtime(&root, "namespace-a");
        initialize_local_root_identities(&first).expect("enroll root");

        let mut changed = runtime(&root, "namespace-a");
        changed.membership.policy.namespace_redundancy = 3;
        assert!(initialize_local_root_identities(&changed).is_err());
        let _ = fs::remove_dir_all(root);
    }
}
