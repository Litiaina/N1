use crate::distribution::membership::{ClusterRuntime, StorageRoot};
use crate::enums::app_error_enum::AppError;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

pub const ROOT_IDENTITY_FILE: &str = ".n2-root.ncdx";
const ROOT_IDENTITY_FORMAT: &str = "n2-root-v5";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RootIdentity {
    pub format: String,
    pub cluster_id: String,
    pub instance_id: String,
    pub physical_root_id: String,
    /// Informational startup snapshot retained in the v5 marker. Root
    /// ownership is independent, so adding/removing another disk does not
    /// invalidate this device.
    pub instance_root_ids: Vec<String>,
}

impl RootIdentity {
    fn expected(runtime: &ClusterRuntime, root: &StorageRoot) -> Self {
        let mut instance_root_ids = runtime
            .local_storage_roots
            .iter()
            .map(|candidate| candidate.physical_id().to_string())
            .collect::<Vec<_>>();
        instance_root_ids.sort();
        Self {
            format: ROOT_IDENTITY_FORMAT.to_string(),
            cluster_id: runtime.membership.cluster_id.clone(),
            instance_id: runtime.local_instance_id.clone(),
            physical_root_id: root.physical_id().to_string(),
            instance_root_ids,
        }
    }

    fn belongs_to(&self, runtime: &ClusterRuntime, root: &StorageRoot) -> bool {
        self.format == ROOT_IDENTITY_FORMAT
            && self.cluster_id == runtime.membership.cluster_id
            && self.instance_id == runtime.local_instance_id
            && self.physical_root_id == root.physical_id()
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
        Ok(actual) if actual.belongs_to(runtime, root) => return Ok(()),
        Ok(actual) => {
            return Err(AppError::Conflict(format!(
                "root '{}' belongs to cluster='{}' instance='{}' physical_root='{}', not this physical instance",
                root_path.display(),
                actual.cluster_id,
                actual.instance_id,
                actual.physical_root_id
            )));
        }
        Err(AppError::Io(error, _)) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    if !root_is_empty(root_path)? {
        return Err(AppError::Conflict(format!(
            "unenrolled N2 3.4 root '{}' is not empty; use a new isolated directory",
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
            if actual.belongs_to(runtime, root) {
                Ok(())
            } else {
                Err(AppError::Conflict(format!(
                    "root '{}' was concurrently enrolled by another physical instance",
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

pub(crate) fn verify_local_root_identity(
    runtime: &ClusterRuntime,
    root: &StorageRoot,
) -> Result<(), AppError> {
    let marker_path = identity_path(root);
    let actual = read_identity(&marker_path)?;
    if actual.belongs_to(runtime, root) {
        Ok(())
    } else {
        Err(AppError::Conflict(format!(
            "storage root '{}' is not enrolled to cluster '{}' instance '{}'",
            root.path, runtime.membership.cluster_id, runtime.local_instance_id
        )))
    }
}

pub fn initialize_local_root_identities(runtime: &ClusterRuntime) -> Result<(), AppError> {
    let mut available = 0usize;
    let mut unavailable = Vec::new();
    for root in &runtime.local_storage_roots {
        match fs::metadata(&root.path) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => {
                unavailable.push(format!("{}: path is not a directory", root.id));
                continue;
            }
            Err(error) => {
                unavailable.push(format!("{}: {error}", root.id));
                continue;
            }
        }
        enroll_root(runtime, root)?;
        available = available.saturating_add(1);
    }
    if available == 0 {
        return Err(AppError::ServiceUnavailable(format!(
            "no local N2 storage root is available: {}",
            unavailable.join("; ")
        )));
    }
    for issue in unavailable {
        tracing::warn!("N2 root identity unavailable during degraded startup: {issue}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::layout::ClusterLayoutPolicy;
    use crate::distribution::membership::{
        ClusterMembership, ClusterNode, ClusterPolicy, N2_MEMBERSHIP_FORMAT_VERSION, NodeState,
        PoolState, ServerPool,
    };
    use ulid::Ulid;

    fn runtime(root: &Path, node_id: &str) -> ClusterRuntime {
        let storage_root = StorageRoot {
            id: "ns-root-1".into(),
            physical_id: None,
            path: root.display().to_string(),
        };
        let policy = ClusterPolicy {
            object_data_shards: 1,
            object_parity_shards: 1,
        };
        ClusterRuntime {
            local_node_id: node_id.into(),
            local_instance_id: node_id.into(),
            local_is_reserve: false,
            local_pool_id: "pool-test".into(),
            local_storage_roots: vec![storage_root.clone()],
            membership_path: PathBuf::new(),
            layout_policy: ClusterLayoutPolicy {
                data_shards: 1,
                parity_shards: 1,
            },
            membership: ClusterMembership {
                format_version: N2_MEMBERSHIP_FORMAT_VERSION,
                cluster_id: "cluster-test".into(),
                epoch: 1,
                binding_epoch: 1,
                pools: vec![ServerPool {
                    id: "pool-test".into(),
                    generation: 1,
                    policy,
                    state: PoolState::Writable,
                    node_ids: vec![node_id.into()],
                }],
                nodes: vec![ClusterNode {
                    id: node_id.into(),
                    instance_id: node_id.into(),
                    binding_epoch: 1,
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
                reserve_nodes: Vec::new(),
                retired_instances: Vec::new(),
            },
            auto_rebalance: false,
        }
    }

    #[test]
    fn root_identity_is_stable_and_rejects_rebinding() {
        let root = std::env::temp_dir().join(format!("n2-root-v5-{}", Ulid::new()));
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
        let root = std::env::temp_dir().join(format!("n2-root-v5-dirty-{}", Ulid::new()));
        fs::create_dir_all(&root).expect("create root");
        fs::write(root.join("existing"), b"data").expect("seed root");
        assert!(initialize_local_root_identities(&runtime(&root, "namespace-a")).is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn physical_root_identity_is_independent_of_logical_ec_contract() {
        let root = std::env::temp_dir().join(format!("n2-root-v5-policy-{}", Ulid::new()));
        fs::create_dir_all(&root).expect("create root");
        let first = runtime(&root, "namespace-a");
        initialize_local_root_identities(&first).expect("enroll root");

        let mut changed = runtime(&root, "namespace-a");
        changed.membership.pools[0].policy.object_parity_shards = 2;
        initialize_local_root_identities(&changed)
            .expect("physical identity must not couple a device to one EC contract");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn enrolled_roots_are_independent_and_allow_local_root_set_growth() {
        let first_root = std::env::temp_dir().join(format!("n2-root-v5-set-a-{}", Ulid::new()));
        let second_root = std::env::temp_dir().join(format!("n2-root-v5-set-b-{}", Ulid::new()));
        fs::create_dir_all(&first_root).expect("create first root");
        fs::create_dir_all(&second_root).expect("create second root");
        let first = runtime(&first_root, "node-a");
        initialize_local_root_identities(&first).expect("enroll first root set");

        let mut changed = runtime(&first_root, "node-a");
        let added = StorageRoot {
            id: "root-2".into(),
            physical_id: None,
            path: second_root.display().to_string(),
        };
        changed.local_storage_roots.push(added.clone());
        changed.membership.nodes[0].storage_roots.push(added);
        initialize_local_root_identities(&changed).expect("enroll added independent root");
        verify_local_root_identity(&changed, &changed.local_storage_roots[0])
            .expect("first root remains enrolled");
        verify_local_root_identity(&changed, &changed.local_storage_roots[1])
            .expect("second root is enrolled");

        let _ = fs::remove_dir_all(first_root);
        let _ = fs::remove_dir_all(second_root);
    }

    #[test]
    fn restart_identity_check_tolerates_one_unavailable_enrolled_root() {
        let healthy = std::env::temp_dir().join(format!("n2-root-v5-healthy-{}", Ulid::new()));
        let unavailable =
            std::env::temp_dir().join(format!("n2-root-v5-unavailable-{}", Ulid::new()));
        fs::create_dir_all(&healthy).expect("create healthy root");
        fs::write(&unavailable, b"unavailable mount").expect("create unavailable root marker");

        let mut runtime = runtime(&healthy, "node-a");
        let missing = StorageRoot {
            id: "root-unavailable".into(),
            physical_id: None,
            path: unavailable.display().to_string(),
        };
        runtime.local_storage_roots.push(missing.clone());
        runtime.membership.nodes[0].storage_roots.push(missing);

        initialize_local_root_identities(&runtime)
            .expect("one healthy enrolled root keeps the node startable");
        verify_local_root_identity(&runtime, &runtime.local_storage_roots[0])
            .expect("healthy root is enrolled");

        let _ = fs::remove_dir_all(healthy);
        let _ = fs::remove_file(unavailable);
    }

    #[test]
    fn restart_identity_check_rejects_zero_available_roots() {
        let unavailable = std::env::temp_dir().join(format!("n2-root-v5-none-{}", Ulid::new()));
        fs::write(&unavailable, b"unavailable mount").expect("create unavailable root marker");
        let error = initialize_local_root_identities(&runtime(&unavailable, "node-a"))
            .expect_err("a node with no available root must fail startup");
        assert!(
            error
                .to_string()
                .contains("no local N2 storage root is available")
        );
        let _ = fs::remove_file(unavailable);
    }
}
