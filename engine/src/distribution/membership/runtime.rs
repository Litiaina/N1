use super::*;

#[derive(Clone, Debug)]
pub struct ClusterRuntime {
    /// Logical slot ID while active; physical instance ID while unassigned reserve.
    pub local_node_id: String,
    pub local_instance_id: String,
    pub local_is_reserve: bool,
    pub local_pool_id: String,
    pub local_storage_roots: Vec<StorageRoot>,
    pub membership_path: PathBuf,
    pub layout_policy: ClusterLayoutPolicy,
    pub membership: ClusterMembership,
    pub auto_rebalance: bool,
}

static CLUSTER_RUNTIME: OnceLock<ArcSwap<ClusterRuntime>> = OnceLock::new();

pub fn active_cluster_runtime() -> Result<Arc<ClusterRuntime>, AppError> {
    CLUSTER_RUNTIME
        .get()
        .map(ArcSwap::load_full)
        .ok_or_else(|| {
            AppError::InternalServerError("N2 cluster runtime is not initialized".into())
        })
}

pub fn try_active_cluster_runtime() -> Option<Arc<ClusterRuntime>> {
    CLUSTER_RUNTIME.get().map(ArcSwap::load_full)
}

pub fn publish_cluster_runtime(runtime: Arc<ClusterRuntime>) -> Result<(), AppError> {
    let current = CLUSTER_RUNTIME.get().ok_or_else(|| {
        AppError::InternalServerError("N2 cluster runtime is not initialized".into())
    })?;
    current.store(runtime);
    Ok(())
}

pub(super) fn parse_usize(
    raw: Option<&str>,
    key: &str,
    default_value: usize,
) -> Result<usize, AppError> {
    match raw.map(str::trim) {
        None | Some("") => Ok(default_value),
        Some("auto") => Err(AppError::BadRequest(format!(
            "{key}=auto is not valid in N2 3.4; pool EC is static"
        ))),
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| AppError::BadRequest(format!("{key} must be a positive integer"))),
    }
}

pub(super) fn parse_bool(raw: Option<&str>, default_value: bool) -> bool {
    match raw.map(str::trim) {
        None | Some("") => default_value,
        Some(value) if matches!(value, "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON") => true,
        Some(value) if matches!(value, "0" | "false" | "FALSE" | "no" | "NO" | "off" | "OFF") => {
            false
        }
        Some(_) => default_value,
    }
}

pub(super) fn resolve_membership_path(raw: &str, fs_storage_path: &str) -> PathBuf {
    let path = PathBuf::from(raw.trim());
    if path.is_absolute() {
        path
    } else {
        PathBuf::from(fs_storage_path).join(path)
    }
}

pub(super) fn physical_root_sets_match(configured: &[StorageRoot], bound: &[StorageRoot]) -> bool {
    let mut configured = configured
        .iter()
        .map(|root| (root.physical_id().to_string(), root.path.clone()))
        .collect::<Vec<_>>();
    let mut bound = bound
        .iter()
        .map(|root| (root.physical_id().to_string(), root.path.clone()))
        .collect::<Vec<_>>();
    configured.sort();
    bound.sort();
    configured == bound
}

pub(super) fn resolve_startup_local_binding(
    membership: &ClusterMembership,
    instance_id: &str,
) -> Result<(String, String, bool, Vec<StorageRoot>), AppError> {
    if let Some(binding) = membership.node_for_instance(instance_id) {
        let pool = membership.pool_for_node(&binding.id).ok_or_else(|| {
            AppError::Conflict(format!(
                "local logical slot '{}' is not assigned to a server pool",
                binding.id
            ))
        })?;
        return Ok((
            binding.id.clone(),
            pool.id.clone(),
            false,
            binding.storage_roots.clone(),
        ));
    }
    if let Some(reserve) = membership.reserve(instance_id) {
        return Ok((
            instance_id.to_string(),
            String::new(),
            true,
            reserve.storage_roots.clone(),
        ));
    }
    Err(AppError::Conflict(format!(
        "physical instance '{instance_id}' has no active or reserve membership"
    )))
}

pub(super) fn local_membership(
    node_id: &str,
    pool_id: &str,
    pool_generation: u64,
    endpoint: &str,
    native_endpoint: &str,
    storage_roots: &[StorageRoot],
    policy: ClusterPolicy,
) -> ClusterMembership {
    ClusterMembership {
        format_version: N2_MEMBERSHIP_FORMAT_VERSION,
        cluster_id: "n2-local".to_string(),
        epoch: 1,
        binding_epoch: 1,
        pools: vec![ServerPool {
            id: pool_id.to_string(),
            generation: pool_generation,
            policy,
            state: PoolState::Writable,
            node_ids: vec![node_id.to_string()],
        }],
        nodes: vec![ClusterNode {
            id: node_id.to_string(),
            instance_id: node_id.to_string(),
            binding_epoch: 1,
            endpoint: endpoint.to_string(),
            native_endpoint: Some(native_endpoint.to_string()),
            state: Some(NodeState::Online),
            weight: 1,
            storage_roots: storage_roots.to_vec(),
            joined_epoch: 1,
            capacity_bytes: None,
            used_bytes: None,
            last_seen_unix: Some(Utc::now().timestamp()),
        }],
        reserve_nodes: Vec::new(),
        retired_instances: Vec::new(),
    }
}

pub(super) fn load_or_create_membership(
    path: &Path,
    node_id: &str,
    pool_id: &str,
    pool_generation: u64,
    endpoint: &str,
    native_endpoint: &str,
    storage_roots: &[StorageRoot],
    policy: ClusterPolicy,
) -> Result<ClusterMembership, AppError> {
    if path.exists() {
        let bytes = fs::read(path)
            .map_err(|e| AppError::Io(e, format!("read N2 membership file {}", path.display())))?;
        let mut membership: ClusterMembership = serde_json::from_slice(&bytes).map_err(|e| {
            AppError::InternalServerError(format!(
                "decode N2 3.4 membership file '{}': {e}",
                path.display()
            ))
        })?;
        membership.validate()?;
        let now = Utc::now().timestamp();
        if let Some(local_index) = membership
            .nodes
            .iter()
            .position(|node| node.instance_id == node_id)
        {
            let local_slot_id = membership.nodes[local_index].id.clone();
            let bound_pool = membership.pool_for_node(&local_slot_id).ok_or_else(|| {
                AppError::Conflict(format!(
                    "logical slot '{}' is not assigned to a server pool",
                    local_slot_id
                ))
            })?;
            if bound_pool.id != pool_id
                || bound_pool.generation != pool_generation
                || bound_pool.policy != policy
            {
                return Err(AppError::Conflict(format!(
                    "local slot contract {} generation {} {:?} does not match membership pool {} generation {} {:?}",
                    pool_id,
                    pool_generation,
                    policy,
                    bound_pool.id,
                    bound_pool.generation,
                    bound_pool.policy
                )));
            }
            let local = &mut membership.nodes[local_index];
            if !physical_root_sets_match(storage_roots, &local.storage_roots) {
                return Err(AppError::Conflict(format!(
                    "configured roots for physical instance '{}' do not match logical slot '{}'",
                    node_id, local.id
                )));
            }
            local.endpoint = endpoint.to_string();
            local.native_endpoint = Some(native_endpoint.to_string());
            local.state.get_or_insert(NodeState::Online);
            local.last_seen_unix = Some(now);
        } else if let Some(local) = membership
            .reserve_nodes
            .iter_mut()
            .find(|node| node.instance_id == node_id)
        {
            if !physical_root_sets_match(storage_roots, &local.storage_roots) {
                return Err(AppError::Conflict(format!(
                    "configured roots for reserve instance '{}' do not match membership",
                    node_id
                )));
            }
            local.endpoint = endpoint.to_string();
            local.native_endpoint = Some(native_endpoint.to_string());
            local.state.get_or_insert(NodeState::Online);
            local.last_seen_unix = Some(now);
        } else if membership.is_fenced_instance(node_id) {
            return Err(AppError::Conflict(format!(
                "physical instance '{}' is fenced and cannot rejoin without explicit reserve re-enrollment",
                node_id
            )));
        } else {
            return Err(AppError::Conflict(format!(
                "physical instance '{}' is absent from active slots and the reserve set",
                node_id
            )));
        }
        membership.validate()?;
        write_membership(path, &membership)?;
        return Ok(membership);
    }

    let membership = local_membership(
        node_id,
        pool_id,
        pool_generation,
        endpoint,
        native_endpoint,
        storage_roots,
        policy,
    );
    write_membership(path, &membership)?;
    Ok(membership)
}

pub fn write_membership(path: &Path, membership: &ClusterMembership) -> Result<(), AppError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            AppError::Io(
                e,
                format!("create N2 membership directory {}", parent.display()),
            )
        })?;
    }
    let bytes = serde_json::to_vec_pretty(membership)
        .map_err(|e| AppError::InternalServerError(format!("encode N2 membership file: {e}")))?;
    // Promotion/reconciliation tasks can publish the same local membership
    // concurrently. A process-only suffix lets one task rename another task's
    // temporary file. Keep each atomic publisher independent without adding a
    // lock to the membership fast path.
    let temp = path.with_extension(format!("tmp.{}.{}", std::process::id(), Ulid::new()));
    {
        use std::io::Write;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp)
            .map_err(|e| AppError::Io(e, format!("write N2 membership file {}", temp.display())))?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|e| AppError::Io(e, format!("sync N2 membership file {}", temp.display())))?;
    }
    fs::rename(&temp, path)
        .map_err(|e| AppError::Io(e, format!("publish N2 membership file {}", path.display())))?;
    if let Some(parent) = path.parent() {
        fs::File::open(parent)
            .and_then(|dir| dir.sync_all())
            .map_err(|e| {
                AppError::Io(
                    e,
                    format!("sync N2 membership directory {}", parent.display()),
                )
            })?;
    }
    Ok(())
}

pub fn init_cluster_runtime(
    config: &Ini,
    fs_storage_path: &str,
    local_storage_roots: &[StorageRoot],
    https_endpoint: &str,
    native_endpoint: &str,
) -> Result<Arc<ClusterRuntime>, AppError> {
    let section = config.section(Some("cluster_config"));
    if section.and_then(|s| s.get("node_role")).is_some() {
        return Err(AppError::BadRequest(
            "node_role was removed in N2 3.4; every server-pool node is unified".into(),
        ));
    }
    if section
        .and_then(|s| s.get("namespace_redundancy"))
        .is_some()
    {
        return Err(AppError::BadRequest(
            "namespace_redundancy was removed in N2 3.4; NCNX replicas equal parity + one".into(),
        ));
    }
    let node_id = section
        .and_then(|s| s.get("node_id"))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("node-1")
        .to_string();
    let pool_id = section
        .and_then(|s| s.get("pool_id"))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("pool-1")
        .to_string();
    let pool_generation = parse_usize(
        section.and_then(|s| s.get("pool_generation")),
        "pool_generation",
        1,
    )? as u64;
    let node_endpoint = section
        .and_then(|s| s.get("node_endpoint"))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(https_endpoint);
    let native_endpoint = section
        .and_then(|s| s.get("native_endpoint"))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(native_endpoint);
    let membership_file = section
        .and_then(|s| s.get("membership_file"))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("n2_membership.json");
    let membership_path = resolve_membership_path(membership_file, fs_storage_path);
    let policy = ClusterPolicy {
        object_data_shards: parse_usize(
            section.and_then(|s| s.get("object_data_shards")),
            "object_data_shards",
            1,
        )?,
        object_parity_shards: parse_usize(
            section.and_then(|s| s.get("object_parity_shards")),
            "object_parity_shards",
            1,
        )?,
    };
    policy.validate()?;
    if parse_bool(section.and_then(|s| s.get("auto_rebalance")), false) {
        return Err(AppError::BadRequest(
            "auto_rebalance=true is unsupported: N2 preserves exact object layouts".into(),
        ));
    }
    let membership = load_or_create_membership(
        &membership_path,
        &node_id,
        &pool_id,
        pool_generation,
        node_endpoint,
        native_endpoint,
        local_storage_roots,
        policy,
    )?;
    membership.validate_minimum_topology()?;
    let write_policy = membership
        .newest_writable_pool()
        .ok_or_else(|| {
            AppError::ServiceUnavailable("membership has no writable server pool".into())
        })?
        .policy
        .clone();
    let layout_policy = ClusterLayoutPolicy {
        data_shards: write_policy.object_data_shards,
        parity_shards: write_policy.object_parity_shards,
    };
    let (local_node_id, local_pool_id, local_is_reserve, runtime_storage_roots) =
        resolve_startup_local_binding(&membership, &node_id)?;
    let runtime = Arc::new(ClusterRuntime {
        local_node_id,
        local_instance_id: node_id,
        local_is_reserve,
        local_pool_id,
        // A promoted reserve keeps its physical device but inherits the
        // replaced slot's logical root IDs. Reload those recorded IDs after a
        // restart so peer placement validation remains identical to the live
        // promotion path.
        local_storage_roots: runtime_storage_roots,
        membership_path,
        layout_policy,
        membership,
        auto_rebalance: false,
    });
    CLUSTER_RUNTIME
        .set(ArcSwap::from(runtime.clone()))
        .map_err(|_| {
            AppError::InternalServerError("N2 cluster runtime initialized twice".into())
        })?;
    Ok(runtime)
}

pub fn commit_local_membership(
    membership: ClusterMembership,
) -> Result<Arc<ClusterRuntime>, AppError> {
    membership.validate()?;
    membership.validate_minimum_topology()?;
    let current = active_cluster_runtime()?;
    let (local_node_id, local_pool_id, local_is_reserve, local_storage_roots) =
        if let Some(binding) = membership.node_for_instance(&current.local_instance_id) {
            let pool = membership.pool_for_node(&binding.id).ok_or_else(|| {
                AppError::Conflict(format!(
                    "local logical slot '{}' is not assigned to a server pool",
                    binding.id
                ))
            })?;
            (
                binding.id.clone(),
                pool.id.clone(),
                false,
                binding.storage_roots.clone(),
            )
        } else if let Some(reserve) = membership.reserve(&current.local_instance_id) {
            (
                current.local_instance_id.clone(),
                String::new(),
                true,
                reserve.storage_roots.clone(),
            )
        } else if membership.is_fenced_instance(&current.local_instance_id) {
            (
                current.local_instance_id.clone(),
                String::new(),
                true,
                current.local_storage_roots.clone(),
            )
        } else {
            return Err(AppError::Conflict(format!(
                "local physical instance '{}' is absent from committed membership",
                current.local_instance_id
            )));
        };
    let write_policy = membership
        .newest_writable_pool()
        .ok_or_else(|| AppError::ServiceUnavailable("membership has no writable pool".into()))?
        .policy
        .clone();
    let next = Arc::new(ClusterRuntime {
        local_node_id,
        local_instance_id: current.local_instance_id.clone(),
        local_is_reserve,
        local_pool_id,
        local_storage_roots,
        membership_path: current.membership_path.clone(),
        layout_policy: ClusterLayoutPolicy {
            data_shards: write_policy.object_data_shards,
            parity_shards: write_policy.object_parity_shards,
        },
        membership,
        auto_rebalance: false,
    });
    write_membership(&next.membership_path, &next.membership)?;
    publish_cluster_runtime(next.clone())?;
    Ok(next)
}
