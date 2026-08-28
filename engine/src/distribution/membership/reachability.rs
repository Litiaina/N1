use super::*;

pub(super) fn native_socket_addr(endpoint: &str) -> Option<String> {
    endpoint
        .trim()
        .strip_prefix("n2://")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub(super) async fn native_endpoint_is_reachable(endpoint: &str) -> bool {
    let Some(addr) = native_socket_addr(endpoint) else {
        return false;
    };
    matches!(
        timeout(CLUSTER_NODE_PROBE_TIMEOUT, TcpStream::connect(addr)).await,
        Ok(Ok(_))
    )
}

pub(super) async fn http_endpoint_is_reachable(endpoint: &str) -> bool {
    let url = format!("{}/noa/v1/ping", endpoint.trim_end_matches('/'));
    matches!(
        CLUSTER_PROBE_HTTP_CLIENT.get(url).send().await,
        Ok(response) if response.status().is_success()
    )
}

pub(super) async fn mutation_authority_endpoint_is_reachable(endpoint: &str) -> bool {
    for attempt in 0..MUTATION_AUTHORITY_PROBE_ATTEMPTS {
        if http_endpoint_is_reachable(endpoint).await {
            return true;
        }
        if attempt + 1 < MUTATION_AUTHORITY_PROBE_ATTEMPTS {
            tokio::task::yield_now().await;
        }
    }
    false
}

pub async fn live_mutation_authority_nodes_for_pool(
    runtime: &ClusterRuntime,
    pool_id: &str,
) -> Vec<ClusterNode> {
    let probes = runtime
        .membership
        .active_write_nodes_for_pool(pool_id)
        .into_iter()
        .map(|node| {
            let local_instance_id = runtime.local_instance_id.clone();
            async move {
                (node.is_local(&local_instance_id)
                    || mutation_authority_endpoint_is_reachable(&node.endpoint).await)
                    .then_some(node)
            }
        });
    join_all(probes).await.into_iter().flatten().collect()
}

pub async fn cluster_node_is_reachable(local_node_id: &str, node: &ClusterNode) -> bool {
    if node.is_local(local_node_id) {
        return true;
    }
    if let Some(native_endpoint) = node
        .native_endpoint
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        return native_endpoint_is_reachable(native_endpoint).await;
    }
    http_endpoint_is_reachable(&node.endpoint).await
}

pub(super) async fn live_filter_nodes(
    local_node_id: &str,
    candidates: Vec<ClusterNode>,
) -> Vec<ClusterNode> {
    let probes = candidates.into_iter().map(|node| {
        let local_node_id = local_node_id.to_string();
        async move {
            cluster_node_is_reachable(&local_node_id, &node)
                .await
                .then_some(node)
        }
    });
    join_all(probes).await.into_iter().flatten().collect()
}

pub async fn live_active_write_nodes(runtime: &ClusterRuntime) -> Vec<ClusterNode> {
    live_filter_nodes(
        &runtime.local_instance_id,
        runtime.membership.active_write_nodes(),
    )
    .await
}

pub async fn live_active_write_nodes_for_pool(
    runtime: &ClusterRuntime,
    pool_id: &str,
) -> Vec<ClusterNode> {
    live_filter_nodes(
        &runtime.local_instance_id,
        runtime.membership.active_write_nodes_for_pool(pool_id),
    )
    .await
}

/// Minimum connected component that can safely own degraded mutation traffic.
///
/// `K` preserves reconstructability. A strict pool majority prevents two
/// disjoint network components from both acknowledging NCNX mutations. The
/// larger requirement is the authority boundary for the fixed pool.
pub fn required_pool_mutation_authority(pool: &ServerPool) -> usize {
    pool.policy
        .object_data_shards
        .max(pool.node_ids.len() / 2 + 1)
}

/// Prove that this coordinator belongs to the only pool component large
/// enough to acknowledge a mutation.
///
/// Healthy operation still reaches the complete exact pool. During node loss
/// or a partition, at most the component containing both `K` nodes and a
/// strict majority can write. Smaller components remain readable when their
/// available shards permit it, but cannot manufacture mutation authority from
/// a transport timeout.
pub fn validate_pool_mutation_authority(
    runtime: &ClusterRuntime,
    pool_id: &str,
    reachable: &[ClusterNode],
) -> Result<(), AppError> {
    if runtime.local_is_reserve {
        return Err(AppError::ServiceUnavailable(
            "an unassigned reserve instance cannot coordinate client writes".into(),
        ));
    }
    let pool = runtime.membership.pool(pool_id).ok_or_else(|| {
        AppError::ServiceUnavailable(format!("unknown writable server pool '{pool_id}'"))
    })?;
    let configured = runtime.membership.nodes_for_pool(pool_id);
    let configured_ids = configured
        .iter()
        .map(|node| node.id.as_str())
        .collect::<BTreeSet<_>>();
    if !configured_ids.contains(runtime.local_node_id.as_str()) {
        return Err(AppError::ServiceUnavailable(format!(
            "local logical slot '{}' does not belong to writable pool '{pool_id}'",
            runtime.local_node_id
        )));
    }
    let administratively_writable = configured
        .iter()
        .filter(|node| node.effective_state().accepts_new_writes())
        .map(|node| node.id.as_str())
        .collect::<BTreeSet<_>>();
    if administratively_writable != configured_ids {
        let excluded = configured_ids
            .difference(&administratively_writable)
            .copied()
            .collect::<Vec<_>>();
        return Err(AppError::ServiceUnavailable(format!(
            "N2 write authority is unavailable for pool '{pool_id}': logical slot state excludes {}; complete the binding/fencing transition before writing",
            excluded.join(", ")
        )));
    }
    let reachable_ids = reachable
        .iter()
        .map(|node| node.id.as_str())
        .collect::<BTreeSet<_>>();
    let missing = configured_ids
        .difference(&reachable_ids)
        .copied()
        .collect::<Vec<_>>();
    let unexpected = reachable_ids
        .difference(&configured_ids)
        .copied()
        .collect::<Vec<_>>();
    if !unexpected.is_empty() {
        return Err(AppError::ServiceUnavailable(format!(
            "N2 write authority is invalid for pool '{pool_id}': reached unexpected logical slot(s) {}",
            unexpected.join(", "),
        )));
    }
    let required = required_pool_mutation_authority(pool);
    if reachable_ids.len() < required {
        return Err(AppError::ServiceUnavailable(format!(
            "N2 write authority is unavailable for pool '{pool_id}': reached {} of {} exact slots (missing: {}). Mutation requires max(K={}, strict-majority={}) = {} reachable slots; restore a binding or promote a reserve before writing",
            reachable_ids.len(),
            configured_ids.len(),
            if missing.is_empty() {
                "none".into()
            } else {
                missing.join(", ")
            },
            pool.policy.object_data_shards,
            configured_ids.len() / 2 + 1,
            required,
        )));
    }
    Ok(())
}

/// Prove exact authority for a new object write. Unlike namespace mutation of
/// an existing object, new object placement additionally requires a writable
/// pool; a sealed historical pool remains mutable only for its recorded NCNX.
pub fn validate_pool_write_authority(
    runtime: &ClusterRuntime,
    pool_id: &str,
    reachable: &[ClusterNode],
) -> Result<(), AppError> {
    let pool = runtime.membership.pool(pool_id).ok_or_else(|| {
        AppError::ServiceUnavailable(format!("unknown writable server pool '{pool_id}'"))
    })?;
    if !pool.writable() {
        return Err(AppError::ServiceUnavailable(format!(
            "server pool '{pool_id}' is sealed"
        )));
    }
    validate_pool_mutation_authority(runtime, pool_id, reachable)
}

pub async fn pool_mutation_authority_nodes(
    runtime: &ClusterRuntime,
    pool_id: &str,
) -> Result<Vec<ClusterNode>, AppError> {
    let reachable = live_mutation_authority_nodes_for_pool(runtime, pool_id).await;
    validate_pool_mutation_authority(runtime, pool_id, &reachable)?;
    Ok(reachable)
}

pub async fn pool_write_authority_nodes(
    runtime: &ClusterRuntime,
    pool_id: &str,
) -> Result<Vec<ClusterNode>, AppError> {
    let reachable = live_mutation_authority_nodes_for_pool(runtime, pool_id).await;
    validate_pool_write_authority(runtime, pool_id, &reachable)?;
    Ok(reachable)
}

pub async fn pool_write_nodes(
    runtime: &ClusterRuntime,
    pool_id: &str,
) -> Result<Vec<ClusterNode>, AppError> {
    pool_write_authority_nodes(runtime, pool_id).await?;

    // Authority belongs to the exact NCNX control plane. The shard/native
    // plane remains independently degradable: callers enforce K before
    // committing the fixed object layout and record every unavailable exact
    // target for healing.
    Ok(live_active_write_nodes_for_pool(runtime, pool_id).await)
}

pub async fn live_newest_pool_write_nodes(runtime: &ClusterRuntime) -> Vec<ClusterNode> {
    let Some(pool) = runtime.membership.newest_writable_pool() else {
        return Vec::new();
    };
    live_active_write_nodes_for_pool(runtime, &pool.id).await
}

pub async fn live_readable_nodes(runtime: &ClusterRuntime) -> Vec<ClusterNode> {
    live_filter_nodes(
        &runtime.local_instance_id,
        runtime.membership.readable_nodes(),
    )
    .await
}
