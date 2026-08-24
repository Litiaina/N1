use super::*;

async fn list_node(
    runtime: &ClusterRuntime,
    pool: &ServerPool,
    node: &ClusterNode,
    fragment_id: &str,
    prefix: &str,
    after: Option<&str>,
    limit: usize,
    trash: bool,
    canonical_only: bool,
) -> Result<Vec<PeerNcnxName>, AppError> {
    if node.effective_state() == crate::distribution::membership::NodeState::Degraded {
        return Err(AppError::ServiceUnavailable(format!(
            "NCNX slot '{}' is recovering; listing must use surviving replicas",
            node.id
        )));
    }
    let request = PeerNcnxRequest {
        action: if trash && canonical_only {
            PeerNcnxAction::ListTrash
        } else if trash {
            PeerNcnxAction::ListTrashAll
        } else if canonical_only {
            PeerNcnxAction::ListLive
        } else {
            PeerNcnxAction::ListLiveAll
        },
        pool_id: pool.id.clone(),
        pool_generation: pool.generation,
        target_slot_id: node.id.clone(),
        target_instance_id: node.instance_id.clone(),
        binding_epoch: runtime.membership.binding_epoch,
        fragment_id: fragment_id.to_string(),
        logical_key: prefix.to_string(),
        root_id: String::new(),
        trash: None,
        bytes: None,
        after: after.map(ToOwned::to_owned),
        limit: Some(limit),
    };
    if node.id == runtime.local_node_id {
        return Ok(run_local_list(Arc::new(runtime.clone()), request)
            .await?
            .names);
    }
    let client = peer_http_client()?;
    let mut call = client
        .post(format!(
            "{}{}",
            node.endpoint.trim_end_matches('/'),
            PEER_NCNX_PATH
        ))
        .timeout(PEER_NCNX_TIMEOUT)
        .json(&request);
    if let Some(secret) = cluster_secret() {
        call = call.header("x-n2-cluster-secret", secret);
    }
    let response = call.send().await.map_err(|err| {
        AppError::ServiceUnavailable(format!("list NCNX from '{}': {err}", node.id))
    })?;
    if !response.status().is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "list NCNX from '{}' returned {}",
            node.id,
            response.status()
        )));
    }
    response
        .json::<PeerNcnxResponse>()
        .await
        .map(|response| response.names)
        .map_err(|err| AppError::InternalServerError(format!("decode NCNX listing: {err}")))
}

pub(super) struct NodeListPage {
    pub(super) pool_id: String,
    pub(super) node_id: String,
    pub(super) allowed_failures: usize,
    pub(super) result: Result<Vec<PeerNcnxName>, AppError>,
}

async fn list_all_nodes(
    runtime: &ClusterRuntime,
    fragment_id: &str,
    prefix: &str,
    after: Option<&str>,
    limit: usize,
    trash: bool,
    canonical_only: bool,
) -> Vec<NodeListPage> {
    let mut calls = Vec::new();
    for pool in &runtime.membership.pools {
        for node in runtime.membership.nodes_for_pool(&pool.id) {
            let runtime = runtime.clone();
            let pool = pool.clone();
            let fragment_id = fragment_id.to_string();
            let prefix = prefix.to_string();
            let after = after.map(ToOwned::to_owned);
            calls.push(async move {
                let result = list_node(
                    &runtime,
                    &pool,
                    &node,
                    &fragment_id,
                    &prefix,
                    after.as_deref(),
                    limit,
                    trash,
                    canonical_only,
                )
                .await;
                NodeListPage {
                    pool_id: pool.id,
                    node_id: node.id,
                    allowed_failures: pool.policy.object_parity_shards,
                    result,
                }
            });
        }
    }
    join_all(calls).await
}

pub(super) fn merge_node_pages(
    pages: Vec<NodeListPage>,
    limit: usize,
    tolerate_failures: bool,
) -> Result<Vec<PeerNcnxName>, AppError> {
    let mut failures = BTreeMap::<String, (usize, usize, Vec<String>)>::new();
    let mut merged = BTreeMap::<String, PeerNcnxName>::new();
    for page in pages {
        match page.result {
            Ok(names) => {
                for name in names {
                    merged.insert(ncnx_cursor(&name.logical_key, name.trash.as_ref()), name);
                }
            }
            Err(err) => {
                let failure = failures
                    .entry(page.pool_id)
                    .or_insert_with(|| (0, page.allowed_failures, Vec::new()));
                failure.0 = failure.0.saturating_add(1);
                failure.2.push(format!("{}: {}", page.node_id, err));
            }
        }
    }

    if let Some((pool_id, (failed, allowed, details))) = failures
        .iter()
        .find(|(_, (failed, allowed, _))| !tolerate_failures || failed > allowed)
    {
        return Err(AppError::ServiceUnavailable(format!(
            "NCNX listing cannot prove a complete result for pool '{}': {} node failures, tolerance {}; {}",
            pool_id,
            failed,
            allowed,
            details.join("; ")
        )));
    }

    Ok(merged.into_values().take(limit).collect())
}

pub async fn list_canonical_names(
    fragment_id: &str,
    prefix: &str,
    after: Option<&str>,
    limit: usize,
    trash: bool,
) -> Result<Vec<PeerNcnxName>, AppError> {
    let runtime = active_cluster_runtime()?;
    let limit = limit.clamp(1, 10_000);
    // A survival upload may initially have only one of its R=M+1 NCNX copies,
    // and that copy is not necessarily the canonical holder. Merge bounded
    // physical replica pages so every acknowledged key is immediately
    // visible without opening NCNX or NCDX.
    let replicas = list_all_nodes(
        runtime.as_ref(),
        fragment_id,
        prefix,
        after,
        limit,
        trash,
        false,
    )
    .await;
    merge_node_pages(replicas, limit, true)
}

/// Healing discovery asks every root for physical names so a missing
/// canonical replica cannot hide an otherwise surviving key.
pub async fn list_replica_names(
    fragment_id: &str,
    prefix: &str,
    after: Option<&str>,
    limit: usize,
    trash: bool,
) -> Result<Vec<PeerNcnxName>, AppError> {
    let runtime = active_cluster_runtime()?;
    let limit = limit.clamp(1, 10_000);
    let replicas = list_all_nodes(
        runtime.as_ref(),
        fragment_id,
        prefix,
        after,
        limit,
        trash,
        false,
    )
    .await;
    merge_node_pages(replicas, limit, true)
}

/// Revalidate an exact physical NCNX name after a lockless listing. A false
/// result proves that another namespace/GC mutation already removed the
/// generation; transport failures remain errors rather than false absence.
pub async fn name_is_still_advertised(
    fragment_id: &str,
    target: &PeerNcnxName,
) -> Result<bool, AppError> {
    let target_identity = ncnx_cursor(&target.logical_key, target.trash.as_ref());
    let mut cursor = None;
    loop {
        let page = list_replica_names(
            fragment_id,
            &target.logical_key,
            cursor.as_deref(),
            1_000,
            target.trash.is_some(),
        )
        .await?;
        if page
            .iter()
            .any(|name| ncnx_cursor(&name.logical_key, name.trash.as_ref()) == target_identity)
        {
            return Ok(true);
        }
        if page.len() < 1_000 {
            return Ok(false);
        }
        cursor = page
            .last()
            .map(|name| ncnx_cursor(&name.logical_key, name.trash.as_ref()));
    }
}

pub async fn read_canonical_record(
    fragment_id: &str,
    name: &PeerNcnxName,
) -> Result<LocatedNcnx, AppError> {
    if name.trash.is_none() {
        return find_live(fragment_id, &name.logical_key)
            .await?
            .ok_or(AppError::NotFound);
    }
    let runtime = active_cluster_runtime()?;
    let locator = name.trash.as_ref().unwrap();
    let mut pools = runtime.membership.pools.iter().collect::<Vec<_>>();
    pools.sort_by_key(|pool| std::cmp::Reverse(pool.generation));
    for pool in pools {
        let layout = build_ncnx_layout(&runtime, pool, fragment_id, &name.logical_key)?;
        if let Some((record, bytes)) = read_from_layout(
            &runtime,
            &layout,
            fragment_id,
            &name.logical_key,
            Some(locator),
            true,
        )
        .await?
        {
            return Ok(LocatedNcnx {
                pool_id: pool.id.clone(),
                pool_generation: pool.generation,
                logical_key: name.logical_key.clone(),
                record,
                bytes,
            });
        }
    }
    Err(AppError::NotFound)
}

/// Revalidate an exact namespace-to-object reference after an object lookup
/// failed. This is used only on the exceptional healing path so a concurrent
/// trash expiry, recovery, or rename cannot turn a mixed lockless scan into a
/// false dangling-pointer incident.
pub async fn key_references_object(
    fragment_id: &str,
    logical_key: &str,
    object_id: &str,
) -> Result<bool, AppError> {
    if let Some(live) = find_live(fragment_id, logical_key).await?
        && live.record.object_id == object_id
    {
        return Ok(true);
    }

    let mut cursor = None;
    loop {
        let page =
            list_replica_names(fragment_id, logical_key, cursor.as_deref(), 1_000, true).await?;
        for name in &page {
            if name.logical_key != logical_key {
                continue;
            }
            if name.trash.as_ref().is_some_and(trash_is_expired) {
                continue;
            }
            match read_canonical_record(fragment_id, name).await {
                Ok(located) if located.record.object_id == object_id => return Ok(true),
                Ok(_) | Err(AppError::NotFound) => {}
                Err(error) => return Err(error),
            }
        }
        if page.len() < 1_000 {
            return Ok(false);
        }
        cursor = page
            .last()
            .map(|name| ncnx_cursor(&name.logical_key, name.trash.as_ref()));
    }
}
