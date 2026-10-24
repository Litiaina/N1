use super::*;

fn apply_store_mutation(store: &NcnxStore, request: &PeerNcnxRequest) -> Result<bool, AppError> {
    match request.action {
        PeerNcnxAction::CreateLive => store.create_live(
            &request.fragment_id,
            &request.logical_key,
            required_bytes(request)?,
        ),
        PeerNcnxAction::CreateTrash => store.create_trash(
            &request.fragment_id,
            required_trash(request)?,
            required_bytes(request)?,
        ),
        PeerNcnxAction::RemoveLive => store.remove_live_if_exact(
            &request.fragment_id,
            &request.logical_key,
            required_bytes(request)?,
        ),
        PeerNcnxAction::RemoveTrash => store.remove_trash_if_exact(
            &request.fragment_id,
            required_trash(request)?,
            required_bytes(request)?,
        ),
        PeerNcnxAction::RepairLive => store.repair_live(
            &request.fragment_id,
            &request.logical_key,
            required_bytes(request)?,
        ),
        PeerNcnxAction::RepairTrash => store.repair_trash(
            &request.fragment_id,
            required_trash(request)?,
            required_bytes(request)?,
        ),
        _ => Err(AppError::BadRequest(
            "NCNX read/list action is not a store mutation".into(),
        )),
    }
}

fn local_apply(
    runtime: &ClusterRuntime,
    request: &PeerNcnxRequest,
) -> Result<PeerNcnxResponse, AppError> {
    let pool = validate_pool_contract(runtime, request)?;
    let expected_layout =
        build_ncnx_layout(runtime, pool, &request.fragment_id, &request.logical_key)?;
    let assigned = expected_layout
        .replicas
        .iter()
        .find(|replica| replica.node_id == runtime.local_node_id)
        .ok_or_else(|| {
            AppError::Forbidden(format!(
                "node '{}' is not an NCNX holder for key '{}'",
                runtime.local_node_id, request.logical_key
            ))
        })?;
    if assigned.root_id != request.root_id {
        return Err(AppError::Conflict(format!(
            "NCNX key '{}' belongs on root '{}', not '{}'",
            request.logical_key, assigned.root_id, request.root_id
        )));
    }
    let mut response = PeerNcnxResponse {
        node_id: runtime.local_node_id.clone(),
        root_id: request.root_id.clone(),
        changed: false,
        bytes: None,
        names: Vec::new(),
    };
    if matches!(
        request.action,
        PeerNcnxAction::RepairLive | PeerNcnxAction::RepairTrash
    ) {
        let desired = required_bytes(request)?;
        let record = NcnxRecord::decode(desired)?;
        if record.fragment_id != request.fragment_id {
            return Err(AppError::Conflict(
                "NCNX fragment identity differs from repair target".into(),
            ));
        }
        let mut identical_root = None;
        // Roots are interchangeable routes within one node. A newly writable
        // preferred root must not receive an obsolete generation when a valid
        // newer generation already resides on a fallback root.
        for store in local_stores(runtime, &request.root_id) {
            let existing = if matches!(request.action, PeerNcnxAction::RepairTrash) {
                store.read_trash(&request.fragment_id, required_trash(request)?)
            } else {
                store.read_live(&request.fragment_id, &request.logical_key)
            };
            match existing {
                Ok(Some((_, existing))) => {
                    if existing != desired {
                        return Err(AppError::Conflict(format!(
                            "refused to repair a different valid NCNX generation on local root '{}'",
                            store.root_id()
                        )));
                    }
                    identical_root = Some(store.root_id().to_string());
                }
                Ok(None) | Err(AppError::Conflict(_)) => {}
                // Corrupt bytes can be repaired. An I/O failure cannot prove
                // absence: that root might hold a valid different generation.
                Err(error) => return Err(error),
            }
        }
        if let Some(root_id) = identical_root {
            response.root_id = root_id;
            return Ok(response);
        }
    }
    match request.action {
        PeerNcnxAction::ReadLive => {
            let mut first_error = None;
            let mut local_disk_responded = false;
            for store in local_stores(runtime, &request.root_id) {
                match store.read_live(&request.fragment_id, &request.logical_key) {
                    Ok(Some((_, bytes))) => {
                        response.root_id = store.root_id().to_string();
                        response.bytes = Some(ByteBuf::from(bytes));
                        return Ok(response);
                    }
                    Ok(None) => local_disk_responded = true,
                    Err(error) => {
                        first_error.get_or_insert(error);
                    }
                }
            }
            if !local_disk_responded && let Some(error) = first_error {
                return Err(error);
            }
        }
        PeerNcnxAction::ReadTrash => {
            let locator = required_trash(request)?;
            let mut first_error = None;
            let mut local_disk_responded = false;
            for store in local_stores(runtime, &request.root_id) {
                match store.read_trash(&request.fragment_id, locator) {
                    Ok(Some((_, bytes))) => {
                        response.root_id = store.root_id().to_string();
                        response.bytes = Some(ByteBuf::from(bytes));
                        return Ok(response);
                    }
                    Ok(None) => local_disk_responded = true,
                    Err(error) => {
                        first_error.get_or_insert(error);
                    }
                }
            }
            if !local_disk_responded && let Some(error) = first_error {
                return Err(error);
            }
        }
        PeerNcnxAction::CreateLive
        | PeerNcnxAction::CreateTrash
        | PeerNcnxAction::RepairLive
        | PeerNcnxAction::RepairTrash => {
            let mut failures = Vec::new();
            for store in writable_local_stores(runtime, &request.root_id)? {
                match apply_store_mutation(&store, request) {
                    Ok(changed) => {
                        response.root_id = store.root_id().to_string();
                        response.changed = changed;
                        return Ok(response);
                    }
                    Err(error @ AppError::Conflict(_)) => return Err(error),
                    Err(error) => failures.push(format!("{}: {}", store.root_id(), error)),
                }
            }
            return Err(AppError::ServiceUnavailable(format!(
                "NCNX mutation could not use any healthy disk on node '{}': {}",
                runtime.local_node_id,
                failures.join("; ")
            )));
        }
        PeerNcnxAction::RemoveLive | PeerNcnxAction::RemoveTrash => {
            let mut successes = 0usize;
            let mut failures = Vec::new();
            for store in local_stores(runtime, &request.root_id) {
                match apply_store_mutation(&store, request) {
                    Ok(changed) => {
                        successes = successes.saturating_add(1);
                        response.changed |= changed;
                    }
                    Err(error @ AppError::Conflict(_)) => return Err(error),
                    Err(error) => failures.push(format!("{}: {}", store.root_id(), error)),
                }
            }
            if successes == 0 || !failures.is_empty() {
                return Err(AppError::ServiceUnavailable(format!(
                    "NCNX removal could not reach any local disk on node '{}': {}",
                    runtime.local_node_id,
                    failures.join("; ")
                )));
            }
        }
        PeerNcnxAction::ListLive
        | PeerNcnxAction::ListTrash
        | PeerNcnxAction::ListLiveAll
        | PeerNcnxAction::ListTrashAll => {
            return Err(AppError::BadRequest(
                "NCNX list requests use the node-wide listing handler".into(),
            ));
        }
    }
    Ok(response)
}

fn validate_batch_contract<'a>(
    runtime: &'a ClusterRuntime,
    request: &PeerNcnxBatchRequest,
) -> Result<&'a ServerPool, AppError> {
    if request.items.is_empty() || request.items.len() > PEER_NCNX_BATCH_MAX {
        return Err(AppError::BadRequest(format!(
            "NCNX batch requires 1..={PEER_NCNX_BATCH_MAX} entries"
        )));
    }
    let pool = runtime
        .membership
        .pool(&request.pool_id)
        .ok_or_else(|| AppError::Conflict(format!("unknown NCNX pool '{}'", request.pool_id)))?;
    if pool.generation != request.pool_generation
        || runtime.local_is_reserve
        || request.binding_epoch != runtime.membership.binding_epoch
        || request.target_slot_id != runtime.local_node_id
        || request.target_instance_id != runtime.local_instance_id
        || !pool.node_ids.iter().any(|id| id == &runtime.local_node_id)
    {
        return Err(AppError::Conflict(
            "stale, misbound, or non-member NCNX batch request".into(),
        ));
    }
    let mut keys = BTreeSet::new();
    for item in &request.items {
        if let Some(locator) = &item.trash
            && validate_logical_key(&locator.logical_key)?
                != validate_logical_key(&item.logical_key)?
        {
            return Err(AppError::BadRequest(
                "NCNX trash locator differs from item key".into(),
            ));
        }
        let normalized_key = validate_logical_key(&item.logical_key)?;
        let identity = if let Some(locator) = &item.trash {
            format!(
                "{normalized_key}\0{:020}\0{}",
                locator.deleted_at_unix_nanos, locator.delete_mutation_id
            )
        } else {
            normalized_key
        };
        if !keys.insert(identity) {
            return Err(AppError::Conflict(format!(
                "duplicate NCNX key '{}' in peer batch",
                item.logical_key
            )));
        }
        match request.action {
            PeerNcnxBatchAction::CreateLive | PeerNcnxBatchAction::RemoveLive
                if item.trash.is_some() =>
            {
                return Err(AppError::BadRequest(
                    "live NCNX batch item cannot carry a trash locator".into(),
                ));
            }
            PeerNcnxBatchAction::CreateTrash | PeerNcnxBatchAction::RemoveTrash
                if item.trash.is_none() =>
            {
                return Err(AppError::BadRequest(
                    "trash NCNX batch item requires a trash locator".into(),
                ));
            }
            _ => {}
        }
        let layout = build_ncnx_layout(runtime, pool, &request.fragment_id, &item.logical_key)?;
        let assigned = layout
            .replicas
            .iter()
            .find(|replica| replica.node_id == runtime.local_node_id)
            .ok_or_else(|| {
                AppError::Forbidden(format!(
                    "node '{}' is not an NCNX holder for key '{}'",
                    runtime.local_node_id, item.logical_key
                ))
            })?;
        if assigned.root_id != request.root_id {
            return Err(AppError::Conflict(format!(
                "NCNX key '{}' belongs on root '{}', not '{}'",
                item.logical_key, assigned.root_id, request.root_id
            )));
        }
    }
    Ok(pool)
}

fn local_apply_batch(
    runtime: &ClusterRuntime,
    request: &PeerNcnxBatchRequest,
) -> Result<PeerNcnxBatchResponse, AppError> {
    validate_batch_contract(runtime, request)?;
    let live_entries = request
        .items
        .iter()
        .map(|item| (item.logical_key.clone(), item.bytes.clone().into_vec()))
        .collect::<Vec<_>>();
    let trash_entries = request
        .items
        .iter()
        .filter_map(|item| {
            item.trash
                .clone()
                .map(|locator| (locator, item.bytes.clone().into_vec()))
        })
        .collect::<Vec<_>>();
    let mut changed = 0usize;
    let mut selected_root = request.root_id.clone();
    match request.action {
        PeerNcnxBatchAction::CreateLive => {
            let mut failures = Vec::new();
            for store in writable_local_stores(runtime, &request.root_id)? {
                match store.create_live_batch(&request.fragment_id, &live_entries) {
                    Ok(count) => {
                        changed = count;
                        selected_root = store.root_id().to_string();
                        failures.clear();
                        break;
                    }
                    Err(error @ AppError::Conflict(_)) => return Err(error),
                    Err(error) => failures.push(format!("{}: {}", store.root_id(), error)),
                }
            }
            if !failures.is_empty() {
                return Err(AppError::ServiceUnavailable(format!(
                    "NCNX batch publication could not use any healthy disk on node '{}': {}",
                    runtime.local_node_id,
                    failures.join("; ")
                )));
            }
        }
        PeerNcnxBatchAction::CreateTrash => {
            let mut failures = Vec::new();
            for store in writable_local_stores(runtime, &request.root_id)? {
                match store.create_trash_batch(&request.fragment_id, &trash_entries) {
                    Ok(count) => {
                        changed = count;
                        selected_root = store.root_id().to_string();
                        failures.clear();
                        break;
                    }
                    Err(error @ AppError::Conflict(_)) => return Err(error),
                    Err(error) => failures.push(format!("{}: {}", store.root_id(), error)),
                }
            }
            if !failures.is_empty() {
                return Err(AppError::ServiceUnavailable(format!(
                    "NCNX trash batch publication could not use any healthy disk on node '{}': {}",
                    runtime.local_node_id,
                    failures.join("; ")
                )));
            }
        }
        PeerNcnxBatchAction::RemoveLive => {
            let mut successes = 0usize;
            let mut failures = Vec::new();
            for store in local_stores(runtime, &request.root_id) {
                match store.remove_live_batch_if_exact(&request.fragment_id, &live_entries) {
                    Ok(count) => {
                        successes = successes.saturating_add(1);
                        changed = changed.saturating_add(count);
                        if count > 0 {
                            selected_root = store.root_id().to_string();
                        }
                    }
                    Err(error @ AppError::Conflict(_)) => return Err(error),
                    Err(error) => failures.push(format!("{}: {}", store.root_id(), error)),
                }
            }
            if successes == 0 || !failures.is_empty() {
                return Err(AppError::ServiceUnavailable(format!(
                    "NCNX batch removal could not reach any local disk on node '{}': {}",
                    runtime.local_node_id,
                    failures.join("; ")
                )));
            }
        }
        PeerNcnxBatchAction::RemoveTrash => {
            let mut successes = 0usize;
            let mut failures = Vec::new();
            for store in local_stores(runtime, &request.root_id) {
                match store.remove_trash_batch_if_exact(&request.fragment_id, &trash_entries) {
                    Ok(count) => {
                        successes = successes.saturating_add(1);
                        changed = changed.saturating_add(count);
                        if count > 0 {
                            selected_root = store.root_id().to_string();
                        }
                    }
                    Err(error @ AppError::Conflict(_)) => return Err(error),
                    Err(error) => failures.push(format!("{}: {}", store.root_id(), error)),
                }
            }
            if successes == 0 || !failures.is_empty() {
                return Err(AppError::ServiceUnavailable(format!(
                    "NCNX trash batch removal could not reach any local disk on node '{}': {}",
                    runtime.local_node_id,
                    failures.join("; ")
                )));
            }
        }
    }
    Ok(PeerNcnxBatchResponse {
        node_id: runtime.local_node_id.clone(),
        root_id: selected_root,
        changed,
    })
}

pub(super) async fn run_local_batch(
    runtime: Arc<ClusterRuntime>,
    request: PeerNcnxBatchRequest,
) -> Result<PeerNcnxBatchResponse, AppError> {
    let admission = LOCAL_NCNX_ADMISSION
        .clone()
        .try_acquire_owned()
        .map_err(|_| AppError::ServiceUnavailable("NCNX receiver admission is full".into()))?;
    validate_batch_contract(&runtime, &request)?;
    let key_guards = coordination::lock_keys(
        &request.fragment_id,
        request.items.iter().map(|item| item.logical_key.as_str()),
    )
    .await?;
    let permit = LOCAL_NCNX_BATCH_BUDGET
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| AppError::ServiceUnavailable("NCNX batch budget is closed".into()))?;
    tokio::task::spawn_blocking(move || {
        let _key_guards = key_guards;
        let _admission = admission;
        let _permit = permit;
        let _binding = crate::distribution::membership::guard_cluster_binding()?;
        lease::validate_tokens(
            &runtime,
            &request.pool_id,
            request.pool_generation,
            &request.fragment_id,
            request.items.iter().map(|item| {
                (
                    item.logical_key.as_str(),
                    request.root_id.as_str(),
                    item.lease_token.as_deref().unwrap_or(""),
                )
            }),
        )?;
        local_apply_batch(&runtime, &request)
    })
    .await
    .map_err(|error| AppError::InternalServerError(format!("NCNX batch worker failed: {error}")))?
}

pub(super) async fn run_local_apply(
    runtime: Arc<ClusterRuntime>,
    request: PeerNcnxRequest,
) -> Result<PeerNcnxResponse, AppError> {
    let admission = LOCAL_NCNX_ADMISSION
        .clone()
        .try_acquire_owned()
        .map_err(|_| AppError::ServiceUnavailable("NCNX receiver admission is full".into()))?;
    validate_pool_contract(&runtime, &request)?;
    let key_guards =
        coordination::lock_keys(&request.fragment_id, [request.logical_key.as_str()]).await?;
    let permit = LOCAL_NCNX_IO_BUDGET
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| AppError::ServiceUnavailable("NCNX filesystem budget is closed".into()))?;
    tokio::task::spawn_blocking(move || {
        let _key_guards = key_guards;
        let _admission = admission;
        let _permit = permit;
        let _binding = crate::distribution::membership::guard_cluster_binding()?;
        if !matches!(
            request.action,
            PeerNcnxAction::ReadLive | PeerNcnxAction::ReadTrash
        ) || request.lease_token.is_some()
        {
            lease::validate_tokens(
                &runtime,
                &request.pool_id,
                request.pool_generation,
                &request.fragment_id,
                [(
                    request.logical_key.as_str(),
                    request.root_id.as_str(),
                    request.lease_token.as_deref().unwrap_or(""),
                )],
            )?;
        }
        local_apply(&runtime, &request)
    })
    .await
    .map_err(|error| AppError::InternalServerError(format!("NCNX worker failed: {error}")))?
}

pub(super) async fn run_local_list(
    runtime: Arc<ClusterRuntime>,
    request: PeerNcnxRequest,
) -> Result<PeerNcnxResponse, AppError> {
    let admission = LOCAL_NCNX_ADMISSION
        .clone()
        .try_acquire_owned()
        .map_err(|_| AppError::ServiceUnavailable("NCNX receiver admission is full".into()))?;
    let permit = LOCAL_NCNX_IO_BUDGET
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| AppError::ServiceUnavailable("NCNX filesystem budget is closed".into()))?;
    tokio::task::spawn_blocking(move || {
        let _admission = admission;
        let _permit = permit;
        local_list(&runtime, &request)
    })
    .await
    .map_err(|error| AppError::InternalServerError(format!("NCNX list worker failed: {error}")))?
}

pub(super) fn local_list(
    runtime: &ClusterRuntime,
    request: &PeerNcnxRequest,
) -> Result<PeerNcnxResponse, AppError> {
    let pool = validate_pool_contract(runtime, request)?;
    let limit = request.limit.unwrap_or(200).clamp(1, 10_000);
    let mut names = BTreeMap::<String, PeerNcnxName>::new();
    for root in &runtime.local_storage_roots {
        let store = NcnxStore::new(&root.id, PathBuf::from(&root.path));
        let canonical_only = matches!(
            request.action,
            PeerNcnxAction::ListLive | PeerNcnxAction::ListTrash
        );
        let mut source_after = request.after.clone();
        let mut accepted_from_root = 0usize;

        // The physical scan limit is not the logical page limit: on a replica
        // root many entries can be non-canonical. Refill this root until it
        // contributes a complete logical page or its ordered stream ends.
        loop {
            let candidates = match request.action {
                PeerNcnxAction::ListLive | PeerNcnxAction::ListLiveAll => store.list_live_names(
                    &request.fragment_id,
                    &request.logical_key,
                    source_after.as_deref(),
                    limit,
                )?,
                PeerNcnxAction::ListTrash | PeerNcnxAction::ListTrashAll => store
                    .list_trash_names(
                        &request.fragment_id,
                        &request.logical_key,
                        source_after.as_deref(),
                        limit,
                    )?,
                _ => unreachable!(),
            };
            if candidates.is_empty() {
                break;
            }
            let source_exhausted = candidates.len() < limit;
            let next_source_after = candidates
                .last()
                .map(|item| ncnx_cursor(&item.logical_key, item.trash.as_ref()));

            for item in candidates {
                let layout =
                    build_ncnx_layout(runtime, pool, &request.fragment_id, &item.logical_key)?;
                let canonical = layout.canonical()?;
                if canonical_only
                    && (canonical.node_id != runtime.local_node_id || canonical.root_id != root.id)
                {
                    continue;
                }
                accepted_from_root = accepted_from_root.saturating_add(1);
                let cursor = ncnx_cursor(&item.logical_key, item.trash.as_ref());
                names.insert(
                    cursor,
                    PeerNcnxName {
                        logical_key: item.logical_key,
                        trash: item.trash,
                    },
                );
                if names.len() > limit {
                    names.pop_last();
                }
            }

            if source_exhausted || accepted_from_root >= limit {
                break;
            }
            if next_source_after == source_after {
                return Err(AppError::InternalServerError(
                    "NCNX listing source did not advance".into(),
                ));
            }
            source_after = next_source_after;
        }
    }
    Ok(PeerNcnxResponse {
        node_id: runtime.local_node_id.clone(),
        root_id: String::new(),
        changed: false,
        bytes: None,
        names: names.into_values().collect(),
    })
}

fn required_bytes(request: &PeerNcnxRequest) -> Result<&[u8], AppError> {
    request
        .bytes
        .as_ref()
        .map(|bytes| bytes.as_slice())
        .ok_or_else(|| AppError::BadRequest("NCNX operation requires bytes".into()))
}

fn required_trash(request: &PeerNcnxRequest) -> Result<&TrashLocator, AppError> {
    let locator = request
        .trash
        .as_ref()
        .ok_or_else(|| AppError::BadRequest("NCNX operation requires trash locator".into()))?;
    if validate_logical_key(&locator.logical_key)? != validate_logical_key(&request.logical_key)? {
        return Err(AppError::BadRequest(
            "NCNX trash locator differs from request key".into(),
        ));
    }
    Ok(locator)
}

async fn peer_handler(
    headers: HeaderMap,
    State(_state): State<Arc<AppState>>,
    Json(request): Json<PeerNcnxRequest>,
) -> Result<Json<PeerNcnxResponse>, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    let response = match request.action {
        PeerNcnxAction::ListLive
        | PeerNcnxAction::ListTrash
        | PeerNcnxAction::ListLiveAll
        | PeerNcnxAction::ListTrashAll => run_local_list(runtime, request).await?,
        _ => run_local_apply(runtime, request).await?,
    };
    Ok(Json(response))
}

async fn peer_batch_handler(
    headers: HeaderMap,
    State(_state): State<Arc<AppState>>,
    Json(request): Json<PeerNcnxBatchRequest>,
) -> Result<Json<PeerNcnxBatchResponse>, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    let response = run_local_batch(runtime, request).await?;
    Ok(Json(response))
}

pub fn build_ncnx_peer_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route(PEER_NCNX_PATH, post(peer_handler))
        .route(PEER_NCNX_BATCH_PATH, post(peer_batch_handler))
        .merge(lease::build_lease_peer_routes())
}
