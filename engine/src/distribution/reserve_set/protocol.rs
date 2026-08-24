use super::*;

pub(super) async fn failed_node_is_unreachable(
    runtime: &ClusterRuntime,
    failed: &ClusterNode,
) -> bool {
    !cluster_node_is_reachable(&runtime.local_instance_id, failed).await
}

pub(super) async fn local_prepare_vote(
    request: &PeerReservePrepareRequest,
) -> Result<PeerReservePrepareResponse, AppError> {
    let runtime = active_cluster_runtime()?;
    if runtime.membership.cluster_id != request.cluster_id
        || runtime.membership.epoch != request.topology_epoch
        || runtime.membership.binding_epoch != request.binding_epoch
    {
        return Err(AppError::Conflict(
            "reserve prepare does not match local topology/binding epoch".into(),
        ));
    }
    let failed = runtime
        .membership
        .nodes
        .iter()
        .find(|node| node.id == request.slot_id)
        .ok_or(AppError::NotFound)?;
    if failed.instance_id != request.failed_instance_id {
        return Err(AppError::Conflict(
            "reserve prepare identifies a stale failed instance".into(),
        ));
    }
    let reserve = runtime
        .membership
        .reserve(&request.reserve_instance_id)
        .ok_or_else(|| AppError::Conflict("reserve candidate is no longer unassigned".into()))?;
    compatible_reserve(failed, reserve)?;
    let observed_failed_unreachable = failed_node_is_unreachable(&runtime, failed).await;
    if !observed_failed_unreachable {
        return Err(AppError::Conflict(format!(
            "logical slot '{}' is still reachable from physical instance '{}'",
            request.slot_id, runtime.local_instance_id
        )));
    }
    Ok(PeerReservePrepareResponse {
        voter_slot_id: runtime.local_node_id.clone(),
        voter_instance_id: runtime.local_instance_id.clone(),
        accepted: true,
        observed_failed_unreachable,
        plan_id: request.plan_id.clone(),
    })
}

pub(super) async fn post_prepare(
    endpoint: &str,
    request: &PeerReservePrepareRequest,
) -> Result<PeerReservePrepareResponse, AppError> {
    let client = peer_http_client()?;
    let mut req = client
        .post(format!(
            "{}/n2/peer/reserve/prepare",
            endpoint.trim_end_matches('/')
        ))
        .json(request);
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(
        Duration::from_secs(CONFIG.reserve_set.promotion_timeout_seconds),
        req.send(),
    )
    .await
    .map_err(|_| AppError::ServiceUnavailable("reserve prepare timed out".into()))?
    .map_err(|error| {
        AppError::ServiceUnavailable(format!("reserve prepare request failed: {error}"))
    })?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "reserve prepare returned {status}: {body}"
        )));
    }
    response
        .json()
        .await
        .map_err(|error| AppError::InternalServerError(format!("decode reserve vote: {error}")))
}

pub(super) async fn local_commit(
    request: &PeerReserveCommitRequest,
) -> Result<PeerReserveCommitResponse, AppError> {
    let current = active_cluster_runtime()?;
    if current.membership.binding_epoch == request.membership.binding_epoch {
        let current_bytes = serde_json::to_vec(&current.membership).ok();
        let requested_bytes = serde_json::to_vec(&request.membership).ok();
        if current_bytes == requested_bytes {
            return Ok(PeerReserveCommitResponse {
                local_instance_id: current.local_instance_id.clone(),
                binding_epoch: current.membership.binding_epoch,
                committed: true,
                plan_id: request.plan_id.clone(),
            });
        }
    }
    if current.membership.binding_epoch != request.previous_binding_epoch {
        return Err(AppError::Conflict(format!(
            "local binding epoch {} cannot commit transition from {}",
            current.membership.binding_epoch, request.previous_binding_epoch
        )));
    }
    validate_reserve_transition(&current.membership, &request.membership)?;
    let local_instance_id = current.local_instance_id.clone();
    let next = commit_local_membership(request.membership.clone())?;
    Ok(PeerReserveCommitResponse {
        local_instance_id,
        binding_epoch: next.membership.binding_epoch,
        committed: true,
        plan_id: request.plan_id.clone(),
    })
}

pub(super) async fn post_commit(
    endpoint: &str,
    request: &PeerReserveCommitRequest,
) -> Result<PeerReserveCommitResponse, AppError> {
    let client = peer_http_client()?;
    let mut req = client
        .post(format!(
            "{}/n2/peer/reserve/commit",
            endpoint.trim_end_matches('/')
        ))
        .json(request);
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(
        Duration::from_secs(CONFIG.reserve_set.promotion_timeout_seconds),
        req.send(),
    )
    .await
    .map_err(|_| AppError::ServiceUnavailable("reserve commit timed out".into()))?
    .map_err(|error| {
        AppError::ServiceUnavailable(format!("reserve commit request failed: {error}"))
    })?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "reserve commit returned {status}: {body}"
        )));
    }
    response
        .json()
        .await
        .map_err(|error| AppError::InternalServerError(format!("decode reserve commit: {error}")))
}

pub(super) async fn local_activate(
    request: &PeerReserveActivateRequest,
) -> Result<PeerReserveCommitResponse, AppError> {
    let current = active_cluster_runtime()?;
    if serde_json::to_vec(&current.membership).ok() == serde_json::to_vec(&request.membership).ok()
    {
        return Ok(PeerReserveCommitResponse {
            local_instance_id: current.local_instance_id.clone(),
            binding_epoch: current.membership.binding_epoch,
            committed: true,
            plan_id: request.plan_id.clone(),
        });
    }
    validate_activation_transition(&current.membership, &request.membership)?;
    let local_instance_id = current.local_instance_id.clone();
    let runtime = commit_local_membership(request.membership.clone())?;
    Ok(PeerReserveCommitResponse {
        local_instance_id,
        binding_epoch: runtime.membership.binding_epoch,
        committed: true,
        plan_id: request.plan_id.clone(),
    })
}

pub(super) async fn post_activate(
    endpoint: &str,
    request: &PeerReserveActivateRequest,
) -> Result<PeerReserveCommitResponse, AppError> {
    let client = peer_http_client()?;
    let mut req = client
        .post(format!(
            "{}/n2/peer/reserve/activate",
            endpoint.trim_end_matches('/')
        ))
        .json(request);
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(
        Duration::from_secs(CONFIG.reserve_set.promotion_timeout_seconds),
        req.send(),
    )
    .await
    .map_err(|_| AppError::ServiceUnavailable("reserve activation timed out".into()))?
    .map_err(|error| {
        AppError::ServiceUnavailable(format!("reserve activation request failed: {error}"))
    })?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "reserve activation returned {status}: {body}"
        )));
    }
    response.json().await.map_err(|error| {
        AppError::InternalServerError(format!("decode reserve activation: {error}"))
    })
}

pub(super) async fn post_replacement_healing(
    endpoint: &str,
) -> Result<AutoHealingStatus, AppError> {
    let client = peer_http_client()?;
    let mut req = client.post(format!(
        "{}/n2/peer/healing/replacement-cycle",
        endpoint.trim_end_matches('/')
    ));
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(
        Duration::from_secs(CONFIG.auto_healing.max_duration_seconds.saturating_add(30)),
        req.send(),
    )
    .await
    .map_err(|_| AppError::ServiceUnavailable("replacement healing timed out".into()))?
    .map_err(|error| {
        AppError::ServiceUnavailable(format!("replacement healing request failed: {error}"))
    })?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "replacement healing returned {status}: {body}"
        )));
    }
    response.json().await.map_err(|error| {
        AppError::InternalServerError(format!("decode replacement healing status: {error}"))
    })
}

pub(super) async fn post_fragment_sync(endpoint: &str) -> Result<(), AppError> {
    let client = peer_http_client()?;
    let mut req = client.post(format!(
        "{}/n2/peer/fragments/sync",
        endpoint.trim_end_matches('/')
    ));
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(Duration::from_secs(30), req.send())
        .await
        .map_err(|_| AppError::ServiceUnavailable("fragment synchronization timed out".into()))?
        .map_err(|error| {
            AppError::ServiceUnavailable(format!(
                "fragment synchronization request failed: {error}"
            ))
        })?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "fragment synchronization returned {status}: {body}"
        )));
    }
    Ok(())
}

pub(super) async fn run_replacement_recovery(
    plan_id: String,
    slot_id: String,
) -> Result<(), AppError> {
    // Promotion changes only a physical binding. Make every replacement
    // participant observe the existing immutable fragment controls before any
    // recorded-layout healing is allowed to declare completion.
    let runtime = active_cluster_runtime()?;
    let sync_results = join_all(runtime.membership.nodes.iter().map(|node| {
        let node = node.clone();
        let runtime = runtime.clone();
        async move {
            if node.instance_id == runtime.local_instance_id {
                sync_fragment_controls_from_cluster().await.map(|_| ())
            } else {
                post_fragment_sync(&node.endpoint).await
            }
        }
    }))
    .await;
    for result in sync_results {
        result?;
    }

    // Each bounded cycle resumes from persisted per-fragment continuation.
    // This loop yields between rounds and has no healthy-path request cost.
    for _round in 0..1_024u32 {
        let runtime = active_cluster_runtime()?;
        let nodes = runtime.membership.nodes.clone();
        let results = join_all(nodes.iter().map(|node| {
            let node = node.clone();
            let runtime = runtime.clone();
            async move {
                if node.instance_id == runtime.local_instance_id {
                    run_replacement_healing_cycle().await
                } else {
                    post_replacement_healing(&node.endpoint).await
                }
            }
        }))
        .await;
        let mut incomplete = 0u64;
        let mut busy = false;
        for result in results {
            let status = match result {
                Ok(status) => status,
                Err(AppError::Conflict(message))
                    if message.contains("auto-healing cycle is already running") =>
                {
                    busy = true;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if status.safety_read_only
                || status.unrecoverable_objects > 0
                || status.failed_objects > 0
                || status.last_error.is_some()
            {
                return Err(AppError::ServiceUnavailable(format!(
                    "replacement healing for slot '{}' did not prove a safe complete layout",
                    slot_id
                )));
            }
            incomplete = incomplete.saturating_add(status.incomplete_fragments);
        }
        if !busy && incomplete == 0 {
            let runtime = active_cluster_runtime()?;
            let activated = activation_membership(&runtime.membership, &slot_id)?;
            let request = PeerReserveActivateRequest {
                plan_id: plan_id.clone(),
                membership: activated.clone(),
            };
            // Remote participants first, local publication last.
            for node in &runtime.membership.nodes {
                if node.instance_id != runtime.local_instance_id {
                    post_activate(&node.endpoint, &request).await?;
                }
            }
            local_activate(&request).await?;
            return Ok(());
        }
        sleep(Duration::from_millis(if busy { 250 } else { 100 })).await;
    }
    Err(AppError::ServiceUnavailable(format!(
        "replacement healing for slot '{}' exceeded 1024 bounded resume cycles",
        slot_id
    )))
}

pub(super) async fn fetch_peer_membership(
    endpoint: &str,
) -> Result<PeerReserveMembershipResponse, AppError> {
    let client = peer_http_client()?;
    let mut req = client.get(format!(
        "{}/n2/peer/reserve/membership",
        endpoint.trim_end_matches('/')
    ));
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(Duration::from_secs(5), req.send())
        .await
        .map_err(|_| AppError::ServiceUnavailable("reserve membership query timed out".into()))?
        .map_err(|error| {
            AppError::ServiceUnavailable(format!("reserve membership query failed: {error}"))
        })?;
    if !response.status().is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "reserve membership query returned {}",
            response.status()
        )));
    }
    response.json().await.map_err(|error| {
        AppError::InternalServerError(format!("decode reserve membership: {error}"))
    })
}

pub(super) async fn reconcile_binding_epoch() -> Result<(), AppError> {
    let runtime = active_cluster_runtime()?;
    let mut endpoints = runtime
        .membership
        .nodes
        .iter()
        .filter(|node| node.instance_id != runtime.local_instance_id)
        .map(|node| node.endpoint.clone())
        .chain(
            runtime
                .membership
                .reserve_nodes
                .iter()
                .filter(|node| node.instance_id != runtime.local_instance_id)
                .map(|node| node.endpoint.clone()),
        )
        .collect::<Vec<_>>();
    endpoints.sort();
    endpoints.dedup();
    let observations = join_all(endpoints.iter().map(|endpoint| {
        let endpoint = endpoint.clone();
        async move { (endpoint.clone(), fetch_peer_membership(&endpoint).await) }
    }))
    .await;

    let mut newest = runtime.membership.clone();
    for (_, observation) in &observations {
        let Ok(observation) = observation else {
            continue;
        };
        observation.membership.validate()?;
        if observation.membership.cluster_id != runtime.membership.cluster_id
            || observation.membership.epoch != runtime.membership.epoch
        {
            continue;
        }
        if observation.membership.binding_epoch > newest.binding_epoch {
            newest = observation.membership.clone();
        }
    }
    if newest.binding_epoch == runtime.membership.binding_epoch.saturating_add(1) {
        validate_reserve_transition(&runtime.membership, &newest)?;
        commit_local_membership(newest)?;
        return Ok(());
    }
    if newest.binding_epoch > runtime.membership.binding_epoch.saturating_add(1) {
        return Err(AppError::Conflict(format!(
            "local binding epoch {} is more than one transition behind observed epoch {}; operator replay is required",
            runtime.membership.binding_epoch, newest.binding_epoch
        )));
    }

    // A node that already committed the newest epoch repairs any reachable
    // one-step survivor left behind by a coordinator crash.
    for (endpoint, observation) in observations {
        let Ok(observation) = observation else {
            continue;
        };
        if observation.membership.cluster_id != runtime.membership.cluster_id
            || observation.membership.epoch != runtime.membership.epoch
            || observation.membership.binding_epoch.saturating_add(1)
                != runtime.membership.binding_epoch
        {
            continue;
        }
        validate_reserve_transition(&observation.membership, &runtime.membership)?;
        let request = PeerReserveCommitRequest {
            previous_binding_epoch: observation.membership.binding_epoch,
            plan_id: format!(
                "reconcile-{}-{}",
                runtime.membership.binding_epoch, observation.local_instance_id
            ),
            membership: runtime.membership.clone(),
        };
        let _ = post_commit(&endpoint, &request).await?;
    }
    Ok(())
}

pub(super) async fn write_report(report: &ReservePromotionReport) -> Result<(), AppError> {
    let runtime = active_cluster_runtime()?;
    let root = runtime
        .membership_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(".reports")
        .join("reserve-set");
    fs::create_dir_all(&root)
        .await
        .map_err(|error| AppError::Io(error, format!("create {}", root.display())))?;
    let path = root.join(format!("{}.json", report.plan_id));
    let temp = root.join(format!("{}.tmp", report.plan_id));
    let bytes = serde_json::to_vec_pretty(report).map_err(|error| {
        AppError::InternalServerError(format!("encode reserve promotion report: {error}"))
    })?;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp)
        .await
        .map_err(|error| AppError::Io(error, format!("write {}", temp.display())))?;
    file.write_all(&bytes)
        .await
        .map_err(|error| AppError::Io(error, format!("write {}", temp.display())))?;
    file.sync_all()
        .await
        .map_err(|error| AppError::Io(error, format!("sync {}", temp.display())))?;
    drop(file);
    fs::rename(&temp, &path)
        .await
        .map_err(|error| AppError::Io(error, format!("publish {}", path.display())))
}
