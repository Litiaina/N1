use super::*;

pub(super) async fn execute_promotion(
    slot_id: &str,
    reserve_instance_id: &str,
) -> Result<ReserveSetStatus, AppError> {
    if PROMOTION_RUNNING
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err(AppError::Conflict(
            "another reserve promotion is already running".into(),
        ));
    }
    let _guard = PromotionGuard;
    let runtime = active_cluster_runtime()?;
    if runtime.local_is_reserve {
        return Err(AppError::Forbidden(
            "an unassigned reserve cannot coordinate promotion".into(),
        ));
    }
    let pool = runtime
        .membership
        .pool_for_node(slot_id)
        .ok_or(AppError::NotFound)?;
    let failed = runtime
        .membership
        .nodes
        .iter()
        .find(|node| node.id == slot_id)
        .cloned()
        .ok_or(AppError::NotFound)?;
    let reserve = runtime
        .membership
        .reserve(reserve_instance_id)
        .cloned()
        .ok_or_else(|| AppError::Conflict("reserve candidate is not unassigned".into()))?;
    compatible_reserve(&failed, &reserve)?;
    if !failed_node_is_unreachable(&runtime, &failed).await {
        return Err(AppError::Conflict(format!(
            "logical slot '{slot_id}' is reachable; promotion refused"
        )));
    }

    let surviving = runtime
        .membership
        .nodes_for_pool(&pool.id)
        .into_iter()
        .filter(|node| node.id != slot_id)
        .collect::<Vec<_>>();
    let live = join_all(
        surviving
            .iter()
            .map(|node| cluster_node_is_reachable(&runtime.local_instance_id, node)),
    )
    .await;
    if live.iter().any(|reachable| !reachable) {
        return Err(AppError::ServiceUnavailable(format!(
            "pool '{}' has more than one unreachable active slot; autonomous fencing is unsafe",
            pool.id
        )));
    }
    if !cluster_node_is_reachable(&runtime.local_instance_id, &reserve_probe_node(&reserve)).await {
        return Err(AppError::ServiceUnavailable(format!(
            "reserve instance '{}' is unreachable",
            reserve.instance_id
        )));
    }
    let mut coordinators = surviving
        .iter()
        .map(|node| node.id.as_str())
        .collect::<Vec<_>>();
    coordinators.sort_unstable();
    if coordinators.first().copied() != Some(runtime.local_node_id.as_str()) {
        return Err(AppError::Conflict(format!(
            "logical slot '{}' is not deterministic reserve coordinator '{}'",
            runtime.local_node_id,
            coordinators.first().copied().unwrap_or("")
        )));
    }

    let plan_id = promotion_plan_id(&runtime.membership, slot_id, reserve_instance_id)?;
    {
        let mut status = STATUS.write().await;
        status.running = true;
        status.last_started_at_unix = Some(Utc::now().timestamp());
        status.last_plan_id = Some(plan_id.clone());
        status.last_error = None;
    }
    let prepare = PeerReservePrepareRequest {
        cluster_id: runtime.membership.cluster_id.clone(),
        topology_epoch: runtime.membership.epoch,
        binding_epoch: runtime.membership.binding_epoch,
        slot_id: slot_id.to_string(),
        failed_instance_id: failed.instance_id.clone(),
        reserve_instance_id: reserve_instance_id.to_string(),
        plan_id: plan_id.clone(),
    };

    let mut votes = Vec::with_capacity(surviving.len() + 1);
    for voter in &surviving {
        let vote = if voter.instance_id == runtime.local_instance_id {
            local_prepare_vote(&prepare).await?
        } else {
            post_prepare(&voter.endpoint, &prepare).await?
        };
        if !vote.accepted || !vote.observed_failed_unreachable || vote.plan_id != plan_id {
            return Err(AppError::Conflict(format!(
                "reserve fencing vote from '{}' was not affirmative",
                voter.id
            )));
        }
        votes.push(vote.voter_instance_id);
    }
    let reserve_vote = post_prepare(&reserve.endpoint, &prepare).await?;
    if !reserve_vote.accepted
        || !reserve_vote.observed_failed_unreachable
        || reserve_vote.plan_id != plan_id
    {
        return Err(AppError::Conflict(
            "reserve candidate did not affirm the fencing observation".into(),
        ));
    }
    votes.push(reserve_vote.voter_instance_id);

    let next = plan_reserve_promotion(&runtime.membership, slot_id, reserve_instance_id)?;
    let commit = PeerReserveCommitRequest {
        previous_binding_epoch: runtime.membership.binding_epoch,
        plan_id: plan_id.clone(),
        membership: next.clone(),
    };

    // Commit the replacement first so the new logical participant is ready,
    // then commit each surviving exact-pool participant. All commits are
    // idempotent and carry the same complete membership snapshot.
    let reserve_commit = post_commit(&reserve.endpoint, &commit).await?;
    if !reserve_commit.committed {
        return Err(AppError::ServiceUnavailable(
            "reserve instance did not commit its new binding".into(),
        ));
    }
    let mut committed = vec![reserve_commit.local_instance_id];
    for voter in &surviving {
        if voter.instance_id == runtime.local_instance_id {
            continue;
        }
        let outcome = post_commit(&voter.endpoint, &commit).await?;
        if !outcome.committed || outcome.binding_epoch != next.binding_epoch {
            return Err(AppError::ServiceUnavailable(format!(
                "surviving instance '{}' did not commit binding epoch {}",
                voter.instance_id, next.binding_epoch
            )));
        }
        committed.push(outcome.local_instance_id);
    }
    for voter in &surviving {
        if voter.instance_id != runtime.local_instance_id {
            continue;
        }
        let outcome = local_commit(&commit).await?;
        if !outcome.committed || outcome.binding_epoch != next.binding_epoch {
            return Err(AppError::ServiceUnavailable(format!(
                "surviving instance '{}' did not commit binding epoch {}",
                voter.instance_id, next.binding_epoch
            )));
        }
        committed.push(outcome.local_instance_id);
    }

    let report = ReservePromotionReport {
        plan_id: plan_id.clone(),
        slot_id: slot_id.to_string(),
        retired_instance_id: failed.instance_id,
        promoted_instance_id: reserve_instance_id.to_string(),
        topology_epoch: next.epoch,
        previous_binding_epoch: runtime.membership.binding_epoch,
        committed_binding_epoch: next.binding_epoch,
        committed_voters: committed,
        healing_state: "recovering_via_autonomous_ncnx_ncdx_and_shard_healing".into(),
        finished_at_unix: Utc::now().timestamp(),
    };
    write_report(&report).await?;
    let mut status = status_snapshot().await;
    status.running = true;
    status.binding_epoch = next.binding_epoch;
    status.last_promoted_slot = Some(slot_id.to_string());
    status.last_promoted_instance = Some(reserve_instance_id.to_string());
    status.last_finished_at_unix = Some(report.finished_at_unix);
    status.last_error = None;
    *STATUS.write().await = status.clone();
    let recovery_plan = plan_id.clone();
    let recovery_slot = slot_id.to_string();
    tokio::spawn(async move {
        // execute_promotion still owns the guard until it returns. Acquire it
        // immediately afterward so no periodic worker can duplicate recovery.
        loop {
            if PROMOTION_RUNNING
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        let _guard = PromotionGuard;
        let result = run_replacement_recovery(recovery_plan, recovery_slot).await;
        let mut status = STATUS.write().await;
        status.running = false;
        status.last_finished_at_unix = Some(Utc::now().timestamp());
        if let Err(error) = result {
            status.last_error = Some(error.to_string());
        }
    });
    Ok(status)
}

pub(super) async fn status_snapshot() -> ReserveSetStatus {
    let runtime = active_cluster_runtime().ok();
    let previous = STATUS.read().await.clone();
    let mut status = previous;
    status.enabled = CONFIG.reserve_set.enabled;
    status.automatic_promotion = CONFIG.reserve_set.automatic_promotion;
    if let Some(runtime) = runtime {
        status.topology_epoch = runtime.membership.epoch;
        status.binding_epoch = runtime.membership.binding_epoch;
        status.active_slots = runtime.membership.nodes.len();
        status.reserve_instances = runtime.membership.reserve_nodes.len();
        status.recovering_slots = runtime
            .membership
            .nodes
            .iter()
            .filter(|node| node.effective_state() == NodeState::Degraded)
            .map(|node| node.id.clone())
            .collect();
    }
    status
}

pub(super) async fn automatic_cycle() -> Result<(), AppError> {
    reconcile_binding_epoch().await?;
    let runtime = active_cluster_runtime()?;
    if runtime.local_is_reserve {
        return Ok(());
    }

    // A committed degraded binding is durable recovery state. If any process
    // restarted after commit but before activation, the deterministic lowest
    // logical slot resumes the bounded, persisted healing pass.
    if let Some(recovering) = runtime
        .membership
        .nodes
        .iter()
        .find(|node| node.effective_state() == NodeState::Degraded)
    {
        let mut coordinators = runtime
            .membership
            .nodes
            .iter()
            .filter(|node| node.id != recovering.id)
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>();
        coordinators.sort_unstable();
        if coordinators.first().copied() == Some(runtime.local_node_id.as_str())
            && PROMOTION_RUNNING
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            let _guard = PromotionGuard;
            {
                let mut status = STATUS.write().await;
                status.running = true;
                status.last_started_at_unix = Some(Utc::now().timestamp());
                status.last_error = None;
            }
            let result = run_replacement_recovery(
                format!(
                    "resume-binding-{}-{}",
                    runtime.membership.binding_epoch, recovering.id
                ),
                recovering.id.clone(),
            )
            .await;
            let mut status = STATUS.write().await;
            status.running = false;
            status.last_finished_at_unix = Some(Utc::now().timestamp());
            if let Err(error) = result {
                status.last_error = Some(error.to_string());
                return Ok(());
            }
        }
        return Ok(());
    }
    if runtime.membership.reserve_nodes.is_empty() {
        return Ok(());
    }
    let mut unavailable = Vec::new();
    for pool in &runtime.membership.pools {
        for node in runtime.membership.nodes_for_pool(&pool.id) {
            if !cluster_node_is_reachable(&runtime.local_instance_id, &node).await {
                unavailable.push(node);
            }
        }
    }
    let mut counts = FAILURE_COUNTS.lock().await;
    let unavailable_ids = unavailable
        .iter()
        .map(|node| node.id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    counts.retain(|slot, _| unavailable_ids.contains(slot));
    for node in &unavailable {
        let entry = counts.entry(node.id.clone()).or_default();
        *entry = entry.saturating_add(1);
    }
    let suspects = counts
        .iter()
        .filter(|(_, count)| **count >= CONFIG.reserve_set.failure_threshold)
        .map(|(slot, _)| slot.clone())
        .collect::<Vec<_>>();
    drop(counts);
    STATUS.write().await.suspected_slots = suspects.clone();

    for slot_id in suspects {
        let runtime = active_cluster_runtime()?;
        let Some(pool) = runtime.membership.pool_for_node(&slot_id) else {
            continue;
        };
        let mut survivors = runtime
            .membership
            .nodes_for_pool(&pool.id)
            .into_iter()
            .filter(|node| node.id != slot_id)
            .collect::<Vec<_>>();
        survivors.sort_by(|left, right| left.id.cmp(&right.id));
        if survivors.first().map(|node| node.id.as_str()) != Some(runtime.local_node_id.as_str()) {
            continue;
        }
        let Some(reserve) = runtime
            .membership
            .reserve_nodes
            .iter()
            .filter(|reserve| {
                runtime
                    .membership
                    .nodes
                    .iter()
                    .find(|node| node.id == slot_id)
                    .is_some_and(|failed| compatible_reserve(failed, reserve).is_ok())
            })
            .min_by(|left, right| left.instance_id.cmp(&right.instance_id))
        else {
            continue;
        };
        execute_promotion(&slot_id, &reserve.instance_id).await?;
        FAILURE_COUNTS.lock().await.remove(&slot_id);
        break;
    }
    Ok(())
}
