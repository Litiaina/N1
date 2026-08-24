use super::*;

async fn apply_rename_batch_stage(
    items: &[NcnxRenameBatchItem],
    action: PeerNcnxBatchAction,
) -> Result<(), AppError> {
    if items.is_empty() || items.len() > PEER_NCNX_BATCH_MAX {
        return Err(AppError::BadRequest(format!(
            "rename batch requires 1..={PEER_NCNX_BATCH_MAX} entries"
        )));
    }
    let fragment_id = items[0].located.record.fragment_id.clone();
    let runtime = active_cluster_runtime()?;
    let mut pool_ids = BTreeSet::new();
    let mut groups = BTreeMap::<PeerNcnxBatchTarget, Vec<PeerNcnxBatchItem>>::new();
    for item in items {
        if item.located.record.fragment_id != fragment_id
            || item.destination_record.fragment_id != fragment_id
            || item.destination_record.object_id != item.located.record.object_id
        {
            return Err(AppError::Conflict(
                "rename batch must preserve one fragment and every object identity".into(),
            ));
        }
        let pool = pool_for_located(&runtime, &item.located)?;
        pool_ids.insert(pool.id.clone());
        let (logical_key, bytes) = match action {
            PeerNcnxBatchAction::CreateLive => (
                item.destination_key.as_str(),
                item.destination_record.encode()?,
            ),
            PeerNcnxBatchAction::RemoveLive => (
                item.located.logical_key.as_str(),
                item.located.bytes.clone(),
            ),
            PeerNcnxBatchAction::CreateTrash | PeerNcnxBatchAction::RemoveTrash => {
                return Err(AppError::InternalServerError(
                    "trash action passed to rename batch".into(),
                ));
            }
        };
        let layout = build_ncnx_layout(&runtime, pool, &fragment_id, logical_key)?;
        for replica in layout.replicas {
            groups
                .entry(PeerNcnxBatchTarget {
                    pool_id: layout.pool_id.clone(),
                    pool_generation: layout.pool_generation,
                    node_id: replica.node_id,
                    instance_id: replica.instance_id,
                    binding_epoch: replica.binding_epoch,
                    root_id: replica.root_id,
                    endpoint: replica.endpoint,
                })
                .or_default()
                .push(PeerNcnxBatchItem {
                    logical_key: logical_key.to_string(),
                    trash: None,
                    bytes: ByteBuf::from(bytes.clone()),
                });
        }
    }

    let authority_checks = pool_ids
        .iter()
        .map(|pool_id| exact_pool_mutation_nodes(&runtime, pool_id));
    for result in join_all(authority_checks).await {
        result?;
    }

    let calls = groups.into_iter().map(|(target, entries)| {
        let runtime = runtime.clone();
        let action = action.clone();
        let fragment_id = fragment_id.clone();
        async move { send_batch_to_target(&runtime, &target, action, &fragment_id, entries).await }
    });
    let results = join_all(calls).await;
    let mut failures = Vec::new();
    for result in results {
        match result {
            Ok(_) => {}
            Err(error @ AppError::Conflict(_)) => return Err(error),
            Err(error) => failures.push(error.to_string()),
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(AppError::ServiceUnavailable(format!(
            "NCNX rename batch did not reach every exact replica: {}",
            failures.join("; ")
        )))
    }
}

/// Fast path for a bounded prefix-rename slice. Destination replicas become
/// durable before any exact source pointer is removed.
pub async fn rename_live_batch(items: &[NcnxRenameBatchItem]) -> Result<(), AppError> {
    apply_rename_batch_stage(items, PeerNcnxBatchAction::CreateLive).await?;
    apply_rename_batch_stage(items, PeerNcnxBatchAction::RemoveLive).await
}

async fn apply_trash_move_batch_stage(
    items: &[(LocatedNcnx, TrashLocator)],
    action: PeerNcnxBatchAction,
) -> Result<(), AppError> {
    if items.is_empty() || items.len() > PEER_NCNX_BATCH_MAX {
        return Err(AppError::BadRequest(format!(
            "namespace batch requires 1..={PEER_NCNX_BATCH_MAX} entries"
        )));
    }
    let fragment_id = items[0].0.record.fragment_id.clone();
    let runtime = active_cluster_runtime()?;
    let mut identities = BTreeSet::new();
    let mut pool_ids = BTreeSet::new();
    let mut groups = BTreeMap::<PeerNcnxBatchTarget, Vec<PeerNcnxBatchItem>>::new();
    for (located, locator) in items {
        if located.record.fragment_id != fragment_id || locator.logical_key != located.logical_key {
            return Err(AppError::Conflict(
                "namespace batch must preserve fragment, key, and object identity".into(),
            ));
        }
        let identity = ncnx_cursor(&located.logical_key, Some(locator));
        if !identities.insert(identity) {
            return Err(AppError::Conflict(format!(
                "duplicate namespace generation for '{}' in one batch",
                located.logical_key
            )));
        }
        let pool = pool_for_located(&runtime, located)?;
        pool_ids.insert(pool.id.clone());
        let layout = build_ncnx_layout(&runtime, pool, &fragment_id, &located.logical_key)?;
        let trash = matches!(
            action,
            PeerNcnxBatchAction::CreateTrash | PeerNcnxBatchAction::RemoveTrash
        )
        .then(|| locator.clone());
        for replica in layout.replicas {
            groups
                .entry(PeerNcnxBatchTarget {
                    pool_id: layout.pool_id.clone(),
                    pool_generation: layout.pool_generation,
                    node_id: replica.node_id,
                    instance_id: replica.instance_id,
                    binding_epoch: replica.binding_epoch,
                    root_id: replica.root_id,
                    endpoint: replica.endpoint,
                })
                .or_default()
                .push(PeerNcnxBatchItem {
                    logical_key: located.logical_key.clone(),
                    trash: trash.clone(),
                    bytes: ByteBuf::from(located.bytes.clone()),
                });
        }
    }

    let authority_checks = pool_ids
        .iter()
        .map(|pool_id| exact_pool_mutation_nodes(&runtime, pool_id));
    for result in join_all(authority_checks).await {
        result?;
    }

    let calls = groups.into_iter().map(|(target, entries)| {
        let runtime = runtime.clone();
        let action = action.clone();
        let fragment_id = fragment_id.clone();
        async move { send_batch_to_target(&runtime, &target, action, &fragment_id, entries).await }
    });
    let mut failures = Vec::new();
    for result in join_all(calls).await {
        match result {
            Ok(_) => {}
            Err(error @ AppError::Conflict(_)) => return Err(error),
            Err(error) => failures.push(error.to_string()),
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(AppError::ServiceUnavailable(format!(
            "NCNX namespace batch did not reach every exact replica: {}",
            failures.join("; ")
        )))
    }
}

/// Publishes every immutable trash generation before removing any matching
/// live pointer. A retry is byte-idempotent and can safely converge a partial
/// destination-first operation.
pub async fn trash_live_batch(items: &[NcnxTrashBatchItem]) -> Result<(), AppError> {
    let exact = items
        .iter()
        .map(|item| (item.located.clone(), item.locator.clone()))
        .collect::<Vec<_>>();
    apply_trash_move_batch_stage(&exact, PeerNcnxBatchAction::CreateTrash).await?;
    apply_trash_move_batch_stage(&exact, PeerNcnxBatchAction::RemoveLive).await
}

/// Restores every immutable live pointer before removing any exact trash
/// generation. Existing live content remains a strict conflict.
pub async fn recover_trash_batch(items: &[NcnxRecoverBatchItem]) -> Result<(), AppError> {
    let exact = items
        .iter()
        .map(|item| (item.located.clone(), item.locator.clone()))
        .collect::<Vec<_>>();
    apply_trash_move_batch_stage(&exact, PeerNcnxBatchAction::CreateLive).await?;
    apply_trash_move_batch_stage(&exact, PeerNcnxBatchAction::RemoveTrash).await
}
