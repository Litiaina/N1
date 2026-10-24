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
    let mut ownership_keys = Vec::new();
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
        ownership_keys.push((layout.clone(), logical_key.to_string()));
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
                    lease_token: None,
                });
        }
    }

    let authority_checks = pool_ids.iter().map(|pool_id| async {
        Ok::<_, AppError>((
            pool_id.clone(),
            pool_mutation_authority_nodes(&runtime, pool_id)
                .await?
                .into_iter()
                .map(|node| node.id)
                .collect::<BTreeSet<_>>(),
        ))
    });
    let mut authority_by_pool = BTreeMap::new();
    for result in join_all(authority_checks).await {
        let (pool_id, authority) = result?;
        authority_by_pool.insert(pool_id, authority);
    }
    if let Some(target) = groups.keys().find(|target| {
        !authority_by_pool
            .get(&target.pool_id)
            .is_some_and(|nodes| nodes.contains(&target.node_id))
    }) {
        return Err(AppError::ServiceUnavailable(format!(
            "authorized pool component cannot reach exact NCNX replica '{}' for rename batch",
            target.node_id
        )));
    }

    let ownership =
        stage::StageOwnership::acquire(runtime.clone(), &fragment_id, &ownership_keys).await?;
    for (target, entries) in &mut groups {
        ownership.attach_batch(target, entries)?;
    }
    let calls = groups.into_iter().map(|(target, entries)| {
        let runtime = runtime.clone();
        let action = action.clone();
        let fragment_id = fragment_id.clone();
        async move { send_batch_to_target(&runtime, &target, action, &fragment_id, entries).await }
    });
    let results = stage::join_calls(calls).await;
    ownership.release().await;
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
            "NCNX rename batch did not reach every authorized exact replica: {}",
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
    ownership: &stage::StageOwnership,
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
                    lease_token: None,
                });
        }
    }

    let authority_checks = pool_ids.iter().map(|pool_id| async {
        Ok::<_, AppError>((
            pool_id.clone(),
            pool_mutation_authority_nodes(&runtime, pool_id)
                .await?
                .into_iter()
                .map(|node| node.id)
                .collect::<BTreeSet<_>>(),
        ))
    });
    let mut authority_by_pool = BTreeMap::new();
    for result in join_all(authority_checks).await {
        let (pool_id, authority) = result?;
        authority_by_pool.insert(pool_id, authority);
    }
    if let Some(target) = groups.keys().find(|target| {
        !authority_by_pool
            .get(&target.pool_id)
            .is_some_and(|nodes| nodes.contains(&target.node_id))
    }) {
        return Err(AppError::ServiceUnavailable(format!(
            "authorized pool component cannot reach exact NCNX replica '{}' for namespace batch",
            target.node_id
        )));
    }

    for (target, entries) in &mut groups {
        ownership.attach_batch(target, entries)?;
    }
    let calls = groups.into_iter().map(|(target, entries)| {
        let runtime = runtime.clone();
        let action = action.clone();
        let fragment_id = fragment_id.clone();
        async move { send_batch_to_target(&runtime, &target, action, &fragment_id, entries).await }
    });
    let mut failures = Vec::new();
    let results = stage::join_calls(calls).await;
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
            "NCNX namespace batch did not reach every authorized exact replica: {}",
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
    move_trash_exact(&exact, false).await
}

/// Restores every immutable live pointer before removing any exact trash
/// generation. An exact-byte live destination is a retry, not a conflict.
pub async fn recover_trash_batch(items: &[NcnxRecoverBatchItem]) -> Result<(), AppError> {
    let exact = items
        .iter()
        .map(|item| (item.located.clone(), item.locator.clone()))
        .collect::<Vec<_>>();
    move_trash_exact(&exact, true).await
}

async fn move_trash_exact(
    items: &[(LocatedNcnx, TrashLocator)],
    recovering: bool,
) -> Result<(), AppError> {
    if items.is_empty() || items.len() > PEER_NCNX_BATCH_MAX {
        return Err(AppError::BadRequest("invalid namespace batch size".into()));
    }
    let runtime = active_cluster_runtime()?;
    let fragment = &items[0].0.record.fragment_id;
    let layouts = items
        .iter()
        .map(|(located, _)| {
            let pool = pool_for_located(&runtime, located)?;
            Ok((
                build_ncnx_layout(&runtime, pool, fragment, &located.logical_key)?,
                located.logical_key.clone(),
            ))
        })
        .collect::<Result<Vec<_>, AppError>>()?;
    // One owner spans destination publication AND source removal. Otherwise
    // opposite trash/recover operations could each remove the other's copy.
    let ownership = stage::StageOwnership::acquire(runtime.clone(), fragment, &layouts).await?;
    for ((located, locator), (layout, key)) in items.iter().zip(&layouts) {
        let live = read_from_layout_owned(
            &runtime,
            layout,
            fragment,
            key,
            None,
            true,
            Some(&ownership),
        )
        .await?;
        let trash = read_from_layout_owned(
            &runtime,
            layout,
            fragment,
            key,
            Some(locator),
            true,
            Some(&ownership),
        )
        .await?;
        if live
            .as_ref()
            .is_some_and(|(_, bytes)| bytes != &located.bytes)
            || trash
                .as_ref()
                .is_some_and(|(_, bytes)| bytes != &located.bytes)
        {
            return Err(AppError::Conflict(format!(
                "namespace generation changed for '{key}'"
            )));
        }
        if recovering && trash.is_none() || !recovering && live.is_none() {
            // Source absence only completes a retry when the exact destination
            // is already durable. Never resurrect an unobserved old pointer.
            let destination = if recovering { &live } else { &trash };
            if destination.is_none() {
                return Err(AppError::NotFound);
            }
        }
    }
    let (create, remove) = if recovering {
        (
            PeerNcnxBatchAction::CreateLive,
            PeerNcnxBatchAction::RemoveTrash,
        )
    } else {
        (
            PeerNcnxBatchAction::CreateTrash,
            PeerNcnxBatchAction::RemoveLive,
        )
    };
    apply_trash_move_batch_stage(items, create, &ownership).await?;
    let result = apply_trash_move_batch_stage(items, remove, &ownership).await;
    ownership.release().await;
    result
}
