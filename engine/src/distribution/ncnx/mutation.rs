use super::*;

pub(super) fn request_for_replica(
    action: PeerNcnxAction,
    layout: &NcnxLayout,
    replica: &NcnxReplica,
    fragment_id: &str,
    logical_key: &str,
    trash: Option<TrashLocator>,
    bytes: Option<&[u8]>,
) -> PeerNcnxRequest {
    PeerNcnxRequest {
        action,
        pool_id: layout.pool_id.clone(),
        pool_generation: layout.pool_generation,
        target_slot_id: replica.node_id.clone(),
        target_instance_id: replica.instance_id.clone(),
        binding_epoch: replica.binding_epoch,
        fragment_id: fragment_id.to_string(),
        logical_key: logical_key.to_string(),
        root_id: replica.root_id.clone(),
        trash,
        bytes: bytes.map(|bytes| ByteBuf::from(bytes.to_vec())),
        after: None,
        limit: None,
        lease_token: None,
    }
}

pub(super) async fn read_from_layout(
    runtime: &ClusterRuntime,
    layout: &NcnxLayout,
    fragment_id: &str,
    logical_key: &str,
    trash: Option<&TrashLocator>,
    require_complete_absence: bool,
) -> Result<Option<(NcnxRecord, Vec<u8>)>, AppError> {
    read_from_layout_owned(
        runtime,
        layout,
        fragment_id,
        logical_key,
        trash,
        require_complete_absence,
        None,
    )
    .await
}

pub(super) async fn read_from_layout_owned(
    runtime: &ClusterRuntime,
    layout: &NcnxLayout,
    fragment_id: &str,
    logical_key: &str,
    trash: Option<&TrashLocator>,
    require_complete_absence: bool,
    ownership: Option<&stage::StageOwnership>,
) -> Result<Option<(NcnxRecord, Vec<u8>)>, AppError> {
    let action = if trash.is_some() {
        PeerNcnxAction::ReadTrash
    } else {
        PeerNcnxAction::ReadLive
    };
    let calls = layout.replicas.iter().map(|replica| {
        let mut request = request_for_replica(
            action.clone(),
            layout,
            replica,
            fragment_id,
            logical_key,
            trash.cloned(),
            None,
        );
        let attached = ownership
            .map(|ownership| ownership.attach(&mut request, layout, replica))
            .transpose();
        async move {
            attached?;
            send_to_replica(runtime, replica, request).await
        }
    });
    let observations = join_all(calls).await;
    let mut valid = BTreeMap::<Vec<u8>, NcnxRecord>::new();
    let mut successful_responses = 0usize;
    let mut invalid_responses = 0usize;
    for response in observations.into_iter().flatten() {
        successful_responses += 1;
        if let Some(bytes) = response.bytes {
            let bytes = bytes.into_vec();
            let Ok(record) = NcnxRecord::decode(&bytes) else {
                invalid_responses += 1;
                continue;
            };
            if record.fragment_id != fragment_id {
                invalid_responses += 1;
                continue;
            }
            valid.entry(bytes).or_insert(record);
        }
    }
    if valid.len() > 1 {
        return Err(AppError::Conflict(format!(
            "NCNX replicas disagree for key '{logical_key}'"
        )));
    }
    if let Some((bytes, record)) = valid.into_iter().next() {
        return Ok(Some((record, bytes)));
    }
    if invalid_responses > 0 {
        return Err(AppError::Conflict(format!(
            "all available NCNX bytes for key '{logical_key}' are corrupt or belong to another fragment"
        )));
    }
    if successful_responses == 0 {
        return Err(AppError::ServiceUnavailable(format!(
            "no NCNX replica responded for key '{logical_key}'"
        )));
    }
    if require_complete_absence && successful_responses != layout.replicas.len() {
        return Err(AppError::ServiceUnavailable(format!(
            "NCNX absence for key '{logical_key}' is unproven: {successful_responses} of {} replicas responded",
            layout.replicas.len()
        )));
    }
    Ok(None)
}

fn authority_node_ids(authority: &[ClusterNode]) -> BTreeSet<&str> {
    authority.iter().map(|node| node.id.as_str()).collect()
}

fn authority_is_complete(
    runtime: &ClusterRuntime,
    pool_id: &str,
    authority: &[ClusterNode],
) -> bool {
    authority.len() == runtime.membership.nodes_for_pool(pool_id).len()
}

#[allow(clippy::too_many_arguments)]
async fn apply_authorized(
    runtime: &ClusterRuntime,
    layout: &NcnxLayout,
    action: PeerNcnxAction,
    fragment_id: &str,
    logical_key: &str,
    trash: Option<&TrashLocator>,
    bytes: &[u8],
    authority: &[ClusterNode],
) -> Result<(), AppError> {
    let authority_ids = authority_node_ids(authority);
    let authorized_replicas = layout
        .replicas
        .iter()
        .filter(|replica| authority_ids.contains(replica.node_id.as_str()))
        .count();
    if authorized_replicas != layout.replicas.len() {
        return Err(AppError::ServiceUnavailable(format!(
            "authorized pool component reaches {authorized_replicas} of {} exact NCNX replicas for key '{logical_key}'; complete NCNX publication is required",
            layout.replicas.len()
        )));
    }
    apply_at_least(
        runtime,
        layout,
        action,
        fragment_id,
        logical_key,
        trash,
        bytes,
        layout.replicas.len(),
        Some(&authority_ids),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn apply_at_least(
    runtime: &ClusterRuntime,
    layout: &NcnxLayout,
    action: PeerNcnxAction,
    fragment_id: &str,
    logical_key: &str,
    trash: Option<&TrashLocator>,
    bytes: &[u8],
    minimum_successes: usize,
    allowed_nodes: Option<&BTreeSet<&str>>,
) -> Result<(), AppError> {
    let ownership = stage::StageOwnership::acquire(
        Arc::new(runtime.clone()),
        fragment_id,
        &[(layout.clone(), logical_key.to_string())],
    )
    .await?;
    let calls = layout
        .replicas
        .iter()
        .filter(|replica| {
            allowed_nodes.is_none_or(|nodes| nodes.contains(replica.node_id.as_str()))
        })
        .map(|replica| {
            let mut request = request_for_replica(
                action.clone(),
                layout,
                replica,
                fragment_id,
                logical_key,
                trash.cloned(),
                Some(bytes),
            );
            let attached = ownership.attach(&mut request, layout, replica);
            async move {
                attached?;
                send_to_replica(runtime, replica, request).await
            }
        });
    let results = stage::join_calls(calls).await;
    ownership.release().await;
    let mut successes = 0usize;
    let mut failures = Vec::new();
    for result in results {
        match result {
            Ok(_) => successes = successes.saturating_add(1),
            Err(error @ AppError::Conflict(_)) => return Err(error),
            Err(error) => failures.push(error.to_string()),
        }
    }
    if successes < minimum_successes {
        Err(AppError::ServiceUnavailable(format!(
            "NCNX publication reached {successes} of {} exact replicas; {minimum_successes} required: {}",
            layout.replicas.len(),
            failures.join("; ")
        )))
    } else {
        Ok(())
    }
}

async fn find_live_with_absence_mode(
    fragment_id: &str,
    logical_key: &str,
    require_complete_absence: bool,
) -> Result<Option<LocatedNcnx>, AppError> {
    let logical_key = validate_logical_key(logical_key)?;
    let runtime = active_cluster_runtime()?;
    let mut pools = runtime.membership.pools.iter().collect::<Vec<_>>();
    pools.sort_by_key(|pool| std::cmp::Reverse(pool.generation));
    let mut found: Option<LocatedNcnx> = None;
    for pool in pools {
        let layout = build_ncnx_layout(&runtime, pool, fragment_id, &logical_key)?;
        if let Some((record, bytes)) = read_from_layout(
            &runtime,
            &layout,
            fragment_id,
            &logical_key,
            None,
            require_complete_absence,
        )
        .await?
        {
            if let Some(existing) = &found {
                if existing.record.object_id != record.object_id {
                    return Err(AppError::Conflict(format!(
                        "logical key '{logical_key}' points to different objects in two pools"
                    )));
                }
                continue;
            }
            found = Some(LocatedNcnx {
                pool_id: pool.id.clone(),
                pool_generation: pool.generation,
                logical_key: logical_key.clone(),
                record,
                bytes,
            });
        }
    }
    Ok(found)
}

pub async fn find_live(
    fragment_id: &str,
    logical_key: &str,
) -> Result<Option<LocatedNcnx>, AppError> {
    find_live_with_absence_mode(fragment_id, logical_key, true).await
}

pub async fn find_live_for_survival_write(
    fragment_id: &str,
    logical_key: &str,
) -> Result<Option<LocatedNcnx>, AppError> {
    find_live_with_absence_mode(fragment_id, logical_key, false).await
}

pub async fn publish_new(
    fragment_id: &str,
    logical_key: &str,
    record: &NcnxRecord,
) -> Result<LocatedNcnx, AppError> {
    let logical_key = validate_logical_key(logical_key)?;
    let runtime = active_cluster_runtime()?;
    let write_pool = runtime.membership.newest_writable_pool().ok_or_else(|| {
        AppError::ServiceUnavailable("cluster has no writable server pool".into())
    })?;
    let write_authority = pool_write_authority_nodes(&runtime, &write_pool.id).await?;
    let complete_authority = authority_is_complete(&runtime, &write_pool.id, &write_authority);

    let existing = if complete_authority {
        find_live(fragment_id, &logical_key).await?
    } else {
        find_live_for_survival_write(fragment_id, &logical_key).await?
    };
    if let Some(existing) = existing {
        if existing.record.object_id != record.object_id {
            return Err(AppError::Conflict(format!(
                "logical key '{logical_key}' already points to another object"
            )));
        }
        let pool = pool_for_located(&runtime, &existing)?;
        let authority = if pool.id == write_pool.id {
            write_authority.clone()
        } else {
            pool_mutation_authority_nodes(&runtime, &pool.id).await?
        };
        let layout = build_ncnx_layout(&runtime, pool, fragment_id, &logical_key)?;
        apply_authorized(
            &runtime,
            &layout,
            PeerNcnxAction::CreateLive,
            fragment_id,
            &logical_key,
            None,
            &existing.bytes,
            &authority,
        )
        .await?;
        return Ok(existing);
    }

    let pool = write_pool;
    let layout = build_ncnx_layout(&runtime, pool, fragment_id, &logical_key)?;
    let bytes = record.encode()?;
    apply_authorized(
        &runtime,
        &layout,
        PeerNcnxAction::CreateLive,
        fragment_id,
        &logical_key,
        None,
        &bytes,
        &write_authority,
    )
    .await?;
    Ok(LocatedNcnx {
        pool_id: pool.id.clone(),
        pool_generation: pool.generation,
        logical_key,
        record: record.clone(),
        bytes,
    })
}

/// Publishes independent upload pointers through grouped per-node NCNX
/// writes. Each key retains exact deterministic placement, conflict isolation,
/// complete NCNX publication, and an independent
/// result. A failed peer group is retried per key to avoid poisoning unrelated
/// uploads with one conflicting name.
pub async fn publish_new_batch(
    fragment_id: &str,
    items: &[NcnxPublishBatchItem],
) -> Vec<Result<LocatedNcnx, AppError>> {
    if items.is_empty() {
        return Vec::new();
    }
    if items.len() > PEER_NCNX_BATCH_MAX {
        return items
            .iter()
            .map(|_| {
                Err(AppError::BadRequest(format!(
                    "NCNX publication batch exceeds {PEER_NCNX_BATCH_MAX} entries"
                )))
            })
            .collect();
    }

    let runtime = match active_cluster_runtime() {
        Ok(runtime) => runtime,
        Err(error) => {
            let message = error.to_string();
            return items
                .iter()
                .map(|_| Err(AppError::ServiceUnavailable(message.clone())))
                .collect();
        }
    };
    let write_pool = match runtime.membership.newest_writable_pool() {
        Some(pool) => pool,
        None => {
            return items
                .iter()
                .map(|_| {
                    Err(AppError::ServiceUnavailable(
                        "cluster has no writable server pool".into(),
                    ))
                })
                .collect();
        }
    };
    let write_authority = match pool_write_authority_nodes(&runtime, &write_pool.id).await {
        Ok(authority) => authority,
        Err(error) => {
            let message = error.to_string();
            return items
                .iter()
                .map(|_| Err(AppError::ServiceUnavailable(message.clone())))
                .collect();
        }
    };
    let complete_authority = authority_is_complete(&runtime, &write_pool.id, &write_authority);
    let reads = items.iter().map(|item| async {
        if item.record.fragment_id != fragment_id {
            return Err(AppError::Conflict(
                "NCNX publication batch contains another fragment".into(),
            ));
        }
        let logical_key = validate_logical_key(&item.logical_key)?;
        let existing = if complete_authority {
            find_live(fragment_id, &logical_key).await?
        } else {
            find_live_for_survival_write(fragment_id, &logical_key).await?
        };
        Ok::<_, AppError>((logical_key.clone(), existing))
    });
    let observations = join_all(reads).await;
    let mut prepared = (0..items.len()).map(|_| None).collect::<Vec<_>>();
    let mut terminal = (0..items.len()).map(|_| None).collect::<Vec<_>>();
    let mut successes = vec![0usize; items.len()];
    let mut required_successes = vec![0usize; items.len()];
    let mut failures = vec![Vec::<String>::new(); items.len()];
    let mut groups = BTreeMap::<PeerNcnxBatchTarget, Vec<(usize, PeerNcnxBatchItem)>>::new();
    let mut ownership_keys = Vec::new();
    let mut authority_by_pool = BTreeMap::<String, BTreeSet<String>>::new();
    authority_by_pool.insert(
        write_pool.id.clone(),
        write_authority.into_iter().map(|node| node.id).collect(),
    );

    for (index, observation) in observations.into_iter().enumerate() {
        let (logical_key, existing) = match observation {
            Ok(value) => value,
            Err(error) => {
                terminal[index] = Some(error);
                continue;
            }
        };
        let (located, pool) = match existing {
            Some(existing) if existing.record.object_id != items[index].record.object_id => {
                terminal[index] = Some(AppError::Conflict(format!(
                    "logical key '{logical_key}' already points to another object"
                )));
                continue;
            }
            Some(existing) => {
                let pool = match pool_for_located(&runtime, &existing) {
                    Ok(pool) => pool,
                    Err(error) => {
                        terminal[index] = Some(error);
                        continue;
                    }
                };
                (existing, pool)
            }
            None => {
                let Some(pool) = runtime.membership.newest_writable_pool() else {
                    terminal[index] = Some(AppError::ServiceUnavailable(
                        "cluster has no writable server pool".into(),
                    ));
                    continue;
                };
                let bytes = match items[index].record.encode() {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        terminal[index] = Some(error);
                        continue;
                    }
                };
                (
                    LocatedNcnx {
                        pool_id: pool.id.clone(),
                        pool_generation: pool.generation,
                        logical_key: logical_key.clone(),
                        record: items[index].record.clone(),
                        bytes,
                    },
                    pool,
                )
            }
        };
        let layout = match build_ncnx_layout(&runtime, pool, fragment_id, &logical_key) {
            Ok(layout) => layout,
            Err(error) => {
                terminal[index] = Some(error);
                continue;
            }
        };
        if !authority_by_pool.contains_key(&pool.id) {
            match pool_mutation_authority_nodes(&runtime, &pool.id).await {
                Ok(authority) => {
                    authority_by_pool.insert(
                        pool.id.clone(),
                        authority.into_iter().map(|node| node.id).collect(),
                    );
                }
                Err(error) => {
                    terminal[index] = Some(error);
                    continue;
                }
            }
        }
        let authority_ids = &authority_by_pool[&pool.id];
        let exact_replica_count = layout.replicas.len();
        let authorized_replica_count = layout
            .replicas
            .iter()
            .filter(|replica| authority_ids.contains(&replica.node_id))
            .count();
        if authorized_replica_count != exact_replica_count {
            terminal[index] = Some(AppError::ServiceUnavailable(format!(
                "authorized pool component reaches {authorized_replica_count} of {exact_replica_count} exact NCNX replicas for key '{logical_key}'; complete NCNX publication is required"
            )));
            continue;
        }
        required_successes[index] = exact_replica_count;
        ownership_keys.push((layout.clone(), logical_key.clone()));
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
                .push((
                    index,
                    PeerNcnxBatchItem {
                        logical_key: logical_key.clone(),
                        trash: None,
                        bytes: ByteBuf::from(located.bytes.clone()),
                        lease_token: None,
                    },
                ));
        }
        prepared[index] = Some(located);
    }

    let ownership = if ownership_keys.is_empty() {
        None
    } else {
        match stage::StageOwnership::acquire(runtime.clone(), fragment_id, &ownership_keys).await {
            Ok(ownership) => Some(ownership),
            Err(_) => {
                // A busy key must not poison independent entries. Re-enter
                // the bounded per-key path before any grouped write starts.
                let mut isolated = Vec::with_capacity(items.len());
                for item in items {
                    isolated.push(publish_new(fragment_id, &item.logical_key, &item.record).await);
                }
                return isolated;
            }
        }
    };
    if let Some(ownership) = &ownership {
        for (target, indexed) in &mut groups {
            for (_, item) in indexed {
                if let Err(error) = ownership.attach_batch(target, std::slice::from_mut(item)) {
                    let message = error.to_string();
                    return items
                        .iter()
                        .map(|_| Err(AppError::Conflict(message.clone())))
                        .collect();
                }
            }
        }
    }
    let calls = groups.into_iter().map(|(target, indexed)| {
        let runtime = runtime.clone();
        let fragment_id = fragment_id.to_string();
        let request_items = indexed.iter().map(|(_, item)| item.clone()).collect();
        async move {
            let result = send_batch_to_target(
                &runtime,
                &target,
                PeerNcnxBatchAction::CreateLive,
                &fragment_id,
                request_items,
            )
            .await;
            (target, indexed, result)
        }
    });
    for (target, indexed, group_result) in stage::join_calls(calls).await {
        if group_result.is_ok() {
            for (index, _) in indexed {
                successes[index] = successes[index].saturating_add(1);
            }
            continue;
        }
        // Exact fallback is exceptional: it isolates one conflict or failed
        // disk without discarding successful entries from the same batch.
        for (index, item) in indexed {
            match send_batch_to_target(
                &runtime,
                &target,
                PeerNcnxBatchAction::CreateLive,
                fragment_id,
                vec![item],
            )
            .await
            {
                Ok(_) => successes[index] = successes[index].saturating_add(1),
                Err(error @ AppError::Conflict(_)) => terminal[index] = Some(error),
                Err(error) => failures[index].push(error.to_string()),
            }
        }
    }

    for index in 0..prepared.len() {
        if terminal[index].is_some() {
            continue;
        }
        let Some(_located) = prepared[index].as_ref() else {
            terminal[index] = Some(AppError::InternalServerError(
                "NCNX publication batch lost its prepared record".into(),
            ));
            continue;
        };
        let expected = required_successes[index];
        if successes[index] >= expected && expected > 0 {
            continue;
        }
        terminal[index] = Some(AppError::ServiceUnavailable(format!(
            "NCNX publication did not reach every authorized exact replica (reached {}, required {}): {}",
            successes[index],
            expected,
            failures[index].join("; ")
        )));
    }

    if let Some(ownership) = ownership {
        ownership.release().await;
    }
    prepared
        .into_iter()
        .enumerate()
        .map(|(index, located)| {
            if let Some(error) = terminal[index].take() {
                return Err(error);
            }
            located.ok_or_else(|| {
                AppError::InternalServerError(
                    "NCNX publication batch lost its prepared record".into(),
                )
            })
        })
        .collect()
}

pub(super) fn pool_for_located<'a>(
    runtime: &'a ClusterRuntime,
    located: &LocatedNcnx,
) -> Result<&'a ServerPool, AppError> {
    let pool = runtime
        .membership
        .pool(&located.pool_id)
        .ok_or_else(|| AppError::Conflict(format!("unknown NCNX pool '{}'", located.pool_id)))?;
    if pool.generation != located.pool_generation {
        return Err(AppError::Conflict(
            "NCNX pool generation changed from its exact home layout".into(),
        ));
    }
    Ok(pool)
}

pub async fn copy_live_to_key(
    located: &LocatedNcnx,
    destination_key: &str,
) -> Result<(), AppError> {
    copy_live_to_key_bytes(located, destination_key, &located.bytes).await
}

pub async fn copy_live_to_key_with_record(
    located: &LocatedNcnx,
    destination_key: &str,
    destination_record: &NcnxRecord,
) -> Result<(), AppError> {
    if destination_record.fragment_id != located.record.fragment_id
        || destination_record.object_id != located.record.object_id
    {
        return Err(AppError::Conflict(
            "renamed NCNX must preserve fragment and object identity".into(),
        ));
    }
    let bytes = destination_record.encode()?;
    copy_live_to_key_bytes(located, destination_key, &bytes).await
}

async fn copy_live_to_key_bytes(
    located: &LocatedNcnx,
    destination_key: &str,
    destination_bytes: &[u8],
) -> Result<(), AppError> {
    let destination_key = validate_logical_key(destination_key)?;
    let runtime = active_cluster_runtime()?;
    let source_pool = pool_for_located(&runtime, located)?;
    let source_authority = pool_mutation_authority_nodes(&runtime, &source_pool.id).await?;
    let existing = if authority_is_complete(&runtime, &source_pool.id, &source_authority) {
        find_live(&located.record.fragment_id, &destination_key).await?
    } else {
        find_live_for_survival_write(&located.record.fragment_id, &destination_key).await?
    };
    if let Some(existing) = existing {
        if existing.record.object_id == located.record.object_id {
            let pool = pool_for_located(&runtime, &existing)?;
            let authority = if pool.id == source_pool.id {
                source_authority.clone()
            } else {
                pool_mutation_authority_nodes(&runtime, &pool.id).await?
            };
            let layout = build_ncnx_layout(
                &runtime,
                pool,
                &located.record.fragment_id,
                &destination_key,
            )?;
            apply_authorized(
                &runtime,
                &layout,
                PeerNcnxAction::CreateLive,
                &located.record.fragment_id,
                &destination_key,
                None,
                &existing.bytes,
                &authority,
            )
            .await?;
            return Ok(());
        }
        return Err(AppError::Conflict(format!(
            "destination key '{destination_key}' already exists"
        )));
    }
    let layout = build_ncnx_layout(
        &runtime,
        source_pool,
        &located.record.fragment_id,
        &destination_key,
    )?;
    apply_authorized(
        &runtime,
        &layout,
        PeerNcnxAction::CreateLive,
        &located.record.fragment_id,
        &destination_key,
        None,
        destination_bytes,
        &source_authority,
    )
    .await
}

pub async fn copy_live_to_trash(
    located: &LocatedNcnx,
    locator: &TrashLocator,
) -> Result<(), AppError> {
    let runtime = active_cluster_runtime()?;
    let pool = pool_for_located(&runtime, located)?;
    let authority = pool_mutation_authority_nodes(&runtime, &pool.id).await?;
    let layout = build_ncnx_layout(
        &runtime,
        pool,
        &located.record.fragment_id,
        &located.logical_key,
    )?;
    apply_authorized(
        &runtime,
        &layout,
        PeerNcnxAction::CreateTrash,
        &located.record.fragment_id,
        &located.logical_key,
        Some(locator),
        &located.bytes,
        &authority,
    )
    .await
}

pub async fn remove_live(located: &LocatedNcnx) -> Result<(), AppError> {
    let runtime = active_cluster_runtime()?;
    let pool = pool_for_located(&runtime, located)?;
    let authority = pool_mutation_authority_nodes(&runtime, &pool.id).await?;
    let layout = build_ncnx_layout(
        &runtime,
        pool,
        &located.record.fragment_id,
        &located.logical_key,
    )?;
    apply_authorized(
        &runtime,
        &layout,
        PeerNcnxAction::RemoveLive,
        &located.record.fragment_id,
        &located.logical_key,
        None,
        &located.bytes,
        &authority,
    )
    .await
}

pub async fn remove_trash(located: &LocatedNcnx, locator: &TrashLocator) -> Result<(), AppError> {
    let runtime = active_cluster_runtime()?;
    let pool = pool_for_located(&runtime, located)?;
    let authority = pool_mutation_authority_nodes(&runtime, &pool.id).await?;
    let layout = build_ncnx_layout(
        &runtime,
        pool,
        &located.record.fragment_id,
        &located.logical_key,
    )?;
    apply_authorized(
        &runtime,
        &layout,
        PeerNcnxAction::RemoveTrash,
        &located.record.fragment_id,
        &located.logical_key,
        Some(locator),
        &located.bytes,
        &authority,
    )
    .await
}
