use super::*;

pub(super) async fn bulk_trash_exact(
    fragment: &str,
    keys: Vec<String>,
) -> Vec<(String, Result<NamespaceMutationSummary, AppError>)> {
    let total = keys.len();
    let mut slots = (0..total).map(|_| None).collect::<Vec<_>>();
    let mut pending = keys.iter().cloned().enumerate();
    let mut planning = FuturesUnordered::new();
    let mut work = Vec::new();
    let mutation_id = Ulid::new().to_string();
    let now_nanos = Utc::now().timestamp_nanos_opt().unwrap_or(1);
    loop {
        while planning.len() < PREFIX_MUTATION_CONCURRENCY {
            let Some((index, key)) = pending.next() else {
                break;
            };
            let fragment = fragment.to_string();
            planning.push(async move {
                let result = async {
                    let _permit = PREFIX_PLANNING_BUDGET
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|_| {
                            AppError::ServiceUnavailable(
                                "namespace planning budget is closed".into(),
                            )
                        })?;
                    let key = validate_logical_key(&key)?;
                    Ok::<_, AppError>((
                        key.clone(),
                        find_live_for_survival_write(&fragment, &key).await?,
                    ))
                }
                .await;
                (index, key, result)
            });
        }
        match planning.next().await {
            Some((index, _, Ok((key, Some(located))))) => {
                let locator = TrashLocator {
                    logical_key: key.clone(),
                    deleted_at_unix_nanos: now_nanos,
                    delete_mutation_id: mutation_id.clone(),
                };
                let summary = mutation_summary(
                    "trash",
                    NamespaceEntryKind::File,
                    1,
                    Some(key.clone()),
                    None,
                    Some(mutation_id.clone()),
                    Some(now_nanos),
                );
                work.push((index, NcnxTrashBatchItem { located, locator }, summary));
            }
            Some((index, _, Ok((key, None)))) => {
                slots[index] = Some(
                    commit_namespace_summary(PeerNamespaceApplyRequest {
                        mutation_id: None,
                        kind: NamespaceMutationKind::Trash,
                        fragment: fragment.to_string(),
                        source_path: Some(key),
                        destination_path: None,
                        object_id: None,
                        version_id: None,
                        metadata: None,
                    })
                    .await,
                );
            }
            Some((index, _, Err(error))) => slots[index] = Some(Err(error)),
            None => break,
        }
    }
    work.sort_by_key(|(index, _, _)| *index);
    for chunk in work.chunks(PEER_NCNX_BATCH_MAX) {
        let batch = chunk
            .iter()
            .map(|(_, item, _)| item.clone())
            .collect::<Vec<_>>();
        if trash_live_batch(&batch).await.is_ok() {
            for (index, _, summary) in chunk {
                slots[*index] = Some(Ok(summary.clone()));
            }
        } else {
            for (index, item, summary) in chunk {
                slots[*index] = Some(
                    trash_one(
                        &item.located,
                        &item.locator.delete_mutation_id,
                        item.locator.deleted_at_unix_nanos,
                    )
                    .await
                    .map(|_| summary.clone()),
                );
            }
        }
    }
    keys.into_iter()
        .enumerate()
        .map(|(index, key)| {
            (
                key,
                slots[index].take().unwrap_or_else(|| {
                    Err(AppError::InternalServerError(
                        "bulk trash did not classify an input".into(),
                    ))
                }),
            )
        })
        .collect()
}

pub(super) async fn bulk_recover_exact(
    fragment: &str,
    keys: Vec<String>,
) -> Vec<(String, Result<NamespaceMutationSummary, AppError>)> {
    let total = keys.len();
    let mut slots = (0..total).map(|_| None).collect::<Vec<_>>();
    let mut parent_groups = BTreeMap::<String, Vec<(usize, String)>>::new();
    for (index, requested) in keys.iter().enumerate() {
        match validate_logical_key(requested) {
            Ok(key) => parent_groups
                .entry(parent_path(&key))
                .or_default()
                .push((index, key)),
            Err(error) => slots[index] = Some(Err(error)),
        }
    }

    // One ordered trash scan per parent replaces one distributed scan per
    // object. A 256-file selection from one directory therefore traverses the
    // namespace once while preserving newest-generation selection per key.
    let mut pending_groups = parent_groups.into_iter();
    let mut listings = FuturesUnordered::new();
    let mut candidates = BTreeMap::<String, PeerNcnxName>::new();
    loop {
        while listings.len() < PREFIX_MUTATION_CONCURRENCY {
            let Some((parent, requested)) = pending_groups.next() else {
                break;
            };
            let fragment = fragment.to_string();
            listings.push(async move {
                let result = async {
                    let _permit = PREFIX_PLANNING_BUDGET
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|_| {
                            AppError::ServiceUnavailable(
                                "namespace planning budget is closed".into(),
                            )
                        })?;
                    let prefix = if parent.is_empty() {
                        String::new()
                    } else {
                        format!("{parent}/")
                    };
                    all_names(&fragment, &prefix, true).await
                }
                .await;
                (requested, result)
            });
        }
        match listings.next().await {
            Some((requested, Ok(names))) => {
                let wanted = requested
                    .iter()
                    .map(|(_, key)| key.as_str())
                    .collect::<BTreeSet<_>>();
                for name in names.into_iter().filter(|name| {
                    wanted.contains(name.logical_key.as_str())
                        && name
                            .trash
                            .as_ref()
                            .is_some_and(|trash| !trash_is_expired(trash))
                }) {
                    let replace = candidates.get(&name.logical_key).is_none_or(|current| {
                        current.trash.as_ref().zip(name.trash.as_ref()).is_none_or(
                            |(current, proposed)| {
                                proposed.deleted_at_unix_nanos > current.deleted_at_unix_nanos
                            },
                        )
                    });
                    if replace {
                        candidates.insert(name.logical_key.clone(), name);
                    }
                }
                for (index, key) in requested {
                    if !candidates.contains_key(&key) {
                        slots[index] = Some(
                            commit_namespace_summary(PeerNamespaceApplyRequest {
                                mutation_id: None,
                                kind: NamespaceMutationKind::Recover,
                                fragment: fragment.to_string(),
                                source_path: Some(key),
                                destination_path: None,
                                object_id: None,
                                version_id: None,
                                metadata: None,
                            })
                            .await,
                        );
                    }
                }
            }
            Some((requested, Err(error))) => {
                let message = error.to_string();
                for (index, _) in requested {
                    slots[index] = Some(Err(AppError::ServiceUnavailable(message.clone())));
                }
            }
            None => break,
        }
    }

    let pending_items = keys
        .iter()
        .cloned()
        .enumerate()
        .filter_map(|(index, key)| {
            (slots[index].is_none())
                .then(|| candidates.get(&key).cloned().map(|name| (index, key, name)))
                .flatten()
        })
        .collect::<Vec<_>>();
    let mut pending = pending_items.into_iter();
    let mut planning = FuturesUnordered::new();
    let mut work = Vec::new();
    loop {
        while planning.len() < PREFIX_MUTATION_CONCURRENCY {
            let Some((index, requested, name)) = pending.next() else {
                break;
            };
            let fragment = fragment.to_string();
            planning.push(async move {
                let result = async {
                    let _permit = PREFIX_PLANNING_BUDGET
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|_| {
                            AppError::ServiceUnavailable(
                                "namespace planning budget is closed".into(),
                            )
                        })?;
                    let located = read_canonical_record(&fragment, &name).await?;
                    let live = find_live_for_survival_write(&fragment, &name.logical_key).await?;
                    Ok::<_, AppError>((name, located, live))
                }
                .await;
                (index, requested, result)
            });
        }
        match planning.next().await {
            Some((index, _, Ok((name, located, live))))
                if live
                    .as_ref()
                    .is_none_or(|existing| existing.bytes == located.bytes) =>
            {
                let locator = name.trash.expect("trash recovery has locator");
                let summary = mutation_summary(
                    "recover",
                    NamespaceEntryKind::File,
                    1,
                    Some(name.logical_key.clone()),
                    Some(name.logical_key),
                    Some(locator.delete_mutation_id.clone()),
                    Some(locator.deleted_at_unix_nanos),
                );
                work.push((index, NcnxRecoverBatchItem { located, locator }, summary));
            }
            Some((index, requested, Ok(_))) => {
                slots[index] = Some(Err(AppError::Conflict(format!(
                    "cannot recover '{requested}': live key already exists"
                ))));
            }
            Some((index, _, Err(error))) => slots[index] = Some(Err(error)),
            None => break,
        }
    }
    work.sort_by_key(|(index, _, _)| *index);
    for chunk in work.chunks(PEER_NCNX_BATCH_MAX) {
        let batch = chunk
            .iter()
            .map(|(_, item, _)| item.clone())
            .collect::<Vec<_>>();
        if recover_trash_batch(&batch).await.is_ok() {
            for (index, _, summary) in chunk {
                slots[*index] = Some(Ok(summary.clone()));
            }
        } else {
            for (index, item, summary) in chunk {
                let name = PeerNcnxName {
                    logical_key: item.located.logical_key.clone(),
                    trash: Some(item.locator.clone()),
                };
                slots[*index] = Some(
                    recover_one(&name, &item.located)
                        .await
                        .map(|_| summary.clone()),
                );
            }
        }
    }
    keys.into_iter()
        .enumerate()
        .map(|(index, key)| {
            (
                key,
                slots[index].take().unwrap_or_else(|| {
                    Err(AppError::InternalServerError(
                        "bulk recover did not classify an input".into(),
                    ))
                }),
            )
        })
        .collect()
}
