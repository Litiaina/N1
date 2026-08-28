use super::*;

pub(super) async fn rename_key_or_prefix(
    fragment: &str,
    source: &str,
    destination: &str,
    mutation_id: &str,
    now_nanos: i64,
    control: &NamespaceMutationControl,
) -> Result<NamespaceMutationSummary, AppError> {
    let source = validate_logical_key(source)?;
    let destination = validate_logical_key(destination)?;
    if source == destination {
        return Err(AppError::BadRequest(
            "rename source and destination are identical".into(),
        ));
    }
    if destination.starts_with(&format!("{source}/")) {
        return Err(AppError::BadRequest(
            "cannot move a key prefix inside itself".into(),
        ));
    }
    if let Some(located) = find_live_for_survival_write(fragment, &source).await? {
        rename_one(&located, &destination, mutation_id, now_nanos).await?;
        return Ok(mutation_summary(
            "rename",
            NamespaceEntryKind::File,
            1,
            Some(source),
            Some(destination),
            None,
            None,
        ));
    }

    let source_prefix = format!("{source}/");
    let names = all_names_controlled(fragment, &source_prefix, false, control).await?;
    if names.is_empty() {
        return Err(AppError::NotFound);
    }
    let affected = names.len();
    let mut pending = names.into_iter();
    let mut planning = FuturesUnordered::new();
    let mut work = Vec::with_capacity(affected);
    loop {
        control.ensure_running()?;
        while planning.len() < PREFIX_MUTATION_CONCURRENCY {
            let Some(name) = pending.next() else {
                break;
            };
            let fragment = fragment.to_string();
            let source = source.clone();
            let destination = destination.clone();
            let mutation_id = mutation_id.to_string();
            planning.push(async move {
                let _permit = PREFIX_PLANNING_BUDGET
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| {
                        AppError::ServiceUnavailable("namespace planning budget is closed".into())
                    })?;
                let destination_key = rename_prefix(&name.logical_key, &source, &destination)
                    .map_err(|err| AppError::BadRequest(err.to_string()))?;
                let source_record = read_canonical_record(&fragment, &name).await?;
                let destination_record =
                    match find_live_for_survival_write(&fragment, &destination_key).await? {
                        Some(existing)
                            if existing.record.object_id == source_record.record.object_id =>
                        {
                            // A retry may observe only part of an earlier destination
                            // publication. Reuse that exact immutable generation so
                            // missing replicas converge byte-for-byte.
                            existing.record
                        }
                        Some(_) => {
                            return Err(AppError::Conflict(format!(
                                "rename destination '{destination_key}' already exists"
                            )));
                        }
                        None => NcnxRecord::new(
                            &source_record.record.fragment_id,
                            &source_record.record.object_id,
                            mutation_id,
                            now_nanos,
                        )?,
                    };
                Ok::<_, AppError>(NcnxRenameBatchItem {
                    located: source_record,
                    destination_key,
                    destination_record,
                })
            });
        }
        match planning.next().await {
            Some(Ok(item)) => {
                let current = item.located.logical_key.clone();
                work.push(item);
                control.planning(work.len(), Some(current));
            }
            Some(Err(error)) => return Err(error),
            None => break,
        }
    }
    run_prefix_rename(work, mutation_id, now_nanos, control).await?;
    Ok(mutation_summary(
        "rename",
        NamespaceEntryKind::Directory,
        affected,
        Some(source),
        Some(destination),
        None,
        None,
    ))
}

pub(super) async fn run_prefix_rename(
    work: Vec<NcnxRenameBatchItem>,
    mutation_id: &str,
    now_nanos: i64,
    control: &NamespaceMutationControl,
) -> Result<(), AppError> {
    let total = work.len();
    control.executing(total, 0, 0, 0, None);
    let mut completed = 0usize;
    let mut succeeded = 0usize;
    let mut failed = 0usize;
    for batch in work.chunks(PEER_NCNX_BATCH_MAX) {
        control.ensure_running()?;
        let _batch_permit = PREFIX_BATCH_BUDGET
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| {
                AppError::ServiceUnavailable("namespace mutation budget is closed".into())
            })?;
        match rename_live_batch(batch).await {
            Ok(()) => {
                completed = completed.saturating_add(batch.len());
                succeeded = succeeded.saturating_add(batch.len());
                control.executing(
                    total,
                    completed,
                    succeeded,
                    failed,
                    batch.last().map(|item| item.located.logical_key.clone()),
                );
            }
            Err(_batch_error) => {
                // A rare conflict or transport failure can leave safe,
                // destination-first partial publication. Re-enter the proven
                // per-entry path to classify exact progress and converge every
                // idempotent entry before returning the first real failure.
                for item in batch {
                    control.ensure_running()?;
                    let path = item.located.logical_key.clone();
                    match rename_one(&item.located, &item.destination_key, mutation_id, now_nanos)
                        .await
                    {
                        Ok(()) => {
                            completed = completed.saturating_add(1);
                            succeeded = succeeded.saturating_add(1);
                            control.executing(total, completed, succeeded, failed, Some(path));
                        }
                        Err(error) => {
                            completed = completed.saturating_add(1);
                            failed = failed.saturating_add(1);
                            control.executing(total, completed, succeeded, failed, Some(path));
                            return Err(error);
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

pub(super) async fn rename_one(
    located: &LocatedNcnx,
    destination: &str,
    mutation_id: &str,
    now_nanos: i64,
) -> Result<(), AppError> {
    // The destination is a new immutable namespace generation. Historical
    // trash from an older name then cannot masquerade as an active tombstone
    // for this current live pointer.
    let destination_record = NcnxRecord::new(
        &located.record.fragment_id,
        &located.record.object_id,
        mutation_id,
        now_nanos,
    )?;
    copy_live_to_key_with_record(located, destination, &destination_record).await?;
    remove_live(located).await
}

pub(super) async fn trash_key_or_prefix(
    fragment: &str,
    source: &str,
    mutation_id: &str,
    now_nanos: i64,
    control: &NamespaceMutationControl,
) -> Result<NamespaceMutationSummary, AppError> {
    let source = validate_logical_key(source)?;
    if let Some(located) = find_live_for_survival_write(fragment, &source).await? {
        trash_one(&located, mutation_id, now_nanos).await?;
        return Ok(mutation_summary(
            "trash",
            NamespaceEntryKind::File,
            1,
            Some(source),
            None,
            Some(mutation_id.to_string()),
            Some(now_nanos),
        ));
    }
    let names = all_names_controlled(fragment, &format!("{source}/"), false, control).await?;
    if names.is_empty() {
        // Repeated user deletion is safe and does not become an internal
        // server error. Report the already-trashed key when present.
        if let Some((name, _)) = find_trash_record(fragment, &source).await? {
            let locator = name.trash.unwrap();
            return Ok(mutation_summary(
                "trash",
                NamespaceEntryKind::File,
                0,
                Some(source),
                None,
                Some(locator.delete_mutation_id),
                Some(locator.deleted_at_unix_nanos),
            ));
        }
        return Err(AppError::NotFound);
    }
    let affected = names.len();
    let mut work = Vec::with_capacity(affected);
    let mut pending = names.into_iter();
    let mut planning = FuturesUnordered::new();
    loop {
        control.ensure_running()?;
        while planning.len() < PREFIX_MUTATION_CONCURRENCY {
            let Some(name) = pending.next() else {
                break;
            };
            let fragment = fragment.to_string();
            let mutation_id = mutation_id.to_string();
            planning.push(async move {
                let _permit = PREFIX_PLANNING_BUDGET
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| {
                        AppError::ServiceUnavailable("namespace planning budget is closed".into())
                    })?;
                let located = read_canonical_record(&fragment, &name).await?;
                let locator = TrashLocator {
                    logical_key: located.logical_key.clone(),
                    deleted_at_unix_nanos: now_nanos,
                    delete_mutation_id: mutation_id,
                };
                Ok::<_, AppError>(NcnxTrashBatchItem { located, locator })
            });
        }
        match planning.next().await {
            Some(Ok(item)) => {
                let current = item.located.logical_key.clone();
                work.push(item);
                control.planning(work.len(), Some(current));
            }
            Some(Err(error)) => return Err(error),
            None => break,
        }
    }
    work.sort_by(|left, right| left.located.logical_key.cmp(&right.located.logical_key));
    run_prefix_trash(work, mutation_id, now_nanos, control).await?;
    Ok(mutation_summary(
        "trash",
        NamespaceEntryKind::Directory,
        affected,
        Some(source),
        None,
        Some(mutation_id.to_string()),
        Some(now_nanos),
    ))
}

pub(super) async fn run_prefix_trash(
    work: Vec<NcnxTrashBatchItem>,
    mutation_id: &str,
    now_nanos: i64,
    control: &NamespaceMutationControl,
) -> Result<(), AppError> {
    let total = work.len();
    control.executing(total, 0, 0, 0, None);
    let mut completed = 0usize;
    let mut succeeded = 0usize;
    let mut failed = 0usize;
    for batch in work.chunks(PEER_NCNX_BATCH_MAX) {
        control.ensure_running()?;
        let _batch_permit = PREFIX_BATCH_BUDGET
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| {
                AppError::ServiceUnavailable("namespace mutation budget is closed".into())
            })?;
        match trash_live_batch(batch).await {
            Ok(()) => {
                completed = completed.saturating_add(batch.len());
                succeeded = succeeded.saturating_add(batch.len());
                control.executing(
                    total,
                    completed,
                    succeeded,
                    failed,
                    batch.last().map(|item| item.located.logical_key.clone()),
                );
            }
            Err(_) => {
                for item in batch {
                    control.ensure_running()?;
                    let path = item.located.logical_key.clone();
                    match trash_one(&item.located, mutation_id, now_nanos).await {
                        Ok(()) => {
                            completed = completed.saturating_add(1);
                            succeeded = succeeded.saturating_add(1);
                            control.executing(total, completed, succeeded, failed, Some(path));
                        }
                        Err(error) => {
                            completed = completed.saturating_add(1);
                            failed = failed.saturating_add(1);
                            control.executing(total, completed, succeeded, failed, Some(path));
                            return Err(error);
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

pub(super) async fn run_prefix_recover(
    work: Vec<NcnxRecoverBatchItem>,
    control: &NamespaceMutationControl,
) -> Result<(), AppError> {
    let total = work.len();
    control.executing(total, 0, 0, 0, None);
    let mut completed = 0usize;
    let mut succeeded = 0usize;
    let mut failed = 0usize;
    for batch in work.chunks(PEER_NCNX_BATCH_MAX) {
        control.ensure_running()?;
        let _batch_permit = PREFIX_BATCH_BUDGET
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| {
                AppError::ServiceUnavailable("namespace mutation budget is closed".into())
            })?;
        match recover_trash_batch(batch).await {
            Ok(()) => {
                completed = completed.saturating_add(batch.len());
                succeeded = succeeded.saturating_add(batch.len());
                control.executing(
                    total,
                    completed,
                    succeeded,
                    failed,
                    batch.last().map(|item| item.located.logical_key.clone()),
                );
            }
            Err(_) => {
                for item in batch {
                    control.ensure_running()?;
                    let path = item.located.logical_key.clone();
                    let name = PeerNcnxName {
                        logical_key: path.clone(),
                        trash: Some(item.locator.clone()),
                    };
                    match recover_one(&name, &item.located).await {
                        Ok(()) => {
                            completed = completed.saturating_add(1);
                            succeeded = succeeded.saturating_add(1);
                            control.executing(total, completed, succeeded, failed, Some(path));
                        }
                        Err(error) => {
                            completed = completed.saturating_add(1);
                            failed = failed.saturating_add(1);
                            control.executing(total, completed, succeeded, failed, Some(path));
                            return Err(error);
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

pub(super) async fn trash_one(
    located: &LocatedNcnx,
    mutation_id: &str,
    now_nanos: i64,
) -> Result<(), AppError> {
    let locator = TrashLocator {
        logical_key: located.logical_key.clone(),
        deleted_at_unix_nanos: now_nanos,
        delete_mutation_id: mutation_id.to_string(),
    };
    copy_live_to_trash(located, &locator).await?;
    remove_live(located).await
}

pub(super) async fn recover_one(
    name: &PeerNcnxName,
    located: &LocatedNcnx,
) -> Result<(), AppError> {
    let locator = name
        .trash
        .as_ref()
        .ok_or_else(|| AppError::InternalServerError("trash recovery has no locator".into()))?;
    if find_live_for_survival_write(&located.record.fragment_id, &name.logical_key)
        .await?
        .is_some()
    {
        return Err(AppError::Conflict(format!(
            "cannot recover '{}': live key already exists",
            name.logical_key
        )));
    }
    copy_live_to_key(located, &name.logical_key).await?;
    remove_trash(located, locator).await
}
