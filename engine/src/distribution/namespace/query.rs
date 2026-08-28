use super::*;

/// Classify only the mutation boundary. Exact live/trash keys remain the
/// synchronous fast path; virtual prefixes are handed to the background
/// operation runner without touching object payloads.
pub async fn namespace_mutation_target_kind(
    fragment: &str,
    kind: NamespaceMutationKind,
    requested: &str,
) -> Result<NamespaceEntryKind, AppError> {
    match kind {
        NamespaceMutationKind::Rename | NamespaceMutationKind::Trash => {
            let requested = validate_logical_key(requested)?;
            if find_live_for_survival_write(fragment, &requested)
                .await?
                .is_some()
            {
                return Ok(NamespaceEntryKind::File);
            }
            let prefix = format!("{requested}/");
            if list_canonical_names(fragment, &prefix, None, 1, false)
                .await?
                .is_empty()
            {
                return Err(AppError::NotFound);
            }
            Ok(NamespaceEntryKind::Directory)
        }
        NamespaceMutationKind::Recover => {
            let recovery = find_trash_recovery_set(fragment, requested).await?;
            match recovery.len() {
                0 => Err(AppError::NotFound),
                1 => Ok(NamespaceEntryKind::File),
                _ => Ok(NamespaceEntryKind::Directory),
            }
        }
        NamespaceMutationKind::Mkdir | NamespaceMutationKind::UploadCommit => Err(
            AppError::BadRequest("mutation kind does not have a file-or-prefix target".into()),
        ),
    }
}

pub(super) async fn namespace_entry(
    fragment: &str,
    requested_path: &str,
    survival_write: bool,
) -> Result<Option<NamespaceEntry>, AppError> {
    let normalized =
        normalize_path(requested_path).map_err(|err| AppError::BadRequest(err.to_string()))?;
    if normalized.is_empty() {
        return Ok(Some(directory_entry(fragment, "", "")));
    }
    let located = if survival_write {
        find_live_for_survival_write(fragment, &normalized).await?
    } else {
        find_live(fragment, &normalized).await?
    };
    if let Some(located) = located {
        return Ok(Some(live_file_entry(fragment, &normalized, &located)));
    }
    let prefix = format!("{normalized}/");
    if !list_canonical_names(fragment, &prefix, None, 1, false)
        .await?
        .is_empty()
    {
        return Ok(Some(directory_entry(
            fragment,
            &normalized,
            &base_name(&normalized),
        )));
    }
    Ok(None)
}

pub(super) async fn list_directory_page(
    fragment: &str,
    requested_path: &str,
    trash: bool,
    limit: usize,
    continuation: Option<&str>,
) -> Result<(Vec<NamespaceEntry>, Option<String>), AppError> {
    let directory =
        normalize_path(requested_path).map_err(|err| AppError::BadRequest(err.to_string()))?;
    let prefix = if directory.is_empty() {
        String::new()
    } else {
        format!("{directory}/")
    };
    let mut entries = BTreeMap::<String, NamespaceEntry>::new();
    let mut cursor = continuation.map(ToOwned::to_owned);
    let mut has_more = false;
    loop {
        let names =
            list_canonical_names(fragment, &prefix, cursor.as_deref(), LIST_BATCH_MAX, trash)
                .await?;
        if names.is_empty() {
            break;
        }
        let batch_full = names.len() == LIST_BATCH_MAX;
        for name in names {
            let record_cursor = ncnx_cursor(&name.logical_key, name.trash.as_ref());
            let relative = name.logical_key.strip_prefix(&prefix).unwrap_or_default();
            let first = relative.split('/').next().unwrap_or_default();
            if first.is_empty() {
                cursor = Some(record_cursor);
                continue;
            }
            let child_path = if directory.is_empty() {
                first.to_string()
            } else {
                format!("{directory}/{first}")
            };
            let is_directory = relative.contains('/');
            let entry = if is_directory {
                directory_entry(fragment, &child_path, first)
            } else if trash {
                let located = read_canonical_record(fragment, &name).await?;
                trash_entry(fragment, &name, &located)
            } else {
                // Zero-decode LIST: object identity and object metadata remain
                // absent until HEAD/GET.
                zero_decode_file_entry(fragment, &child_path, first)
            };
            if !entries.contains_key(&child_path) && entries.len() == limit {
                has_more = true;
                break;
            }
            entries.entry(child_path).or_insert(entry);
            cursor = Some(record_cursor);
        }
        if has_more || !batch_full {
            break;
        }
    }
    Ok((
        entries.into_values().collect(),
        has_more.then_some(cursor).flatten(),
    ))
}

pub(super) async fn object_refs_page(
    fragment: &str,
    prefix: &str,
    continuation: Option<&str>,
    limit: usize,
    trash: bool,
) -> Result<NamespaceObjectRefPage, AppError> {
    let limit = limit.clamp(1, 1_000);
    let names = list_canonical_names(fragment, prefix, continuation, limit + 1, trash).await?;
    let has_more = names.len() > limit;
    let selected = names.into_iter().take(limit).collect::<Vec<_>>();
    let continuation_token = has_more
        .then(|| {
            selected
                .last()
                .map(|name| ncnx_cursor(&name.logical_key, name.trash.as_ref()))
        })
        .flatten();
    let mut entries = Vec::with_capacity(selected.len());
    for name in selected {
        let located = read_canonical_record(fragment, &name).await?;
        entries.push(NamespaceObjectRef {
            path: name.logical_key,
            object_id: located.record.object_id,
            version_id: String::new(),
        });
    }
    Ok(NamespaceObjectRefPage {
        entries,
        continuation_token,
    })
}

pub(super) async fn all_names(
    fragment: &str,
    prefix: &str,
    trash: bool,
) -> Result<Vec<PeerNcnxName>, AppError> {
    all_names_controlled(
        fragment,
        prefix,
        trash,
        &NamespaceMutationControl::default(),
    )
    .await
}

pub(super) async fn all_names_controlled(
    fragment: &str,
    prefix: &str,
    trash: bool,
    control: &NamespaceMutationControl,
) -> Result<Vec<PeerNcnxName>, AppError> {
    let mut names = Vec::new();
    let mut cursor = None;
    loop {
        control.ensure_running()?;
        let page = list_canonical_names(fragment, prefix, cursor.as_deref(), LIST_BATCH_MAX, trash)
            .await?;
        if page.is_empty() {
            break;
        }
        cursor = page
            .last()
            .map(|name| ncnx_cursor(&name.logical_key, name.trash.as_ref()));
        let full = page.len() == LIST_BATCH_MAX;
        names.extend(page);
        control.planning(
            names.len(),
            names.last().map(|name| name.logical_key.clone()),
        );
        if !full {
            break;
        }
    }
    Ok(names)
}

pub(super) async fn collect_object_ids(
    fragment: &str,
    trash: bool,
) -> Result<BTreeSet<String>, AppError> {
    let mut ids = BTreeSet::new();
    for name in all_names(fragment, "", trash).await? {
        ids.insert(
            read_canonical_record(fragment, &name)
                .await?
                .record
                .object_id,
        );
    }
    Ok(ids)
}

pub(super) async fn find_trash_record(
    fragment: &str,
    requested: &str,
) -> Result<Option<(PeerNcnxName, LocatedNcnx)>, AppError> {
    let by_id = Ulid::from_string(requested).is_ok();
    let prefix = if by_id {
        "".to_string()
    } else {
        normalize_path(requested).map_err(|err| AppError::BadRequest(err.to_string()))?
    };
    let mut matching = all_names(fragment, &prefix, true)
        .await?
        .into_iter()
        .filter(|name| {
            let locator = name.trash.as_ref().expect("trash listing includes locator");
            if by_id {
                locator.delete_mutation_id == requested
            } else {
                locator.logical_key == prefix
            }
        })
        .collect::<Vec<_>>();
    matching
        .sort_by_key(|name| std::cmp::Reverse(name.trash.as_ref().unwrap().deleted_at_unix_nanos));
    let Some(name) = matching.into_iter().next() else {
        return Ok(None);
    };
    let located = read_canonical_record(fragment, &name).await?;
    Ok(Some((name, located)))
}

/// Selects one complete trash/recover operation. A mutation ULID identifies
/// the exact delete group. A logical file selects its newest exact record; a
/// virtual directory selects the newest mutation group below that prefix.
pub(super) async fn find_trash_recovery_set(
    fragment: &str,
    requested: &str,
) -> Result<Vec<(PeerNcnxName, LocatedNcnx)>, AppError> {
    find_trash_recovery_set_controlled(fragment, requested, &NamespaceMutationControl::default())
        .await
}

pub(super) async fn find_trash_recovery_set_controlled(
    fragment: &str,
    requested: &str,
    control: &NamespaceMutationControl,
) -> Result<Vec<(PeerNcnxName, LocatedNcnx)>, AppError> {
    let by_id = Ulid::from_string(requested).is_ok();
    let requested_key = if by_id {
        String::new()
    } else {
        validate_logical_key(requested)?
    };
    let scan_prefix = if by_id { "" } else { requested_key.as_str() };
    let mut candidates = all_names_controlled(fragment, scan_prefix, true, control)
        .await?
        .into_iter()
        .filter(|name| {
            let Some(locator) = name.trash.as_ref() else {
                return false;
            };
            if trash_is_expired(locator) {
                return false;
            }
            if by_id {
                locator.delete_mutation_id == requested
            } else {
                name.logical_key == requested_key
                    || name.logical_key.starts_with(&format!("{requested_key}/"))
            }
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    if !by_id {
        if let Some(exact) = candidates
            .iter()
            .filter(|name| name.logical_key == requested_key)
            .max_by_key(|name| name.trash.as_ref().unwrap().deleted_at_unix_nanos)
            .cloned()
        {
            candidates = vec![exact];
        } else {
            let newest = candidates
                .iter()
                .max_by_key(|name| name.trash.as_ref().unwrap().deleted_at_unix_nanos)
                .and_then(|name| name.trash.as_ref())
                .expect("trash candidate has locator")
                .delete_mutation_id
                .clone();
            candidates.retain(|name| {
                name.trash
                    .as_ref()
                    .is_some_and(|locator| locator.delete_mutation_id == newest)
            });
        }
    }
    candidates.sort_by(|left, right| left.logical_key.cmp(&right.logical_key));
    let mut recovery = Vec::with_capacity(candidates.len());
    let mut pending = candidates.into_iter();
    let mut reads = FuturesUnordered::new();
    loop {
        control.ensure_running()?;
        while reads.len() < PREFIX_MUTATION_CONCURRENCY {
            let Some(name) = pending.next() else {
                break;
            };
            let fragment = fragment.to_string();
            reads.push(async move {
                let _permit = PREFIX_PLANNING_BUDGET
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| {
                        AppError::ServiceUnavailable("namespace planning budget is closed".into())
                    })?;
                let located = read_canonical_record(&fragment, &name).await?;
                Ok::<_, AppError>((name, located))
            });
        }
        match reads.next().await {
            Some(Ok(item)) => {
                let current = item.0.logical_key.clone();
                recovery.push(item);
                control.planning(recovery.len(), Some(current));
            }
            Some(Err(error)) => return Err(error),
            None => break,
        }
    }
    recovery.sort_by(|left, right| left.0.logical_key.cmp(&right.0.logical_key));
    Ok(recovery)
}

pub(super) fn common_parent_for_keys<'a>(keys: impl Iterator<Item = &'a str>) -> String {
    let mut keys = keys;
    let Some(first) = keys.next() else {
        return String::new();
    };
    let mut common = first.split('/').collect::<Vec<_>>();
    for key in keys {
        let components = key.split('/').collect::<Vec<_>>();
        let shared = common
            .iter()
            .zip(components.iter())
            .take_while(|(left, right)| left == right)
            .count();
        common.truncate(shared);
    }
    if common.len() == first.split('/').count() {
        common.pop();
    }
    common.join("/")
}
