use super::*;

pub async fn namespace_read(
    request: PeerNamespaceReadRequest,
) -> Result<Option<PeerNamespaceReadResponse>, AppError> {
    let response = match request.kind {
        PeerNamespaceReadKind::ListDir => {
            let (entries, continuation_token) = list_directory_page(
                &request.fragment,
                request.path.as_deref().unwrap_or_default(),
                request.include_trashed,
                request.limit.unwrap_or(200).clamp(1, 1_000),
                request.continuation_token.as_deref(),
            )
            .await?;
            PeerNamespaceReadResponse::ListDir {
                entries,
                continuation_token,
            }
        }
        PeerNamespaceReadKind::Entry | PeerNamespaceReadKind::EntryForWrite => {
            let path = request.path.as_deref().unwrap_or_default();
            PeerNamespaceReadResponse::Entry(
                namespace_entry(
                    &request.fragment,
                    path,
                    matches!(request.kind, PeerNamespaceReadKind::EntryForWrite),
                )
                .await?,
            )
        }
        PeerNamespaceReadKind::TrashedEntry => {
            let requested = request.path.as_deref().unwrap_or_default();
            PeerNamespaceReadResponse::Entry(
                find_trash_record(&request.fragment, requested)
                    .await?
                    .map(|(name, located)| trash_entry(&request.fragment, &name, &located)),
            )
        }
        PeerNamespaceReadKind::ResolveLiveObject => {
            let path = request.path.as_deref().unwrap_or_default();
            PeerNamespaceReadResponse::ObjectId(
                find_live(&request.fragment, path)
                    .await?
                    .map(|located| located.record.object_id),
            )
        }
        PeerNamespaceReadKind::ObjectRefs | PeerNamespaceReadKind::TrashObjectRefs => {
            let trash = matches!(request.kind, PeerNamespaceReadKind::TrashObjectRefs);
            let page = object_refs_page(
                &request.fragment,
                request.prefix.as_deref().unwrap_or_default(),
                request.continuation_token.as_deref(),
                request.limit.unwrap_or(200),
                trash,
            )
            .await?;
            PeerNamespaceReadResponse::ObjectRefs(page)
        }
        PeerNamespaceReadKind::Reachability => {
            let mut ids = collect_object_ids(&request.fragment, false).await?;
            if request.include_trashed {
                ids.extend(collect_object_ids(&request.fragment, true).await?);
            }
            PeerNamespaceReadResponse::Reachability(ids)
        }
    };
    Ok(Some(response))
}

pub async fn commit_upload_pointer(
    fragment: &str,
    logical_path: &str,
    physical_object_id: &str,
    metadata: &ObjectMetadata,
) -> Result<String, AppError> {
    let outcome = commit_namespace_mutation(PeerNamespaceApplyRequest {
        mutation_id: None,
        kind: NamespaceMutationKind::UploadCommit,
        fragment: fragment.to_string(),
        source_path: Some(logical_path.to_string()),
        destination_path: None,
        object_id: Some(physical_object_id.to_string()),
        version_id: Some(metadata.version_id.clone()),
        metadata: Some(metadata.clone()),
    })
    .await?;
    Ok(outcome
        .committed_object()
        .unwrap_or_else(|| physical_object_id.to_string()))
}

pub async fn commit_namespace_mutation(
    request: PeerNamespaceApplyRequest,
) -> Result<NamespaceApplyOutcome, AppError> {
    commit_namespace_mutation_controlled(request, NamespaceMutationControl::default(), None).await
}

pub async fn commit_namespace_mutation_controlled(
    request: PeerNamespaceApplyRequest,
    control: NamespaceMutationControl,
    mutation_unix_nanos: Option<i64>,
) -> Result<NamespaceApplyOutcome, AppError> {
    let mutation_id = request
        .mutation_id
        .unwrap_or_else(|| Ulid::new().to_string());
    if Ulid::from_string(&mutation_id).is_err() {
        return Err(AppError::BadRequest(
            "namespace mutation_id must be a ULID".into(),
        ));
    }
    let now_nanos = match mutation_unix_nanos {
        Some(value) => value,
        None => Utc::now().timestamp_nanos_opt().ok_or_else(|| {
            AppError::InternalServerError("system time exceeds nanoseconds".into())
        })?,
    };
    control.ensure_running()?;
    match request.kind {
        NamespaceMutationKind::Mkdir => {
            let path = required(&request.source_path, "mkdir requires source_path")?;
            validate_logical_key(path)?;
            Err(AppError::BadRequest(
                "N2 3.4 is a flat key namespace and does not persist empty directories".into(),
            ))
        }
        NamespaceMutationKind::Rename => {
            let source = required(&request.source_path, "rename requires source_path")?;
            let destination = required(
                &request.destination_path,
                "rename requires destination_path",
            )?;
            let summary = rename_key_or_prefix(
                &request.fragment,
                source,
                destination,
                &mutation_id,
                now_nanos,
                &control,
            )
            .await?;
            Ok(NamespaceApplyOutcome::Summary(summary))
        }
        NamespaceMutationKind::Trash => {
            let source = required(&request.source_path, "trash requires source_path")?;
            let summary =
                trash_key_or_prefix(&request.fragment, source, &mutation_id, now_nanos, &control)
                    .await?;
            Ok(NamespaceApplyOutcome::Summary(summary))
        }
        NamespaceMutationKind::Recover => {
            let requested = required(&request.source_path, "recover requires path or trash id")?;
            let recovery =
                find_trash_recovery_set_controlled(&request.fragment, requested, &control).await?;
            if recovery.is_empty() {
                return Err(AppError::NotFound);
            }
            // Prove every destination before publishing any live pointer.
            // Recovery is strict: any live destination rejects the whole
            // selected generation, even when it points to the same object.
            let mut destinations = FuturesUnordered::new();
            let mut pending_destinations = recovery.iter();
            loop {
                control.ensure_running()?;
                while destinations.len() < PREFIX_MUTATION_CONCURRENCY {
                    let Some((name, _)) = pending_destinations.next() else {
                        break;
                    };
                    let fragment = request.fragment.clone();
                    let logical_key = name.logical_key.clone();
                    destinations.push(async move {
                        let _permit = PREFIX_PLANNING_BUDGET
                            .clone()
                            .acquire_owned()
                            .await
                            .map_err(|_| {
                                AppError::ServiceUnavailable(
                                    "namespace planning budget is closed".into(),
                                )
                            })?;
                        Ok::<_, AppError>((
                            logical_key.clone(),
                            find_live(&fragment, &logical_key).await?.is_some(),
                        ))
                    });
                }
                match destinations.next().await {
                    Some(Ok((logical_key, true))) => {
                        return Err(AppError::Conflict(format!(
                            "cannot recover '{logical_key}': live key already exists"
                        )));
                    }
                    Some(Ok((_, false))) => {}
                    Some(Err(error)) => return Err(error),
                    None => break,
                }
            }
            let affected = recovery.len();
            control.executing(affected, 0, 0, 0, None);
            let first_name = recovery.first().expect("non-empty recovery set").0.clone();
            let first_locator = first_name
                .trash
                .clone()
                .expect("trash recovery name has a locator");
            let source_path = if affected == 1 {
                first_name.logical_key.clone()
            } else {
                common_parent_for_keys(recovery.iter().map(|(name, _)| name.logical_key.as_str()))
            };
            let work = recovery
                .into_iter()
                .map(|(name, located)| NcnxRecoverBatchItem {
                    locator: name.trash.expect("trash recovery name has a locator"),
                    located,
                })
                .collect::<Vec<_>>();
            run_prefix_recover(work, &control).await?;
            Ok(NamespaceApplyOutcome::Summary(NamespaceMutationSummary {
                operation: "recover".into(),
                kind: if affected == 1 {
                    NamespaceEntryKind::File
                } else {
                    NamespaceEntryKind::Directory
                },
                affected_paths: affected,
                affected_objects: affected,
                source_path: Some(source_path.clone()),
                destination_path: Some(source_path),
                trash_entry_id: Some(first_locator.delete_mutation_id),
                deleted_at_unix: Some(first_locator.deleted_at_unix_nanos / 1_000_000_000),
            }))
        }
        NamespaceMutationKind::UploadCommit => {
            let path = required(&request.source_path, "upload commit requires source_path")?;
            let object_id = required(&request.object_id, "upload commit requires object_id")?;
            let record = NcnxRecord::new(&request.fragment, object_id, mutation_id, now_nanos)?;
            publish_new(&request.fragment, path, &record).await?;
            Ok(NamespaceApplyOutcome::CommittedObject(
                object_id.to_string(),
            ))
        }
    }
}

pub async fn commit_namespace_created(
    request: PeerNamespaceApplyRequest,
) -> Result<bool, AppError> {
    Ok(commit_namespace_mutation(request)
        .await?
        .created()
        .unwrap_or(false))
}

pub async fn commit_namespace_summary(
    request: PeerNamespaceApplyRequest,
) -> Result<NamespaceMutationSummary, AppError> {
    commit_namespace_mutation(request)
        .await?
        .summary()
        .ok_or_else(|| {
            AppError::InternalServerError("namespace mutation did not return a summary".into())
        })
}

pub async fn commit_namespace_summary_controlled(
    request: PeerNamespaceApplyRequest,
    control: NamespaceMutationControl,
    mutation_unix_nanos: i64,
) -> Result<NamespaceMutationSummary, AppError> {
    commit_namespace_mutation_controlled(request, control, Some(mutation_unix_nanos))
        .await?
        .summary()
        .ok_or_else(|| {
            AppError::InternalServerError("namespace mutation did not return a summary".into())
        })
}

/// Executes independent exact-key trash/recover requests as bounded NCNX
/// durability groups. Prefixes and retry edge cases retain the full mutation
/// path, so batching changes syscall shape rather than namespace semantics.
pub async fn commit_namespace_bulk(
    fragment: &str,
    kind: NamespaceMutationKind,
    keys: Vec<String>,
) -> Vec<(String, Result<NamespaceMutationSummary, AppError>)> {
    match kind {
        NamespaceMutationKind::Trash => bulk_trash_exact(fragment, keys).await,
        NamespaceMutationKind::Recover => bulk_recover_exact(fragment, keys).await,
        _ => keys
            .into_iter()
            .map(|key| {
                (
                    key,
                    Err(AppError::BadRequest(
                        "namespace bulk supports trash or recover".into(),
                    )),
                )
            })
            .collect(),
    }
}
