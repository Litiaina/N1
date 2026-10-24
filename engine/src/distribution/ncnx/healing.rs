use super::*;

async fn inspect_name_inner(
    fragment_id: &str,
    name: &PeerNcnxName,
    repair: bool,
) -> Result<NcnxRepairOutcome, AppError> {
    let runtime = active_cluster_runtime()?;
    let mut located = match read_canonical_record(fragment_id, name).await {
        Ok(located) => located,
        Err(AppError::NotFound) => {
            return Err(AppError::Conflict(format!(
                "all NCNX replicas are unreadable for key '{}'",
                name.logical_key
            )));
        }
        Err(err) => return Err(err),
    };
    let pool = pool_for_located(&runtime, &located)?;
    let layout = build_ncnx_layout(&runtime, pool, fragment_id, &name.logical_key)?;
    let ownership = if repair {
        let ownership = stage::StageOwnership::acquire(
            runtime.clone(),
            fragment_id,
            &[(layout.clone(), name.logical_key.clone())],
        )
        .await?;
        // The unlocked first lookup selects only a home layout. It is never
        // authority to recreate a pointer: deletion may have completed since.
        let fresh = mutation::read_from_layout_owned(
            &runtime,
            &layout,
            fragment_id,
            &name.logical_key,
            name.trash.as_ref(),
            true,
            Some(&ownership),
        )
        .await?
        .ok_or(AppError::NotFound)?;
        located.record = fresh.0;
        located.bytes = fresh.1;
        Some(ownership)
    } else {
        None
    };

    // Trash is an additional lifecycle hint, not a tombstone or fence. The
    // owned exact-holder stage above is what excludes concurrent removal.
    let deferred_by_trash = if name.trash.is_none() {
        let candidates =
            list_replica_names(fragment_id, &name.logical_key, None, 128, true).await?;
        let mut matching_trash = false;
        for candidate in candidates
            .into_iter()
            .filter(|candidate| candidate.logical_key == name.logical_key)
        {
            if let Ok(trash_record) = read_canonical_record(fragment_id, &candidate).await
                && trash_record.bytes == located.bytes
            {
                matching_trash = true;
                break;
            }
        }
        matching_trash
    } else {
        false
    };

    let read_action = if name.trash.is_some() {
        PeerNcnxAction::ReadTrash
    } else {
        PeerNcnxAction::ReadLive
    };
    let repair_action = if name.trash.is_some() {
        PeerNcnxAction::RepairTrash
    } else {
        PeerNcnxAction::RepairLive
    };
    let mut observations = Vec::with_capacity(layout.replicas.len());
    let mut repair_targets = Vec::new();
    for replica in &layout.replicas {
        let mut request = request_for_replica(
            read_action.clone(),
            &layout,
            replica,
            fragment_id,
            &name.logical_key,
            name.trash.clone(),
            None,
        );
        if let Some(ownership) = &ownership {
            ownership.attach(&mut request, &layout, replica)?;
        }
        let response = send_to_replica(&runtime, replica, request).await;
        match response {
            Ok(response) if response.bytes.as_deref() == Some(&located.bytes) => {
                observations.push(NcnxReplicaObservation {
                    node_id: replica.node_id.clone(),
                    root_id: replica.root_id.clone(),
                    status: "healthy".into(),
                    error: None,
                });
            }
            Ok(response) => {
                let status = if response.bytes.is_some() {
                    "divergent"
                } else {
                    "missing"
                };
                observations.push(NcnxReplicaObservation {
                    node_id: replica.node_id.clone(),
                    root_id: replica.root_id.clone(),
                    status: status.into(),
                    error: None,
                });
                repair_targets.push(replica.clone());
            }
            Err(err) => {
                let status = if matches!(&err, AppError::ServiceUnavailable(_)) {
                    "unavailable"
                } else {
                    "corrupt"
                };
                observations.push(NcnxReplicaObservation {
                    node_id: replica.node_id.clone(),
                    root_id: replica.root_id.clone(),
                    status: status.into(),
                    error: Some(err.to_string()),
                });
                repair_targets.push(replica.clone());
            }
        }
    }
    let degraded = !repair_targets.is_empty();
    let mut repaired_replicas = 0u64;
    if repair && !deferred_by_trash {
        for replica in repair_targets {
            let mut request = request_for_replica(
                repair_action.clone(),
                &layout,
                &replica,
                fragment_id,
                &name.logical_key,
                name.trash.clone(),
                Some(&located.bytes),
            );
            ownership.as_ref().expect("repair owns stage").attach(
                &mut request,
                &layout,
                &replica,
            )?;
            let response = send_to_replica(&runtime, &replica, request).await;
            if let Some(observation) = observations.iter_mut().find(|observation| {
                observation.node_id == replica.node_id && observation.root_id == replica.root_id
            }) {
                match response {
                    Ok(_) => {
                        repaired_replicas = repaired_replicas.saturating_add(1);
                        observation.status = "repaired".into();
                        observation.error = None;
                    }
                    Err(error) => observation.error = Some(error.to_string()),
                }
            }
        }
    }
    let healthy = !degraded
        || (repair
            && !deferred_by_trash
            && observations
                .iter()
                .all(|observation| matches!(observation.status.as_str(), "healthy" | "repaired")));
    if let Some(ownership) = ownership {
        ownership.release().await;
    }
    Ok(NcnxRepairOutcome {
        layout,
        located: Some(located),
        observations,
        degraded,
        repaired_replicas,
        healthy,
        deferred_by_trash,
    })
}

pub async fn inspect_name(
    fragment_id: &str,
    name: &PeerNcnxName,
) -> Result<NcnxRepairOutcome, AppError> {
    inspect_name_inner(fragment_id, name, false).await
}

pub async fn inspect_and_repair_name(
    fragment_id: &str,
    name: &PeerNcnxName,
) -> Result<NcnxRepairOutcome, AppError> {
    inspect_name_inner(fragment_id, name, true).await
}

pub fn ncnx_cursor(key: &str, trash: Option<&TrashLocator>) -> String {
    match trash {
        Some(locator) => format!(
            "{}\0{:020}\0{}",
            key, locator.deleted_at_unix_nanos, locator.delete_mutation_id
        ),
        None => key.to_string(),
    }
}

pub fn unique_object_ids(records: &[LocatedNcnx]) -> BTreeSet<String> {
    records
        .iter()
        .map(|record| record.record.object_id.clone())
        .collect()
}
