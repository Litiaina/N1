use super::*;

async fn inspect_name_inner(
    fragment_id: &str,
    name: &PeerNcnxName,
    repair: bool,
) -> Result<NcnxRepairOutcome, AppError> {
    let runtime = active_cluster_runtime()?;
    let located = match read_canonical_record(fragment_id, name).await {
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

    // Destination-first trash publication is a tombstone for the old live
    // pointer. A healer must not resurrect that pointer while deletion is
    // removing its replicas.
    let deferred_by_trash = if name.trash.is_none() {
        let candidates =
            list_replica_names(fragment_id, &name.logical_key, None, 128, true).await?;
        let mut tombstone = false;
        for candidate in candidates
            .into_iter()
            .filter(|candidate| candidate.logical_key == name.logical_key)
        {
            if let Ok(trash_record) = read_canonical_record(fragment_id, &candidate).await
                && trash_record.bytes == located.bytes
            {
                tombstone = true;
                break;
            }
        }
        tombstone
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
        let response = send_to_replica(
            &runtime,
            replica,
            request_for_replica(
                read_action.clone(),
                &layout,
                replica,
                fragment_id,
                &name.logical_key,
                name.trash.clone(),
                None,
            ),
        )
        .await;
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
            let response = send_to_replica(
                &runtime,
                &replica,
                request_for_replica(
                    repair_action.clone(),
                    &layout,
                    &replica,
                    fragment_id,
                    &name.logical_key,
                    name.trash.clone(),
                    Some(&located.bytes),
                ),
            )
            .await;
            if response.is_ok() {
                repaired_replicas = repaired_replicas.saturating_add(1);
                if let Some(observation) = observations.iter_mut().find(|observation| {
                    observation.node_id == replica.node_id && observation.root_id == replica.root_id
                }) {
                    observation.status = "repaired".into();
                    observation.error = None;
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
