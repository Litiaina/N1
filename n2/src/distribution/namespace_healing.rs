use crate::distribution::healing_report::AutoHealingReport;
use crate::distribution::membership::ClusterRuntime;
use crate::distribution::ncnx::{
    PeerNcnxName, inspect_and_repair_name, list_replica_names, ncnx_cursor, trash_is_expired,
};
use crate::enums::app_error_enum::AppError;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

const HEALING_DISCOVERY_PAGE: usize = 1_000;

#[derive(Clone, Debug, Default)]
pub struct FragmentNamespaceHealingResult {
    pub complete: bool,
    pub scanned_records: u64,
    pub repaired_records: u64,
    pub live_objects: BTreeMap<String, Vec<String>>,
    pub trash_objects: BTreeMap<String, Vec<String>>,
    pub reports: Vec<AutoHealingReport>,
}

pub async fn scan_and_repair_fragment_namespace(
    runtime: Arc<ClusterRuntime>,
    fragment: &str,
    scan_id: &str,
) -> FragmentNamespaceHealingResult {
    let mut result = FragmentNamespaceHealingResult {
        complete: true,
        ..FragmentNamespaceHealingResult::default()
    };
    let mut seen = BTreeSet::new();
    for trash in [false, true] {
        let mut cursor = None;
        loop {
            let page = match list_replica_names(
                fragment,
                "",
                cursor.as_deref(),
                HEALING_DISCOVERY_PAGE,
                trash,
            )
            .await
            {
                Ok(page) => page,
                Err(error) => {
                    result.complete = false;
                    result
                        .reports
                        .push(discovery_report(&runtime, fragment, scan_id, trash, error));
                    break;
                }
            };
            if page.is_empty() {
                break;
            }
            cursor = page
                .last()
                .map(|name| ncnx_cursor(&name.logical_key, name.trash.as_ref()));
            let full = page.len() == HEALING_DISCOVERY_PAGE;
            for name in page {
                let identity = ncnx_cursor(&name.logical_key, name.trash.as_ref());
                if !seen.insert((trash, identity)) {
                    continue;
                }
                // Once TTL has elapsed, GC exclusively owns this trash
                // generation. It reclaims immutable object holders before
                // removing NCNX for restart safety, so healing must neither
                // repair nor classify the transitional pointer as damage.
                if name.trash.as_ref().is_some_and(trash_is_expired) {
                    continue;
                }
                result.scanned_records = result.scanned_records.saturating_add(1);
                match inspect_and_repair_name(fragment, &name).await {
                    Ok(outcome) => {
                        if let Some(located) = &outcome.located {
                            let map = if trash {
                                &mut result.trash_objects
                            } else {
                                &mut result.live_objects
                            };
                            map.entry(located.record.object_id.clone())
                                .or_default()
                                .push(name.logical_key.clone());
                        }
                        if outcome.degraded {
                            let verification_deferred = ncnx_only_temporarily_unavailable(&outcome);
                            if outcome.healthy {
                                result.repaired_records = result.repaired_records.saturating_add(1);
                            } else if !outcome.deferred_by_trash || verification_deferred {
                                // An incomplete transport view must prevent
                                // headless classification, but it is not proof
                                // that an immutable namespace replica is lost.
                                result.complete = false;
                            }
                            result
                                .reports
                                .push(repair_report(&runtime, fragment, scan_id, &name, outcome));
                        }
                    }
                    Err(error) => {
                        // Discovery is intentionally lockless. A namespace mutation can
                        // publish its destination and remove this physical name between
                        // the inventory call above and the record read. Revalidate the
                        // exact physical identity before calling it corruption. If it no
                        // longer exists, this scan crossed a valid mutation boundary; an
                        // incomplete scan also prevents object healing from making a
                        // false headless-object classification from the mixed view.
                        if is_all_replicas_unreadable(&error)
                            && matches!(name_is_still_advertised(fragment, &name).await, Ok(false))
                        {
                            result.complete = false;
                            tracing::debug!(
                                fragment,
                                logical_key = %name.logical_key,
                                trash = name.trash.is_some(),
                                "NCNX discovery was superseded by a concurrent namespace mutation"
                            );
                            continue;
                        }
                        result.complete = false;
                        result.reports.push(unreadable_record_report(
                            &runtime, fragment, scan_id, &name, error,
                        ));
                    }
                }
            }
            if !full {
                break;
            }
        }
    }
    result
}

fn ncnx_only_temporarily_unavailable(
    outcome: &crate::distribution::ncnx::NcnxRepairOutcome,
) -> bool {
    let mut unavailable = false;
    for observation in &outcome.observations {
        match observation.status.as_str() {
            "unavailable" => unavailable = true,
            "missing" | "divergent" | "corrupt" => return false,
            _ => {}
        }
    }
    !outcome.healthy && unavailable
}

fn is_all_replicas_unreadable(error: &AppError) -> bool {
    matches!(
        error,
        AppError::Conflict(message)
            if message.starts_with("all NCNX replicas are unreadable for key '")
    )
}

async fn name_is_still_advertised(fragment: &str, target: &PeerNcnxName) -> Result<bool, AppError> {
    let target_identity = ncnx_cursor(&target.logical_key, target.trash.as_ref());
    let mut cursor = None;
    loop {
        let page = list_replica_names(
            fragment,
            &target.logical_key,
            cursor.as_deref(),
            HEALING_DISCOVERY_PAGE,
            target.trash.is_some(),
        )
        .await?;
        if page
            .iter()
            .any(|name| ncnx_cursor(&name.logical_key, name.trash.as_ref()) == target_identity)
        {
            return Ok(true);
        }
        if page.len() < HEALING_DISCOVERY_PAGE {
            return Ok(false);
        }
        cursor = page
            .last()
            .map(|name| ncnx_cursor(&name.logical_key, name.trash.as_ref()));
    }
}

fn repair_report(
    runtime: &ClusterRuntime,
    fragment: &str,
    scan_id: &str,
    name: &PeerNcnxName,
    outcome: crate::distribution::ncnx::NcnxRepairOutcome,
) -> AutoHealingReport {
    let verification_deferred = ncnx_only_temporarily_unavailable(&outcome);
    let mut report = AutoHealingReport::new(
        scan_id,
        runtime.membership.epoch,
        fragment,
        &runtime.local_node_id,
        if verification_deferred {
            if name.trash.is_some() {
                "trash_ncnx_verification_incomplete"
            } else {
                "live_ncnx_verification_incomplete"
            }
        } else if name.trash.is_some() {
            "trash_ncnx_degraded"
        } else {
            "live_ncnx_degraded"
        },
        if name.trash.is_some() {
            "trash-ncnx"
        } else {
            "live-ncnx"
        },
    );
    report.namespace_record_id = Some(name.logical_key.clone());
    report.object_id = outcome
        .located
        .as_ref()
        .map(|located| located.record.object_id.clone());
    report.logical_path = Some(name.logical_key.clone());
    report.expected_ncnx_layout = Some(outcome.layout);
    report.repair_attempted = !outcome.deferred_by_trash;
    report.initial_status = if verification_deferred {
        "incomplete".into()
    } else {
        "degraded".into()
    };
    report.repair_outcome = if outcome.healthy {
        "repair_succeeded"
    } else if outcome.deferred_by_trash {
        "deferred_by_trash_tombstone"
    } else if verification_deferred {
        "deferred_topology_incomplete"
    } else {
        "repair_failed"
    }
    .into();
    report.cluster_read_only_triggered =
        !outcome.healthy && !outcome.deferred_by_trash && !verification_deferred;
    report.repaired_replicas = outcome.repaired_replicas;
    report.observations = outcome
        .observations
        .iter()
        .map(|observation| json!(observation))
        .collect();
    report.reason = if outcome.deferred_by_trash {
        Some(
            "a matching trash tombstone exists; live replica repair was skipped to prevent \
             deletion resurrection"
                .into(),
        )
    } else if verification_deferred {
        Some(
            "NCNX verification was deferred because a replica participant was temporarily \
             unavailable; transport unavailability is not proof of namespace loss"
                .into(),
        )
    } else {
        None
    };
    report.completed_at_unix_nanos = now_nanos();
    report
}

fn unreadable_record_report(
    runtime: &ClusterRuntime,
    fragment: &str,
    scan_id: &str,
    name: &PeerNcnxName,
    error: crate::enums::app_error_enum::AppError,
) -> AutoHealingReport {
    let mut report = AutoHealingReport::new(
        scan_id,
        runtime.membership.epoch,
        fragment,
        &runtime.local_node_id,
        "ncnx_all_replicas_unreadable",
        if name.trash.is_some() {
            "trash-ncnx"
        } else {
            "live-ncnx"
        },
    );
    report.namespace_record_id = Some(name.logical_key.clone());
    report.logical_path = Some(name.logical_key.clone());
    report.initial_status = "critical".into();
    report.repair_outcome = "unrepairable".into();
    report.cluster_read_only_triggered = true;
    report.reason = Some(error.to_string());
    report.completed_at_unix_nanos = now_nanos();
    report
}

fn discovery_report(
    runtime: &ClusterRuntime,
    fragment: &str,
    scan_id: &str,
    trash: bool,
    error: crate::enums::app_error_enum::AppError,
) -> AutoHealingReport {
    let mut report = AutoHealingReport::new(
        scan_id,
        runtime.membership.epoch,
        fragment,
        &runtime.local_node_id,
        "ncnx_inventory_incomplete",
        if trash {
            "trash-namespace"
        } else {
            "live-namespace"
        },
    );
    report.initial_status = "incomplete".into();
    report.repair_outcome = "deferred".into();
    report.cluster_read_only_triggered = false;
    report.reason = Some(error.to_string());
    report.completed_at_unix_nanos = now_nanos();
    report
}

fn now_nanos() -> i64 {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| chrono::Utc::now().timestamp().saturating_mul(1_000_000_000))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::ncnx::{
        NcnxLayout, NcnxRepairOutcome, NcnxReplica, NcnxReplicaObservation,
    };

    fn outcome(statuses: &[&str], healthy: bool) -> NcnxRepairOutcome {
        NcnxRepairOutcome {
            layout: NcnxLayout {
                pool_id: "pool-a".into(),
                pool_generation: 1,
                replicas: vec![NcnxReplica {
                    node_id: "node-a".into(),
                    instance_id: "node-a".into(),
                    binding_epoch: 1,
                    root_id: "root-a".into(),
                    endpoint: "https://node-a".into(),
                }],
            },
            located: None,
            observations: statuses
                .iter()
                .enumerate()
                .map(|(index, status)| NcnxReplicaObservation {
                    node_id: format!("node-{index}"),
                    root_id: format!("root-{index}"),
                    status: (*status).into(),
                    error: None,
                })
                .collect(),
            degraded: !healthy,
            repaired_replicas: 0,
            healthy,
            deferred_by_trash: false,
        }
    }

    #[test]
    fn only_exact_all_replica_unreadable_conflicts_are_revalidated() {
        assert!(is_all_replicas_unreadable(&AppError::Conflict(
            "all NCNX replicas are unreadable for key 'videos/a.mp4'".into()
        )));
        assert!(!is_all_replicas_unreadable(&AppError::Conflict(
            "NCNX integrity mismatch".into()
        )));
        assert!(!is_all_replicas_unreadable(&AppError::ServiceUnavailable(
            "node unavailable".into()
        )));
    }

    #[test]
    fn transient_ncnx_transport_loss_is_not_namespace_loss() {
        assert!(ncnx_only_temporarily_unavailable(&outcome(
            &["healthy", "healthy", "unavailable"],
            false,
        )));
    }

    #[test]
    fn missing_divergent_or_corrupt_ncnx_is_not_deferred() {
        for status in ["missing", "divergent", "corrupt"] {
            assert!(!ncnx_only_temporarily_unavailable(&outcome(
                &["healthy", status],
                false,
            )));
        }
    }
}
