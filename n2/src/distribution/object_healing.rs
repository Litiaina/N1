use crate::core::repair_manager::{RepairCheckMode, repair_cluster_object_recorded_layout};
use crate::distribution::healing_report::AutoHealingReport;
use crate::distribution::membership::ClusterRuntime;
use crate::distribution::namespace_healing::FragmentNamespaceHealingResult;
use crate::distribution::peer::{
    collect_cluster_object_inventory, inspect_and_repair_metadata_replicas,
    resolve_cluster_object_versions,
};
use crate::distribution::status::inspect_cluster_object_status;
use serde_json::json;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Default)]
pub struct FragmentObjectHealingResult {
    pub scanned_objects: u64,
    pub repaired_objects: u64,
    pub repaired_shards: u64,
    pub unrecoverable_objects: u64,
    pub failed_objects: u64,
    pub headless_objects: u64,
    pub reports: Vec<AutoHealingReport>,
    pub continuation_token: Option<String>,
}

#[allow(clippy::too_many_arguments)]
pub async fn scan_and_repair_fragment_objects(
    runtime: Arc<ClusterRuntime>,
    fragment: &str,
    scan_id: &str,
    namespace: &FragmentNamespaceHealingResult,
    check_mode: RepairCheckMode,
    continuation_token: Option<&str>,
    max_objects: u64,
    max_duration: Duration,
) -> FragmentObjectHealingResult {
    let started = Instant::now();
    let mut result = FragmentObjectHealingResult::default();
    let referenced = namespace
        .live_objects
        .keys()
        .chain(namespace.trash_objects.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let ordered = referenced.iter().cloned().collect::<Vec<_>>();
    let start_index = continuation_token
        .and_then(|token| ordered.iter().position(|id| id == token))
        .map(|index| index.saturating_add(1))
        .unwrap_or(0);
    let mut last_scanned = None;

    for object_id in ordered.iter().skip(start_index) {
        if result.scanned_objects >= max_objects || started.elapsed() >= max_duration {
            break;
        }
        result.scanned_objects = result.scanned_objects.saturating_add(1);
        last_scanned = Some(object_id.clone());
        let logical_path = namespace
            .live_objects
            .get(object_id)
            .or_else(|| namespace.trash_objects.get(object_id))
            .and_then(|paths| paths.first())
            .cloned();

        let versions = match resolve_cluster_object_versions(fragment, object_id).await {
            Ok(versions) if !versions.is_empty() => versions,
            Ok(_) => {
                result.unrecoverable_objects = result.unrecoverable_objects.saturating_add(1);
                result.reports.push(dangling_object_report(
                    &runtime,
                    fragment,
                    scan_id,
                    object_id,
                    logical_path,
                    "no immutable NCDX metadata survives for the namespace object",
                ));
                continue;
            }
            Err(error) => {
                result.failed_objects = result.failed_objects.saturating_add(1);
                result.reports.push(dangling_object_report(
                    &runtime,
                    fragment,
                    scan_id,
                    object_id,
                    logical_path,
                    &format!("object metadata could not be resolved: {error}"),
                ));
                continue;
            }
        };

        for metadata in versions {
            let before = match inspect_cluster_object_status(&metadata, check_mode).await {
                Ok(status) => status,
                Err(error) => {
                    result.failed_objects = result.failed_objects.saturating_add(1);
                    result.reports.push(object_error_report(
                        &runtime,
                        fragment,
                        scan_id,
                        object_id,
                        &metadata.version_id,
                        logical_path.clone(),
                        &format!("object health inspection failed: {error}"),
                    ));
                    continue;
                }
            };
            let metadata_repair = match inspect_and_repair_metadata_replicas(&metadata).await {
                Ok(outcome) => outcome,
                Err(error) => {
                    result.failed_objects = result.failed_objects.saturating_add(1);
                    result.reports.push(object_error_report(
                        &runtime,
                        fragment,
                        scan_id,
                        object_id,
                        &metadata.version_id,
                        logical_path.clone(),
                        &format!("NCDX replica inspection failed: {error}"),
                    ));
                    continue;
                }
            };
            let was_degraded = before.status != "healthy"
                || metadata_repair.repaired_replicas > 0
                || !metadata_repair.healthy;
            if !was_degraded {
                continue;
            }

            let after = if before.status == "healthy" {
                before.clone()
            } else if !before.repairable {
                before.clone()
            } else {
                match repair_cluster_object_recorded_layout(&metadata, check_mode).await {
                    Ok(status) => status,
                    Err(error) => {
                        result.failed_objects = result.failed_objects.saturating_add(1);
                        result.reports.push(object_repair_report(
                            &runtime,
                            fragment,
                            scan_id,
                            object_id,
                            &metadata.version_id,
                            logical_path.clone(),
                            &before,
                            None,
                            &metadata_repair,
                            Some(error.to_string()),
                        ));
                        continue;
                    }
                }
            };

            let healthy =
                after.status == "healthy" && after.failed_repairs == 0 && metadata_repair.healthy;
            if healthy {
                result.repaired_objects = result.repaired_objects.saturating_add(1);
                result.repaired_shards =
                    result.repaired_shards.saturating_add(after.repaired_shards);
            } else {
                result.unrecoverable_objects = result.unrecoverable_objects.saturating_add(1);
            }
            result.reports.push(object_repair_report(
                &runtime,
                fragment,
                scan_id,
                object_id,
                &metadata.version_id,
                logical_path.clone(),
                &before,
                Some(&after),
                &metadata_repair,
                (!healthy).then(|| {
                    if !metadata_repair.healthy {
                        "one or more exact NCDX replicas remain missing or divergent".into()
                    } else {
                        format!("object remained {} after repair", after.status)
                    }
                }),
            ));
        }
    }

    let processed_to_end =
        start_index.saturating_add(result.scanned_objects as usize) >= ordered.len();
    result.continuation_token = (!processed_to_end).then_some(last_scanned).flatten();

    // Headless classification is valid only after the namespace traversal was
    // complete. The blob inventory is physical and independent of namespace.
    if namespace.complete {
        match collect_cluster_object_inventory(fragment).await {
            Ok(inventory) if inventory.complete() => {
                for object_id in inventory.objects.keys() {
                    if referenced.contains(object_id) {
                        continue;
                    }
                    result.headless_objects = result.headless_objects.saturating_add(1);
                    result.reports.push(headless_object_report(
                        &runtime, fragment, scan_id, object_id,
                    ));
                }
            }
            Ok(inventory) => {
                result.failed_objects = result.failed_objects.saturating_add(1);
                result.reports.push(blob_inventory_report(
                    &runtime,
                    fragment,
                    scan_id,
                    inventory.expected_blob_nodes,
                    inventory.responding_blob_nodes,
                    inventory.errors,
                    false,
                ));
            }
            Err(error) => {
                let immutable_divergence =
                    matches!(&error, crate::enums::app_error_enum::AppError::Conflict(_));
                result.failed_objects = result.failed_objects.saturating_add(1);
                result.reports.push(blob_inventory_report(
                    &runtime,
                    fragment,
                    scan_id,
                    runtime.membership.policy.object_data_shards
                        + runtime.membership.policy.object_parity_shards,
                    0,
                    vec![error.to_string()],
                    immutable_divergence,
                ));
            }
        }
    }

    result
}

fn object_repair_report(
    runtime: &ClusterRuntime,
    fragment: &str,
    scan_id: &str,
    object_id: &str,
    version_id: &str,
    logical_path: Option<String>,
    before: &crate::core::repair_manager::ObjectRepairStatus,
    after: Option<&crate::core::repair_manager::ObjectRepairStatus>,
    metadata: &crate::distribution::peer::MetadataReplicaRepairOutcome,
    reason: Option<String>,
) -> AutoHealingReport {
    let healthy = after.is_some_and(|after| after.status == "healthy" && after.failed_repairs == 0)
        && metadata.healthy;
    let mut report = AutoHealingReport::new(
        scan_id,
        runtime.membership.epoch,
        fragment,
        &runtime.local_node_id,
        "object_degraded",
        "object",
    );
    report.object_id = Some(object_id.into());
    report.version_id = Some(version_id.into());
    report.logical_path = logical_path;
    report.initial_status = before.status.clone();
    report.repair_attempted = true;
    report.repair_outcome = if healthy {
        "repair_succeeded"
    } else {
        "repair_failed"
    }
    .into();
    report.cluster_read_only_triggered = !healthy;
    report.object_health_before = Some(json!(before));
    report.object_health_after = after.map(|status| json!(status));
    report.observations = metadata
        .observations
        .iter()
        .map(|observation| json!(observation))
        .collect();
    report.repaired_replicas = metadata.repaired_replicas;
    report.repaired_shards = after.map_or(0, |status| status.repaired_shards);
    report.reason = reason;
    report.completed_at_unix_nanos = now_nanos();
    report
}

fn object_error_report(
    runtime: &ClusterRuntime,
    fragment: &str,
    scan_id: &str,
    object_id: &str,
    version_id: &str,
    logical_path: Option<String>,
    reason: &str,
) -> AutoHealingReport {
    let mut report = AutoHealingReport::new(
        scan_id,
        runtime.membership.epoch,
        fragment,
        &runtime.local_node_id,
        "object_repair_failed",
        "object",
    );
    report.object_id = Some(object_id.into());
    report.version_id = Some(version_id.into());
    report.logical_path = logical_path;
    report.initial_status = "critical".into();
    report.repair_attempted = true;
    report.repair_outcome = "repair_failed".into();
    report.cluster_read_only_triggered = true;
    report.reason = Some(reason.into());
    report.completed_at_unix_nanos = now_nanos();
    report
}

fn dangling_object_report(
    runtime: &ClusterRuntime,
    fragment: &str,
    scan_id: &str,
    object_id: &str,
    logical_path: Option<String>,
    reason: &str,
) -> AutoHealingReport {
    let mut report = object_error_report(
        runtime,
        fragment,
        scan_id,
        object_id,
        "unknown",
        logical_path,
        reason,
    );
    report.finding = "dangling_object_pointer".into();
    report.entry_type = "dangling-object".into();
    report.repair_attempted = false;
    report.repair_outcome = "unrepairable".into();
    report
}

fn headless_object_report(
    runtime: &ClusterRuntime,
    fragment: &str,
    scan_id: &str,
    object_id: &str,
) -> AutoHealingReport {
    let mut report = AutoHealingReport::new(
        scan_id,
        runtime.membership.epoch,
        fragment,
        &runtime.local_node_id,
        "headless_object",
        "headless-object",
    );
    report.object_id = Some(object_id.into());
    report.initial_status = "headless".into();
    report.repair_outcome = "preserved_reported".into();
    report.cluster_read_only_triggered = false;
    report.reason = Some(
        "published NCDX object has no live or trash namespace reference; it was preserved because this can be failed-publication residue and does not prove namespace loss"
            .into(),
    );
    report.completed_at_unix_nanos = now_nanos();
    report
}

fn blob_inventory_report(
    runtime: &ClusterRuntime,
    fragment: &str,
    scan_id: &str,
    expected: usize,
    responding: usize,
    errors: Vec<String>,
    immutable_divergence: bool,
) -> AutoHealingReport {
    let mut report = AutoHealingReport::new(
        scan_id,
        runtime.membership.epoch,
        fragment,
        &runtime.local_node_id,
        "blob_inventory_incomplete",
        "fragment-blob-inventory",
    );
    report.initial_status = if immutable_divergence {
        "critical"
    } else {
        "incomplete"
    }
    .into();
    report.repair_attempted = false;
    report.repair_outcome = if immutable_divergence {
        "immutable_divergence"
    } else {
        "deferred"
    }
    .into();
    report.cluster_read_only_triggered = immutable_divergence;
    report.observations = errors
        .into_iter()
        .map(|error| json!({ "error": error }))
        .collect();
    report.reason = Some(format!(
        "only {responding}/{expected} configured blob nodes returned a physical object inventory"
    ));
    report.completed_at_unix_nanos = now_nanos();
    report
}

fn now_nanos() -> i64 {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| chrono::Utc::now().timestamp().saturating_mul(1_000_000_000))
}
