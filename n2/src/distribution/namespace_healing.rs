use crate::distribution::healing_report::AutoHealingReport;
use crate::distribution::inode::{InodeRepairOutcome, inspect_and_repair_inode};
use crate::distribution::membership::ClusterRuntime;
use crate::enums::app_error_enum::AppError;
use crate::namespace::catalog::{
    catalog_layout, inspect_and_repair_namespace_catalog, resolve_fragment_namespace,
};
use crate::namespace::inode_store::{InodeDentryTarget, InodeLayout};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[derive(Clone, Debug, Default)]
pub struct FragmentNamespaceHealingResult {
    pub complete: bool,
    pub scanned_inodes: u64,
    pub repaired_inodes: u64,
    pub live_objects: BTreeMap<String, Vec<String>>,
    pub trash_objects: BTreeMap<String, Vec<String>>,
    pub reports: Vec<AutoHealingReport>,
}

struct PendingInode {
    path: String,
    inode_id: String,
    layout: InodeLayout,
    expected_parent: Option<String>,
    trash: bool,
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
    let catalog = inspect_and_repair_namespace_catalog(runtime.clone()).await;
    if catalog.degraded {
        let mut report = AutoHealingReport::new(
            scan_id,
            runtime.membership.epoch,
            fragment,
            &runtime.local_node_id,
            "namespace_catalog_degraded",
            "namespace-catalog",
        );
        report.inode_id = Some("fragment-catalog".into());
        report.expected_inode_layout = catalog_layout(&runtime).ok();
        report.repair_attempted = true;
        report.repair_outcome = if catalog.healthy {
            "repair_succeeded"
        } else if catalog.deferred {
            "deferred"
        } else {
            "repair_failed"
        }
        .into();
        report.cluster_read_only_triggered = !catalog.healthy && !catalog.deferred;
        report.repaired_replicas = catalog.repaired_replicas;
        report.observations = catalog
            .observations
            .iter()
            .map(|observation| json!(observation))
            .collect();
        report.reason = catalog.error;
        report.completed_at_unix_nanos = now_nanos();
        result.reports.push(report);
    }
    if !catalog.healthy {
        result.complete = false;
        return result;
    }
    let namespace = match resolve_fragment_namespace(runtime.clone(), fragment).await {
        Ok(namespace) => namespace,
        Err(error) => {
            result.complete = false;
            result.reports.push(unreachable_fragment_report(
                &runtime, fragment, scan_id, error,
            ));
            return result;
        }
    };

    let mut pending = vec![
        PendingInode {
            path: String::new(),
            inode_id: namespace.root_inode_id,
            layout: namespace.root_layout,
            expected_parent: None,
            trash: false,
        },
        PendingInode {
            path: ".trash".into(),
            inode_id: namespace.trash_inode_id,
            layout: namespace.trash_layout,
            expected_parent: None,
            trash: true,
        },
    ];
    let mut visited = BTreeSet::new();

    while let Some(current) = pending.pop() {
        if !visited.insert(current.inode_id.clone()) {
            result.complete = false;
            result.reports.push(simple_namespace_report(
                &runtime,
                &namespace.stable_fragment_id,
                scan_id,
                "directory_cycle_or_duplicate",
                "directory-inode",
                Some(current.inode_id),
                Some(current.path),
                current.layout,
                "unrepairable",
                "the same directory inode was reached more than once",
            ));
            continue;
        }

        result.scanned_inodes = result.scanned_inodes.saturating_add(1);
        let outcome = inspect_and_repair_inode(
            runtime.clone(),
            &namespace.stable_fragment_id,
            &current.inode_id,
            &current.layout,
        )
        .await;
        if outcome.repaired_replicas > 0 {
            result.repaired_inodes = result.repaired_inodes.saturating_add(1);
        }
        if outcome.degraded {
            result.reports.push(inode_repair_report(
                &runtime,
                &namespace.stable_fragment_id,
                scan_id,
                &current,
                &outcome,
            ));
        }
        let Some(snapshot) = outcome.snapshot else {
            result.complete = false;
            continue;
        };
        if !outcome.healthy {
            result.complete = false;
            continue;
        }
        if snapshot.inode.parent_inode_id != current.expected_parent {
            result.complete = false;
            result.reports.push(simple_namespace_report(
                &runtime,
                &namespace.stable_fragment_id,
                scan_id,
                "directory_parent_mismatch",
                "directory-inode",
                Some(current.inode_id.clone()),
                Some(current.path.clone()),
                current.layout.clone(),
                "unrepairable",
                "directory inode parent identity differs from the parent dentry",
            ));
            continue;
        }

        for entry in snapshot.entries {
            let path = join_path(&current.path, &entry.name);
            match entry.target {
                InodeDentryTarget::Directory { inode_id, layout } => pending.push(PendingInode {
                    path,
                    inode_id,
                    layout,
                    expected_parent: Some(current.inode_id.clone()),
                    trash: current.trash,
                }),
                InodeDentryTarget::Object { object_id } => {
                    let objects = if current.trash {
                        &mut result.trash_objects
                    } else {
                        &mut result.live_objects
                    };
                    objects.entry(object_id).or_default().push(path);
                }
                InodeDentryTarget::Symlink { .. } => {}
                InodeDentryTarget::Fragment { .. } => {
                    result.complete = false;
                    result.reports.push(simple_namespace_report(
                        &runtime,
                        &namespace.stable_fragment_id,
                        scan_id,
                        "fragment_target_inside_namespace",
                        "directory-inode",
                        Some(current.inode_id.clone()),
                        Some(path),
                        current.layout.clone(),
                        "unrepairable",
                        "a fragment catalog target appeared inside a fragment namespace",
                    ));
                }
            }
        }
    }

    result
}

fn inode_repair_report(
    runtime: &ClusterRuntime,
    fragment: &str,
    scan_id: &str,
    pending: &PendingInode,
    outcome: &InodeRepairOutcome,
) -> AutoHealingReport {
    let missing_target = outcome.snapshot.is_none();
    let root_unreachable = missing_target && pending.path.is_empty();
    let mut report = AutoHealingReport::new(
        scan_id,
        runtime.membership.epoch,
        fragment,
        &runtime.local_node_id,
        if outcome.deferred {
            "directory_scan_deferred"
        } else if root_unreachable {
            "fragment_namespace_unreachable"
        } else if missing_target {
            "dangling_directory_pointer"
        } else {
            "directory_inode_degraded"
        },
        if outcome.deferred {
            "directory-inode"
        } else if root_unreachable {
            "fragment-namespace"
        } else if missing_target {
            "dangling-directory"
        } else {
            "directory-inode"
        },
    );
    report.inode_id = Some(pending.inode_id.clone());
    report.logical_path = Some(pending.path.clone());
    report.expected_inode_layout = Some(pending.layout.clone());
    if outcome.deferred {
        report.initial_status = "busy".into();
    }
    report.repair_attempted = true;
    report.repair_outcome = if missing_target {
        if outcome.deferred {
            "deferred"
        } else {
            "unrepairable"
        }
    } else if outcome.healthy {
        "repair_succeeded"
    } else if outcome.deferred {
        "deferred"
    } else {
        "repair_failed"
    }
    .into();
    report.cluster_read_only_triggered = !outcome.healthy && !outcome.deferred;
    report.repaired_replicas = outcome.repaired_replicas;
    report.observations = outcome
        .observations
        .iter()
        .map(|observation| json!(observation))
        .collect();
    report.reason = outcome.error.clone();
    report.completed_at_unix_nanos = now_nanos();
    report
}

fn unreachable_fragment_report(
    runtime: &ClusterRuntime,
    fragment: &str,
    scan_id: &str,
    error: AppError,
) -> AutoHealingReport {
    let mut report = AutoHealingReport::new(
        scan_id,
        runtime.membership.epoch,
        fragment,
        &runtime.local_node_id,
        "fragment_namespace_unreachable",
        "fragment-namespace",
    );
    report.initial_status = "critical".into();
    report.repair_outcome = "unrepairable".into();
    report.cluster_read_only_triggered = true;
    report.reason = Some(error.to_string());
    report.completed_at_unix_nanos = now_nanos();
    report
}

#[allow(clippy::too_many_arguments)]
fn simple_namespace_report(
    runtime: &ClusterRuntime,
    fragment: &str,
    scan_id: &str,
    finding: &str,
    entry_type: &str,
    inode_id: Option<String>,
    logical_path: Option<String>,
    layout: InodeLayout,
    outcome: &str,
    reason: &str,
) -> AutoHealingReport {
    let mut report = AutoHealingReport::new(
        scan_id,
        runtime.membership.epoch,
        fragment,
        &runtime.local_node_id,
        finding,
        entry_type,
    );
    report.inode_id = inode_id;
    report.logical_path = logical_path;
    report.expected_inode_layout = Some(layout);
    report.initial_status = "critical".into();
    report.repair_outcome = outcome.into();
    report.cluster_read_only_triggered = true;
    report.reason = Some(reason.into());
    report.completed_at_unix_nanos = now_nanos();
    report
}

fn join_path(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{parent}/{name}")
    }
}

fn now_nanos() -> i64 {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| chrono::Utc::now().timestamp().saturating_mul(1_000_000_000))
}
