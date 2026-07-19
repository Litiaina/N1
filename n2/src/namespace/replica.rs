use std::collections::BTreeSet;
use std::path::PathBuf;

use serde::Serialize;

use crate::distribution::membership::active_cluster_runtime;
use crate::distribution::ncnx::{inspect_name, list_replica_names, ncnx_cursor};
use crate::enums::app_error_enum::AppError;

const INSPECTION_PAGE_SIZE: usize = 1_000;

#[derive(Debug, Clone, Serialize, Default)]
pub struct NamespaceReplicaInspection {
    pub fragment: String,
    pub records_checked: u64,
    pub expected_replicas: u64,
    pub present_replicas: u64,
    pub valid_replicas: u64,
    pub missing_replicas: u64,
    pub divergent_replicas: u64,
    pub corrupt_replicas: u64,
    pub healthy: bool,
    pub strict_replicas_verified: bool,
    pub replica_counts_complete: bool,
    pub canonical_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct NamespaceReplicaRepairReport {
    pub fragment: String,
    pub expected_replicas: u64,
    pub missing_repaired: u64,
    pub divergent_repaired: u64,
    pub corrupt_found: u64,
    pub selected_source: Option<String>,
}

fn observation_is_corrupt(error: Option<&str>) -> bool {
    error.is_some_and(|error| {
        let error = error.to_ascii_lowercase();
        error.contains("decode ncnx")
            || error.contains("integrity mismatch")
            || error.contains("crc")
    })
}

pub async fn inspect_namespace_replicas(
    fragment: &str,
    _storage_roots: &[PathBuf],
    deep_scan: bool,
) -> Result<NamespaceReplicaInspection, AppError> {
    let mut inspection = NamespaceReplicaInspection {
        fragment: fragment.to_string(),
        healthy: true,
        canonical_path: Some("@f_<fragment>/.namespace/<key>.ncnx".into()),
        ..NamespaceReplicaInspection::default()
    };

    // A shallow check proves that every configured node can participate in
    // namespace inventory. It deliberately does not claim replica integrity.
    if !deep_scan {
        for trash in [false, true] {
            list_replica_names(fragment, "", None, 1, trash).await?;
        }
        return Ok(inspection);
    }

    let runtime = active_cluster_runtime()?;
    let fallback_replicas = runtime
        .membership
        .newest_writable_pool()
        .map(|pool| pool.policy.namespace_replicas() as u64)
        .unwrap_or(0);
    let mut seen = BTreeSet::new();
    for trash in [false, true] {
        let mut cursor = None;
        loop {
            let page =
                list_replica_names(fragment, "", cursor.as_deref(), INSPECTION_PAGE_SIZE, trash)
                    .await?;
            if page.is_empty() {
                break;
            }
            cursor = page
                .last()
                .map(|name| ncnx_cursor(&name.logical_key, name.trash.as_ref()));
            let full = page.len() == INSPECTION_PAGE_SIZE;
            for name in page {
                let identity = ncnx_cursor(&name.logical_key, name.trash.as_ref());
                if !seen.insert((trash, identity)) {
                    continue;
                }
                inspection.records_checked = inspection.records_checked.saturating_add(1);
                match inspect_name(fragment, &name).await {
                    Ok(outcome) => {
                        inspection.expected_replicas = inspection
                            .expected_replicas
                            .saturating_add(outcome.layout.replicas.len() as u64);
                        for observation in outcome.observations {
                            match observation.status.as_str() {
                                "healthy" => {
                                    inspection.present_replicas =
                                        inspection.present_replicas.saturating_add(1);
                                    inspection.valid_replicas =
                                        inspection.valid_replicas.saturating_add(1);
                                }
                                "divergent" => {
                                    inspection.present_replicas =
                                        inspection.present_replicas.saturating_add(1);
                                    inspection.divergent_replicas =
                                        inspection.divergent_replicas.saturating_add(1);
                                }
                                "unreachable_or_corrupt"
                                    if observation_is_corrupt(observation.error.as_deref()) =>
                                {
                                    inspection.present_replicas =
                                        inspection.present_replicas.saturating_add(1);
                                    inspection.corrupt_replicas =
                                        inspection.corrupt_replicas.saturating_add(1);
                                }
                                _ => {
                                    inspection.missing_replicas =
                                        inspection.missing_replicas.saturating_add(1);
                                }
                            }
                        }
                        if outcome.degraded {
                            inspection.healthy = false;
                        }
                    }
                    Err(_) => {
                        inspection.expected_replicas = inspection
                            .expected_replicas
                            .saturating_add(fallback_replicas);
                        inspection.missing_replicas = inspection
                            .missing_replicas
                            .saturating_add(fallback_replicas);
                        inspection.healthy = false;
                    }
                }
            }
            if !full {
                break;
            }
        }
    }
    inspection.strict_replicas_verified = true;
    inspection.replica_counts_complete = true;
    Ok(inspection)
}

pub async fn repair_namespace_replicas(
    fragment: &str,
    storage_roots: &[PathBuf],
    dry_run: bool,
) -> Result<NamespaceReplicaRepairReport, AppError> {
    let inspection = inspect_namespace_replicas(fragment, storage_roots, true).await?;
    if dry_run {
        return Ok(NamespaceReplicaRepairReport {
            fragment: fragment.to_string(),
            expected_replicas: inspection.expected_replicas,
            corrupt_found: inspection.corrupt_replicas,
            selected_source: inspection.canonical_path,
            ..NamespaceReplicaRepairReport::default()
        });
    }
    let runtime = active_cluster_runtime()?;
    let result = crate::distribution::namespace_healing::scan_and_repair_fragment_namespace(
        runtime,
        fragment,
        &format!("manual-{}", ulid::Ulid::new()),
    )
    .await;
    if !result.complete {
        return Err(AppError::ServiceUnavailable(
            "one or more NCNX records could not be repaired".into(),
        ));
    }
    Ok(NamespaceReplicaRepairReport {
        fragment: fragment.to_string(),
        expected_replicas: inspection.expected_replicas,
        missing_repaired: result.repaired_records,
        corrupt_found: inspection.corrupt_replicas,
        selected_source: inspection.canonical_path,
        ..NamespaceReplicaRepairReport::default()
    })
}
