use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::distribution::dentry_group::configured_namespace_replicas;
use crate::distribution::dentry_group::quorum_required;
use crate::distribution::dentry_service::DistributedNamespace;
use crate::enums::app_error_enum::AppError;

#[derive(Debug, Clone, Serialize, Default)]
pub struct NamespaceReplicaInspection {
    pub fragment: String,
    pub expected_replicas: u64,
    pub present_replicas: u64,
    pub valid_replicas: u64,
    pub missing_replicas: u64,
    pub divergent_replicas: u64,
    pub corrupt_replicas: u64,
    pub healthy: bool,
    pub quorum_verified: bool,
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

pub fn namespace_db_path_for_root(root: &Path, _fragment: &str, partition_id: u16) -> PathBuf {
    root.join(".n2")
        .join("namespace")
        .join("partitions")
        .join(format!("{partition_id:016x}"))
}

pub fn namespace_db_paths_for_roots(fragment: &str, roots: &[PathBuf]) -> Vec<PathBuf> {
    roots
        .iter()
        .map(|root| namespace_db_path_for_root(root, fragment, 0))
        .collect()
}

pub async fn inspect_namespace_replicas(
    fragment: &str,
    _storage_roots: &[PathBuf],
) -> Result<NamespaceReplicaInspection, AppError> {
    let expected = configured_namespace_replicas() as u64;
    let quorum = quorum_required(expected as usize) as u64;
    match DistributedNamespace::object_reference_counts(fragment).await {
        Ok(_) => Ok(NamespaceReplicaInspection {
            fragment: fragment.to_string(),
            expected_replicas: expected,
            present_replicas: quorum,
            valid_replicas: quorum,
            healthy: true,
            quorum_verified: true,
            replica_counts_complete: false,
            canonical_path: Some(".n2/namespace/partitions".into()),
            ..NamespaceReplicaInspection::default()
        }),
        Err(AppError::ServiceUnavailable(_)) | Err(AppError::Timeout(_)) => {
            Ok(NamespaceReplicaInspection {
                fragment: fragment.to_string(),
                expected_replicas: expected,
                missing_replicas: 0,
                healthy: false,
                quorum_verified: false,
                replica_counts_complete: false,
                canonical_path: Some(".n2/namespace/partitions".into()),
                ..NamespaceReplicaInspection::default()
            })
        }
        Err(err) => Err(err),
    }
}

pub async fn repair_namespace_replicas(
    fragment: &str,
    storage_roots: &[PathBuf],
    _dry_run: bool,
) -> Result<NamespaceReplicaRepairReport, AppError> {
    let inspection = inspect_namespace_replicas(fragment, storage_roots).await?;
    if !inspection.healthy {
        return Err(AppError::ServiceUnavailable(
            "namespace quorum is unavailable; repair requires a quorum-confirmed source".into(),
        ));
    }
    Ok(NamespaceReplicaRepairReport {
        fragment: fragment.to_string(),
        expected_replicas: inspection.expected_replicas,
        selected_source: inspection.canonical_path,
        ..NamespaceReplicaRepairReport::default()
    })
}
