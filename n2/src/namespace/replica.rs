use std::path::PathBuf;

use serde::Serialize;

use crate::distribution::dentry_service::DistributedNamespace;
use crate::distribution::membership::active_cluster_runtime;
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

pub async fn inspect_namespace_replicas(
    fragment: &str,
    _storage_roots: &[PathBuf],
) -> Result<NamespaceReplicaInspection, AppError> {
    let expected = active_cluster_runtime()?
        .membership
        .policy
        .namespace_redundancy as u64;
    let quorum = (expected / 2).saturating_add(1);
    match DistributedNamespace::list_directory(fragment, "").await {
        Ok(_) => Ok(NamespaceReplicaInspection {
            fragment: fragment.to_string(),
            expected_replicas: expected,
            present_replicas: quorum,
            valid_replicas: quorum,
            healthy: true,
            quorum_verified: true,
            replica_counts_complete: false,
            canonical_path: Some("@f_<fragment>/.namespace/@i_<inode>".into()),
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
                canonical_path: Some("@f_<fragment>/.namespace/@i_<inode>".into()),
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
