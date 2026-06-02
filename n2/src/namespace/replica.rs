use crate::core::index_store;
use crate::enums::app_error_enum::AppError;
use serde::Serialize;
use std::path::{Path, PathBuf};

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

pub fn namespace_db_path_for_root(root: &Path, fragment: &str) -> PathBuf {
    index_store::fragment_root_on(root, fragment)
}

pub fn namespace_db_paths_for_roots(fragment: &str, roots: &[PathBuf]) -> Vec<PathBuf> {
    roots
        .iter()
        .map(|root| namespace_db_path_for_root(root, fragment))
        .collect()
}

pub async fn inspect_namespace_replicas(
    fragment: &str,
    index_roots: &[PathBuf],
) -> Result<NamespaceReplicaInspection, AppError> {
    let inspection = index_store::inspect_fragment_partition_replicas(fragment, index_roots);
    Ok(NamespaceReplicaInspection {
        fragment: fragment.to_string(),
        expected_replicas: inspection.expected_replicas,
        present_replicas: inspection.present_replicas,
        valid_replicas: inspection.valid_replicas,
        missing_replicas: inspection.missing_replicas,
        divergent_replicas: inspection.divergent_replicas,
        corrupt_replicas: inspection.corrupt_replicas,
        healthy: inspection.healthy,
        canonical_path: inspection.canonical_path,
    })
}

pub async fn repair_namespace_replicas(
    fragment: &str,
    _index_roots: &[PathBuf],
    dry_run: bool,
) -> Result<NamespaceReplicaRepairReport, AppError> {
    let (missing, divergent, corrupt) =
        index_store::repair_fragment_all_partition_replicas(fragment, dry_run)?;
    Ok(NamespaceReplicaRepairReport {
        fragment: fragment.to_string(),
        expected_replicas: 0,
        missing_repaired: missing,
        divergent_repaired: divergent,
        corrupt_found: corrupt,
        selected_source: None,
    })
}
