use crate::distribution::membership::active_cluster_runtime;
use crate::distribution::namespace::{
    NamespaceLedgerSummary, namespace_local_ledger_summary, repair_namespace_from_peers,
};
use crate::distribution::namespace_group::{
    assignment_nodes, namespace_partition_count, resolve_namespace_group,
};
use crate::distribution::peer::{cluster_secret, peer_http_client};
use crate::enums::app_error_enum::AppError;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::Serialize;
use std::path::{Path, PathBuf};
use tokio::time::{Duration, timeout};

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

pub fn namespace_db_path_for_root(root: &Path, fragment: &str, partition_id: u16) -> PathBuf {
    root.join(".n2")
        .join("namespace-ledger")
        .join(fragment)
        .join(format!("{partition_id}.requests.jsonl"))
}

pub fn namespace_db_paths_for_roots(fragment: &str, roots: &[PathBuf]) -> Vec<PathBuf> {
    roots
        .iter()
        .flat_map(|root| {
            (0..namespace_partition_count())
                .map(move |partition_id| namespace_db_path_for_root(root, fragment, partition_id))
        })
        .collect()
}

async fn fetch_summary(
    endpoint: &str,
    fragment: &str,
    partition_id: u16,
) -> Result<NamespaceLedgerSummary, AppError> {
    let url = format!(
        "{}/n2/peer/namespace/summary/{}/{}",
        endpoint.trim_end_matches('/'),
        utf8_percent_encode(fragment, NON_ALPHANUMERIC),
        partition_id
    );
    let client = peer_http_client()?;
    let mut request = client.get(url);
    if let Some(secret) = cluster_secret() {
        request = request.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(Duration::from_secs(3), request.send())
        .await
        .map_err(|_| AppError::ServiceUnavailable("namespace summary timed out".into()))?
        .map_err(|err| AppError::ServiceUnavailable(format!("namespace summary failed: {err}")))?;
    if !response.status().is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "namespace summary returned {}",
            response.status()
        )));
    }
    response
        .json()
        .await
        .map_err(|err| AppError::InternalServerError(format!("decode namespace summary: {err}")))
}

pub async fn inspect_namespace_replicas(
    fragment: &str,
    _index_roots: &[PathBuf],
) -> Result<NamespaceReplicaInspection, AppError> {
    let runtime = active_cluster_runtime()?;
    let mut expected_replicas = 0u64;
    let mut present_replicas = 0u64;
    let mut valid_replicas = 0u64;
    let mut missing_replicas = 0u64;
    let mut divergent_replicas = 0u64;
    let mut canonical_path = None;
    for partition_id in 0..namespace_partition_count() {
        let assignment = resolve_namespace_group(&runtime, fragment, partition_id).await?;
        if canonical_path.is_none() {
            canonical_path = assignment.voter_node_ids.first().cloned();
        }
        let nodes = assignment_nodes(&runtime, &assignment)?;
        expected_replicas = expected_replicas.saturating_add(nodes.len() as u64);
        let mut positions = Vec::new();
        for node in nodes {
            let result = if node.id == runtime.local_node_id {
                namespace_local_ledger_summary(fragment, partition_id)
            } else {
                fetch_summary(&node.endpoint, fragment, partition_id).await
            };
            match result {
                Ok(summary) => positions.push(summary.committed_log_index),
                Err(_) => missing_replicas = missing_replicas.saturating_add(1),
            }
        }
        let canonical = positions.iter().copied().max().unwrap_or(0);
        let partition_valid = positions
            .iter()
            .filter(|position| **position == canonical)
            .count() as u64;
        valid_replicas = valid_replicas.saturating_add(partition_valid);
        present_replicas = present_replicas.saturating_add(positions.len() as u64);
        divergent_replicas =
            divergent_replicas.saturating_add(positions.len() as u64 - partition_valid);
    }
    Ok(NamespaceReplicaInspection {
        fragment: fragment.to_string(),
        expected_replicas,
        present_replicas,
        valid_replicas,
        missing_replicas,
        divergent_replicas,
        corrupt_replicas: 0,
        healthy: missing_replicas == 0 && divergent_replicas == 0,
        canonical_path,
    })
}

pub async fn repair_namespace_replicas(
    fragment: &str,
    index_roots: &[PathBuf],
    dry_run: bool,
) -> Result<NamespaceReplicaRepairReport, AppError> {
    let before = inspect_namespace_replicas(fragment, index_roots).await?;
    let repaired = if dry_run {
        0
    } else {
        repair_namespace_from_peers(fragment).await?
    };
    Ok(NamespaceReplicaRepairReport {
        fragment: fragment.to_string(),
        expected_replicas: before.expected_replicas,
        missing_repaired: 0,
        divergent_repaired: repaired,
        corrupt_found: before.corrupt_replicas,
        selected_source: before.canonical_path,
    })
}
