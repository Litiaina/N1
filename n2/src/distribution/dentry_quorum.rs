use crate::distribution::dentry_group::{
    DentryPartitionAssignment, assignment_for_partition, assignment_for_partition_at_epoch,
    assignment_nodes, quorum_required,
};
use crate::distribution::dentry_transaction::{
    NamespaceCommitCertificate, replay_committed_partition,
};
use crate::distribution::membership::{ClusterNode, ClusterRuntime, active_cluster_runtime};
use crate::distribution::peer::{authorize_peer, cluster_secret, peer_http_client};
use crate::enums::app_error_enum::AppError;
use crate::namespace::dentry::{
    DentryKey, DirectoryLayout, NamespaceDentry, directory_bucket, virtual_partition,
    virtual_partition_for_bucket,
};
use crate::namespace::facade::global_namespace_facade;
use crate::namespace::transaction::{
    NamespaceCommitDecision, NamespaceTransactionLeg, NamespaceTransactionStore,
    PreparedNamespacePartition, decision_partition,
};
use crate::routes::storage_routes::AppState;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::post;
use axum::{Json, Router};
use futures_util::{StreamExt, future::join_all, stream::FuturesUnordered};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::time::{Duration, sleep, timeout};

const DEFAULT_DENTRY_READ_TIMEOUT: Duration = Duration::from_millis(5_000);
const DENTRY_READ_RETRIES: usize = 4;
const DENTRY_READ_RETRY_BASE: Duration = Duration::from_millis(25);

fn dentry_read_timeout() -> Duration {
    std::env::var("N2_NAMESPACE_READ_REMOTE_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(|millis| Duration::from_millis(millis.clamp(500, 30_000)))
        .unwrap_or(DEFAULT_DENTRY_READ_TIMEOUT)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerDentryReadRequest {
    pub key: DentryKey,
    pub parent_layout: DirectoryLayout,
    pub assignment: DentryPartitionAssignment,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerDentryReadResponse {
    pub node_id: String,
    pub virtual_partition: u64,
    pub record: Option<NamespaceDentry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent: Option<PreparedNamespacePartition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerDentryBucketRequest {
    pub fragment: String,
    pub parent_directory_id: String,
    pub layout: DirectoryLayout,
    pub bucket: u32,
    pub assignment: DentryPartitionAssignment,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerDentryBucketResponse {
    pub node_id: String,
    pub virtual_partition: u64,
    pub entries: Vec<NamespaceDentry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub intents: Vec<PreparedNamespacePartition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerDecisionReadRequest {
    pub transaction_id: String,
    pub decision_partition: u64,
    pub assignment: DentryPartitionAssignment,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerDecisionReadResponse {
    pub node_id: String,
    pub decision_partition: u64,
    pub decision: Option<NamespaceCommitDecision>,
}

pub async fn quorum_get_dentry(
    key: DentryKey,
    parent_layout: DirectoryLayout,
) -> Result<Option<NamespaceDentry>, AppError> {
    let runtime = active_cluster_runtime()?;
    quorum_get_dentry_with_runtime(runtime, key, parent_layout).await
}

pub async fn quorum_get_dentry_with_runtime(
    runtime: Arc<ClusterRuntime>,
    key: DentryKey,
    parent_layout: DirectoryLayout,
) -> Result<Option<NamespaceDentry>, AppError> {
    let mut attempt = 0usize;
    loop {
        match quorum_get_dentry_once_with_runtime(runtime.clone(), key.clone(), parent_layout).await
        {
            Err(err) if attempt < DENTRY_READ_RETRIES && is_transient_quorum_read_error(&err) => {
                attempt = attempt.saturating_add(1);
                sleep(DENTRY_READ_RETRY_BASE * attempt as u32).await;
            }
            result => return result,
        }
    }
}

async fn quorum_get_dentry_once_with_runtime(
    runtime: Arc<ClusterRuntime>,
    key: DentryKey,
    parent_layout: DirectoryLayout,
) -> Result<Option<NamespaceDentry>, AppError> {
    let partition = virtual_partition(
        &key.fragment,
        &key.parent_directory_id,
        &key.name,
        parent_layout,
    );
    let assignment = assignment_for_partition_at_epoch(&runtime, partition, parent_layout.epoch)?;
    let voters = assignment_nodes(&runtime, &assignment)?;
    let required = quorum_required(voters.len());
    let mut responses = FuturesUnordered::new();
    for node in voters {
        let runtime = runtime.clone();
        let assignment = assignment.clone();
        let key = key.clone();
        responses.push(tokio::spawn(async move {
            read_dentry_from_node(
                runtime,
                node,
                PeerDentryReadRequest {
                    key,
                    parent_layout,
                    assignment,
                },
            )
            .await
        }));
    }
    let mut candidates = BTreeMap::new();
    let mut successful = 0usize;
    let mut last_error = None;
    while let Some(result) = responses.next().await {
        let mut response = match result {
            Ok(Ok(response)) => response,
            Ok(Err(err)) => {
                last_error = Some(err);
                continue;
            }
            Err(err) => {
                last_error = Some(AppError::ServiceUnavailable(format!(
                    "namespace dentry read task failed: {err}"
                )));
                continue;
            }
        };
        successful = successful.saturating_add(1);
        if let Some(intent) = response.intent.as_ref() {
            if let Some(certificate) =
                quorum_read_decision_with_runtime(runtime.clone(), &intent.transaction_id).await?
            {
                response.record = projected_record(&key, response.record.take(), intent)?;
                replay_committed_partition(intent.clone(), certificate).await?;
            }
        }
        if let Some(value) = record_quorum_candidate(&mut candidates, response.record, required)? {
            return Ok(value);
        }
    }
    if let Some(err) = last_error {
        tracing::warn!(
            successful,
            required,
            error = %err,
            "namespace dentry read voter failed"
        );
    }
    Err(AppError::ServiceUnavailable(format!(
        "namespace dentry has no identical {required}-voter quorum"
    )))
}

pub async fn quorum_list_parent(
    fragment: &str,
    parent_directory_id: &str,
    layout: DirectoryLayout,
) -> Result<Vec<NamespaceDentry>, AppError> {
    let runtime = active_cluster_runtime()?;
    let buckets = (0..layout.bucket_count()).map(|bucket| {
        quorum_list_bucket_with_runtime(
            runtime.clone(),
            fragment.to_string(),
            parent_directory_id.to_string(),
            layout,
            bucket,
        )
    });
    let mut entries = BTreeMap::new();
    for result in join_all(buckets).await {
        for entry in result? {
            if entries.insert(entry.key.name.clone(), entry).is_some() {
                return Err(AppError::InternalServerError(
                    "namespace directory contains a duplicate entry name across buckets".into(),
                ));
            }
        }
    }
    Ok(entries.into_values().collect())
}

async fn quorum_list_bucket_with_runtime(
    runtime: Arc<ClusterRuntime>,
    fragment: String,
    parent_directory_id: String,
    layout: DirectoryLayout,
    bucket: u32,
) -> Result<Vec<NamespaceDentry>, AppError> {
    let mut attempt = 0usize;
    loop {
        match quorum_list_bucket_once_with_runtime(
            runtime.clone(),
            fragment.clone(),
            parent_directory_id.clone(),
            layout,
            bucket,
        )
        .await
        {
            Err(err) if attempt < DENTRY_READ_RETRIES && is_transient_quorum_read_error(&err) => {
                attempt = attempt.saturating_add(1);
                sleep(DENTRY_READ_RETRY_BASE * attempt as u32).await;
            }
            result => return result,
        }
    }
}

async fn quorum_list_bucket_once_with_runtime(
    runtime: Arc<ClusterRuntime>,
    fragment: String,
    parent_directory_id: String,
    layout: DirectoryLayout,
    bucket: u32,
) -> Result<Vec<NamespaceDentry>, AppError> {
    let partition = virtual_partition_for_bucket(&fragment, &parent_directory_id, bucket);
    let assignment = assignment_for_partition_at_epoch(&runtime, partition, layout.epoch)?;
    let voters = assignment_nodes(&runtime, &assignment)?;
    let required = quorum_required(voters.len());
    let mut responses = FuturesUnordered::new();
    for node in voters {
        let runtime = runtime.clone();
        let assignment = assignment.clone();
        let fragment = fragment.clone();
        let parent_directory_id = parent_directory_id.clone();
        responses.push(tokio::spawn(async move {
            read_bucket_from_node(
                runtime,
                node,
                PeerDentryBucketRequest {
                    fragment,
                    parent_directory_id,
                    layout,
                    bucket,
                    assignment,
                },
            )
            .await
        }));
    }
    let mut candidates = BTreeMap::new();
    let mut successful = 0usize;
    let mut last_error = None;
    while let Some(result) = responses.next().await {
        let mut response = match result {
            Ok(Ok(response)) => response,
            Ok(Err(err)) => {
                last_error = Some(err);
                continue;
            }
            Err(err) => {
                last_error = Some(AppError::ServiceUnavailable(format!(
                    "namespace bucket read task failed: {err}"
                )));
                continue;
            }
        };
        successful = successful.saturating_add(1);
        for intent in response.intents.clone() {
            if let Some(certificate) =
                quorum_read_decision_with_runtime(runtime.clone(), &intent.transaction_id).await?
            {
                apply_projected_bucket(&mut response.entries, &intent)?;
                replay_committed_partition(intent, certificate).await?;
            }
        }
        response
            .entries
            .sort_by(|left, right| left.key.name.cmp(&right.key.name));
        if let Some(value) = record_quorum_candidate(&mut candidates, response.entries, required)? {
            return Ok(value);
        }
    }
    if let Some(err) = last_error {
        tracing::warn!(
            successful,
            required,
            error = %err,
            "namespace bucket read voter failed"
        );
    }
    Err(AppError::ServiceUnavailable(format!(
        "namespace directory bucket has no identical {required}-voter quorum"
    )))
}

async fn read_dentry_from_node(
    runtime: Arc<ClusterRuntime>,
    node: ClusterNode,
    request: PeerDentryReadRequest,
) -> Result<PeerDentryReadResponse, AppError> {
    if node.id == runtime.local_node_id {
        return local_dentry_read(&runtime, request);
    }
    let client = peer_http_client()?;
    let mut send = client
        .post(format!(
            "{}/n2/peer/namespace-v2/dentry",
            node.endpoint.trim_end_matches('/')
        ))
        .json(&request);
    if let Some(secret) = cluster_secret() {
        send = send.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(dentry_read_timeout(), send.send())
        .await
        .map_err(|_| AppError::ServiceUnavailable(format!("dentry read timed out at {}", node.id)))?
        .map_err(|err| {
            AppError::ServiceUnavailable(format!("dentry read failed at {}: {err}", node.id))
        })?;
    if !response.status().is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "dentry read returned {} from {}",
            response.status(),
            node.id
        )));
    }
    response.json().await.map_err(|err| {
        AppError::InternalServerError(format!("decode dentry read from {}: {err}", node.id))
    })
}

async fn read_bucket_from_node(
    runtime: Arc<ClusterRuntime>,
    node: ClusterNode,
    request: PeerDentryBucketRequest,
) -> Result<PeerDentryBucketResponse, AppError> {
    if node.id == runtime.local_node_id {
        return local_bucket_read(&runtime, request);
    }
    let client = peer_http_client()?;
    let mut send = client
        .post(format!(
            "{}/n2/peer/namespace-v2/bucket",
            node.endpoint.trim_end_matches('/')
        ))
        .json(&request);
    if let Some(secret) = cluster_secret() {
        send = send.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(dentry_read_timeout(), send.send())
        .await
        .map_err(|_| AppError::ServiceUnavailable(format!("bucket read timed out at {}", node.id)))?
        .map_err(|err| {
            AppError::ServiceUnavailable(format!("bucket read failed at {}: {err}", node.id))
        })?;
    if !response.status().is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "bucket read returned {} from {}",
            response.status(),
            node.id
        )));
    }
    response.json().await.map_err(|err| {
        AppError::InternalServerError(format!("decode bucket read from {}: {err}", node.id))
    })
}

fn local_dentry_read(
    runtime: &ClusterRuntime,
    request: PeerDentryReadRequest,
) -> Result<PeerDentryReadResponse, AppError> {
    let partition = validate_dentry_assignment(runtime, &request)?;
    let transactions = NamespaceTransactionStore::new(global_namespace_facade()?.store().clone());
    Ok(PeerDentryReadResponse {
        node_id: runtime.local_node_id.clone(),
        virtual_partition: partition,
        record: global_namespace_facade()?
            .store()
            .get(&request.key, request.parent_layout)?,
        intent: transactions.intent_for_key(&request.key, request.parent_layout)?,
    })
}

fn local_bucket_read(
    runtime: &ClusterRuntime,
    request: PeerDentryBucketRequest,
) -> Result<PeerDentryBucketResponse, AppError> {
    if request.bucket >= request.layout.bucket_count() {
        return Err(AppError::BadRequest(
            "namespace bucket is outside layout".into(),
        ));
    }
    let partition = virtual_partition_for_bucket(
        &request.fragment,
        &request.parent_directory_id,
        request.bucket,
    );
    validate_assignment(
        runtime,
        partition,
        request.layout.epoch,
        &request.assignment,
    )?;
    let mut entries = global_namespace_facade()?.store().list_bucket(
        &request.fragment,
        &request.parent_directory_id,
        request.layout,
        request.bucket,
    )?;
    entries.sort_by(|left, right| left.key.name.cmp(&right.key.name));
    let intents = NamespaceTransactionStore::new(global_namespace_facade()?.store().clone())
        .intents_for_partition(partition)?;
    Ok(PeerDentryBucketResponse {
        node_id: runtime.local_node_id.clone(),
        virtual_partition: partition,
        entries,
        intents,
    })
}

async fn quorum_read_decision_with_runtime(
    runtime: Arc<ClusterRuntime>,
    transaction_id: &str,
) -> Result<Option<NamespaceCommitCertificate>, AppError> {
    let partition = decision_partition(transaction_id);
    let assignment = assignment_for_partition(&runtime, partition)?;
    let voters = assignment_nodes(&runtime, &assignment)?;
    let required = quorum_required(voters.len());
    let request = PeerDecisionReadRequest {
        transaction_id: transaction_id.to_string(),
        decision_partition: partition,
        assignment: assignment.clone(),
    };
    let mut candidates: BTreeMap<Vec<u8>, (Option<NamespaceCommitDecision>, Vec<String>)> =
        BTreeMap::new();
    let mut responses = FuturesUnordered::new();
    for node in voters {
        let runtime = runtime.clone();
        let request = request.clone();
        responses.push(tokio::spawn(async move {
            read_decision_from_node(runtime, node, request).await
        }));
    }
    let mut last_error = None;
    while let Some(result) = responses.next().await {
        let response = match result {
            Ok(Ok(response)) => response,
            Ok(Err(err)) => {
                last_error = Some(err);
                continue;
            }
            Err(err) => {
                last_error = Some(AppError::ServiceUnavailable(format!(
                    "namespace decision read task failed: {err}"
                )));
                continue;
            }
        };
        let encoded = rmp_serde::to_vec_named(&response.decision)?;
        let candidate = candidates
            .entry(encoded)
            .or_insert_with(|| (response.decision.clone(), Vec::new()));
        candidate.1.push(response.node_id);
        if candidate.1.len() >= required {
            return Ok(candidate
                .0
                .clone()
                .map(|decision| NamespaceCommitCertificate {
                    decision_partition: partition,
                    assignment,
                    decision,
                    acknowledged_voters: candidate.1.clone(),
                }));
        }
    }
    if let Some(err) = last_error {
        tracing::warn!(
            transaction_id,
            required,
            error = %err,
            "namespace decision read voter failed"
        );
    }
    Err(AppError::ServiceUnavailable(format!(
        "namespace transaction decision has no identical {required}-voter quorum"
    )))
}

async fn read_decision_from_node(
    runtime: Arc<ClusterRuntime>,
    node: ClusterNode,
    request: PeerDecisionReadRequest,
) -> Result<PeerDecisionReadResponse, AppError> {
    if node.id == runtime.local_node_id {
        return local_decision_read(&runtime, request);
    }
    let client = peer_http_client()?;
    let mut send = client
        .post(format!(
            "{}/n2/peer/namespace-v2/decision/read",
            node.endpoint.trim_end_matches('/')
        ))
        .json(&request);
    if let Some(secret) = cluster_secret() {
        send = send.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(dentry_read_timeout(), send.send())
        .await
        .map_err(|_| AppError::ServiceUnavailable("decision read timed out".into()))?
        .map_err(|err| AppError::ServiceUnavailable(format!("decision read failed: {err}")))?;
    if !response.status().is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "decision read returned {} from {}",
            response.status(),
            node.id
        )));
    }
    response.json().await.map_err(|err| {
        AppError::InternalServerError(format!("decode decision read from {}: {err}", node.id))
    })
}

fn local_decision_read(
    runtime: &ClusterRuntime,
    request: PeerDecisionReadRequest,
) -> Result<PeerDecisionReadResponse, AppError> {
    if decision_partition(&request.transaction_id) != request.decision_partition {
        return Err(AppError::BadRequest(
            "namespace decision read is routed to the wrong partition".into(),
        ));
    }
    validate_assignment(
        runtime,
        request.decision_partition,
        request.assignment.membership_epoch,
        &request.assignment,
    )?;
    let decision = NamespaceTransactionStore::new(global_namespace_facade()?.store().clone())
        .read_decision(request.decision_partition, &request.transaction_id)?;
    Ok(PeerDecisionReadResponse {
        node_id: runtime.local_node_id.clone(),
        decision_partition: request.decision_partition,
        decision,
    })
}

fn projected_record(
    key: &DentryKey,
    base: Option<NamespaceDentry>,
    intent: &PreparedNamespacePartition,
) -> Result<Option<NamespaceDentry>, AppError> {
    let mut projected = base;
    for leg in &intent.legs {
        if leg.key() != key {
            continue;
        }
        match leg {
            NamespaceTransactionLeg::Insert { record, .. } => projected = Some(record.clone()),
            NamespaceTransactionLeg::Remove { .. } => projected = None,
        }
    }
    Ok(projected)
}

fn apply_projected_bucket(
    entries: &mut Vec<NamespaceDentry>,
    intent: &PreparedNamespacePartition,
) -> Result<(), AppError> {
    let mut by_name = entries
        .drain(..)
        .map(|entry| (entry.key.name.clone(), entry))
        .collect::<BTreeMap<_, _>>();
    for leg in &intent.legs {
        match leg {
            NamespaceTransactionLeg::Insert { record, .. } => {
                by_name.insert(record.key.name.clone(), record.clone());
            }
            NamespaceTransactionLeg::Remove { key, .. } => {
                by_name.remove(&key.name);
            }
        }
    }
    *entries = by_name.into_values().collect();
    Ok(())
}

fn validate_dentry_assignment(
    runtime: &ClusterRuntime,
    request: &PeerDentryReadRequest,
) -> Result<u64, AppError> {
    if directory_bucket(&request.key.name, request.parent_layout)
        >= request.parent_layout.bucket_count()
    {
        return Err(AppError::BadRequest(
            "invalid namespace dentry bucket".into(),
        ));
    }
    let partition = virtual_partition(
        &request.key.fragment,
        &request.key.parent_directory_id,
        &request.key.name,
        request.parent_layout,
    );
    validate_assignment(
        runtime,
        partition,
        request.parent_layout.epoch,
        &request.assignment,
    )?;
    Ok(partition)
}

fn validate_assignment(
    runtime: &ClusterRuntime,
    partition: u64,
    placement_epoch: u64,
    supplied: &DentryPartitionAssignment,
) -> Result<(), AppError> {
    let expected = assignment_for_partition_at_epoch(runtime, partition, placement_epoch)?;
    if &expected != supplied {
        return Err(AppError::Conflict(
            "namespace partition assignment does not match current membership epoch".into(),
        ));
    }
    if !supplied
        .voter_node_ids
        .iter()
        .any(|node_id| node_id == &runtime.local_node_id)
    {
        return Err(AppError::Forbidden(
            "this node is not a voter for the namespace partition".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
fn select_quorum_value<T>(
    values: impl IntoIterator<Item = T>,
    required: usize,
    label: &str,
) -> Result<T, AppError>
where
    T: Clone + Serialize,
{
    let mut candidates: BTreeMap<Vec<u8>, (T, usize)> = BTreeMap::new();
    for value in values {
        let encoded = rmp_serde::to_vec_named(&value)?;
        let candidate = candidates.entry(encoded).or_insert((value, 0));
        candidate.1 = candidate.1.saturating_add(1);
    }
    candidates
        .into_values()
        .filter(|(_, count)| *count >= required)
        .max_by_key(|(_, count)| *count)
        .map(|(value, _)| value)
        .ok_or_else(|| {
            AppError::ServiceUnavailable(format!(
                "{label} has no identical {required}-voter quorum"
            ))
        })
}

fn record_quorum_candidate<T>(
    candidates: &mut BTreeMap<Vec<u8>, (T, usize)>,
    value: T,
    required: usize,
) -> Result<Option<T>, AppError>
where
    T: Clone + Serialize,
{
    let encoded = rmp_serde::to_vec_named(&value)?;
    let candidate = candidates.entry(encoded).or_insert((value, 0));
    candidate.1 = candidate.1.saturating_add(1);
    if candidate.1 >= required {
        Ok(Some(candidate.0.clone()))
    } else {
        Ok(None)
    }
}

fn is_transient_quorum_read_error(err: &AppError) -> bool {
    match err {
        AppError::ServiceUnavailable(message) => {
            message.contains("no identical")
                || message.contains("timed out")
                || message.contains("failed at")
                || message.contains("task failed")
        }
        _ => false,
    }
}

async fn peer_read_dentry(
    headers: HeaderMap,
    State(_state): State<Arc<AppState>>,
    Json(request): Json<PeerDentryReadRequest>,
) -> Result<Json<PeerDentryReadResponse>, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    Ok(Json(local_dentry_read(&runtime, request)?))
}

async fn peer_read_bucket(
    headers: HeaderMap,
    State(_state): State<Arc<AppState>>,
    Json(request): Json<PeerDentryBucketRequest>,
) -> Result<Json<PeerDentryBucketResponse>, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    Ok(Json(local_bucket_read(&runtime, request)?))
}

async fn peer_read_decision(
    headers: HeaderMap,
    State(_state): State<Arc<AppState>>,
    Json(request): Json<PeerDecisionReadRequest>,
) -> Result<Json<PeerDecisionReadResponse>, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    Ok(Json(local_decision_read(&runtime, request)?))
}

pub fn build_dentry_quorum_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/n2/peer/namespace-v2/dentry", post(peer_read_dentry))
        .route("/n2/peer/namespace-v2/bucket", post(peer_read_bucket))
        .route(
            "/n2/peer/namespace-v2/decision/read",
            post(peer_read_decision),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quorum_selection_never_unions_divergent_values() {
        let selected =
            select_quorum_value(vec![Some(1u8), Some(1), Some(2)], 2, "test").expect("majority");
        assert_eq!(selected, Some(1));
        assert!(select_quorum_value(vec![Some(1u8), Some(2)], 2, "test").is_err());
    }
}
