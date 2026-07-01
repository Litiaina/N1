use crate::distribution::dentry_group::{
    DentryPartitionAssignment, assignment_for_partition, assignment_for_partition_at_epoch,
    assignment_nodes, quorum_required,
};
use crate::distribution::membership::{ClusterNode, ClusterRuntime, active_cluster_runtime};
use crate::distribution::peer::{authorize_peer, cluster_secret, peer_http_client};
use crate::enums::app_error_enum::AppError;
use crate::namespace::facade::global_namespace_facade;
use crate::namespace::transaction::{
    NamespaceCommitDecision, NamespaceTransactionStore, PreparedNamespacePartition,
    decision_partition, mutation_digest, prepared_partition_digest, prepared_placement_epoch,
};
use crate::routes::storage_routes::AppState;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::post;
use axum::{Json, Router};
use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::time::{Duration, timeout};

const MUTATION_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreparePartitionRequest {
    pub prepared: PreparedNamespacePartition,
    pub assignment: DentryPartitionAssignment,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMutationRequest {
    pub transaction_id: String,
    pub virtual_partition: u64,
    pub assignment: DentryPartitionAssignment,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub certificate: Option<NamespaceCommitCertificate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prepared: Option<PreparedNamespacePartition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistDecisionRequest {
    pub decision_partition: u64,
    pub assignment: DentryPartitionAssignment,
    pub decision: NamespaceCommitDecision,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceCommitCertificate {
    pub decision_partition: u64,
    pub assignment: DentryPartitionAssignment,
    pub decision: NamespaceCommitDecision,
    pub acknowledged_voters: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationAck {
    pub node_id: String,
    pub transaction_id: String,
    pub virtual_partition: u64,
}

pub async fn commit_namespace_transaction(
    mut partitions: Vec<PreparedNamespacePartition>,
) -> Result<NamespaceCommitCertificate, AppError> {
    if partitions.is_empty() {
        return Err(AppError::BadRequest(
            "namespace transaction requires at least one partition".into(),
        ));
    }
    partitions.sort_by_key(|prepared| prepared.virtual_partition);
    let transaction_id = partitions[0].transaction_id.clone();
    if partitions
        .iter()
        .any(|prepared| prepared.transaction_id != transaction_id)
    {
        return Err(AppError::BadRequest(
            "namespace transaction partitions have different identities".into(),
        ));
    }
    let runtime = active_cluster_runtime()?;

    // Cross-partition transactions acquire partitions in one global order.
    // Preparing these legs in parallel allows opposite renames to each hold
    // one partition and livelock. Disjoint transactions still execute fully
    // concurrently because they share no partition locks.
    for prepared in partitions.iter().cloned() {
        if let Err(error) = prepare_partition_quorum(runtime.clone(), prepared).await {
            abort_all(runtime.clone(), &partitions).await;
            return Err(error);
        }
    }

    let participant_partitions = partitions
        .iter()
        .map(|prepared| prepared.virtual_partition)
        .collect::<Vec<_>>();
    let partition_digests = partitions
        .iter()
        .map(|prepared| {
            Ok((
                prepared.virtual_partition,
                prepared_partition_digest(prepared)?,
            ))
        })
        .collect::<Result<_, AppError>>()?;
    let decision = NamespaceCommitDecision {
        transaction_id: transaction_id.clone(),
        participant_partitions,
        mutation_digest: mutation_digest(&partitions)?,
        partition_digests,
        committed_at_unix: chrono::Utc::now().timestamp(),
    };
    let certificate = match persist_decision_quorum(runtime.clone(), decision).await {
        Ok(certificate) => certificate,
        Err(err) => {
            abort_all(runtime.clone(), &partitions).await;
            return Err(err);
        }
    };

    let commit_results =
        join_all(partitions.iter().cloned().map(|prepared| {
            commit_partition_quorum(runtime.clone(), prepared, certificate.clone())
        }))
        .await;
    if let Some(error) = commit_results.into_iter().find_map(Result::err) {
        return Err(AppError::ServiceUnavailable(format!(
            "namespace transaction {} is durably committed but requires replay: {error}",
            transaction_id
        )));
    }
    Ok(certificate)
}

async fn prepare_partition_quorum(
    runtime: Arc<ClusterRuntime>,
    prepared: PreparedNamespacePartition,
) -> Result<(), AppError> {
    let assignment = assignment_for_partition_at_epoch(
        &runtime,
        prepared.virtual_partition,
        prepared_placement_epoch(&prepared)?,
    )?;
    let voters = assignment_nodes(&runtime, &assignment)?;
    let required = quorum_required(voters.len());
    let request = PreparePartitionRequest {
        prepared,
        assignment,
    };
    // Every coordinator visits voters in the assignment's deterministic order.
    // The first reachable voter therefore arbitrates only conflicting keys;
    // unrelated keys and partitions remain fully concurrent. Parallel prepare
    // fan-out permits two coordinators to split locks across voters and makes
    // both otherwise-valid transactions lose progress.
    let mut acknowledgements = 0usize;
    let mut last_error = None;
    for node in voters {
        match send_prepare(runtime.clone(), node, request.clone()).await {
            Ok(_) => acknowledgements += 1,
            Err(err @ AppError::Conflict(_)) => return Err(err),
            Err(err) => last_error = Some(err),
        }
    }
    if acknowledgements < required {
        if let Some(err) = last_error {
            tracing::warn!(
                transaction_id = %request.prepared.transaction_id,
                virtual_partition = request.prepared.virtual_partition,
                error = %err,
                "namespace prepare voter failed"
            );
        }
        return Err(AppError::ServiceUnavailable(format!(
            "namespace partition prepare received {acknowledgements}/{required} required votes"
        )));
    }
    Ok(())
}

async fn persist_decision_quorum(
    runtime: Arc<ClusterRuntime>,
    decision: NamespaceCommitDecision,
) -> Result<NamespaceCommitCertificate, AppError> {
    let partition = decision_partition(&decision.transaction_id);
    let assignment = assignment_for_partition(&runtime, partition)?;
    let voters = assignment_nodes(&runtime, &assignment)?;
    let required = quorum_required(voters.len());
    let request = PersistDecisionRequest {
        decision_partition: partition,
        assignment: assignment.clone(),
        decision: decision.clone(),
    };
    let acknowledged_voters = join_all(
        voters
            .into_iter()
            .map(|node| send_decision(runtime.clone(), node, request.clone())),
    )
    .await
    .into_iter()
    .filter_map(Result::ok)
    .map(|ack| ack.node_id)
    .collect::<Vec<_>>();
    if acknowledged_voters.len() < required {
        return Err(AppError::ServiceUnavailable(format!(
            "namespace commit decision received {}/{} required votes",
            acknowledged_voters.len(),
            required
        )));
    }
    Ok(NamespaceCommitCertificate {
        decision_partition: partition,
        assignment,
        decision,
        acknowledged_voters,
    })
}

async fn commit_partition_quorum(
    runtime: Arc<ClusterRuntime>,
    prepared: PreparedNamespacePartition,
    certificate: NamespaceCommitCertificate,
) -> Result<(), AppError> {
    let assignment = assignment_for_partition_at_epoch(
        &runtime,
        prepared.virtual_partition,
        prepared_placement_epoch(&prepared)?,
    )?;
    let voters = assignment_nodes(&runtime, &assignment)?;
    let required = quorum_required(voters.len());
    let request = PartitionMutationRequest {
        transaction_id: prepared.transaction_id.clone(),
        virtual_partition: prepared.virtual_partition,
        assignment,
        certificate: Some(certificate),
        prepared: Some(prepared),
    };
    let acknowledgements = join_all(
        voters
            .into_iter()
            .map(|node| send_commit(runtime.clone(), node, request.clone())),
    )
    .await
    .into_iter()
    .filter_map(Result::ok)
    .count();
    if acknowledgements < required {
        return Err(AppError::ServiceUnavailable(format!(
            "namespace partition commit received {acknowledgements}/{required} required votes"
        )));
    }
    Ok(())
}

pub async fn replay_committed_partition(
    prepared: PreparedNamespacePartition,
    certificate: NamespaceCommitCertificate,
) -> Result<(), AppError> {
    let runtime = active_cluster_runtime()?;
    commit_partition_quorum(runtime, prepared, certificate).await
}

async fn abort_all(runtime: Arc<ClusterRuntime>, partitions: &[PreparedNamespacePartition]) {
    for prepared in partitions {
        let Ok(placement_epoch) = prepared_placement_epoch(prepared) else {
            continue;
        };
        let Ok(assignment) = assignment_for_partition_at_epoch(
            &runtime,
            prepared.virtual_partition,
            placement_epoch,
        ) else {
            continue;
        };
        let Ok(voters) = assignment_nodes(&runtime, &assignment) else {
            continue;
        };
        let request = PartitionMutationRequest {
            transaction_id: prepared.transaction_id.clone(),
            virtual_partition: prepared.virtual_partition,
            assignment,
            certificate: None,
            prepared: None,
        };
        let _ = join_all(
            voters
                .into_iter()
                .map(|node| send_abort(runtime.clone(), node, request.clone())),
        )
        .await;
    }
}

async fn send_prepare(
    runtime: Arc<ClusterRuntime>,
    node: ClusterNode,
    request: PreparePartitionRequest,
) -> Result<MutationAck, AppError> {
    if node.id == runtime.local_node_id {
        return local_prepare(&runtime, request);
    }
    post_peer(&node, "/n2/peer/namespace-v2/prepare", &request).await
}

async fn send_decision(
    runtime: Arc<ClusterRuntime>,
    node: ClusterNode,
    request: PersistDecisionRequest,
) -> Result<MutationAck, AppError> {
    if node.id == runtime.local_node_id {
        return local_decision(&runtime, request);
    }
    post_peer(&node, "/n2/peer/namespace-v2/decision", &request).await
}

async fn send_commit(
    runtime: Arc<ClusterRuntime>,
    node: ClusterNode,
    request: PartitionMutationRequest,
) -> Result<MutationAck, AppError> {
    if node.id == runtime.local_node_id {
        return local_commit(&runtime, request);
    }
    post_peer(&node, "/n2/peer/namespace-v2/commit", &request).await
}

async fn send_abort(
    runtime: Arc<ClusterRuntime>,
    node: ClusterNode,
    request: PartitionMutationRequest,
) -> Result<MutationAck, AppError> {
    if node.id == runtime.local_node_id {
        return local_abort(&runtime, request);
    }
    post_peer(&node, "/n2/peer/namespace-v2/abort", &request).await
}

async fn post_peer<T: Serialize + ?Sized>(
    node: &ClusterNode,
    route: &str,
    request: &T,
) -> Result<MutationAck, AppError> {
    let client = peer_http_client()?;
    let mut send = client
        .post(format!("{}{}", node.endpoint.trim_end_matches('/'), route))
        .json(request);
    if let Some(secret) = cluster_secret() {
        send = send.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(MUTATION_TIMEOUT, send.send())
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!("namespace mutation timed out at {}", node.id))
        })?
        .map_err(|err| {
            AppError::ServiceUnavailable(format!("namespace mutation failed at {}: {err}", node.id))
        })?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if status == axum::http::StatusCode::CONFLICT {
            return Err(AppError::Conflict(if body.is_empty() {
                format!("namespace mutation conflicted at {}", node.id)
            } else {
                body
            }));
        }
        if status == axum::http::StatusCode::NOT_FOUND {
            return Err(AppError::NotFound);
        }
        return Err(AppError::ServiceUnavailable(format!(
            "namespace mutation returned {status} from {}: {body}",
            node.id
        )));
    }
    response.json().await.map_err(|err| {
        AppError::InternalServerError(format!("decode mutation ack from {}: {err}", node.id))
    })
}

fn local_prepare(
    runtime: &ClusterRuntime,
    request: PreparePartitionRequest,
) -> Result<MutationAck, AppError> {
    if request.assignment.membership_epoch != prepared_placement_epoch(&request.prepared)? {
        return Err(AppError::Conflict(
            "namespace prepare assignment does not match directory placement epoch".into(),
        ));
    }
    validate_local_assignment(
        runtime,
        request.prepared.virtual_partition,
        &request.assignment,
    )?;
    transaction_store()?.prepare_partition(&request.prepared)?;
    Ok(MutationAck {
        node_id: runtime.local_node_id.clone(),
        transaction_id: request.prepared.transaction_id,
        virtual_partition: request.prepared.virtual_partition,
    })
}

fn local_decision(
    runtime: &ClusterRuntime,
    request: PersistDecisionRequest,
) -> Result<MutationAck, AppError> {
    validate_local_assignment(runtime, request.decision_partition, &request.assignment)?;
    if decision_partition(&request.decision.transaction_id) != request.decision_partition {
        return Err(AppError::BadRequest(
            "namespace decision is routed to the wrong partition".into(),
        ));
    }
    transaction_store()?.persist_decision(request.decision_partition, &request.decision)?;
    Ok(MutationAck {
        node_id: runtime.local_node_id.clone(),
        transaction_id: request.decision.transaction_id,
        virtual_partition: request.decision_partition,
    })
}

fn local_commit(
    runtime: &ClusterRuntime,
    request: PartitionMutationRequest,
) -> Result<MutationAck, AppError> {
    validate_local_assignment(runtime, request.virtual_partition, &request.assignment)?;
    let certificate = request.certificate.as_ref().ok_or_else(|| {
        AppError::BadRequest("namespace commit requires a quorum certificate".into())
    })?;
    validate_certificate(runtime, certificate, &request.transaction_id)?;
    if !certificate
        .decision
        .participant_partitions
        .contains(&request.virtual_partition)
    {
        return Err(AppError::BadRequest(
            "namespace commit partition is absent from decision".into(),
        ));
    }
    let expected_digest = certificate
        .decision
        .partition_digests
        .get(&request.virtual_partition)
        .ok_or_else(|| AppError::BadRequest("namespace decision has no partition digest".into()))?;
    let prepared = request.prepared.as_ref().ok_or_else(|| {
        AppError::BadRequest("namespace commit requires its quorum-certified payload".into())
    })?;
    if prepared.transaction_id != request.transaction_id
        || prepared.virtual_partition != request.virtual_partition
    {
        return Err(AppError::BadRequest(
            "namespace commit payload identity mismatch".into(),
        ));
    }
    transaction_store()?.commit_prepared_partition(prepared, expected_digest)?;
    Ok(MutationAck {
        node_id: runtime.local_node_id.clone(),
        transaction_id: request.transaction_id,
        virtual_partition: request.virtual_partition,
    })
}

fn local_abort(
    runtime: &ClusterRuntime,
    request: PartitionMutationRequest,
) -> Result<MutationAck, AppError> {
    validate_local_assignment(runtime, request.virtual_partition, &request.assignment)?;
    transaction_store()?.abort_partition(&request.transaction_id, request.virtual_partition)?;
    Ok(MutationAck {
        node_id: runtime.local_node_id.clone(),
        transaction_id: request.transaction_id,
        virtual_partition: request.virtual_partition,
    })
}

fn validate_local_assignment(
    runtime: &ClusterRuntime,
    partition: u64,
    supplied: &DentryPartitionAssignment,
) -> Result<(), AppError> {
    let expected =
        assignment_for_partition_at_epoch(runtime, partition, supplied.membership_epoch)?;
    if &expected != supplied {
        return Err(AppError::Conflict(
            "namespace assignment does not match membership epoch".into(),
        ));
    }
    if !supplied
        .voter_node_ids
        .iter()
        .any(|node_id| node_id == &runtime.local_node_id)
    {
        return Err(AppError::Forbidden(
            "this node is not a namespace partition voter".into(),
        ));
    }
    Ok(())
}

fn validate_certificate(
    runtime: &ClusterRuntime,
    certificate: &NamespaceCommitCertificate,
    transaction_id: &str,
) -> Result<(), AppError> {
    if certificate.decision.transaction_id != transaction_id
        || decision_partition(transaction_id) != certificate.decision_partition
    {
        return Err(AppError::BadRequest(
            "namespace commit certificate identity mismatch".into(),
        ));
    }
    let expected = assignment_for_partition(runtime, certificate.decision_partition)?;
    if expected != certificate.assignment {
        return Err(AppError::Conflict(
            "namespace decision assignment changed".into(),
        ));
    }
    let acknowledgements = certificate
        .acknowledged_voters
        .iter()
        .collect::<BTreeSet<_>>();
    if acknowledgements.len() < quorum_required(expected.voter_node_ids.len())
        || acknowledgements
            .iter()
            .any(|node_id| !expected.voter_node_ids.contains(node_id))
    {
        return Err(AppError::Forbidden(
            "namespace commit certificate lacks a valid voter quorum".into(),
        ));
    }
    Ok(())
}

fn transaction_store() -> Result<NamespaceTransactionStore, AppError> {
    Ok(NamespaceTransactionStore::new(
        global_namespace_facade()?.store().clone(),
    ))
}

async fn peer_prepare(
    headers: HeaderMap,
    State(_state): State<Arc<AppState>>,
    Json(request): Json<PreparePartitionRequest>,
) -> Result<Json<MutationAck>, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    Ok(Json(local_prepare(&runtime, request)?))
}

async fn peer_decision(
    headers: HeaderMap,
    State(_state): State<Arc<AppState>>,
    Json(request): Json<PersistDecisionRequest>,
) -> Result<Json<MutationAck>, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    Ok(Json(local_decision(&runtime, request)?))
}

async fn peer_commit(
    headers: HeaderMap,
    State(_state): State<Arc<AppState>>,
    Json(request): Json<PartitionMutationRequest>,
) -> Result<Json<MutationAck>, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    Ok(Json(local_commit(&runtime, request)?))
}

async fn peer_abort(
    headers: HeaderMap,
    State(_state): State<Arc<AppState>>,
    Json(request): Json<PartitionMutationRequest>,
) -> Result<Json<MutationAck>, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    Ok(Json(local_abort(&runtime, request)?))
}

pub fn build_dentry_transaction_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/n2/peer/namespace-v2/prepare", post(peer_prepare))
        .route("/n2/peer/namespace-v2/decision", post(peer_decision))
        .route("/n2/peer/namespace-v2/commit", post(peer_commit))
        .route("/n2/peer/namespace-v2/abort", post(peer_abort))
}
