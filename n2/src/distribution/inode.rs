use crate::distribution::membership::{
    ClusterNode, ClusterRuntime, NodeRole, active_cluster_runtime, live_namespace_write_nodes,
};
use crate::distribution::peer::{authorize_peer, cluster_secret, peer_http_client};
use crate::enums::app_error_enum::AppError;
use crate::namespace::inode_store::{
    DirectoryInode, DirectoryInodeSnapshot, InodeCommitCertificate, InodeDentry, InodeLayout,
    InodeReplica, InodeStore, InodeTransactionDecision, InodeTransactionParticipant,
    PreparedInodeMutation,
};
use crate::routes::storage_routes::AppState;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::post;
use axum::{Json, Router};
use chrono::Utc;
use futures_util::future::join_all;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

const PEER_INODE_PREFIX: &str = "/n2/peer/inode-v2";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InodeReadKind {
    Inode,
    Dentry,
    List,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerInodeReadRequest {
    pub fragment_id: String,
    pub inode_id: String,
    pub layout: InodeLayout,
    pub kind: InodeReadKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PeerInodeReadResponse {
    pub node_id: String,
    pub root_id: String,
    pub inode: DirectoryInode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dentry: Option<InodeDentry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<InodeDentry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerInodeCreateRequest {
    pub inode: DirectoryInode,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerInodeRepairRequest {
    pub layout: InodeLayout,
    pub snapshot: DirectoryInodeSnapshot,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InodeReplicaObservation {
    pub node_id: String,
    pub root_id: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct InodeRepairOutcome {
    pub snapshot: Option<DirectoryInodeSnapshot>,
    pub observations: Vec<InodeReplicaObservation>,
    pub degraded: bool,
    pub deferred: bool,
    pub repaired_replicas: u64,
    pub healthy: bool,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerInodeMutationRequest {
    pub layout: InodeLayout,
    pub mutation: PreparedInodeMutation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerInodeCommitRequest {
    pub layout: InodeLayout,
    pub mutation: PreparedInodeMutation,
    pub certificate: InodeCommitCertificate,
    pub committed_at_unix_nanos: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerInodeDecisionRequest {
    pub layout: InodeLayout,
    pub mutation: PreparedInodeMutation,
    pub decision: InodeTransactionDecision,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerInodeTransactionCommitRequest {
    pub layout: InodeLayout,
    pub mutation: PreparedInodeMutation,
    pub certificate: InodeCommitCertificate,
    pub decision: InodeTransactionDecision,
    pub committed_at_unix_nanos: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicatedInodeMutation {
    pub layout: InodeLayout,
    pub mutation: PreparedInodeMutation,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PeerInodeAck {
    pub node_id: String,
    pub root_id: String,
    pub inode_generation: u64,
    pub mutation_id: String,
    pub mutation_digest: String,
}

fn quorum_required(replica_count: usize) -> usize {
    (replica_count / 2).saturating_add(1)
}

fn placement_score(fragment_id: &str, inode_id: &str, node_id: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"n2-inode-placement-v2");
    hasher.update([0]);
    hasher.update(fragment_id.as_bytes());
    hasher.update([0]);
    hasher.update(inode_id.as_bytes());
    hasher.update([0]);
    hasher.update(node_id.as_bytes());
    hasher.finalize().into()
}

fn root_score(fragment_id: &str, inode_id: &str, node_id: &str, root_id: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"n2-inode-root-placement-v2");
    hasher.update([0]);
    hasher.update(fragment_id.as_bytes());
    hasher.update([0]);
    hasher.update(inode_id.as_bytes());
    hasher.update([0]);
    hasher.update(node_id.as_bytes());
    hasher.update([0]);
    hasher.update(root_id.as_bytes());
    hasher.finalize().into()
}

pub fn build_inode_layout(
    membership_epoch: u64,
    redundancy: usize,
    fragment_id: &str,
    inode_id: &str,
    candidates: &[ClusterNode],
) -> Result<InodeLayout, AppError> {
    if redundancy < 2 {
        return Err(AppError::BadRequest(
            "inode redundancy must be at least two".into(),
        ));
    }
    let mut candidates = candidates
        .iter()
        .filter(|node| node.role == NodeRole::Namespace)
        .collect::<Vec<_>>();
    if candidates.len() < redundancy {
        return Err(AppError::ServiceUnavailable(format!(
            "inode placement requires {redundancy} namespace nodes, but only {} are eligible",
            candidates.len()
        )));
    }
    candidates.sort_by(|left, right| {
        placement_score(fragment_id, inode_id, &right.id)
            .cmp(&placement_score(fragment_id, inode_id, &left.id))
            .then(left.id.cmp(&right.id))
    });
    let mut replicas = Vec::with_capacity(redundancy);
    for node in candidates.into_iter().take(redundancy) {
        let root = node
            .storage_roots
            .iter()
            .max_by(|left, right| {
                root_score(fragment_id, inode_id, &node.id, &left.id)
                    .cmp(&root_score(fragment_id, inode_id, &node.id, &right.id))
                    .then_with(|| right.id.cmp(&left.id))
            })
            .ok_or_else(|| {
                AppError::ServiceUnavailable(format!(
                    "namespace node '{}' has no eligible root",
                    node.id
                ))
            })?;
        replicas.push(InodeReplica {
            node_id: node.id.clone(),
            root_id: root.id.clone(),
            endpoint: node.endpoint.clone(),
            native_endpoint: node.native_endpoint.clone(),
        });
    }
    let layout = InodeLayout {
        generation: membership_epoch.max(1),
        replicas,
    };
    layout.validate()?;
    Ok(layout)
}

pub async fn select_inode_layout(
    runtime: &ClusterRuntime,
    fragment_id: &str,
    inode_id: &str,
) -> Result<InodeLayout, AppError> {
    let live = live_namespace_write_nodes(runtime).await;
    build_inode_layout(
        runtime.membership.epoch,
        runtime.membership.policy.namespace_redundancy,
        fragment_id,
        inode_id,
        &live,
    )
}

fn local_replica<'a>(
    runtime: &ClusterRuntime,
    layout: &'a InodeLayout,
) -> Result<&'a InodeReplica, AppError> {
    if runtime.local_role != NodeRole::Namespace {
        return Err(AppError::ServiceUnavailable(format!(
            "node '{}' is not a namespace node",
            runtime.local_node_id
        )));
    }
    let replica = layout
        .replicas
        .iter()
        .find(|replica| replica.node_id == runtime.local_node_id)
        .ok_or_else(|| {
            AppError::Forbidden(format!(
                "inode layout does not assign a replica to node '{}'",
                runtime.local_node_id
            ))
        })?;
    if !runtime
        .local_storage_roots
        .iter()
        .any(|root| root.id == replica.root_id)
    {
        return Err(AppError::Conflict(format!(
            "inode layout assigns unknown root '{}' to node '{}'",
            replica.root_id, runtime.local_node_id
        )));
    }
    Ok(replica)
}

fn local_store(runtime: &ClusterRuntime, layout: &InodeLayout) -> Result<InodeStore, AppError> {
    let replica = local_replica(runtime, layout)?;
    let root = runtime
        .local_storage_roots
        .iter()
        .find(|root| root.id == replica.root_id)
        .expect("local replica root validated");
    InodeStore::new(root.id.clone(), PathBuf::from(&root.path))
}

fn validate_stored_layout(expected: &InodeLayout, inode: &DirectoryInode) -> Result<(), AppError> {
    if &inode.layout != expected {
        return Err(AppError::Conflict(format!(
            "inode '{}' stored layout differs from its exact locator",
            inode.inode_id
        )));
    }
    Ok(())
}

fn local_read(
    runtime: &ClusterRuntime,
    request: &PeerInodeReadRequest,
) -> Result<PeerInodeReadResponse, AppError> {
    let replica = local_replica(runtime, &request.layout)?;
    let store = local_store(runtime, &request.layout)?;
    let inode = store.read_inode_consistent(&request.fragment_id, &request.inode_id)?;
    validate_stored_layout(&request.layout, &inode)?;
    let (dentry, entries) = match request.kind {
        InodeReadKind::Inode => (None, Vec::new()),
        InodeReadKind::Dentry => {
            let name = request
                .name
                .as_deref()
                .ok_or_else(|| AppError::BadRequest("inode dentry read requires a name".into()))?;
            (
                store.get_dentry(&request.fragment_id, &request.inode_id, name)?,
                Vec::new(),
            )
        }
        InodeReadKind::List => (
            None,
            store.list_dentries(&request.fragment_id, &request.inode_id)?,
        ),
    };
    let after = store.read_inode_consistent(&request.fragment_id, &request.inode_id)?;
    if after.generation != inode.generation
        || after.last_mutation_id != inode.last_mutation_id
        || after.last_mutation_digest != inode.last_mutation_digest
    {
        return Err(AppError::ServiceUnavailable(format!(
            "inode '{}' changed during read; retry the operation",
            request.inode_id
        )));
    }
    Ok(PeerInodeReadResponse {
        node_id: runtime.local_node_id.clone(),
        root_id: replica.root_id.clone(),
        inode,
        dentry,
        entries,
    })
}

fn local_create(
    runtime: &ClusterRuntime,
    request: &PeerInodeCreateRequest,
) -> Result<PeerInodeAck, AppError> {
    crate::core::operation_gate::ensure_client_mutation_allowed()?;
    let replica = local_replica(runtime, &request.inode.layout)?;
    let store = local_store(runtime, &request.inode.layout)?;
    store.create_inode(&request.inode)?;
    Ok(PeerInodeAck {
        node_id: runtime.local_node_id.clone(),
        root_id: replica.root_id.clone(),
        inode_generation: 0,
        mutation_id: String::new(),
        mutation_digest: String::new(),
    })
}

fn local_repair(
    runtime: &ClusterRuntime,
    request: &PeerInodeRepairRequest,
) -> Result<PeerInodeAck, AppError> {
    if request.layout != request.snapshot.inode.layout {
        return Err(AppError::Conflict(
            "inode repair request layout differs from its source snapshot".into(),
        ));
    }
    let replica = local_replica(runtime, &request.layout)?;
    let store = local_store(runtime, &request.layout)?;
    store.restore_inode_snapshot(&request.snapshot)?;
    Ok(PeerInodeAck {
        node_id: runtime.local_node_id.clone(),
        root_id: replica.root_id.clone(),
        inode_generation: request.snapshot.inode.generation,
        mutation_id: request.snapshot.inode.last_mutation_id.clone(),
        mutation_digest: request.snapshot.inode.last_mutation_digest.clone(),
    })
}

fn local_prepare(
    runtime: &ClusterRuntime,
    request: &PeerInodeMutationRequest,
) -> Result<PeerInodeAck, AppError> {
    crate::core::operation_gate::ensure_client_mutation_allowed()?;
    let replica = local_replica(runtime, &request.layout)?;
    let store = local_store(runtime, &request.layout)?;
    let inode = store.read_inode(&request.mutation.fragment_id, &request.mutation.inode_id)?;
    validate_stored_layout(&request.layout, &inode)?;
    store.prepare(&request.mutation)?;
    Ok(PeerInodeAck {
        node_id: runtime.local_node_id.clone(),
        root_id: replica.root_id.clone(),
        inode_generation: request.mutation.expected_generation,
        mutation_id: request.mutation.mutation_id.clone(),
        mutation_digest: request.mutation.mutation_digest.clone(),
    })
}

fn local_commit(
    runtime: &ClusterRuntime,
    request: &PeerInodeCommitRequest,
) -> Result<PeerInodeAck, AppError> {
    let replica = local_replica(runtime, &request.layout)?;
    let store = local_store(runtime, &request.layout)?;
    let inode = store.commit(
        &request.mutation,
        &request.certificate,
        request.committed_at_unix_nanos,
    )?;
    Ok(PeerInodeAck {
        node_id: runtime.local_node_id.clone(),
        root_id: replica.root_id.clone(),
        inode_generation: inode.generation,
        mutation_id: inode.last_mutation_id,
        mutation_digest: inode.last_mutation_digest,
    })
}

fn local_abort(
    runtime: &ClusterRuntime,
    request: &PeerInodeMutationRequest,
) -> Result<PeerInodeAck, AppError> {
    let replica = local_replica(runtime, &request.layout)?;
    local_store(runtime, &request.layout)?.abort(&request.mutation)?;
    Ok(PeerInodeAck {
        node_id: runtime.local_node_id.clone(),
        root_id: replica.root_id.clone(),
        inode_generation: request.mutation.expected_generation,
        mutation_id: request.mutation.mutation_id.clone(),
        mutation_digest: request.mutation.mutation_digest.clone(),
    })
}

fn local_record_decision(
    runtime: &ClusterRuntime,
    request: &PeerInodeDecisionRequest,
) -> Result<PeerInodeAck, AppError> {
    let replica = local_replica(runtime, &request.layout)?;
    local_store(runtime, &request.layout)?.record_transaction_decision(
        &request.layout,
        &request.mutation,
        &request.decision,
    )?;
    Ok(PeerInodeAck {
        node_id: runtime.local_node_id.clone(),
        root_id: replica.root_id.clone(),
        inode_generation: request.mutation.expected_generation,
        mutation_id: request.mutation.mutation_id.clone(),
        mutation_digest: request.mutation.mutation_digest.clone(),
    })
}

fn local_commit_transaction(
    runtime: &ClusterRuntime,
    request: &PeerInodeTransactionCommitRequest,
) -> Result<PeerInodeAck, AppError> {
    let replica = local_replica(runtime, &request.layout)?;
    let inode = local_store(runtime, &request.layout)?.commit_transaction_participant(
        &request.layout,
        &request.mutation,
        &request.certificate,
        &request.decision,
        request.committed_at_unix_nanos,
    )?;
    Ok(PeerInodeAck {
        node_id: runtime.local_node_id.clone(),
        root_id: replica.root_id.clone(),
        inode_generation: inode.generation,
        mutation_id: inode.last_mutation_id,
        mutation_digest: inode.last_mutation_digest,
    })
}

fn local_finalize_transaction(
    runtime: &ClusterRuntime,
    request: &PeerInodeDecisionRequest,
) -> Result<PeerInodeAck, AppError> {
    let replica = local_replica(runtime, &request.layout)?;
    local_store(runtime, &request.layout)?
        .finalize_transaction_participant(&request.mutation, &request.decision)?;
    Ok(PeerInodeAck {
        node_id: runtime.local_node_id.clone(),
        root_id: replica.root_id.clone(),
        inode_generation: request.mutation.target_generation,
        mutation_id: request.mutation.mutation_id.clone(),
        mutation_digest: request.mutation.mutation_digest.clone(),
    })
}

async fn post_peer<T: Serialize + ?Sized, R: DeserializeOwned>(
    replica: &InodeReplica,
    suffix: &str,
    request: &T,
) -> Result<R, AppError> {
    let client = peer_http_client()?;
    let url = format!(
        "{}{}{}",
        replica.endpoint.trim_end_matches('/'),
        PEER_INODE_PREFIX,
        suffix
    );
    let mut builder = client.post(url).json(request);
    if let Some(secret) = cluster_secret() {
        builder = builder.header("x-n2-cluster-secret", secret);
    }
    let response = builder.send().await.map_err(|error| {
        AppError::ServiceUnavailable(format!(
            "inode peer '{}' request failed: {error}",
            replica.node_id
        ))
    })?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "inode peer '{}' returned {status}: {body}",
            replica.node_id
        )));
    }
    response.json::<R>().await.map_err(|error| {
        AppError::InternalServerError(format!(
            "decode inode peer '{}' response: {error}",
            replica.node_id
        ))
    })
}

async fn read_replica(
    runtime: Arc<ClusterRuntime>,
    replica: InodeReplica,
    request: PeerInodeReadRequest,
) -> Result<PeerInodeReadResponse, AppError> {
    if replica.node_id == runtime.local_node_id {
        local_read(&runtime, &request)
    } else {
        post_peer(&replica, "/read", &request).await
    }
}

async fn create_replica(
    runtime: Arc<ClusterRuntime>,
    replica: InodeReplica,
    request: PeerInodeCreateRequest,
) -> Result<PeerInodeAck, AppError> {
    if replica.node_id == runtime.local_node_id {
        local_create(&runtime, &request)
    } else {
        post_peer(&replica, "/create", &request).await
    }
}

async fn repair_replica(
    runtime: Arc<ClusterRuntime>,
    replica: InodeReplica,
    request: PeerInodeRepairRequest,
) -> Result<PeerInodeAck, AppError> {
    if replica.node_id == runtime.local_node_id {
        local_repair(&runtime, &request)
    } else {
        post_peer(&replica, "/repair-snapshot", &request).await
    }
}

async fn prepare_replica(
    runtime: Arc<ClusterRuntime>,
    replica: InodeReplica,
    request: PeerInodeMutationRequest,
) -> Result<PeerInodeAck, AppError> {
    if replica.node_id == runtime.local_node_id {
        local_prepare(&runtime, &request)
    } else {
        post_peer(&replica, "/prepare", &request).await
    }
}

async fn commit_replica(
    runtime: Arc<ClusterRuntime>,
    replica: InodeReplica,
    request: PeerInodeCommitRequest,
) -> Result<PeerInodeAck, AppError> {
    if replica.node_id == runtime.local_node_id {
        local_commit(&runtime, &request)
    } else {
        post_peer(&replica, "/commit", &request).await
    }
}

async fn abort_replica(
    runtime: Arc<ClusterRuntime>,
    replica: InodeReplica,
    request: PeerInodeMutationRequest,
) -> Result<PeerInodeAck, AppError> {
    if replica.node_id == runtime.local_node_id {
        local_abort(&runtime, &request)
    } else {
        post_peer(&replica, "/abort", &request).await
    }
}

async fn record_decision_replica(
    runtime: Arc<ClusterRuntime>,
    replica: InodeReplica,
    request: PeerInodeDecisionRequest,
) -> Result<PeerInodeAck, AppError> {
    if replica.node_id == runtime.local_node_id {
        local_record_decision(&runtime, &request)
    } else {
        post_peer(&replica, "/decision", &request).await
    }
}

async fn commit_transaction_replica(
    runtime: Arc<ClusterRuntime>,
    replica: InodeReplica,
    request: PeerInodeTransactionCommitRequest,
) -> Result<PeerInodeAck, AppError> {
    if replica.node_id == runtime.local_node_id {
        local_commit_transaction(&runtime, &request)
    } else {
        post_peer(&replica, "/transaction-commit", &request).await
    }
}

async fn finalize_transaction_replica(
    runtime: Arc<ClusterRuntime>,
    replica: InodeReplica,
    request: PeerInodeDecisionRequest,
) -> Result<PeerInodeAck, AppError> {
    if replica.node_id == runtime.local_node_id {
        local_finalize_transaction(&runtime, &request)
    } else {
        post_peer(&replica, "/finalize", &request).await
    }
}

pub async fn create_replicated_inode(
    runtime: Arc<ClusterRuntime>,
    inode: DirectoryInode,
) -> Result<(), AppError> {
    inode.layout.validate()?;
    let request = PeerInodeCreateRequest {
        inode: inode.clone(),
    };
    let operations = inode
        .layout
        .replicas
        .iter()
        .cloned()
        .map(|replica| create_replica(runtime.clone(), replica, request.clone()));
    let results = join_all(operations).await;
    let accepted = results.iter().filter(|result| result.is_ok()).count();
    if accepted != inode.layout.replicas.len() {
        let errors = results
            .into_iter()
            .filter_map(Result::err)
            .map(|error| error.to_string())
            .collect::<Vec<_>>()
            .join("; ");
        return Err(AppError::ServiceUnavailable(format!(
            "inode creation reached {accepted}/{} replicas: {errors}",
            inode.layout.replicas.len()
        )));
    }
    Ok(())
}

fn identical_quorum<T>(values: Vec<T>, required: usize, label: &str) -> Result<T, AppError>
where
    T: Clone + Serialize,
{
    let mut candidates = BTreeMap::<Vec<u8>, (usize, T)>::new();
    for value in values {
        let key = rmp_serde::to_vec_named(&value)?;
        let candidate = candidates.entry(key).or_insert((0, value));
        candidate.0 = candidate.0.saturating_add(1);
        if candidate.0 >= required {
            return Ok(candidate.1.clone());
        }
    }
    Err(AppError::ServiceUnavailable(format!(
        "{label} has no identical {required}-replica quorum"
    )))
}

pub async fn quorum_read(
    runtime: Arc<ClusterRuntime>,
    request: PeerInodeReadRequest,
) -> Result<PeerInodeReadResponse, AppError> {
    let required = quorum_required(request.layout.replicas.len());
    let operations = request
        .layout
        .replicas
        .iter()
        .cloned()
        .map(|replica| read_replica(runtime.clone(), replica, request.clone()));
    let values = join_all(operations)
        .await
        .into_iter()
        .filter_map(Result::ok)
        .map(|mut response| {
            response.node_id.clear();
            response.root_id.clear();
            response
        })
        .collect::<Vec<_>>();
    identical_quorum(values, required, "inode read")
}

fn snapshot_is_certified(snapshot: &DirectoryInodeSnapshot) -> bool {
    let inode = &snapshot.inode;
    if inode.generation == 0 {
        return inode.last_mutation_id.is_empty()
            && inode.last_mutation_digest.is_empty()
            && inode.commit_certificate.is_none();
    }
    let Some(certificate) = inode.commit_certificate.as_ref() else {
        return false;
    };
    let eligible = inode
        .layout
        .replicas
        .iter()
        .map(|replica| replica.node_id.as_str())
        .collect::<BTreeSet<_>>();
    let voters = certificate
        .voter_node_ids
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    certificate.mutation_id == inode.last_mutation_id
        && certificate.mutation_digest == inode.last_mutation_digest
        && certificate.inode_layout_generation == inode.layout.generation
        && certificate.target_generation == inode.generation
        && voters.len() >= quorum_required(inode.layout.replicas.len())
        && voters.iter().all(|node| eligible.contains(node))
}

fn repair_is_deferred_error(error: &AppError) -> bool {
    let message = error.to_string();
    message.contains("pending generation claim")
        || message.contains("pending cross-inode decision")
        || message.contains("committed cross-inode decision awaiting complete apply")
        || message.contains("changed during read; retry")
        || message.contains("changed while its repair snapshot was read")
}

fn select_repair_snapshot(
    snapshots: &[DirectoryInodeSnapshot],
    required: usize,
) -> Result<DirectoryInodeSnapshot, AppError> {
    let highest = snapshots
        .iter()
        .map(|snapshot| snapshot.inode.generation)
        .max()
        .ok_or_else(|| AppError::ServiceUnavailable("no readable inode replica survives".into()))?;
    let candidates = snapshots
        .iter()
        .filter(|snapshot| snapshot.inode.generation == highest)
        .collect::<Vec<_>>();
    let mut unique = BTreeMap::<Vec<u8>, (usize, &DirectoryInodeSnapshot)>::new();
    for candidate in candidates {
        let entry = unique
            .entry(rmp_serde::to_vec_named(candidate)?)
            .or_insert((0, candidate));
        entry.0 = entry.0.saturating_add(1);
    }
    if unique.len() == 1 {
        let (count, candidate) = unique
            .into_values()
            .next()
            .expect("one unique inode repair candidate");
        if count >= required || snapshot_is_certified(candidate) {
            return Ok(candidate.clone());
        }
    } else {
        let quorum = unique
            .into_values()
            .filter(|(count, _)| *count >= required)
            .collect::<Vec<_>>();
        if quorum.len() == 1 {
            return Ok(quorum[0].1.clone());
        }
    }
    Err(AppError::ServiceUnavailable(format!(
        "inode replicas have no unique certified generation at {highest}"
    )))
}

async fn read_repair_snapshots(
    runtime: Arc<ClusterRuntime>,
    fragment_id: &str,
    inode_id: &str,
    layout: &InodeLayout,
) -> Vec<(InodeReplica, Result<DirectoryInodeSnapshot, AppError>)> {
    let request = PeerInodeReadRequest {
        fragment_id: fragment_id.to_string(),
        inode_id: inode_id.to_string(),
        layout: layout.clone(),
        kind: InodeReadKind::List,
        name: None,
    };
    join_all(layout.replicas.iter().cloned().map(|replica| {
        let runtime = runtime.clone();
        let request = request.clone();
        async move {
            let result = read_replica(runtime, replica.clone(), request)
                .await
                .and_then(|response| {
                    let snapshot = DirectoryInodeSnapshot {
                        inode: response.inode,
                        entries: response.entries,
                    };
                    snapshot.validate()?;
                    Ok(snapshot)
                });
            (replica, result)
        }
    }))
    .await
}

/// Verify and, where authority is provable, restore one complete directory
/// inode replica group. Replica width and exact placement never change.
pub async fn inspect_and_repair_inode(
    runtime: Arc<ClusterRuntime>,
    fragment_id: &str,
    inode_id: &str,
    layout: &InodeLayout,
) -> InodeRepairOutcome {
    let required = quorum_required(layout.replicas.len());
    let first = read_repair_snapshots(runtime.clone(), fragment_id, inode_id, layout).await;
    let readable = first
        .iter()
        .filter_map(|(_, result)| result.as_ref().ok().cloned())
        .collect::<Vec<_>>();
    let canonical = match select_repair_snapshot(&readable, required) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            let deferred = first
                .iter()
                .filter_map(|(_, result)| result.as_ref().err())
                .any(repair_is_deferred_error);
            return InodeRepairOutcome {
                snapshot: None,
                observations: first
                    .into_iter()
                    .map(|(replica, result)| match result {
                        Ok(snapshot) => InodeReplicaObservation {
                            node_id: replica.node_id,
                            root_id: replica.root_id,
                            status: "divergent".into(),
                            generation: Some(snapshot.inode.generation),
                            error: None,
                        },
                        Err(error) => InodeReplicaObservation {
                            node_id: replica.node_id,
                            root_id: replica.root_id,
                            status: "unavailable_or_invalid".into(),
                            generation: None,
                            error: Some(error.to_string()),
                        },
                    })
                    .collect(),
                degraded: true,
                deferred,
                repaired_replicas: 0,
                healthy: false,
                error: Some(if deferred {
                    "inode scan was deferred because a certified namespace mutation is in progress"
                        .into()
                } else {
                    error.to_string()
                }),
            };
        }
    };

    let mut observations = Vec::with_capacity(first.len());
    let mut repair_targets = Vec::new();
    let mut deferred = false;
    for (replica, result) in first {
        match result {
            Ok(snapshot) if snapshot == canonical => observations.push(InodeReplicaObservation {
                node_id: replica.node_id,
                root_id: replica.root_id,
                status: "healthy".into(),
                generation: Some(snapshot.inode.generation),
                error: None,
            }),
            Ok(snapshot) if snapshot.inode.generation < canonical.inode.generation => {
                observations.push(InodeReplicaObservation {
                    node_id: replica.node_id.clone(),
                    root_id: replica.root_id.clone(),
                    status: "stale".into(),
                    generation: Some(snapshot.inode.generation),
                    error: None,
                });
                repair_targets.push(replica);
            }
            Ok(snapshot) => observations.push(InodeReplicaObservation {
                node_id: replica.node_id,
                root_id: replica.root_id,
                status: "divergent".into(),
                generation: Some(snapshot.inode.generation),
                error: Some("valid replica differs at the authoritative generation".into()),
            }),
            Err(error) if repair_is_deferred_error(&error) => {
                deferred = true;
                observations.push(InodeReplicaObservation {
                    node_id: replica.node_id,
                    root_id: replica.root_id,
                    status: "busy".into(),
                    generation: None,
                    error: Some(error.to_string()),
                });
            }
            Err(error) => {
                observations.push(InodeReplicaObservation {
                    node_id: replica.node_id.clone(),
                    root_id: replica.root_id.clone(),
                    status: "unavailable_or_invalid".into(),
                    generation: None,
                    error: Some(error.to_string()),
                });
                repair_targets.push(replica);
            }
        }
    }
    let degraded = observations.iter().any(|item| item.status != "healthy");
    let mut repaired_replicas = 0u64;
    let request = PeerInodeRepairRequest {
        layout: layout.clone(),
        snapshot: canonical.clone(),
    };
    for replica in repair_targets {
        match repair_replica(runtime.clone(), replica, request.clone()).await {
            Ok(_) => repaired_replicas = repaired_replicas.saturating_add(1),
            Err(error) if repair_is_deferred_error(&error) => deferred = true,
            Err(_) => {}
        }
    }

    let final_reads = read_repair_snapshots(runtime, fragment_id, inode_id, layout).await;
    deferred |= final_reads
        .iter()
        .filter_map(|(_, result)| result.as_ref().err())
        .any(repair_is_deferred_error);
    deferred |= final_reads
        .iter()
        .filter_map(|(_, result)| result.as_ref().ok())
        .any(|snapshot| snapshot.inode.generation > canonical.inode.generation);
    let healthy = final_reads
        .iter()
        .all(|(_, result)| result.as_ref().is_ok_and(|snapshot| snapshot == &canonical));
    let error = (!healthy).then(|| {
        if deferred {
            "inode repair was deferred because a certified namespace mutation is in progress"
                .to_string()
        } else {
            "one or more exact inode replicas remained unavailable, corrupt, stale, or divergent"
                .to_string()
        }
    });
    InodeRepairOutcome {
        snapshot: Some(canonical),
        observations,
        degraded,
        deferred,
        repaired_replicas,
        healthy,
        error,
    }
}

pub async fn commit_replicated_mutation(
    runtime: Arc<ClusterRuntime>,
    layout: InodeLayout,
    mutation: PreparedInodeMutation,
) -> Result<InodeCommitCertificate, AppError> {
    layout.validate()?;
    let required = quorum_required(layout.replicas.len());
    let prepare_request = PeerInodeMutationRequest {
        layout: layout.clone(),
        mutation: mutation.clone(),
    };
    let prepare_operations = layout.replicas.iter().cloned().map(|replica| {
        let request = prepare_request.clone();
        let runtime = runtime.clone();
        async move {
            let result = prepare_replica(runtime, replica.clone(), request).await;
            (replica, result)
        }
    });
    let prepared = join_all(prepare_operations).await;
    let successful = prepared
        .iter()
        .filter_map(|(replica, result)| result.as_ref().ok().map(|_| replica.clone()))
        .collect::<Vec<_>>();
    if successful.len() < required {
        let aborts = successful
            .into_iter()
            .map(|replica| abort_replica(runtime.clone(), replica, prepare_request.clone()));
        let _ = join_all(aborts).await;
        return Err(AppError::ServiceUnavailable(format!(
            "inode mutation prepare reached {}/{} required replicas",
            prepared.iter().filter(|(_, result)| result.is_ok()).count(),
            required
        )));
    }
    let voter_node_ids = successful
        .iter()
        .map(|replica| replica.node_id.clone())
        .collect::<Vec<_>>();
    let certificate = InodeCommitCertificate::new(&mutation, &layout, voter_node_ids)?;
    let commit_request = PeerInodeCommitRequest {
        layout: layout.clone(),
        mutation: mutation.clone(),
        certificate: certificate.clone(),
        committed_at_unix_nanos: Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_else(|| Utc::now().timestamp().saturating_mul(1_000_000_000)),
    };
    let commits = successful
        .into_iter()
        .map(|replica| commit_replica(runtime.clone(), replica, commit_request.clone()));
    let committed = join_all(commits)
        .await
        .into_iter()
        .filter(Result::is_ok)
        .count();
    if committed < required {
        return Err(AppError::ServiceUnavailable(format!(
            "inode mutation commit reached {committed}/{required} required replicas; recovery must replay mutation '{}'",
            mutation.mutation_id
        )));
    }
    Ok(certificate)
}

pub async fn commit_replicated_transaction(
    runtime: Arc<ClusterRuntime>,
    transaction_id: String,
    mut participants: Vec<ReplicatedInodeMutation>,
) -> Result<InodeTransactionDecision, AppError> {
    if participants.len() < 2 {
        return Err(AppError::BadRequest(
            "replicated inode transaction requires at least two participants".into(),
        ));
    }
    participants.sort_by(|left, right| left.mutation.inode_id.cmp(&right.mutation.inode_id));
    let mut inode_ids = BTreeSet::new();
    if participants
        .iter()
        .any(|participant| !inode_ids.insert(participant.mutation.inode_id.clone()))
    {
        return Err(AppError::BadRequest(
            "replicated inode transaction contains duplicate inode groups".into(),
        ));
    }
    for participant in &participants {
        participant.layout.validate()?;
    }

    let mut prepared_groups = Vec::<(ReplicatedInodeMutation, Vec<InodeReplica>)>::new();
    for participant in &participants {
        let required = quorum_required(participant.layout.replicas.len());
        let request = PeerInodeMutationRequest {
            layout: participant.layout.clone(),
            mutation: participant.mutation.clone(),
        };
        let operations = participant.layout.replicas.iter().cloned().map(|replica| {
            let request = request.clone();
            let runtime = runtime.clone();
            async move {
                let result = prepare_replica(runtime, replica.clone(), request).await;
                (replica, result)
            }
        });
        let results = join_all(operations).await;
        let successful = results
            .into_iter()
            .filter_map(|(replica, result)| result.ok().map(|_| replica))
            .collect::<Vec<_>>();
        if successful.len() < required {
            for (prepared, replicas) in prepared_groups {
                let abort_request = PeerInodeMutationRequest {
                    layout: prepared.layout,
                    mutation: prepared.mutation,
                };
                let aborts = replicas
                    .into_iter()
                    .map(|replica| abort_replica(runtime.clone(), replica, abort_request.clone()));
                let _ = join_all(aborts).await;
            }
            let aborts = successful
                .into_iter()
                .map(|replica| abort_replica(runtime.clone(), replica, request.clone()));
            let _ = join_all(aborts).await;
            return Err(AppError::ServiceUnavailable(format!(
                "transaction '{}' could not prepare inode '{}' on {required} replicas",
                transaction_id, participant.mutation.inode_id
            )));
        }
        prepared_groups.push((participant.clone(), successful));
    }

    let decision = InodeTransactionDecision::new(
        transaction_id.clone(),
        participants
            .iter()
            .map(|participant| {
                InodeTransactionParticipant::new(&participant.layout, &participant.mutation)
            })
            .collect(),
    )?;

    for (participant, replicas) in &prepared_groups {
        let required = quorum_required(participant.layout.replicas.len());
        let request = PeerInodeDecisionRequest {
            layout: participant.layout.clone(),
            mutation: participant.mutation.clone(),
            decision: decision.clone(),
        };
        let results = join_all(
            replicas
                .iter()
                .cloned()
                .map(|replica| record_decision_replica(runtime.clone(), replica, request.clone())),
        )
        .await;
        if results.iter().filter(|result| result.is_ok()).count() < required {
            return Err(AppError::ServiceUnavailable(format!(
                "transaction '{}' decision is incomplete for inode '{}'; retry the same transaction ID",
                transaction_id, participant.mutation.inode_id
            )));
        }
    }

    let committed_at_unix_nanos = Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| Utc::now().timestamp().saturating_mul(1_000_000_000));
    let mut committed_groups = Vec::new();
    for (participant, replicas) in &prepared_groups {
        let required = quorum_required(participant.layout.replicas.len());
        let certificate = InodeCommitCertificate::new(
            &participant.mutation,
            &participant.layout,
            replicas
                .iter()
                .map(|replica| replica.node_id.clone())
                .collect(),
        )?;
        let request = PeerInodeTransactionCommitRequest {
            layout: participant.layout.clone(),
            mutation: participant.mutation.clone(),
            certificate,
            decision: decision.clone(),
            committed_at_unix_nanos,
        };
        let results = join_all(replicas.iter().cloned().map(|replica| {
            let request = request.clone();
            let runtime = runtime.clone();
            async move {
                let result = commit_transaction_replica(runtime, replica.clone(), request).await;
                (replica, result)
            }
        }))
        .await;
        let committed = results
            .into_iter()
            .filter_map(|(replica, result)| result.ok().map(|_| replica))
            .collect::<Vec<_>>();
        if committed.len() < required {
            return Err(AppError::ServiceUnavailable(format!(
                "transaction '{}' commit is incomplete for inode '{}'; durable decision requires replay",
                transaction_id, participant.mutation.inode_id
            )));
        }
        committed_groups.push((participant.clone(), committed));
    }

    for (participant, replicas) in committed_groups {
        let required = quorum_required(participant.layout.replicas.len());
        let request = PeerInodeDecisionRequest {
            layout: participant.layout,
            mutation: participant.mutation,
            decision: decision.clone(),
        };
        let finalized = join_all(replicas.into_iter().map(|replica| {
            finalize_transaction_replica(runtime.clone(), replica, request.clone())
        }))
        .await
        .into_iter()
        .filter(Result::is_ok)
        .count();
        if finalized < required {
            return Err(AppError::ServiceUnavailable(format!(
                "transaction '{}' committed inode '{}' but finalized {finalized}/{required} replicas; retry the same transaction ID",
                transaction_id, request.mutation.inode_id
            )));
        }
    }
    Ok(decision)
}

async fn peer_read_handler(
    headers: HeaderMap,
    State(_state): State<Arc<AppState>>,
    Json(request): Json<PeerInodeReadRequest>,
) -> Result<Json<PeerInodeReadResponse>, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    Ok(Json(local_read(&runtime, &request)?))
}

async fn peer_create_handler(
    headers: HeaderMap,
    State(_state): State<Arc<AppState>>,
    Json(request): Json<PeerInodeCreateRequest>,
) -> Result<Json<PeerInodeAck>, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    Ok(Json(local_create(&runtime, &request)?))
}

async fn peer_repair_handler(
    headers: HeaderMap,
    State(_state): State<Arc<AppState>>,
    Json(request): Json<PeerInodeRepairRequest>,
) -> Result<Json<PeerInodeAck>, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    Ok(Json(local_repair(&runtime, &request)?))
}

async fn peer_prepare_handler(
    headers: HeaderMap,
    State(_state): State<Arc<AppState>>,
    Json(request): Json<PeerInodeMutationRequest>,
) -> Result<Json<PeerInodeAck>, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    Ok(Json(local_prepare(&runtime, &request)?))
}

async fn peer_commit_handler(
    headers: HeaderMap,
    State(_state): State<Arc<AppState>>,
    Json(request): Json<PeerInodeCommitRequest>,
) -> Result<Json<PeerInodeAck>, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    Ok(Json(local_commit(&runtime, &request)?))
}

async fn peer_abort_handler(
    headers: HeaderMap,
    State(_state): State<Arc<AppState>>,
    Json(request): Json<PeerInodeMutationRequest>,
) -> Result<Json<PeerInodeAck>, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    Ok(Json(local_abort(&runtime, &request)?))
}

async fn peer_decision_handler(
    headers: HeaderMap,
    State(_state): State<Arc<AppState>>,
    Json(request): Json<PeerInodeDecisionRequest>,
) -> Result<Json<PeerInodeAck>, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    Ok(Json(local_record_decision(&runtime, &request)?))
}

async fn peer_transaction_commit_handler(
    headers: HeaderMap,
    State(_state): State<Arc<AppState>>,
    Json(request): Json<PeerInodeTransactionCommitRequest>,
) -> Result<Json<PeerInodeAck>, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    Ok(Json(local_commit_transaction(&runtime, &request)?))
}

async fn peer_finalize_handler(
    headers: HeaderMap,
    State(_state): State<Arc<AppState>>,
    Json(request): Json<PeerInodeDecisionRequest>,
) -> Result<Json<PeerInodeAck>, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    Ok(Json(local_finalize_transaction(&runtime, &request)?))
}

pub fn build_inode_peer_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            &format!("{PEER_INODE_PREFIX}/read"),
            post(peer_read_handler),
        )
        .route(
            &format!("{PEER_INODE_PREFIX}/create"),
            post(peer_create_handler),
        )
        .route(
            &format!("{PEER_INODE_PREFIX}/repair-snapshot"),
            post(peer_repair_handler),
        )
        .route(
            &format!("{PEER_INODE_PREFIX}/prepare"),
            post(peer_prepare_handler),
        )
        .route(
            &format!("{PEER_INODE_PREFIX}/commit"),
            post(peer_commit_handler),
        )
        .route(
            &format!("{PEER_INODE_PREFIX}/abort"),
            post(peer_abort_handler),
        )
        .route(
            &format!("{PEER_INODE_PREFIX}/decision"),
            post(peer_decision_handler),
        )
        .route(
            &format!("{PEER_INODE_PREFIX}/transaction-commit"),
            post(peer_transaction_commit_handler),
        )
        .route(
            &format!("{PEER_INODE_PREFIX}/finalize"),
            post(peer_finalize_handler),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::membership::{NodeState, StorageRoot};

    fn node(id: &str, joined_epoch: u64) -> ClusterNode {
        ClusterNode {
            id: id.into(),
            role: NodeRole::Namespace,
            endpoint: format!("https://{id}"),
            native_endpoint: None,
            state: Some(NodeState::Online),
            weight: 1,
            storage_roots: vec![StorageRoot {
                id: format!("root-{id}"),
                path: format!("/disk-{id}"),
            }],
            joined_epoch,
            capacity_bytes: None,
            used_bytes: None,
            last_seen_unix: None,
        }
    }

    #[test]
    fn inode_layout_is_bounded_and_records_exact_roots() {
        let nodes = vec![node("a", 1), node("b", 1), node("c", 1), node("d", 1)];
        let layout = build_inode_layout(9, 2, "fragment", "inode", &nodes).expect("layout");
        assert_eq!(layout.replicas.len(), 2);
        assert!(
            layout
                .replicas
                .iter()
                .all(|replica| !replica.root_id.is_empty())
        );
        assert_eq!(
            layout
                .replicas
                .iter()
                .map(|replica| &replica.node_id)
                .collect::<BTreeSet<_>>()
                .len(),
            2
        );
    }

    #[test]
    fn recorded_inode_layout_does_not_remap_after_expansion() {
        let mut nodes = vec![node("a", 1), node("b", 1)];
        let recorded = build_inode_layout(7, 2, "fragment", "inode", &nodes).expect("layout");
        nodes.push(node("c", 8));
        assert_eq!(recorded.replicas.len(), 2);
        assert!(
            !recorded
                .replicas
                .iter()
                .any(|replica| replica.node_id == "c")
        );
        let next = build_inode_layout(8, 2, "fragment", "other-inode", &nodes).expect("new");
        assert_eq!(next.replicas.len(), 2);
    }

    #[test]
    fn commit_certificate_rejects_less_than_quorum() {
        let layout = build_inode_layout(
            1,
            3,
            "fragment",
            "inode",
            &[node("a", 1), node("b", 1), node("c", 1)],
        )
        .expect("layout");
        let mutation = PreparedInodeMutation::new(
            "fragment".into(),
            "inode".into(),
            0,
            "mutation".into(),
            1,
            vec![crate::namespace::inode_store::InodeMutationChange::Remove {
                name: "entry".into(),
            }],
        )
        .expect("mutation");
        assert!(InodeCommitCertificate::new(&mutation, &layout, vec!["a".into()]).is_err());
    }

    fn certified_snapshot_with_width(
        parent: Option<&str>,
        redundancy: usize,
    ) -> DirectoryInodeSnapshot {
        let candidates = [node("a", 1), node("b", 1), node("c", 1)];
        let layout = build_inode_layout(
            1,
            redundancy,
            "fragment",
            "inode",
            &candidates[..redundancy],
        )
        .expect("layout");
        let mutation = PreparedInodeMutation::new(
            "fragment".into(),
            "inode".into(),
            0,
            "mutation".into(),
            1,
            vec![crate::namespace::inode_store::InodeMutationChange::Remove {
                name: "absent".into(),
            }],
        )
        .expect("mutation");
        let voters = layout
            .replicas
            .iter()
            .map(|replica| replica.node_id.clone())
            .collect();
        let certificate =
            InodeCommitCertificate::new(&mutation, &layout, voters).expect("certificate");
        let mut inode = DirectoryInode::new(
            "fragment".into(),
            "inode".into(),
            parent.map(str::to_string),
            layout,
            1,
        )
        .expect("inode");
        inode.generation = 1;
        inode.last_mutation_id = mutation.mutation_id;
        inode.last_mutation_digest = mutation.mutation_digest;
        inode.commit_certificate = Some(certificate);
        DirectoryInodeSnapshot {
            inode,
            entries: Vec::new(),
        }
    }

    fn certified_snapshot(parent: Option<&str>) -> DirectoryInodeSnapshot {
        certified_snapshot_with_width(parent, 2)
    }

    fn advance_snapshot(mut snapshot: DirectoryInodeSnapshot) -> DirectoryInodeSnapshot {
        let mutation = PreparedInodeMutation::new(
            snapshot.inode.fragment_id.clone(),
            snapshot.inode.inode_id.clone(),
            snapshot.inode.generation,
            "mutation-next".into(),
            2,
            vec![crate::namespace::inode_store::InodeMutationChange::Remove {
                name: "still-absent".into(),
            }],
        )
        .expect("next mutation");
        let voters = snapshot
            .inode
            .layout
            .replicas
            .iter()
            .map(|replica| replica.node_id.clone())
            .collect();
        let certificate = InodeCommitCertificate::new(&mutation, &snapshot.inode.layout, voters)
            .expect("next certificate");
        snapshot.inode.generation = mutation.target_generation;
        snapshot.inode.last_mutation_id = mutation.mutation_id;
        snapshot.inode.last_mutation_digest = mutation.mutation_digest;
        snapshot.inode.commit_certificate = Some(certificate);
        snapshot
    }

    #[test]
    fn one_surviving_certified_two_replica_snapshot_can_rebuild_its_peer() {
        let source = certified_snapshot(None);
        assert_eq!(
            select_repair_snapshot(std::slice::from_ref(&source), 2).expect("certified source"),
            source
        );
    }

    #[test]
    fn same_generation_divergence_is_never_guessed() {
        let first = certified_snapshot(None);
        let second = certified_snapshot(Some("another-parent"));
        assert!(select_repair_snapshot(&[first, second], 2).is_err());
    }

    #[test]
    fn certified_newer_generation_wins_over_an_older_identical_quorum() {
        let older = certified_snapshot_with_width(None, 3);
        let newer = advance_snapshot(older.clone());
        assert_eq!(
            select_repair_snapshot(&[older.clone(), older, newer.clone()], 2)
                .expect("newer certified generation"),
            newer
        );
    }
}
