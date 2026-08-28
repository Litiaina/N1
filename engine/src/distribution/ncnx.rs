use crate::distribution::membership::{
    ClusterNode, ClusterRuntime, ServerPool, active_cluster_runtime, pool_mutation_authority_nodes,
    pool_write_authority_nodes,
};
use crate::distribution::peer::{authorize_peer, cluster_secret, peer_http_client};
use crate::distribution::root_routing::{ordered_local_roots, writable_local_roots};
use crate::enums::app_error_enum::AppError;
use crate::namespace::ncnx_store::{NcnxRecord, NcnxStore, TrashLocator, validate_logical_key};
use crate::routes::storage_routes::AppState;
use crate::settings::config::CONFIG;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::Duration;

mod batch;
mod healing;
mod listing;
mod local;
mod mutation;
mod transport;

pub use batch::*;
pub use healing::*;
pub use listing::*;
pub use local::*;
pub use mutation::*;

#[cfg(test)]
use listing::{NodeListPage, merge_node_pages};
#[cfg(test)]
use local::local_list;
use local::{run_local_apply, run_local_batch, run_local_list};
use mutation::{pool_for_located, read_from_layout, request_for_replica};
use transport::{PeerNcnxBatchTarget, send_batch_to_target, send_to_replica};

const PEER_NCNX_PATH: &str = "/n2/peer/ncnx-v4";
const PEER_NCNX_BATCH_PATH: &str = "/n2/peer/ncnx-v4/batch";
const PEER_NCNX_TIMEOUT: Duration = Duration::from_secs(20);
pub const PEER_NCNX_BATCH_MAX: usize = 128;
const LOCAL_NCNX_BATCH_CONCURRENCY: usize = 4;

static LOCAL_NCNX_BATCH_BUDGET: once_cell::sync::Lazy<Arc<Semaphore>> =
    once_cell::sync::Lazy::new(|| Arc::new(Semaphore::new(LOCAL_NCNX_BATCH_CONCURRENCY)));
static LOCAL_NCNX_IO_BUDGET: once_cell::sync::Lazy<Arc<Semaphore>> =
    once_cell::sync::Lazy::new(|| {
        let workers = std::thread::available_parallelism()
            .map(|value| value.get().saturating_mul(2))
            .unwrap_or(4)
            .clamp(4, 32);
        Arc::new(Semaphore::new(workers))
    });

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NcnxReplica {
    pub node_id: String,
    pub instance_id: String,
    pub binding_epoch: u64,
    pub root_id: String,
    pub endpoint: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NcnxLayout {
    pub pool_id: String,
    pub pool_generation: u64,
    pub replicas: Vec<NcnxReplica>,
}

impl NcnxLayout {
    pub fn canonical(&self) -> Result<&NcnxReplica, AppError> {
        self.replicas.first().ok_or_else(|| {
            AppError::InternalServerError("NCNX layout has no canonical replica".into())
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerNcnxAction {
    ReadLive,
    ReadTrash,
    CreateLive,
    CreateTrash,
    RemoveLive,
    RemoveTrash,
    RepairLive,
    RepairTrash,
    ListLive,
    ListTrash,
    ListLiveAll,
    ListTrashAll,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerNcnxRequest {
    pub action: PeerNcnxAction,
    pub pool_id: String,
    pub pool_generation: u64,
    pub target_slot_id: String,
    pub target_instance_id: String,
    pub binding_epoch: u64,
    pub fragment_id: String,
    #[serde(default)]
    pub logical_key: String,
    #[serde(default)]
    pub root_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trash: Option<TrashLocator>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<ByteBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PeerNcnxName {
    pub logical_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trash: Option<TrashLocator>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerNcnxResponse {
    pub node_id: String,
    pub root_id: String,
    #[serde(default)]
    pub changed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<ByteBuf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub names: Vec<PeerNcnxName>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PeerNcnxBatchAction {
    CreateLive,
    CreateTrash,
    RemoveLive,
    RemoveTrash,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PeerNcnxBatchItem {
    logical_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    trash: Option<TrashLocator>,
    bytes: ByteBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PeerNcnxBatchRequest {
    action: PeerNcnxBatchAction,
    pool_id: String,
    pool_generation: u64,
    target_slot_id: String,
    target_instance_id: String,
    binding_epoch: u64,
    fragment_id: String,
    root_id: String,
    items: Vec<PeerNcnxBatchItem>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PeerNcnxBatchResponse {
    node_id: String,
    root_id: String,
    changed: usize,
}

#[derive(Clone, Debug)]
pub struct NcnxRenameBatchItem {
    pub located: LocatedNcnx,
    pub destination_key: String,
    pub destination_record: NcnxRecord,
}

#[derive(Clone, Debug)]
pub struct NcnxTrashBatchItem {
    pub located: LocatedNcnx,
    pub locator: TrashLocator,
}

#[derive(Clone, Debug)]
pub struct NcnxRecoverBatchItem {
    pub located: LocatedNcnx,
    pub locator: TrashLocator,
}

#[derive(Clone, Debug)]
pub struct NcnxPublishBatchItem {
    pub logical_key: String,
    pub record: NcnxRecord,
}

#[derive(Clone, Debug)]
pub struct LocatedNcnx {
    pub pool_id: String,
    pub pool_generation: u64,
    pub logical_key: String,
    pub record: NcnxRecord,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NcnxReplicaObservation {
    pub node_id: String,
    pub root_id: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct NcnxRepairOutcome {
    pub layout: NcnxLayout,
    pub located: Option<LocatedNcnx>,
    pub observations: Vec<NcnxReplicaObservation>,
    pub degraded: bool,
    pub repaired_replicas: u64,
    pub healthy: bool,
    pub deferred_by_trash: bool,
}

fn trash_is_expired_at(locator: &TrashLocator, now_unix: i64, retention_seconds: u64) -> bool {
    let deleted_at = locator.deleted_at_unix_nanos / 1_000_000_000;
    now_unix.saturating_sub(deleted_at.max(0)).max(0) as u64 >= retention_seconds
}

pub fn trash_is_expired(locator: &TrashLocator) -> bool {
    trash_is_expired_at(
        locator,
        chrono::Utc::now().timestamp(),
        CONFIG.garbage_collector.retention_seconds,
    )
}

fn placement_score(
    cluster_id: &str,
    pool: &ServerPool,
    fragment_id: &str,
    logical_key: &str,
    node_id: &str,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"n2-ncnx-placement-v3");
    for value in [
        cluster_id,
        pool.id.as_str(),
        fragment_id,
        logical_key,
        node_id,
    ] {
        hasher.update([0]);
        hasher.update(value.as_bytes());
    }
    hasher.update([0]);
    hasher.update(pool.generation.to_be_bytes());
    hasher.finalize().into()
}

fn root_score(
    cluster_id: &str,
    pool: &ServerPool,
    fragment_id: &str,
    logical_key: &str,
    node_id: &str,
    root_id: &str,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"n2-ncnx-root-v3");
    for value in [
        cluster_id,
        pool.id.as_str(),
        fragment_id,
        logical_key,
        node_id,
        root_id,
    ] {
        hasher.update([0]);
        hasher.update(value.as_bytes());
    }
    hasher.update([0]);
    hasher.update(pool.generation.to_be_bytes());
    hasher.finalize().into()
}

pub fn build_ncnx_layout(
    runtime: &ClusterRuntime,
    pool: &ServerPool,
    fragment_id: &str,
    logical_key: &str,
) -> Result<NcnxLayout, AppError> {
    let logical_key = validate_logical_key(logical_key)?;
    let required = pool.policy.namespace_replicas();
    let mut nodes = runtime.membership.nodes_for_pool(&pool.id);
    if nodes.len() < required {
        return Err(AppError::ServiceUnavailable(format!(
            "pool '{}' requires {required} NCNX replicas but has {} nodes",
            pool.id,
            nodes.len()
        )));
    }
    nodes.sort_by(|left, right| {
        placement_score(
            &runtime.membership.cluster_id,
            pool,
            fragment_id,
            &logical_key,
            &right.id,
        )
        .cmp(&placement_score(
            &runtime.membership.cluster_id,
            pool,
            fragment_id,
            &logical_key,
            &left.id,
        ))
        .then(left.id.cmp(&right.id))
    });
    let replicas = nodes
        .into_iter()
        .take(required)
        .map(|node| {
            let root = node
                .storage_roots
                .iter()
                .max_by(|left, right| {
                    root_score(
                        &runtime.membership.cluster_id,
                        pool,
                        fragment_id,
                        &logical_key,
                        &node.id,
                        &left.id,
                    )
                    .cmp(&root_score(
                        &runtime.membership.cluster_id,
                        pool,
                        fragment_id,
                        &logical_key,
                        &node.id,
                        &right.id,
                    ))
                    .then_with(|| right.id.cmp(&left.id))
                })
                .ok_or_else(|| {
                    AppError::ServiceUnavailable(format!(
                        "node '{}' has no root for NCNX placement",
                        node.id
                    ))
                })?;
            Ok(NcnxReplica {
                node_id: node.id,
                instance_id: node.instance_id,
                binding_epoch: runtime.membership.binding_epoch,
                root_id: root.id.clone(),
                endpoint: node.endpoint,
            })
        })
        .collect::<Result<Vec<_>, AppError>>()?;
    Ok(NcnxLayout {
        pool_id: pool.id.clone(),
        pool_generation: pool.generation,
        replicas,
    })
}

fn validate_pool_contract<'a>(
    runtime: &'a ClusterRuntime,
    request: &PeerNcnxRequest,
) -> Result<&'a ServerPool, AppError> {
    let pool = runtime
        .membership
        .pool(&request.pool_id)
        .ok_or_else(|| AppError::Conflict(format!("unknown NCNX pool '{}'", request.pool_id)))?;
    if pool.generation != request.pool_generation {
        return Err(AppError::Conflict(format!(
            "NCNX pool generation {} does not match recorded generation {}",
            request.pool_generation, pool.generation
        )));
    }
    if runtime.local_is_reserve
        || request.binding_epoch != runtime.membership.binding_epoch
        || request.target_slot_id != runtime.local_node_id
        || request.target_instance_id != runtime.local_instance_id
    {
        return Err(AppError::Conflict(format!(
            "stale or misbound NCNX request for slot '{}' instance '{}' epoch {}",
            request.target_slot_id, request.target_instance_id, request.binding_epoch
        )));
    }
    if !pool.node_ids.iter().any(|id| id == &runtime.local_node_id) {
        return Err(AppError::Forbidden(format!(
            "node '{}' is not a member of NCNX pool '{}'",
            runtime.local_node_id, pool.id
        )));
    }
    Ok(pool)
}

fn local_stores(runtime: &ClusterRuntime, preferred_root_id: &str) -> Vec<NcnxStore> {
    ordered_local_roots(runtime, preferred_root_id)
        .into_iter()
        .map(|root| NcnxStore::new(root.id, PathBuf::from(root.path)))
        .collect()
}

fn writable_local_stores(
    runtime: &ClusterRuntime,
    preferred_root_id: &str,
) -> Result<Vec<NcnxStore>, AppError> {
    Ok(writable_local_roots(runtime, preferred_root_id, 0)?
        .into_iter()
        .map(|root| NcnxStore::new(root.id, PathBuf::from(root.path)))
        .collect())
}

#[cfg(test)]
#[path = "ncnx/tests.rs"]
mod tests;
