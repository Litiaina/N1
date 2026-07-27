use crate::distribution::membership::{
    ClusterNode, ClusterRuntime, ServerPool, active_cluster_runtime,
};
use crate::distribution::peer::{authorize_peer, cluster_secret, peer_http_client};
use crate::enums::app_error_enum::AppError;
use crate::namespace::ncnx_store::{NcnxRecord, NcnxStore, TrashLocator, validate_logical_key};
use crate::routes::storage_routes::AppState;
use crate::settings::config::CONFIG;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::post;
use axum::{Json, Router};
use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::time::Duration;

const PEER_NCNX_PATH: &str = "/n2/peer/ncnx-v4";
const PEER_NCNX_TIMEOUT: Duration = Duration::from_secs(20);

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

fn local_store(runtime: &ClusterRuntime, root_id: &str) -> Result<NcnxStore, AppError> {
    let root = runtime
        .local_storage_roots
        .iter()
        .find(|root| root.id == root_id)
        .ok_or_else(|| {
            AppError::Forbidden(format!(
                "NCNX request assigns unknown local root '{root_id}'"
            ))
        })?;
    Ok(NcnxStore::new(&root.id, PathBuf::from(&root.path)))
}

fn local_apply(
    runtime: &ClusterRuntime,
    request: &PeerNcnxRequest,
) -> Result<PeerNcnxResponse, AppError> {
    let pool = validate_pool_contract(runtime, request)?;
    let store = local_store(runtime, &request.root_id)?;
    let expected_layout =
        build_ncnx_layout(runtime, pool, &request.fragment_id, &request.logical_key)?;
    let assigned = expected_layout
        .replicas
        .iter()
        .find(|replica| replica.node_id == runtime.local_node_id)
        .ok_or_else(|| {
            AppError::Forbidden(format!(
                "node '{}' is not an NCNX holder for key '{}'",
                runtime.local_node_id, request.logical_key
            ))
        })?;
    if assigned.root_id != request.root_id {
        return Err(AppError::Conflict(format!(
            "NCNX key '{}' belongs on root '{}', not '{}'",
            request.logical_key, assigned.root_id, request.root_id
        )));
    }
    let mut response = PeerNcnxResponse {
        node_id: runtime.local_node_id.clone(),
        root_id: request.root_id.clone(),
        changed: false,
        bytes: None,
        names: Vec::new(),
    };
    match request.action {
        PeerNcnxAction::ReadLive => {
            response.bytes = store
                .read_live(&request.fragment_id, &request.logical_key)?
                .map(|(_, bytes)| ByteBuf::from(bytes));
        }
        PeerNcnxAction::ReadTrash => {
            let locator = required_trash(request)?;
            response.bytes = store
                .read_trash(&request.fragment_id, locator)?
                .map(|(_, bytes)| ByteBuf::from(bytes));
        }
        PeerNcnxAction::CreateLive => {
            response.changed = store.create_live(
                &request.fragment_id,
                &request.logical_key,
                required_bytes(request)?,
            )?;
        }
        PeerNcnxAction::CreateTrash => {
            response.changed = store.create_trash(
                &request.fragment_id,
                required_trash(request)?,
                required_bytes(request)?,
            )?;
        }
        PeerNcnxAction::RemoveLive => {
            response.changed = store.remove_live_if_exact(
                &request.fragment_id,
                &request.logical_key,
                required_bytes(request)?,
            )?;
        }
        PeerNcnxAction::RemoveTrash => {
            response.changed = store.remove_trash_if_exact(
                &request.fragment_id,
                required_trash(request)?,
                required_bytes(request)?,
            )?;
        }
        PeerNcnxAction::RepairLive => {
            response.changed = store.repair_live(
                &request.fragment_id,
                &request.logical_key,
                required_bytes(request)?,
            )?;
        }
        PeerNcnxAction::RepairTrash => {
            response.changed = store.repair_trash(
                &request.fragment_id,
                required_trash(request)?,
                required_bytes(request)?,
            )?;
        }
        PeerNcnxAction::ListLive
        | PeerNcnxAction::ListTrash
        | PeerNcnxAction::ListLiveAll
        | PeerNcnxAction::ListTrashAll => {
            return Err(AppError::BadRequest(
                "NCNX list requests use the node-wide listing handler".into(),
            ));
        }
    }
    Ok(response)
}

fn local_list(
    runtime: &ClusterRuntime,
    request: &PeerNcnxRequest,
) -> Result<PeerNcnxResponse, AppError> {
    let pool = validate_pool_contract(runtime, request)?;
    let limit = request.limit.unwrap_or(200).clamp(1, 10_000);
    let mut names = BTreeMap::<String, PeerNcnxName>::new();
    for root in &runtime.local_storage_roots {
        let store = NcnxStore::new(&root.id, PathBuf::from(&root.path));
        let canonical_only = matches!(
            request.action,
            PeerNcnxAction::ListLive | PeerNcnxAction::ListTrash
        );
        let mut source_after = request.after.clone();
        let mut accepted_from_root = 0usize;

        // The physical scan limit is not the logical page limit: on a replica
        // root many entries can be non-canonical. Refill this root until it
        // contributes a complete logical page or its ordered stream ends.
        loop {
            let candidates = match request.action {
                PeerNcnxAction::ListLive | PeerNcnxAction::ListLiveAll => store.list_live_names(
                    &request.fragment_id,
                    &request.logical_key,
                    source_after.as_deref(),
                    limit,
                )?,
                PeerNcnxAction::ListTrash | PeerNcnxAction::ListTrashAll => store
                    .list_trash_names(
                        &request.fragment_id,
                        &request.logical_key,
                        source_after.as_deref(),
                        limit,
                    )?,
                _ => unreachable!(),
            };
            if candidates.is_empty() {
                break;
            }
            let source_exhausted = candidates.len() < limit;
            let next_source_after = candidates
                .last()
                .map(|item| ncnx_cursor(&item.logical_key, item.trash.as_ref()));

            for item in candidates {
                let layout =
                    build_ncnx_layout(runtime, pool, &request.fragment_id, &item.logical_key)?;
                let canonical = layout.canonical()?;
                if canonical_only
                    && (canonical.node_id != runtime.local_node_id || canonical.root_id != root.id)
                {
                    continue;
                }
                accepted_from_root = accepted_from_root.saturating_add(1);
                let cursor = ncnx_cursor(&item.logical_key, item.trash.as_ref());
                names.insert(
                    cursor,
                    PeerNcnxName {
                        logical_key: item.logical_key,
                        trash: item.trash,
                    },
                );
                if names.len() > limit {
                    names.pop_last();
                }
            }

            if source_exhausted || accepted_from_root >= limit {
                break;
            }
            if next_source_after == source_after {
                return Err(AppError::InternalServerError(
                    "NCNX listing source did not advance".into(),
                ));
            }
            source_after = next_source_after;
        }
    }
    Ok(PeerNcnxResponse {
        node_id: runtime.local_node_id.clone(),
        root_id: String::new(),
        changed: false,
        bytes: None,
        names: names.into_values().collect(),
    })
}

fn required_bytes(request: &PeerNcnxRequest) -> Result<&[u8], AppError> {
    request
        .bytes
        .as_ref()
        .map(|bytes| bytes.as_slice())
        .ok_or_else(|| AppError::BadRequest("NCNX operation requires bytes".into()))
}

fn required_trash(request: &PeerNcnxRequest) -> Result<&TrashLocator, AppError> {
    request
        .trash
        .as_ref()
        .ok_or_else(|| AppError::BadRequest("NCNX operation requires trash locator".into()))
}

async fn peer_handler(
    headers: HeaderMap,
    State(_state): State<Arc<AppState>>,
    Json(request): Json<PeerNcnxRequest>,
) -> Result<Json<PeerNcnxResponse>, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    let response = match request.action {
        PeerNcnxAction::ListLive
        | PeerNcnxAction::ListTrash
        | PeerNcnxAction::ListLiveAll
        | PeerNcnxAction::ListTrashAll => local_list(&runtime, &request)?,
        _ => local_apply(&runtime, &request)?,
    };
    Ok(Json(response))
}

pub fn build_ncnx_peer_routes() -> Router<Arc<AppState>> {
    Router::new().route(PEER_NCNX_PATH, post(peer_handler))
}

async fn send_to_replica(
    runtime: &ClusterRuntime,
    replica: &NcnxReplica,
    request: PeerNcnxRequest,
) -> Result<PeerNcnxResponse, AppError> {
    if replica.node_id == runtime.local_node_id {
        return local_apply(runtime, &request);
    }
    let client = peer_http_client()?;
    let mut call = client
        .post(format!(
            "{}{}",
            replica.endpoint.trim_end_matches('/'),
            PEER_NCNX_PATH
        ))
        .timeout(PEER_NCNX_TIMEOUT)
        .json(&request);
    if let Some(secret) = cluster_secret() {
        call = call.header("x-n2-cluster-secret", secret);
    }
    let response = call.send().await.map_err(|err| {
        AppError::ServiceUnavailable(format!(
            "NCNX peer '{}' request failed: {err}",
            replica.node_id
        ))
    })?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "NCNX peer '{}' returned {status}: {body}",
            replica.node_id
        )));
    }
    response.json().await.map_err(|err| {
        AppError::InternalServerError(format!(
            "decode NCNX peer '{}' response: {err}",
            replica.node_id
        ))
    })
}

fn request_for_replica(
    action: PeerNcnxAction,
    layout: &NcnxLayout,
    replica: &NcnxReplica,
    fragment_id: &str,
    logical_key: &str,
    trash: Option<TrashLocator>,
    bytes: Option<&[u8]>,
) -> PeerNcnxRequest {
    PeerNcnxRequest {
        action,
        pool_id: layout.pool_id.clone(),
        pool_generation: layout.pool_generation,
        target_slot_id: replica.node_id.clone(),
        target_instance_id: replica.instance_id.clone(),
        binding_epoch: replica.binding_epoch,
        fragment_id: fragment_id.to_string(),
        logical_key: logical_key.to_string(),
        root_id: replica.root_id.clone(),
        trash,
        bytes: bytes.map(|bytes| ByteBuf::from(bytes.to_vec())),
        after: None,
        limit: None,
    }
}

async fn read_from_layout(
    runtime: &ClusterRuntime,
    layout: &NcnxLayout,
    fragment_id: &str,
    logical_key: &str,
    trash: Option<&TrashLocator>,
) -> Result<Option<(NcnxRecord, Vec<u8>)>, AppError> {
    let action = if trash.is_some() {
        PeerNcnxAction::ReadTrash
    } else {
        PeerNcnxAction::ReadLive
    };
    let calls = layout.replicas.iter().map(|replica| {
        send_to_replica(
            runtime,
            replica,
            request_for_replica(
                action.clone(),
                layout,
                replica,
                fragment_id,
                logical_key,
                trash.cloned(),
                None,
            ),
        )
    });
    let observations = join_all(calls).await;
    let mut valid = BTreeMap::<Vec<u8>, NcnxRecord>::new();
    let mut successful_responses = 0usize;
    let mut invalid_responses = 0usize;
    for response in observations.into_iter().flatten() {
        successful_responses += 1;
        if let Some(bytes) = response.bytes {
            let bytes = bytes.into_vec();
            let Ok(record) = NcnxRecord::decode(&bytes) else {
                invalid_responses += 1;
                continue;
            };
            if record.fragment_id != fragment_id {
                invalid_responses += 1;
                continue;
            }
            valid.entry(bytes).or_insert(record);
        }
    }
    if valid.len() > 1 {
        return Err(AppError::Conflict(format!(
            "NCNX replicas disagree for key '{logical_key}'"
        )));
    }
    if let Some((bytes, record)) = valid.into_iter().next() {
        return Ok(Some((record, bytes)));
    }
    if invalid_responses > 0 {
        return Err(AppError::Conflict(format!(
            "all available NCNX bytes for key '{logical_key}' are corrupt or belong to another fragment"
        )));
    }
    if successful_responses == 0 {
        return Err(AppError::ServiceUnavailable(format!(
            "no NCNX replica responded for key '{logical_key}'"
        )));
    }
    if successful_responses != layout.replicas.len() {
        return Err(AppError::ServiceUnavailable(format!(
            "NCNX absence for key '{logical_key}' is unproven: {successful_responses} of {} replicas responded",
            layout.replicas.len()
        )));
    }
    Ok(None)
}

async fn apply_all(
    runtime: &ClusterRuntime,
    layout: &NcnxLayout,
    action: PeerNcnxAction,
    fragment_id: &str,
    logical_key: &str,
    trash: Option<&TrashLocator>,
    bytes: &[u8],
) -> Result<(), AppError> {
    let calls = layout.replicas.iter().map(|replica| {
        send_to_replica(
            runtime,
            replica,
            request_for_replica(
                action.clone(),
                layout,
                replica,
                fragment_id,
                logical_key,
                trash.cloned(),
                Some(bytes),
            ),
        )
    });
    let results = join_all(calls).await;
    let failures = results
        .into_iter()
        .filter_map(Result::err)
        .map(|err| err.to_string())
        .collect::<Vec<_>>();
    if failures.is_empty() {
        Ok(())
    } else {
        Err(AppError::ServiceUnavailable(format!(
            "strict NCNX {}-replica publication failed: {}",
            layout.replicas.len(),
            failures.join("; ")
        )))
    }
}

pub async fn find_live(
    fragment_id: &str,
    logical_key: &str,
) -> Result<Option<LocatedNcnx>, AppError> {
    let logical_key = validate_logical_key(logical_key)?;
    let runtime = active_cluster_runtime()?;
    let mut pools = runtime.membership.pools.iter().collect::<Vec<_>>();
    pools.sort_by_key(|pool| std::cmp::Reverse(pool.generation));
    let mut found: Option<LocatedNcnx> = None;
    for pool in pools {
        let layout = build_ncnx_layout(&runtime, pool, fragment_id, &logical_key)?;
        if let Some((record, bytes)) =
            read_from_layout(&runtime, &layout, fragment_id, &logical_key, None).await?
        {
            if let Some(existing) = &found {
                if existing.record.object_id != record.object_id {
                    return Err(AppError::Conflict(format!(
                        "logical key '{logical_key}' points to different objects in two pools"
                    )));
                }
                continue;
            }
            found = Some(LocatedNcnx {
                pool_id: pool.id.clone(),
                pool_generation: pool.generation,
                logical_key: logical_key.clone(),
                record,
                bytes,
            });
        }
    }
    Ok(found)
}

pub async fn publish_new(
    fragment_id: &str,
    logical_key: &str,
    record: &NcnxRecord,
) -> Result<LocatedNcnx, AppError> {
    let logical_key = validate_logical_key(logical_key)?;
    if let Some(existing) = find_live(fragment_id, &logical_key).await? {
        if existing.record.object_id == record.object_id {
            // A previous strict publication may have reached only part of the
            // exact replica layout before its coordinator returned an error.
            // Reapplying the surviving immutable bytes completes that same
            // publication without a lock, journal, or alternate record.
            let runtime = active_cluster_runtime()?;
            let pool = pool_for_located(&runtime, &existing)?;
            let layout = build_ncnx_layout(&runtime, pool, fragment_id, &logical_key)?;
            apply_all(
                &runtime,
                &layout,
                PeerNcnxAction::CreateLive,
                fragment_id,
                &logical_key,
                None,
                &existing.bytes,
            )
            .await?;
            return Ok(existing);
        }
        return Err(AppError::Conflict(format!(
            "logical key '{logical_key}' already points to another object"
        )));
    }
    let runtime = active_cluster_runtime()?;
    let pool = runtime.membership.newest_writable_pool().ok_or_else(|| {
        AppError::ServiceUnavailable("cluster has no writable server pool".into())
    })?;
    let layout = build_ncnx_layout(&runtime, pool, fragment_id, &logical_key)?;
    let bytes = record.encode()?;
    apply_all(
        &runtime,
        &layout,
        PeerNcnxAction::CreateLive,
        fragment_id,
        &logical_key,
        None,
        &bytes,
    )
    .await?;
    Ok(LocatedNcnx {
        pool_id: pool.id.clone(),
        pool_generation: pool.generation,
        logical_key,
        record: record.clone(),
        bytes,
    })
}

fn pool_for_located<'a>(
    runtime: &'a ClusterRuntime,
    located: &LocatedNcnx,
) -> Result<&'a ServerPool, AppError> {
    let pool = runtime
        .membership
        .pool(&located.pool_id)
        .ok_or_else(|| AppError::Conflict(format!("unknown NCNX pool '{}'", located.pool_id)))?;
    if pool.generation != located.pool_generation {
        return Err(AppError::Conflict(
            "NCNX pool generation changed from its exact home layout".into(),
        ));
    }
    Ok(pool)
}

pub async fn copy_live_to_key(
    located: &LocatedNcnx,
    destination_key: &str,
) -> Result<(), AppError> {
    copy_live_to_key_bytes(located, destination_key, &located.bytes).await
}

pub async fn copy_live_to_key_with_record(
    located: &LocatedNcnx,
    destination_key: &str,
    destination_record: &NcnxRecord,
) -> Result<(), AppError> {
    if destination_record.fragment_id != located.record.fragment_id
        || destination_record.object_id != located.record.object_id
    {
        return Err(AppError::Conflict(
            "renamed NCNX must preserve fragment and object identity".into(),
        ));
    }
    let bytes = destination_record.encode()?;
    copy_live_to_key_bytes(located, destination_key, &bytes).await
}

async fn copy_live_to_key_bytes(
    located: &LocatedNcnx,
    destination_key: &str,
    destination_bytes: &[u8],
) -> Result<(), AppError> {
    let destination_key = validate_logical_key(destination_key)?;
    if let Some(existing) = find_live(&located.record.fragment_id, &destination_key).await? {
        if existing.record.object_id == located.record.object_id {
            let runtime = active_cluster_runtime()?;
            let pool = pool_for_located(&runtime, &existing)?;
            let layout = build_ncnx_layout(
                &runtime,
                pool,
                &located.record.fragment_id,
                &destination_key,
            )?;
            apply_all(
                &runtime,
                &layout,
                PeerNcnxAction::CreateLive,
                &located.record.fragment_id,
                &destination_key,
                None,
                &existing.bytes,
            )
            .await?;
            return Ok(());
        }
        return Err(AppError::Conflict(format!(
            "destination key '{destination_key}' already exists"
        )));
    }
    let runtime = active_cluster_runtime()?;
    let pool = pool_for_located(&runtime, located)?;
    let layout = build_ncnx_layout(
        &runtime,
        pool,
        &located.record.fragment_id,
        &destination_key,
    )?;
    apply_all(
        &runtime,
        &layout,
        PeerNcnxAction::CreateLive,
        &located.record.fragment_id,
        &destination_key,
        None,
        destination_bytes,
    )
    .await
}

pub async fn copy_live_to_trash(
    located: &LocatedNcnx,
    locator: &TrashLocator,
) -> Result<(), AppError> {
    let runtime = active_cluster_runtime()?;
    let pool = pool_for_located(&runtime, located)?;
    let layout = build_ncnx_layout(
        &runtime,
        pool,
        &located.record.fragment_id,
        &located.logical_key,
    )?;
    apply_all(
        &runtime,
        &layout,
        PeerNcnxAction::CreateTrash,
        &located.record.fragment_id,
        &located.logical_key,
        Some(locator),
        &located.bytes,
    )
    .await
}

pub async fn remove_live(located: &LocatedNcnx) -> Result<(), AppError> {
    let runtime = active_cluster_runtime()?;
    let pool = pool_for_located(&runtime, located)?;
    let layout = build_ncnx_layout(
        &runtime,
        pool,
        &located.record.fragment_id,
        &located.logical_key,
    )?;
    apply_all(
        &runtime,
        &layout,
        PeerNcnxAction::RemoveLive,
        &located.record.fragment_id,
        &located.logical_key,
        None,
        &located.bytes,
    )
    .await
}

pub async fn remove_trash(located: &LocatedNcnx, locator: &TrashLocator) -> Result<(), AppError> {
    let runtime = active_cluster_runtime()?;
    let pool = pool_for_located(&runtime, located)?;
    let layout = build_ncnx_layout(
        &runtime,
        pool,
        &located.record.fragment_id,
        &located.logical_key,
    )?;
    apply_all(
        &runtime,
        &layout,
        PeerNcnxAction::RemoveTrash,
        &located.record.fragment_id,
        &located.logical_key,
        Some(locator),
        &located.bytes,
    )
    .await
}

async fn list_node(
    runtime: &ClusterRuntime,
    pool: &ServerPool,
    node: &ClusterNode,
    fragment_id: &str,
    prefix: &str,
    after: Option<&str>,
    limit: usize,
    trash: bool,
    canonical_only: bool,
) -> Result<Vec<PeerNcnxName>, AppError> {
    if node.effective_state() == crate::distribution::membership::NodeState::Degraded {
        return Err(AppError::ServiceUnavailable(format!(
            "NCNX slot '{}' is recovering; listing must use surviving replicas",
            node.id
        )));
    }
    let request = PeerNcnxRequest {
        action: if trash && canonical_only {
            PeerNcnxAction::ListTrash
        } else if trash {
            PeerNcnxAction::ListTrashAll
        } else if canonical_only {
            PeerNcnxAction::ListLive
        } else {
            PeerNcnxAction::ListLiveAll
        },
        pool_id: pool.id.clone(),
        pool_generation: pool.generation,
        target_slot_id: node.id.clone(),
        target_instance_id: node.instance_id.clone(),
        binding_epoch: runtime.membership.binding_epoch,
        fragment_id: fragment_id.to_string(),
        logical_key: prefix.to_string(),
        root_id: String::new(),
        trash: None,
        bytes: None,
        after: after.map(ToOwned::to_owned),
        limit: Some(limit),
    };
    if node.id == runtime.local_node_id {
        return Ok(local_list(runtime, &request)?.names);
    }
    let client = peer_http_client()?;
    let mut call = client
        .post(format!(
            "{}{}",
            node.endpoint.trim_end_matches('/'),
            PEER_NCNX_PATH
        ))
        .timeout(PEER_NCNX_TIMEOUT)
        .json(&request);
    if let Some(secret) = cluster_secret() {
        call = call.header("x-n2-cluster-secret", secret);
    }
    let response = call.send().await.map_err(|err| {
        AppError::ServiceUnavailable(format!("list NCNX from '{}': {err}", node.id))
    })?;
    if !response.status().is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "list NCNX from '{}' returned {}",
            node.id,
            response.status()
        )));
    }
    response
        .json::<PeerNcnxResponse>()
        .await
        .map(|response| response.names)
        .map_err(|err| AppError::InternalServerError(format!("decode NCNX listing: {err}")))
}

struct NodeListPage {
    pool_id: String,
    node_id: String,
    allowed_failures: usize,
    result: Result<Vec<PeerNcnxName>, AppError>,
}

async fn list_all_nodes(
    runtime: &ClusterRuntime,
    fragment_id: &str,
    prefix: &str,
    after: Option<&str>,
    limit: usize,
    trash: bool,
    canonical_only: bool,
) -> Vec<NodeListPage> {
    let mut calls = Vec::new();
    for pool in &runtime.membership.pools {
        for node in runtime.membership.nodes_for_pool(&pool.id) {
            let runtime = runtime.clone();
            let pool = pool.clone();
            let fragment_id = fragment_id.to_string();
            let prefix = prefix.to_string();
            let after = after.map(ToOwned::to_owned);
            calls.push(async move {
                let result = list_node(
                    &runtime,
                    &pool,
                    &node,
                    &fragment_id,
                    &prefix,
                    after.as_deref(),
                    limit,
                    trash,
                    canonical_only,
                )
                .await;
                NodeListPage {
                    pool_id: pool.id,
                    node_id: node.id,
                    allowed_failures: pool.policy.object_parity_shards,
                    result,
                }
            });
        }
    }
    join_all(calls).await
}

fn merge_node_pages(
    pages: Vec<NodeListPage>,
    limit: usize,
    tolerate_failures: bool,
) -> Result<Vec<PeerNcnxName>, AppError> {
    let mut failures = BTreeMap::<String, (usize, usize, Vec<String>)>::new();
    let mut merged = BTreeMap::<String, PeerNcnxName>::new();
    for page in pages {
        match page.result {
            Ok(names) => {
                for name in names {
                    merged.insert(ncnx_cursor(&name.logical_key, name.trash.as_ref()), name);
                }
            }
            Err(err) => {
                let failure = failures
                    .entry(page.pool_id)
                    .or_insert_with(|| (0, page.allowed_failures, Vec::new()));
                failure.0 = failure.0.saturating_add(1);
                failure.2.push(format!("{}: {}", page.node_id, err));
            }
        }
    }

    if let Some((pool_id, (failed, allowed, details))) = failures
        .iter()
        .find(|(_, (failed, allowed, _))| !tolerate_failures || failed > allowed)
    {
        return Err(AppError::ServiceUnavailable(format!(
            "NCNX listing cannot prove a complete result for pool '{}': {} node failures, tolerance {}; {}",
            pool_id,
            failed,
            allowed,
            details.join("; ")
        )));
    }

    Ok(merged.into_values().take(limit).collect())
}

pub async fn list_canonical_names(
    fragment_id: &str,
    prefix: &str,
    after: Option<&str>,
    limit: usize,
    trash: bool,
) -> Result<Vec<PeerNcnxName>, AppError> {
    let runtime = active_cluster_runtime()?;
    let limit = limit.clamp(1, 10_000);
    let canonical = list_all_nodes(
        runtime.as_ref(),
        fragment_id,
        prefix,
        after,
        limit,
        trash,
        true,
    )
    .await;
    if canonical.iter().all(|page| page.result.is_ok()) {
        return merge_node_pages(canonical, limit, false);
    }

    tracing::warn!("NCNX canonical listing lost a source; retrying surviving namespace replicas");
    let replicas = list_all_nodes(
        runtime.as_ref(),
        fragment_id,
        prefix,
        after,
        limit,
        trash,
        false,
    )
    .await;
    merge_node_pages(replicas, limit, true)
}

/// Healing discovery asks every root for physical names so a missing
/// canonical replica cannot hide an otherwise surviving key.
pub async fn list_replica_names(
    fragment_id: &str,
    prefix: &str,
    after: Option<&str>,
    limit: usize,
    trash: bool,
) -> Result<Vec<PeerNcnxName>, AppError> {
    let runtime = active_cluster_runtime()?;
    let limit = limit.clamp(1, 10_000);
    let replicas = list_all_nodes(
        runtime.as_ref(),
        fragment_id,
        prefix,
        after,
        limit,
        trash,
        false,
    )
    .await;
    merge_node_pages(replicas, limit, true)
}

pub async fn read_canonical_record(
    fragment_id: &str,
    name: &PeerNcnxName,
) -> Result<LocatedNcnx, AppError> {
    if name.trash.is_none() {
        return find_live(fragment_id, &name.logical_key)
            .await?
            .ok_or(AppError::NotFound);
    }
    let runtime = active_cluster_runtime()?;
    let locator = name.trash.as_ref().unwrap();
    let mut pools = runtime.membership.pools.iter().collect::<Vec<_>>();
    pools.sort_by_key(|pool| std::cmp::Reverse(pool.generation));
    for pool in pools {
        let layout = build_ncnx_layout(&runtime, pool, fragment_id, &name.logical_key)?;
        if let Some((record, bytes)) = read_from_layout(
            &runtime,
            &layout,
            fragment_id,
            &name.logical_key,
            Some(locator),
        )
        .await?
        {
            return Ok(LocatedNcnx {
                pool_id: pool.id.clone(),
                pool_generation: pool.generation,
                logical_key: name.logical_key.clone(),
                record,
                bytes,
            });
        }
    }
    Err(AppError::NotFound)
}

/// Revalidate an exact namespace-to-object reference after an object lookup
/// failed. This is used only on the exceptional healing path so a concurrent
/// trash expiry, recovery, or rename cannot turn a mixed lockless scan into a
/// false dangling-pointer incident.
pub async fn key_references_object(
    fragment_id: &str,
    logical_key: &str,
    object_id: &str,
) -> Result<bool, AppError> {
    if let Some(live) = find_live(fragment_id, logical_key).await?
        && live.record.object_id == object_id
    {
        return Ok(true);
    }

    let mut cursor = None;
    loop {
        let page =
            list_replica_names(fragment_id, logical_key, cursor.as_deref(), 1_000, true).await?;
        for name in &page {
            if name.logical_key != logical_key {
                continue;
            }
            if name.trash.as_ref().is_some_and(trash_is_expired) {
                continue;
            }
            match read_canonical_record(fragment_id, name).await {
                Ok(located) if located.record.object_id == object_id => return Ok(true),
                Ok(_) | Err(AppError::NotFound) => {}
                Err(error) => return Err(error),
            }
        }
        if page.len() < 1_000 {
            return Ok(false);
        }
        cursor = page
            .last()
            .map(|name| ncnx_cursor(&name.logical_key, name.trash.as_ref()));
    }
}

async fn inspect_name_inner(
    fragment_id: &str,
    name: &PeerNcnxName,
    repair: bool,
) -> Result<NcnxRepairOutcome, AppError> {
    let runtime = active_cluster_runtime()?;
    let located = match read_canonical_record(fragment_id, name).await {
        Ok(located) => located,
        Err(AppError::NotFound) => {
            return Err(AppError::Conflict(format!(
                "all NCNX replicas are unreadable for key '{}'",
                name.logical_key
            )));
        }
        Err(err) => return Err(err),
    };
    let pool = pool_for_located(&runtime, &located)?;
    let layout = build_ncnx_layout(&runtime, pool, fragment_id, &name.logical_key)?;

    // Destination-first trash publication is a tombstone for the old live
    // pointer. A healer must not resurrect that pointer while deletion is
    // removing its replicas.
    let deferred_by_trash = if name.trash.is_none() {
        let candidates =
            list_replica_names(fragment_id, &name.logical_key, None, 128, true).await?;
        let mut tombstone = false;
        for candidate in candidates
            .into_iter()
            .filter(|candidate| candidate.logical_key == name.logical_key)
        {
            if let Ok(trash_record) = read_canonical_record(fragment_id, &candidate).await
                && trash_record.bytes == located.bytes
            {
                tombstone = true;
                break;
            }
        }
        tombstone
    } else {
        false
    };

    let read_action = if name.trash.is_some() {
        PeerNcnxAction::ReadTrash
    } else {
        PeerNcnxAction::ReadLive
    };
    let repair_action = if name.trash.is_some() {
        PeerNcnxAction::RepairTrash
    } else {
        PeerNcnxAction::RepairLive
    };
    let mut observations = Vec::with_capacity(layout.replicas.len());
    let mut repair_targets = Vec::new();
    for replica in &layout.replicas {
        let response = send_to_replica(
            &runtime,
            replica,
            request_for_replica(
                read_action.clone(),
                &layout,
                replica,
                fragment_id,
                &name.logical_key,
                name.trash.clone(),
                None,
            ),
        )
        .await;
        match response {
            Ok(response) if response.bytes.as_deref() == Some(&located.bytes) => {
                observations.push(NcnxReplicaObservation {
                    node_id: replica.node_id.clone(),
                    root_id: replica.root_id.clone(),
                    status: "healthy".into(),
                    error: None,
                });
            }
            Ok(response) => {
                let status = if response.bytes.is_some() {
                    "divergent"
                } else {
                    "missing"
                };
                observations.push(NcnxReplicaObservation {
                    node_id: replica.node_id.clone(),
                    root_id: replica.root_id.clone(),
                    status: status.into(),
                    error: None,
                });
                repair_targets.push(replica.clone());
            }
            Err(err) => {
                observations.push(NcnxReplicaObservation {
                    node_id: replica.node_id.clone(),
                    root_id: replica.root_id.clone(),
                    status: "unreachable_or_corrupt".into(),
                    error: Some(err.to_string()),
                });
                repair_targets.push(replica.clone());
            }
        }
    }
    let degraded = !repair_targets.is_empty();
    let mut repaired_replicas = 0u64;
    if repair && !deferred_by_trash {
        for replica in repair_targets {
            let response = send_to_replica(
                &runtime,
                &replica,
                request_for_replica(
                    repair_action.clone(),
                    &layout,
                    &replica,
                    fragment_id,
                    &name.logical_key,
                    name.trash.clone(),
                    Some(&located.bytes),
                ),
            )
            .await;
            if response.is_ok() {
                repaired_replicas = repaired_replicas.saturating_add(1);
            }
        }
    }
    let healthy = !degraded
        || (repair
            && !deferred_by_trash
            && repaired_replicas as usize
                == observations
                    .iter()
                    .filter(|observation| observation.status != "healthy")
                    .count());
    Ok(NcnxRepairOutcome {
        layout,
        located: Some(located),
        observations,
        degraded,
        repaired_replicas,
        healthy,
        deferred_by_trash,
    })
}

pub async fn inspect_name(
    fragment_id: &str,
    name: &PeerNcnxName,
) -> Result<NcnxRepairOutcome, AppError> {
    inspect_name_inner(fragment_id, name, false).await
}

pub async fn inspect_and_repair_name(
    fragment_id: &str,
    name: &PeerNcnxName,
) -> Result<NcnxRepairOutcome, AppError> {
    inspect_name_inner(fragment_id, name, true).await
}

pub fn ncnx_cursor(key: &str, trash: Option<&TrashLocator>) -> String {
    match trash {
        Some(locator) => format!(
            "{}\0{:020}\0{}",
            key, locator.deleted_at_unix_nanos, locator.delete_mutation_id
        ),
        None => key.to_string(),
    }
}

pub fn unique_object_ids(records: &[LocatedNcnx]) -> BTreeSet<String> {
    records
        .iter()
        .map(|record| record.record.object_id.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::membership::{
        ClusterMembership, ClusterPolicy, N2_MEMBERSHIP_FORMAT_VERSION, NodeState, PoolState,
        StorageRoot,
    };
    use ulid::Ulid;

    fn runtime() -> ClusterRuntime {
        let policy = ClusterPolicy {
            object_data_shards: 4,
            object_parity_shards: 2,
        };
        let nodes = (0..6)
            .map(|index| ClusterNode {
                id: format!("n{index}"),
                instance_id: format!("n{index}"),
                binding_epoch: 1,
                endpoint: format!("https://n{index}:50001"),
                native_endpoint: None,
                state: Some(NodeState::Online),
                weight: 1,
                storage_roots: vec![StorageRoot {
                    id: format!("r{index}"),
                    physical_id: None,
                    path: format!("/disk/{index}"),
                }],
                joined_epoch: 1,
                capacity_bytes: None,
                used_bytes: None,
                last_seen_unix: None,
            })
            .collect::<Vec<_>>();
        ClusterRuntime {
            local_node_id: "n0".into(),
            local_instance_id: "n0".into(),
            local_is_reserve: false,
            local_pool_id: "pool-a".into(),
            local_storage_roots: nodes[0].storage_roots.clone(),
            membership_path: PathBuf::from("/membership"),
            layout_policy: crate::distribution::layout::ClusterLayoutPolicy {
                data_shards: 4,
                parity_shards: 2,
            },
            membership: ClusterMembership {
                format_version: N2_MEMBERSHIP_FORMAT_VERSION,
                cluster_id: "cluster-a".into(),
                epoch: 1,
                binding_epoch: 1,
                pools: vec![ServerPool {
                    id: "pool-a".into(),
                    generation: 1,
                    policy,
                    state: PoolState::Writable,
                    node_ids: (0..6).map(|index| format!("n{index}")).collect(),
                }],
                nodes,
                reserve_nodes: Vec::new(),
                retired_instances: Vec::new(),
            },
            auto_rebalance: false,
        }
    }

    #[test]
    fn four_plus_two_derives_three_distinct_ncnx_holders() {
        let runtime = runtime();
        let pool = runtime.membership.pool("pool-a").unwrap();
        let layout = build_ncnx_layout(&runtime, pool, "fragment", "videos/a.mp4").unwrap();
        assert_eq!(layout.replicas.len(), 3);
        assert_eq!(
            layout
                .replicas
                .iter()
                .map(|replica| &replica.node_id)
                .collect::<BTreeSet<_>>()
                .len(),
            3
        );
    }

    #[test]
    fn ncnx_placement_is_stable_and_key_specific() {
        let runtime = runtime();
        let pool = runtime.membership.pool("pool-a").unwrap();
        let first = build_ncnx_layout(&runtime, pool, "fragment", "videos/a.mp4").unwrap();
        let retry = build_ncnx_layout(&runtime, pool, "fragment", "videos/a.mp4").unwrap();
        let other = build_ncnx_layout(&runtime, pool, "fragment", "videos/b.mp4").unwrap();
        assert_eq!(first, retry);
        assert_ne!(first.replicas, other.replicas);
    }

    #[test]
    fn trash_expiration_matches_gc_boundary() {
        let locator = TrashLocator {
            logical_key: "videos/a.mp4".into(),
            deleted_at_unix_nanos: 100 * 1_000_000_000,
            delete_mutation_id: ulid::Ulid::new().to_string(),
        };
        assert!(!trash_is_expired_at(&locator, 109, 10));
        assert!(trash_is_expired_at(&locator, 110, 10));
        assert!(!trash_is_expired_at(&locator, 99, 10));
    }

    #[test]
    fn local_canonical_listing_refills_past_noncanonical_physical_names() {
        let root = std::env::temp_dir().join(format!("n2-ncnx-refill-{}", Ulid::new()));
        std::fs::create_dir_all(&root).expect("create NCNX test root");
        let mut runtime = runtime();
        runtime.local_storage_roots[0].path = root.to_string_lossy().to_string();
        runtime.membership.nodes[0].storage_roots[0].path = root.to_string_lossy().to_string();
        let pool = runtime.membership.pool("pool-a").unwrap();

        let mut first_noncanonical = None;
        let mut later_canonical = None;
        for index in 0..20_000 {
            let key = format!("key-{index:05}");
            let layout = build_ncnx_layout(&runtime, pool, "fragment", &key).unwrap();
            let stored_here = layout
                .replicas
                .iter()
                .any(|replica| replica.node_id == "n0" && replica.root_id == "r0");
            if !stored_here {
                continue;
            }
            if first_noncanonical.is_none() && layout.canonical().unwrap().node_id != "n0" {
                first_noncanonical = Some(key);
            } else if first_noncanonical.is_some() && layout.canonical().unwrap().node_id == "n0" {
                later_canonical = Some(key);
                break;
            }
        }
        let first_noncanonical = first_noncanonical.expect("find noncanonical key");
        let later_canonical = later_canonical.expect("find later canonical key");
        assert!(first_noncanonical < later_canonical);

        let store = NcnxStore::new("r0", &root);
        for (index, key) in [&first_noncanonical, &later_canonical]
            .into_iter()
            .enumerate()
        {
            let record = NcnxRecord::new(
                "fragment",
                format!("object-{index}"),
                Ulid::new().to_string(),
                1,
            )
            .unwrap();
            store
                .create_live("fragment", key, &record.encode().unwrap())
                .unwrap();
        }
        let request = PeerNcnxRequest {
            action: PeerNcnxAction::ListLive,
            pool_id: "pool-a".into(),
            pool_generation: 1,
            target_slot_id: "n0".into(),
            target_instance_id: "n0".into(),
            binding_epoch: 1,
            fragment_id: "fragment".into(),
            logical_key: String::new(),
            root_id: String::new(),
            trash: None,
            bytes: None,
            after: None,
            limit: Some(1),
        };
        let page = local_list(&runtime, &request).expect("list canonical page");
        assert_eq!(
            page.names,
            vec![PeerNcnxName {
                logical_key: later_canonical,
                trash: None,
            }]
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn replica_listing_tolerates_only_the_configured_namespace_failure_budget() {
        let name = PeerNcnxName {
            logical_key: "videos/a.mp4".into(),
            trash: None,
        };
        let pages = vec![
            NodeListPage {
                pool_id: "pool-a".into(),
                node_id: "n0".into(),
                allowed_failures: 2,
                result: Ok(vec![name.clone()]),
            },
            NodeListPage {
                pool_id: "pool-a".into(),
                node_id: "n1".into(),
                allowed_failures: 2,
                result: Err(AppError::ServiceUnavailable("offline".into())),
            },
            NodeListPage {
                pool_id: "pool-a".into(),
                node_id: "n2".into(),
                allowed_failures: 2,
                result: Err(AppError::ServiceUnavailable("offline".into())),
            },
        ];
        assert_eq!(merge_node_pages(pages, 100, true).unwrap(), vec![name]);

        let pages = (0..3)
            .map(|index| NodeListPage {
                pool_id: "pool-a".into(),
                node_id: format!("n{index}"),
                allowed_failures: 2,
                result: Err(AppError::ServiceUnavailable("offline".into())),
            })
            .collect();
        assert!(merge_node_pages(pages, 100, true).is_err());
    }
}
