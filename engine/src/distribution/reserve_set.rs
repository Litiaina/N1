use crate::distribution::auto_healing::{AutoHealingStatus, run_replacement_healing_cycle};
use crate::distribution::membership::{
    ClusterMembership, ClusterNode, ClusterRuntime, NodeState, ReserveNode, RetiredInstance,
    StorageRoot, active_cluster_runtime, cluster_node_is_reachable, commit_local_membership,
};
use crate::distribution::peer::{
    authorize_peer, cluster_secret, peer_http_client, sync_fragment_controls_from_cluster,
};
use crate::enums::app_error_enum::AppError;
use crate::routes::storage_routes::AppState;
use crate::settings::config::CONFIG;
use crate::structs::auth_structs::Claims;
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{Duration, sleep, timeout};

#[derive(Clone, Debug, Default, Serialize)]
pub struct ReserveSetStatus {
    pub enabled: bool,
    pub automatic_promotion: bool,
    pub running: bool,
    pub topology_epoch: u64,
    pub binding_epoch: u64,
    pub active_slots: usize,
    pub reserve_instances: usize,
    pub recovering_slots: Vec<String>,
    pub suspected_slots: Vec<String>,
    pub last_plan_id: Option<String>,
    pub last_promoted_slot: Option<String>,
    pub last_promoted_instance: Option<String>,
    pub last_started_at_unix: Option<i64>,
    pub last_finished_at_unix: Option<i64>,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReservePromotionRequest {
    pub slot_id: String,
    pub reserve_instance_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PeerReservePrepareRequest {
    cluster_id: String,
    topology_epoch: u64,
    binding_epoch: u64,
    slot_id: String,
    failed_instance_id: String,
    reserve_instance_id: String,
    plan_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PeerReservePrepareResponse {
    voter_slot_id: String,
    voter_instance_id: String,
    accepted: bool,
    observed_failed_unreachable: bool,
    plan_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PeerReserveCommitRequest {
    previous_binding_epoch: u64,
    plan_id: String,
    membership: ClusterMembership,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PeerReserveCommitResponse {
    local_instance_id: String,
    binding_epoch: u64,
    committed: bool,
    plan_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PeerReserveMembershipResponse {
    local_instance_id: String,
    membership: ClusterMembership,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PeerReserveActivateRequest {
    plan_id: String,
    membership: ClusterMembership,
}

#[derive(Clone, Debug, Serialize)]
struct ReservePromotionReport {
    plan_id: String,
    slot_id: String,
    retired_instance_id: String,
    promoted_instance_id: String,
    topology_epoch: u64,
    previous_binding_epoch: u64,
    committed_binding_epoch: u64,
    committed_voters: Vec<String>,
    healing_state: String,
    finished_at_unix: i64,
}

static STATUS: LazyLock<Arc<RwLock<ReserveSetStatus>>> =
    LazyLock::new(|| Arc::new(RwLock::new(ReserveSetStatus::default())));
static FAILURE_COUNTS: LazyLock<Mutex<HashMap<String, u32>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static PROMOTION_RUNNING: AtomicBool = AtomicBool::new(false);

struct PromotionGuard;

impl Drop for PromotionGuard {
    fn drop(&mut self) {
        PROMOTION_RUNNING.store(false, Ordering::Release);
    }
}

fn reserve_probe_node(reserve: &ReserveNode) -> ClusterNode {
    ClusterNode {
        id: reserve.instance_id.clone(),
        instance_id: reserve.instance_id.clone(),
        binding_epoch: 0,
        endpoint: reserve.endpoint.clone(),
        native_endpoint: reserve.native_endpoint.clone(),
        state: reserve.state.clone(),
        weight: 1,
        storage_roots: reserve.storage_roots.clone(),
        joined_epoch: 0,
        capacity_bytes: reserve.capacity_bytes,
        used_bytes: None,
        last_seen_unix: reserve.last_seen_unix,
    }
}

fn compatible_reserve(failed: &ClusterNode, reserve: &ReserveNode) -> Result<(), AppError> {
    if !reserve.effective_state().accepts_new_writes() {
        return Err(AppError::ServiceUnavailable(format!(
            "reserve instance '{}' is not write-capable",
            reserve.instance_id
        )));
    }
    if failed.storage_roots.len() != reserve.storage_roots.len() {
        return Err(AppError::Conflict(format!(
            "reserve instance '{}' has {} root(s), but logical slot '{}' requires {}",
            reserve.instance_id,
            reserve.storage_roots.len(),
            failed.id,
            failed.storage_roots.len()
        )));
    }
    if let (Some(required), Some(capacity)) = (failed.used_bytes, reserve.capacity_bytes)
        && capacity < required
    {
        return Err(AppError::Conflict(format!(
            "reserve instance '{}' capacity {} is below slot '{}' recorded usage {}",
            reserve.instance_id, capacity, failed.id, required
        )));
    }
    Ok(())
}

fn mapped_replacement_roots(
    failed: &ClusterNode,
    reserve: &ReserveNode,
) -> Result<Vec<StorageRoot>, AppError> {
    compatible_reserve(failed, reserve)?;
    let mut logical = failed.storage_roots.clone();
    logical.sort_by(|left, right| left.id.cmp(&right.id));
    let mut physical = reserve.storage_roots.clone();
    physical.sort_by(|left, right| {
        left.physical_id()
            .cmp(right.physical_id())
            .then_with(|| left.path.cmp(&right.path))
    });
    Ok(logical
        .into_iter()
        .zip(physical)
        .map(|(logical, physical)| StorageRoot {
            id: logical.id,
            physical_id: Some(physical.physical_id().to_string()),
            path: physical.path,
        })
        .collect())
}

pub fn plan_reserve_promotion(
    current: &ClusterMembership,
    slot_id: &str,
    reserve_instance_id: &str,
) -> Result<ClusterMembership, AppError> {
    current.validate()?;
    let failed_index = current
        .nodes
        .iter()
        .position(|node| node.id == slot_id)
        .ok_or_else(|| AppError::NotFound)?;
    let reserve_index = current
        .reserve_nodes
        .iter()
        .position(|node| node.instance_id == reserve_instance_id)
        .ok_or_else(|| {
            AppError::Conflict(format!(
                "physical instance '{}' is not in the reserve set",
                reserve_instance_id
            ))
        })?;
    let failed = current.nodes[failed_index].clone();
    let reserve = current.reserve_nodes[reserve_index].clone();
    let roots = mapped_replacement_roots(&failed, &reserve)?;
    let next_epoch = current
        .binding_epoch
        .checked_add(1)
        .ok_or_else(|| AppError::InternalServerError("N2 binding epoch exhausted".into()))?;

    let mut next = current.clone();
    next.binding_epoch = next_epoch;
    next.nodes[failed_index] = ClusterNode {
        id: failed.id.clone(),
        instance_id: reserve.instance_id.clone(),
        binding_epoch: next_epoch,
        endpoint: reserve.endpoint.clone(),
        native_endpoint: reserve.native_endpoint.clone(),
        state: Some(NodeState::Degraded),
        weight: failed.weight,
        storage_roots: roots,
        joined_epoch: failed.joined_epoch,
        capacity_bytes: reserve.capacity_bytes,
        used_bytes: Some(0),
        last_seen_unix: reserve.last_seen_unix,
    };
    next.reserve_nodes.remove(reserve_index);
    next.retired_instances.push(RetiredInstance {
        instance_id: failed.instance_id,
        former_slot_id: failed.id,
        fenced_at_binding_epoch: next_epoch,
        endpoint: failed.endpoint,
        native_endpoint: failed.native_endpoint,
        storage_roots: failed.storage_roots,
    });
    next.validate()?;
    Ok(next)
}

pub fn validate_reserve_transition(
    current: &ClusterMembership,
    next: &ClusterMembership,
) -> Result<(String, String), AppError> {
    current.validate()?;
    next.validate()?;
    if current.cluster_id != next.cluster_id
        || current.epoch != next.epoch
        || current.pools.len() != next.pools.len()
        || serde_json::to_value(&current.pools).ok() != serde_json::to_value(&next.pools).ok()
        || next.binding_epoch != current.binding_epoch.saturating_add(1)
        || next.nodes.len() != current.nodes.len()
        || next.reserve_nodes.len().saturating_add(1) != current.reserve_nodes.len()
        || next.retired_instances.len() != current.retired_instances.len().saturating_add(1)
    {
        return Err(AppError::Conflict(
            "reserve transition changed topology or did not advance exactly one binding".into(),
        ));
    }
    let changed = current
        .nodes
        .iter()
        .zip(&next.nodes)
        .filter(|(before, after)| {
            before.id != after.id
                || before.instance_id != after.instance_id
                || before.binding_epoch != after.binding_epoch
                || before.endpoint != after.endpoint
                || before.native_endpoint != after.native_endpoint
                || before.storage_roots != after.storage_roots
        })
        .collect::<Vec<_>>();
    if changed.len() != 1 {
        return Err(AppError::Conflict(format!(
            "reserve transition changed {} active slot bindings; exactly one is required",
            changed.len()
        )));
    }
    let (before, after) = changed[0];
    if before.id != after.id
        || after.binding_epoch != next.binding_epoch
        || !current
            .reserve_nodes
            .iter()
            .any(|reserve| reserve.instance_id == after.instance_id)
        || !next.retired_instances.iter().any(|retired| {
            retired.instance_id == before.instance_id
                && retired.former_slot_id == before.id
                && retired.fenced_at_binding_epoch == next.binding_epoch
        })
    {
        return Err(AppError::Conflict(
            "reserve transition did not preserve logical slot identity and fencing".into(),
        ));
    }
    Ok((before.id.clone(), after.instance_id.clone()))
}

fn activation_membership(
    current: &ClusterMembership,
    slot_id: &str,
) -> Result<ClusterMembership, AppError> {
    let mut next = current.clone();
    let node = next
        .nodes
        .iter_mut()
        .find(|node| node.id == slot_id)
        .ok_or(AppError::NotFound)?;
    if node.effective_state() != NodeState::Degraded {
        return Err(AppError::Conflict(format!(
            "logical slot '{slot_id}' is not in recovering state"
        )));
    }
    node.state = Some(NodeState::Online);
    next.validate()?;
    Ok(next)
}

fn validate_activation_transition(
    current: &ClusterMembership,
    next: &ClusterMembership,
) -> Result<String, AppError> {
    current.validate()?;
    next.validate()?;
    if current.cluster_id != next.cluster_id
        || current.epoch != next.epoch
        || current.binding_epoch != next.binding_epoch
        || serde_json::to_value(&current.pools).ok() != serde_json::to_value(&next.pools).ok()
        || serde_json::to_value(&current.reserve_nodes).ok()
            != serde_json::to_value(&next.reserve_nodes).ok()
        || serde_json::to_value(&current.retired_instances).ok()
            != serde_json::to_value(&next.retired_instances).ok()
        || current.nodes.len() != next.nodes.len()
    {
        return Err(AppError::Conflict(
            "replacement activation attempted to change topology or bindings".into(),
        ));
    }
    let changed = current
        .nodes
        .iter()
        .zip(&next.nodes)
        .filter(|(before, after)| {
            before.id != after.id
                || before.instance_id != after.instance_id
                || before.binding_epoch != after.binding_epoch
                || before.endpoint != after.endpoint
                || before.native_endpoint != after.native_endpoint
                || before.storage_roots != after.storage_roots
                || before.state != after.state
        })
        .collect::<Vec<_>>();
    if changed.len() != 1 {
        return Err(AppError::Conflict(
            "replacement activation must change exactly one slot state".into(),
        ));
    }
    let (before, after) = changed[0];
    if before.id != after.id
        || before.instance_id != after.instance_id
        || before.binding_epoch != after.binding_epoch
        || before.effective_state() != NodeState::Degraded
        || after.effective_state() != NodeState::Online
    {
        return Err(AppError::Conflict(
            "replacement activation is not a degraded-to-online transition".into(),
        ));
    }
    Ok(after.id.clone())
}

fn promotion_plan_id(
    membership: &ClusterMembership,
    slot_id: &str,
    reserve_instance_id: &str,
) -> Result<String, AppError> {
    let mut hasher = Sha256::new();
    hasher.update(b"n2-reserve-promotion-v1");
    hasher.update([0]);
    hasher.update(membership.cluster_id.as_bytes());
    hasher.update([0]);
    hasher.update(membership.epoch.to_be_bytes());
    hasher.update(membership.binding_epoch.to_be_bytes());
    hasher.update(slot_id.as_bytes());
    hasher.update([0]);
    hasher.update(reserve_instance_id.as_bytes());
    Ok(hex::encode(hasher.finalize()))
}

async fn failed_node_is_unreachable(runtime: &ClusterRuntime, failed: &ClusterNode) -> bool {
    !cluster_node_is_reachable(&runtime.local_instance_id, failed).await
}

async fn local_prepare_vote(
    request: &PeerReservePrepareRequest,
) -> Result<PeerReservePrepareResponse, AppError> {
    let runtime = active_cluster_runtime()?;
    if runtime.membership.cluster_id != request.cluster_id
        || runtime.membership.epoch != request.topology_epoch
        || runtime.membership.binding_epoch != request.binding_epoch
    {
        return Err(AppError::Conflict(
            "reserve prepare does not match local topology/binding epoch".into(),
        ));
    }
    let failed = runtime
        .membership
        .nodes
        .iter()
        .find(|node| node.id == request.slot_id)
        .ok_or(AppError::NotFound)?;
    if failed.instance_id != request.failed_instance_id {
        return Err(AppError::Conflict(
            "reserve prepare identifies a stale failed instance".into(),
        ));
    }
    let reserve = runtime
        .membership
        .reserve(&request.reserve_instance_id)
        .ok_or_else(|| AppError::Conflict("reserve candidate is no longer unassigned".into()))?;
    compatible_reserve(failed, reserve)?;
    let observed_failed_unreachable = failed_node_is_unreachable(&runtime, failed).await;
    if !observed_failed_unreachable {
        return Err(AppError::Conflict(format!(
            "logical slot '{}' is still reachable from physical instance '{}'",
            request.slot_id, runtime.local_instance_id
        )));
    }
    Ok(PeerReservePrepareResponse {
        voter_slot_id: runtime.local_node_id.clone(),
        voter_instance_id: runtime.local_instance_id.clone(),
        accepted: true,
        observed_failed_unreachable,
        plan_id: request.plan_id.clone(),
    })
}

async fn post_prepare(
    endpoint: &str,
    request: &PeerReservePrepareRequest,
) -> Result<PeerReservePrepareResponse, AppError> {
    let client = peer_http_client()?;
    let mut req = client
        .post(format!(
            "{}/n2/peer/reserve/prepare",
            endpoint.trim_end_matches('/')
        ))
        .json(request);
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(
        Duration::from_secs(CONFIG.reserve_set.promotion_timeout_seconds),
        req.send(),
    )
    .await
    .map_err(|_| AppError::ServiceUnavailable("reserve prepare timed out".into()))?
    .map_err(|error| {
        AppError::ServiceUnavailable(format!("reserve prepare request failed: {error}"))
    })?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "reserve prepare returned {status}: {body}"
        )));
    }
    response
        .json()
        .await
        .map_err(|error| AppError::InternalServerError(format!("decode reserve vote: {error}")))
}

async fn local_commit(
    request: &PeerReserveCommitRequest,
) -> Result<PeerReserveCommitResponse, AppError> {
    let current = active_cluster_runtime()?;
    if current.membership.binding_epoch == request.membership.binding_epoch {
        let current_bytes = serde_json::to_vec(&current.membership).ok();
        let requested_bytes = serde_json::to_vec(&request.membership).ok();
        if current_bytes == requested_bytes {
            return Ok(PeerReserveCommitResponse {
                local_instance_id: current.local_instance_id.clone(),
                binding_epoch: current.membership.binding_epoch,
                committed: true,
                plan_id: request.plan_id.clone(),
            });
        }
    }
    if current.membership.binding_epoch != request.previous_binding_epoch {
        return Err(AppError::Conflict(format!(
            "local binding epoch {} cannot commit transition from {}",
            current.membership.binding_epoch, request.previous_binding_epoch
        )));
    }
    validate_reserve_transition(&current.membership, &request.membership)?;
    let local_instance_id = current.local_instance_id.clone();
    let next = commit_local_membership(request.membership.clone())?;
    Ok(PeerReserveCommitResponse {
        local_instance_id,
        binding_epoch: next.membership.binding_epoch,
        committed: true,
        plan_id: request.plan_id.clone(),
    })
}

async fn post_commit(
    endpoint: &str,
    request: &PeerReserveCommitRequest,
) -> Result<PeerReserveCommitResponse, AppError> {
    let client = peer_http_client()?;
    let mut req = client
        .post(format!(
            "{}/n2/peer/reserve/commit",
            endpoint.trim_end_matches('/')
        ))
        .json(request);
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(
        Duration::from_secs(CONFIG.reserve_set.promotion_timeout_seconds),
        req.send(),
    )
    .await
    .map_err(|_| AppError::ServiceUnavailable("reserve commit timed out".into()))?
    .map_err(|error| {
        AppError::ServiceUnavailable(format!("reserve commit request failed: {error}"))
    })?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "reserve commit returned {status}: {body}"
        )));
    }
    response
        .json()
        .await
        .map_err(|error| AppError::InternalServerError(format!("decode reserve commit: {error}")))
}

async fn local_activate(
    request: &PeerReserveActivateRequest,
) -> Result<PeerReserveCommitResponse, AppError> {
    let current = active_cluster_runtime()?;
    if serde_json::to_vec(&current.membership).ok() == serde_json::to_vec(&request.membership).ok()
    {
        return Ok(PeerReserveCommitResponse {
            local_instance_id: current.local_instance_id.clone(),
            binding_epoch: current.membership.binding_epoch,
            committed: true,
            plan_id: request.plan_id.clone(),
        });
    }
    validate_activation_transition(&current.membership, &request.membership)?;
    let local_instance_id = current.local_instance_id.clone();
    let runtime = commit_local_membership(request.membership.clone())?;
    Ok(PeerReserveCommitResponse {
        local_instance_id,
        binding_epoch: runtime.membership.binding_epoch,
        committed: true,
        plan_id: request.plan_id.clone(),
    })
}

async fn post_activate(
    endpoint: &str,
    request: &PeerReserveActivateRequest,
) -> Result<PeerReserveCommitResponse, AppError> {
    let client = peer_http_client()?;
    let mut req = client
        .post(format!(
            "{}/n2/peer/reserve/activate",
            endpoint.trim_end_matches('/')
        ))
        .json(request);
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(
        Duration::from_secs(CONFIG.reserve_set.promotion_timeout_seconds),
        req.send(),
    )
    .await
    .map_err(|_| AppError::ServiceUnavailable("reserve activation timed out".into()))?
    .map_err(|error| {
        AppError::ServiceUnavailable(format!("reserve activation request failed: {error}"))
    })?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "reserve activation returned {status}: {body}"
        )));
    }
    response.json().await.map_err(|error| {
        AppError::InternalServerError(format!("decode reserve activation: {error}"))
    })
}

async fn post_replacement_healing(endpoint: &str) -> Result<AutoHealingStatus, AppError> {
    let client = peer_http_client()?;
    let mut req = client.post(format!(
        "{}/n2/peer/healing/replacement-cycle",
        endpoint.trim_end_matches('/')
    ));
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(
        Duration::from_secs(CONFIG.auto_healing.max_duration_seconds.saturating_add(30)),
        req.send(),
    )
    .await
    .map_err(|_| AppError::ServiceUnavailable("replacement healing timed out".into()))?
    .map_err(|error| {
        AppError::ServiceUnavailable(format!("replacement healing request failed: {error}"))
    })?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "replacement healing returned {status}: {body}"
        )));
    }
    response.json().await.map_err(|error| {
        AppError::InternalServerError(format!("decode replacement healing status: {error}"))
    })
}

async fn post_fragment_sync(endpoint: &str) -> Result<(), AppError> {
    let client = peer_http_client()?;
    let mut req = client.post(format!(
        "{}/n2/peer/fragments/sync",
        endpoint.trim_end_matches('/')
    ));
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(Duration::from_secs(30), req.send())
        .await
        .map_err(|_| AppError::ServiceUnavailable("fragment synchronization timed out".into()))?
        .map_err(|error| {
            AppError::ServiceUnavailable(format!(
                "fragment synchronization request failed: {error}"
            ))
        })?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "fragment synchronization returned {status}: {body}"
        )));
    }
    Ok(())
}

async fn run_replacement_recovery(plan_id: String, slot_id: String) -> Result<(), AppError> {
    // Promotion changes only a physical binding. Make every replacement
    // participant observe the existing immutable fragment controls before any
    // recorded-layout healing is allowed to declare completion.
    let runtime = active_cluster_runtime()?;
    let sync_results = join_all(runtime.membership.nodes.iter().map(|node| {
        let node = node.clone();
        let runtime = runtime.clone();
        async move {
            if node.instance_id == runtime.local_instance_id {
                sync_fragment_controls_from_cluster().await.map(|_| ())
            } else {
                post_fragment_sync(&node.endpoint).await
            }
        }
    }))
    .await;
    for result in sync_results {
        result?;
    }

    // Each bounded cycle resumes from persisted per-fragment continuation.
    // This loop yields between rounds and has no healthy-path request cost.
    for _round in 0..1_024u32 {
        let runtime = active_cluster_runtime()?;
        let nodes = runtime.membership.nodes.clone();
        let results = join_all(nodes.iter().map(|node| {
            let node = node.clone();
            let runtime = runtime.clone();
            async move {
                if node.instance_id == runtime.local_instance_id {
                    run_replacement_healing_cycle().await
                } else {
                    post_replacement_healing(&node.endpoint).await
                }
            }
        }))
        .await;
        let mut incomplete = 0u64;
        let mut busy = false;
        for result in results {
            let status = match result {
                Ok(status) => status,
                Err(AppError::Conflict(message))
                    if message.contains("auto-healing cycle is already running") =>
                {
                    busy = true;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if status.safety_read_only
                || status.unrecoverable_objects > 0
                || status.failed_objects > 0
                || status.last_error.is_some()
            {
                return Err(AppError::ServiceUnavailable(format!(
                    "replacement healing for slot '{}' did not prove a safe complete layout",
                    slot_id
                )));
            }
            incomplete = incomplete.saturating_add(status.incomplete_fragments);
        }
        if !busy && incomplete == 0 {
            let runtime = active_cluster_runtime()?;
            let activated = activation_membership(&runtime.membership, &slot_id)?;
            let request = PeerReserveActivateRequest {
                plan_id: plan_id.clone(),
                membership: activated.clone(),
            };
            // Remote participants first, local publication last.
            for node in &runtime.membership.nodes {
                if node.instance_id != runtime.local_instance_id {
                    post_activate(&node.endpoint, &request).await?;
                }
            }
            local_activate(&request).await?;
            return Ok(());
        }
        sleep(Duration::from_millis(if busy { 250 } else { 100 })).await;
    }
    Err(AppError::ServiceUnavailable(format!(
        "replacement healing for slot '{}' exceeded 1024 bounded resume cycles",
        slot_id
    )))
}

async fn fetch_peer_membership(endpoint: &str) -> Result<PeerReserveMembershipResponse, AppError> {
    let client = peer_http_client()?;
    let mut req = client.get(format!(
        "{}/n2/peer/reserve/membership",
        endpoint.trim_end_matches('/')
    ));
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(Duration::from_secs(5), req.send())
        .await
        .map_err(|_| AppError::ServiceUnavailable("reserve membership query timed out".into()))?
        .map_err(|error| {
            AppError::ServiceUnavailable(format!("reserve membership query failed: {error}"))
        })?;
    if !response.status().is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "reserve membership query returned {}",
            response.status()
        )));
    }
    response.json().await.map_err(|error| {
        AppError::InternalServerError(format!("decode reserve membership: {error}"))
    })
}

async fn reconcile_binding_epoch() -> Result<(), AppError> {
    let runtime = active_cluster_runtime()?;
    let mut endpoints = runtime
        .membership
        .nodes
        .iter()
        .filter(|node| node.instance_id != runtime.local_instance_id)
        .map(|node| node.endpoint.clone())
        .chain(
            runtime
                .membership
                .reserve_nodes
                .iter()
                .filter(|node| node.instance_id != runtime.local_instance_id)
                .map(|node| node.endpoint.clone()),
        )
        .collect::<Vec<_>>();
    endpoints.sort();
    endpoints.dedup();
    let observations = join_all(endpoints.iter().map(|endpoint| {
        let endpoint = endpoint.clone();
        async move { (endpoint.clone(), fetch_peer_membership(&endpoint).await) }
    }))
    .await;

    let mut newest = runtime.membership.clone();
    for (_, observation) in &observations {
        let Ok(observation) = observation else {
            continue;
        };
        observation.membership.validate()?;
        if observation.membership.cluster_id != runtime.membership.cluster_id
            || observation.membership.epoch != runtime.membership.epoch
        {
            continue;
        }
        if observation.membership.binding_epoch > newest.binding_epoch {
            newest = observation.membership.clone();
        }
    }
    if newest.binding_epoch == runtime.membership.binding_epoch.saturating_add(1) {
        validate_reserve_transition(&runtime.membership, &newest)?;
        commit_local_membership(newest)?;
        return Ok(());
    }
    if newest.binding_epoch > runtime.membership.binding_epoch.saturating_add(1) {
        return Err(AppError::Conflict(format!(
            "local binding epoch {} is more than one transition behind observed epoch {}; operator replay is required",
            runtime.membership.binding_epoch, newest.binding_epoch
        )));
    }

    // A node that already committed the newest epoch repairs any reachable
    // one-step survivor left behind by a coordinator crash.
    for (endpoint, observation) in observations {
        let Ok(observation) = observation else {
            continue;
        };
        if observation.membership.cluster_id != runtime.membership.cluster_id
            || observation.membership.epoch != runtime.membership.epoch
            || observation.membership.binding_epoch.saturating_add(1)
                != runtime.membership.binding_epoch
        {
            continue;
        }
        validate_reserve_transition(&observation.membership, &runtime.membership)?;
        let request = PeerReserveCommitRequest {
            previous_binding_epoch: observation.membership.binding_epoch,
            plan_id: format!(
                "reconcile-{}-{}",
                runtime.membership.binding_epoch, observation.local_instance_id
            ),
            membership: runtime.membership.clone(),
        };
        let _ = post_commit(&endpoint, &request).await?;
    }
    Ok(())
}

async fn write_report(report: &ReservePromotionReport) -> Result<(), AppError> {
    let runtime = active_cluster_runtime()?;
    let root = runtime
        .membership_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(".reports")
        .join("reserve-set");
    fs::create_dir_all(&root)
        .await
        .map_err(|error| AppError::Io(error, format!("create {}", root.display())))?;
    let path = root.join(format!("{}.json", report.plan_id));
    let temp = root.join(format!("{}.tmp", report.plan_id));
    let bytes = serde_json::to_vec_pretty(report).map_err(|error| {
        AppError::InternalServerError(format!("encode reserve promotion report: {error}"))
    })?;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp)
        .await
        .map_err(|error| AppError::Io(error, format!("write {}", temp.display())))?;
    file.write_all(&bytes)
        .await
        .map_err(|error| AppError::Io(error, format!("write {}", temp.display())))?;
    file.sync_all()
        .await
        .map_err(|error| AppError::Io(error, format!("sync {}", temp.display())))?;
    drop(file);
    fs::rename(&temp, &path)
        .await
        .map_err(|error| AppError::Io(error, format!("publish {}", path.display())))
}

async fn execute_promotion(
    slot_id: &str,
    reserve_instance_id: &str,
) -> Result<ReserveSetStatus, AppError> {
    if PROMOTION_RUNNING
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err(AppError::Conflict(
            "another reserve promotion is already running".into(),
        ));
    }
    let _guard = PromotionGuard;
    let runtime = active_cluster_runtime()?;
    if runtime.local_is_reserve {
        return Err(AppError::Forbidden(
            "an unassigned reserve cannot coordinate promotion".into(),
        ));
    }
    let pool = runtime
        .membership
        .pool_for_node(slot_id)
        .ok_or(AppError::NotFound)?;
    let failed = runtime
        .membership
        .nodes
        .iter()
        .find(|node| node.id == slot_id)
        .cloned()
        .ok_or(AppError::NotFound)?;
    let reserve = runtime
        .membership
        .reserve(reserve_instance_id)
        .cloned()
        .ok_or_else(|| AppError::Conflict("reserve candidate is not unassigned".into()))?;
    compatible_reserve(&failed, &reserve)?;
    if !failed_node_is_unreachable(&runtime, &failed).await {
        return Err(AppError::Conflict(format!(
            "logical slot '{slot_id}' is reachable; promotion refused"
        )));
    }

    let surviving = runtime
        .membership
        .nodes_for_pool(&pool.id)
        .into_iter()
        .filter(|node| node.id != slot_id)
        .collect::<Vec<_>>();
    let live = join_all(
        surviving
            .iter()
            .map(|node| cluster_node_is_reachable(&runtime.local_instance_id, node)),
    )
    .await;
    if live.iter().any(|reachable| !reachable) {
        return Err(AppError::ServiceUnavailable(format!(
            "pool '{}' has more than one unreachable active slot; autonomous fencing is unsafe",
            pool.id
        )));
    }
    if !cluster_node_is_reachable(&runtime.local_instance_id, &reserve_probe_node(&reserve)).await {
        return Err(AppError::ServiceUnavailable(format!(
            "reserve instance '{}' is unreachable",
            reserve.instance_id
        )));
    }
    let mut coordinators = surviving
        .iter()
        .map(|node| node.id.as_str())
        .collect::<Vec<_>>();
    coordinators.sort_unstable();
    if coordinators.first().copied() != Some(runtime.local_node_id.as_str()) {
        return Err(AppError::Conflict(format!(
            "logical slot '{}' is not deterministic reserve coordinator '{}'",
            runtime.local_node_id,
            coordinators.first().copied().unwrap_or("")
        )));
    }

    let plan_id = promotion_plan_id(&runtime.membership, slot_id, reserve_instance_id)?;
    {
        let mut status = STATUS.write().await;
        status.running = true;
        status.last_started_at_unix = Some(Utc::now().timestamp());
        status.last_plan_id = Some(plan_id.clone());
        status.last_error = None;
    }
    let prepare = PeerReservePrepareRequest {
        cluster_id: runtime.membership.cluster_id.clone(),
        topology_epoch: runtime.membership.epoch,
        binding_epoch: runtime.membership.binding_epoch,
        slot_id: slot_id.to_string(),
        failed_instance_id: failed.instance_id.clone(),
        reserve_instance_id: reserve_instance_id.to_string(),
        plan_id: plan_id.clone(),
    };

    let mut votes = Vec::with_capacity(surviving.len() + 1);
    for voter in &surviving {
        let vote = if voter.instance_id == runtime.local_instance_id {
            local_prepare_vote(&prepare).await?
        } else {
            post_prepare(&voter.endpoint, &prepare).await?
        };
        if !vote.accepted || !vote.observed_failed_unreachable || vote.plan_id != plan_id {
            return Err(AppError::Conflict(format!(
                "reserve fencing vote from '{}' was not affirmative",
                voter.id
            )));
        }
        votes.push(vote.voter_instance_id);
    }
    let reserve_vote = post_prepare(&reserve.endpoint, &prepare).await?;
    if !reserve_vote.accepted
        || !reserve_vote.observed_failed_unreachable
        || reserve_vote.plan_id != plan_id
    {
        return Err(AppError::Conflict(
            "reserve candidate did not affirm the fencing observation".into(),
        ));
    }
    votes.push(reserve_vote.voter_instance_id);

    let next = plan_reserve_promotion(&runtime.membership, slot_id, reserve_instance_id)?;
    let commit = PeerReserveCommitRequest {
        previous_binding_epoch: runtime.membership.binding_epoch,
        plan_id: plan_id.clone(),
        membership: next.clone(),
    };

    // Commit the replacement first so the new logical participant is ready,
    // then commit each surviving exact-pool participant. All commits are
    // idempotent and carry the same complete membership snapshot.
    let reserve_commit = post_commit(&reserve.endpoint, &commit).await?;
    if !reserve_commit.committed {
        return Err(AppError::ServiceUnavailable(
            "reserve instance did not commit its new binding".into(),
        ));
    }
    let mut committed = vec![reserve_commit.local_instance_id];
    for voter in &surviving {
        if voter.instance_id == runtime.local_instance_id {
            continue;
        }
        let outcome = post_commit(&voter.endpoint, &commit).await?;
        if !outcome.committed || outcome.binding_epoch != next.binding_epoch {
            return Err(AppError::ServiceUnavailable(format!(
                "surviving instance '{}' did not commit binding epoch {}",
                voter.instance_id, next.binding_epoch
            )));
        }
        committed.push(outcome.local_instance_id);
    }
    for voter in &surviving {
        if voter.instance_id != runtime.local_instance_id {
            continue;
        }
        let outcome = local_commit(&commit).await?;
        if !outcome.committed || outcome.binding_epoch != next.binding_epoch {
            return Err(AppError::ServiceUnavailable(format!(
                "surviving instance '{}' did not commit binding epoch {}",
                voter.instance_id, next.binding_epoch
            )));
        }
        committed.push(outcome.local_instance_id);
    }

    let report = ReservePromotionReport {
        plan_id: plan_id.clone(),
        slot_id: slot_id.to_string(),
        retired_instance_id: failed.instance_id,
        promoted_instance_id: reserve_instance_id.to_string(),
        topology_epoch: next.epoch,
        previous_binding_epoch: runtime.membership.binding_epoch,
        committed_binding_epoch: next.binding_epoch,
        committed_voters: committed,
        healing_state: "recovering_via_autonomous_ncnx_ncdx_and_shard_healing".into(),
        finished_at_unix: Utc::now().timestamp(),
    };
    write_report(&report).await?;
    let mut status = status_snapshot().await;
    status.running = true;
    status.binding_epoch = next.binding_epoch;
    status.last_promoted_slot = Some(slot_id.to_string());
    status.last_promoted_instance = Some(reserve_instance_id.to_string());
    status.last_finished_at_unix = Some(report.finished_at_unix);
    status.last_error = None;
    *STATUS.write().await = status.clone();
    let recovery_plan = plan_id.clone();
    let recovery_slot = slot_id.to_string();
    tokio::spawn(async move {
        // execute_promotion still owns the guard until it returns. Acquire it
        // immediately afterward so no periodic worker can duplicate recovery.
        loop {
            if PROMOTION_RUNNING
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        let _guard = PromotionGuard;
        let result = run_replacement_recovery(recovery_plan, recovery_slot).await;
        let mut status = STATUS.write().await;
        status.running = false;
        status.last_finished_at_unix = Some(Utc::now().timestamp());
        if let Err(error) = result {
            status.last_error = Some(error.to_string());
        }
    });
    Ok(status)
}

async fn status_snapshot() -> ReserveSetStatus {
    let runtime = active_cluster_runtime().ok();
    let previous = STATUS.read().await.clone();
    let mut status = previous;
    status.enabled = CONFIG.reserve_set.enabled;
    status.automatic_promotion = CONFIG.reserve_set.automatic_promotion;
    if let Some(runtime) = runtime {
        status.topology_epoch = runtime.membership.epoch;
        status.binding_epoch = runtime.membership.binding_epoch;
        status.active_slots = runtime.membership.nodes.len();
        status.reserve_instances = runtime.membership.reserve_nodes.len();
        status.recovering_slots = runtime
            .membership
            .nodes
            .iter()
            .filter(|node| node.effective_state() == NodeState::Degraded)
            .map(|node| node.id.clone())
            .collect();
    }
    status
}

async fn automatic_cycle() -> Result<(), AppError> {
    reconcile_binding_epoch().await?;
    let runtime = active_cluster_runtime()?;
    if runtime.local_is_reserve {
        return Ok(());
    }

    // A committed degraded binding is durable recovery state. If any process
    // restarted after commit but before activation, the deterministic lowest
    // logical slot resumes the bounded, persisted healing pass.
    if let Some(recovering) = runtime
        .membership
        .nodes
        .iter()
        .find(|node| node.effective_state() == NodeState::Degraded)
    {
        let mut coordinators = runtime
            .membership
            .nodes
            .iter()
            .filter(|node| node.id != recovering.id)
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>();
        coordinators.sort_unstable();
        if coordinators.first().copied() == Some(runtime.local_node_id.as_str())
            && PROMOTION_RUNNING
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            let _guard = PromotionGuard;
            {
                let mut status = STATUS.write().await;
                status.running = true;
                status.last_started_at_unix = Some(Utc::now().timestamp());
                status.last_error = None;
            }
            let result = run_replacement_recovery(
                format!(
                    "resume-binding-{}-{}",
                    runtime.membership.binding_epoch, recovering.id
                ),
                recovering.id.clone(),
            )
            .await;
            let mut status = STATUS.write().await;
            status.running = false;
            status.last_finished_at_unix = Some(Utc::now().timestamp());
            if let Err(error) = result {
                status.last_error = Some(error.to_string());
                return Ok(());
            }
        }
        return Ok(());
    }
    if runtime.membership.reserve_nodes.is_empty() {
        return Ok(());
    }
    let mut unavailable = Vec::new();
    for pool in &runtime.membership.pools {
        for node in runtime.membership.nodes_for_pool(&pool.id) {
            if !cluster_node_is_reachable(&runtime.local_instance_id, &node).await {
                unavailable.push(node);
            }
        }
    }
    let mut counts = FAILURE_COUNTS.lock().await;
    let unavailable_ids = unavailable
        .iter()
        .map(|node| node.id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    counts.retain(|slot, _| unavailable_ids.contains(slot));
    for node in &unavailable {
        let entry = counts.entry(node.id.clone()).or_default();
        *entry = entry.saturating_add(1);
    }
    let suspects = counts
        .iter()
        .filter(|(_, count)| **count >= CONFIG.reserve_set.failure_threshold)
        .map(|(slot, _)| slot.clone())
        .collect::<Vec<_>>();
    drop(counts);
    STATUS.write().await.suspected_slots = suspects.clone();

    for slot_id in suspects {
        let runtime = active_cluster_runtime()?;
        let Some(pool) = runtime.membership.pool_for_node(&slot_id) else {
            continue;
        };
        let mut survivors = runtime
            .membership
            .nodes_for_pool(&pool.id)
            .into_iter()
            .filter(|node| node.id != slot_id)
            .collect::<Vec<_>>();
        survivors.sort_by(|left, right| left.id.cmp(&right.id));
        if survivors.first().map(|node| node.id.as_str()) != Some(runtime.local_node_id.as_str()) {
            continue;
        }
        let Some(reserve) = runtime
            .membership
            .reserve_nodes
            .iter()
            .filter(|reserve| {
                runtime
                    .membership
                    .nodes
                    .iter()
                    .find(|node| node.id == slot_id)
                    .is_some_and(|failed| compatible_reserve(failed, reserve).is_ok())
            })
            .min_by(|left, right| left.instance_id.cmp(&right.instance_id))
        else {
            continue;
        };
        execute_promotion(&slot_id, &reserve.instance_id).await?;
        FAILURE_COUNTS.lock().await.remove(&slot_id);
        break;
    }
    Ok(())
}

pub fn spawn_reserve_set_task() {
    if !CONFIG.reserve_set.enabled {
        return;
    }
    let cfg = CONFIG.reserve_set.clone();
    tokio::spawn(async move {
        sleep(Duration::from_secs(cfg.initial_delay_seconds)).await;
        loop {
            if cfg.automatic_promotion
                && let Err(error) = automatic_cycle().await
            {
                let mut status = STATUS.write().await;
                status.running = false;
                status.last_error = Some(error.to_string());
            }
            sleep(Duration::from_secs(cfg.probe_interval_seconds)).await;
        }
    });
}

pub async fn reserve_status(claims: Claims) -> Result<Json<ReserveSetStatus>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    Ok(Json(status_snapshot().await))
}

pub async fn promote_reserve(
    claims: Claims,
    Json(request): Json<ReservePromotionRequest>,
) -> Result<Json<ReserveSetStatus>, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden("Modify permission required".into()));
    }
    Ok(Json(
        execute_promotion(&request.slot_id, &request.reserve_instance_id).await?,
    ))
}

async fn peer_prepare(
    headers: HeaderMap,
    Json(request): Json<PeerReservePrepareRequest>,
) -> Result<Json<PeerReservePrepareResponse>, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(local_prepare_vote(&request).await?))
}

async fn peer_commit(
    headers: HeaderMap,
    Json(request): Json<PeerReserveCommitRequest>,
) -> Result<Json<PeerReserveCommitResponse>, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(local_commit(&request).await?))
}

async fn peer_activate(
    headers: HeaderMap,
    Json(request): Json<PeerReserveActivateRequest>,
) -> Result<Json<PeerReserveCommitResponse>, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(local_activate(&request).await?))
}

async fn peer_membership(
    headers: HeaderMap,
) -> Result<Json<PeerReserveMembershipResponse>, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    Ok(Json(PeerReserveMembershipResponse {
        local_instance_id: runtime.local_instance_id.clone(),
        membership: runtime.membership.clone(),
    }))
}

pub fn build_reserve_peer_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/n2/peer/reserve/membership", get(peer_membership))
        .route("/n2/peer/reserve/prepare", post(peer_prepare))
        .route("/n2/peer/reserve/commit", post(peer_commit))
        .route("/n2/peer/reserve/activate", post(peer_activate))
}

pub fn reserve_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/noa/v1/cluster/reserve", get(reserve_status))
        .route("/noa/v1/cluster/reserve/promote", post(promote_reserve))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::layout::ClusterEcLayout;
    use crate::distribution::membership::{
        ClusterPolicy, N2_MEMBERSHIP_FORMAT_VERSION, PoolState, ServerPool,
    };
    use crate::distribution::placement::build_placement_plan;

    fn root(id: &str, path: &str) -> StorageRoot {
        StorageRoot {
            id: id.into(),
            physical_id: None,
            path: path.into(),
        }
    }

    fn membership() -> ClusterMembership {
        ClusterMembership {
            format_version: N2_MEMBERSHIP_FORMAT_VERSION,
            cluster_id: "cluster".into(),
            epoch: 7,
            binding_epoch: 3,
            pools: vec![ServerPool {
                id: "pool-a".into(),
                generation: 7,
                policy: ClusterPolicy {
                    object_data_shards: 2,
                    object_parity_shards: 1,
                },
                state: PoolState::Writable,
                node_ids: vec!["a".into(), "b".into(), "c".into()],
            }],
            nodes: ["a", "b", "c"]
                .into_iter()
                .map(|id| ClusterNode {
                    id: id.into(),
                    instance_id: format!("instance-{id}"),
                    binding_epoch: 1,
                    endpoint: format!("https://{id}"),
                    native_endpoint: Some(format!("n2://{id}:50002")),
                    state: Some(NodeState::Online),
                    weight: 1,
                    storage_roots: vec![root(&format!("logical-{id}"), &format!("/disk/{id}"))],
                    joined_epoch: 1,
                    capacity_bytes: Some(1_000),
                    used_bytes: Some(100),
                    last_seen_unix: None,
                })
                .collect(),
            reserve_nodes: vec![ReserveNode {
                instance_id: "reserve-1".into(),
                endpoint: "https://reserve-1".into(),
                native_endpoint: Some("n2://reserve-1:50002".into()),
                state: Some(NodeState::Online),
                storage_roots: vec![root("physical-reserve", "/reserve/disk")],
                capacity_bytes: Some(1_000),
                last_seen_unix: None,
            }],
            retired_instances: Vec::new(),
        }
    }

    #[test]
    fn promotion_preserves_logical_slot_and_topology() {
        let before = membership();
        let after = plan_reserve_promotion(&before, "b", "reserve-1").unwrap();
        assert_eq!(after.epoch, before.epoch);
        assert_eq!(after.binding_epoch, before.binding_epoch + 1);
        assert_eq!(after.pools, before.pools);
        let binding = after.nodes.iter().find(|node| node.id == "b").unwrap();
        assert_eq!(binding.instance_id, "reserve-1");
        assert_eq!(binding.storage_roots[0].id, "logical-b");
        assert_eq!(
            binding.storage_roots[0].physical_id.as_deref(),
            Some("physical-reserve")
        );
        assert_eq!(binding.storage_roots[0].path, "/reserve/disk");
        assert!(after.reserve_nodes.is_empty());
        assert_eq!(after.retired_instances[0].instance_id, "instance-b");
        validate_reserve_transition(&before, &after).unwrap();
    }

    #[test]
    fn promotion_preserves_every_recorded_placement_coordinate() {
        let before = membership();
        let after = plan_reserve_promotion(&before, "b", "reserve-1").unwrap();
        let layout = ClusterEcLayout {
            data_shards: 2,
            parity_shards: 1,
        };
        let before_plan = build_placement_plan(
            "fragment",
            "videos/main.bin",
            "version",
            "pool-a",
            7,
            before.epoch,
            before.binding_epoch,
            layout,
            &before.active_write_nodes_for_pool("pool-a"),
        )
        .unwrap();
        let after_plan = build_placement_plan(
            "fragment",
            "videos/main.bin",
            "version",
            "pool-a",
            7,
            after.epoch,
            after.binding_epoch,
            layout,
            &after.active_write_nodes_for_pool("pool-a"),
        )
        .unwrap();
        let logical_before = before_plan
            .placements
            .iter()
            .map(|placement| {
                (
                    placement.shard_index,
                    placement.role.clone(),
                    placement.node_id.clone(),
                    placement.root_id.clone(),
                )
            })
            .collect::<Vec<_>>();
        let logical_after = after_plan
            .placements
            .iter()
            .map(|placement| {
                (
                    placement.shard_index,
                    placement.role.clone(),
                    placement.node_id.clone(),
                    placement.root_id.clone(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(logical_after, logical_before);
        let replacement = after_plan
            .placements
            .iter()
            .find(|placement| placement.node_id == "b")
            .unwrap();
        assert_eq!(replacement.instance_id, "reserve-1");
        assert_eq!(replacement.binding_epoch, after.binding_epoch);
    }

    #[test]
    fn reserve_capacity_below_recorded_slot_usage_is_rejected() {
        let mut before = membership();
        before.reserve_nodes[0].capacity_bytes = Some(99);
        let error = plan_reserve_promotion(&before, "b", "reserve-1").unwrap_err();
        assert!(error.to_string().contains("below slot 'b' recorded usage"));
    }

    #[test]
    fn activation_changes_only_recovery_state() {
        let promoted = plan_reserve_promotion(&membership(), "b", "reserve-1").unwrap();
        let activated = activation_membership(&promoted, "b").unwrap();
        assert_eq!(
            validate_activation_transition(&promoted, &activated).unwrap(),
            "b"
        );
        assert_eq!(promoted.epoch, activated.epoch);
        assert_eq!(promoted.binding_epoch, activated.binding_epoch);
        assert_eq!(promoted.pools, activated.pools);
        assert_eq!(
            serde_json::to_value(&promoted.retired_instances).unwrap(),
            serde_json::to_value(&activated.retired_instances).unwrap()
        );
        let mut invalid = activated.clone();
        invalid.nodes[0].endpoint.push_str("/changed");
        assert!(validate_activation_transition(&promoted, &invalid).is_err());
    }

    #[test]
    fn membership_v5_serializes_explicit_slot_bindings() {
        let value = serde_json::to_value(membership()).unwrap();
        assert_eq!(value["format_version"], 5);
        assert!(value.get("slot_bindings").is_some());
        assert!(value.get("nodes").is_none());
        assert!(value["slot_bindings"][0].get("slot_id").is_some());
        assert!(value["slot_bindings"][0].get("id").is_none());
        assert!(value["pools"][0].get("slot_ids").is_some());
        assert!(value["pools"][0].get("node_ids").is_none());
    }

    #[test]
    fn reserve_is_never_added_to_pool_or_placement_width() {
        let before = membership();
        assert_eq!(before.nodes_for_pool("pool-a").len(), 3);
        assert_eq!(before.active_write_nodes().len(), 3);
        assert_eq!(before.reserve_nodes.len(), 1);
        assert!(before.pool_for_node("reserve-1").is_none());
    }

    #[test]
    fn incompatible_root_count_is_rejected() {
        let mut before = membership();
        before.reserve_nodes[0]
            .storage_roots
            .push(root("physical-reserve-2", "/reserve/disk-2"));
        assert!(plan_reserve_promotion(&before, "b", "reserve-1").is_err());
    }
}
