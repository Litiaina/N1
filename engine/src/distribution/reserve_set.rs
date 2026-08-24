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

mod execution;
mod planning;
mod protocol;
mod routes;

use execution::*;
pub use planning::*;
use protocol::*;
pub use routes::*;

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

#[cfg(test)]
#[path = "reserve_set/tests.rs"]
mod tests;
