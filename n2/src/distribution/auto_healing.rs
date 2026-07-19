use crate::core::disk_manager::FS_STORAGE_ROOT;
use crate::core::operation_gate::{
    SafetyReadOnlyState, apply_safety_read_only_state, cluster_safety_read_only_state,
    is_cluster_safety_read_only,
};
use crate::core::repair_manager::RepairCheckMode;
use crate::distribution::healing_report::{AutoHealingReport, write_local_report};
use crate::distribution::maintenance::discover_local_fragments;
use crate::distribution::membership::{
    ClusterNode, ClusterRuntime, active_cluster_runtime, live_active_write_nodes,
};
use crate::distribution::namespace_healing::scan_and_repair_fragment_namespace;
use crate::distribution::ncnx::build_ncnx_layout;
use crate::distribution::object_healing::scan_and_repair_fragment_objects;
use crate::distribution::peer::{authorize_peer, cluster_secret, peer_http_client};
use crate::enums::app_error_enum::AppError;
use crate::routes::storage_routes::AppState;
use crate::settings::config::CONFIG;
use crate::structs::auth_structs::Claims;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::post;
use axum::{Json, Router};
use chrono::Utc;
use futures_util::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex as StdMutex};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{Duration, sleep, timeout};

const HEALING_LEASE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct FragmentProgress {
    continuation_token: Option<String>,
    completed_cycles: u64,
    last_finished_at_unix: Option<i64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct HealingProgressStore {
    cycle: u64,
    fragments: HashMap<String, FragmentProgress>,
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(default)]
pub struct AutoHealingStatus {
    pub enabled: bool,
    pub running: bool,
    pub cycle: u64,
    pub check_mode: String,
    pub membership_epoch: u64,
    pub configured_nodes: usize,
    pub live_nodes: usize,
    pub healer_pool: Vec<String>,
    pub discovered_fragments: usize,
    pub owned_fragments: usize,
    pub leased_fragments: usize,
    pub scanned_objects: u64,
    pub scanned_namespace_records: u64,
    pub repaired_namespace_records: u64,
    pub repaired_objects: u64,
    pub repaired_shards: u64,
    pub unrecoverable_objects: u64,
    pub failed_objects: u64,
    pub headless_objects: u64,
    pub reports_written: u64,
    pub safety_read_only: bool,
    pub started_at_unix: Option<i64>,
    pub finished_at_unix: Option<i64>,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HealingLeaseRecord {
    fragment: String,
    membership_epoch: u64,
    owner_node_id: String,
    lease_id: String,
    expires_at_unix: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerHealingLeaseRequest {
    fragment: String,
    membership_epoch: u64,
    owner_node_id: String,
    lease_id: String,
    lease_seconds: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerHealingLeaseResponse {
    voter_node_id: String,
    granted: bool,
    expires_at_unix: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerSafetyReadOnlyRequest {
    pub state: SafetyReadOnlyState,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerSafetyReadOnlyResponse {
    pub node_id: String,
    pub applied: bool,
}

#[derive(Clone, Debug, Default)]
struct FragmentHealingOutcome {
    scanned_namespace_records: u64,
    repaired_namespace_records: u64,
    scanned_objects: u64,
    repaired_objects: u64,
    repaired_shards: u64,
    unrecoverable_objects: u64,
    failed_objects: u64,
    headless_objects: u64,
    reports_written: u64,
}

static STATUS: LazyLock<Arc<RwLock<AutoHealingStatus>>> =
    LazyLock::new(|| Arc::new(RwLock::new(AutoHealingStatus::default())));
static PROGRESS: LazyLock<Mutex<Option<HealingProgressStore>>> = LazyLock::new(|| Mutex::new(None));
static CYCLE_RUNNING: AtomicBool = AtomicBool::new(false);
static LEASE_LOCKS: LazyLock<StdMutex<HashMap<String, Arc<Mutex<()>>>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

struct CycleGuard;

impl Drop for CycleGuard {
    fn drop(&mut self) {
        CYCLE_RUNNING.store(false, Ordering::Release);
    }
}

fn hash(parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
        hasher.update([0]);
    }
    hasher.finalize().into()
}

#[cfg(test)]
fn healing_partition(fragment: &str) -> u64 {
    let digest = hash(&[b"n2-auto-healing-lease-v1", fragment.as_bytes()]);
    u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 prefix"))
}

fn namespace_lease_voters(runtime: &ClusterRuntime) -> Result<Vec<ClusterNode>, AppError> {
    let pool = runtime
        .membership
        .newest_writable_pool()
        .ok_or_else(|| AppError::ServiceUnavailable("cluster has no writable pool".into()))?;
    let layout = build_ncnx_layout(runtime, pool, "healing-control", "lease-holder")?;
    layout
        .replicas
        .into_iter()
        .map(|replica| {
            runtime
                .membership
                .nodes
                .iter()
                .find(|node| node.id == replica.node_id)
                .cloned()
                .ok_or_else(|| {
                    AppError::Conflict(format!(
                        "NCNX healing voter '{}' is absent from membership",
                        replica.node_id
                    ))
                })
        })
        .collect()
}

fn score(domain: &[u8], runtime: &ClusterRuntime, subject: &str, node: &ClusterNode) -> [u8; 32] {
    hash(&[
        domain,
        runtime.membership.cluster_id.as_bytes(),
        &runtime.membership.epoch.to_be_bytes(),
        subject.as_bytes(),
        node.id.as_bytes(),
    ])
}

pub fn select_healer_pool(
    runtime: &ClusterRuntime,
    live_nodes: &[ClusterNode],
    requested: usize,
) -> Vec<ClusterNode> {
    let mut nodes = live_nodes.to_vec();
    nodes.sort_by(|left, right| {
        score(b"n2-healer-pool-v1", runtime, "", right)
            .cmp(&score(b"n2-healer-pool-v1", runtime, "", left))
            .then(left.id.cmp(&right.id))
    });
    let automatic = live_nodes.len().div_ceil(2).max(1);
    nodes.truncate(if requested == 0 { automatic } else { requested }.min(nodes.len()));
    nodes
}

pub fn fragment_healer_owner<'a>(
    runtime: &ClusterRuntime,
    fragment: &str,
    pool: &'a [ClusterNode],
) -> Option<&'a ClusterNode> {
    pool.iter().max_by(|left, right| {
        score(b"n2-fragment-healer-owner-v1", runtime, fragment, left)
            .cmp(&score(
                b"n2-fragment-healer-owner-v1",
                runtime,
                fragment,
                right,
            ))
            .then_with(|| right.id.cmp(&left.id))
    })
}

fn state_root() -> Result<PathBuf, AppError> {
    FS_STORAGE_ROOT
        .get()
        .map(|root| PathBuf::from(root).join(".n2").join("auto-healing"))
        .ok_or_else(|| AppError::InternalServerError("N2 storage root is not initialized".into()))
}

fn progress_path() -> Result<PathBuf, AppError> {
    Ok(state_root()?.join("progress.json"))
}

fn lease_path(fragment: &str) -> Result<PathBuf, AppError> {
    let name = hex::encode(hash(&[b"n2-healing-lease-file-v1", fragment.as_bytes()]));
    Ok(state_root()?.join("leases").join(format!("{name}.json")))
}

async fn atomic_write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), AppError> {
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError("N2 healing state path has no parent".into())
    })?;
    fs::create_dir_all(parent)
        .await
        .map_err(|err| AppError::Io(err, format!("create {}", parent.display())))?;
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|err| AppError::InternalServerError(format!("encode N2 healing state: {err}")))?;
    let tmp = path.with_extension("json.tmp");
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp)
        .await
        .map_err(|err| AppError::Io(err, format!("open {}", tmp.display())))?;
    file.write_all(&bytes)
        .await
        .map_err(|err| AppError::Io(err, format!("write {}", tmp.display())))?;
    file.flush()
        .await
        .map_err(|err| AppError::Io(err, format!("flush {}", tmp.display())))?;
    file.sync_all()
        .await
        .map_err(|err| AppError::Io(err, format!("sync {}", tmp.display())))?;
    drop(file);
    fs::rename(&tmp, path)
        .await
        .map_err(|err| AppError::Io(err, format!("publish {}", path.display())))?;
    crate::core::upload::sync_dir(parent).await
}

async fn load_progress_locked(guard: &mut Option<HealingProgressStore>) -> Result<(), AppError> {
    if guard.is_some() {
        return Ok(());
    }
    let path = progress_path()?;
    let value = match fs::read(&path).await {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(|err| {
            AppError::InternalServerError(format!(
                "decode N2 auto-healing progress {}: {err}",
                path.display()
            ))
        })?,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => HealingProgressStore::default(),
        Err(err) => return Err(AppError::Io(err, format!("read {}", path.display()))),
    };
    *guard = Some(value);
    Ok(())
}

async fn progress_snapshot() -> Result<HealingProgressStore, AppError> {
    let mut guard = PROGRESS.lock().await;
    load_progress_locked(&mut guard).await?;
    Ok(guard.clone().unwrap_or_default())
}

async fn update_progress(
    fragment: &str,
    cycle: u64,
    continuation_token: Option<String>,
) -> Result<(), AppError> {
    let mut guard = PROGRESS.lock().await;
    load_progress_locked(&mut guard).await?;
    let progress = guard.as_mut().expect("progress loaded");
    progress.cycle = progress.cycle.max(cycle);
    let fragment_progress = progress.fragments.entry(fragment.to_string()).or_default();
    fragment_progress.continuation_token = continuation_token.clone();
    if continuation_token.is_none() {
        fragment_progress.completed_cycles = fragment_progress.completed_cycles.saturating_add(1);
    }
    fragment_progress.last_finished_at_unix = Some(Utc::now().timestamp());
    atomic_write_json(&progress_path()?, progress).await
}

fn lease_lock(fragment: &str) -> Result<Arc<Mutex<()>>, AppError> {
    let mut locks = LEASE_LOCKS
        .lock()
        .map_err(|_| AppError::InternalServerError("N2 healing lease lock map poisoned".into()))?;
    Ok(locks
        .entry(fragment.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone())
}

async fn claim_local_lease(
    request: &PeerHealingLeaseRequest,
) -> Result<PeerHealingLeaseResponse, AppError> {
    let runtime = active_cluster_runtime()?;
    if request.membership_epoch != runtime.membership.epoch {
        return Err(AppError::Conflict(format!(
            "healing lease epoch {} does not match local membership epoch {}",
            request.membership_epoch, runtime.membership.epoch
        )));
    }
    if !runtime
        .membership
        .nodes
        .iter()
        .any(|node| node.id == request.owner_node_id)
    {
        return Err(AppError::BadRequest(format!(
            "healing lease owner {} is absent from membership",
            request.owner_node_id
        )));
    }
    if !namespace_lease_voters(&runtime)?
        .iter()
        .any(|node| node.id == runtime.local_node_id)
    {
        return Err(AppError::Forbidden(
            "this node is not a voter for the fragment healing lease".into(),
        ));
    }

    let lock = lease_lock(&request.fragment)?;
    let _guard = lock.lock().await;
    let path = lease_path(&request.fragment)?;
    let now = Utc::now().timestamp();
    let existing = match fs::read(&path).await {
        Ok(bytes) => serde_json::from_slice::<HealingLeaseRecord>(&bytes).ok(),
        Err(_) => None,
    };
    if let Some(existing) = existing
        && existing.expires_at_unix > now
        && existing.owner_node_id != request.owner_node_id
    {
        return Ok(PeerHealingLeaseResponse {
            voter_node_id: runtime.local_node_id.clone(),
            granted: false,
            expires_at_unix: existing.expires_at_unix,
        });
    }

    let expires_at_unix = now.saturating_add(request.lease_seconds.clamp(10, 3600) as i64);
    let record = HealingLeaseRecord {
        fragment: request.fragment.clone(),
        membership_epoch: request.membership_epoch,
        owner_node_id: request.owner_node_id.clone(),
        lease_id: request.lease_id.clone(),
        expires_at_unix,
    };
    atomic_write_json(&path, &record).await?;
    Ok(PeerHealingLeaseResponse {
        voter_node_id: runtime.local_node_id.clone(),
        granted: true,
        expires_at_unix,
    })
}

async fn claim_remote_lease(
    node: ClusterNode,
    request: PeerHealingLeaseRequest,
) -> Result<PeerHealingLeaseResponse, AppError> {
    let client = peer_http_client()?;
    let mut send = client
        .post(format!(
            "{}/n2/peer/healing/lease",
            node.endpoint.trim_end_matches('/')
        ))
        .json(&request);
    if let Some(secret) = cluster_secret() {
        send = send.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(HEALING_LEASE_TIMEOUT, send.send())
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!("healing lease voter {} timed out", node.id))
        })?
        .map_err(|err| {
            AppError::ServiceUnavailable(format!("healing lease voter {}: {err}", node.id))
        })?;
    if !response.status().is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "healing lease voter {} returned {}",
            node.id,
            response.status()
        )));
    }
    response.json().await.map_err(|err| {
        AppError::InternalServerError(format!(
            "decode healing lease response from {}: {err}",
            node.id
        ))
    })
}

async fn acquire_fragment_lease(
    runtime: Arc<ClusterRuntime>,
    fragment: &str,
    lease_seconds: u64,
    live_node_ids: &[String],
) -> Result<bool, AppError> {
    let voters = namespace_lease_voters(&runtime)?;
    let required = (voters.len() / 2).saturating_add(1);
    let mut sorted_live = live_node_ids.to_vec();
    sorted_live.sort();
    let live_view = sorted_live.join(",");
    let lease_id = hex::encode(hash(&[
        b"n2-healing-lease-id-v1",
        runtime.membership.cluster_id.as_bytes(),
        &runtime.membership.epoch.to_be_bytes(),
        live_view.as_bytes(),
        fragment.as_bytes(),
        runtime.local_node_id.as_bytes(),
    ]));
    let request = PeerHealingLeaseRequest {
        fragment: fragment.to_string(),
        membership_epoch: runtime.membership.epoch,
        owner_node_id: runtime.local_node_id.clone(),
        lease_id,
        lease_seconds,
    };
    let local_id = runtime.local_node_id.clone();
    let results = stream::iter(voters.into_iter().map(|node| {
        let request = request.clone();
        let local_id = local_id.clone();
        async move {
            if node.id == local_id {
                claim_local_lease(&request).await
            } else {
                claim_remote_lease(node, request).await
            }
        }
    }))
    .buffer_unordered(required.max(1))
    .collect::<Vec<_>>()
    .await;
    let granted = results
        .iter()
        .filter(|result| matches!(result, Ok(response) if response.granted))
        .count();
    Ok(granted >= required)
}

async fn apply_remote_safety_state(
    node: ClusterNode,
    state: SafetyReadOnlyState,
) -> Result<PeerSafetyReadOnlyResponse, AppError> {
    let client = peer_http_client()?;
    let mut request = client
        .post(format!(
            "{}/n2/peer/healing/read-only",
            node.endpoint.trim_end_matches('/')
        ))
        .json(&PeerSafetyReadOnlyRequest { state });
    if let Some(secret) = cluster_secret() {
        request = request.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(HEALING_LEASE_TIMEOUT, request.send())
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "safety read-only propagation to '{}' timed out",
                node.id
            ))
        })?
        .map_err(|error| {
            AppError::ServiceUnavailable(format!(
                "safety read-only propagation to '{}': {error}",
                node.id
            ))
        })?;
    if !response.status().is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "safety read-only propagation to '{}' returned {}",
            node.id,
            response.status()
        )));
    }
    response.json().await.map_err(|error| {
        AppError::InternalServerError(format!(
            "decode safety read-only response from '{}': {error}",
            node.id
        ))
    })
}

async fn propagate_safety_state(
    runtime: Arc<ClusterRuntime>,
    state: SafetyReadOnlyState,
) -> Result<(), AppError> {
    let local_id = runtime.local_node_id.clone();
    let results =
        futures_util::future::join_all(runtime.membership.nodes.iter().cloned().map(|node| {
            let state = state.clone();
            let local_id = local_id.clone();
            async move {
                if node.id == local_id {
                    apply_safety_read_only_state(state)
                        .await
                        .map(|_| PeerSafetyReadOnlyResponse {
                            node_id: node.id,
                            applied: true,
                        })
                } else {
                    apply_remote_safety_state(node, state).await
                }
            }
        }))
        .await;
    let configured_nodes = runtime
        .membership
        .nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let applied_nodes = results
        .iter()
        .filter_map(|result| result.as_ref().ok())
        .filter(|response| response.applied && configured_nodes.contains(response.node_id.as_str()))
        .count();
    let required = (configured_nodes.len() / 2).saturating_add(1);
    if applied_nodes < required {
        return Err(AppError::ServiceUnavailable(format!(
            "cluster safety read-only reached {applied_nodes}/{required} required unified nodes"
        )));
    }
    Ok(())
}

async fn activate_cluster_safety_read_only(
    runtime: Arc<ClusterRuntime>,
    report: &AutoHealingReport,
) -> Result<(), AppError> {
    if let Some(existing) = cluster_safety_read_only_state() {
        return propagate_safety_state(runtime, existing).await;
    }
    let state = SafetyReadOnlyState {
        incident_id: report.report_id.clone(),
        cluster_id: runtime.membership.cluster_id.clone(),
        membership_epoch: runtime.membership.epoch,
        detecting_node_id: runtime.local_node_id.clone(),
        fragment_id: report.fragment_id.clone(),
        finding: report.finding.clone(),
        report_filename: report.filename(),
        reason: report
            .reason
            .clone()
            .unwrap_or_else(|| "auto-healing found an unrecoverable invariant".into()),
        activated_at_unix_nanos: report.completed_at_unix_nanos,
    };
    propagate_safety_state(runtime, state).await
}

async fn heal_owned_fragment(
    runtime: Arc<ClusterRuntime>,
    fragment: String,
    cycle: u64,
    check_mode: RepairCheckMode,
    live_node_ids: Vec<String>,
) -> Result<Option<FragmentHealingOutcome>, AppError> {
    let cfg = &CONFIG.auto_healing;
    if !acquire_fragment_lease(
        runtime.clone(),
        &fragment,
        cfg.lease_seconds,
        &live_node_ids,
    )
    .await?
    {
        return Ok(None);
    }
    let progress = progress_snapshot().await?;
    let continuation_token = progress
        .fragments
        .get(&fragment)
        .and_then(|progress| progress.continuation_token.clone());
    let scan_id = format!("{}-{}", cycle, ulid::Ulid::new());
    let namespace = scan_and_repair_fragment_namespace(runtime.clone(), &fragment, &scan_id).await;
    let objects = scan_and_repair_fragment_objects(
        runtime.clone(),
        &fragment,
        &scan_id,
        &namespace,
        check_mode,
        continuation_token.as_deref(),
        cfg.max_objects_per_fragment_cycle,
        Duration::from_secs(cfg.max_duration_seconds),
    )
    .await;

    let mut reports = namespace.reports;
    reports.extend(objects.reports);
    let critical = reports
        .iter()
        .find(|report| report.cluster_read_only_triggered)
        .cloned();
    let mut reports_written = 0u64;
    let mut report_error = None;
    for report in &reports {
        match write_local_report(report).await {
            Ok(_) => reports_written = reports_written.saturating_add(1),
            Err(error) => {
                tracing::error!(
                    report_id = %report.report_id,
                    fragment = %fragment,
                    "N2 could not persist an auto-healing report: {error}"
                );
                if report_error.is_none() {
                    report_error = Some(error);
                }
            }
        }
    }
    if let Some(report) = critical.as_ref() {
        activate_cluster_safety_read_only(runtime, report).await?;
    }
    if let Some(error) = report_error {
        return Err(error);
    }
    update_progress(&fragment, cycle, objects.continuation_token.clone()).await?;
    Ok(Some(FragmentHealingOutcome {
        scanned_namespace_records: namespace.scanned_records,
        repaired_namespace_records: namespace.repaired_records,
        scanned_objects: objects.scanned_objects,
        repaired_objects: objects.repaired_objects,
        repaired_shards: objects.repaired_shards,
        unrecoverable_objects: objects.unrecoverable_objects,
        failed_objects: objects.failed_objects,
        headless_objects: objects.headless_objects,
        reports_written,
    }))
}

pub async fn run_auto_healing_cycle() -> Result<AutoHealingStatus, AppError> {
    if CYCLE_RUNNING
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err(AppError::Conflict(
            "an N2 auto-healing cycle is already running on this node".into(),
        ));
    }
    let _cycle_guard = CycleGuard;
    let cfg = CONFIG.auto_healing.clone();
    let runtime = active_cluster_runtime()?;
    if let Some(state) = cluster_safety_read_only_state() {
        propagate_safety_state(runtime.clone(), state).await?;
    }
    let progress = progress_snapshot().await?;
    let cycle = progress.cycle.saturating_add(1);
    let check_mode = if cycle % cfg.checksum_scan_every_cycles == 0 {
        RepairCheckMode::Checksum
    } else {
        RepairCheckMode::Availability
    };
    let live_nodes = live_active_write_nodes(&runtime).await;
    let healer_pool = select_healer_pool(&runtime, &live_nodes, cfg.coordinator_nodes);
    let fragments = discover_local_fragments();
    let owned = fragments
        .iter()
        .filter(|fragment| {
            fragment_healer_owner(&runtime, fragment, &healer_pool)
                .is_some_and(|node| node.id == runtime.local_node_id)
        })
        .cloned()
        .collect::<Vec<_>>();
    let live_node_ids = live_nodes
        .iter()
        .map(|node| node.id.clone())
        .collect::<Vec<_>>();
    let mut status = AutoHealingStatus {
        enabled: cfg.enabled,
        running: true,
        cycle,
        check_mode: match check_mode {
            RepairCheckMode::Availability => "availability",
            _ => "checksum",
        }
        .to_string(),
        membership_epoch: runtime.membership.epoch,
        configured_nodes: runtime.membership.nodes.len(),
        live_nodes: live_nodes.len(),
        healer_pool: healer_pool.iter().map(|node| node.id.clone()).collect(),
        discovered_fragments: fragments.len(),
        owned_fragments: owned.len(),
        safety_read_only: is_cluster_safety_read_only(),
        started_at_unix: Some(Utc::now().timestamp()),
        ..AutoHealingStatus::default()
    };
    *STATUS.write().await = status.clone();

    let results = stream::iter(owned.into_iter().map(|fragment| {
        let runtime = runtime.clone();
        let live_node_ids = live_node_ids.clone();
        async move {
            let result =
                heal_owned_fragment(runtime, fragment.clone(), cycle, check_mode, live_node_ids)
                    .await;
            (fragment, result)
        }
    }))
    .buffer_unordered(cfg.max_parallel_fragments)
    .collect::<Vec<_>>()
    .await;

    for (fragment, result) in results {
        match result {
            Ok(Some(result)) => {
                status.leased_fragments = status.leased_fragments.saturating_add(1);
                status.scanned_namespace_records = status
                    .scanned_namespace_records
                    .saturating_add(result.scanned_namespace_records);
                status.repaired_namespace_records = status
                    .repaired_namespace_records
                    .saturating_add(result.repaired_namespace_records);
                status.scanned_objects = status
                    .scanned_objects
                    .saturating_add(result.scanned_objects);
                status.repaired_objects = status
                    .repaired_objects
                    .saturating_add(result.repaired_objects);
                status.repaired_shards = status
                    .repaired_shards
                    .saturating_add(result.repaired_shards);
                status.unrecoverable_objects = status
                    .unrecoverable_objects
                    .saturating_add(result.unrecoverable_objects);
                status.failed_objects = status.failed_objects.saturating_add(result.failed_objects);
                status.headless_objects = status
                    .headless_objects
                    .saturating_add(result.headless_objects);
                status.reports_written = status
                    .reports_written
                    .saturating_add(result.reports_written);
            }
            Ok(None) => {}
            Err(err) => {
                status.last_error = Some(format!("fragment {fragment}: {err}"));
            }
        }
    }

    {
        let mut progress = PROGRESS.lock().await;
        load_progress_locked(&mut progress).await?;
        let store = progress.as_mut().expect("progress loaded");
        store.cycle = store.cycle.max(cycle);
        atomic_write_json(&progress_path()?, store).await?;
    }
    status.running = false;
    status.safety_read_only = is_cluster_safety_read_only();
    status.finished_at_unix = Some(Utc::now().timestamp());
    *STATUS.write().await = status.clone();
    Ok(status)
}

pub fn spawn_auto_healing_task() {
    let cfg = CONFIG.auto_healing.clone();
    if !cfg.enabled {
        tracing::info!("N2 autonomous healing is disabled by configuration");
        return;
    }
    tracing::info!(
        interval_seconds = cfg.scan_interval_seconds,
        max_parallel_fragments = cfg.max_parallel_fragments,
        max_objects_per_fragment_cycle = cfg.max_objects_per_fragment_cycle,
        checksum_scan_every_cycles = cfg.checksum_scan_every_cycles,
        "N2 autonomous recorded-layout healing is enabled"
    );
    tokio::spawn(async move {
        sleep(Duration::from_secs(cfg.initial_delay_seconds)).await;
        loop {
            if let Err(err) = run_auto_healing_cycle().await {
                tracing::warn!("N2 autonomous healing cycle failed: {err}");
                let mut status = STATUS.write().await;
                status.enabled = true;
                status.running = false;
                status.finished_at_unix = Some(Utc::now().timestamp());
                status.last_error = Some(err.to_string());
            }
            sleep(Duration::from_secs(cfg.scan_interval_seconds)).await;
        }
    });
}

pub async fn auto_healing_status(claims: Claims) -> Result<Json<AutoHealingStatus>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let mut status = STATUS.read().await.clone();
    status.enabled = CONFIG.auto_healing.enabled;
    status.safety_read_only = is_cluster_safety_read_only();
    Ok(Json(status))
}

pub async fn auto_healing_status_snapshot() -> AutoHealingStatus {
    let mut status = STATUS.read().await.clone();
    status.enabled = CONFIG.auto_healing.enabled;
    status.safety_read_only = is_cluster_safety_read_only();
    status
}

pub async fn trigger_auto_healing_cycle(
    claims: Claims,
) -> Result<Json<AutoHealingStatus>, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden("Modify permission required".into()));
    }
    Ok(Json(run_auto_healing_cycle().await?))
}

pub async fn peer_healing_lease(
    headers: HeaderMap,
    State(_state): State<Arc<AppState>>,
    Json(request): Json<PeerHealingLeaseRequest>,
) -> Result<Json<PeerHealingLeaseResponse>, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(claim_local_lease(&request).await?))
}

pub async fn peer_safety_read_only(
    headers: HeaderMap,
    State(_state): State<Arc<AppState>>,
    Json(request): Json<PeerSafetyReadOnlyRequest>,
) -> Result<Json<PeerSafetyReadOnlyResponse>, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    if request.state.cluster_id != runtime.membership.cluster_id
        || request.state.membership_epoch > runtime.membership.epoch
    {
        return Err(AppError::Conflict(
            "safety read-only marker does not belong to this cluster epoch".into(),
        ));
    }
    apply_safety_read_only_state(request.state).await?;
    Ok(Json(PeerSafetyReadOnlyResponse {
        node_id: runtime.local_node_id.clone(),
        applied: true,
    }))
}

pub fn build_auto_healing_peer_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/n2/peer/healing/lease", post(peer_healing_lease))
        .route("/n2/peer/healing/read-only", post(peer_safety_read_only))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::layout::ClusterLayoutPolicy;
    use crate::distribution::membership::{
        ClusterMembership, ClusterPolicy, N2_MEMBERSHIP_FORMAT_VERSION, NodeState, PoolState,
        ServerPool, StorageRoot,
    };

    fn runtime() -> ClusterRuntime {
        ClusterRuntime {
            local_node_id: "node-a".into(),
            local_pool_id: "pool-a".into(),
            local_storage_roots: vec![],
            membership_path: PathBuf::new(),
            layout_policy: ClusterLayoutPolicy::default(),
            membership: ClusterMembership {
                format_version: N2_MEMBERSHIP_FORMAT_VERSION,
                cluster_id: "healing-test".into(),
                epoch: 9,
                pools: vec![ServerPool {
                    id: "pool-a".into(),
                    generation: 9,
                    policy: ClusterPolicy {
                        object_data_shards: 2,
                        object_parity_shards: 1,
                    },
                    state: PoolState::Writable,
                    node_ids: ["node-a", "node-b", "node-c", "node-d"]
                        .into_iter()
                        .map(str::to_string)
                        .collect(),
                }],
                nodes: ["node-a", "node-b", "node-c", "node-d"]
                    .into_iter()
                    .enumerate()
                    .map(|(_index, id)| ClusterNode {
                        id: id.into(),
                        endpoint: format!("https://{id}"),
                        native_endpoint: None,
                        state: Some(NodeState::Online),
                        weight: 1,
                        storage_roots: vec![StorageRoot {
                            id: format!("root-{id}"),
                            path: format!("/disk-{id}"),
                        }],
                        joined_epoch: 1,
                        capacity_bytes: None,
                        used_bytes: None,
                        last_seen_unix: None,
                    })
                    .collect(),
            },
            auto_rebalance: false,
        }
    }

    #[test]
    fn healer_pool_and_fragment_owner_are_deterministic_and_bounded() {
        let runtime = runtime();
        let first = select_healer_pool(&runtime, &runtime.membership.nodes, 2);
        let second = select_healer_pool(&runtime, &runtime.membership.nodes, 2);
        assert_eq!(
            first.iter().map(|node| &node.id).collect::<Vec<_>>(),
            second.iter().map(|node| &node.id).collect::<Vec<_>>()
        );
        assert_eq!(first.len(), 2);
        assert_eq!(
            fragment_healer_owner(&runtime, "fragment-a", &first).map(|node| &node.id),
            fragment_healer_owner(&runtime, "fragment-a", &second).map(|node| &node.id)
        );
    }

    #[test]
    fn automatic_pool_uses_half_of_live_nodes_for_ha() {
        let runtime = runtime();
        assert_eq!(
            select_healer_pool(&runtime, &runtime.membership.nodes, 0).len(),
            2
        );
        assert_eq!(
            select_healer_pool(&runtime, &runtime.membership.nodes[..3], 0).len(),
            2
        );
    }

    #[test]
    fn lease_partition_is_stable_and_fragment_specific() {
        assert_eq!(
            healing_partition("fragment-a"),
            healing_partition("fragment-a")
        );
        assert_ne!(
            healing_partition("fragment-a"),
            healing_partition("fragment-b")
        );
    }
}
