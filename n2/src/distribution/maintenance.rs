use crate::core::disk_manager::ObjectPathManager;
use crate::core::fragment_identity::{list_fragment_aliases, stable_fragment_id_for_root};
use crate::core::fragment_metadata::resolve_stable_fragment_id;
use crate::core::garbage_collector::{
    GcJobStatus, get_gc_status, start_garbage_collector_job,
};
use crate::distribution::membership::{
    ClusterNode, active_cluster_runtime, live_active_write_nodes,
};
use crate::distribution::peer::{authorize_peer, cluster_secret, peer_http_client};
use crate::distribution::rebalance::{
    ClusterRebalanceRequest, run_rewrite_fragment_once,
};
use crate::enums::app_error_enum::AppError;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, LazyLock, Mutex};
use tokio::time::{Duration, sleep, timeout};
use ulid::Ulid;

const PEER_MAINTENANCE_TIMEOUT: Duration = Duration::from_secs(10);

fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DistributedMaintenanceKind {
    Gc,
    Repair,
    Rebalance,
}

impl DistributedMaintenanceKind {
    fn job_name(&self) -> &'static str {
        match self {
            Self::Gc => "distributed_gc",
            Self::Repair => "distributed_repair",
            Self::Rebalance => "distributed_rebalance",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct DistributedMaintenanceRequest {
    pub kind: DistributedMaintenanceKind,
    pub fragments: Vec<String>,
    pub prefix: Option<String>,
    pub dry_run: bool,
    pub mode: Option<String>,
    pub fast_mode: bool,
    pub limit: Option<usize>,
    #[serde(default = "default_true")]
    pub mismatched_only: bool,
    pub max_duration_seconds: Option<u64>,
    pub max_objects: Option<u64>,
    pub max_bytes: Option<u64>,
    pub max_directories: Option<u64>,
    pub priority: Option<crate::io_control::IoPriority>,
}

impl Default for DistributedMaintenanceRequest {
    fn default() -> Self {
        Self {
            kind: DistributedMaintenanceKind::Repair,
            fragments: Vec::new(),
            prefix: None,
            dry_run: false,
            mode: None,
            fast_mode: true,
            limit: None,
            mismatched_only: true,
            max_duration_seconds: None,
            max_objects: None,
            max_bytes: None,
            max_directories: None,
            priority: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FragmentMaintenanceState {
    Assigned,
    Running,
    Completed,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct FragmentMaintenanceAssignment {
    pub fragment: String,
    pub node_id: String,
    pub endpoint: String,
    pub state: FragmentMaintenanceState,
    pub scanned: u64,
    pub rewritten: u64,
    pub skipped: u64,
    pub failed: u64,
    pub bytes_processed: u64,
    pub gc_fragments_scanned: u64,
    pub gc_refund_bytes: u64,
    pub last_error: Option<String>,
}

impl Default for FragmentMaintenanceAssignment {
    fn default() -> Self {
        Self {
            fragment: String::new(),
            node_id: String::new(),
            endpoint: String::new(),
            state: FragmentMaintenanceState::Assigned,
            scanned: 0,
            rewritten: 0,
            skipped: 0,
            failed: 0,
            bytes_processed: 0,
            gc_fragments_scanned: 0,
            gc_refund_bytes: 0,
            last_error: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct DistributedMaintenanceStatus {
    pub running: bool,
    pub job_id: String,
    pub kind: Option<DistributedMaintenanceKind>,
    pub coordinator_node_id: String,
    pub membership_epoch: u64,
    pub total_fragments: usize,
    pub live_workers: usize,
    pub started_at_unix: Option<i64>,
    pub finished_at_unix: Option<i64>,
    pub assignments: Vec<FragmentMaintenanceAssignment>,
    pub scanned: u64,
    pub rewritten: u64,
    pub skipped: u64,
    pub failed: u64,
    pub bytes_processed: u64,
    pub gc_fragments_scanned: u64,
    pub gc_refund_bytes: u64,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerDistributedMaintenanceRequest {
    pub job_id: String,
    pub kind: DistributedMaintenanceKind,
    pub fragments: Vec<String>,
    pub prefix: Option<String>,
    pub dry_run: bool,
    pub mode: Option<String>,
    pub fast_mode: bool,
    pub limit: Option<usize>,
    pub mismatched_only: bool,
    pub max_duration_seconds: Option<u64>,
    pub max_objects: Option<u64>,
    pub max_bytes: Option<u64>,
    pub max_directories: Option<u64>,
    pub priority: Option<crate::io_control::IoPriority>,
}

static DISTRIBUTED_STATUS: LazyLock<Mutex<DistributedMaintenanceStatus>> =
    LazyLock::new(|| Mutex::new(DistributedMaintenanceStatus::default()));
static PEER_STATUS: LazyLock<Mutex<HashMap<String, DistributedMaintenanceStatus>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub fn discover_local_fragments() -> Vec<String> {
    let path_manager = ObjectPathManager::new();
    let mut fragments = BTreeSet::new();
    for root in path_manager.get_all_index_roots() {
        for alias in list_fragment_aliases(&root) {
            if let Some(stable) = stable_fragment_id_for_root(&root, &alias) {
                fragments.insert(stable);
            }
        }
    }
    fragments.into_iter().collect()
}

pub fn assign_fragments_to_nodes(
    fragments: &[String],
    nodes: &[ClusterNode],
) -> Vec<FragmentMaintenanceAssignment> {
    let mut fragments = fragments.to_vec();
    fragments.sort();
    fragments.dedup();
    let mut nodes = nodes.to_vec();
    nodes.sort_by(|left, right| left.id.cmp(&right.id));
    if nodes.is_empty() {
        return Vec::new();
    }
    fragments
        .into_iter()
        .enumerate()
        .map(|(index, fragment)| {
            let node = &nodes[index % nodes.len()];
            FragmentMaintenanceAssignment {
                fragment,
                node_id: node.id.clone(),
                endpoint: node.endpoint.clone(),
                ..Default::default()
            }
        })
        .collect()
}

fn rewrite_request_for_fragment(
    req: &PeerDistributedMaintenanceRequest,
    fragment: &str,
) -> ClusterRebalanceRequest {
    ClusterRebalanceRequest {
        fragment: fragment.to_string(),
        prefix: req.prefix.clone(),
        limit: req.limit,
        dry_run: req.dry_run,
        mismatched_only: req.mismatched_only,
        max_duration_seconds: req.max_duration_seconds,
        max_objects: req.max_objects,
        max_bytes: req.max_bytes,
        max_directories: req.max_directories,
        continuation_token: None,
        priority: req.priority,
    }
}

async fn wait_gc_job() -> GcJobStatus {
    loop {
        let snapshot = get_gc_status().await;
        if !snapshot.is_running {
            return snapshot;
        }
        sleep(Duration::from_millis(300)).await;
    }
}

fn summarize_assignments(status: &mut DistributedMaintenanceStatus) {
    status.scanned = 0;
    status.rewritten = 0;
    status.skipped = 0;
    status.failed = 0;
    status.bytes_processed = 0;
    status.gc_fragments_scanned = 0;
    status.gc_refund_bytes = 0;
    for assignment in &status.assignments {
        status.scanned = status.scanned.saturating_add(assignment.scanned);
        status.rewritten = status.rewritten.saturating_add(assignment.rewritten);
        status.skipped = status.skipped.saturating_add(assignment.skipped);
        status.failed = status.failed.saturating_add(assignment.failed);
        status.bytes_processed = status
            .bytes_processed
            .saturating_add(assignment.bytes_processed);
        status.gc_fragments_scanned = status
            .gc_fragments_scanned
            .saturating_add(assignment.gc_fragments_scanned);
        status.gc_refund_bytes = status
            .gc_refund_bytes
            .saturating_add(assignment.gc_refund_bytes);
    }
}

fn set_peer_status(job_id: &str, f: impl FnOnce(&mut DistributedMaintenanceStatus)) {
    if let Ok(mut guard) = PEER_STATUS.lock() {
        let status = guard.entry(job_id.to_string()).or_default();
        f(status);
        summarize_assignments(status);
    }
}

fn get_peer_status(job_id: &str) -> Option<DistributedMaintenanceStatus> {
    PEER_STATUS
        .lock()
        .ok()
        .and_then(|guard| guard.get(job_id).cloned())
}

async fn run_peer_maintenance_worker(
    state: Arc<AppState>,
    local_node_id: String,
    req: PeerDistributedMaintenanceRequest,
) {
    for fragment in req.fragments.clone() {
        set_peer_status(&req.job_id, |status| {
            if let Some(assignment) = status
                .assignments
                .iter_mut()
                .find(|assignment| assignment.fragment == fragment)
            {
                assignment.state = FragmentMaintenanceState::Running;
            }
        });
        let result = match req.kind {
            DistributedMaintenanceKind::Repair | DistributedMaintenanceKind::Rebalance => {
                let rewrite_req = rewrite_request_for_fragment(&req, &fragment);
                let timeout_secs = req.max_duration_seconds.unwrap_or(900).clamp(5, 86_400);
                let snapshot = timeout(
                    Duration::from_secs(timeout_secs),
                    run_rewrite_fragment_once(
                        rewrite_req,
                        matches!(req.kind, DistributedMaintenanceKind::Repair),
                    ),
                )
                .await
                .map_err(|_| {
                    AppError::Timeout(format!(
                        "distributed {:?} fragment {} exceeded {}s",
                        req.kind, fragment, timeout_secs
                    ))
                });
                match snapshot {
                    Ok(snapshot) => Ok(FragmentMaintenanceAssignment {
                        fragment: fragment.clone(),
                        node_id: local_node_id.clone(),
                        state: if snapshot.failed > 0 {
                            FragmentMaintenanceState::Failed
                        } else {
                            FragmentMaintenanceState::Completed
                        },
                        scanned: snapshot.scanned,
                        rewritten: snapshot.rewritten,
                        skipped: snapshot.skipped,
                        failed: snapshot.failed,
                        bytes_processed: snapshot.bytes_processed,
                        last_error: snapshot.last_error,
                        ..Default::default()
                    }),
                    Err(err) => Err(err),
                }
            }
            DistributedMaintenanceKind::Gc => {
                match start_garbage_collector_job(
                    state.clone(),
                    Some(fragment.clone()),
                    req.mode.clone(),
                    Some(req.fast_mode),
                )
                .await
                {
                    Ok(()) => {
                        let snapshot = wait_gc_job().await;
                        Ok(FragmentMaintenanceAssignment {
                            fragment: fragment.clone(),
                            node_id: local_node_id.clone(),
                            state: if snapshot.total_errors > 0 {
                                FragmentMaintenanceState::Failed
                            } else {
                                FragmentMaintenanceState::Completed
                            },
                            failed: snapshot.total_errors,
                            gc_fragments_scanned: snapshot.total_fragments_scanned,
                            gc_refund_bytes: snapshot.total_refund_bytes,
                            last_error: snapshot.last_error,
                            ..Default::default()
                        })
                    }
                    Err(err) => Err(err),
                }
            }
        };
        set_peer_status(&req.job_id, |status| {
            if let Some(assignment) = status
                .assignments
                .iter_mut()
                .find(|assignment| assignment.fragment == fragment)
            {
                match result {
                    Ok(snapshot) => {
                        assignment.state = snapshot.state;
                        assignment.scanned = snapshot.scanned;
                        assignment.rewritten = snapshot.rewritten;
                        assignment.skipped = snapshot.skipped;
                        assignment.failed = snapshot.failed;
                        assignment.bytes_processed = snapshot.bytes_processed;
                        assignment.gc_fragments_scanned = snapshot.gc_fragments_scanned;
                        assignment.gc_refund_bytes = snapshot.gc_refund_bytes;
                        assignment.last_error = snapshot.last_error;
                    }
                    Err(err) => {
                        assignment.state = FragmentMaintenanceState::Failed;
                        assignment.failed = assignment.failed.saturating_add(1);
                        assignment.last_error = Some(err.to_string());
                    }
                }
            }
            let all_done = status.assignments.iter().all(|assignment| {
                matches!(
                    assignment.state,
                    FragmentMaintenanceState::Completed | FragmentMaintenanceState::Failed
                )
            });
            if all_done {
                status.running = false;
                status.finished_at_unix = Some(chrono::Utc::now().timestamp());
            }
        });
    }
}

fn begin_peer_maintenance(
    state: Arc<AppState>,
    req: PeerDistributedMaintenanceRequest,
) -> Result<DistributedMaintenanceStatus, AppError> {
    let runtime = active_cluster_runtime()?;
    let local_node_id = runtime.local_node_id.clone();
    {
        let guard = PEER_STATUS
            .lock()
            .map_err(|_| AppError::InternalServerError("N2 peer maintenance lock poisoned".into()))?;
        if guard.values().any(|status| status.running) {
            return Err(AppError::Conflict(
                "N2 peer maintenance is already running on this node".into(),
            ));
        }
    }
    let assignments = req
        .fragments
        .iter()
        .map(|fragment| FragmentMaintenanceAssignment {
            fragment: fragment.clone(),
            node_id: local_node_id.clone(),
            endpoint: runtime
                .membership
                .nodes
                .iter()
                .find(|node| node.id == local_node_id)
                .map(|node| node.endpoint.clone())
                .unwrap_or_default(),
            state: FragmentMaintenanceState::Assigned,
            ..Default::default()
        })
        .collect::<Vec<_>>();
    let status = DistributedMaintenanceStatus {
        running: true,
        job_id: req.job_id.clone(),
        kind: Some(req.kind.clone()),
        coordinator_node_id: local_node_id.clone(),
        membership_epoch: runtime.membership.epoch,
        total_fragments: assignments.len(),
        live_workers: 1,
        started_at_unix: Some(chrono::Utc::now().timestamp()),
        assignments,
        ..Default::default()
    };
    if let Ok(mut guard) = PEER_STATUS.lock() {
        guard.insert(req.job_id.clone(), status.clone());
    }
    tokio::spawn(run_peer_maintenance_worker(state, local_node_id, req));
    Ok(status)
}

async fn post_peer_start(
    endpoint: &str,
    req: &PeerDistributedMaintenanceRequest,
) -> Result<DistributedMaintenanceStatus, AppError> {
    let client = peer_http_client()?;
    let url = format!("{}/n2/peer/maintenance/start", endpoint.trim_end_matches('/'));
    let mut request = client.post(url).json(req);
    if let Some(secret) = cluster_secret() {
        request = request.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(PEER_MAINTENANCE_TIMEOUT, request.send())
        .await
        .map_err(|_| AppError::ServiceUnavailable(format!("N2 maintenance peer {endpoint} timed out")))?
        .map_err(|err| AppError::ServiceUnavailable(format!("N2 maintenance peer {endpoint}: {err}")))?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "N2 maintenance peer {endpoint} returned {status}: {body}"
        )));
    }
    response
        .json::<DistributedMaintenanceStatus>()
        .await
        .map_err(|err| AppError::InternalServerError(format!("decode N2 maintenance peer response: {err}")))
}

async fn get_peer_remote_status(
    endpoint: &str,
    job_id: &str,
) -> Result<DistributedMaintenanceStatus, AppError> {
    let client = peer_http_client()?;
    let url = format!(
        "{}/n2/peer/maintenance/status/{}",
        endpoint.trim_end_matches('/'),
        job_id
    );
    let mut request = client.get(url);
    if let Some(secret) = cluster_secret() {
        request = request.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(PEER_MAINTENANCE_TIMEOUT, request.send())
        .await
        .map_err(|_| AppError::ServiceUnavailable(format!("N2 maintenance peer {endpoint} status timed out")))?
        .map_err(|err| AppError::ServiceUnavailable(format!("N2 maintenance peer {endpoint} status: {err}")))?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "N2 maintenance peer {endpoint} status returned {status}: {body}"
        )));
    }
    response
        .json::<DistributedMaintenanceStatus>()
        .await
        .map_err(|err| AppError::InternalServerError(format!("decode N2 maintenance peer status: {err}")))
}

fn coordinator_status_snapshot() -> DistributedMaintenanceStatus {
    DISTRIBUTED_STATUS
        .lock()
        .map(|guard| guard.clone())
        .unwrap_or_default()
}

async fn resolve_request_fragments(req: &DistributedMaintenanceRequest) -> Result<Vec<String>, AppError> {
    if req.fragments.is_empty() {
        let fragments = discover_local_fragments();
        if fragments.is_empty() {
            return Err(AppError::NotFound);
        }
        return Ok(fragments);
    }
    let mut out = BTreeSet::new();
    for fragment in &req.fragments {
        out.insert(resolve_stable_fragment_id(fragment).await?);
    }
    Ok(out.into_iter().collect())
}

pub async fn start_distributed_maintenance(
    claims: Claims,
    State(state): State<Arc<AppState>>,
    Json(req): Json<DistributedMaintenanceRequest>,
) -> Result<Json<DistributedMaintenanceStatus>, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden("Modify permission required".into()));
    }
    if matches!(req.kind, DistributedMaintenanceKind::Gc) && !claims.delete {
        return Err(AppError::Forbidden(
            "Delete permission required for distributed GC".into(),
        ));
    }
    let runtime = active_cluster_runtime()?;
    let live_nodes = live_active_write_nodes(&runtime).await;
    if live_nodes.is_empty() {
        return Err(AppError::ServiceUnavailable(
            "N2 distributed maintenance has no reachable write nodes".into(),
        ));
    }
    let fragments = resolve_request_fragments(&req).await?;
    let assignments = assign_fragments_to_nodes(&fragments, &live_nodes);
    let job_id = format!("{}-{}", req.kind.job_name(), Ulid::new());
    let mut started_assignments = Vec::new();
    let mut grouped = BTreeMap::<String, (ClusterNode, Vec<String>)>::new();
    for assignment in assignments {
        let Some(node) = live_nodes
            .iter()
            .find(|node| node.id == assignment.node_id)
            .cloned()
        else {
            continue;
        };
        grouped
            .entry(node.id.clone())
            .or_insert_with(|| (node, Vec::new()))
            .1
            .push(assignment.fragment);
    }

    for (_node_id, (node, node_fragments)) in grouped {
        let peer_req = PeerDistributedMaintenanceRequest {
            job_id: job_id.clone(),
            kind: req.kind.clone(),
            fragments: node_fragments.clone(),
            prefix: req.prefix.clone(),
            dry_run: req.dry_run,
            mode: req.mode.clone(),
            fast_mode: req.fast_mode,
            limit: req.limit,
            mismatched_only: req.mismatched_only,
            max_duration_seconds: req.max_duration_seconds,
            max_objects: req.max_objects,
            max_bytes: req.max_bytes,
            max_directories: req.max_directories,
            priority: req.priority,
        };
        let start_result = if node.id == runtime.local_node_id {
            begin_peer_maintenance(state.clone(), peer_req)
        } else {
            post_peer_start(&node.endpoint, &peer_req).await
        };
        match start_result {
            Ok(status) => started_assignments.extend(status.assignments),
            Err(err) => {
                for fragment in node_fragments {
                    started_assignments.push(FragmentMaintenanceAssignment {
                        fragment,
                        node_id: node.id.clone(),
                        endpoint: node.endpoint.clone(),
                        state: FragmentMaintenanceState::Failed,
                        failed: 1,
                        last_error: Some(err.to_string()),
                        ..Default::default()
                    });
                }
            }
        }
    }
    let mut status = DistributedMaintenanceStatus {
        running: started_assignments
            .iter()
            .any(|assignment| assignment.state != FragmentMaintenanceState::Failed),
        job_id,
        kind: Some(req.kind),
        coordinator_node_id: runtime.local_node_id.clone(),
        membership_epoch: runtime.membership.epoch,
        total_fragments: fragments.len(),
        live_workers: live_nodes.len(),
        started_at_unix: Some(chrono::Utc::now().timestamp()),
        assignments: started_assignments,
        ..Default::default()
    };
    summarize_assignments(&mut status);
    if let Ok(mut guard) = DISTRIBUTED_STATUS.lock() {
        *guard = status.clone();
    }
    Ok(Json(status))
}

pub async fn distributed_maintenance_status(
    claims: Claims,
) -> Result<Json<DistributedMaintenanceStatus>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let mut status = refresh_distributed_status().await;
    summarize_assignments(&mut status);
    Ok(Json(status))
}

async fn refresh_distributed_status() -> DistributedMaintenanceStatus {
    let current = coordinator_status_snapshot();
    if current.job_id.is_empty() || !current.running {
        return current;
    }
    let runtime = match active_cluster_runtime() {
        Ok(runtime) => runtime,
        Err(_) => return current,
    };
    let mut by_fragment = current
        .assignments
        .iter()
        .map(|assignment| (assignment.fragment.clone(), assignment.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut peer_jobs = Vec::new();
    for node in runtime.membership.nodes.iter().filter(|node| {
        current
            .assignments
            .iter()
            .any(|assignment| assignment.node_id == node.id)
    }) {
        let job_id = current.job_id.clone();
        if node.id == runtime.local_node_id {
            if let Some(peer_status) = get_peer_status(&job_id) {
                for assignment in peer_status.assignments {
                    by_fragment.insert(assignment.fragment.clone(), assignment);
                }
            }
        } else {
            peer_jobs.push(async move {
                get_peer_remote_status(&node.endpoint, &job_id).await.ok()
            });
        }
    }
    for peer_status in join_all(peer_jobs).await.into_iter().flatten() {
        for assignment in peer_status.assignments {
            by_fragment.insert(assignment.fragment.clone(), assignment);
        }
    }
    let mut status = current;
    status.assignments = by_fragment.into_values().collect();
    status.running = status.assignments.iter().any(|assignment| {
        matches!(
            assignment.state,
            FragmentMaintenanceState::Assigned | FragmentMaintenanceState::Running
        )
    });
    if !status.running && status.finished_at_unix.is_none() {
        status.finished_at_unix = Some(chrono::Utc::now().timestamp());
    }
    summarize_assignments(&mut status);
    if let Ok(mut guard) = DISTRIBUTED_STATUS.lock() {
        *guard = status.clone();
    }
    status
}

pub async fn peer_distributed_maintenance_start(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(req): Json<PeerDistributedMaintenanceRequest>,
) -> Result<Json<DistributedMaintenanceStatus>, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(begin_peer_maintenance(state, req)?))
}

pub async fn peer_distributed_maintenance_status(
    headers: HeaderMap,
    Path(job_id): Path<String>,
) -> Result<Json<DistributedMaintenanceStatus>, AppError> {
    authorize_peer(&headers)?;
    get_peer_status(&job_id)
        .map(Json)
        .ok_or(AppError::NotFound)
}

pub fn build_distributed_maintenance_peer_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/n2/peer/maintenance/start",
            post(peer_distributed_maintenance_start),
        )
        .route(
            "/n2/peer/maintenance/status/{job_id}",
            get(peer_distributed_maintenance_status),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::membership::{ClusterNode, NodeState};

    fn node(id: &str) -> ClusterNode {
        ClusterNode {
            id: id.to_string(),
            endpoint: format!("https://{id}:50001"),
            native_endpoint: Some(format!("n2://{id}:50002")),
            state: Some(NodeState::Online),
            weight: 1,
            index_roots: Vec::new(),
            blob_roots: Vec::new(),
            capacity_bytes: None,
            used_bytes: None,
            last_seen_unix: None,
        }
    }

    #[test]
    fn fragment_assignment_spreads_across_live_nodes() {
        let fragments = vec![
            "f3".to_string(),
            "f1".to_string(),
            "f2".to_string(),
            "f4".to_string(),
        ];
        let assignments = assign_fragments_to_nodes(&fragments, &[node("node-a"), node("node-b")]);
        let by_node = assignments.into_iter().fold(
            BTreeMap::<String, usize>::new(),
            |mut counts, assignment| {
                *counts.entry(assignment.node_id).or_default() += 1;
                counts
            },
        );
        assert_eq!(by_node.get("node-a").copied(), Some(2));
        assert_eq!(by_node.get("node-b").copied(), Some(2));
    }

    #[test]
    fn fragment_assignment_is_stable_after_sorting() {
        let left = assign_fragments_to_nodes(
            &["b".to_string(), "a".to_string(), "c".to_string()],
            &[node("node-b"), node("node-a")],
        );
        let right = assign_fragments_to_nodes(
            &["c".to_string(), "b".to_string(), "a".to_string()],
            &[node("node-a"), node("node-b")],
        );
        let left_pairs = left
            .into_iter()
            .map(|assignment| (assignment.fragment, assignment.node_id))
            .collect::<Vec<_>>();
        let right_pairs = right
            .into_iter()
            .map(|assignment| (assignment.fragment, assignment.node_id))
            .collect::<Vec<_>>();
        assert_eq!(left_pairs, right_pairs);
    }
}
