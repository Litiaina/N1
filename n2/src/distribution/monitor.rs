use crate::core::garbage_collector::get_gc_status;
use crate::core::index;
use crate::core::repair_manager::{ObjectLayoutStatus, ObjectRepairStatus, RepairCheckMode};
use crate::distribution::auto_healing::{AutoHealingStatus, auto_healing_status_snapshot};
use crate::distribution::health::{ClusterHealthSnapshot, cluster_health_snapshot};
use crate::distribution::layout::resolve_write_layout;
use crate::distribution::membership::{
    ClusterNode, active_cluster_runtime, live_newest_pool_write_nodes, live_readable_nodes,
};
use crate::distribution::peer::{
    PeerMonitorResponse, cluster_secret, local_peer_monitor_snapshot, peer_http_client,
};
use crate::distribution::rebalance::{
    ClusterRebalancePlan, ClusterRewriteJobStatus, object_layout_status, plan_rebalance,
    rebalance_status, repair_status,
};
use crate::distribution::status::inspect_cluster_object_status;
use crate::enums::app_error_enum::AppError;
use crate::io_control::{IoControlSnapshot, io_control_snapshot};
use crate::namespace::runtime::global_namespace_runtime;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tokio::time::{Duration, timeout};

const PEER_MONITOR_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_OBJECT_SAMPLE_LIMIT: usize = 64;
const MAX_OBJECT_SAMPLE_LIMIT: usize = 512;
const MAX_OBJECT_SAMPLES_RETURNED: usize = 16;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ClusterMonitorQuery {
    #[serde(default)]
    pub fragment: Option<String>,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub object_limit: Option<usize>,
    #[serde(default)]
    pub probe_objects: bool,
    #[serde(default)]
    pub refresh: bool,
    #[serde(default)]
    pub deep_scan: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ClusterMonitorSnapshot {
    pub sampled_at_unix: i64,
    pub overview: ClusterMonitorOverview,
    pub nodes: Vec<ClusterMonitorNode>,
    pub topology: ClusterMonitorTopology,
    pub object_health: ClusterObjectHealthSummary,
    pub io_control: IoControlSnapshot,
    pub redistribution: ClusterRedistributionSummary,
    pub jobs: ClusterJobsSummary,
    pub namespace_replication: ClusterNamespaceReplicationSummary,
    pub uploads: ClusterUploadActivitySummary,
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ClusterMonitorOverview {
    pub state: String,
    pub membership_epoch: u64,
    pub cluster_id: String,
    pub local_node_id: String,
    pub active_write_nodes: usize,
    pub readable_nodes: usize,
    pub configured_nodes: usize,
    pub data_shards: usize,
    pub parity_shards: usize,
    pub data_plane_readable: bool,
    pub data_plane_writable: bool,
    pub ncnx_strictly_writable: bool,
    pub effective_client_writable: bool,
    pub automatic_rebalance: bool,
    pub layout_drift_is_health_failure: bool,
    pub summary: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ClusterMonitorNode {
    pub node_id: String,
    pub endpoint: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub native_endpoint: Option<String>,
    pub configured_state: String,
    pub effective_state: String,
    pub accepts_writes: bool,
    pub readable: bool,
    pub reachable: bool,
    pub local: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen_unix: Option<i64>,
    pub storage_roots: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uploads: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub io_control: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maintenance: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disks: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub monitor_error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ClusterMonitorTopology {
    pub membership_epoch: u64,
    pub static_data_shards: usize,
    pub static_parity_shards: usize,
    pub current_data_shards: usize,
    pub current_parity_shards: usize,
    pub current_total_shards: usize,
    pub active_write_nodes: usize,
    pub readable_nodes: usize,
    pub ncnx_replicas: usize,
    pub estimated_ncnx_replicas_reachable: usize,
    pub ncnx_replicas_required: usize,
    pub object_level_erasure_layout: bool,
    pub bounded_object_erasure_set: bool,
    pub old_layouts_remain_valid: bool,
    pub manual_redistribution_only: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ClusterObjectHealthSummary {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fragment: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    pub sample_limit: usize,
    pub scanned: u64,
    pub current_layout: u64,
    pub older_recorded_layout: u64,
    pub placement_mismatch: u64,
    pub redistributable: u64,
    pub probed_objects: u64,
    pub healthy: u64,
    pub degraded: u64,
    pub critical: u64,
    pub lost: u64,
    pub unknown_health: u64,
    pub repairable: u64,
    pub missing_shards: u64,
    pub corrupt_shards: u64,
    pub unavailable_shards: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub continuation_token: Option<String>,
    pub samples: Vec<ClusterObjectHealthSample>,
    pub errors: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ClusterObjectHealthSample {
    pub path: String,
    pub object_id: String,
    pub version_id: String,
    pub layout_state: String,
    pub redistributable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health_status: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub mismatch_reasons: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ClusterRedistributionSummary {
    pub plan: ClusterRebalancePlan,
    pub targeted_rebalance_available: bool,
    pub mismatched_only_default: bool,
    pub already_current_objects_skipped: bool,
    pub sampled_mismatched_objects: u64,
    pub sampled_current_objects: u64,
    pub rebalance: ClusterRewriteJobStatus,
}

#[derive(Clone, Debug, Serialize)]
pub struct ClusterJobsSummary {
    pub repair: ClusterRewriteJobStatus,
    pub rebalance: ClusterRewriteJobStatus,
    pub healing: AutoHealingStatus,
    pub gc: Value,
    pub any_running: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ClusterNamespaceReplicationSummary {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fragment: Option<String>,
    pub replicas: usize,
    pub estimated_replicas_reachable: usize,
    pub replicas_required: usize,
    pub strictly_writable: bool,
    pub local_node_id: String,
    pub local_store: NamespaceLocalStoreSummary,
    pub status: String,
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct NamespaceLocalStoreSummary {
    pub storage_roots: Vec<String>,
    pub configured_replica_limit: usize,
    pub identity_model: &'static str,
    pub transaction_model: &'static str,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ClusterUploadActivitySummary {
    pub active_sessions: u64,
    pub active_fragments: u64,
    pub reserved_bytes: u64,
    pub committed_parts: u64,
    pub committed_bytes: u64,
    pub stale_sessions: u64,
    pub stale_reserved_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_session_age_seconds: Option<u64>,
    pub reporting_nodes: usize,
}

pub async fn cluster_monitor_snapshot(
    state: Arc<AppState>,
    claims: Claims,
    query: ClusterMonitorQuery,
) -> Result<ClusterMonitorSnapshot, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }

    let runtime = active_cluster_runtime()?;
    let health = cluster_health_snapshot(&runtime).await?;
    let active = live_newest_pool_write_nodes(&runtime).await;
    let readable = live_readable_nodes(&runtime).await;
    let layout = resolve_write_layout(
        active.len(),
        runtime.membership.active_newest_pool_nodes().len(),
        &runtime.layout_policy,
    )
    .map_err(AppError::BadRequest)?;
    let plan = plan_rebalance(&runtime)?;
    let fragment = query
        .fragment
        .clone()
        .or_else(|| (!claims.fragment.trim().is_empty()).then_some(claims.fragment.clone()));

    let nodes = monitor_nodes(
        state,
        &runtime,
        &health,
        fragment.clone(),
        query.refresh,
        query.deep_scan,
    )
    .await;
    let uploads = summarize_uploads(&nodes);
    let object_health = sample_object_health(&runtime, fragment.clone(), &query).await;
    let namespace_replication = namespace_replication_summary(&runtime, fragment.clone(), &health);
    let repair = repair_status();
    let rebalance = rebalance_status();
    let healing = auto_healing_status_snapshot().await;
    let gc = serde_json::to_value(get_gc_status().await).unwrap_or(Value::Null);
    let sampled_at_unix = chrono::Utc::now().timestamp();

    let mut notes = health.notes.clone();
    notes.push("cluster monitor is read-only; autonomous healing restores recorded layouts, while usage scans, GC, and rebalance remain explicit operator actions".to_string());
    notes.push("multipart resumability is coordinator/client aware; the gateway only routes and does not own cluster truth".to_string());
    if let Some(fragment) = fragment.as_deref() {
        notes.push(format!(
            "object sample covers fragment '{}' with {} object(s) scanned",
            fragment, object_health.scanned
        ));
    } else {
        notes.push(
            "mount a fragment or pass fragment=... to include object and namespace samples"
                .to_string(),
        );
    }

    Ok(ClusterMonitorSnapshot {
        sampled_at_unix,
        overview: ClusterMonitorOverview {
            state: format!("{:?}", health.state).to_ascii_lowercase(),
            membership_epoch: runtime.membership.epoch,
            cluster_id: runtime.membership.cluster_id.clone(),
            local_node_id: runtime.local_node_id.clone(),
            active_write_nodes: health.active_write_nodes,
            readable_nodes: health.readable_nodes,
            configured_nodes: runtime.membership.nodes.len(),
            data_shards: health.data_shards,
            parity_shards: health.parity_shards,
            data_plane_readable: health.data_plane_readable,
            data_plane_writable: health.data_plane_writable,
            ncnx_strictly_writable: health.ncnx_strictly_writable,
            effective_client_writable: health.effective_client_writable,
            automatic_rebalance: health.automatic_rebalance,
            layout_drift_is_health_failure: health.layout_drift_is_health_failure,
            summary: format!(
                "{} • epoch {} • layout {}+{} • data_writable {} • namespace_writable {} • readable {}",
                format!("{:?}", health.state).to_ascii_lowercase(),
                runtime.membership.epoch,
                health.data_shards,
                health.parity_shards,
                health.data_plane_writable,
                health.ncnx_strictly_writable,
                health.readable_nodes
            ),
        },
        nodes,
        topology: ClusterMonitorTopology {
            membership_epoch: runtime.membership.epoch,
            static_data_shards: runtime.layout_policy.data_shards,
            static_parity_shards: runtime.layout_policy.parity_shards,
            current_data_shards: layout.data_shards,
            current_parity_shards: layout.parity_shards,
            current_total_shards: layout.total_shards(),
            active_write_nodes: active.len(),
            readable_nodes: readable.len(),
            ncnx_replicas: health.ncnx_replicas,
            estimated_ncnx_replicas_reachable: health.estimated_ncnx_replicas_reachable,
            ncnx_replicas_required: health.ncnx_replicas_required,
            object_level_erasure_layout: true,
            bounded_object_erasure_set: true,
            old_layouts_remain_valid: true,
            manual_redistribution_only: !runtime.auto_rebalance,
        },
        object_health: object_health.clone(),
        io_control: io_control_snapshot(),
        redistribution: ClusterRedistributionSummary {
            plan,
            targeted_rebalance_available: true,
            mismatched_only_default: true,
            already_current_objects_skipped: true,
            sampled_mismatched_objects: object_health
                .older_recorded_layout
                .saturating_add(object_health.placement_mismatch),
            sampled_current_objects: object_health.current_layout,
            rebalance,
        },
        jobs: ClusterJobsSummary {
            any_running: repair.running || rebalance_status().running || healing.running,
            repair,
            rebalance: rebalance_status(),
            healing,
            gc,
        },
        namespace_replication,
        uploads,
        notes,
    })
}

async fn monitor_nodes(
    state: Arc<AppState>,
    runtime: &crate::distribution::membership::ClusterRuntime,
    health: &ClusterHealthSnapshot,
    fragment: Option<String>,
    refresh: bool,
    deep_scan: bool,
) -> Vec<ClusterMonitorNode> {
    let health_by_id = health
        .nodes
        .iter()
        .map(|node| (node.node_id.clone(), node.clone()))
        .collect::<std::collections::HashMap<_, _>>();
    let futures = runtime.membership.nodes.iter().cloned().map(|node| {
        let state = state.clone();
        let runtime_local = runtime.local_node_id.clone();
        let fragment = fragment.clone();
        let health_by_id = health_by_id.clone();
        async move {
            let node_id = node.id.clone();
            let peer_monitor = if node.is_local(&runtime_local) {
                local_peer_monitor_snapshot(state, fragment, false, refresh, deep_scan)
                    .await
                    .map_err(|err| err.to_string())
            } else {
                fetch_peer_monitor(&node, fragment, refresh, deep_scan).await
            };
            monitor_node_from_peer(
                node,
                &runtime_local,
                health_by_id.get(&node_id),
                peer_monitor,
            )
        }
    });
    futures_util::future::join_all(futures).await
}

fn monitor_node_from_peer(
    node: ClusterNode,
    local_node_id: &str,
    health: Option<&crate::distribution::health::ClusterNodeHealth>,
    peer_monitor: Result<PeerMonitorResponse, String>,
) -> ClusterMonitorNode {
    let configured_state = format!("{:?}", node.effective_state()).to_ascii_lowercase();
    let effective_state = health
        .map(|health| format!("{:?}", health.state).to_ascii_lowercase())
        .unwrap_or_else(|| configured_state.clone());
    let (reachable, monitor, monitor_error) = match peer_monitor {
        Ok(snapshot) => (true, Some(snapshot), None),
        Err(err) => (false, None, Some(err)),
    };
    let accepts_writes = health
        .map(|health| health.accepts_writes)
        .unwrap_or_else(|| node.effective_state().accepts_new_writes());
    let readable = health
        .map(|health| health.readable)
        .unwrap_or_else(|| node.effective_state().is_readable());
    let health_value = monitor.as_ref().map(|snapshot| &snapshot.health);
    ClusterMonitorNode {
        node_id: node.id.clone(),
        endpoint: node.endpoint,
        native_endpoint: node.native_endpoint,
        configured_state,
        effective_state,
        accepts_writes,
        readable,
        reachable,
        local: node.id == local_node_id,
        last_seen_unix: node.last_seen_unix,
        storage_roots: node
            .storage_roots
            .into_iter()
            .map(|root| root.path)
            .collect(),
        health_status: health_value
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str)
            .map(str::to_string),
        system: health_value.and_then(|value| value.get("system")).cloned(),
        uploads: health_value.and_then(|value| value.get("uploads")).cloned(),
        io_control: health_value
            .and_then(|value| value.get("io_control"))
            .cloned(),
        storage: health_value.and_then(|value| value.get("storage")).cloned(),
        maintenance: health_value
            .and_then(|value| value.get("maintenance"))
            .cloned(),
        disks: health_value.and_then(|value| value.get("disks")).cloned(),
        monitor_error,
    }
}

async fn fetch_peer_monitor(
    node: &ClusterNode,
    fragment: Option<String>,
    refresh: bool,
    deep_scan: bool,
) -> Result<PeerMonitorResponse, String> {
    let mut url = format!("{}/n2/peer/monitor", node.endpoint.trim_end_matches('/'));
    let mut query = Vec::new();
    if let Some(fragment) = fragment.as_deref().filter(|value| !value.trim().is_empty()) {
        query.push(format!(
            "fragment={}",
            utf8_percent_encode(fragment, NON_ALPHANUMERIC)
        ));
    }
    if refresh {
        query.push("refresh=true".to_string());
    }
    if deep_scan {
        query.push("deep_scan=true".to_string());
    }
    if !query.is_empty() {
        url.push('?');
        url.push_str(&query.join("&"));
    }
    let client = peer_http_client().map_err(|err| err.to_string())?;
    let mut req = client.get(url);
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(PEER_MONITOR_TIMEOUT, req.send())
        .await
        .map_err(|_| "peer monitor request timed out".to_string())?
        .map_err(|err| format!("peer monitor request failed: {err}"))?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        return Err(format!("peer monitor returned {status}: {text}"));
    }
    response
        .json::<PeerMonitorResponse>()
        .await
        .map_err(|err| format!("decode peer monitor response: {err}"))
}

async fn sample_object_health(
    runtime: &crate::distribution::membership::ClusterRuntime,
    fragment: Option<String>,
    query: &ClusterMonitorQuery,
) -> ClusterObjectHealthSummary {
    let limit = query
        .object_limit
        .unwrap_or(DEFAULT_OBJECT_SAMPLE_LIMIT)
        .clamp(1, MAX_OBJECT_SAMPLE_LIMIT);
    let Some(fragment) = fragment.filter(|value| !value.trim().is_empty()) else {
        return ClusterObjectHealthSummary {
            sample_limit: limit,
            ..ClusterObjectHealthSummary::default()
        };
    };

    let mut summary = ClusterObjectHealthSummary {
        fragment: Some(fragment.clone()),
        prefix: query.prefix.clone(),
        sample_limit: limit,
        ..ClusterObjectHealthSummary::default()
    };
    let page = match global_namespace_runtime()
        .list_live_object_refs_page(&fragment, query.prefix.as_deref(), None, limit)
        .await
    {
        Ok(page) => page,
        Err(err) => {
            summary.errors.push(err.to_string());
            return summary;
        }
    };
    summary.continuation_token = page.continuation_token;
    for entry in page.entries {
        summary.scanned = summary.scanned.saturating_add(1);
        let meta = match index::read_object_version(&fragment, &entry.object_id, &entry.version_id)
            .await
        {
            Ok(meta) => meta,
            Err(err) => {
                summary.errors.push(format!("{}: {}", entry.path, err));
                summary.unknown_health = summary.unknown_health.saturating_add(1);
                continue;
            }
        };
        let layout_status = match object_layout_status(&meta, runtime) {
            Ok(status) => status,
            Err(err) => {
                summary.errors.push(format!("{}: {}", entry.path, err));
                summary.unknown_health = summary.unknown_health.saturating_add(1);
                continue;
            }
        };
        count_layout(&mut summary, &layout_status);

        let repair_status = if query.probe_objects {
            match inspect_cluster_object_status(&meta, RepairCheckMode::Availability).await {
                Ok(status) => {
                    count_repair_status(&mut summary, &status);
                    Some(status.status)
                }
                Err(err) => {
                    summary.errors.push(format!("{}: {}", entry.path, err));
                    summary.unknown_health = summary.unknown_health.saturating_add(1);
                    None
                }
            }
        } else {
            summary.unknown_health = summary.unknown_health.saturating_add(1);
            None
        };

        if layout_status.redistributable || summary.samples.len() < MAX_OBJECT_SAMPLES_RETURNED {
            push_object_sample(
                &mut summary,
                entry.path,
                entry.object_id,
                entry.version_id,
                layout_status,
                repair_status,
            );
        }
    }
    summary
}

fn count_layout(summary: &mut ClusterObjectHealthSummary, layout: &ObjectLayoutStatus) {
    match layout.state.as_str() {
        "current_layout" => summary.current_layout = summary.current_layout.saturating_add(1),
        "older_recorded_layout" => {
            summary.older_recorded_layout = summary.older_recorded_layout.saturating_add(1)
        }
        "placement_mismatch" => {
            summary.placement_mismatch = summary.placement_mismatch.saturating_add(1)
        }
        _ => {}
    }
    if layout.redistributable {
        summary.redistributable = summary.redistributable.saturating_add(1);
    }
}

fn count_repair_status(summary: &mut ClusterObjectHealthSummary, status: &ObjectRepairStatus) {
    summary.probed_objects = summary.probed_objects.saturating_add(1);
    match status.status.as_str() {
        "healthy" => summary.healthy = summary.healthy.saturating_add(1),
        "degraded" => summary.degraded = summary.degraded.saturating_add(1),
        "critical" => summary.critical = summary.critical.saturating_add(1),
        "lost" => summary.lost = summary.lost.saturating_add(1),
        "incomplete" => summary.unknown_health = summary.unknown_health.saturating_add(1),
        _ => summary.unknown_health = summary.unknown_health.saturating_add(1),
    }
    if status.repairable {
        summary.repairable = summary.repairable.saturating_add(1);
    }
    summary.missing_shards = summary.missing_shards.saturating_add(status.missing_shards);
    summary.corrupt_shards = summary.corrupt_shards.saturating_add(status.corrupt_shards);
    summary.unavailable_shards = summary
        .unavailable_shards
        .saturating_add(status.unavailable_shards);
}

fn push_object_sample(
    summary: &mut ClusterObjectHealthSummary,
    path: String,
    object_id: String,
    version_id: String,
    layout: ObjectLayoutStatus,
    health_status: Option<String>,
) {
    if summary.samples.len() >= MAX_OBJECT_SAMPLES_RETURNED && !layout.redistributable {
        return;
    }
    if summary.samples.len() >= MAX_OBJECT_SAMPLES_RETURNED {
        summary.samples.remove(0);
    }
    summary.samples.push(ClusterObjectHealthSample {
        path,
        object_id,
        version_id,
        layout_state: layout.state,
        redistributable: layout.redistributable,
        health_status,
        mismatch_reasons: layout.mismatch_reasons,
    });
}

fn namespace_replication_summary(
    runtime: &crate::distribution::membership::ClusterRuntime,
    fragment: Option<String>,
    health: &ClusterHealthSnapshot,
) -> ClusterNamespaceReplicationSummary {
    let notes = vec![
        "namespace mutations publish immutable NCNX records to every deterministic replica; no inode consensus database is present".to_string(),
    ];
    let local_store = NamespaceLocalStoreSummary {
        storage_roots: crate::core::disk_manager::ObjectPathManager::new()
            .get_storage_roots()
            .into_iter()
            .map(|root| root.display().to_string())
            .collect(),
        configured_replica_limit: runtime
            .membership
            .newest_writable_pool()
            .map(|pool| pool.policy.namespace_replicas())
            .unwrap_or(0),
        identity_model: "flat_physical_key+ncnx_pointer+deterministic_replica_layout",
        transaction_model: "immutable_destination_first_copy_delete",
    };
    let status = if fragment.is_some() {
        "strict_ncnx_replicas_configured"
    } else {
        "fragment_not_selected"
    }
    .to_string();
    ClusterNamespaceReplicationSummary {
        fragment,
        replicas: health.ncnx_replicas,
        estimated_replicas_reachable: health.estimated_ncnx_replicas_reachable,
        replicas_required: health.ncnx_replicas_required,
        strictly_writable: health.ncnx_strictly_writable,
        local_node_id: runtime.local_node_id.clone(),
        local_store,
        status,
        notes,
    }
}

fn summarize_uploads(nodes: &[ClusterMonitorNode]) -> ClusterUploadActivitySummary {
    let mut summary = ClusterUploadActivitySummary::default();
    for node in nodes {
        let Some(uploads) = node.uploads.as_ref() else {
            continue;
        };
        summary.reporting_nodes = summary.reporting_nodes.saturating_add(1);
        summary.active_sessions = summary
            .active_sessions
            .saturating_add(value_u64(uploads, "active_sessions"));
        summary.active_fragments = summary
            .active_fragments
            .saturating_add(value_u64(uploads, "active_fragments"));
        summary.reserved_bytes = summary
            .reserved_bytes
            .saturating_add(value_u64(uploads, "reserved_bytes"));
        summary.committed_parts = summary
            .committed_parts
            .saturating_add(value_u64(uploads, "committed_parts"));
        summary.committed_bytes = summary
            .committed_bytes
            .saturating_add(value_u64(uploads, "committed_bytes"));
        summary.stale_sessions = summary
            .stale_sessions
            .saturating_add(value_u64(uploads, "stale_sessions"));
        summary.stale_reserved_bytes = summary
            .stale_reserved_bytes
            .saturating_add(value_u64(uploads, "stale_reserved_bytes"));
        if let Some(age) = uploads
            .get("oldest_session_age_seconds")
            .and_then(Value::as_u64)
        {
            summary.oldest_session_age_seconds = Some(
                summary
                    .oldest_session_age_seconds
                    .map(|current| current.max(age))
                    .unwrap_or(age),
            );
        }
    }
    summary
}

fn value_u64(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or(0)
}
