use crate::distribution::health::cluster_health_snapshot;
use crate::distribution::maintenance::{
    distributed_maintenance_status, start_distributed_maintenance,
};
use crate::distribution::membership::{
    ClusterNode, active_cluster_runtime, live_active_write_nodes,
};
use crate::distribution::monitor::{ClusterMonitorQuery, cluster_monitor_snapshot};
use crate::distribution::peer::{
    PeerUsageResponse, cluster_secret, local_peer_usage_snapshot_refresh, peer_http_client,
};
use crate::distribution::placement::build_placement_plan;
use crate::distribution::rebalance::{
    ClusterRebalanceRequest, plan_rebalance, rebalance_status, repair_status, start_rebalance,
    start_repair, stop_rebalance, stop_repair,
};
use crate::distribution::repair::repair_policy;
use crate::distribution::resolve_layout;
use crate::enums::app_error_enum::AppError;
use crate::structs::auth_structs::Claims;
use axum::extract::{Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{Duration, timeout};

use crate::routes::storage_routes::AppState;

#[derive(Clone, Debug, Deserialize)]
pub struct PlacementPreviewQuery {
    pub fragment: String,
    pub object_key: String,
    #[serde(default)]
    pub version_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ClusterUsageQuery {
    #[serde(default)]
    pub fragment: Option<String>,
    #[serde(default)]
    pub refresh: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ClusterNodeUsage {
    pub node_id: String,
    pub endpoint: String,
    pub state: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<PeerUsageResponse>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ClusterUsageResponse {
    pub membership_epoch: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fragment: Option<String>,
    pub capacity_admission_enabled: bool,
    pub nodes: Vec<ClusterNodeUsage>,
    pub cluster_capacity_total_bytes: u64,
    pub cluster_capacity_used_bytes: u64,
    pub cluster_capacity_available_bytes: u64,
    pub fragment_live_bytes: u64,
    pub fragment_trash_bytes: u64,
    pub fragment_physical_bytes: u64,
}

pub async fn cluster_membership(
    claims: Claims,
) -> Result<impl axum::response::IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let runtime = active_cluster_runtime()?;
    Ok(Json(serde_json::json!({
        "local_node_id": runtime.local_node_id,
        "membership_path": runtime.membership_path,
        "membership": runtime.membership,
    })))
}

pub async fn cluster_layout(claims: Claims) -> Result<impl axum::response::IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let runtime = active_cluster_runtime()?;
    let active = live_active_write_nodes(&runtime).await;
    let layout =
        resolve_layout(active.len(), &runtime.layout_policy).map_err(AppError::BadRequest)?;
    Ok(Json(serde_json::json!({
        "membership_epoch": runtime.membership.epoch,
        "policy": runtime.layout_policy,
        "active_write_nodes": active.len(),
        "layout": layout,
        "bounded_object_erasure_set": true,
        "max_auto_data_shards": runtime.layout_policy.auto_max_data_shards,
        "max_auto_parity_shards": runtime.layout_policy.auto_max_parity_shards,
        "adaptive_without_rebalance": true,
        "automatic_rebalance": runtime.auto_rebalance,
    })))
}

pub async fn cluster_health(claims: Claims) -> Result<impl axum::response::IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let runtime = active_cluster_runtime()?;
    Ok(Json(cluster_health_snapshot(&runtime).await?))
}

pub async fn cluster_monitor(
    claims: Claims,
    State(state): State<Arc<AppState>>,
    Query(query): Query<ClusterMonitorQuery>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    Ok(Json(cluster_monitor_snapshot(state, claims, query).await?))
}

async fn usage_for_node(
    local_node_id: String,
    node: ClusterNode,
    fragment: Option<String>,
    refresh: bool,
) -> ClusterNodeUsage {
    let state = format!("{:?}", node.effective_state()).to_ascii_lowercase();
    let endpoint = node.endpoint.clone();
    let result = if node.id == local_node_id {
        local_peer_usage_snapshot_refresh(fragment, refresh).await
    } else {
        fetch_remote_usage(&node.endpoint, fragment, refresh).await
    };
    match result {
        Ok(usage) => ClusterNodeUsage {
            node_id: node.id,
            endpoint,
            state,
            ok: true,
            error: None,
            usage: Some(usage),
        },
        Err(err) => ClusterNodeUsage {
            node_id: node.id,
            endpoint,
            state,
            ok: false,
            error: Some(err.to_string()),
            usage: None,
        },
    }
}

async fn fetch_remote_usage(
    endpoint: &str,
    fragment: Option<String>,
    refresh: bool,
) -> Result<PeerUsageResponse, AppError> {
    let mut url = format!("{}/n2/peer/usage", endpoint.trim_end_matches('/'));
    let mut separator = "?";
    if let Some(fragment) = fragment.as_deref().filter(|value| !value.trim().is_empty()) {
        url.push_str(separator);
        url.push_str("fragment=");
        url.push_str(&utf8_percent_encode(fragment, NON_ALPHANUMERIC).to_string());
        separator = "&";
    }
    if refresh {
        url.push_str(separator);
        url.push_str("refresh=true");
    }
    let client = peer_http_client()?;
    let mut req = client.get(url);
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(Duration::from_secs(2), req.send())
        .await
        .map_err(|_| AppError::ServiceUnavailable("N2 peer usage request timed out".into()))?
        .map_err(|err| {
            AppError::ServiceUnavailable(format!("N2 peer usage request failed: {err}"))
        })?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "N2 peer usage returned {status}: {text}"
        )));
    }
    response.json::<PeerUsageResponse>().await.map_err(|err| {
        AppError::InternalServerError(format!("decode N2 peer usage response: {err}"))
    })
}

pub async fn cluster_usage(
    claims: Claims,
    Query(query): Query<ClusterUsageQuery>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    Ok(Json(
        cluster_usage_snapshot(claims, query.fragment, query.refresh).await?,
    ))
}

pub async fn cluster_usage_snapshot(
    claims: Claims,
    fragment: Option<String>,
    refresh: bool,
) -> Result<ClusterUsageResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let runtime = active_cluster_runtime()?;
    let fragment = fragment.or_else(|| Some(claims.fragment.clone()));
    let mut nodes = Vec::new();
    for node in runtime.membership.readable_nodes() {
        nodes.push(usage_for_node(
            runtime.local_node_id.clone(),
            node,
            fragment.clone(),
            refresh,
        ));
    }
    let nodes = futures_util::future::join_all(nodes).await;
    let mut cluster_capacity_total_bytes = 0u64;
    let mut cluster_capacity_used_bytes = 0u64;
    let mut cluster_capacity_available_bytes = 0u64;
    let mut fragment_live_bytes = 0u64;
    let mut fragment_trash_bytes = 0u64;
    for node in &nodes {
        if let Some(usage) = &node.usage {
            cluster_capacity_total_bytes =
                cluster_capacity_total_bytes.saturating_add(usage.capacity.capacity_allocated);
            cluster_capacity_used_bytes =
                cluster_capacity_used_bytes.saturating_add(usage.capacity.capacity_used);
            cluster_capacity_available_bytes =
                cluster_capacity_available_bytes.saturating_add(usage.capacity.capacity_available);
            if let Some(fragment_usage) = usage.fragment_usage {
                fragment_live_bytes = fragment_live_bytes.saturating_add(fragment_usage.live_bytes);
                fragment_trash_bytes =
                    fragment_trash_bytes.saturating_add(fragment_usage.trash_bytes);
            }
        }
    }
    Ok(ClusterUsageResponse {
        membership_epoch: runtime.membership.epoch,
        fragment,
        capacity_admission_enabled: true,
        nodes,
        cluster_capacity_total_bytes,
        cluster_capacity_used_bytes,
        cluster_capacity_available_bytes,
        fragment_live_bytes,
        fragment_trash_bytes,
        fragment_physical_bytes: fragment_live_bytes.saturating_add(fragment_trash_bytes),
    })
}

pub async fn placement_preview(
    claims: Claims,
    Query(query): Query<PlacementPreviewQuery>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let runtime = active_cluster_runtime()?;
    let active = live_active_write_nodes(&runtime).await;
    let layout =
        resolve_layout(active.len(), &runtime.layout_policy).map_err(AppError::BadRequest)?;
    let version = query.version_id.unwrap_or_else(|| "preview".to_string());
    let plan = build_placement_plan(
        &query.fragment,
        &query.object_key,
        &version,
        runtime.membership.epoch,
        layout,
        &active,
    )?;
    Ok(Json(plan))
}

pub async fn rebalance_plan(claims: Claims) -> Result<impl axum::response::IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let runtime = active_cluster_runtime()?;
    Ok(Json(plan_rebalance(&runtime)?))
}

pub async fn rebalance_start(
    claims: Claims,
    Json(request): Json<ClusterRebalanceRequest>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden("Modify permission required".into()));
    }
    Ok(Json(start_rebalance(request)?))
}

pub async fn rebalance_status_route(
    claims: Claims,
) -> Result<impl axum::response::IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    Ok(Json(rebalance_status()))
}

pub async fn rebalance_stop(claims: Claims) -> Result<impl axum::response::IntoResponse, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden("Modify permission required".into()));
    }
    Ok(Json(stop_rebalance()))
}

pub async fn repair_plan(claims: Claims) -> Result<impl axum::response::IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let runtime = active_cluster_runtime()?;
    Ok(Json(repair_policy(&runtime)))
}

pub async fn repair_start(
    claims: Claims,
    Json(request): Json<ClusterRebalanceRequest>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden("Modify permission required".into()));
    }
    Ok(Json(start_repair(request)?))
}

pub async fn repair_status_route(
    claims: Claims,
) -> Result<impl axum::response::IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    Ok(Json(repair_status()))
}

pub async fn repair_stop(claims: Claims) -> Result<impl axum::response::IntoResponse, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden("Modify permission required".into()));
    }
    Ok(Json(stop_repair()))
}

pub async fn reload_membership(
    claims: Claims,
    Query(_params): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden("Modify permission required".into()));
    }
    Err(AppError::ServiceUnavailable(
        "membership reload is intentionally disabled until safe live epoch transition is implemented; restart N2 after editing the membership file".into(),
    ))
}

pub fn build_cluster_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/n2/cluster/membership", get(cluster_membership))
        .route("/n2/cluster/membership/reload", post(reload_membership))
        .route("/n2/cluster/layout", get(cluster_layout))
        .route("/n2/cluster/health", get(cluster_health))
        .route("/n2/cluster/monitor", get(cluster_monitor))
        .route("/n2/cluster/usage", get(cluster_usage))
        .route("/n2/cluster/placement/preview", get(placement_preview))
        .route("/n2/cluster/rebalance/plan", get(rebalance_plan))
        .route("/n2/cluster/rebalance/start", post(rebalance_start))
        .route("/n2/cluster/rebalance/stop", post(rebalance_stop))
        .route("/n2/cluster/rebalance/status", get(rebalance_status_route))
        .route("/n2/cluster/repair/plan", get(repair_plan))
        .route("/n2/cluster/repair/start", post(repair_start))
        .route("/n2/cluster/repair/stop", post(repair_stop))
        .route("/n2/cluster/repair/status", get(repair_status_route))
        .route(
            "/n2/cluster/maintenance/distributed/start",
            post(start_distributed_maintenance),
        )
        .route(
            "/n2/cluster/maintenance/distributed/status",
            get(distributed_maintenance_status),
        )
}
