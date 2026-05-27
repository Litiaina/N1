use crate::distribution::health::cluster_health_snapshot;
use crate::distribution::membership::active_cluster_runtime;
use crate::distribution::placement::build_placement_plan;
use crate::distribution::rebalance::{
    ClusterRebalanceRequest, plan_rebalance, rebalance_status, repair_status, start_rebalance,
    start_repair,
};
use crate::distribution::repair::repair_policy;
use crate::distribution::resolve_layout;
use crate::enums::app_error_enum::AppError;
use crate::structs::auth_structs::Claims;
use axum::extract::Query;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

use crate::routes::storage_routes::AppState;

#[derive(Clone, Debug, Deserialize)]
pub struct PlacementPreviewQuery {
    pub fragment: String,
    pub object_key: String,
    #[serde(default)]
    pub version_id: Option<String>,
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
    let active = runtime.membership.active_write_nodes();
    let layout =
        resolve_layout(active.len(), &runtime.layout_policy).map_err(AppError::BadRequest)?;
    Ok(Json(serde_json::json!({
        "membership_epoch": runtime.membership.epoch,
        "policy": runtime.layout_policy,
        "active_write_nodes": active.len(),
        "layout": layout,
        "adaptive_without_rebalance": true,
        "automatic_rebalance": runtime.auto_rebalance,
    })))
}

pub async fn cluster_health(claims: Claims) -> Result<impl axum::response::IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let runtime = active_cluster_runtime()?;
    Ok(Json(cluster_health_snapshot(&runtime)?))
}

pub async fn placement_preview(
    claims: Claims,
    Query(query): Query<PlacementPreviewQuery>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let runtime = active_cluster_runtime()?;
    let active = runtime.membership.active_write_nodes();
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
        .route("/n2/cluster/placement/preview", get(placement_preview))
        .route("/n2/cluster/rebalance/plan", get(rebalance_plan))
        .route("/n2/cluster/rebalance/start", post(rebalance_start))
        .route("/n2/cluster/rebalance/status", get(rebalance_status_route))
        .route("/n2/cluster/repair/plan", get(repair_plan))
        .route("/n2/cluster/repair/start", post(repair_start))
        .route("/n2/cluster/repair/status", get(repair_status_route))
}
