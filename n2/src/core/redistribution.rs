use crate::core::disk_manager::ObjectMetadata;
use crate::distribution::membership::active_cluster_runtime;
use crate::distribution::rebalance::{
    ClusterRebalanceRequest, ClusterRewriteJobStatus, object_layout_status, rebalance_status,
    start_rebalance, stop_rebalance,
};
use crate::enums::app_error_enum::AppError;
use crate::io_control::IoPriority;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::manager::WsConnectionManager;
use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use chrono::Utc;
use serde::Serialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::time::{Duration, sleep};

#[derive(Debug, Clone, Serialize, Default)]
pub struct RedistributionJobStatus {
    pub is_running: bool,
    pub phase: String,
    pub fast_mode: bool,
    pub durability_mode: String,
    pub scope: String,
    pub total_fragments_scanned: u64,
    pub total_objects_scanned: u64,
    pub total_objects_need_redistribution: u64,
    pub total_objects_migrated: u64,
    pub total_errors: u64,
    pub current_fragment: String,
    pub last_object: Option<String>,
    pub last_error: Option<String>,
    pub start_timestamp: i64,
    pub duration_seconds: u64,
    pub scan_rate_per_sec: f64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct RedistributionIntentDebtSnapshot {
    pub pending_intents: u64,
    pub failed_intents: u64,
    pub fragments_with_pending_intents: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_pending_intent_age_seconds: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
pub enum RedistributionDurabilityMode {
    Auto,
    Strict,
    Batched,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RedistributionExecutionMode {
    #[default]
    Availability,
    Strict,
    Scrub,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RedistributionScope {
    #[default]
    All,
    Blob,
    Index,
}

#[derive(Clone, Debug)]
pub struct RedistributionRunOptions {
    pub fast_mode: bool,
    pub execution_mode: RedistributionExecutionMode,
    pub scope: RedistributionScope,
    pub target_fragment: Option<String>,
    pub durability_mode: RedistributionDurabilityMode,
    pub object_concurrency: usize,
    pub status_batch: u64,
}

impl Default for RedistributionRunOptions {
    fn default() -> Self {
        Self {
            fast_mode: false,
            execution_mode: RedistributionExecutionMode::Availability,
            scope: RedistributionScope::All,
            target_fragment: None,
            durability_mode: RedistributionDurabilityMode::Auto,
            object_concurrency: 1,
            status_batch: 64,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RedistributionNeeds {
    pub blob: bool,
    pub index: bool,
}

fn bool_param(params: &HashMap<String, String>, key: &str, default: bool) -> bool {
    params
        .get(key)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

fn legacy_status(status: ClusterRewriteJobStatus) -> RedistributionJobStatus {
    let started = status.started_at_unix.unwrap_or_default();
    let finished = status
        .finished_at_unix
        .unwrap_or_else(|| Utc::now().timestamp());
    let duration_seconds = if started > 0 {
        finished.saturating_sub(started) as u64
    } else {
        0
    };
    let needs = status
        .rewritten
        .saturating_add(status.skipped_current_layout);
    RedistributionJobStatus {
        is_running: status.running,
        phase: if status.running {
            "Rewriting mismatched object layouts".into()
        } else if status.failed > 0 {
            "Completed with errors".into()
        } else if status.finished_at_unix.is_some() {
            "Completed".into()
        } else {
            "Idle".into()
        },
        fast_mode: false,
        durability_mode: "immutable-object-publish".into(),
        scope: "object".into(),
        total_fragments_scanned: u64::from(status.scanned > 0),
        total_objects_scanned: status.scanned,
        total_objects_need_redistribution: needs,
        total_objects_migrated: status.rewritten,
        total_errors: status.failed,
        current_fragment: status.fragment,
        last_object: status.last_object,
        last_error: status.last_error,
        start_timestamp: started,
        duration_seconds,
        scan_rate_per_sec: if duration_seconds == 0 {
            0.0
        } else {
            status.scanned as f64 / duration_seconds as f64
        },
    }
}

pub async fn get_redist_status() -> RedistributionJobStatus {
    legacy_status(rebalance_status())
}

pub async fn get_redist_status_api(
    claims: Claims,
) -> Result<Json<RedistributionJobStatus>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    Ok(Json(get_redist_status().await))
}

pub async fn trigger_redistribution_job_api(
    claims: Claims,
    State(_state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write || !claims.modify || !claims.delete {
        return Err(AppError::Forbidden(
            "Write, Modify and Delete permissions required".into(),
        ));
    }
    if params
        .get("scope")
        .is_some_and(|scope| scope.eq_ignore_ascii_case("index"))
    {
        return Err(AppError::BadRequest(
            "N2 has no replicated object-index plane; redistribution rewrites immutable object layouts"
                .into(),
        ));
    }
    let fragment = params
        .get("fragment")
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or(&claims.fragment)
        .to_string();
    let request = ClusterRebalanceRequest {
        fragment,
        prefix: params.get("prefix").cloned(),
        limit: params.get("limit").and_then(|value| value.parse().ok()),
        dry_run: bool_param(&params, "dry_run", false),
        mismatched_only: bool_param(&params, "mismatched_only", true),
        max_duration_seconds: params
            .get("max_duration_seconds")
            .and_then(|value| value.parse().ok()),
        max_objects: params
            .get("max_objects")
            .and_then(|value| value.parse().ok()),
        max_bytes: params.get("max_bytes").and_then(|value| value.parse().ok()),
        max_directories: params
            .get("max_directories")
            .and_then(|value| value.parse().ok()),
        continuation_token: params.get("continuation_token").cloned(),
        priority: Some(if bool_param(&params, "fast_mode", false) {
            IoPriority::Normal
        } else {
            IoPriority::Low
        }),
    };
    start_rebalance(request)?;
    Ok((StatusCode::ACCEPTED, "Object redistribution initiated.").into_response())
}

pub async fn stop_redistribution_job_api(
    claims: Claims,
    State(_state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write || !claims.modify || !claims.delete {
        return Err(AppError::Forbidden(
            "Write, Modify and Delete permissions required".into(),
        ));
    }
    stop_rebalance();
    Ok((StatusCode::OK, "Stop signal sent.").into_response())
}

pub async fn collect_redistribution_intent_debt(
    _index_roots: &[PathBuf],
    _fragment: Option<String>,
) -> RedistributionIntentDebtSnapshot {
    // N2 1.0 keeps one resumable cluster job journal under .n2/jobs. There are no
    // per-index-root redistribution intents to drift or require reconciliation.
    RedistributionIntentDebtSnapshot::default()
}

pub async fn replay_pending_intents_for_fragment_now(
    _fragment_hash: &str,
    _strict_durability: bool,
) -> Result<(), AppError> {
    Ok(())
}

pub(crate) async fn check_needs_redistribution(
    meta: &ObjectMetadata,
    scope: RedistributionScope,
    target_data: usize,
    target_parity: usize,
    target_stripe_size: usize,
    _target_num_disks: usize,
    _target_blob_roots: &[PathBuf],
    _target_index_roots: &[PathBuf],
    _is_trash: bool,
) -> RedistributionNeeds {
    if scope == RedistributionScope::Index {
        return RedistributionNeeds::default();
    }
    if let Ok(runtime) = active_cluster_runtime()
        && let Ok(status) = object_layout_status(meta, &runtime)
    {
        return RedistributionNeeds {
            blob: !(status.layout_matches_current && status.placement_matches_current),
            index: false,
        };
    }

    let recorded_data = usize::from(meta.data_shards);
    let recorded_parity = usize::from(meta.parity_shards);
    let missing = meta
        .parts
        .iter()
        .flat_map(|part| &part.shards)
        .any(|shard| {
            let path = Path::new(&shard.absolute_path);
            !path.exists() && !shard.absolute_path.starts_with("n2://")
        });
    RedistributionNeeds {
        blob: recorded_data != target_data
            || recorded_parity != target_parity
            || meta.stripe_size_bytes != target_stripe_size
            || missing,
        index: false,
    }
}

pub async fn run_redistribution_job_with_options(
    dry_run: bool,
    opts: RedistributionRunOptions,
    _ws_manager: Arc<WsConnectionManager>,
) -> Result<(), AppError> {
    if opts.scope == RedistributionScope::Index {
        return Err(AppError::BadRequest(
            "N2 has no object-index redistribution scope".into(),
        ));
    }
    let fragment = opts.target_fragment.ok_or_else(|| {
        AppError::BadRequest("fragment is required for N2 object redistribution".into())
    })?;
    start_rebalance(ClusterRebalanceRequest {
        fragment,
        prefix: None,
        limit: None,
        dry_run,
        mismatched_only: true,
        max_duration_seconds: None,
        max_objects: None,
        max_bytes: None,
        max_directories: None,
        continuation_token: None,
        priority: Some(IoPriority::Low),
    })?;
    loop {
        let status = rebalance_status();
        if !status.running {
            return if status.failed == 0 {
                Ok(())
            } else {
                Err(AppError::InternalServerError(
                    status
                        .last_error
                        .unwrap_or_else(|| "object redistribution failed".into()),
                ))
            };
        }
        sleep(Duration::from_millis(50)).await;
    }
}

#[cfg(test)]
pub async fn run_redistribution_job(
    dry_run: bool,
    ws_manager: Arc<WsConnectionManager>,
) -> Result<(), AppError> {
    run_redistribution_job_with_options(dry_run, RedistributionRunOptions::default(), ws_manager)
        .await
}
