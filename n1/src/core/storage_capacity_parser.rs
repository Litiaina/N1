use axum::{Json, extract::Path as AxumPath, response::IntoResponse};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{
    core::{
        disk_manager::CapacityUsageResponse,
        fragment_metadata::resolve_stable_fragment_id,
        index_store,
        quota_manager::{
            FragmentBlobUsage, apply_fragment_project_quota, fragment_quota_backend,
            fragment_quota_enforcement_enabled, get_fragment_blob_usage, get_storage_usage,
        },
    },
    enums::app_error_enum::AppError,
    middleware::auth_middleware::MASTER_KEY,
    structs::auth_structs::Claims,
};

static FRAGMENT_QUOTA_RESIZE_LOCK: Mutex<()> = Mutex::const_new(());

#[derive(Debug, Serialize)]
pub struct FragmentQuotaUsageResponse {
    pub fragment: String,
    pub state: String,
    pub quota_bytes: u64,
    pub committed_bytes: u64,
    pub reserved_bytes: u64,
    pub trash_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
    pub usage_percent: f64,
    pub quota_enforced: bool,
    pub quota_backend: &'static str,
    pub created_at_unix: i64,
    pub updated_at_unix: i64,
}

#[derive(Debug, Deserialize)]
pub struct ResizeFragmentQuotaRequest {
    pub quota_bytes: u64,
    pub secret: String,
}

#[derive(Debug, Serialize)]
pub struct ResizeFragmentQuotaResponse {
    pub fragment: String,
    pub previous_quota_bytes: u64,
    pub quota_bytes: u64,
    pub committed_bytes: u64,
    pub reserved_bytes: u64,
    pub trash_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
    pub usage_percent: f64,
    pub active_quota_bytes: u64,
    pub capacity_allocated: u64,
    pub quota_enforced: bool,
    pub quota_backend: &'static str,
    pub changed: bool,
    pub direction: String,
}

fn quota_usage_from_meta(
    fragment: String,
    meta: index_store::FragmentMeta,
    blob_usage: FragmentBlobUsage,
) -> FragmentQuotaUsageResponse {
    let used_bytes = blob_usage.used_bytes();
    let available_bytes = meta.quota_bytes.saturating_sub(used_bytes);
    let usage_percent = if meta.quota_bytes > 0 {
        (used_bytes as f64 / meta.quota_bytes as f64) * 100.0
    } else {
        0.0
    };

    FragmentQuotaUsageResponse {
        fragment,
        state: meta.state,
        quota_bytes: meta.quota_bytes,
        committed_bytes: blob_usage.live_bytes,
        reserved_bytes: 0,
        trash_bytes: blob_usage.trash_bytes,
        used_bytes,
        available_bytes,
        usage_percent,
        quota_enforced: fragment_quota_enforcement_enabled(),
        quota_backend: fragment_quota_backend().public_name(),
        created_at_unix: meta.created_at_unix,
        updated_at_unix: meta.updated_at_unix,
    }
}

pub async fn get_capacity_usage(
    claims: Claims,
    AxumPath(fragment): AxumPath<String>,
) -> Result<impl IntoResponse, AppError> {
    if claims.read == false {
        return Err(AppError::Forbidden("Read permission required".into()));
    }

    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let usage = get_storage_usage().await?;

    Ok(Json(CapacityUsageResponse {
        capacity_used: usage.capacity_used,
        capacity_allocated: usage.capacity_allocated,
        capacity_available: usage.capacity_available,
        filesystem_count: usage.filesystem_count,
    }))
}

pub async fn get_fragment_quota_usage(claims: Claims) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }

    let fragment = claims.fragment.clone();
    let lookup_fragment = fragment.clone();
    let meta =
        tokio::task::spawn_blocking(move || index_store::fragment_quota_meta(&lookup_fragment))
            .await
            .map_err(|err| AppError::InternalServerError(format!("quota usage worker: {err}")))??;

    let Some(meta) = meta else {
        return Err(AppError::NotFound);
    };

    let blob_usage = get_fragment_blob_usage(&fragment).await?;

    Ok(Json(quota_usage_from_meta(fragment, meta, blob_usage)))
}

pub async fn resize_fragment_quota(
    claims: Claims,
    AxumPath(fragment): AxumPath<String>,
    Json(payload): Json<ResizeFragmentQuotaRequest>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = resolve_stable_fragment_id(&fragment).await?;

    if payload.secret != *MASTER_KEY {
        return Err(AppError::Forbidden(
            "Master Secret required to resize fragment quota".into(),
        ));
    }

    if payload.quota_bytes == 0 {
        return Err(AppError::BadRequest(
            "quota_bytes must be greater than 0".to_string(),
        ));
    }

    if claims.fragment != fragment && !claims.modify {
        return Err(AppError::Forbidden(
            "Modify permission required to resize fragment quota".into(),
        ));
    }

    let _guard = FRAGMENT_QUOTA_RESIZE_LOCK.lock().await;
    let lookup_fragment = fragment.clone();
    let meta =
        tokio::task::spawn_blocking(move || index_store::fragment_quota_meta(&lookup_fragment))
            .await
            .map_err(|err| {
                AppError::InternalServerError(format!("quota resize worker: {err}"))
            })??;

    let Some(meta) = meta else {
        return Err(AppError::NotFound);
    };

    if meta.state != "active" {
        return Err(AppError::Conflict(format!(
            "fragment quota can only be resized while active; current state is {}",
            meta.state
        )));
    }

    let previous_quota_bytes = meta.quota_bytes;
    let blob_usage = get_fragment_blob_usage(&fragment).await?;
    let used_bytes = blob_usage.used_bytes();

    if payload.quota_bytes < used_bytes {
        return Err(AppError::Conflict(format!(
            "new quota_bytes {} is below current blob filesystem used_bytes {} (blob {} + trash {})",
            payload.quota_bytes, used_bytes, blob_usage.live_bytes, blob_usage.trash_bytes
        )));
    }

    let usage = get_storage_usage().await?;
    let active_quota_bytes = tokio::task::spawn_blocking(index_store::active_fragment_quota_bytes)
        .await
        .map_err(|err| AppError::InternalServerError(format!("quota total worker: {err}")))??;

    let next_active_quota_bytes = active_quota_bytes
        .saturating_sub(previous_quota_bytes)
        .saturating_add(payload.quota_bytes);

    if payload.quota_bytes > previous_quota_bytes
        && next_active_quota_bytes > usage.capacity_allocated
    {
        return Err(AppError::InsufficientStorage);
    }

    let changed = payload.quota_bytes != previous_quota_bytes;
    if changed {
        let project_fragment = fragment.clone();
        let project_id = tokio::task::spawn_blocking(move || {
            index_store::ensure_fragment_quota_project_id(&project_fragment)
        })
        .await
        .map_err(|err| AppError::InternalServerError(format!("quota project worker: {err}")))??;

        apply_fragment_project_quota(&fragment, payload.quota_bytes, project_id).await?;

        let fragment_for_update = fragment.clone();
        let quota_bytes = payload.quota_bytes;
        if let Err(err) = tokio::task::spawn_blocking(move || {
            index_store::set_fragment_quota(&fragment_for_update, quota_bytes)
        })
        .await
        .map_err(|err| AppError::InternalServerError(format!("quota update worker: {err}")))?
        {
            if fragment_quota_enforcement_enabled() {
                let _ =
                    apply_fragment_project_quota(&fragment, previous_quota_bytes, project_id).await;
            }
            return Err(err);
        }
    }

    let available_bytes = payload.quota_bytes.saturating_sub(used_bytes);
    let usage_percent = if payload.quota_bytes > 0 {
        (used_bytes as f64 / payload.quota_bytes as f64) * 100.0
    } else {
        0.0
    };
    let direction = if payload.quota_bytes > previous_quota_bytes {
        "increase"
    } else if payload.quota_bytes < previous_quota_bytes {
        "decrease"
    } else {
        "unchanged"
    }
    .to_string();

    Ok(Json(ResizeFragmentQuotaResponse {
        fragment,
        previous_quota_bytes,
        quota_bytes: payload.quota_bytes,
        committed_bytes: blob_usage.live_bytes,
        reserved_bytes: 0,
        trash_bytes: blob_usage.trash_bytes,
        used_bytes,
        available_bytes,
        usage_percent,
        active_quota_bytes: next_active_quota_bytes,
        capacity_allocated: usage.capacity_allocated,
        quota_enforced: fragment_quota_enforcement_enabled(),
        quota_backend: fragment_quota_backend().public_name(),
        changed,
        direction,
    }))
}
