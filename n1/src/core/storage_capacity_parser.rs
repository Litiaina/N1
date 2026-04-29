use axum::{Json, extract::Path as AxumPath, response::IntoResponse};
use serde::Serialize;

use crate::{
    core::{disk_manager::CapacityUsageResponse, indexdb_store, quota_manager::get_storage_usage},
    enums::app_error_enum::AppError,
    structs::auth_structs::Claims,
};

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
    pub created_at_unix: i64,
    pub updated_at_unix: i64,
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
        tokio::task::spawn_blocking(move || indexdb_store::fragment_quota_meta(&lookup_fragment))
            .await
            .map_err(|err| AppError::InternalServerError(format!("quota usage worker: {err}")))??;

    let Some(meta) = meta else {
        return Err(AppError::NotFound);
    };

    let used_bytes = meta
        .committed_bytes
        .saturating_add(meta.reserved_bytes)
        .saturating_add(meta.trash_bytes);
    let available_bytes = meta.quota_bytes.saturating_sub(used_bytes);
    let usage_percent = if meta.quota_bytes > 0 {
        (used_bytes as f64 / meta.quota_bytes as f64) * 100.0
    } else {
        0.0
    };

    Ok(Json(FragmentQuotaUsageResponse {
        fragment,
        state: meta.state,
        quota_bytes: meta.quota_bytes,
        committed_bytes: meta.committed_bytes,
        reserved_bytes: meta.reserved_bytes,
        trash_bytes: meta.trash_bytes,
        used_bytes,
        available_bytes,
        usage_percent,
        created_at_unix: meta.created_at_unix,
        updated_at_unix: meta.updated_at_unix,
    }))
}
