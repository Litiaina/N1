use axum::{Json, extract::Path as AxumPath, response::IntoResponse};
use serde::Serialize;

use crate::{
    core::{
        capacity_manager::{FragmentBlobUsage, get_fragment_blob_usage, get_storage_usage},
        disk_manager::CapacityUsageResponse,
        index_store,
    },
    enums::app_error_enum::AppError,
    structs::auth_structs::Claims,
};

#[derive(Debug, Serialize)]
pub struct FragmentUsageResponse {
    pub fragment: String,
    pub state: String,
    pub committed_bytes: u64,
    pub reserved_bytes: u64,
    pub trash_bytes: u64,
    pub used_bytes: u64,
    pub capacity_used: u64,
    pub capacity_allocated: u64,
    pub capacity_available: u64,
    pub filesystem_count: usize,
    pub capacity_usage_percent: f64,
    pub created_at_unix: i64,
    pub updated_at_unix: i64,
}

fn usage_percent(used: u64, allocated: u64) -> f64 {
    if allocated == 0 {
        0.0
    } else {
        (used as f64 / allocated as f64) * 100.0
    }
}

fn fragment_usage_from_meta(
    fragment: String,
    meta: index_store::FragmentMeta,
    blob_usage: FragmentBlobUsage,
    storage_usage: crate::core::capacity_manager::StorageUsage,
) -> FragmentUsageResponse {
    let used_bytes = blob_usage.used_bytes();

    FragmentUsageResponse {
        fragment,
        state: meta.state,
        committed_bytes: blob_usage.live_bytes,
        reserved_bytes: 0,
        trash_bytes: blob_usage.trash_bytes,
        used_bytes,
        capacity_used: storage_usage.capacity_used,
        capacity_allocated: storage_usage.capacity_allocated,
        capacity_available: storage_usage.capacity_available,
        filesystem_count: storage_usage.filesystem_count,
        capacity_usage_percent: usage_percent(
            storage_usage.capacity_used,
            storage_usage.capacity_allocated,
        ),
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

pub async fn get_fragment_usage(claims: Claims) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }

    let fragment = claims.fragment.clone();
    let lookup_fragment = fragment.clone();
    let meta = tokio::task::spawn_blocking(move || index_store::fragment_meta(&lookup_fragment))
        .await
        .map_err(|err| AppError::InternalServerError(format!("fragment usage worker: {err}")))??;

    let Some(meta) = meta else {
        return Err(AppError::NotFound);
    };

    let blob_usage = get_fragment_blob_usage(&fragment).await?;
    let storage_usage = get_storage_usage().await?;

    Ok(Json(fragment_usage_from_meta(
        fragment,
        meta,
        blob_usage,
        storage_usage,
    )))
}
