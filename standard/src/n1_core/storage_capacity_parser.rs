use axum::{Json, extract::Path as AxumPath, response::IntoResponse};

use crate::{
    enums::app_error_enum::AppError,
    n1_core::{disk_manager::CapacityUsageResponse, quota_manager::get_storage_usage},
    structs::auth_structs::Claims,
};

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
