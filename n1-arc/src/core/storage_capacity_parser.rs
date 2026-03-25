use axum::{Json, extract::Path as AxumPath, response::IntoResponse};

use crate::{
    core::{disk_manager::CapacityUsageResponse, quota_manager::get_quota},
    enums::app_error_enum::AppError,
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

    let quota = get_quota(&fragment).await?;

    Ok(Json(CapacityUsageResponse {
        capacity_used: quota.storage_used,
        capacity_allocated: quota.allocated_storage,
    }))
}
