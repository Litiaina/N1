use crate::{enums::app_error_enum::AppError, object_storage::quota_manager::get_quota};

pub async fn calculate_storage_usage(uid: &str) -> Result<u64, AppError> {
    let quota = get_quota(uid).await?;
    Ok(quota.storage_used)
}
