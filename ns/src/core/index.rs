use crate::core::disk_manager::ObjectMetadata;
use crate::enums::app_error_enum::AppError;

pub async fn read_object_version(
    _fragment: &str,
    _object_key: &str,
    _version_id: &str,
) -> Result<ObjectMetadata, AppError> {
    Err(AppError::NotFound)
}
