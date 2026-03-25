use crate::core::disk_manager::ObjectMetadata;
use crate::core::index;
use crate::enums::app_error_enum::AppError;
use tracing::warn;

pub async fn find_latest_metadata_on_disk(
    fragment: &str,
    object_key: &str,
) -> Result<ObjectMetadata, AppError> {
    match index::read_latest_object(fragment, object_key).await {
        Ok(mut metadata) => {
            if metadata.fragment != fragment {
                metadata.fragment = fragment.to_string();
            }
            Ok(metadata)
        }
        Err(AppError::NotFound) => Err(AppError::NotFound),
        Err(e) => {
            warn!(
                "Index latest read failed for {}/{}: {}",
                fragment, object_key, e
            );
            Err(AppError::NotFound)
        }
    }
}

pub async fn get_metadata(
    fragment: &str,
    object_key: &str,
    _requesting_fragment_context: &str,
) -> Result<ObjectMetadata, AppError> {
    let mut metadata = find_latest_metadata_on_disk(fragment, object_key).await?;

    if metadata.fragment != fragment {
        metadata.fragment = fragment.to_string();
    }

    Ok(metadata)
}

pub async fn get_version_metadata(
    fragment: &str,
    object_key: &str,
    version_id: &str,
    _requesting_fragment_context: &str,
) -> Result<ObjectMetadata, AppError> {
    match index::read_object_version(fragment, object_key, version_id).await {
        Ok(mut metadata) => {
            if metadata.fragment != fragment {
                metadata.fragment = fragment.to_string();
            }
            Ok(metadata)
        }
        Err(AppError::NotFound) => Err(AppError::NotFound),
        Err(e) => {
            warn!(
                "Index version read failed for {}/{}/{}: {}",
                fragment, object_key, version_id, e
            );
            Err(AppError::NotFound)
        }
    }
}
