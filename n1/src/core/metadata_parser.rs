use crate::core::disk_manager::ObjectMetadata;
use crate::core::index;
use crate::enums::app_error_enum::AppError;
use crate::namespace::runtime::global_namespace_runtime;
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
    let requested_key = object_key.trim_start_matches('/').to_string();
    let physical_object_id = global_namespace_runtime()
        .resolve_live_object_id(fragment, &requested_key)
        .await?
        .unwrap_or_else(|| requested_key.clone());
    let mut metadata = find_latest_metadata_on_disk(fragment, &physical_object_id).await?;

    if metadata.fragment != fragment {
        metadata.fragment = fragment.to_string();
    }
    metadata.object_key = requested_key;

    Ok(metadata)
}

pub async fn get_version_metadata(
    fragment: &str,
    object_key: &str,
    version_id: &str,
    _requesting_fragment_context: &str,
) -> Result<ObjectMetadata, AppError> {
    let requested_key = object_key.trim_start_matches('/').to_string();
    let physical_object_id = global_namespace_runtime()
        .resolve_any_object_id(fragment, &requested_key)
        .await?
        .unwrap_or_else(|| requested_key.clone());
    match index::read_object_version(fragment, &physical_object_id, version_id).await {
        Ok(mut metadata) => {
            if metadata.fragment != fragment {
                metadata.fragment = fragment.to_string();
            }
            metadata.object_key = requested_key;
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
