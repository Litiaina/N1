use crate::core::disk_manager::ObjectMetadata;
use crate::core::index;
use crate::core::index_store;
use crate::enums::app_error_enum::AppError;
use crate::namespace::runtime::{global_namespace_runtime, logical_not_found};
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
    let physical_object_id = match global_namespace_runtime()
        .resolve_live_object_id(fragment, &requested_key)
        .await?
    {
        Some(object_id) => object_id,
        None => {
            index_store::normalize_object_id(&requested_key).map_err(|_| logical_not_found())?
        }
    };
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

    let resolved = match global_namespace_runtime()
        .resolve_any_object_id(fragment, &requested_key)
        .await
    {
        Ok(Some(object_id)) => Some(object_id),
        Ok(None) => index_store::normalize_object_id(&requested_key).ok(),
        Err(err) => {
            warn!(
                "Namespace version lookup failed for {}/{}@{}; falling back to immutable version index: {}",
                fragment, object_key, version_id, err
            );
            index_store::normalize_object_id(&requested_key).ok()
        }
    };

    if let Some(physical_object_id) = resolved {
        match index::read_object_version(fragment, &physical_object_id, version_id).await {
            Ok(mut metadata) => {
                if metadata.fragment != fragment {
                    metadata.fragment = fragment.to_string();
                }
                metadata.object_key = requested_key;
                return Ok(metadata);
            }
            Err(AppError::NotFound) => {}
            Err(e) => {
                warn!(
                    "Index version read failed for {}/{}/{}: {}",
                    fragment, physical_object_id, version_id, e
                );
            }
        }
    }

    match index::find_live_object_version_by_id(fragment, version_id).await? {
        Some(mut metadata) => {
            if metadata.fragment != fragment {
                metadata.fragment = fragment.to_string();
            }
            metadata.object_key = requested_key;
            Ok(metadata)
        }
        None => Err(logical_not_found()),
    }
}
