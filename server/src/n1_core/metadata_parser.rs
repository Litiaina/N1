use crate::enums::app_error_enum::AppError;
use crate::n1_core::disk_manager::ObjectMetadata;
use crate::n1_core::index;

pub async fn find_latest_metadata_on_disk(
    fragment: &str,
    object_key: &str,
) -> Result<ObjectMetadata, AppError> {
    // Exact-key reads resolve through index latest state and then rehydrate
    // the referenced immutable version record before returning metadata.
    index::read_latest_object(fragment, object_key).await
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
    let mut metadata = index::read_object_version(fragment, object_key, version_id).await?;
    if metadata.fragment != fragment {
        metadata.fragment = fragment.to_string();
    }
    Ok(metadata)
}
