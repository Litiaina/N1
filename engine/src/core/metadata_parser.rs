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
            Err(e)
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
    let physical_object_id = match global_namespace_runtime()
        .resolve_any_object_id(fragment, &requested_key)
        .await?
    {
        Some(object_id) => object_id,
        None => {
            index_store::normalize_object_id(&requested_key).map_err(|_| logical_not_found())?
        }
    };
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
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn invalid_metadata_lookup_does_not_masquerade_as_absence() {
        let _guard = crate::test_support::global_test_lock().lock().await;
        crate::middleware::auth_middleware::tests::ensure_test_env();
        let error = find_latest_metadata_on_disk("metadata-errors", "../invalid")
            .await
            .unwrap_err();
        assert_ne!(error.status_code(), axum::http::StatusCode::NOT_FOUND);
    }
}
