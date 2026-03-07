use crate::enums::app_error_enum::AppError;
use crate::n1_core::disk_manager::ObjectMetadata;
use crate::n1_core::index;
use tracing::warn;

pub async fn find_latest_metadata_on_disk(
    fragment: &str,
    object_key: &str,
) -> Result<ObjectMetadata, AppError> {
    let latest_marker = match index::read_latest_object(fragment, object_key).await {
        Ok(mut metadata) => {
            if metadata.fragment != fragment {
                metadata.fragment = fragment.to_string();
            }
            Some(metadata)
        }
        Err(AppError::NotFound) => None,
        Err(e) => {
            warn!(
                "Index latest read failed for {}/{}: {}",
                fragment, object_key, e
            );
            None
        }
    };

    let latest_versions =
        match index::list_live_object_versions_metadata(fragment, object_key).await {
            Ok(mut versions) => {
                versions.sort_by(|a, b| {
                    b.created_at
                        .cmp(&a.created_at)
                        .then(b.version_id.cmp(&a.version_id))
                });
                versions.into_iter().next().map(|mut m| {
                    if m.fragment != fragment {
                        m.fragment = fragment.to_string();
                    }
                    m
                })
            }
            Err(_) => None,
        };

    match (latest_marker, latest_versions) {
        (Some(a), Some(b)) => {
            let a_shards: usize = a.parts.iter().map(|p| p.shards.len()).sum();
            let b_shards: usize = b.parts.iter().map(|p| p.shards.len()).sum();
            if b.created_at > a.created_at
                || (b.created_at == a.created_at && b.version_id > a.version_id)
                || (b.created_at == a.created_at
                    && b.version_id == a.version_id
                    && b_shards >= a_shards)
            {
                return Ok(b);
            }
            return Ok(a);
        }
        (Some(a), None) => return Ok(a),
        (None, Some(b)) => return Ok(b),
        (None, None) => {}
    }

    Err(AppError::NotFound)
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
