use crate::enums::app_error_enum::AppError;
use crate::object_storage::metadata_parser::{find_latest_metadata_on_disk, read_bson_metadata};
use crate::structs::object_storage_structs::{ObjectMetadata, ObjectPathManager};
use moka::future::Cache;
use once_cell::sync::Lazy;
use std::time::Duration;
use tokio::fs;

static META_CACHE: Lazy<Cache<String, ObjectMetadata>> = Lazy::new(|| {
    Cache::builder()
        .time_to_live(Duration::from_secs(300))
        .max_capacity(1_000_000)
        .build()
});

pub fn get_meta_cache() -> &'static Cache<String, ObjectMetadata> {
    &META_CACHE
}

pub async fn get_metadata_with_cache(
    fragment: &str,
    object_key: &str,
    uid: &str,
) -> Result<ObjectMetadata, AppError> {
    let cache_key = format!("{}:{}:{}", uid, fragment, object_key);
    let cache = get_meta_cache();

    if let Some(metadata) = cache.get(&cache_key).await {
        return Ok(metadata);
    }

    let mut metadata = find_latest_metadata_on_disk(uid, fragment, object_key).await?;

    if metadata.fragment != fragment {
        metadata.fragment = fragment.to_string();
    }

    cache.insert(cache_key, metadata.clone()).await;

    Ok(metadata)
}

pub async fn get_version_metadata_with_cache(
    fragment: &str,
    object_key: &str,
    version_id: &str,
    uid: &str,
) -> Result<ObjectMetadata, AppError> {
    let cache_key = format!("{}:{}:{}:{}", uid, fragment, object_key, version_id);
    let cache = get_meta_cache();

    if let Some(metadata) = cache.get(&cache_key).await {
        return Ok(metadata);
    }

    let path_manager = ObjectPathManager::new();

    let meta_paths = path_manager.get_all_metadata_paths(uid, fragment, object_key, version_id);

    let mut active_path = None;
    for path in meta_paths {
        if fs::try_exists(&path).await.unwrap_or(false) {
            active_path = Some(path);
            break;
        }
    }

    let meta_path = active_path.ok_or(AppError::NotFound)?;

    let mut metadata = read_bson_metadata(&meta_path).await?;

    if metadata.fragment != fragment {
        metadata.fragment = fragment.to_string();
    }

    cache.insert(cache_key, metadata.clone()).await;

    Ok(metadata)
}

pub async fn invalidate_cache(fragment: &str, object_key: &str, uid: &str) {
    let cache_key = format!("{}:{}:{}", uid, fragment, object_key);
    get_meta_cache().invalidate(&cache_key).await;
}
