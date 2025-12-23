use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::OBJECT_META_FILE;
use crate::object_storage::metadata_parser::{find_latest_metadata_on_disk, read_bson_metadata};
use crate::structs::object_storage_structs::{ObjectMetadata, ObjectPathManager};
use moka::future::Cache;
use std::sync::OnceLock;
use std::time::Duration;

static METADATA_CACHE: OnceLock<Cache<String, ObjectMetadata>> = OnceLock::new();

fn get_cache() -> &'static Cache<String, ObjectMetadata> {
    METADATA_CACHE.get_or_init(|| {
        Cache::builder()
            .max_capacity(10_000)
            .time_to_live(Duration::from_secs(300))
            .build()
    })
}

// Existing function for latest version
pub async fn get_metadata_with_cache(
    fragment: &str,
    object_key: &str,
    uid: &str,
) -> Result<ObjectMetadata, AppError> {
    let cache_key = format!("{}:{}:{}", uid, fragment, object_key);
    let cache = get_cache();

    if let Some(metadata) = cache.get(&cache_key).await {
        return Ok(metadata);
    }

    let metadata = find_latest_metadata_on_disk(uid, fragment, object_key).await?;

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
    let cache = get_cache();

    if let Some(metadata) = cache.get(&cache_key).await {
        return Ok(metadata);
    }

    let path_manager = ObjectPathManager::new();
    let version_dir = path_manager.get_version_dir(uid, fragment, object_key, version_id);
    let meta_path = version_dir.join(OBJECT_META_FILE);

    if !tokio::fs::try_exists(&meta_path).await.unwrap_or(false) {
         return Err(AppError::NotFound);
    }

    let metadata = read_bson_metadata(&meta_path).await?;

    cache.insert(cache_key, metadata.clone()).await;

    Ok(metadata)
}

pub async fn invalidate_cache(fragment: &str, object_key: &str, uid: &str) {
    let cache_key = format!("{}:{}:{}", uid, fragment, object_key);
    get_cache().invalidate(&cache_key).await;
}
