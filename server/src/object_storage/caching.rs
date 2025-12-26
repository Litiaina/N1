use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::{OBJECT_DATA_ROOT, OBJECT_META_FILE};
use crate::object_storage::metadata_parser::{find_latest_metadata_on_disk, read_bson_metadata};
use crate::structs::object_storage_structs::{ObjectMetadata, ObjectPathManager};
use moka::future::Cache;
use walkdir::WalkDir;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

static METADATA_CACHE: OnceLock<Cache<String, ObjectMetadata>> = OnceLock::new();
static LIST_CACHE: OnceLock<Cache<String, Arc<Vec<String>>>> = OnceLock::new();

fn get_list_cache() -> &'static Cache<String, Arc<Vec<String>>> {
    LIST_CACHE.get_or_init(|| {
        Cache::builder()
            .max_capacity(1_000)
            .time_to_live(Duration::from_secs(300))
            .build()
    })
}

fn get_cache() -> &'static Cache<String, ObjectMetadata> {
    METADATA_CACHE.get_or_init(|| {
        Cache::builder()
            .max_capacity(10_000)
            .time_to_live(Duration::from_secs(300))
            .build()
    })
}

pub async fn get_object_keys_with_cache(
    uid: &str, 
    fragment: &str
) -> Result<Arc<Vec<String>>, AppError> {
    let cache_key = format!("{}:{}", uid, fragment);
    let cache = get_list_cache();

    cache.try_get_with(cache_key, async {
        let path_manager = ObjectPathManager::new();
        let fragment_dir = path_manager
            .root
            .join(OBJECT_DATA_ROOT)
            .join(uid)
            .join(fragment);

        let keys = tokio::task::spawn_blocking(move || -> Result<Vec<String>, std::io::Error> {
            if !fragment_dir.exists() {
                return Ok(Vec::new());
            }

            let mut collected_keys = Vec::with_capacity(1000);
            
            for entry in WalkDir::new(&fragment_dir)
                .min_depth(1)
                .into_iter()
                .filter_map(|e| e.ok())
            {
                if entry.file_type().is_dir() {
                    if let Ok(relative) = entry.path().strip_prefix(&fragment_dir) {
                        let key = relative.to_string_lossy().replace("\\", "/");
                        collected_keys.push(key);
                    }
                }
            }

            collected_keys.sort(); 
            Ok(collected_keys)
        })
        .await
        .map_err(|e| AppError::InternalServerError(format!("Task join error: {}", e)))?
        .map_err(|e| AppError::Io(e, "list objects scan".into()))?;

        Ok::<Arc<Vec<String>>, AppError>(Arc::new(keys))
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("Cache error: {}", e)))
}

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

pub async fn invalidate_list_cache(uid: &str, fragment: &str) {
    let cache_key = format!("{}:{}", uid, fragment);
    get_list_cache().invalidate(&cache_key).await;
}