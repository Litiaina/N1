use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::{OBJECT_DATA_ROOT, OBJECT_META_FILE};
use crate::object_storage::metadata_parser::{find_latest_metadata_on_disk, read_bson_metadata};
use crate::structs::object_storage_structs::{ObjectMetadata, ObjectPathManager};
use moka::future::Cache;
use once_cell::sync::Lazy;
use tracing::warn;
use walkdir::WalkDir;
use std::sync::Arc;
use std::time::Duration;

static LIST_CACHE: Lazy<Cache<String, Arc<Vec<String>>>> = Lazy::new(|| {
    Cache::builder()
        .time_to_live(Duration::from_secs(3600)) 
        .max_capacity(100_000) 
        .build()
});

static META_CACHE: Lazy<Cache<String, ObjectMetadata>> = Lazy::new(|| {
    Cache::builder()
        .time_to_live(Duration::from_secs(300))
        .max_capacity(1_000_000)
        .build()
});

pub fn get_list_cache() -> &'static Cache<String, Arc<Vec<String>>> {
    &LIST_CACHE
}

pub fn get_meta_cache() -> &'static Cache<String, ObjectMetadata> {
    &META_CACHE
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

            let mut collected_keys = Vec::with_capacity(4096);
            
            let walker = WalkDir::new(&fragment_dir)
                .min_depth(1)
                .follow_links(false);

            for entry_res in walker.into_iter() {
                match entry_res {
                    Ok(entry) => {
                        let file_name = entry.file_name().to_string_lossy();
                        if file_name.starts_with('.') {
                            continue;
                        }

                        if entry.file_type().is_dir() {
                            if let Ok(relative) = entry.path().strip_prefix(&fragment_dir) {
                                let key = relative.to_string_lossy().replace("\\", "/");
                                collected_keys.push(key);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Error scanning entry in fragment {}: {}", fragment_dir.display(), e);
                    }
                }
            }

            collected_keys.sort(); 
            Ok(collected_keys)
        })
        .await
        .map_err(|e| AppError::InternalServerError(format!("Scanner task failed: {}", e)))?
        .map_err(|e| AppError::Io(e, "List objects scan failed".into()))?;

        Ok::<Arc<Vec<String>>, AppError>(Arc::new(keys))
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("Cache interaction failed: {}", e)))
}

pub async fn add_key_to_list_cache(uid: &str, fragment: &str, new_key: String) {
    let cache_key = format!("{}:{}", uid, fragment);
    let cache = get_list_cache();

    if let Some(current_list) = cache.get(&cache_key).await {
        let mut new_list = current_list.as_ref().clone();
        
        if let Err(pos) = new_list.binary_search(&new_key) {
            new_list.insert(pos, new_key);
            cache.insert(cache_key, Arc::new(new_list)).await;
        }
    }
}

pub async fn remove_key_from_list_cache(uid: &str, fragment: &str, key: &str) {
    let cache_key = format!("{}:{}", uid, fragment);
    let cache = get_list_cache();

    if let Some(current_list) = cache.get(&cache_key).await {
        let mut new_list = current_list.as_ref().clone();
        
        if let Ok(pos) = new_list.binary_search(&key.to_string()) {
            new_list.remove(pos);
            cache.insert(cache_key, Arc::new(new_list)).await;
        }
    }
}

pub async fn rename_key_in_list_cache(
    uid: &str, 
    src_fragment: &str, 
    src_key: &str, 
    dst_fragment: &str, 
    dst_key: &str
) {
    remove_key_from_list_cache(uid, src_fragment, src_key).await;
    add_key_to_list_cache(uid, dst_fragment, dst_key.to_string()).await;
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
    let cache = get_meta_cache();

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
    get_meta_cache().invalidate(&cache_key).await;
}

pub async fn invalidate_list_cache(uid: &str, fragment: &str) {
    let cache_key = format!("{}:{}", uid, fragment);
    get_list_cache().invalidate(&cache_key).await;
}
