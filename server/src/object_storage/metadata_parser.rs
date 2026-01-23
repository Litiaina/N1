use crate::enums::app_error_enum::AppError;
use crate::structs::object_storage_structs::{ObjectMetadata, ObjectPathManager};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use tokio::fs;
use tracing::error;

pub async fn find_latest_metadata_on_disk(
    uid: &str,
    fragment: &str,
    object_key: &str,
) -> Result<ObjectMetadata, AppError> {
    let path_manager = ObjectPathManager::new();

    let mut hasher = Sha256::new();
    hasher.update(object_key.as_bytes());
    let hash = hex::encode(hasher.finalize());

    let shard_path_suffix = PathBuf::from(&hash[0..2]).join(&hash[2..4]);
    let index_dirs = path_manager.get_all_index_dirs(uid, fragment);
    let prefix = format!("{}#", hash);

    let mut best_path: Option<PathBuf> = None;
    let mut best_ver: Option<String> = None;

    for dir in index_dirs {
        let shard_dir = dir.join(&shard_path_suffix);
        if !fs::try_exists(&shard_dir).await.unwrap_or(false) {
            continue;
        }

        let mut entries = match fs::read_dir(&shard_dir).await {
            Ok(e) => e,
            Err(_) => continue,
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.starts_with(&prefix) || !name.ends_with(".bson") {
                continue;
            }

            let ver_part = match name.strip_prefix(&prefix) {
                Some(v) => v,
                None => continue,
            };
            let ver = match ver_part.strip_suffix(".bson") {
                Some(v) => v,
                None => continue,
            };

            let take = match &best_ver {
                None => true,
                Some(curr) => ver > curr.as_str(),
            };

            if take {
                best_ver = Some(ver.to_string());
                best_path = Some(shard_dir.join(&name));
            }
        }
    }

    let path = best_path.ok_or(AppError::NotFound)?;
    read_bson_metadata(&path).await
}

pub async fn read_bson_metadata(path: &PathBuf) -> Result<ObjectMetadata, AppError> {
    let data = fs::read(path).await.map_err(|e| {
        error!("Failed to read metadata file: {}", e);
        AppError::InternalServerError("Failed to read metadata.".to_string())
    })?;

    let metadata: ObjectMetadata = mongodb::bson::from_slice(&data).map_err(|e| {
        error!("Failed to deserialize BSON metadata: {}", e);
        AppError::InternalServerError("Invalid metadata format.".to_string())
    })?;

    Ok(metadata)
}

pub async fn get_metadata(
    fragment: &str,
    object_key: &str,
    uid: &str,
) -> Result<ObjectMetadata, AppError> {
    let mut metadata = find_latest_metadata_on_disk(uid, fragment, object_key).await?;

    if metadata.fragment != fragment {
        metadata.fragment = fragment.to_string();
    }

    Ok(metadata)
}

pub async fn get_version_metadata(
    fragment: &str,
    object_key: &str,
    version_id: &str,
    uid: &str,
) -> Result<ObjectMetadata, AppError> {
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

    Ok(metadata)
}
