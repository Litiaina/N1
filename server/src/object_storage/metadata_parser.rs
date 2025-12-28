use crate::enums::app_error_enum::AppError;
use crate::structs::object_storage_structs::{ObjectMetadata, ObjectPathManager};
use std::path::PathBuf;
use sha2::{Digest, Sha256};
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

    let shard_dir = path_manager.get_index_dir(uid, fragment)
        .join(&hash[0..2])
        .join(&hash[2..4]);

    if !fs::try_exists(&shard_dir).await.unwrap_or(false) {
        return Err(AppError::NotFound);
    }

    let prefix = format!("{}#", hash);
    let mut latest_meta: Option<ObjectMetadata> = None;

    let mut entries = fs::read_dir(&shard_dir).await.map_err(|e| {
        error!("Failed to read index shard directory: {}", e);
        AppError::InternalServerError("Failed to read storage index.".to_string())
    })?;

    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_string();
        
        if name.starts_with(&prefix) && name.ends_with(".bson") {
            if let Ok(data) = fs::read(entry.path()).await {
                if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                    match &latest_meta {
                        None => latest_meta = Some(meta),
                        Some(current) => {
                            if meta.created_at > current.created_at {
                                latest_meta = Some(meta);
                            }
                        }
                    }
                }
            }
        }
    }

    latest_meta.ok_or(AppError::NotFound)
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
