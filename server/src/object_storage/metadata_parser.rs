use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::OBJECT_META_FILE;
use crate::structs::object_storage_structs::{ObjectMetadata, ObjectPathManager};
use std::path::PathBuf;
use tokio::fs;
use tracing::error;

pub async fn find_latest_metadata_on_disk(
    uid: &str,
    fragment: &str,
    object_key: &str,
) -> Result<ObjectMetadata, AppError> {
    let path_manager = ObjectPathManager::new();
    let object_dir = path_manager.get_object_dir(uid, fragment, object_key);

    if !fs::try_exists(&object_dir).await.unwrap_or(false) {
        return Err(AppError::NotFound);
    }

    let mut entries = fs::read_dir(&object_dir).await.map_err(|e| {
        error!("Failed to read object directory: {}", e);
        AppError::InternalServerError("Failed to read storage directory.".to_string())
    })?;

    let mut versions = Vec::new();

    while let Ok(Some(entry)) = entries.next_entry().await {
        if let Ok(file_type) = entry.file_type().await {
            if file_type.is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    versions.push(name.to_string());
                }
            }
        }
    }

    versions.sort();

    for version in versions.iter().rev() {
        let meta_path = object_dir.join(version).join(OBJECT_META_FILE);
        if fs::try_exists(&meta_path).await.unwrap_or(false) {
            match read_bson_metadata(&meta_path).await {
                Ok(meta) => return Ok(meta),
                Err(_) => continue,
            }
        }
    }

    Err(AppError::NotFound)
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
