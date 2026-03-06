use crate::enums::app_error_enum::AppError;
use crate::n1_core::constants::METADATA_EXT;
use crate::n1_core::disk_manager::{ObjectMetadata, ObjectPathManager};
use crate::n1_core::index;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::time::SystemTime;
use tokio::fs;
use tracing::{error, warn};

pub async fn find_latest_metadata_on_disk(
    fragment: &str,
    object_key: &str,
) -> Result<ObjectMetadata, AppError> {
    let path_manager = ObjectPathManager::new();

    let mut hasher = Sha256::new();
    hasher.update(object_key.as_bytes());
    let hash = hex::encode(hasher.finalize());

    let shard1 = &hash[0..2];
    let shard2 = &hash[2..4];
    let shard3 = &hash[4..6];
    let shard4 = &hash[6..8];

    let index_dirs = path_manager.get_all_index_dirs(fragment);
    let prefix = format!("{}#", hash);

    #[derive(Clone)]
    struct Candidate {
        path: PathBuf,
        version_id: String,
        modified: SystemTime,
    }

    let mut candidates: Vec<Candidate> = Vec::new();

    for dir in index_dirs {
        let candidate = dir.join(shard1).join(shard2).join(shard3).join(shard4);
        if !fs::try_exists(&candidate).await.unwrap_or(false) {
            continue;
        }

        let mut entries = fs::read_dir(&candidate).await.map_err(|e| {
            error!("Failed to read index shard directory: {}", e);
            AppError::InternalServerError("Failed to read storage index.".to_string())
        })?;

        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();

            if !name.starts_with(&prefix) || !name.ends_with(METADATA_EXT) {
                continue;
            }

            let ver_part = match name.strip_prefix(&prefix) {
                Some(v) => v,
                None => continue,
            };
            let version_id = match ver_part.strip_suffix(METADATA_EXT) {
                Some(v) if !v.is_empty() => v.to_string(),
                _ => continue,
            };

            let path = candidate.join(&name);
            let modified = entry
                .metadata()
                .await
                .ok()
                .and_then(|m| m.modified().ok())
                .unwrap_or(SystemTime::UNIX_EPOCH);

            candidates.push(Candidate {
                path,
                version_id,
                modified,
            });
        }
    }

    if !candidates.is_empty() {
        candidates.sort_by(|a, b| {
            b.version_id
                .cmp(&a.version_id)
                .then_with(|| b.modified.cmp(&a.modified))
        });

        for candidate in candidates {
            match read_metadata(&candidate.path).await {
                Ok(m) => return Ok(m),
                Err(e) => {
                    warn!(
                        "Skipping unreadable metadata replica {}: {}",
                        candidate.path.display(),
                        e
                    );
                }
            }
        }
    }

    match index::read_latest_object(fragment, object_key).await {
        Ok(metadata) => return Ok(metadata),
        Err(AppError::NotFound) => {}
        Err(e) => {
            warn!(
                "Partitioned index latest read failed for {}/{}: {}",
                fragment, object_key, e
            );
        }
    }

    Err(AppError::NotFound)
}

pub async fn read_metadata(path: &PathBuf) -> Result<ObjectMetadata, AppError> {
    let data = fs::read(path).await.map_err(|e| {
        error!("Failed to read metadata file: {}", e);
        AppError::InternalServerError("Failed to read metadata.".to_string())
    })?;

    let metadata: ObjectMetadata = rmp_serde::from_slice(&data).map_err(|e| {
        error!("Failed to deserialize MessagePack metadata: {}", e);
        AppError::InternalServerError("Invalid metadata format.".to_string())
    })?;

    Ok(metadata)
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
    let path_manager = ObjectPathManager::new();
    let meta_paths = path_manager.get_all_metadata_paths(fragment, object_key, version_id);

    let mut replicas: Vec<(PathBuf, SystemTime)> = Vec::new();
    for path in meta_paths {
        if fs::try_exists(&path).await.unwrap_or(false) {
            let modified = fs::metadata(&path)
                .await
                .ok()
                .and_then(|m| m.modified().ok())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            replicas.push((path, modified));
        }
    }

    if !replicas.is_empty() {
        replicas.sort_by(|a, b| b.1.cmp(&a.1));

        let mut last_err: Option<AppError> = None;
        for (meta_path, _) in replicas {
            match read_metadata(&meta_path).await {
                Ok(mut metadata) => {
                    if metadata.fragment != fragment {
                        metadata.fragment = fragment.to_string();
                    }
                    return Ok(metadata);
                }
                Err(e) => {
                    warn!(
                        "Skipping unreadable version metadata replica {}: {}",
                        meta_path.display(),
                        e
                    );
                    last_err = Some(e);
                }
            }
        }

        if let Some(err) = last_err {
            return Err(err);
        }
    }

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
                "Partitioned index version read failed for {}/{}/{}: {}",
                fragment, object_key, version_id, e
            );
            Err(AppError::NotFound)
        }
    }
}
