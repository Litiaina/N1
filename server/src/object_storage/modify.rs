use std::collections::HashSet;
use std::sync::Arc;

use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::METADATA_EXT;
use crate::object_storage::disk_manager::{ObjectMetadata, ObjectPathManager, RenameObjectRequest};
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::ws_structs::StorageEventPayload;
use axum::extract::State;
use axum::{Json as AxumJson, http::StatusCode, response::IntoResponse};
use futures_util::future::join_all;
use sha2::{Digest, Sha256};
use tokio::fs;
use tracing::{error, info, warn};

pub async fn rename_object(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumJson(payload): AxumJson<RenameObjectRequest>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden("Modify permission required".into()));
    }

    if claims.fragment != payload.source_fragment {
        return Err(AppError::Forbidden(
            "Access denied to source fragment".into(),
        ));
    }

    if payload.source_fragment != payload.destination_fragment {
        if claims.fragment != payload.destination_fragment {
            return Err(AppError::Forbidden(
                "Access denied to destination fragment".into(),
            ));
        }
    }

    let path_manager = ObjectPathManager::new();

    let mut hasher = Sha256::new();
    hasher.update(payload.source_key.as_bytes());
    let src_key_hash = hex::encode(hasher.finalize());

    let mut hasher = Sha256::new();
    hasher.update(payload.destination_key.as_bytes());
    let dst_key_hash = hex::encode(hasher.finalize());

    let index_roots = path_manager.get_all_index_roots();
    let payload_arc = Arc::new(payload);
    let src_key_hash_arc = Arc::new(src_key_hash);
    let dst_key_hash_arc = Arc::new(dst_key_hash);

    let rename_futures = index_roots.into_iter().map(|index_root| {
        let payload = payload_arc.clone();
        let src_key_hash = src_key_hash_arc.clone();
        let dst_key_hash = dst_key_hash_arc.clone();

        async move {
            let src_shard = index_root
                .join(&payload.source_fragment)
                .join("index")
                .join(&src_key_hash[0..2])
                .join(&src_key_hash[2..4])
                .join(&src_key_hash[4..6])
                .join(&src_key_hash[6..8]);

            let dst_shard = index_root
                .join(&payload.destination_fragment)
                .join("index")
                .join(&dst_key_hash[0..2])
                .join(&dst_key_hash[2..4])
                .join(&dst_key_hash[4..6])
                .join(&dst_key_hash[6..8]);

            if !fs::try_exists(&src_shard).await.unwrap_or(false) {
                return Ok(Vec::new());
            }

            if fs::try_exists(&dst_shard).await.unwrap_or(false) {
                let prefix = format!("{}#", dst_key_hash);
                let mut entries = fs::read_dir(&dst_shard)
                    .await
                    .map_err(|e| AppError::Io(e, "read dest shard".into()))?;
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let fname = entry.file_name().to_string_lossy().to_string();
                    if fname.starts_with(&prefix) && fname.ends_with(METADATA_EXT) {
                        return Err(AppError::Conflict(
                            "Destination object already exists".into(),
                        ));
                    }
                }
            }

            fs::create_dir_all(&dst_shard).await.map_err(|e| {
                error!("Failed to create dest shard: {}", e);
                AppError::InternalServerError("Failed to prepare rename.".into())
            })?;

            let src_prefix = format!("{}#", src_key_hash);
            let mut entries = fs::read_dir(&src_shard)
                .await
                .map_err(|e| AppError::Io(e, "read src shard".into()))?;

            let mut moved_versions = Vec::new();

            while let Ok(Some(entry)) = entries.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_string();

                if name.starts_with(&src_prefix) && name.ends_with(METADATA_EXT) {
                    if let Some((_, version_ext)) = name.split_once('#') {
                        if let Some(version_id) = version_ext.strip_suffix(METADATA_EXT) {
                            let new_filename =
                                format!("{}#{}{}", dst_key_hash, version_id, METADATA_EXT);
                            let dest_path = dst_shard.join(&new_filename);

                            if let Ok(data) = fs::read(entry.path()).await {
                                if let Ok(mut meta) = rmp_serde::from_slice::<ObjectMetadata>(&data)
                                {
                                    meta.object_key = payload.destination_key.clone();
                                    meta.fragment = payload.destination_fragment.clone();

                                    if let Ok(bytes) = rmp_serde::to_vec(&meta) {
                                        if let Err(e) = fs::write(&dest_path, bytes).await {
                                            error!("Failed to write new meta during rename: {}", e);
                                            continue;
                                        }
                                        let _ = fs::remove_file(entry.path()).await;
                                        moved_versions.push(version_id.to_string());
                                    }
                                }
                            }
                        }
                    }
                }
            }

            Ok(moved_versions)
        }
    });

    let results = join_all(rename_futures).await;

    let mut affected_versions = HashSet::new();
    let mut success = false;

    for result in results {
        match result {
            Ok(versions) => {
                if !versions.is_empty() {
                    success = true;
                    for v in versions {
                        affected_versions.insert(v);
                    }
                }
            }
            Err(e) => return Err(e),
        }
    }

    if !success {
        return Err(AppError::NotFound);
    }

    if payload_arc.source_fragment != payload_arc.destination_fragment {
        let blob_roots = path_manager.get_blob_roots();

        for root in blob_roots {
            for version_id in &affected_versions {
                let src_blob_dir = root
                    .join(&payload_arc.source_fragment)
                    .join("blob")
                    .join(version_id);

                let dst_blob_dir = root
                    .join(&payload_arc.destination_fragment)
                    .join("blob")
                    .join(version_id);

                if src_blob_dir.exists() {
                    if let Some(parent) = dst_blob_dir.parent() {
                        if !parent.exists() {
                            let _ = fs::create_dir_all(parent).await;
                        }
                    }

                    if let Err(e) = fs::rename(&src_blob_dir, &dst_blob_dir).await {
                        warn!(
                            "Failed to rename blob dir {:?} -> {:?}: {}",
                            src_blob_dir, dst_blob_dir, e
                        );
                    }
                }
            }
        }
    }

    info!(
        "Renamed object from {}/{} to {}/{}",
        payload_arc.source_fragment,
        payload_arc.source_key,
        payload_arc.destination_fragment,
        payload_arc.destination_key
    );

    state.emit(
        &payload_arc.source_fragment,
        StorageEventPayload::FileRenamed {
            source_fragment: payload_arc.source_fragment.clone(),
            source_key: payload_arc.source_key.clone(),
            destination_fragment: payload_arc.destination_fragment.clone(),
            destination_key: payload_arc.destination_key.clone(),
        },
    );

    Ok(StatusCode::OK)
}
