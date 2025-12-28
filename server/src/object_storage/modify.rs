use std::sync::Arc;

use crate::enums::app_error_enum::AppError;
use crate::object_storage::caching::{invalidate_cache, invalidate_list_cache, rename_key_in_list_cache};
use crate::object_storage::constants::OBJECT_DATA_ROOT;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{
    ObjectMetadata, ObjectPathManager, RenameFragmentQuery, RenameObjectRequest
};
use crate::websocket::ws_structs::StorageEventPayload;
use axum::extract::{Query, State};
use axum::{Json as AxumJson, http::StatusCode, response::IntoResponse};
use sha2::{Digest, Sha256};
use tokio::fs;
use tracing::{error, info};

pub async fn rename_object(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumJson(payload): AxumJson<RenameObjectRequest>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let path_manager = ObjectPathManager::new();

    let mut hasher = Sha256::new();
    hasher.update(payload.source_key.as_bytes());
    let src_hash = hex::encode(hasher.finalize());
    let src_shard = path_manager.get_index_dir(&uid, &payload.source_fragment)
        .join(&src_hash[0..2])
        .join(&src_hash[2..4]);

    let mut hasher = Sha256::new();
    hasher.update(payload.destination_key.as_bytes());
    let dst_hash = hex::encode(hasher.finalize());
    let dst_shard = path_manager.get_index_dir(&uid, &payload.destination_fragment)
        .join(&dst_hash[0..2])
        .join(&dst_hash[2..4]);

    if !fs::try_exists(&src_shard).await.unwrap_or(false) {
        return Err(AppError::NotFound);
    }

    if fs::try_exists(&dst_shard).await.unwrap_or(false) {
        let prefix = format!("{}#", dst_hash);
        let mut entries = fs::read_dir(&dst_shard).await.map_err(|e| AppError::Io(e, "read dest shard".into()))?;
        while let Ok(Some(entry)) = entries.next_entry().await {
            if entry.file_name().to_string_lossy().starts_with(&prefix) {
                return Err(AppError::Conflict("Destination object already exists".into()));
            }
        }
    }

    fs::create_dir_all(&dst_shard).await.map_err(|e| {
        error!("Failed to create dest shard: {}", e);
        AppError::InternalServerError("Failed to prepare rename.".into())
    })?;

    let src_prefix = format!("{}#", src_hash);
    let mut entries = fs::read_dir(&src_shard).await.map_err(|e| AppError::Io(e, "read src shard".into()))?;
    
    let mut found = false;

    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(&src_prefix) && name.ends_with(".bson") {
            found = true;
            if let Some((_, version_ext)) = name.split_once('#') {
                if let Some(version_id) = version_ext.strip_suffix(".bson") {
                    
                    let new_filename = format!("{}#{}.bson", dst_hash, version_id);
                    let dest_path = dst_shard.join(&new_filename);
                    
                    if let Ok(data) = fs::read(entry.path()).await {
                        if let Ok(mut meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                            meta.object_key = payload.destination_key.clone();
                            meta.fragment = payload.destination_fragment.clone();
                            
                            if let Ok(bytes) = mongodb::bson::to_vec(&meta) {
                                if let Err(e) = fs::write(&dest_path, bytes).await {
                                     error!("Failed to write new meta during rename: {}", e);
                                     continue;
                                }
                                let _ = fs::remove_file(entry.path()).await;
                            }
                        }
                    }
                }
            }
        }
    }

    if !found {
        return Err(AppError::NotFound);
    }

    invalidate_cache(&payload.source_fragment, &payload.source_key, &uid).await;
    invalidate_cache(&payload.destination_fragment, &payload.destination_key, &uid).await;

    rename_key_in_list_cache(
        &uid,
        &payload.source_fragment,
        &payload.source_key,
        &payload.destination_fragment,
        &payload.destination_key
    ).await;

    info!(
        "Renamed object from {}/{} to {}/{}",
        payload.source_fragment,
        payload.source_key,
        payload.destination_fragment,
        payload.destination_key
    );

    state.emit(&uid, StorageEventPayload::FileRenamed {
        source_fragment: payload.source_fragment,
        source_key: payload.source_key,
        destination_fragment: payload.destination_fragment,
        destination_key: payload.destination_key,
    });

    Ok(StatusCode::OK)
}

pub async fn rename_fragment(
    claims: Claims,
    Query(query): Query<RenameFragmentQuery>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let path_manager = ObjectPathManager::new();

    let user_storage_root = path_manager.root.join(OBJECT_DATA_ROOT).join(&uid);

    let (query, uid) = tokio::task::spawn_blocking(move || -> Result<(RenameFragmentQuery, String), AppError> {
        let source_path = user_storage_root.join(&query.fragment_to_rename);
        let dest_path = user_storage_root.join(&query.new_name);

        if !source_path.exists() {
            return Err(AppError::NotFound);
        }

        if dest_path.exists() {
            return Err(AppError::Conflict(format!(
                "Destination fragment '{}' already exists.",
                query.new_name
            )));
        }

        std::fs::rename(&source_path, &dest_path).map_err(|e| {
            error!(
                "Failed to rename fragment from {:?} to {:?}: {}",
                source_path, dest_path, e
            );
            AppError::InternalServerError("Failed to rename fragment.".to_string())
        })?;

        info!(
            "Renamed fragment '{}' to '{}' for user {}",
            query.fragment_to_rename, query.new_name, uid
        );

        Ok((query, uid))
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("Task join error: {}", e)))??;

    invalidate_list_cache(&uid, &query.fragment_to_rename).await;
    invalidate_list_cache(&uid, &query.new_name).await;

    Ok((
        StatusCode::OK,
        AxumJson(serde_json::json!({
            "status": "success",
            "message": "Fragment renamed successfully"
        })),
    ))
}
