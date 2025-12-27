use std::sync::Arc;

use crate::enums::app_error_enum::AppError;
use crate::object_storage::caching::{invalidate_cache, invalidate_list_cache, rename_key_in_list_cache};
use crate::object_storage::constants::{OBJECT_DATA_ROOT, OBJECT_META_FILE};
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{
    ObjectMetadata, ObjectPathManager, RenameFragmentQuery, RenameObjectRequest
};
use crate::websocket::ws_structs::StorageEventPayload;
use axum::extract::{Query, State};
use axum::{Json as AxumJson, http::StatusCode, response::IntoResponse};
use tokio::fs;
use tracing::{error, info};

pub async fn rename_object(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumJson(payload): AxumJson<RenameObjectRequest>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let path_manager = ObjectPathManager::new();

    let source_dir =
        path_manager.get_object_dir(&uid, &payload.source_fragment, &payload.source_key);
    let dest_dir = path_manager.get_object_dir(
        &uid,
        &payload.destination_fragment,
        &payload.destination_key,
    );

    if !fs::try_exists(&source_dir).await.unwrap_or(false) {
        return Err(AppError::NotFound);
    }

    if fs::try_exists(&dest_dir).await.unwrap_or(false) {
        return Err(AppError::Conflict(
            "An object with the destination name already exists.".to_string(),
        ));
    }

    if let Some(parent) = dest_dir.parent() {
        if !fs::try_exists(parent).await.unwrap_or(false) {
            fs::create_dir_all(parent).await.map_err(|e| {
                error!("Failed to create destination fragment directory: {}", e);
                AppError::InternalServerError(
                    "Failed to prepare destination directory.".to_string(),
                )
            })?;
        }
    }

    fs::rename(&source_dir, &dest_dir).await.map_err(|e| {
        error!(
            "Failed to move object directory from {:?} to {:?}: {}",
            source_dir, dest_dir, e
        );
        AppError::InternalServerError("Failed to rename object.".to_string())
    })?;
    
    let dest_key_bg = payload.destination_key.clone();
    let dest_frag_bg = payload.destination_fragment.clone();

    tokio::spawn(async move {
        let mut read_dir = match fs::read_dir(&dest_dir).await {
            Ok(entries) => entries,
            Err(e) => {
                error!("Background Rename Task: Failed to read dir {:?}: {}", dest_dir, e);
                return;
            }
        };

        while let Ok(Some(entry)) = read_dir.next_entry().await {
            if let Ok(ft) = entry.file_type().await {
                if ft.is_dir() {
                    let version_path = entry.path();
                    let meta_path = version_path.join(OBJECT_META_FILE);
                    
                    if fs::try_exists(&meta_path).await.unwrap_or(false) {
                        if let Ok(data) = fs::read(&meta_path).await {
                            if let Ok(mut meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                                meta.object_key = dest_key_bg.clone();
                                meta.fragment = dest_frag_bg.clone();
                                
                                if let Ok(bytes) = mongodb::bson::to_vec(&meta) {
                                    let _ = fs::write(&meta_path, bytes).await;
                                }
                            }
                        }
                    }
                }
            }
        }
    });

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
