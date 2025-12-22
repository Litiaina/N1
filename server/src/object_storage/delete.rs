use crate::object_storage::caching::invalidate_cache;
use crate::object_storage::constants::OBJECT_META_FILE;
use crate::object_storage::storage_capacity_parser::increment_storage_usage;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{ObjectMetadata, ObjectPathManager};
use crate::{enums::app_error_enum::AppError};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use chrono::Utc;
use ulid::Ulid;
use std::sync::Arc;
use tokio::fs;
use tracing::{error, info};

pub async fn delete_object_version(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((fragment, object_key, version_id)): Path<(String, String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let path_manager = ObjectPathManager::new();
    let version_dir = path_manager.get_version_dir(&uid, &fragment, &object_key, &version_id);

    if !fs::try_exists(&version_dir).await.unwrap_or(false) {
        return Err(AppError::NotFound);
    }

    let meta_path = version_dir.join(OBJECT_META_FILE);
    let mut size_to_free = 0;
    if let Ok(data) = fs::read(&meta_path).await {
        if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
            if !meta.is_delete_marker {
                size_to_free = meta.total_size_bytes as i64;
            }
        }
    }

    fs::remove_dir_all(&version_dir).await.map_err(|e| {
        error!("Failed to delete version directory: {}", e);
        AppError::InternalServerError("Failed to delete version.".to_string())
    })?;

    if size_to_free > 0 {
        increment_storage_usage(&state.db_client, &uid, -size_to_free).await?;
    }
    
    invalidate_cache(&fragment, &object_key, &uid).await;
    info!("Hard deleted version: {}/{} (v: {})", fragment, object_key, version_id);

    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_object(
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let version_id = Ulid::new().to_string();
    let session_id = Ulid::new().to_string();

    let path_manager = ObjectPathManager::new();
    let version_dir = path_manager.get_version_dir(&uid, &fragment, &object_key, &version_id);

    fs::create_dir_all(&version_dir).await.map_err(|e| AppError::Io(e, "create marker dir".into()))?;

    let delete_marker = ObjectMetadata {
        object_key: object_key.clone(),
        fragment: fragment.clone(),
        version_id: version_id.clone(),
        upload_session_id: session_id,
        owner_uid: uid.clone(),
        content_type: "application/x-delete-marker".to_string(),
        total_size_bytes: 0,
        created_at: Utc::now().timestamp(),
        parts: vec![],
        is_delete_marker: true,
    };

    let bson_data = mongodb::bson::to_vec(&delete_marker).map_err(|_| AppError::InternalServerError("BSON error".into()))?;
    
    let meta_path = version_dir.join(OBJECT_META_FILE);
    fs::write(&meta_path, bson_data).await.map_err(|e| AppError::Io(e, "write marker".into()))?;

    invalidate_cache(&fragment, &object_key, &uid).await;
    info!("Created delete marker: {}/{}", fragment, object_key);

    Ok(StatusCode::NO_CONTENT)
}
