use crate::object_storage::caching::invalidate_cache;
use crate::object_storage::metadata_parser::find_latest_metadata_on_disk;
use crate::object_storage::storage_capacity_parser::increment_storage_usage;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::ObjectPathManager;
use crate::{enums::app_error_enum::AppError, object_storage::constants::OBJECT_DATA_ROOT};
use axum::Json as AxumJson;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use std::sync::Arc;
use tokio::fs;
use tracing::{error, info};

pub async fn delete_object(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;

    let meta = find_latest_metadata_on_disk(&uid, &fragment, &object_key).await?;
    let size_to_free = meta.total_size_bytes as i64;

    let path_manager = ObjectPathManager::new();
    let object_dir = path_manager.get_object_dir(&uid, &fragment, &object_key);

    let fs_task = async {
        fs::remove_dir_all(&object_dir).await.map_err(|e| {
            error!("Failed to delete object directory: {}", e);
            AppError::InternalServerError("Failed to delete object.".to_string())
        })
    };

    let db_task = increment_storage_usage(&state.db_client, &uid, -size_to_free);
    let cache_task = invalidate_cache(&fragment, &object_key, &uid);
    let (fs_result, db_result, _) = tokio::join!(fs_task, db_task, cache_task);
    fs_result?;
    db_result?;
    info!("Deleted object: {}/{}", fragment, object_key);

    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_fragment(
    claims: Claims,
    Path(fragment_name): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let path_manager = ObjectPathManager::new();

    let user_storage_root = path_manager.root.join(OBJECT_DATA_ROOT).join(&uid);

    tokio::task::spawn_blocking(move || -> Result<(), AppError> {
        let fragment_path = user_storage_root.join(&fragment_name);

        if !fragment_path.exists() {
            return Err(AppError::NotFound);
        }

        std::fs::remove_dir_all(&fragment_path).map_err(|e| {
            error!("Failed to delete fragment {:?}: {}", fragment_path, e);
            AppError::InternalServerError("Failed to delete fragment.".to_string())
        })?;

        info!("Deleted fragment '{}' for user {}", fragment_name, uid);

        Ok(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("Task join error: {}", e)))??;

    Ok((
        StatusCode::OK,
        AxumJson(serde_json::json!({
            "status": "success",
            "message": "Fragment deleted successfully"
        })),
    ))
}
