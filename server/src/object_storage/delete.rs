use crate::enums::app_error_enum::AppError;
use crate::object_storage::caching::invalidate_cache;
use crate::object_storage::metadata_parser::find_latest_metadata_on_disk;
use crate::object_storage::storage_capacity_parser::increment_storage_usage;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::ObjectPathManager;
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

    fs::remove_dir_all(&object_dir).await.map_err(|e| {
        error!("Failed to delete object directory: {}", e);
        AppError::InternalServerError("Failed to delete object.".to_string())
    })?;

    increment_storage_usage(&state.db_client, &uid, -size_to_free).await?;

    invalidate_cache(&fragment, &object_key, &uid).await;

    info!("Deleted object: {}/{}", fragment, object_key);

    Ok(StatusCode::NO_CONTENT)
}
