use std::path::PathBuf;
use std::sync::Arc;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use futures_util::TryStreamExt;
use mongodb::bson::doc;
use tokio::fs;
use tracing::{info, warn};
use crate::database::database_constants::OBJECT_STORAGE_DATABASE;
use crate::enums::app_error_enum::AppError;
use crate::fs::fragmented_upload::with_io_timeout;
use crate::object_storage::constants::{OBJECT_DATA_ROOT, OBJECT_STORAGE_ROOT};
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::ObjectMetadata;

pub async fn delete_object(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let collection = state
        .db_client
        .database(OBJECT_STORAGE_DATABASE)
        .collection::<ObjectMetadata>("objects");

    let filter =
        doc! { "fragment": fragment.clone(), "key": object_key.clone(), "owner_uid": uid.clone() };
    let mut cursor = collection
        .find(filter.clone())
        .await
        .map_err(|_| AppError::Conflict("Database query for find failed.".to_string()))?;

    let mut physical_files_to_delete = Vec::new();
    while let Some(metadata) = cursor
        .try_next()
        .await
        .map_err(|_| AppError::Conflict("Cursor error.".to_string()))?
    {
        physical_files_to_delete.push(metadata.physical_location);
    }

    if physical_files_to_delete.is_empty() {
        return Err(AppError::BadRequest("Object not found.".to_string()));
    }

    collection
        .delete_many(filter)
        .await
        .map_err(|_| AppError::Conflict("Database delete_many failed.".to_string()))?;

    for location in physical_files_to_delete {
        let physical_path = PathBuf::from(OBJECT_STORAGE_ROOT)
            .join(OBJECT_DATA_ROOT)
            .join(location);
        if fs::try_exists(&physical_path).await.unwrap_or(false) {
            if let Err(e) = with_io_timeout(
                "deleting physical object version",
                fs::remove_file(physical_path),
            )
                .await
            {
                warn!("Failed to delete physical file version: {}", e);
            }
        }
    }

    info!(
        "Deleted all versions of object: {}/{}",
        fragment, object_key
    );
    Ok(StatusCode::NO_CONTENT)
}
