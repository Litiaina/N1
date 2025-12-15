use crate::database::database_constants::OBJECT_STORAGE_DATABASE;
use crate::enums::app_error_enum::AppError;
use crate::fs::fragmented_upload::with_io_timeout;
use crate::object_storage::constants::OBJECT_DATA_ROOT;
use crate::routes::storage_routes::AppState;
use crate::statics::merge_semaphore_static::PREFIX_DELETE_LOCKS;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{DeleteByPrefixRequest, OBJECT_STORAGE_ROOT, ObjectMetadata};
use axum::Json as AxumJson;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use futures_util::TryStreamExt;
use mongodb::bson::doc;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tracing::{error, info, warn};

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
        let root_path = OBJECT_STORAGE_ROOT.get().expect("Config not initialized!");
        let physical_path = PathBuf::from(root_path)
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

pub async fn delete_objects_by_prefix(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumJson(payload): AxumJson<DeleteByPrefixRequest>,
) -> Result<impl IntoResponse, AppError> {
    let lock = PREFIX_DELETE_LOCKS
        .entry(payload.prefix.clone())
        .or_default()
        .clone();

    let prefix_lock_guard = match Arc::clone(&lock).try_lock_owned() {
        Ok(guard) => guard,
        Err(_) => {
            return Err(AppError::Conflict(
                "A delete operation on this prefix is already in progress. Please try again later."
                    .to_string(),
            ));
        }
    };

    let uid = claims.uid;
    let collection = state
        .db_client
        .database(OBJECT_STORAGE_DATABASE)
        .collection::<ObjectMetadata>("objects");

    if payload.prefix.is_empty() {
        drop(prefix_lock_guard);
        PREFIX_DELETE_LOCKS.remove(&payload.prefix);
        return Err(AppError::BadRequest(
            "Prefix cannot be empty for a prefix-based delete.".to_string(),
        ));
    }

    let filter = doc! {
        "fragment": &payload.fragment,
        "owner_uid": &uid,
        "key": { "$regex": format!("^{}", regex::escape(&payload.prefix)) }
    };

    let mut cursor = collection.find(filter.clone()).await.map_err(|e| {
        error!("Database find failed during prefix delete: {}", e);
        AppError::InternalServerError("Could not query objects for deletion.".to_string())
    })?;

    let mut physical_files_to_delete = Vec::new();
    while let Some(metadata) = cursor.try_next().await.map_err(|e| {
        error!("Cursor error during prefix delete: {}", e);
        AppError::InternalServerError("Error reading object metadata.".to_string())
    })? {
        if !metadata.physical_location.is_empty() {
            physical_files_to_delete.push(metadata.physical_location);
        }
    }

    if physical_files_to_delete.is_empty() {
        info!(
            "No objects found with prefix '{}' in fragment '{}' to delete for user {}.",
            payload.prefix, payload.fragment, uid
        );
        drop(prefix_lock_guard);
        PREFIX_DELETE_LOCKS.remove(&payload.prefix);
        return Ok((
            StatusCode::OK,
            "No objects found with the specified prefix.",
        )
            .into_response());
    }

    let db_delete_result = collection.delete_many(filter).await.map_err(|e| {
        error!("Database delete_many failed during prefix delete: {}", e);
        AppError::InternalServerError("Failed to delete object records from database.".to_string())
    })?;

    info!(
        "Deleted {} metadata records for prefix '{}' in fragment '{}'. Starting physical file cleanup.",
        db_delete_result.deleted_count, payload.prefix, payload.fragment
    );

    let prefix_for_task = payload.prefix.clone();

    tokio::spawn(async move {
        let mut deleted_count = 0;
        for location in physical_files_to_delete {
            let root_path = OBJECT_STORAGE_ROOT.get().expect("Config not initialized!");
            let physical_path = PathBuf::from(root_path)
                .join(OBJECT_DATA_ROOT)
                .join(location);

            if let Ok(exists) = fs::try_exists(&physical_path).await {
                if exists {
                    if let Err(e) = fs::remove_file(physical_path).await {
                        warn!("Failed to delete physical file in background task: {}", e);
                    } else {
                        deleted_count += 1;
                    }
                }
            }
        }
        info!(
            "Background task finished. Deleted {} physical files for prefix '{}'.",
            deleted_count, prefix_for_task
        );

        drop(prefix_lock_guard);
        PREFIX_DELETE_LOCKS.remove(&prefix_for_task);
    });

    Ok((
        StatusCode::ACCEPTED,
        "Deletion process for objects with the specified prefix has been initiated.",
    )
        .into_response())
}
