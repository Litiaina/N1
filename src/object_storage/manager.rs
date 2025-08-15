use crate::enums::app_error_enum::AppError;
use crate::fs::fragmented_upload::with_io_timeout;
use crate::object_storage::constants::{OBJECT_DATA_ROOT, OBJECT_STORAGE_ROOT};
use crate::routes::file_system_routes::AppState;
use crate::structs::object_storage_structs::ObjectMetadata;
use crate::utils::header_extractor::extract_uid_from_fragmented_header;
use axum::{
    Json as AxumJson, Json,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
};
use futures_util::stream::TryStreamExt;
use headers::HeaderValue;
use mongodb::{Client as MongoClient, bson::doc};
use std::sync::Arc;
use std::{collections::HashMap, path::PathBuf};
use tokio::fs;
use tower::ServiceExt;
use tracing::{error, info, warn};

pub async fn get_object(
    State(state): State<Arc<AppState>>,
    header: HeaderMap,
    Path((fragment, object_key)): Path<(String, String)>,
    req: axum::http::Request<Body>,
) -> Result<impl IntoResponse, AppError> {
    let uid = extract_uid_from_fragmented_header(&header)?;
    let metadata =
        find_latest_object_metadata(&state.db_client, &fragment, &object_key, &uid).await?;

    let physical_path = PathBuf::from(OBJECT_STORAGE_ROOT)
        .join(OBJECT_DATA_ROOT)
        .join(&metadata.physical_location);

    if !fs::try_exists(&physical_path).await.unwrap_or(false) {
        error!(
            "CRITICAL: Metadata found but physical file is missing at {}",
            physical_path.display()
        );
        return Err(AppError::Conflict("Object data is missing.".to_string()));
    }

    let service = tower_http::services::ServeFile::new(physical_path);

    match service.oneshot(req).await {
        Ok(mut response) => {
            let content_type = metadata.content_type;
            match HeaderValue::from_str(&content_type) {
                Ok(header_value) => {
                    response
                        .headers_mut()
                        .insert(header::CONTENT_TYPE, header_value);
                }
                Err(e) => {
                    error!("Invalid content-type string in metadata: {}", e);
                }
            }
            Ok(response.into_response())
        }
        Err(err) => {
            error!("Failed to serve file: {}", err);
            Err(AppError::Conflict(format!("Failed to serve file: {}", err)))
        }
    }
}

pub async fn get_object_metadata(
    State(state): State<Arc<AppState>>,
    header: HeaderMap,
    Path((fragment, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = extract_uid_from_fragmented_header(&header)?;
    let metadata =
        find_latest_object_metadata(&state.db_client, &fragment, &object_key, &uid).await?;
    Ok(AxumJson(metadata))
}

pub async fn delete_object(
    State(state): State<Arc<AppState>>,
    header: HeaderMap,
    Path((fragment, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = extract_uid_from_fragmented_header(&header)?;
    let collection = state
        .db_client
        .database("litiaina-storage")
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
                warn!(
                    "Failed to delete physical file version during cleanup: {}",
                    e
                );
            }
        }
    }

    info!(
        "Deleted all versions of object: {}/{}",
        fragment, object_key
    );
    Ok(StatusCode::NO_CONTENT)
}

pub async fn list_objects(
    State(state): State<Arc<AppState>>,
    header: HeaderMap,
    Path(fragment): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let uid = extract_uid_from_fragmented_header(&header)?;
    let collection = state
        .db_client
        .database("litiaina-storage")
        .collection::<ObjectMetadata>("objects");
    let limit: i64 = params
        .get("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    let marker = params.get("marker").cloned();

    let mut query = doc! { "fragment": fragment, "owner_uid": uid, "is_latest": true };
    if let Some(marker_key) = marker {
        query.insert("key", doc! { "$gt": marker_key });
    }

    let find_options = mongodb::options::FindOptions::builder()
        .limit(limit)
        .sort(doc! { "key": 1 })
        .build();

    let mut cursor = collection
        .find(query)
        .with_options(find_options)
        .await
        .map_err(|_| AppError::Conflict("Database query failed.".to_string()))?;

    let mut objects = Vec::new();
    while let Some(result) = cursor
        .try_next()
        .await
        .map_err(|_| AppError::Conflict("Failed to read from database cursor.".to_string()))?
    {
        objects.push(result);
    }
    Ok(Json(objects))
}

async fn find_latest_object_metadata(
    db: &MongoClient,
    fragment: &str,
    object_key: &str,
    uid: &str,
) -> Result<ObjectMetadata, AppError> {
    let collection = db
        .database("litiaina-storage")
        .collection::<ObjectMetadata>("objects");

    collection
        .find_one(
            doc! { "fragment": fragment, "key": object_key, "owner_uid": uid, "is_latest": true },
        )
        .await
        .map_err(|_| AppError::Conflict("Database query failed.".to_string()))?
        .ok_or(AppError::BadRequest("Object not found.".to_string()))
}
