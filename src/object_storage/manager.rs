use crate::enums::app_error_enum::AppError;
use crate::fs::fragmented_upload::with_io_timeout;
use crate::fs::fs_constants::INITIAL_CHUNK_SIZE;
use crate::object_storage::constants::{OBJECT_DATA_ROOT, OBJECT_STORAGE_ROOT};
use crate::object_storage::error_parser::physical_path_error;
use crate::object_storage::header_parser::parse_range_header;
use crate::object_storage::metadata_parser::find_latest_object_metadata;
use crate::routes::storage_routes::AppState;
use crate::structs::object_storage_structs::ObjectMetadata;
use crate::utils::header_extractor::extract_uid_from_fragmented_header;
use axum::Json as AxumJson;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use futures_util::stream::TryStreamExt;
use mongodb::bson::doc;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};
use tokio_util::io::ReaderStream;
use tower::ServiceExt;
use tracing::{info, warn};

pub async fn get_object(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((fragment, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = extract_uid_from_fragmented_header(&headers)?;
    let metadata =
        find_latest_object_metadata(&state.db_client, &fragment, &object_key, &uid).await?;
    stream_object_logic(headers, metadata).await
}

pub async fn download_object(
    State(state): State<Arc<AppState>>,
    header: HeaderMap,
    Path((fragment, object_key)): Path<(String, String)>,
    req: axum::http::Request<Body>,
) -> Result<impl IntoResponse, AppError> {
    let uid = extract_uid_from_fragmented_header(&header)?;
    let metadata =
        find_latest_object_metadata(&state.db_client, &fragment, &object_key, &uid).await?;
    download_object_logic(req, metadata).await
}

pub async fn download_object_logic(
    req: axum::http::Request<Body>,
    metadata: ObjectMetadata,
) -> Result<impl IntoResponse, AppError> {
    let physical_path = PathBuf::from(OBJECT_STORAGE_ROOT)
        .join(OBJECT_DATA_ROOT)
        .join(&metadata.physical_location);

    physical_path_error(&physical_path).await?;

    let service = tower_http::services::ServeFile::new(physical_path);
    match service.oneshot(req).await {
        Ok(mut response) => {
            let content_type = metadata.content_type;
            if let Ok(hv) = HeaderValue::from_str(&content_type) {
                response.headers_mut().insert(header::CONTENT_TYPE, hv);
            }
            response.headers_mut().insert(
                header::CONTENT_DISPOSITION,
                HeaderValue::from_str(&format!("attachment; filename=\"{}\"", metadata.key))
                    .unwrap_or(HeaderValue::from_static("attachment")),
            );
            Ok(response.into_response())
        }
        Err(err) => {
            tracing::error!("Failed to serve file for download: {}", err);
            Err(AppError::Conflict(format!("Failed to serve file: {}", err)))
        }
    }
}

pub async fn stream_object_logic(
    headers: HeaderMap,
    metadata: ObjectMetadata,
) -> Result<impl IntoResponse, AppError> {
    let physical_path = PathBuf::from(OBJECT_STORAGE_ROOT)
        .join(OBJECT_DATA_ROOT)
        .join(&metadata.physical_location);

    physical_path_error(&physical_path).await?;

    let file_size = metadata.size_bytes;
    let content_type = metadata.content_type;
    let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());

    let (start, end) = parse_range_header(range_header, file_size)
        .unwrap_or((0, INITIAL_CHUNK_SIZE.min(file_size).saturating_sub(1)));

    if start >= file_size || end >= file_size || start > end {
        return Err(AppError::RangeNotSatisfiable);
    }

    let mut file = File::open(&physical_path)
        .await
        .map_err(|e| AppError::Io(e, format!("opening file '{}'", physical_path.display())))?;

    file.seek(SeekFrom::Start(start))
        .await
        .map_err(|e| AppError::Io(e, format!("seeking in file '{}'", physical_path.display())))?;

    let content_length = end - start + 1;
    let reader = BufReader::new(file);
    let stream = ReaderStream::new(reader.take(content_length));
    let body = Body::from_stream(stream);

    let response = Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, content_length.to_string())
        .header(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", start, end, file_size),
        )
        .header(header::CACHE_CONTROL, "no-cache")
        .body(body)
        .map_err(|e| AppError::InternalServerError(format!("Failed to build response: {}", e)))?;

    Ok(response)
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
        .map_err(|_| AppError::Conflict("Failed to read from db cursor.".to_string()))?
    {
        objects.push(result);
    }
    Ok(AxumJson(objects))
}
