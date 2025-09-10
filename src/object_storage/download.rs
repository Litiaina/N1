use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::{OBJECT_DATA_ROOT, OBJECT_STORAGE_ROOT};
use crate::object_storage::error_parser::physical_path_error;
use crate::object_storage::metadata_parser::find_latest_object_metadata;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{ChunkDownloadParams, ObjectMetadata};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, Response, StatusCode, header};
use axum::response::IntoResponse;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tower::ServiceExt;

pub async fn download_object(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
    req: axum::http::Request<Body>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
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

pub async fn download_object_chunk(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
    Query(params): Query<ChunkDownloadParams>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;

    let metadata =
        find_latest_object_metadata(&state.db_client, &fragment, &object_key, &uid).await?;

    let physical_path = PathBuf::from(OBJECT_STORAGE_ROOT)
        .join(OBJECT_DATA_ROOT)
        .join(&metadata.physical_location);

    let mut file = match File::open(&physical_path).await {
        Ok(f) => f,
        Err(e) => {
            tracing::error!(
                "Physical file not found for object '{}': {}",
                metadata.id,
                e
            );
            return Err(AppError::NotFound);
        }
    };

    if let Err(e) = file.seek(SeekFrom::Start(params.offset)).await {
        tracing::error!("Failed to seek in file {:?}: {}", physical_path, e);
        return Err(AppError::InternalServerError(
            "Failed to read file chunk.".to_string(),
        ));
    }

    let mut buffer = vec![0; params.chunk_size];
    if let Err(e) = file.read_exact(&mut buffer).await {
        tracing::error!(
            "Failed to read exact chunk from file {:?}: {}",
            physical_path,
            e
        );
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(StatusCode::NO_CONTENT.into_response());
        }
        return Err(AppError::InternalServerError(
            "Failed to read file chunk.".to_string(),
        ));
    }

    let body = Body::from(buffer);
    Ok(Response::builder()
        .header("Content-Type", "application/octet-stream")
        .body(body)
        .unwrap())
}
