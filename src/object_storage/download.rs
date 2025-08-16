use std::path::PathBuf;
use std::sync::Arc;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderValue};
use axum::response::IntoResponse;
use tower::ServiceExt;
use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::{OBJECT_DATA_ROOT, OBJECT_STORAGE_ROOT};
use crate::object_storage::error_parser::physical_path_error;
use crate::object_storage::metadata_parser::find_latest_object_metadata;
use crate::routes::storage_routes::AppState;
use crate::structs::object_storage_structs::ObjectMetadata;
use crate::utils::header_extractor::extract_uid_from_fragmented_header;

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
