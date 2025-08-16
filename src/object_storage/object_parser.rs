use std::io::SeekFrom;
use std::path::PathBuf;
use axum::body::Body;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};
use tokio_util::io::ReaderStream;
use tower::ServiceExt;
use tracing::error;
use crate::enums::app_error_enum::AppError;
use crate::fs::fs_constants::INITIAL_CHUNK_SIZE;
use crate::object_storage::constants::{OBJECT_DATA_ROOT, OBJECT_STORAGE_ROOT};
use crate::object_storage::error_parser::physical_path_error;
use crate::object_storage::header_parser::parse_range_header;
use crate::structs::object_storage_structs::ObjectMetadata;

pub async fn get_object_logic(
    metadata: ObjectMetadata,
    req: axum::http::Request<Body>,
) -> Result<impl IntoResponse, AppError> {
    let physical_path = PathBuf::from(OBJECT_STORAGE_ROOT)
        .join(OBJECT_DATA_ROOT)
        .join(&metadata.physical_location);

    match physical_path_error(&physical_path).await {
        Ok(_) => {
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
        Err(err) => Err(err),
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