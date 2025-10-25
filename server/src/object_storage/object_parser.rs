use crate::enums::app_error_enum::AppError;
use crate::fs::fs_constants::INITIAL_CHUNK_SIZE;
use crate::object_storage::constants::{OBJECT_DATA_ROOT, OBJECT_STORAGE_ROOT};
use crate::object_storage::error_parser::physical_path_error;
use crate::object_storage::header_parser::parse_range_header;
use crate::structs::object_storage_structs::ObjectMetadata;
use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures_util::stream;
use std::io::SeekFrom;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tower::ServiceExt;
use tracing::error;

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

    let chunk_size: usize = match file_size {
        0..=10_000_000 => 64 * 1024,
        10_000_001..=100_000_000 => 128 * 1024,
        100_000_001..=1_000_000_000 => 256 * 1024,
        _ => 512 * 1024,
    };

    let stream = stream::unfold(
        (file, content_length),
        move |(mut file, mut remaining)| async move {
            if remaining == 0 {
                return None;
            }

            let mut buf = vec![0u8; chunk_size.min(remaining as usize)];
            match file.read(&mut buf).await {
                Ok(0) => None,
                Ok(n) => {
                    remaining -= n as u64;
                    buf.truncate(n);
                    let bytes = Bytes::from(buf);
                    Some((Ok::<Bytes, std::io::Error>(bytes), (file, remaining)))
                }
                Err(e) => Some((Err(e), (file, remaining))),
            }
        },
    );

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
        .header(header::CACHE_CONTROL, "public, max-age=3600")
        .body(body)
        .map_err(|e| AppError::InternalServerError(format!("Failed to build response: {}", e)))?;

    Ok(response)
}
