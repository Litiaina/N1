use crate::fs::fs_constants::INITIAL_CHUNK_SIZE;
use crate::structs::file_detail_params_struct::FileDetail;
use crate::structs::uid_params_struct::UIDParams;
use crate::utils::header_extractor::extract_uid_from_header;
use crate::validation::directory_validation::{get_dir_size, sanitize_path};
use axum::Json;
use axum::body::{Body, Bytes};
use axum::extract::{Path, Query};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use futures_util::stream::StreamExt;
use mime_guess::from_path;
use serde_json::json;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};
use tokio_util::io::ReaderStream;
use crate::object_storage::header_parser::parse_range_header;

pub async fn view_directory(
    header: HeaderMap,
    path: Option<Path<String>>,
) -> Result<impl IntoResponse, StatusCode> {
    let validated_uid = extract_uid_from_header(&header).map_err(|code| code)?;

    let relative_path = path
        .as_ref()
        .map(|Path(p)| sanitize_path(p))
        .unwrap_or_default();

    let full_path = if relative_path.is_empty() {
        format!("storage-files/{}", validated_uid)
    } else {
        format!("storage-files/{}/{}", validated_uid, relative_path)
    };

    let file_path = PathBuf::from(full_path);
    let mut files = Vec::new();

    match fs::read_dir(&file_path).await {
        Ok(mut dir) => {
            while let Ok(Some(entry)) = dir.next_entry().await {
                let path = entry.path();
                let file_name = match entry.file_name().into_string() {
                    Ok(name) => name,
                    Err(_) => continue,
                };

                if let Ok(metadata) = entry.metadata().await {
                    let created = metadata.created().ok();
                    let unix_timestamp = created
                        .and_then(|created| created.duration_since(UNIX_EPOCH).ok())
                        .map_or(0, |duration| duration.as_secs());

                    let mime_type = if metadata.is_dir() {
                        "directory".to_string()
                    } else {
                        from_path(&path).first_or_octet_stream().to_string()
                    };

                    let file_size = if metadata.is_dir() {
                        get_dir_size(&path).await
                    } else {
                        metadata.len()
                    };

                    files.push(FileDetail {
                        name: file_name,
                        created: unix_timestamp,
                        file_type: mime_type,
                        size: file_size,
                    });
                }
            }
        }
        Err(_) => return Err(StatusCode::NOT_FOUND),
    };

    let response_body = json!({ "files": files });
    Ok(Json(response_body))
}

pub async fn view_file(
    Query(params): Query<UIDParams>,
    Path(path): Path<String>,
) -> Result<impl IntoResponse, StatusCode> {
    let validated_uid = if !params.uid.is_empty() {
        params.uid
    } else {
        return Err(StatusCode::BAD_REQUEST);
    };
    let sanitized_directory = sanitize_path(&path);
    let file_path = PathBuf::from(format!(
        "storage-files/{}/{}",
        validated_uid, sanitized_directory
    ));
    let mut file = fs::File::open(&file_path)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    let mut buffer = Vec::new();

    file.read_to_end(&mut buffer)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let bytes = Bytes::from(buffer);
    let mime_type = from_path(&file_path).first_or_octet_stream();

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", mime_type.to_string())
        .header("Cache-Control", "max-age=3600")
        .body(Body::from(bytes))
        .unwrap()
        .into_response())
}

pub async fn stream_file(
    Query(params): Query<UIDParams>,
    headers: HeaderMap,
    Path(path): Path<String>,
) -> Result<impl IntoResponse, StatusCode> {
    let validated_uid = if !params.uid.is_empty() {
        params.uid
    } else {
        return Err(StatusCode::BAD_REQUEST);
    };
    let sanitized_directory = sanitize_path(&path);
    let file_path = PathBuf::from(format!(
        "storage-files/{}/{}",
        validated_uid, sanitized_directory
    ));

    let mime_type = from_path(&file_path).first_or_octet_stream();

    let mut file = fs::File::open(&file_path)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;

    let file_size = file
        .metadata()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .len();

    let range_header = headers.get("range").and_then(|v| v.to_str().ok());

    let (start, end, status_code) = if let Some(range) = range_header {
        if let Some(range) = range.strip_prefix("bytes=") {
            let mut parts = range.split('-');
            let start = parts.next().unwrap_or("0").parse::<u64>().unwrap_or(0);
            let end = parts
                .next()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(file_size - 1);

            if start > end || end >= file_size {
                return Err(StatusCode::RANGE_NOT_SATISFIABLE);
            }

            (start, end, StatusCode::PARTIAL_CONTENT)
        } else {
            return Err(StatusCode::RANGE_NOT_SATISFIABLE);
        }
    } else {
        (0, file_size - 1, StatusCode::OK)
    };

    file.seek(SeekFrom::Start(start))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let reader = BufReader::new(file).take(end - start + 1);
    let stream = ReaderStream::new(reader).map(|result| {
        result
            .map(Bytes::from)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Stream error"))
    });

    let body = Body::from_stream(stream);

    let mut response = Response::builder()
        .status(status_code)
        .header("Content-Type", mime_type.to_string())
        .header("Content-Length", (end - start + 1).to_string());

    if status_code == StatusCode::PARTIAL_CONTENT {
        response = response.header(
            "Content-Range",
            format!("bytes {}-{}/{}", start, end, file_size),
        );
    }

    Ok(response.body(body).unwrap())
}

pub async fn stream_file_v2(
    Query(params): Query<UIDParams>,
    headers: HeaderMap,
    Path(path): Path<String>,
) -> Result<impl IntoResponse, Response> {
    if params.uid.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "UID cannot be empty.").into_response());
    }

    let sanitized_directory = sanitize_path(&path);
    let file_path = PathBuf::from(format!(
        "storage-files/{}/{}",
        params.uid, sanitized_directory
    ));

    let metadata = fs::metadata(&file_path)
        .await
        .map_err(|_| (StatusCode::NOT_FOUND, "File not found.").into_response())?;

    let file_size = metadata.len();
    let mime_type = from_path(&file_path).first_or_octet_stream();
    let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    let (start, end) = parse_range_header(range_header, file_size)
        .unwrap_or((0, INITIAL_CHUNK_SIZE.min(file_size).saturating_sub(1)));

    if start >= file_size || end >= file_size || start > end {
        return Err(StatusCode::RANGE_NOT_SATISFIABLE.into_response());
    }

    let mut file = fs::File::open(&file_path)
        .await
        .map_err(|_| (StatusCode::NOT_FOUND, "File not found.").into_response())?;

    file.seek(SeekFrom::Start(start))
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Error seeking file.").into_response())?;

    let content_length = end - start + 1;
    let reader = BufReader::new(file);
    let stream = ReaderStream::new(reader.take(content_length));
    let body = Body::from_stream(stream);

    let response = Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(header::CONTENT_TYPE, mime_type.to_string())
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, content_length.to_string())
        .header(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", start, end, file_size),
        )
        .header(header::CACHE_CONTROL, "no-cache")
        .body(body)
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to build response.",
            )
                .into_response()
        })?;

    Ok(response)
}

pub async fn stream_hls_file(
    Query(params): Query<UIDParams>,
    Path(path): Path<String>,
) -> Result<impl IntoResponse, Response> {
    if params.uid.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "UID cannot be empty.").into_response());
    }

    let sanitized_directory = sanitize_path(&path);
    let file_path = PathBuf::from(format!(
        "storage-files/{}/{}",
        params.uid, sanitized_directory
    ));

    let file_name = match std::path::Path::new(&path)
        .file_name()
        .and_then(|s| s.to_str())
    {
        Some(name) => name,
        None => return Err((StatusCode::BAD_REQUEST, "Invalid file path.").into_response()),
    };

    if !file_name.ends_with(".m3u8") && !file_name.ends_with(".ts") {
        return Err((StatusCode::BAD_REQUEST, "Invalid file type.").into_response());
    }

    let metadata = match fs::metadata(&file_path).await {
        Ok(meta) => meta,
        Err(_) => return Err((StatusCode::NOT_FOUND, "File not found.").into_response()),
    };

    let mime_type = if file_name.ends_with(".m3u8") {
        "application/vnd.apple.mpegurl"
    } else {
        "video/mp2t"
    };
    let file_size = metadata.len();
    let file = fs::File::open(&file_path)
        .await
        .map_err(|_| (StatusCode::NOT_FOUND, "File not found.").into_response())?;
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, mime_type)
        .header(header::CONTENT_LENGTH, file_size.to_string())
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CACHE_CONTROL, "public, max-age=3600")
        .body(body)
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to build response.",
            )
                .into_response()
        })?;

    Ok(response)
}
