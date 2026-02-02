use crate::fs::fs_constants::INITIAL_CHUNK_SIZE;
use crate::object_storage::header_parser::parse_range_header;
use crate::structs::auth_structs::FileSystemClaims;
use crate::structs::file_detail_params_struct::FileDetail;
use crate::validation::directory_validation::sanitize_path;
use axum::Json;
use axum::body::{Body, Bytes};
use axum::extract::Path;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use futures_util::stream::StreamExt;
use mime_guess::from_path;
use serde_json::json;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};
use tokio::{fs, task};
use tokio_util::io::ReaderStream;
use tracing::error;
use walkdir::WalkDir;

pub async fn view_directory(
    claims: FileSystemClaims,
    path: Option<Path<String>>,
) -> Result<impl IntoResponse, StatusCode> {
    let validated_uid = claims.uid.clone();

    let relative_path = path
        .as_ref()
        .map(|Path(p)| sanitize_path(p))
        .unwrap_or_default();

    let user_storage_root = PathBuf::from(format!("storage-files/{}", validated_uid));

    let full_path = if relative_path.is_empty() {
        user_storage_root.clone()
    } else {
        user_storage_root.join(&relative_path)
    };

    if !full_path.is_dir() {
        error!(
            "Requested path not found or not a directory: {:?}",
            full_path
        );
        return Err(StatusCode::NOT_FOUND);
    }

    let root_clone = user_storage_root.clone();

    let files_result = task::spawn_blocking(move || {
        let mut files = Vec::new();

        for entry in WalkDir::new(&full_path)
            .min_depth(1)
            .into_iter()
            .filter_map(Result::ok)
        {
            let entry_path = entry.path();

            let file_name = match entry.file_name().to_str() {
                Some(name) => name.to_string(),
                None => continue,
            };

            if let Ok(metadata) = entry.metadata() {
                let unix_timestamp = metadata
                    .created()
                    .ok()
                    .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                    .map_or(0, |d| d.as_secs());

                let is_dir = metadata.is_dir();
                let mime_type = if is_dir {
                    "directory".to_string()
                } else {
                    from_path(entry_path).first_or_octet_stream().to_string()
                };

                let file_size = if is_dir { 0 } else { metadata.len() };

                let relative_entry_path = entry_path
                    .strip_prefix(&root_clone)
                    .unwrap_or(entry_path)
                    .to_string_lossy()
                    .replace('\\', "/");

                files.push(FileDetail {
                    name: file_name,
                    path: relative_entry_path,
                    created: unix_timestamp,
                    file_type: mime_type,
                    size: file_size,
                });
            }
        }
        files
    })
    .await;

    match files_result {
        Ok(files) => Ok(Json(json!({ "files": files }))),
        Err(e) => {
            error!("Directory scan task failed: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn view_file(
    claims: FileSystemClaims,
    Path(path): Path<String>,
) -> Result<impl IntoResponse, StatusCode> {
    let validated_uid = &claims.uid;
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
    claims: FileSystemClaims,
    headers: HeaderMap,
    Path(path): Path<String>,
) -> Result<impl IntoResponse, StatusCode> {
    let validated_uid = claims.uid;
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
    claims: FileSystemClaims,
    headers: HeaderMap,
    Path(path): Path<String>,
) -> Result<impl IntoResponse, Response> {
    let sanitized_directory = sanitize_path(&path);
    let file_path = PathBuf::from(format!(
        "storage-files/{}/{}",
        &claims.uid, sanitized_directory
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
    claims: FileSystemClaims,
    Path(path): Path<String>,
) -> Result<impl IntoResponse, Response> {
    let sanitized_directory = sanitize_path(&path);
    let file_path = PathBuf::from(format!(
        "storage-files/{}/{}",
        &claims.uid, sanitized_directory
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
