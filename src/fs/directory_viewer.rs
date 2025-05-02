use futures_util::stream::StreamExt;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;
use axum::body::{Body, Bytes};
use axum::extract::{Path, Query};
use axum::http::StatusCode;
use axum::Json;
use axum::response::{IntoResponse, Response};
use mime_guess::from_path;
use serde_json::json;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};
use tokio_util::io::ReaderStream;
use walkdir::WalkDir;
use crate::structs::file_detail_struct::FileDetail;
use crate::structs::range_header_struct::RangeHeader;
use crate::utils::directory_validation::sanitize_directory_name;

pub async fn view_directory(Path(path): Path<String>) -> impl IntoResponse {
    let sanitized_directory = sanitize_directory_name(&path);
    let file_path = PathBuf::from(format!("storage-files/{}", sanitized_directory));
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
        Err(_) => (),
    };

    let response_body = json!({ "files": files });
    Json(response_body)
}

pub async fn view_file(Path(path): Path<String>) -> Result<impl IntoResponse, StatusCode> {
    let file_path = PathBuf::from(format!("storage-files/{}", path));
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
    Path(path): Path<String>,
    Query(range_header): Query<RangeHeader>,
) -> Result<impl IntoResponse, StatusCode> {
    let file_path = format!("storage-files/{}", path);
    let file = fs::File::open(&file_path)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;

    let mime_type = from_path(&file_path).first_or_octet_stream();
    let file_size = file
        .metadata()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .len();

    let (start, end) = if let Some(range) = range_header.range {
        let range = range
            .strip_prefix("bytes=")
            .ok_or(StatusCode::RANGE_NOT_SATISFIABLE)?;

        let mut parts = range.split('-');
        let start = parts.next().unwrap_or("0").parse::<u64>().unwrap_or(0);
        let end = parts
            .next()
            .unwrap_or(&file_size.to_string())
            .parse::<u64>()
            .unwrap_or(file_size);

        if start > end || end >= file_size {
            return Err(StatusCode::RANGE_NOT_SATISFIABLE);
        }

        (start, end)
    } else {
        (0, file_size - 1)
    };

    let file = fs::File::open(file_path)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    let mut reader = BufReader::new(file);
    reader
        .seek(SeekFrom::Start(start))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let stream = ReaderStream::new(reader).map(|result| {
        result
            .map(|bytes| axum::body::Bytes::from(bytes))
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Stream error"))
    });

    let body = Body::from_stream(stream);

    Ok(Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header("Content-Type", mime_type.to_string())
        .header(
            "Content-Range",
            format!("bytes {}-{}/{}", start, end, file_size),
        )
        .header("Content-Length", end - start + 1)
        .body(body)
        .unwrap())
}

async fn get_dir_size(path: &PathBuf) -> u64 {
    let mut total_size = 0;

    for entry in WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
        let metadata = fs::metadata(entry.path()).await;

        if let Ok(metadata) = metadata {
            if metadata.is_file() {
                total_size += metadata.len();
            }
        }
    }

    total_size
}