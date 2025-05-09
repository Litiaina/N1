use std::io::SeekFrom;
use axum::body::Body;
use std::path::PathBuf;
use axum::extract::{Path, Query};
use axum::http::{StatusCode};
use axum::response::{IntoResponse, Response};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use crate::structs::download_params_struct::DownloadParams;
use crate::validation::directory_validation::sanitize_path;

pub async fn download_file(Query(params): Query<DownloadParams>, path: Option<Path<String>>) -> impl IntoResponse {
    let validated_uid = if !params.uid.is_empty() {
        params.uid
    } else {
        return StatusCode::BAD_REQUEST.into_response()
    };
    
    let relative_path = path
        .as_ref()
        .map(|Path(p)| sanitize_path(p))
        .unwrap_or_default();

    let file_path = if relative_path.is_empty() {
        PathBuf::from(format!("storage-files/{}/{}", validated_uid, params.file_name))
    } else {
        PathBuf::from(format!("storage-files/{}/{}/{}", validated_uid, relative_path, params.file_name))
    };

    let mut file = match File::open(&file_path).await {
        Ok(f) => f,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };

    let mut buffer = vec![0; params.chunk_size];
    if let Err(_) = file.seek(SeekFrom::Start(params.offset)).await {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    let bytes_read = match file.read(&mut buffer).await {
        Ok(n) => n,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    if bytes_read == 0 {
        return StatusCode::NO_CONTENT.into_response();
    }

    let body = Body::from(buffer[..bytes_read].to_vec());

    Response::builder()
        .header("Content-Type", "application/octet-stream")
        .body(body)
        .unwrap()
}
