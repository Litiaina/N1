use crate::structs::download_params_struct::DownloadParams;
use crate::validation::directory_validation::sanitize_path;
use axum::body::Body;
use axum::extract::{Path, Query};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use std::io::SeekFrom;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use crate::fs::file_path_parser::file_path;
use crate::structs::auth_structs::FileSystemClaims;

pub async fn download_file(
    claims: FileSystemClaims,
    Query(params): Query<DownloadParams>,
    path: Option<Path<String>>,
) -> impl IntoResponse {
    let validated_uid = &claims.uid;
    let relative_path = path
        .as_ref()
        .map(|Path(p)| sanitize_path(p))
        .unwrap_or_default();

    let file_path = file_path(validated_uid, &relative_path, &params.file_name).await;

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
