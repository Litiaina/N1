use crate::fs::fs_constants::DEFAULT_BUFFER_SIZE;
use crate::structs::auth_structs::FileSystemClaims;
use crate::utils::file_extension_util::get_file_extension;
use crate::validation::directory_validation::sanitize_path;
use axum::Json;
use axum::extract::{Multipart, Path};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use futures_util::TryStreamExt;
use serde_json::json;
use std::path::PathBuf;
use tokio::fs;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio_util::io::StreamReader;
use tracing::{error, info, warn};

pub async fn upload_file(
    claims: FileSystemClaims,
    path: Option<Path<String>>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, StatusCode> {
    if !&claims.write {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let validated_fs_path = &claims.path;

    let relative_path = path
        .as_ref()
        .map(|Path(p)| sanitize_path(p))
        .unwrap_or_default();

    let directory_path = if relative_path.is_empty() {
        PathBuf::from(format!("{}", validated_fs_path))
    } else {
        PathBuf::from(format!("{}/{}", validated_fs_path, relative_path))
    };

    fs::create_dir_all(&directory_path).await.map_err(|err| {
        error!("Failed to create directory: {err}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    while let Some(field) = multipart.next_field().await.map_err(|err| {
        error!("Failed to read multipart field: {err}");
        StatusCode::INTERNAL_SERVER_ERROR
    })? {
        let original_filename = field.file_name().unwrap_or("upload").to_string();
        let content_type = field.content_type().ok_or_else(|| {
            warn!("Missing content type");
            StatusCode::BAD_REQUEST
        })?;

        let path = std::path::Path::new(&original_filename);
        let stem = path
            .file_stem()
            .map(|s| s.to_string_lossy())
            .unwrap_or_else(|| "upload".into());
        let ext = path
            .extension()
            .map(|e| e.to_string_lossy())
            .or_else(|| get_file_extension(content_type).map(|e| e.into()))
            .ok_or_else(|| {
                error!("Could not determine file extension for: {original_filename}");
                StatusCode::UNSUPPORTED_MEDIA_TYPE
            })?;

        let mut counter = 0;
        let final_filename = loop {
            let candidate = if counter == 0 {
                format!("{}.{}", stem, ext)
            } else {
                format!("{}[{}].{}", stem, counter, ext)
            };
            let test_path = directory_path.join(&candidate);

            if !fs::try_exists(&test_path).await.map_err(|err| {
                error!("Failed to check for existing file: {err}");
                StatusCode::INTERNAL_SERVER_ERROR
            })? {
                break candidate;
            }

            counter += 1;
        };

        let temp_filename = format!("{}.part", final_filename);
        let temp_path = directory_path.join(&temp_filename);
        let final_path = directory_path.join(&final_filename);

        let temp_file = fs::File::create(&temp_path).await.map_err(|err| {
            error!("Failed to create temp file: {err}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        let mut writer = BufWriter::with_capacity(DEFAULT_BUFFER_SIZE, temp_file);

        let field_stream = field
            .into_stream()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
        let mut reader = StreamReader::new(field_stream);

        let bytes_copied = tokio::io::copy(&mut reader, &mut writer)
            .await
            .map_err(|e| {
                error!("Failed to copy data: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

        writer.flush().await.map_err(|e| {
            error!("Failed to flush writer: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        writer.get_mut().sync_all().await.map_err(|e| {
            error!("Failed to sync file: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        fs::rename(&temp_path, &final_path).await.map_err(|e| {
            error!("Failed to rename file: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        if relative_path.is_empty() {
            info!(
                "Uploaded '{}' to '{}', total bytes: {}",
                final_filename, validated_fs_path, bytes_copied
            );
        } else {
            info!(
                "Uploaded '{}' to '{}/{}', total bytes: {}",
                final_filename, validated_fs_path, relative_path, bytes_copied
            );
        }
    }

    Ok(Json(json!({ "message": "File uploaded successfully" })))
}
