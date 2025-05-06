use std::path::PathBuf;
use std::sync::Arc;
use axum::extract::{Multipart, Path};
use axum::http::StatusCode;
use axum::Json;
use axum::response::IntoResponse;
use serde_json::json;
use tokio::fs;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::{Mutex, Semaphore};
use tracing::{error, info, warn};
use crate::fs::fs_constants::{DEFAULT_BUFFER_SIZE, MAX_CONCURRENT};
use crate::utils::directory_validation::sanitize_directory_name;
use crate::utils::file_extension_util::get_file_extension;

pub async fn upload_file(
    Path(directory): Path<String>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, StatusCode> {
    let sanitized_directory = sanitize_directory_name(&directory);
    let directory_path = PathBuf::from(format!("storage-files/{}", sanitized_directory));

    fs::create_dir_all(&directory_path)
        .await
        .map_err(|err| {
            error!("Failed to create directory: {err}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    loop {
        let mut field = match multipart.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => break,
            Err(err) => {
                error!("Failed to read field: {err}");
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        };

        let original_filename = field.file_name().unwrap_or("upload").to_string();
        let content_type = field.content_type().ok_or_else(|| {
            warn!("Missing content type");
            StatusCode::BAD_REQUEST
        })?;

        let path = std::path::Path::new(&original_filename);
        let stem = path.file_stem()
            .map(|s| s.to_string_lossy())
            .unwrap_or_else(|| "upload".into());
        let ext = path.extension()
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

        let file = fs::File::create(&temp_path).await.map_err(|err| {
            error!("Failed to create temp file: {err}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        let writer = Arc::new(Mutex::new(BufWriter::with_capacity(DEFAULT_BUFFER_SIZE, file)));
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT));
        let mut tasks = Vec::new();
        let mut total_bytes = 0usize;

        loop {
            let chunk = match field.chunk().await {
                Ok(Some(data)) => data,
                Ok(None) => break,
                Err(err) => {
                    error!("Failed to read chunk: {err}");
                    return Err(StatusCode::INTERNAL_SERVER_ERROR);
                }
            };

            total_bytes += chunk.len();
            let writer_clone = Arc::clone(&writer);
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let chunk_copy = chunk.clone();

            let task = tokio::spawn(async move {
                let mut writer = writer_clone.lock().await;
                writer.write_all(&chunk_copy).await.map_err(|err| {
                    error!("Failed to write chunk: {err}");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;
                drop(permit);
                Ok::<_, StatusCode>(())
            });

            tasks.push(task);
        }

        for task in tasks {
            task.await.map_err(|err| {
                error!("Join error during chunk write: {err:?}");
                StatusCode::INTERNAL_SERVER_ERROR
            })??;
        }

        let mut writer = writer.lock().await;
        writer.flush().await.map_err(|err| {
            error!("Failed to flush buffer: {err}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        writer.get_mut().sync_all().await.map_err(|err| {
            error!("Failed to sync file to disk: {err}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        fs::rename(&temp_path, &final_path).await.map_err(|err| {
            error!("Failed to rename file: {err}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        info!(
            "Uploaded '{}' to '{}', total bytes: {}",
            final_filename, sanitized_directory, total_bytes
        );
    }

    Ok(Json(json!({ "message": "File uploaded successfully" })))
}
