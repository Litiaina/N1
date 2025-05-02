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
use tracing::{error, warn};
use crate::utils::directory_validation::sanitize_directory_name;
use crate::utils::file_extension_util::get_file_extension;

const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024;
const MAX_CONCURRENT: usize = 10;

pub async fn upload_file(
    Path(directory): Path<String>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, StatusCode> {
    let sanitized_directory = sanitize_directory_name(&directory);
    let directory_path = PathBuf::from(format!("storage-files/{}", sanitized_directory));
    fs::create_dir_all(&directory_path)
        .await
        .map_err(|err| {
            error!("Failed to create 'storage-files' folder: {}", err);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    while let Some(mut field) = multipart.next_field().await.map_err(|error| {
        tracing::error!("Error getting next field: {error}");
        StatusCode::INTERNAL_SERVER_ERROR
    })? {
        let name = field
            .name()
            .map(ToString::to_string)
            .unwrap_or_else(|| "name".to_owned());

        let file_type = field
            .content_type()
            .map(ToString::to_string)
            .ok_or_else(|| {
                warn!("No content type found");
                StatusCode::BAD_REQUEST
            })?;

        let file_extension = get_file_extension(&file_type).ok_or_else(|| {
            error!("Unsupported file type: {}", file_type);
            StatusCode::UNSUPPORTED_MEDIA_TYPE
        })?;

        let file_path = directory_path.join(format!("{}.{}", name, file_extension));
        let file = fs::File::create(&file_path).await.map_err(|error| {
            tracing::error!("Error creating file: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        let writer = BufWriter::with_capacity(DEFAULT_BUFFER_SIZE, file);
        let writer = Arc::new(Mutex::new(writer));

        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT));
        let mut chunk_tasks = vec![];

        while let Some(data) = field.chunk().await.map_err(|error| {
            tracing::error!("Error getting chunk: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        })? {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let writer_clone = writer.clone();

            let chunk_task = tokio::spawn(async move {
                let mut writer = writer_clone.lock().await;
                writer.write_all(&data).await.map_err(|error| {
                    tracing::error!("Error writing chunk to file: {error}");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;

                drop(permit);
                Ok::<(), StatusCode>(())
            });

            chunk_tasks.push(chunk_task);
        }

        for chunk_task in chunk_tasks {
            chunk_task.await.map_err(|error| {
                tracing::error!("Chunk write task failed: {error:?}");
                StatusCode::INTERNAL_SERVER_ERROR
            })??;
        }

        writer.lock().await.flush().await.map_err(|error| {
            tracing::error!("Error flushing file buffer: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    }

    Ok(Json(json!({"message": "File uploaded successfully"})))
}