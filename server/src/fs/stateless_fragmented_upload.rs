use crate::enums::app_error_enum::AppError;
use crate::fs::fs_constants::{
    IO_TIMEOUT, MAX_BUFFER_SIZE, MAX_CHUNK_SIZE, MERGE_CONCURRENCY, MIN_BUFFER_SIZE, STORAGE_ROOT,
    TMP_EXT,
};
use crate::statics::merge_semaphore_static::MERGE_SEMAPHORE;
use crate::validation::directory_validation::sanitize_fragmented_path;

use crate::fs::file_path_parser::find_unique_path;
use crate::fs::fragmented_upload::with_io_timeout;
use crate::object_storage::part_parser::parse_uploaded_parts;
use crate::structs::auth_structs::FileSystemClaims;
use crate::structs::stateless_fragment_upload_structs::{
    InitiateUploadParams, InitiateUploadResponse, PathManager, UploadMetadata, UploadStatusResponse,
};
use axum::body::Body;
use axum::http::HeaderMap;
use axum::{
    Json as AxumJson,
    extract::{Multipart, Path, Query},
    http::StatusCode,
    response::{IntoResponse, Json},
};
use base64::Engine;
use base64::engine::general_purpose;
use futures_util::StreamExt;
use futures_util::stream::TryStreamExt;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    future::Future,
    path::{Path as StdPath, PathBuf},
};
use tokio::{
    fs::{self, File},
    io::{self, AsyncWriteExt, BufReader, BufWriter},
    sync::Semaphore,
    time::timeout,
};
use tracing::{error, info, warn};
use uuid::Uuid;

pub async fn initiate_upload_session(
    claims: FileSystemClaims,
    path: Option<Path<String>>,
    AxumJson(params): AxumJson<InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let file_id = Uuid::new_v4().to_string();
    let paths = PathManager::new(&file_id);

    with_io_timeout(
        "create temp upload directory",
        fs::create_dir_all(paths.temp_upload_dir()),
    )
    .await?;

    let final_relative_path = path.map_or_else(String::new, |p| p.0);

    let metadata = UploadMetadata {
        file_id: file_id.clone(),
        uid,
        original_filename: params.original_filename,
        final_relative_path,
        total_size_bytes: params.total_size_bytes,
        total_chunks: params.total_chunks,
        mime_type: params.mime_type,
        creation_timestamp: chrono::Utc::now().to_rfc3339(),
    };

    let metadata_content = serde_json::to_string_pretty(&metadata)
        .map_err(|e| AppError::BadRequest(format!("Failed to serialize metadata: {}", e)))?;

    with_io_timeout(
        "writing metadata file",
        fs::write(paths.metadata_path(), metadata_content),
    )
    .await?;

    info!("Initiated upload session: {}", file_id);
    Ok((
        StatusCode::CREATED,
        Json(InitiateUploadResponse { file_id }),
    ))
}

pub async fn upload_chunk_binary(
    path_params: Path<(String, u64)>,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, AppError> {
    let (file_id, chunk_index) = path_params.0;
    let paths = PathManager::new(&file_id);

    let expected_checksum_b64 = headers
        .get("Digest")
        .and_then(|value| value.to_str().ok())
        .and_then(|s| s.strip_prefix("sha-256="))
        .map(|s| s.to_string());

    let expected_buffer_size = headers
        .get("Buffer")
        .and_then(|value| value.to_str().ok())
        .and_then(|s| s.strip_prefix("memory-size="))
        .and_then(|s| s.parse::<usize>().ok());

    save_chunk_binary(
        &paths,
        chunk_index,
        body,
        expected_checksum_b64,
        expected_buffer_size,
    )
    .await?;

    Ok((StatusCode::OK, "Chunk uploaded successfully."))
}

pub async fn upload_chunk_multipart(
    path_params: Path<(String, u64)>,
    Query(params): Query<HashMap<String, String>>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, AppError> {
    let (file_id, chunk_index) = path_params.0;
    let paths = PathManager::new(&file_id);

    let field = with_timeout("reading multipart field", multipart.next_field())
        .await?
        .map_err(AppError::from)?
        .ok_or_else(|| AppError::BadRequest("No multipart field found in request.".to_string()))?;

    let expected_checksum = params.get("checksum").map(|s| s.as_str());

    save_chunk_multipart(&paths, chunk_index, field, expected_checksum).await?;

    Ok((StatusCode::OK, "Chunk uploaded successfully."))
}

pub async fn get_upload_status(Path(file_id): Path<String>) -> Result<impl IntoResponse, AppError> {
    let paths = PathManager::new(&file_id);

    let metadata: UploadMetadata = {
        let content = with_io_timeout(
            "reading metadata",
            fs::read_to_string(paths.metadata_path()),
        )
        .await?;
        serde_json::from_str(&content)
            .map_err(|_| AppError::Conflict("Invalid metadata file.".to_string()))?
    };

    let mut dir_reader = with_io_timeout(
        "reading temp upload directory",
        fs::read_dir(paths.temp_upload_dir()),
    )
    .await?;
    let uploaded_chunks = parse_uploaded_parts(&mut dir_reader).await;

    Ok(Json(UploadStatusResponse {
        original_filename: metadata.original_filename,
        total_size_bytes: metadata.total_size_bytes,
        uploaded_chunks,
        total_chunks: metadata.total_chunks,
    }))
}

pub async fn finalize_upload_session(
    Path(file_id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    info!("Finalization requested for upload session: {}", file_id);
    tokio::spawn(async move {
        let paths = PathManager::new(&file_id);
        if let Err(e) = assemble_and_finalize_file(paths).await {
            error!("Failed to assemble file_id '{}'. Error: {:?}", file_id, e);
        }
    });

    Ok((StatusCode::ACCEPTED, "Finalization process started."))
}

async fn save_chunk_binary(
    paths: &PathManager<'_>,
    index: u64,
    body: Body,
    expected_checksum_b64: Option<String>,
    chunk_buffer_size: Option<usize>,
) -> Result<(), AppError> {
    let final_path = paths.chunk_path(index);
    let temp_path = final_path.with_extension(TMP_EXT);
    let mut hasher = Sha256::new();

    let buffer_size = chunk_buffer_size
        .unwrap_or(MIN_BUFFER_SIZE)
        .min(MAX_BUFFER_SIZE);

    let file = with_io_timeout("creating chunk file", File::create(&temp_path)).await?;
    let mut writer = BufWriter::with_capacity(buffer_size, file);
    let mut total_written: usize = 0;

    let mut stream = body.into_data_stream();

    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result
            .map_err(|e| AppError::BadRequest(format!("Error reading request body: {}", e)))?;

        total_written += chunk.len();
        if total_written > MAX_CHUNK_SIZE {
            writer.flush().await.ok();
            drop(writer);
            let _ = fs::remove_file(&temp_path).await;
            return Err(AppError::PayloadTooLarge(
                "Chunk size exceeds limit.".to_string(),
            ));
        }
        hasher.update(&chunk);
        with_io_timeout("writing chunk data", writer.write_all(&chunk)).await?;
    }

    with_io_timeout("flushing chunk file", writer.flush()).await?;

    match expected_checksum_b64 {
        Some(expected_b64) => {
            let expected_bytes = general_purpose::STANDARD
                .decode(expected_b64)
                .map_err(|_| {
                    AppError::BadRequest("Invalid Base64 in Digest header.".to_string())
                })?;

            let computed_hash = hasher.finalize();
            if expected_bytes.as_slice() != computed_hash.as_ref() as &[u8] {
                warn!(
                    "Checksum mismatch for chunk {}: expected (b64) {}, got (hex) {}",
                    index,
                    general_purpose::STANDARD.encode(&expected_bytes),
                    hex::encode(computed_hash)
                );
                let _ = fs::remove_file(&temp_path).await;
                return Err(AppError::BadRequest("Checksum mismatch.".to_string()));
            }
        }
        None => {
            warn!(
                "Missing Digest header for binary chunk upload for chunk index {}",
                index
            );
            let _ = fs::remove_file(&temp_path).await;
            return Err(AppError::BadRequest(
                "Digest header is required for binary chunk uploads.".to_string(),
            ));
        }
    }

    with_io_timeout("finalizing chunk", fs::rename(&temp_path, &final_path)).await?;
    Ok(())
}

async fn save_chunk_multipart(
    paths: &PathManager<'_>,
    index: u64,
    field: axum::extract::multipart::Field<'_>,
    expected_checksum: Option<&str>,
) -> Result<(), AppError> {
    let final_path = paths.chunk_path(index);
    let temp_path = final_path.with_extension(TMP_EXT);
    let mut hasher = Sha256::new();

    {
        let mut file = with_io_timeout("creating chunk file", File::create(&temp_path)).await?;
        let mut writer = BufWriter::new(&mut file);
        let mut total_written = 0;
        let mut stream = field.map_err(AppError::from);

        loop {
            match with_timeout("reading chunk stream", stream.try_next()).await? {
                Ok(Some(chunk)) => {
                    total_written += chunk.len();
                    if total_written > MAX_CHUNK_SIZE {
                        let _ = fs::remove_file(&temp_path).await;
                        return Err(AppError::PayloadTooLarge(
                            "Chunk size exceeds limit.".to_string(),
                        ));
                    }
                    hasher.update(&chunk);
                    with_io_timeout("writing chunk data", writer.write_all(&chunk)).await?;
                }
                Ok(None) => break,
                Err(e) => {
                    let _ = fs::remove_file(&temp_path).await;
                    return Err(e);
                }
            }
        }
        with_io_timeout("flushing chunk file", writer.flush()).await?;
    }

    if let Some(expected) = expected_checksum {
        let computed = hex::encode(hasher.finalize());
        if expected != computed {
            warn!(
                "Checksum mismatch for chunk {}: expected {}, got {}",
                index, expected, computed
            );
            let _ = fs::remove_file(&temp_path).await;
            return Err(AppError::BadRequest("Checksum mismatch.".to_string()));
        }
    }

    with_io_timeout("finalizing chunk", fs::rename(&temp_path, &final_path)).await?;
    Ok(())
}

async fn assemble_and_finalize_file(paths: PathManager<'_>) -> Result<(), AppError> {
    let semaphore = MERGE_SEMAPHORE
        .get_or_init(|| async { Semaphore::new(MERGE_CONCURRENCY) })
        .await;
    let _permit = semaphore
        .acquire()
        .await
        .expect("Semaphore is never closed");

    let metadata: UploadMetadata = {
        let content = with_io_timeout(
            "reading metadata for assembly",
            fs::read_to_string(paths.metadata_path()),
        )
        .await?;
        serde_json::from_str(&content)
            .map_err(|_| AppError::Conflict("Invalid metadata file for assembly.".to_string()))?
    };

    info!("Starting assembly for file_id: {}", metadata.file_id);

    let final_destination_dir = PathBuf::from(STORAGE_ROOT)
        .join(&metadata.uid)
        .join(sanitize_fragmented_path(&metadata.final_relative_path));

    with_io_timeout(
        "create final destination directory",
        fs::create_dir_all(&final_destination_dir),
    )
    .await?;

    let final_path =
        determine_final_path(&final_destination_dir, &metadata.original_filename).await?;
    let temp_merged_path = paths
        .temp_upload_dir()
        .join(format!("{}.merged.tmp", metadata.file_id));

    {
        let output_file =
            with_io_timeout("creating merged file", File::create(&temp_merged_path)).await?;
        let mut writer = BufWriter::new(output_file);

        for i in 0..metadata.total_chunks {
            let chunk_path = paths.chunk_path(i);
            if !fs::try_exists(&chunk_path).await.unwrap_or(false) {
                error!(
                    "Missing chunk {} during assembly for file_id: {}",
                    i, metadata.file_id
                );
                let _ = fs::remove_file(&temp_merged_path).await;
                return Err(AppError::Conflict(format!(
                    "Missing chunk {} during final assembly.",
                    i
                )));
            }
            let mut chunk_file = with_io_timeout("opening chunk", File::open(&chunk_path)).await?;
            let mut reader = BufReader::new(&mut chunk_file);
            with_io_timeout("copying chunk data", io::copy(&mut reader, &mut writer)).await?;
        }
        with_io_timeout("flushing merged file", writer.flush()).await?;
    }

    with_io_timeout(
        "moving assembled file to final destination",
        fs::rename(&temp_merged_path, &final_path),
    )
    .await?;

    with_io_timeout(
        "cleaning up temp directory",
        fs::remove_dir_all(paths.temp_upload_dir()),
    )
    .await?;

    info!(
        "Successfully assembled and moved file: {}",
        final_path.display()
    );
    Ok(())
}

pub async fn determine_final_path(
    base_dir: &StdPath,
    original_filename: &str,
) -> Result<PathBuf, AppError> {
    let final_path = base_dir.join(original_filename);
    if !fs::try_exists(&final_path)
        .await
        .map_err(|e| AppError::from((e, "checking final path")))?
    {
        return Ok(final_path);
    }
    let stem = final_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or_default();
    let ext = final_path
        .extension()
        .and_then(|s| s.to_str())
        .map(|s| format!(".{}", s))
        .unwrap_or_default();

    if let Some(candidate) = find_unique_path(base_dir, stem, &ext, 1000).await? {
        Ok(candidate)
    } else {
        Err(AppError::Conflict(
            "Could not determine a unique filename.".to_string(),
        ))
    }
}

async fn with_timeout<F, T>(context: &'static str, fut: F) -> Result<T, AppError>
where
    F: Future<Output = T>,
{
    timeout(IO_TIMEOUT, fut)
        .await
        .map_err(|_| AppError::Timeout(context.to_string()))
}
