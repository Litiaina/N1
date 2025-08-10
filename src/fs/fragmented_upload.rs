use crate::enums::app_error_enum::AppError;
use crate::fs::fs_constants::{
    CHUNK_PREFIX, IO_TIMEOUT, MAX_CHUNK_SIZE, MERGE_CONCURRENCY, STORAGE_ROOT, TMP_EXT,
};
use crate::statics::merge_semaphore_static::MERGE_SEMAPHORE;
use crate::structs::path_manager_struct::PathManager;
use crate::structs::upload_metadata_struct::UploadMetadata;
use crate::structs::upload_params_struct::UploadParams;
use crate::types::shared_state_type::SharedState;
use crate::utils::header_extractor::extract_uid_from_fragmented_header;
use crate::validation::directory_validation::sanitize_fragmented_path;
use axum::{
    body::Body,
    extract::{Multipart, Path, Query, State},
    http::{HeaderMap, Response, StatusCode},
    response::{IntoResponse, Json},
};
use futures_util::stream::TryStreamExt;
use sha2::{Digest, Sha256};
use std::{
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

impl IntoResponse for AppError {
    fn into_response(self) -> Response<Body> {
        let (status, message) = match self {
            AppError::Io(e, context) => {
                error!("IO error during '{}': {}", context, e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("IO operation failed: {}", context),
                )
            }
            AppError::Timeout(context) => {
                error!("Timeout during '{}'", context);
                (
                    StatusCode::REQUEST_TIMEOUT,
                    format!("Operation timed out: {}", context),
                )
            }
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.to_string()),
            AppError::Conflict(msg) => (StatusCode::CONFLICT, msg.to_string()),
            AppError::PayloadTooLarge(msg) => (StatusCode::PAYLOAD_TOO_LARGE, msg.to_string()),
            AppError::Unauthorized => (StatusCode::UNAUTHORIZED, "Unauthorized".to_string()),
            AppError::Multipart(e) => {
                error!("Multipart error: {}", e);
                (
                    StatusCode::BAD_REQUEST,
                    "Invalid multipart request".to_string(),
                )
            }
        };
        (status, message).into_response()
    }
}

impl PathManager {
    fn new(uid: String, relative_path: String, file_id: String) -> Self {
        Self {
            uid,
            relative_path: sanitize_fragmented_path(&relative_path),
            file_id,
        }
    }
    fn upload_dir(&self) -> PathBuf {
        PathBuf::from(STORAGE_ROOT)
            .join(&self.uid)
            .join(&self.relative_path)
            .join(&self.file_id)
    }
    fn chunk_path(&self, index: usize) -> PathBuf {
        self.upload_dir().join(format!("{}{}", CHUNK_PREFIX, index))
    }
}

pub async fn upload_fragments(
    State(state): State<SharedState>,
    header: HeaderMap,
    path: Option<Path<String>>,
    Query(params): Query<UploadParams>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, AppError> {
    let uid = extract_uid_from_fragmented_header(&header)?;
    if params.chunk_index >= params.total_chunks {
        return Err(AppError::BadRequest("Invalid chunk index."));
    }

    let path_manager = PathManager::new(
        uid.clone(),
        path.map_or_else(String::new, |p| p.0),
        params.file_id.clone(),
    );

    with_io_timeout(
        "create upload directory",
        fs::create_dir_all(path_manager.upload_dir()),
    )
    .await?;

    let field = with_timeout("reading multipart field", multipart.next_field())
        .await?
        .map_err(AppError::Multipart)?
        .ok_or(AppError::BadRequest("No multipart field found in request."))?;

    save_and_verify_chunk_stream(&path_manager, params.chunk_index, field, &params.checksum)
        .await?;

    let all_chunks_uploaded = update_upload_state(
        &state,
        &params.file_id,
        params.chunk_index,
        params.total_chunks,
    )
    .await;

    if all_chunks_uploaded {
        state.lock().await.remove(&params.file_id);
        tokio::spawn(async move {
            info!(
                "All chunks for file_id '{}' received. Spawning merge task.",
                params.file_id
            );
            if let Err(e) = assemble_and_finalize_file(
                path_manager,
                params.total_chunks,
                params.original_filename,
                params.mime_type,
            )
            .await
            {
                error!(
                    "Failed to assemble file_id '{}'. Error: {:?}",
                    params.file_id, e
                );
            }
        });
        Ok((
            StatusCode::ACCEPTED,
            "All chunks received; merge process started.",
        )
            .into_response())
    } else {
        Ok((StatusCode::OK, "Chunk uploaded successfully.").into_response())
    }
}

pub async fn check_uploaded_fragments(
    State(state): State<SharedState>,
    header: HeaderMap,
    Path(file_id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let _uid = extract_uid_from_fragmented_header(&header)?;
    let uploaded_chunks = state
        .lock()
        .await
        .get(&file_id)
        .cloned()
        .unwrap_or_default();
    Ok(Json(uploaded_chunks))
}

async fn save_and_verify_chunk_stream(
    paths: &PathManager,
    index: usize,
    field: axum::extract::multipart::Field<'_>,
    expected_checksum: &Option<String>,
) -> Result<(), AppError> {
    let final_path = paths.chunk_path(index);
    if fs::try_exists(&final_path).await.unwrap_or(false) {
        return Err(AppError::Conflict("Chunk already exists."));
    }
    let temp_path = final_path.with_extension(TMP_EXT);
    let mut file = with_io_timeout("creating chunk file", File::create(&temp_path)).await?;
    let mut writer = BufWriter::new(&mut file);
    let mut hasher = Sha256::new();
    let mut total_written = 0;
    let mut stream = field.map_err(AppError::Multipart);

    loop {
        match with_timeout("reading chunk stream", stream.try_next()).await? {
            Ok(Some(chunk)) => {
                total_written += chunk.len();
                if total_written > MAX_CHUNK_SIZE {
                    let _ = fs::remove_file(&temp_path).await;
                    return Err(AppError::PayloadTooLarge("Chunk size exceeds the limit."));
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

    if let Some(expected) = expected_checksum {
        let computed = hex::encode(hasher.finalize());
        if expected != &computed {
            warn!(
                "Checksum mismatch for chunk {}: expected {}, got {}",
                index, expected, computed
            );
            let _ = fs::remove_file(&temp_path).await;
            return Err(AppError::BadRequest("Checksum mismatch."));
        }
    }

    with_io_timeout("finalizing chunk", fs::rename(&temp_path, &final_path)).await?;
    Ok(())
}

async fn update_upload_state(
    state: &SharedState,
    file_id: &str,
    index: usize,
    total: usize,
) -> bool {
    let mut guard = state.lock().await;
    let entry = guard
        .entry(file_id.to_string())
        .or_insert_with(|| vec![false; total]);
    if index < entry.len() {
        entry[index] = true;
    }
    entry.iter().all(|&uploaded| uploaded)
}

async fn assemble_and_finalize_file(
    paths: PathManager,
    total_chunks: usize,
    original_filename: String,
    mime_type: Option<String>,
) -> Result<impl IntoResponse, AppError> {
    let semaphore = MERGE_SEMAPHORE
        .get_or_init(|| async { Semaphore::new(MERGE_CONCURRENCY) })
        .await;
    let _permit = semaphore
        .acquire()
        .await
        .expect("Semaphore is never closed");

    info!("Starting assembly for file_id: {}", paths.file_id);
    let final_path =
        determine_final_path(&paths.upload_dir().parent().unwrap(), &original_filename).await?;
    let temp_merged_path = final_path.with_extension("merged.tmp");

    {
        let mut output_file =
            with_io_timeout("creating merged file", File::create(&temp_merged_path)).await?;
        let mut writer = BufWriter::new(&mut output_file);
        for i in 0..total_chunks {
            let chunk_path = paths.chunk_path(i);
            let mut chunk_file = with_io_timeout("opening chunk", File::open(&chunk_path)).await?;
            let mut reader = BufReader::new(&mut chunk_file);
            with_io_timeout("copying chunk data", io::copy(&mut reader, &mut writer)).await?;
        }
        with_io_timeout("flushing merged file", writer.flush()).await?;
    }

    with_io_timeout(
        "renaming merged file",
        fs::rename(&temp_merged_path, &final_path),
    )
    .await?;
    with_io_timeout("cleaning up chunks", fs::remove_dir_all(paths.upload_dir())).await?;
    info!("Successfully assembled file: {}", final_path.display());

    let metadata = UploadMetadata {
        original_filename,
        mime_type,
        uploaded_at: chrono::Utc::now().to_rfc3339(),
    };

    Ok((StatusCode::OK, Json(metadata)))
}

async fn determine_final_path(
    base_dir: &StdPath,
    original_filename: &str,
) -> Result<PathBuf, AppError> {
    let mut final_path = base_dir.join(original_filename);
    if !fs::try_exists(&final_path)
        .await
        .map_err(|e| AppError::Io(e, "checking final path"))?
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

    for i in 1..=1000 {
        let new_name = format!("{} ({}){}", stem, i, ext);
        let candidate = base_dir.join(&new_name);
        if !fs::try_exists(&candidate)
            .await
            .map_err(|e| AppError::Io(e, "checking candidate path"))?
        {
            final_path = candidate;
            break;
        }
    }
    Ok(final_path)
}

async fn with_timeout<F, T>(context: &'static str, fut: F) -> Result<T, AppError>
where
    F: Future<Output = T>,
{
    timeout(IO_TIMEOUT, fut)
        .await
        .map_err(|_| AppError::Timeout(context))
}

async fn with_io_timeout<F, T>(context: &'static str, fut: F) -> Result<T, AppError>
where
    F: Future<Output = Result<T, io::Error>>,
{
    match timeout(IO_TIMEOUT, fut).await {
        Ok(Ok(val)) => Ok(val),
        Ok(Err(e)) => Err(AppError::Io(e, context)),
        Err(_) => Err(AppError::Timeout(context)),
    }
}
