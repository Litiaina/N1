use crate::enums::app_error_enum::AppError;
use crate::fs::fs_constants::{IO_TIMEOUT, MAX_CHUNK_SIZE, MERGE_CONCURRENCY, TMP_EXT};
use crate::statics::merge_semaphore_static::{FILE_UPLOAD_LOCKS, MERGE_SEMAPHORE};
use crate::structs::path_manager_struct::PathManager;
use crate::structs::resumable_upload_status_struct::ResumableUploadStatus;
use crate::structs::upload_params_struct::UploadParams;
use crate::structs::upload_session_struct::UploadSession;
use axum::{
    extract::{Multipart, Path, Query, multipart::MultipartError},
    http::StatusCode,
    response::{IntoResponse, Json},
};
use futures_util::stream::TryStreamExt;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    future::Future,
};
use tokio::{
    fs::{self, File},
    io::{self, AsyncWriteExt, BufReader, BufWriter},
    sync::Semaphore,
    time::timeout,
};
use tracing::{error, info, warn};
use crate::fs::stateless_fragmented_upload::determine_final_path;
use crate::structs::auth_structs::Claims;

pub async fn upload_fragments(
    claims: Claims,
    path: Option<Path<String>>,
    Query(params): Query<UploadParams>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;

    if params.chunk_index >= params.total_chunks {
        return Err(AppError::BadRequest("Invalid chunk index.".to_string()));
    }

    let paths = PathManager::new(
        uid,
        path.map_or_else(String::new, |p| p.0),
        params.file_id.clone(),
    );

    with_io_timeout(
        "create upload directory",
        fs::create_dir_all(paths.upload_dir()),
    )
    .await?;

    let field = with_timeout("reading multipart field", multipart.next_field())
        .await?
        .map_err(AppError::from)?
        .ok_or_else(|| AppError::BadRequest("No multipart field found in request.".to_string()))?;

    save_and_verify_chunk_stream(&paths, params.chunk_index, field, &params.checksum).await?;

    let is_complete = update_session_and_check_completion(&paths, &params).await?;

    if is_complete {
        info!(
            "All chunks for file_id '{}' received. Spawning merge task.",
            params.file_id
        );
        tokio::spawn(async move {
            match UploadSession::from_disk(&paths).await {
                Ok(session) => {
                    let (file_id, original_filename, total_chunks) = (
                        session.file_id,
                        session.original_filename,
                        session.total_chunks,
                    );
                    if let Err(e) =
                        assemble_and_finalize_file(paths, original_filename, total_chunks).await
                    {
                        error!("Failed to assemble file_id '{}'. Error: {:?}", file_id, e);
                    }
                }
                Err(e) => {
                    error!(
                        "Could not load session for merging file_id '{}': {:?}",
                        paths.file_id, e
                    );
                }
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
    claims: Claims,
    path_params: Option<Path<String>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let file_id = params
        .get("file_id")
        .ok_or_else(|| AppError::BadRequest("Query param 'file_id' is required.".to_string()))?;

    let paths = PathManager::new(
        uid,
        path_params.map_or_else(String::new, |p| p.0),
        file_id.clone(),
    );

    match UploadSession::from_disk(&paths).await {
        Ok(session) => Ok(Json(ResumableUploadStatus {
            uploaded_chunks: session.uploaded_chunks,
        })),
        Err(_) => {
            warn!("No active upload session found for file_id '{}'.", file_id);
            Ok(Json(ResumableUploadStatus {
                uploaded_chunks: vec![],
            }))
        }
    }
}

async fn update_session_and_check_completion(
    paths: &PathManager,
    params: &UploadParams,
) -> Result<bool, AppError> {
    let file_lock = {
        let mut locks_manager = FILE_UPLOAD_LOCKS.lock().await;
        locks_manager
            .entry(params.file_id.clone())
            .or_default()
            .clone()
    };
    let _guard = file_lock.lock().await;

    let mut session = match UploadSession::from_disk(paths).await {
        Ok(s) => {
            if s.total_chunks != params.total_chunks
                || s.original_filename != params.original_filename
            {
                return Err(AppError::Conflict(
                    "Upload parameters mismatch with existing session.".to_string(),
                ));
            }
            s
        }
        Err(_) => UploadSession::new(paths, params).await?,
    };

    session.mark_chunk_as_uploaded(params.chunk_index);
    session.save().await?;

    Ok(session.is_complete())
}

async fn save_and_verify_chunk_stream(
    paths: &PathManager,
    index: usize,
    field: axum::extract::multipart::Field<'_>,
    expected_checksum: &Option<String>,
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
                            "Chunk size exceeds the limit.".to_string(),
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
        if expected != &computed {
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

async fn assemble_and_finalize_file(
    paths: PathManager,
    original_filename: String,
    total_chunks: usize,
) -> Result<(), AppError> {
    let semaphore = MERGE_SEMAPHORE
        .get_or_init(|| async { Semaphore::new(MERGE_CONCURRENCY) })
        .await;
    let _permit = semaphore
        .acquire()
        .await
        .expect("Semaphore is never closed");

    info!("Starting assembly for file_id: {}", paths.file_id);
    let final_path = determine_final_path(&paths.final_dir(), &original_filename).await?;
    let temp_merged_path = final_path.with_extension("merged.tmp");

    {
        let output_file =
            with_io_timeout("creating merged file", File::create(&temp_merged_path)).await?;
        let mut writer = BufWriter::new(output_file);

        for i in 0..total_chunks {
            let chunk_path = paths.chunk_path(i);
            if !fs::try_exists(&chunk_path).await.unwrap_or(false) {
                error!(
                    "Missing chunk {} during assembly for file_id: {}",
                    i, paths.file_id
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
        "renaming merged file",
        fs::rename(&temp_merged_path, &final_path),
    )
    .await?;

    with_io_timeout("cleaning up chunks", fs::remove_dir_all(paths.upload_dir())).await?;

    info!("Successfully assembled file: {}", final_path.display());
    Ok(())
}

pub async fn with_timeout<F, T>(context: &'static str, fut: F) -> Result<T, AppError>
where
    F: Future<Output = T>,
{
    timeout(IO_TIMEOUT, fut)
        .await
        .map_err(|_| AppError::Timeout(context.to_string()))
}

pub(crate) async fn with_io_timeout<F, T>(context: &'static str, fut: F) -> Result<T, AppError>
where
    F: Future<Output = Result<T, io::Error>>,
{
    match timeout(IO_TIMEOUT, fut).await {
        Ok(Ok(val)) => Ok(val),
        Ok(Err(e)) => Err(AppError::from((e, context))),
        Err(_) => Err(AppError::Timeout(context.to_string())),
    }
}
impl From<(io::Error, &'static str)> for AppError {
    fn from(err: (io::Error, &'static str)) -> Self {
        AppError::Io(err.0, err.1.to_string())
    }
}

impl From<MultipartError> for AppError {
    fn from(err: MultipartError) -> Self {
        AppError::Multipart(err.to_string())
    }
}
