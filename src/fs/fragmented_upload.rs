use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use axum::extract::{Multipart, Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use tokio::{fs, io};
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::Semaphore;
use tokio::time::{sleep, timeout};
use tracing::{error, info, warn};
use crate::fs::fs_constants::{CLEANUP_INTERVAL, FILE_TTL, IO_TIMEOUT, MAX_CHUNK_SIZE};
use crate::statics::merge_semaphore_static::MERGE_SEMAPHORE;
use crate::structs::file_metadata_struct::FileMetadata;
use crate::structs::upload_params_struct::UploadParams;
use crate::types::shared_state_type::SharedState;
use crate::utils::directory_validation::sanitize_directory_name;

pub async fn upload_fragments(
    Path(path): Path<String>,
    Query(params): Query<UploadParams>,
    State(state): State<SharedState>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    let UploadParams {
        file_id,
        chunk_index,
        total_chunks,
        original_filename,
        mime_type,
    } = params;

    if chunk_index >= total_chunks {
        return (StatusCode::BAD_REQUEST, "Invalid chunk index").into_response();
    }

    let sanitized_directory = sanitize_directory_name(&path);
    let upload_dir = PathBuf::from(format!("storage-files/{}/{}", sanitized_directory, &file_id));
    fs::create_dir_all(&upload_dir).await.unwrap();

    let mut field = match timeout(IO_TIMEOUT, multipart.next_field()).await {
        Ok(Ok(Some(field))) => field,
        Ok(Ok(None)) => return (StatusCode::BAD_REQUEST, "No multipart field found").into_response(),
        Ok(Err(e)) => {
            error!("Multipart error: {:?}", e);
            return (StatusCode::BAD_REQUEST, "Invalid multipart data").into_response();
        },
        Err(e) => {
            error!("Timeout while reading multipart field: {:?}", e);
            return (StatusCode::REQUEST_TIMEOUT, "Timeout reading multipart").into_response();
        }
    };

    let file_path = upload_dir.join(format!("chunk_{}", chunk_index));
    let temp_path = file_path.with_extension("tmp");

    if file_path.exists() {
        return (StatusCode::CONFLICT, "Chunk already uploaded").into_response();
    }

    let mut file = match timeout(IO_TIMEOUT, File::create(&temp_path)).await {
        Ok(Ok(f)) => f,
        _ => return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to create chunk file").into_response(),
    };

    let mut hasher = Sha256::new();
    let mut total_written = 0;

    while let Some(chunk) = match timeout(IO_TIMEOUT, field.chunk()).await {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => {
            error!("Chunk read error: {:?}", e);
            return (StatusCode::BAD_REQUEST, "Failed to read chunk").into_response();
        },
        Err(e) => {
            error!("Timeout while reading chunk: {:?}", e);
            return (StatusCode::REQUEST_TIMEOUT, "Timeout reading chunk").into_response();
        }
    } {
        total_written += chunk.len();
        if total_written > MAX_CHUNK_SIZE {
            warn!("Chunk size limit exceeded: {} bytes", total_written);
            return (StatusCode::PAYLOAD_TOO_LARGE, "Chunk too large").into_response();
        }
        hasher.update(&chunk);
        if let Err(e) = timeout(IO_TIMEOUT, file.write_all(&chunk)).await {
            error!("Write timeout or error: {:?}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, "Failed writing chunk").into_response();
        }
    }

    if let Err(e) = timeout(IO_TIMEOUT, file.flush()).await {
        error!("Flush error: {:?}", e);
        return (StatusCode::INTERNAL_SERVER_ERROR, "Flush failed").into_response();
    }

    if let Err(e) = fs::rename(&temp_path, &file_path).await {
        error!("Rename error: {:?}", e);
        return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to finalize chunk file").into_response();
    }

    info!("Chunk {} written with checksum {:?}", chunk_index, hex::encode(hasher.finalize()));

    {
        let mut guard = state.lock().await;
        let entry = guard.entry(file_id.clone()).or_insert_with(|| vec![None; total_chunks]);
        entry[chunk_index] = Some(file_path.clone());

        if entry.iter().all(|x| x.is_some()) {
            drop(guard);

            let sem = MERGE_SEMAPHORE.get_or_init(|| async {
                Semaphore::new(2)
            }).await;

            let permit = match sem.acquire().await {
                Ok(p) => p,
                Err(_) => return (StatusCode::SERVICE_UNAVAILABLE, "Merge semaphore error").into_response(),
            };

            let result = merge_fragments(
                &sanitized_directory,
                &file_id,
                total_chunks,
                original_filename.clone(),
                mime_type.clone(),
            ).await;

            drop(permit);

            return match result {
                Ok(_) => (StatusCode::OK, "File assembled").into_response(),
                Err(err) => {
                    error!("Merge error: {}", err);
                    (StatusCode::INTERNAL_SERVER_ERROR, "Merge failed").into_response()
                }
            };
        }
    }

    (StatusCode::OK, "Chunk received").into_response()
}

pub async fn check_uploaded_chunks(
    Path(path): Path<String>,
    Query(params): Query<UploadParams>,
) -> impl IntoResponse {
    let upload_dir = PathBuf::from(format!("storage-files/{}/{}/", sanitize_directory_name(&path), params.file_id));
    let mut uploaded = vec![false; params.total_chunks];

    for i in 0..params.total_chunks {
        let chunk_path = upload_dir.join(format!("chunk_{}", i));
        if chunk_path.exists() {
            uploaded[i] = true;
        }
    }

    (StatusCode::OK, axum::Json(uploaded)).into_response()
}

pub async fn cleanup_stale_uploads() {
    loop {
        sleep(CLEANUP_INTERVAL).await;

        let root = PathBuf::from("storage-files");
        if let Ok(mut dirs) = fs::read_dir(&root).await {
            while let Ok(Some(entry)) = dirs.next_entry().await {
                if let Ok(mut uploads) = fs::read_dir(entry.path()).await {
                    while let Ok(Some(upload)) = uploads.next_entry().await {
                        if let Ok(metadata) = fs::metadata(upload.path()).await {
                            if let Ok(modified) = metadata.modified() {
                                if SystemTime::now().duration_since(modified).unwrap_or(Duration::ZERO) > FILE_TTL {
                                    let _ = fs::remove_dir_all(upload.path()).await;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn merge_fragments(
    directory: &str,
    file_id: &str,
    total_chunks: usize,
    original_filename: Option<String>,
    mime_type: Option<String>,
) -> Result<(), io::Error> {
    let upload_dir = PathBuf::from(format!("storage-files/{}/{}", directory, file_id));
    let merged_path = PathBuf::from(format!("storage-files/{}/{}.merged", directory, file_id));
    let output_file = File::create(&merged_path).await?;
    let mut writer = BufWriter::new(output_file);

    for i in 0..total_chunks {
        let chunk_path = upload_dir.join(format!("chunk_{}", i));
        if !chunk_path.exists() {
            return Err(io::Error::new(io::ErrorKind::NotFound, "Missing chunk"));
        }
        let chunk_file = File::open(&chunk_path).await?;
        let mut reader = BufReader::new(chunk_file);
        io::copy(&mut reader, &mut writer).await?;
    }
    writer.flush().await?;

    let metadata = FileMetadata {
        original_filename,
        mime_type,
        uploaded_at: chrono::Utc::now().to_rfc3339(),
        merged_path: merged_path.to_string_lossy().to_string(),
    };
    let json = serde_json::to_string_pretty(&metadata)?;
    fs::write(format!("storage-files/{}/{}.json", directory, file_id), json).await?;

    for i in 0..total_chunks {
        let chunk_path = upload_dir.join(format!("chunk_{}", i));
        fs::remove_file(chunk_path).await?;
    }
    fs::remove_dir(upload_dir).await?;

    info!("File {} reassembled at {}", file_id, merged_path.display());
    Ok(())
}

pub async fn finalize_merged_fragments(Path(path): Path<String>) -> impl IntoResponse {
    let sanitized_directory = sanitize_directory_name(&path);
    let new_dir = format!("storage-files/{}", sanitized_directory);
    let uploads_dir = std::path::Path::new(&new_dir);

    let mut entries = match fs::read_dir(uploads_dir).await {
        Ok(entries) => entries,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to read uploads dir: {}", e)).into_response(),
    };

    let mut rename_tasks = FuturesUnordered::new();

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }

        rename_tasks.push(tokio::spawn(async move {
            let json_str = fs::read_to_string(&path).await.ok()?;
            let metadata: FileMetadata = serde_json::from_str(&json_str).ok()?;
            let orig_name = metadata.original_filename?;
            let ext = std::path::Path::new(&orig_name).extension()?.to_str()?;

            let merged_path = std::path::Path::new(&metadata.merged_path);
            if merged_path.exists() && merged_path.extension()?.to_str()? == "merged" {
                let new_path = merged_path.with_extension(ext);
                fs::rename(merged_path, &new_path).await.ok()?;
                fs::remove_file(path).await.ok()?;
                Some(())
            } else {
                None
            }
        }));
    }

    let mut renamed_files = 0;
    while let Some(result) = rename_tasks.next().await {
        if result.ok().flatten().is_some() {
            renamed_files += 1;
        }
    }

    (StatusCode::OK, format!("{} files successfully assembled", renamed_files)).into_response()
}
