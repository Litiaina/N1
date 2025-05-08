use std::path::PathBuf;
use axum::extract::{Multipart, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use tokio::{fs, io};
use tokio::fs::{File, ReadDir};
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::Semaphore;
use tokio::time::timeout;
use tracing::{error, info, warn};
use crate::fs::fs_constants::{IO_TIMEOUT, MAX_CHUNK_SIZE};
use crate::statics::merge_semaphore_static::MERGE_SEMAPHORE;
use crate::structs::file_metadata_struct::FileMetadata;
use crate::structs::upload_params_struct::UploadParams;
use crate::types::shared_state_type::SharedState;
use crate::utils::directory_validation::sanitize_path;
use crate::utils::header_extractor::extract_uid_from_header;

pub async fn upload_fragments(
    header: HeaderMap,
    path: Option<Path<String>>,
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
        checksum,
    } = params;

    let validated_uid = match extract_uid_from_header(&header).await {
        Ok(email) => email,
        Err(code) => return code.into_response(),
    };

    if chunk_index >= total_chunks {
        return (StatusCode::BAD_REQUEST, "Invalid chunk index").into_response();
    }

    let relative_path = path
        .as_ref()
        .map(|Path(p)| sanitize_path(p))
        .unwrap_or_default();

    let upload_dir = if relative_path.is_empty() {
        PathBuf::from(format!("storage-files/{}/{}", validated_uid, &file_id))
    } else {
        PathBuf::from(format!("storage-files/{}/{}/{}", validated_uid, relative_path, &file_id))
    };

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

    let actual_checksum = hex::encode(hasher.finalize());

    if let Some(expected_checksum) = checksum {
        if expected_checksum != actual_checksum {
            error!(
                "Checksum mismatch for chunk {}: expected {}, got {}",
                chunk_index, expected_checksum, actual_checksum
            );
            let _ = fs::remove_file(&temp_path).await;
            return (StatusCode::BAD_REQUEST, "Checksum mismatch").into_response();
        }
    }

    if let Err(e) = fs::rename(&temp_path, &file_path).await {
        error!("Rename error: {:?}", e);
        return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to finalize chunk file").into_response();
    }

    info!("Chunk {} written and verified with checksum {}", chunk_index, actual_checksum);

    let mut should_merge = false;

    {
        let mut guard = state.lock().await;
        let entry = guard.entry(file_id.clone()).or_insert_with(|| vec![None; total_chunks]);
        entry[chunk_index] = Some(file_path);

        if entry.iter().all(|x| x.is_some()) {
            should_merge = true;
        }
    }

    if should_merge {
        let uid = validated_uid;
        let dir = relative_path;
        let id = file_id;
        let name = original_filename;
        let mime = mime_type;

        tokio::spawn(async move {
            let sem = MERGE_SEMAPHORE
                .get_or_init(|| async { Semaphore::new(2) })
                .await;

            let _permit = sem.acquire().await;

            if let Err(err) = assemble_fragments(&uid, &dir, &id, total_chunks, name.clone(), mime.clone()).await {
                error!("Async merge failed for {}: {:?}", id, err);
            } else {
                if let Err(e) = finalize_assembled_fragment_after_merge(&uid, &dir).await {
                    error!("Finalization failed after merge for {}: {:?}", id, e);
                }
            }
        });

        return (StatusCode::ACCEPTED, "Final chunk received, merge started").into_response();
    }

    (StatusCode::OK, "Chunk received and validated").into_response()
}

pub async fn check_uploaded_fragments(
    header: HeaderMap,
    Path(path): Path<String>,
    Query(params): Query<UploadParams>,
) -> impl IntoResponse {
    let validated_email = match extract_uid_from_header(&header).await {
        Ok(email) => email,
        Err(code) => return code.into_response(),
    };
    let sanitized_path = sanitize_path(&path);
    let upload_dir = PathBuf::from(format!("storage-files/{}/{}/{}/", validated_email, sanitized_path, params.file_id));
    let mut uploaded = vec![false; params.total_chunks];

    for i in 0..params.total_chunks {
        let chunk_path = upload_dir.join(format!("chunk_{}", i));
        if chunk_path.exists() {
            uploaded[i] = true;
        }
    }

    (StatusCode::OK, axum::Json(uploaded)).into_response()
}

async fn assemble_fragments(
    uid: &str,
    directory: &str,
    file_id: &str,
    total_chunks: usize,
    original_filename: Option<String>,
    mime_type: Option<String>,
) -> Result<(), io::Error> {
    let (upload_dir, merged_path) = if !directory.is_empty() {
        (PathBuf::from(format!("storage-files/{}/{}/{}", uid, directory, file_id)), PathBuf::from(format!("storage-files/{}/{}/{}.merged", uid, directory, file_id)))
    } else {
        (PathBuf::from(format!("storage-files/{}/{}", uid, file_id)), PathBuf::from(format!("storage-files/{}/{}.merged", uid, file_id)))
    };
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
    fs::write(format!("storage-files/{}/{}/{}.json", uid, directory, file_id), json).await?;

    for i in 0..total_chunks {
        let chunk_path = upload_dir.join(format!("chunk_{}", i));
        fs::remove_file(chunk_path).await?;
    }
    fs::remove_dir(upload_dir).await?;

    info!("File {} reassembled at {}", file_id, merged_path.display());
    Ok(())
}

async fn process_finalization(mut entries: ReadDir) -> usize {
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

    if renamed_files > 0 {
        info!("{} files assembled", renamed_files)
    } else {
        warn!("{} files assembled", renamed_files)
    }

    renamed_files
}

pub async fn finalize_assembled_fragment_after_merge(uid: &str, path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let sanitized_path = sanitize_path(&path);
    let new_dir = format!("storage-files/{}/{}", &uid, sanitized_path);
    let uploads_dir = std::path::Path::new(&new_dir);
    let entries = fs::read_dir(uploads_dir).await?;
    process_finalization(entries).await;

    Ok(())
}

pub async fn finalize_assembled_fragments(header: HeaderMap, path: Option<Path<String>>) -> impl IntoResponse {
    let validated_uid = match extract_uid_from_header(&header).await {
        Ok(email) => email,
        Err(code) => return code.into_response(),
    };

    let relative_path = path
        .as_ref()
        .map(|Path(p)| sanitize_path(p))
        .unwrap_or_default();

    let new_dir = if relative_path.is_empty() {
        format!("storage-files/{}", validated_uid)
    } else {
        format!("storage-files/{}/{}", validated_uid, relative_path)
    };

    let uploads_dir = std::path::Path::new(&new_dir);

    let entries = match fs::read_dir(uploads_dir).await {
        Ok(e) => e,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to read uploads dir: {}", e)).into_response(),
    };

    let renamed_files = process_finalization(entries).await;

    (StatusCode::OK, format!("{} files successfully assembled", renamed_files)).into_response()
}
