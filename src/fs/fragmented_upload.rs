use crate::fs::fs_constants::{CHUNK_PREFIX, IO_TIMEOUT, MAX_CHUNK_SIZE, STORAGE_ROOT, TMP_EXT};
use crate::statics::merge_semaphore_static::MERGE_SEMAPHORE;
use crate::structs::upload_metadata_struct::UploadMetadata;
use crate::structs::upload_params_struct::UploadParams;
use crate::types::shared_state_type::SharedState;
use crate::utils::header_extractor::extract_uid_from_header;
use crate::validation::directory_validation::sanitize_path;
use axum::extract::{Multipart, Path, Query, State};
use axum::http::{HeaderMap, Response, StatusCode};
use axum::response::IntoResponse;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use axum::body::Body;
use tokio::fs::{File, ReadDir};
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::Semaphore;
use tokio::time::timeout;
use tokio::{fs, io};
use tracing::{error, info, warn};

async fn with_io_timeout<F, T>(
    desc: &str,
    fut: F,
) -> Result<T, Response<Body>>
where
    F: Future<Output = Result<T, io::Error>>,
{
    match timeout(IO_TIMEOUT, fut).await {
        Ok(Ok(val)) => Ok(val),
        Ok(Err(e)) => {
            error!("{} error: {:?}", desc, e);
            Err((StatusCode::INTERNAL_SERVER_ERROR, format!("{} failed", desc)).into_response())
        }
        Err(_) => {
            error!("Timeout during {}", desc);
            Err((StatusCode::REQUEST_TIMEOUT, format!("Timeout {}", desc)).into_response())
        }
    }
}

pub async fn upload_fragments(
    header: HeaderMap,
    path: Option<Path<String>>,
    Query(params): Query<UploadParams>,
    State(state): State<SharedState>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, impl IntoResponse> {
    let validated_uid =
        extract_uid_from_header(&header).map_err(|status| status.into_response())?;

    if params.chunk_index >= params.total_chunks {
        return Err((StatusCode::BAD_REQUEST, "Invalid chunk index").into_response());
    }

    let relative_path = path
        .as_ref()
        .map(|Path(p)| sanitize_path(p))
        .unwrap_or_default();

    let upload_dir = PathBuf::from(STORAGE_ROOT)
        .join(&validated_uid)
        .join(&relative_path)
        .join(&params.file_id);

    fs::create_dir_all(&upload_dir)
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Failed to create directory").into_response())?;

    let mut field = match timeout(IO_TIMEOUT, multipart.next_field()).await {
        Ok(Ok(Some(field))) => field,
        Ok(Ok(None)) => return Err((StatusCode::BAD_REQUEST, "No multipart field").into_response()),
        Ok(Err(e)) => {
            error!("Multipart error: {:?}", e);
            return Err((StatusCode::BAD_REQUEST, "Invalid multipart").into_response());
        }
        Err(_) => return Err((StatusCode::REQUEST_TIMEOUT, "Timeout reading multipart").into_response()),
    };

    let file_path = upload_dir.join(format!("{}{}", CHUNK_PREFIX, params.chunk_index));
    let temp_path = file_path.with_extension(TMP_EXT);

    if file_path.exists() {
        return Err((StatusCode::CONFLICT, "Chunk already uploaded").into_response());
    }

    let mut file = with_io_timeout("creating chunk file", File::create(&temp_path)).await?;
    let mut hasher = Sha256::new();
    let mut total_written = 0usize;

    while let Some(chunk) = match timeout(IO_TIMEOUT, field.chunk()).await {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => {
            error!("Chunk read error: {:?}", e);
            return Err((StatusCode::BAD_REQUEST, "Failed to read chunk").into_response());
        }
        Err(_) => {
            error!("Timeout while reading chunk");
            return Err((StatusCode::REQUEST_TIMEOUT, "Timeout reading chunk").into_response());
        }
    } {
        total_written += chunk.len();
        if total_written > MAX_CHUNK_SIZE {
            warn!("Chunk size exceeded: {} bytes", total_written);
            return Err((StatusCode::PAYLOAD_TOO_LARGE, "Chunk too large").into_response());
        }
        hasher.update(&chunk);
        with_io_timeout("writing chunk", file.write_all(&chunk)).await?;
    }

    with_io_timeout("flushing file", file.flush()).await?;

    if let Some(expected) = params.checksum.as_ref() {
        let computed = hex::encode(hasher.finalize());
        if expected != &computed {
            error!("Checksum mismatch: expected {}, got {}", expected, computed);
            let _ = fs::remove_file(&temp_path).await;
            return Err((StatusCode::BAD_REQUEST, "Checksum mismatch").into_response());
        }
    }

    fs::rename(&temp_path, &file_path)
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Failed to finalize chunk").into_response())?;

    let should_merge = {
        let mut guard = state.lock().await;
        let entry = guard
            .entry(params.file_id.clone())
            .or_insert_with(|| vec![None; params.total_chunks]);
        entry[params.chunk_index] = Some(file_path.clone());
        entry.iter().all(|x| x.is_some())
    };

    if should_merge {
        let uid = validated_uid.clone();
        let dir = relative_path.clone();
        let id = params.file_id.clone();
        let name = params.original_filename.clone();
        let mime = params.mime_type.clone();

        tokio::spawn(async move {
            let sem = MERGE_SEMAPHORE
                .get_or_init(|| async { Semaphore::new(2) })
                .await;
            let _permit = sem.acquire().await;
            if let Err(err) = assemble_fragments(&uid, &dir, &id, params.total_chunks, name, mime).await {
                error!("Merge failed: {:?}", err);
            } else if let Err(err) = finalize_assembled_fragment_after_merge(&uid, &dir).await {
                error!("Finalization failed: {:?}", err);
            }
        });

        return Ok((StatusCode::ACCEPTED, "Final chunk received, merging").into_response());
    }

    Ok((StatusCode::OK, "Chunk received").into_response())
}

pub async fn check_uploaded_fragments(
    header: HeaderMap,
    Path(path): Path<String>,
    Query(params): Query<UploadParams>,
) -> Result<impl IntoResponse, StatusCode> {
    let validated_uid = extract_uid_from_header(&header).map_err(|status| status)?;
    let sanitized_path = sanitize_path(&path);
    let upload_dir = PathBuf::from(format!(
        "storage-files/{}/{}/{}/",
        validated_uid, sanitized_path, params.file_id
    ));
    let mut uploaded = vec![false; params.total_chunks];

    for i in 0..params.total_chunks {
        let chunk_path = upload_dir.join(format!("chunk_{}", i));
        if chunk_path.exists() {
            uploaded[i] = true;
        }
    }

    Ok((StatusCode::OK, axum::Json(uploaded)))
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
        (
            PathBuf::from(format!("storage-files/{}/{}/{}", uid, directory, file_id)),
            PathBuf::from(format!(
                "storage-files/{}/{}/{}.merged",
                uid, directory, file_id
            )),
        )
    } else {
        (
            PathBuf::from(format!("storage-files/{}/{}", uid, file_id)),
            PathBuf::from(format!("storage-files/{}/{}.merged", uid, file_id)),
        )
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

    let metadata = UploadMetadata {
        original_filename,
        mime_type,
        uploaded_at: chrono::Utc::now().to_rfc3339(),
        merged_path: merged_path.to_string_lossy().to_string(),
    };
    let json = serde_json::to_string_pretty(&metadata)?;
    fs::write(
        format!("storage-files/{}/{}/{}.json", uid, directory, file_id),
        json,
    )
    .await?;

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
            let metadata: UploadMetadata = serde_json::from_str(&json_str).ok()?;
            let orig_name = metadata.original_filename?;

            let merged_path = std::path::Path::new(&metadata.merged_path);
            if !merged_path.exists() || merged_path.extension()?.to_str()? != "merged" {
                return None;
            }

            let parent_dir = merged_path.parent()?;
            let orig_path = parent_dir.join(&orig_name);
            let mut final_path = orig_path.clone();

            if final_path.exists() {
                let stem = std::path::Path::new(&orig_name)
                    .file_stem()?
                    .to_string_lossy();
                let ext = std::path::Path::new(&orig_name)
                    .extension()
                    .map(|e| format!(".{}", e.to_string_lossy()))
                    .unwrap_or_default();

                for i in 1..1000 {
                    let candidate = parent_dir.join(format!("{} [{}]{}", stem, i, ext));
                    if !candidate.exists() {
                        final_path = candidate;
                        break;
                    }
                }
            }

            fs::rename(merged_path, &final_path).await.ok()?;
            fs::remove_file(path).await.ok()?;

            info!("Renamed {:?} to {:?}", merged_path, final_path);
            Some(())
        }));
    }

    let mut renamed_files = 0;
    while let Some(result) = rename_tasks.next().await {
        if result.ok().flatten().is_some() {
            renamed_files += 1;
        }
    }

    if renamed_files > 0 {
        info!("{} files assembled and renamed", renamed_files)
    } else {
        warn!("No files assembled or renamed")
    }

    renamed_files
}

pub async fn finalize_assembled_fragment_after_merge(
    uid: &str,
    path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let sanitized_path = sanitize_path(&path);
    let new_dir = format!("storage-files/{}/{}", &uid, sanitized_path);
    let uploads_dir = std::path::Path::new(&new_dir);
    let entries = fs::read_dir(uploads_dir).await?;
    process_finalization(entries).await;

    Ok(())
}

pub async fn finalize_assembled_fragments(
    header: HeaderMap,
    path: Option<Path<String>>,
) -> Result<impl IntoResponse, impl IntoResponse> {
    let validated_uid =
        extract_uid_from_header(&header).map_err(|status| status.into_response())?;

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
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to read uploads dir: {}", e),
            )
                .into_response());
        }
    };

    let renamed_files = process_finalization(entries).await;

    Ok((
        StatusCode::OK,
        format!("{} files successfully assembled", renamed_files),
    )
        .into_response())
}
