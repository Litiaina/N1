use std::path::PathBuf;
use axum::extract::{Multipart, Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use tokio::{fs, io};
use tokio::fs::File;
use tracing::{error, info};
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

    let sanitized_directory = sanitize_directory_name(&path);
    let upload_dir = PathBuf::from(format!("storage-files/{}/{}", sanitized_directory, &file_id));

    fs::create_dir_all(&upload_dir).await.unwrap();

    while let Some(field) = multipart.next_field().await.unwrap() {
        let file_path = upload_dir.join(format!("chunk_{}", chunk_index));
        let data = field.bytes().await.unwrap();
        fs::write(&file_path, &data).await.unwrap();

        let mut guard = state.lock().await;
        let entry = guard.entry(file_id.clone()).or_insert_with(|| vec![None; total_chunks]);
        entry[chunk_index] = Some(file_path.clone());

        if entry.iter().all(|x| x.is_some()) {
            drop(guard);
            return match merge_fragments(&sanitized_directory, &file_id, total_chunks, original_filename.clone(), mime_type.clone()).await {
                Ok(_) => (StatusCode::OK, "File assembled").into_response(),
                Err(err) => {
                    error!("Merge error: {}", err);
                    (StatusCode::INTERNAL_SERVER_ERROR, "Merge failed").into_response()
                }
            }
        }
    }

    (StatusCode::OK, "Chunk received").into_response()
}

async fn merge_fragments(
    directory: &str,
    file_id: &str,
    total_chunks: usize,
    original_filename: Option<String>,
    mime_type: Option<String>,
) -> Result<(), io::Error> {
    let upload_dir = PathBuf::from(format!("storage-files/{}/{}", directory, file_id));
    let mut chunk_paths = vec![];
    for i in 0..total_chunks {
        let chunk_path = upload_dir.join(format!("chunk_{}", i));
        if !chunk_path.exists() {
            return Err(io::Error::new(io::ErrorKind::NotFound, "Missing chunk"));
        }
        chunk_paths.push(chunk_path);
    }

    let merged_path = PathBuf::from(format!("storage-files/{}/{}.merged", directory, file_id));
    let mut output = File::create(&merged_path).await?;
    for chunk_path in &chunk_paths {
        let mut chunk_file = File::open(chunk_path).await?;
        io::copy(&mut chunk_file, &mut output).await?;
    }

    let metadata = FileMetadata {
        original_filename,
        mime_type,
        uploaded_at: chrono::Utc::now().to_rfc3339(),
        merged_path: merged_path.to_string_lossy().to_string(),
    };
    let json = serde_json::to_string_pretty(&metadata)?;
    fs::write(format!("storage-files/{}/{}.json", directory, file_id), json).await?;

    for chunk_path in chunk_paths {
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

    let mut renamed_files = 0;

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }

        let json_str = match fs::read_to_string(&path).await {
            Ok(content) => content,
            Err(_) => continue,
        };

        let metadata: FileMetadata = match serde_json::from_str(&json_str) {
            Ok(meta) => meta,
            Err(_) => continue,
        };

        let Some(orig_name) = metadata.original_filename else { continue; };
        let Some(ext) = std::path::Path::new(&orig_name).extension().and_then(|e| e.to_str()) else { continue; };

        let merged_path = std::path::Path::new(&metadata.merged_path);
        if merged_path.exists() && merged_path.extension().and_then(|e| e.to_str()) == Some("merged") {
            let new_path = merged_path.with_extension(ext);
            if let Err(e) = fs::rename(merged_path, &new_path).await {
                error!("Failed to rename {:?} -> {:?}: {}", merged_path, new_path, e);
                continue;
            }

            if let Err(e) = fs::remove_file(&path).await {
                error!("Failed to delete metadata file {:?}: {}", path, e);
            }

            renamed_files += 1;
        }
    }

    (StatusCode::OK, error!("Renamed {} merged files", renamed_files)).into_response()
}