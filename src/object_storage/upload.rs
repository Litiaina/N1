use crate::enums::app_error_enum::AppError;
use crate::fs::fragmented_upload::{with_io_timeout, with_timeout};
use crate::fs::fs_constants::{MAX_CHUNK_SIZE, MERGE_CONCURRENCY, TMP_EXT};
use crate::object_storage::chunk_parser::parse_uploaded_chunks;
use crate::object_storage::constants::{OBJECT_DATA_ROOT, OBJECT_STORAGE_ROOT};
use crate::object_storage::storage_capacity_parser::calculate_storage_usage;
use crate::routes::storage_routes::AppState;
use crate::statics::merge_semaphore_static::MERGE_SEMAPHORE;
use crate::structs::object_storage_structs::{
    CapacityUsageResponse, InitiateUploadParams, InitiateUploadResponse, ObjectMetadata,
    ObjectPathManager, TempUploadMetadata, UploadStatusResponse, UserAuthDetails,
};
use crate::utils::header_extractor::extract_uid_from_fragmented_header;
use axum::extract::multipart::Field;
use axum::{
    Json as AxumJson,
    extract::{Multipart, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use futures_util::stream::TryStreamExt;
use mongodb::{Client as MongoClient, bson, bson::doc};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::{collections::HashMap, path::PathBuf};
use tokio::io::BufReader;
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    sync::Semaphore,
};
use tracing::{error, info, warn};
use uuid::Uuid;

pub async fn initiate_object_upload(
    State(state): State<Arc<AppState>>,
    header: HeaderMap,
    Path((fragment, object_key)): Path<(String, String)>,
    AxumJson(params): AxumJson<InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    let uid = extract_uid_from_fragmented_header(&header)?;

    let auth_db = state.db_client.database("litiaina-cloud-database");
    let auth_collection = auth_db.collection::<UserAuthDetails>("auth");
    let user_auth_details = auth_collection
        .find_one(doc! { "uid": &uid })
        .await
        .map_err(|_| AppError::Conflict("Failed to query user details.".to_string()))?
        .ok_or(AppError::Unauthorized)?;

    let allocated_storage = user_auth_details.allocated_storage;
    let current_usage = calculate_storage_usage(&state.db_client, &uid).await?;

    if current_usage.saturating_add(params.total_size_bytes) > allocated_storage {
        warn!(
            "User {} exceeded storage quota. Usage: {}, Allocation: {}, Attempted Upload: {}",
            uid, current_usage, allocated_storage, params.total_size_bytes
        );
        return Err(AppError::InsufficientStorage);
    }

    let file_id = Uuid::new_v4().to_string();
    let paths = ObjectPathManager::new(&file_id);
    with_io_timeout(
        "create temp upload directory",
        fs::create_dir_all(paths.temp_upload_dir()),
    )
    .await?;
    let metadata = TempUploadMetadata {
        file_id: file_id.clone(),
        fragment,
        object_key,
        uid,
        content_type: params.content_type,
        total_size_bytes: params.total_size_bytes,
        total_chunks: params.total_chunks,
        creation_timestamp: chrono::Utc::now().to_rfc3339(),
    };
    let metadata_content = serde_json::to_string_pretty(&metadata)
        .map_err(|e| AppError::BadRequest(format!("Failed to serialize metadata: {}", e)))?;
    with_io_timeout(
        "writing metadata file",
        fs::write(paths.metadata_path(), metadata_content),
    )
    .await?;
    info!("Initiated object upload session: {}", file_id);
    Ok((
        StatusCode::CREATED,
        AxumJson(InitiateUploadResponse { file_id }),
    ))
}

pub async fn get_capacity_usage(
    State(state): State<Arc<AppState>>,
    header: HeaderMap,
) -> Result<impl IntoResponse, AppError> {
    let uid = extract_uid_from_fragmented_header(&header)?;
    let current_usage = calculate_storage_usage(&state.db_client, &uid).await?;
    Ok(AxumJson(CapacityUsageResponse {
        capacity_used: current_usage,
    }))
}

pub async fn object_upload_chunk(
    Path((file_id, chunk_index)): Path<(String, u64)>,
    Query(params): Query<HashMap<String, String>>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, AppError> {
    let paths = ObjectPathManager::new(&file_id);
    let field = with_timeout("reading multipart field", multipart.next_field())
        .await??
        .ok_or_else(|| AppError::BadRequest("No multipart field found.".to_string()))?;
    let expected_checksum = params.get("checksum").map(|s| s.as_str());
    save_chunk_stream(&paths, chunk_index, field, expected_checksum).await?;
    Ok((StatusCode::OK, "Chunk uploaded successfully."))
}

pub async fn get_object_upload_status(
    Path(file_id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let paths = ObjectPathManager::new(&file_id);
    let metadata: TempUploadMetadata = {
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
    let uploaded_chunks = parse_uploaded_chunks(&mut dir_reader).await;
    Ok(AxumJson(UploadStatusResponse {
        total_size_bytes: metadata.total_size_bytes,
        uploaded_chunks,
        total_chunks: metadata.total_chunks,
    }))
}

pub async fn finalize_object_upload(
    State(state): State<Arc<AppState>>,
    Path(file_id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    info!("Finalization requested for upload session: {}", file_id);
    tokio::spawn(async move {
        let paths = ObjectPathManager::new(&file_id);
        if let Err(e) = assemble_and_index_object(&paths, state.db_client.clone()).await {
            error!(
                "Failed to assemble and index object for file_id '{}'. Error: {:?}",
                file_id, e
            );
        }
    });
    Ok((StatusCode::ACCEPTED, "Finalization process started."))
}

async fn save_chunk_stream(
    paths: &ObjectPathManager<'_>,
    index: u64,
    mut field: Field<'_>,
    expected_checksum: Option<&str>,
) -> Result<(), AppError> {
    let final_path = paths.chunk_path(index);
    let temp_path = final_path.with_extension(TMP_EXT);
    let mut hasher = Sha256::new();

    let file = with_io_timeout("creating chunk file", File::create(&temp_path)).await?;
    let mut writer = BufWriter::new(file);
    let mut total_written = 0;

    while let Some(chunk) = field
        .try_next()
        .await
        .map_err(|e| AppError::Multipart(e.to_string()))?
    {
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

    with_io_timeout("flushing chunk file", writer.flush()).await?;

    if let Some(expected) = expected_checksum {
        let computed_checksum = hex::encode(hasher.finalize());
        if expected != computed_checksum {
            warn!(
                "Checksum mismatch for chunk {}: expected {}, got {}",
                index, expected, computed_checksum
            );
            let _ = fs::remove_file(&temp_path).await;
            return Err(AppError::BadRequest("Checksum mismatch.".to_string()));
        }
    }

    with_io_timeout("finalizing chunk", fs::rename(&temp_path, &final_path)).await?;
    Ok(())
}

async fn assemble_and_index_object(
    paths: &ObjectPathManager<'_>,
    db: MongoClient,
) -> Result<(), AppError> {
    let semaphore = MERGE_SEMAPHORE
        .get_or_init(|| async { Semaphore::new(MERGE_CONCURRENCY) })
        .await;
    let _permit = semaphore
        .acquire()
        .await
        .expect("Semaphore is never closed");

    let temp_metadata: TempUploadMetadata = {
        let content = with_io_timeout(
            "reading metadata for assembly",
            fs::read_to_string(paths.metadata_path()),
        )
        .await?;
        serde_json::from_str(&content)
            .map_err(|_| AppError::Conflict("Invalid metadata file for assembly.".to_string()))?
    };

    info!(
        "Starting high-performance assembly for object: {}/{}",
        temp_metadata.fragment, temp_metadata.object_key
    );

    let new_version_id = Uuid::new_v4().to_string();
    let physical_path_str = format!("{}/{}.data", temp_metadata.fragment, new_version_id);
    let physical_path = PathBuf::from(OBJECT_STORAGE_ROOT)
        .join(OBJECT_DATA_ROOT)
        .join(&physical_path_str);

    if let Some(parent) = physical_path.parent() {
        with_io_timeout("create final object directory", fs::create_dir_all(parent)).await?;
    }

    let temp_merged_path = paths
        .temp_upload_dir()
        .join(format!("{}.merged.tmp", temp_metadata.file_id));

    let mut final_hasher = Sha256::new();
    let final_size;

    {
        let output_file =
            with_io_timeout("creating merged file", File::create(&temp_merged_path)).await?;
        let mut writer = BufWriter::new(output_file);
        let mut buffer = vec![0; 1024 * 64];
        for i in 0..temp_metadata.total_chunks {
            let chunk_path = paths.chunk_path(i);
            if !fs::try_exists(&chunk_path).await.unwrap_or(false) {
                return Err(AppError::Conflict(format!(
                    "Missing chunk {} during assembly.",
                    i
                )));
            }

            let chunk_file = with_io_timeout("opening chunk", File::open(&chunk_path)).await?;
            let mut chunk_reader = BufReader::new(chunk_file);

            loop {
                let bytes_read = chunk_reader
                    .read(&mut buffer)
                    .await
                    .map_err(|e| AppError::from((e, "reading chunk piece during assembly")))?;

                if bytes_read == 0 {
                    break;
                }

                let data_slice = &buffer[..bytes_read];
                final_hasher.update(data_slice);
                writer
                    .write_all(data_slice)
                    .await
                    .map_err(|e| AppError::from((e, "writing chunk piece during assembly")))?;
            }
        }
        with_io_timeout("flushing merged file", writer.flush()).await?;
        final_size = with_io_timeout("getting final size", writer.into_inner().metadata())
            .await?
            .len();
    }

    with_io_timeout(
        "moving assembled file to final destination",
        fs::rename(&temp_merged_path, &physical_path),
    )
    .await?;

    let final_checksum = hex::encode(final_hasher.finalize());
    let now = bson::DateTime::now();
    let final_metadata = ObjectMetadata {
        id: Uuid::new_v4().to_string(),
        fragment: temp_metadata.fragment,
        key: temp_metadata.object_key,
        version_id: new_version_id,
        is_latest: true,
        size_bytes: final_size,
        content_type: temp_metadata
            .content_type
            .unwrap_or_else(|| "application/octet-stream".to_string()),
        owner_uid: temp_metadata.uid,
        created_at: now,
        last_modified: now,
        etag: final_checksum,
        physical_location: physical_path_str,
    };

    let collection = db
        .database("litiaina-storage")
        .collection::<ObjectMetadata>("objects");

    let filter = doc! {
        "fragment": final_metadata.fragment.clone(),
        "key": final_metadata.key.clone(),
        "owner_uid": final_metadata.owner_uid.clone(),
        "is_latest": true
    };
    let update = doc! { "$set": { "is_latest": false } };
    collection.update_one(filter, update).await.map_err(|e| {
        error!("Failed to update old version's 'is_latest' flag: {}", e);
        AppError::Conflict("Failed to update old version record.".to_string())
    })?;

    collection.insert_one(&final_metadata).await.map_err(|e| {
        error!("Failed to insert new object version metadata: {}", e);
        AppError::Conflict("Failed to create new version record.".to_string())
    })?;

    with_io_timeout(
        "cleaning up temp directory",
        fs::remove_dir_all(paths.temp_upload_dir()),
    )
    .await?;

    info!(
        "Successfully indexed new version of object: {}/{} (Version ID: {})",
        final_metadata.fragment, final_metadata.key, final_metadata.version_id
    );
    Ok(())
}
