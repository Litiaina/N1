use crate::enums::app_error_enum::AppError;
use crate::fs::fragmented_upload::{with_io_timeout, with_timeout};
use crate::fs::fs_constants::{MAX_CHUNK_SIZE, MERGE_CONCURRENCY, TMP_EXT};
use crate::fs::stateless_fragmented_upload::parse_uploaded_chunks;
use crate::object_storage::object_storage_constants::{OBJECT_DATA_ROOT, OBJECT_STORAGE_ROOT};
use crate::object_storage::object_storage_structs::{
    InitiateUploadParams, InitiateUploadResponse, ObjectMetadata, TempUploadMetadata,
    UploadStatusResponse,
};
use crate::routes::file_system_routes::AppState;
use crate::statics::merge_semaphore_static::MERGE_SEMAPHORE;
use crate::structs::stateless_fragment_upload_structs::PathManager;
use crate::utils::header_extractor::extract_uid_from_fragmented_header;
use axum::extract::multipart::Field;
use axum::{
    Json as AxumJson, Json,
    body::Body,
    extract::{Multipart, Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
};
use futures_util::stream::TryStreamExt;
use headers::HeaderValue;
use mongodb::{Client as MongoClient, bson::doc};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::{collections::HashMap, path::PathBuf};
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    sync::Semaphore,
};
use tower::ServiceExt;
use tracing::{error, info, warn};
use uuid::Uuid;

pub async fn initiate_object_upload(
    header: HeaderMap,
    Path((fragment, object_key)): Path<(String, String)>,
    AxumJson(params): AxumJson<InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    let uid = extract_uid_from_fragmented_header(&header)?;
    let file_id = Uuid::new_v4().to_string();
    let paths = PathManager::new(&file_id);
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
        Json(InitiateUploadResponse { file_id }),
    ))
}

pub async fn upload_chunk(
    Path((file_id, chunk_index)): Path<(String, u64)>,
    Query(params): Query<HashMap<String, String>>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, AppError> {
    let paths = PathManager::new(&file_id);
    let field = with_timeout("reading multipart field", multipart.next_field())
        .await??
        .ok_or_else(|| AppError::BadRequest("No multipart field found.".to_string()))?;
    let expected_checksum = params.get("checksum").map(|s| s.as_str());
    save_chunk_stream(&paths, chunk_index, field, expected_checksum).await?;
    Ok((StatusCode::OK, "Chunk uploaded successfully."))
}

pub async fn get_upload_status(Path(file_id): Path<String>) -> Result<impl IntoResponse, AppError> {
    let paths = PathManager::new(&file_id);
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
    Ok(Json(UploadStatusResponse {
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
        let paths = PathManager::new(&file_id);
        if let Err(e) = assemble_and_index_object(&paths, state.db_client.clone()).await {
            error!(
                "Failed to assemble and index object for file_id '{}'. Error: {:?}",
                file_id, e
            );
        }
    });
    Ok((StatusCode::ACCEPTED, "Finalization process started."))
}

pub async fn get_object(
    State(state): State<Arc<AppState>>,
    header: HeaderMap,
    Path((fragment, object_key)): Path<(String, String)>,
    req: axum::http::Request<Body>,
) -> Result<impl IntoResponse, AppError> {
    let uid = extract_uid_from_fragmented_header(&header)?;
    let metadata =
        find_latest_object_metadata(&state.db_client, &fragment, &object_key, &uid).await?;

    let physical_path = PathBuf::from(OBJECT_STORAGE_ROOT)
        .join(OBJECT_DATA_ROOT)
        .join(&metadata.physical_location);

    if !fs::try_exists(&physical_path).await.unwrap_or(false) {
        error!(
            "CRITICAL: Metadata found but physical file is missing at {}",
            physical_path.display()
        );
        return Err(AppError::Conflict("Object data is missing.".to_string()));
    }

    let service = tower_http::services::ServeFile::new(physical_path);

    match service.oneshot(req).await {
        Ok(mut response) => {
            let content_type = metadata.content_type;
            match HeaderValue::from_str(&content_type) {
                Ok(header_value) => {
                    response
                        .headers_mut()
                        .insert(header::CONTENT_TYPE, header_value);
                }
                Err(e) => {
                    error!("Invalid content-type string in metadata: {}", e);
                }
            }
            Ok(response.into_response())
        }
        Err(err) => {
            error!("Failed to serve file: {}", err);
            Err(AppError::Conflict(format!("Failed to serve file: {}", err)))
        }
    }
}

pub async fn get_object_metadata(
    State(state): State<Arc<AppState>>,
    header: HeaderMap,
    Path((fragment, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = extract_uid_from_fragmented_header(&header)?;
    let metadata =
        find_latest_object_metadata(&state.db_client, &fragment, &object_key, &uid).await?;
    Ok(AxumJson(metadata))
}

pub async fn delete_object(
    State(state): State<Arc<AppState>>,
    header: HeaderMap,
    Path((fragment, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = extract_uid_from_fragmented_header(&header)?;
    let collection = state
        .db_client
        .database("litiaina-storage")
        .collection::<ObjectMetadata>("objects");

    let filter =
        doc! { "fragment": fragment.clone(), "key": object_key.clone(), "owner_uid": uid.clone() };
    let mut cursor = collection
        .find(filter.clone())
        .await
        .map_err(|_| AppError::Conflict("Database query for find failed.".to_string()))?;

    let mut physical_files_to_delete = Vec::new();
    while let Some(metadata) = cursor
        .try_next()
        .await
        .map_err(|_| AppError::Conflict("Cursor error.".to_string()))?
    {
        physical_files_to_delete.push(metadata.physical_location);
    }

    if physical_files_to_delete.is_empty() {
        return Err(AppError::BadRequest("Object not found.".to_string()));
    }

    collection
        .delete_many(filter)
        .await
        .map_err(|_| AppError::Conflict("Database delete_many failed.".to_string()))?;

    for location in physical_files_to_delete {
        let physical_path = PathBuf::from(OBJECT_STORAGE_ROOT)
            .join(OBJECT_DATA_ROOT)
            .join(location);
        if fs::try_exists(&physical_path).await.unwrap_or(false) {
            if let Err(e) = with_io_timeout(
                "deleting physical object version",
                fs::remove_file(physical_path),
            )
            .await
            {
                warn!(
                    "Failed to delete physical file version during cleanup: {}",
                    e
                );
            }
        }
    }

    info!(
        "Deleted all versions of object: {}/{}",
        fragment, object_key
    );
    Ok(StatusCode::NO_CONTENT)
}

pub async fn list_objects(
    State(state): State<Arc<AppState>>,
    header: HeaderMap,
    Path(fragment): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let uid = extract_uid_from_fragmented_header(&header)?;
    let collection = state
        .db_client
        .database("litiaina-storage")
        .collection::<ObjectMetadata>("objects");
    let limit: i64 = params
        .get("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    let marker = params.get("marker").cloned();

    let mut query = doc! { "fragment": fragment, "owner_uid": uid, "is_latest": true };
    if let Some(marker_key) = marker {
        query.insert("key", doc! { "$gt": marker_key });
    }

    let find_options = mongodb::options::FindOptions::builder()
        .limit(limit)
        .sort(doc! { "key": 1 })
        .build();

    let mut cursor = collection
        .find(query)
        .with_options(find_options)
        .await
        .map_err(|_| AppError::Conflict("Database query failed.".to_string()))?;

    let mut objects = Vec::new();
    while let Some(result) = cursor
        .try_next()
        .await
        .map_err(|_| AppError::Conflict("Failed to read from database cursor.".to_string()))?
    {
        objects.push(result);
    }
    Ok(Json(objects))
}

async fn assemble_and_index_object(
    paths: &PathManager<'_>,
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
        "Starting assembly for object: {}/{}",
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
        for i in 0..temp_metadata.total_chunks {
            let chunk_path = paths.chunk_path(i);
            if !fs::try_exists(&chunk_path).await.unwrap_or(false) {
                return Err(AppError::Conflict(format!(
                    "Missing chunk {} during assembly.",
                    i
                )));
            }
            let mut chunk_file = with_io_timeout("opening chunk", File::open(&chunk_path)).await?;
            let mut buf = Vec::new();
            chunk_file
                .read_to_end(&mut buf)
                .await
                .map_err(|e| AppError::from((e, "reading chunk")))?;
            final_hasher.update(&buf);
            writer
                .write_all(&buf)
                .await
                .map_err(|e| AppError::from((e, "writing chunk")))?;
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
    let now = mongodb::bson::DateTime::now();
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

async fn save_chunk_stream(
    paths: &PathManager<'_>,
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

async fn find_latest_object_metadata(
    db: &MongoClient,
    fragment: &str,
    object_key: &str,
    uid: &str,
) -> Result<ObjectMetadata, AppError> {
    let collection = db
        .database("litiaina-storage")
        .collection::<ObjectMetadata>("objects");

    collection
        .find_one(
            doc! { "fragment": fragment, "key": object_key, "owner_uid": uid, "is_latest": true },
        )
        .await
        .map_err(|_| AppError::Conflict("Database query failed.".to_string()))?
        .ok_or(AppError::BadRequest("Object not found.".to_string()))
}
