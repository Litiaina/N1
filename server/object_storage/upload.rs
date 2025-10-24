use crate::database::database_constants::{DATABASE, OBJECT_STORAGE_DATABASE};
use crate::enums::app_error_enum::AppError;
use crate::enums::upload_status_enum::UploadStatus;
use crate::fs::fragmented_upload::{with_io_timeout, with_timeout};
use crate::fs::fs_constants::{
    MAX_BUFFER_SIZE, MAX_CHUNK_SIZE, MERGE_CONCURRENCY, MIN_BUFFER_SIZE, TMP_EXT,
};
use crate::object_storage::chunk_parser::parse_uploaded_chunks;
use crate::object_storage::constants::{OBJECT_DATA_ROOT, OBJECT_STORAGE_ROOT};
use crate::object_storage::storage_capacity_parser::calculate_storage_usage;
use crate::routes::storage_routes::AppState;
use crate::statics::merge_semaphore_static::{
    CHUNK_UPLOAD_SEMAPHORE, CHUNK_WRITE_LOCKS, MERGE_SEMAPHORE, PREFIX_DELETE_LOCKS,
};
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{
    CapacityUsageResponse, InitiateUploadParams, InitiateUploadResponse, ObjectMetadata,
    ObjectPathManager, TempUploadMetadata, UploadStatusResponse, UserAuthDetails,
};
use axum::body::Body;
use axum::extract::multipart::Field;
use axum::http::HeaderMap;
use axum::{
    Json as AxumJson,
    extract::{Multipart, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use base64::Engine;
use base64::engine::general_purpose;
use chrono::Utc;
use futures_util::StreamExt;
use futures_util::stream::TryStreamExt;
use mongodb::bson::{Bson, DateTime};
use mongodb::error::{ErrorKind, WriteFailure};
use mongodb::{Client as MongoClient, bson::doc};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::{collections::HashMap, path::PathBuf};
use tokio::io::BufReader;
use tokio::sync::Mutex;
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    sync::Semaphore,
};
use tracing::{error, info, warn};
use uuid::Uuid;

/*
# Database Setup for Concurrency Control

**Last Updated:** August 22, 2025

## 1. Introduction

This document outlines the critical database setup required for the storage service to handle concurrent upload initiations correctly, especially in a horizontally-scaled (multi-server) environment.

To prevent a race condition where multiple parallel requests could cause a user to exceed their storage quota, we use a "Database as a Lock" pattern. This requires a dedicated MongoDB collection (`upload_locks`) with two specific indexes. These indexes ensure that only one upload initiation process can run at a time for any given user, guaranteeing data consistency without requiring external services like Redis.

**This setup is mandatory for the application to function correctly.**

---

## 2. Prerequisites

* You must have access to the MongoDB instance hosting the application's data.
* You must have the MongoDB Shell (`mongosh`) installed and available in your terminal's PATH.

---

## 3. Step-by-Step Instructions

### Step 1: Connect to Your Database

First, you need to connect to the correct database using the MongoDB Shell.

1.  Open your terminal or command prompt.
2.  Use the `mongosh` command with your database's connection string. Replace `"your-mongodb-connection-string"` with the actual connection string for your environment.

    ```bash
    # Example connection string
    mongosh "mongodb+srv://<user>:<password>@<cluster-url>/<database_name>?retryWrites=true&w=majority"
    ```

3.  Once connected, ensure you are operating on the correct database. Based on your setup, this is likely `litiaina-storage`. Run the `use` command:

    ```javascript
    use litiaina-storage;
    ```

    The shell should respond with `switched to db litiaina-storage`. You are now ready to create the indexes.

### Step 2: Create the `upload_locks` Collection and Indexes

The following commands will automatically create the `upload_locks` collection if it doesn't already exist and then apply the necessary indexes to it.

**1. Create the Unique Index (The Lock Mechanism)**

This is the most critical index. It enforces a unique constraint on the `uid` field, making it impossible for two server instances to acquire a lock for the same user simultaneously.

* **Copy and paste the following command into your `mongosh` terminal and press Enter:**

    ```javascript
    db.upload_locks.createIndex({ "uid": 1 }, { unique: true })
    ```

**2. Create the TTL Index (The Safety Net)**

This index is a safety measure. It automatically deletes lock documents that are older than 30 seconds. This prevents a user from being permanently locked out if a server crashes after acquiring a lock but before releasing it.

* **Copy and paste the following command into your `mongosh` terminal and press Enter:**

    ```javascript
    db.upload_locks.createIndex({ "locked_at": 1 }, { expireAfterSeconds: 30 })
    ```

---

## 4. Verification

After running the commands, you can verify that the indexes were created successfully.

1.  Run the `getIndexes()` command on the `upload_locks` collection:

    ```javascript
    db.upload_locks.getIndexes()
    ```

2.  The output should be an array containing three indexes: the default `_id_` index, and the two you just created. It will look similar to this:

    ```json
    [
      { v: 2, key: { _id: 1 }, name: '_id_' },
      { v: 2, key: { uid: 1 }, name: 'uid_1', unique: true },
      { v: 2, key: { locked_at: 1 }, name: 'locked_at_1', expireAfterSeconds: 30 }
    ]
    ```

If your output matches this structure, your database is correctly configured.

---

## 5. Conclusion

Once these indexes are in place, the storage backend is ready for deployment in a horizontally-scaled environment. The locking mechanism will now correctly prevent storage quota race conditions.
*/
pub async fn initiate_object_upload(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
    AxumJson(params): AxumJson<InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;

    if PREFIX_DELETE_LOCKS
        .iter()
        .any(|lock| object_key.starts_with(lock.key()))
    {
        warn!(
            "User {} tried to upload to a locked path: {}/{}",
            uid, fragment, object_key
        );
        return Err(AppError::Conflict(
            "The target directory is currently undergoing a delete operation. Please try again later."
                .to_string(),
        ));
    }

    let db_client = &state.db_client;
    let locks_collection = db_client
        .database(OBJECT_STORAGE_DATABASE)
        .collection::<mongodb::bson::Document>("upload_locks");

    let lock_doc = doc! {
        "uid": &uid,
        "locked_at": Bson::DateTime(DateTime::now()),
    };

    if let Err(e) = locks_collection.insert_one(lock_doc).await {
        if let ErrorKind::Write(WriteFailure::WriteError(write_error)) = e.kind.as_ref() {
            if write_error.code == 11000 {
                warn!(
                    "User {} tried to initiate an upload while another was in progress.",
                    uid
                );
                return Err(AppError::Conflict(
                    "Another upload is currently being initiated. Please try again in a moment."
                        .to_string(),
                ));
            }
        }
        error!("Failed to acquire database lock for user {}: {}", uid, e);
        return Err(AppError::InternalServerError(
            "Could not acquire upload lock.".to_string(),
        ));
    }

    let result = async {
        let auth_collection = db_client
            .database(DATABASE)
            .collection::<UserAuthDetails>("auth");

        let user_auth_details = auth_collection
            .find_one(doc! { "uid": &uid })
            .await
            .map_err(|_| AppError::Conflict("Failed to query user details.".to_string()))?
            .ok_or(AppError::Unauthorized)?;

        let allocated_storage = user_auth_details.allocated_storage;
        let current_usage = calculate_storage_usage(db_client, &uid).await?;

        if current_usage.saturating_add(params.total_size_bytes) > allocated_storage {
            warn!(
                "User {} exceeded storage quota. Usage: {}, Allocation: {}, Attempted Upload: {}",
                uid, current_usage, allocated_storage, params.total_size_bytes
            );
            return Err(AppError::InsufficientStorage);
        }

        let file_id = Uuid::new_v4().to_string();
        let now = DateTime::now();

        let pending_metadata = ObjectMetadata {
            id: Uuid::new_v4().to_string(),
            fragment: fragment.clone(),
            key: object_key.clone(),
            version_id: "".to_string(),
            is_latest: false,
            size_bytes: params.total_size_bytes,
            content_type: params.content_type.clone().unwrap_or_default(),
            owner_uid: uid.clone(),
            created_at: now,
            last_modified: now,
            etag: "".to_string(),
            physical_location: "".to_string(),
            status: UploadStatus::Pending,
            session_id: Some(file_id.clone()),
        };

        let objects_collection = db_client
            .database(OBJECT_STORAGE_DATABASE)
            .collection::<ObjectMetadata>("objects");

        objects_collection
            .insert_one(&pending_metadata)
            .await
            .map_err(|e| {
                error!("Failed to create pending record: {}", e);
                AppError::InternalServerError("Failed to reserve storage space.".to_string())
            })?;

        let paths = ObjectPathManager::new(&file_id);

        with_io_timeout(
            "create temp upload directory",
            tokio::fs::create_dir_all(paths.temp_upload_dir()),
        )
        .await?;

        let metadata = TempUploadMetadata {
            file_id: file_id.clone(),
            fragment,
            object_key,
            uid: uid.clone(),
            content_type: params.content_type,
            total_size_bytes: params.total_size_bytes,
            total_chunks: params.total_chunks,
            creation_timestamp: Utc::now().to_rfc3339(),
        };

        let metadata_content = serde_json::to_string_pretty(&metadata)
            .map_err(|e| AppError::BadRequest(format!("Failed to serialize metadata: {}", e)))?;

        with_io_timeout(
            "writing metadata file",
            tokio::fs::write(paths.metadata_path(), metadata_content),
        )
        .await?;

        info!(
            "Initiated object upload session and reserved space for: {}",
            file_id
        );

        Ok((
            StatusCode::CREATED,
            AxumJson(InitiateUploadResponse { file_id }),
        ))
    }
    .await;

    if let Err(e) = locks_collection.delete_one(doc! { "uid": &uid }).await {
        error!("Failed to release database lock for user {}: {}", uid, e);
    }

    result
}

pub async fn get_capacity_usage(
    State(state): State<Arc<AppState>>,
    claims: Claims,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
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
    let _permit = CHUNK_UPLOAD_SEMAPHORE.acquire().await.map_err(|_| {
        AppError::InternalServerError("Failed to acquire upload permit.".to_string())
    })?;

    let chunk_key = format!("{}-{}", file_id, chunk_index);
    let chunk_lock = CHUNK_WRITE_LOCKS
        .entry(chunk_key)
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone();

    let _lock_guard = chunk_lock.lock().await;

    let paths = ObjectPathManager::new(&file_id);
    let field = with_timeout("reading multipart field", multipart.next_field())
        .await??
        .ok_or_else(|| AppError::BadRequest("No multipart field found.".to_string()))?;
    let expected_checksum = params.get("checksum").map(|s| s.as_str());
    save_chunk_stream(&paths, chunk_index, field, expected_checksum).await?;

    Ok((StatusCode::OK, "Chunk uploaded successfully."))
}

pub async fn object_upload_chunk_raw(
    Path((file_id, chunk_index)): Path<(String, u64)>,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, AppError> {
    let _permit = CHUNK_UPLOAD_SEMAPHORE.acquire().await.map_err(|_| {
        AppError::InternalServerError("Failed to acquire upload permit.".to_string())
    })?;

    let chunk_key = format!("{}-{}", file_id, chunk_index);
    let chunk_lock = CHUNK_WRITE_LOCKS
        .entry(chunk_key)
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone();
    let _lock_guard = chunk_lock.lock().await;

    let paths = ObjectPathManager::new(&file_id);

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

    save_chunk_from_raw_stream(
        &paths,
        chunk_index,
        body,
        expected_checksum_b64,
        expected_buffer_size,
    )
    .await?;

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
        if let Err(e) = assemble_and_index_object(&paths, Arc::clone(&state.db_client)).await {
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

async fn save_chunk_from_raw_stream(
    paths: &ObjectPathManager<'_>,
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

async fn assemble_and_index_object(
    paths: &ObjectPathManager<'_>,
    db: Arc<MongoClient>,
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
    let now = DateTime::now();

    let collection = db
        .database(OBJECT_STORAGE_DATABASE)
        .collection::<ObjectMetadata>("objects");

    let pending_doc = collection
        .find_one(doc! { "session_id": &temp_metadata.file_id })
        .await
        .map_err(|e| {
            error!(
                "Failed to find pending document for session {}: {}",
                &temp_metadata.file_id, e
            );
            AppError::Conflict("Could not find the pending upload record.".to_string())
        })?
        .ok_or_else(|| {
            error!(
                "No pending document found for session {}",
                &temp_metadata.file_id
            );
            AppError::Conflict("Pending upload record is missing.".to_string())
        })?;

    let final_metadata = ObjectMetadata {
        id: pending_doc.id,
        fragment: temp_metadata.fragment.clone(),
        key: temp_metadata.object_key.clone(),
        version_id: new_version_id,
        is_latest: true,
        size_bytes: final_size,
        content_type: temp_metadata
            .content_type
            .unwrap_or_else(|| "application/octet-stream".to_string()),
        owner_uid: temp_metadata.uid.clone(),
        created_at: now,
        last_modified: now,
        etag: final_checksum,
        physical_location: physical_path_str,
        status: UploadStatus::Completed,
        session_id: None,
    };

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

    let filter_pending = doc! { "session_id": &temp_metadata.file_id };
    collection
        .replace_one(filter_pending, &final_metadata)
        .await
        .map_err(|e| {
            error!(
                "Failed to replace pending record with final metadata: {}",
                e
            );
            AppError::Conflict("Failed to finalize object metadata record.".to_string())
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

    for i in 0..temp_metadata.total_chunks {
        let chunk_key = format!("{}-{}", temp_metadata.file_id, i);
        CHUNK_WRITE_LOCKS.remove(&chunk_key);
    }

    info!(
        "Cleaned up chunk locks for session {}",
        temp_metadata.file_id
    );

    Ok(())
}
