use crate::database::database_constants::DATABASE;
use crate::enums::app_error_enum::AppError;
use crate::fs::fragmented_upload::with_io_timeout;
use crate::object_storage::caching::{add_key_to_list_cache, invalidate_cache};
use crate::object_storage::constants::{PART_META_SUFFIX, PART_SUFFIX};
use crate::object_storage::quota_manager::{init_quota_file, try_reserve_quota, update_quota};
use crate::object_storage::storage_capacity_parser::calculate_storage_usage;
use crate::routes::storage_routes::AppState;
use crate::statics::merge_semaphore_static::CHUNK_UPLOAD_SEMAPHORE;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{
    CapacityUsageResponse, InitiateUploadParams, InitiateUploadResponse, ObjectKeyQuery,
    ObjectMetadata, ObjectPathManager, PartMetadata, ReservationReceipt, UploadPartQuery,
    UploadStatusResponse, UserRecord,
};
use crate::websocket::ws_structs::StorageEventPayload;
use axum::body::Body;
use axum::http::HeaderMap;
use axum::{
    Json as AxumJson,
    extract::{Path as AxumPath, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use chrono::Utc;
use crc32fast::Hasher;
use futures_util::{StreamExt, stream};
use mongodb::bson::doc;
use std::io::SeekFrom;
#[cfg(not(unix))]
use std::path::Path;
use std::sync::Arc;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tracing::{error, info, warn};
use ulid::Ulid;
use walkdir::WalkDir;

#[cfg(target_os = "linux")]
const O_DIRECT: i32 = 0x4000;

#[cfg(unix)]
pub async fn sync_dir(path: &std::path::Path) -> Result<(), AppError> {
    let dir = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY)
        .open(path)
        .await
        .map_err(|e| AppError::Io(e, "open dir for sync".into()))?;
    dir.sync_all().await.map_err(|e| AppError::Io(e, "sync dir".into()))?;
    Ok(())
}

#[cfg(not(unix))]
pub async fn sync_dir(_path: &Path) -> Result<(), AppError> {
    Ok(())
}

pub async fn initiate_object_upload(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumPath((fragment, object_key)): AxumPath<(String, String)>,
    AxumJson(params): AxumJson<InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;

    if let Err(e) = try_reserve_quota(&uid, params.total_size_bytes).await {
        if let AppError::Unauthorized = e {
            info!(
                "Quota missing for {}, fetching allocation from MongoDB...",
                uid
            );
            let auth_collection = state
                .db_client
                .database(DATABASE)
                .collection::<UserRecord>("auth");
            let user_record = auth_collection
                .find_one(doc! { "uid": &uid })
                .await
                .map_err(|_| AppError::InternalServerError("Auth DB unreachable".into()))?
                .ok_or(AppError::Unauthorized)?;

            init_quota_file(&uid, user_record.allocated_storage).await?;

            try_reserve_quota(&uid, params.total_size_bytes).await?;
        } else {
            return Err(e);
        }
    }

    let version_id = Ulid::new().to_string();
    let session_id = Ulid::new().to_string();

    let path_manager = ObjectPathManager::new();
    let blob_dir = path_manager.get_blob_dir(&uid, &fragment, &version_id);

    if let Err(e) = with_io_timeout(
        "creating upload directory",
        fs::create_dir_all(&blob_dir),
    )
    .await
    {
        let _ = update_quota(&uid, -(params.total_size_bytes as i64)).await;
        return Err(e);
    }

    info!(
        "Initiated upload: {}/{} (v: {}) [Reserved: {} bytes]",
        fragment, object_key, version_id, params.total_size_bytes
    );

    add_key_to_list_cache(&uid, &fragment, object_key).await;

    let receipt = ReservationReceipt {
        amount: params.total_size_bytes,
    };

    if let Ok(bytes) = mongodb::bson::to_vec(&receipt) {
        let receipt_path = blob_dir.join("reservation.bson");
        if let Err(e) = fs::write(&receipt_path, bytes).await {
            warn!("Failed to write reservation receipt for {}: {}", uid, e);
        }
    }

    Ok((
        StatusCode::CREATED,
        AxumJson(InitiateUploadResponse {
            session_id,
            version_id,
        }),
    ))
}

pub async fn get_upload_status(
    claims: Claims,
    AxumPath((fragment, version_id)): AxumPath<(String, String)>,
    Query(key_query): Query<ObjectKeyQuery>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let _object_key = key_query.object_key;
    let path_manager = ObjectPathManager::new();
    let blob_dir = path_manager.get_blob_dir(&uid, &fragment, &version_id);

    if !fs::try_exists(&blob_dir).await.unwrap_or(false) {
        return Err(AppError::NotFound);
    }

    let mut uploaded_parts = Vec::new();
    let mut entries = fs::read_dir(&blob_dir)
        .await
        .map_err(|e| AppError::Io(e, "read dir".into()))?;

    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        if name_str.ends_with(PART_META_SUFFIX) {
            if let Some(rest) = name_str.strip_prefix("part_") {
                if let Some(index_str) = rest.strip_suffix(PART_META_SUFFIX) {
                    if let Ok(index) = index_str.parse::<u64>() {
                        uploaded_parts.push(index);
                    }
                }
            }
        }
    }

    uploaded_parts.sort();

    Ok(AxumJson(UploadStatusResponse { uploaded_parts }))
}

pub async fn object_upload_part(
    claims: Claims,
    headers: HeaderMap,
    AxumPath((fragment, version_id, part_index)): AxumPath<(String, String, u64)>,
    Query(query): Query<UploadPartQuery>,
    body: Body,
) -> Result<impl IntoResponse, AppError> {
    let requested_size = query.buffer_size.unwrap_or(131_072).max(4096);
    let align = 4096;
    let buffer_size = (requested_size / align) * align;

    let _permit = CHUNK_UPLOAD_SEMAPHORE
        .acquire()
        .await
        .map_err(|_| AppError::InternalServerError("Server busy.".to_string()))?;

    let uid = claims.uid;
    let path_manager = ObjectPathManager::new();
    let blob_dir = path_manager.get_blob_dir(&uid, &fragment, &version_id);

    let part_filename = format!("part_{}{}", part_index, PART_SUFFIX);
    let part_path = blob_dir.join(&part_filename);

    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);

    #[cfg(target_os = "linux")]
    {
        options.custom_flags(O_DIRECT);
    }

    let mut file = match options.open(&part_path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok((StatusCode::GONE, "Upload cancelled (Dir missing)").into_response());
        }
        Err(e) => return Err(AppError::Io(e, "create part file".into())),
    };

    if let Some(content_length) = headers.get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok()) 
    {
        if content_length > 0 {
            let _ = file.set_len(content_length).await;
        }
    }

    let calc_checksum = query.enable_checksum.unwrap_or(false);
    let verify_checksum = query.checksum.as_ref();
    let mut hasher = if calc_checksum || verify_checksum.is_some() { Some(Hasher::new()) } else { None };

    let mut size_bytes = 0;
    let mut stream = body.into_data_stream();

    let mut raw_buffer = vec![0u8; buffer_size + align];
    let ptr_addr = raw_buffer.as_ptr() as usize;
    let align_offset = (align - (ptr_addr % align)) % align;
    let mut buffer_len = 0;

    while let Some(chunk_res) = stream.next().await {
        let data = match chunk_res {
            Ok(d) => d,
            Err(_) => return Ok((StatusCode::GONE, "Stream interrupted").into_response()),
        };

        let mut data_slice = &data[..];
        while !data_slice.is_empty() {
            let window_start = align_offset + buffer_len;
            let available = buffer_size - buffer_len;
            let to_copy = std::cmp::min(available, data_slice.len());

            raw_buffer[window_start..window_start + to_copy].copy_from_slice(&data_slice[..to_copy]);
            
            if let Some(h) = &mut hasher {
                h.update(&data_slice[..to_copy]);
            }

            buffer_len += to_copy;
            data_slice = &data_slice[to_copy..];

            if buffer_len == buffer_size {
                let aligned_slice = &raw_buffer[align_offset..align_offset + buffer_size];
                
                file.write_all(aligned_slice).await
                    .map_err(|e| AppError::Io(e, "write part aligned".into()))?;

                size_bytes += buffer_len as u64;
                buffer_len = 0;
            }
        }
    }

    if buffer_len > 0 {
        drop(file);
        let mut tail_file = OpenOptions::new().write(true).open(&part_path).await
            .map_err(|e| AppError::Io(e, "open part for tail".into()))?;
        
        tail_file.seek(SeekFrom::Start(size_bytes)).await
            .map_err(|e| AppError::Io(e, "seek tail".into()))?;
            
        tail_file.write_all(&raw_buffer[align_offset..align_offset + buffer_len]).await
            .map_err(|e| AppError::Io(e, "write tail unaligned".into()))?;
            
        size_bytes += buffer_len as u64;
    }

    let checksum_hex = hasher.map(|h| format!("{:08x}", h.finalize()));

    if let Some(expected) = verify_checksum {
        if let Some(calculated) = &checksum_hex {
            if calculated.to_lowercase() != expected.to_lowercase() {
                let _ = fs::remove_file(&part_path).await;
                return Ok((StatusCode::BAD_REQUEST, "Checksum mismatch").into_response());
            }
        }
    }

    let part_meta = PartMetadata {
        part_index,
        size_bytes,
        file_name: part_filename.clone(),
        created_at: Utc::now().timestamp(),
        checksum: checksum_hex,
    };

    let meta_data = mongodb::bson::to_vec(&part_meta).map_err(|e| {
        AppError::InternalServerError(format!("Failed to serialize part metadata: {}", e))
    })?;

    let meta_path = blob_dir.join(format!("part_{}{}", part_index, PART_META_SUFFIX));
    fs::write(&meta_path, meta_data).await
        .map_err(|e| AppError::Io(e, "write part meta".into()))?;

    Ok((StatusCode::OK, "Part uploaded.").into_response())
}

pub async fn finalize_object_upload(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumPath((fragment, version_id, session_id)): AxumPath<(String, String, String)>,
    Query(key_query): Query<ObjectKeyQuery>,
    AxumJson(params): AxumJson<InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    let object_key = key_query.object_key;
    let uid = claims.uid;

    let path_manager = ObjectPathManager::new();
    let blob_dir: Arc<std::path::Path> = Arc::from(path_manager.get_blob_dir(&uid, &fragment, &version_id));
    let blob_dir_for_task = Arc::clone(&blob_dir);

    let part_meta_paths = tokio::task::spawn_blocking(move || -> Result<Vec<std::path::PathBuf>, AppError> {
        let mut paths = Vec::new();
        for entry in WalkDir::new(blob_dir_for_task.as_ref())
            .max_depth(1)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let path = entry.path();
            if path.is_file() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name.ends_with(PART_META_SUFFIX) {
                        paths.push(path.to_owned());
                    }
                }
            }
        }
        Ok(paths)
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("Task join error: {}", e)))??;

    if part_meta_paths.is_empty() {
        return Err(AppError::BadRequest("No parts found to finalize.".to_string()));
    }

    let results: Vec<(PartMetadata, std::path::PathBuf)> = stream::iter(part_meta_paths)
        .map(|path| async move {
            let data = fs::read(&path)
                .await
                .map_err(|e| AppError::Io(e, "read part meta".into()))?;

            let part: PartMetadata = mongodb::bson::from_slice(&data)
                .map_err(|_| AppError::InternalServerError("Corrupt part meta".into()))?;

            Ok::<_, AppError>((part, path))
        })
        .buffer_unordered(50)
        .collect::<Vec<Result<_, _>>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    let (mut parts, paths_for_cleanup): (Vec<_>, Vec<_>) = results.into_iter().unzip();
    
    parts.sort_by_key(|p| p.part_index);

    let mut total_size_calculated = 0;
    for (i, part) in parts.iter().enumerate() {
        if part.part_index != i as u64 {
            return Err(AppError::BadRequest(format!("Missing part index {}", i)));
        }
        total_size_calculated += part.size_bytes;
    }

    let object_meta = ObjectMetadata {
        object_key: object_key.clone(),
        fragment: fragment.clone(),
        version_id: version_id.clone(),
        upload_session_id: session_id,
        owner_uid: uid.clone(),
        content_type: params
            .content_type
            .unwrap_or_else(|| "application/octet-stream".to_string()),
        total_size_bytes: total_size_calculated,
        created_at: Utc::now().timestamp(),
        parts,
        is_delete_marker: false,
    };

    let bson_data = mongodb::bson::to_vec(&object_meta).map_err(|e| {
        AppError::InternalServerError(format!("Failed to serialize object metadata: {}", e))
    })?;

    let index_path = path_manager.get_metadata_path(&uid, &fragment, &object_key, &version_id);
    
    if let Some(parent) = index_path.parent() {
        if !fs::try_exists(parent).await.unwrap_or(false) {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| AppError::Io(e, "create index shard dir".into()))?;
        }
    }

    let mut final_meta_file = File::create(&index_path)
        .await
        .map_err(|e| AppError::Io(e, "create object meta".into()))?;

    final_meta_file
        .write_all(&bson_data)
        .await
        .map_err(|e| AppError::Io(e, "write object meta".into()))?;

    final_meta_file
        .sync_all()
        .await
        .map_err(|e| AppError::Io(e, "sync object meta".into()))?;

    let cleanup_futures = stream::iter(paths_for_cleanup)
        .map(|path| async move {
            if let Err(e) = fs::remove_file(&path).await {
                error!("Failed to cleanup part meta file {:?}: {}", path, e);
            }
        })
        .buffer_unordered(20);

    cleanup_futures.collect::<Vec<_>>().await;
    
    let receipt_path = blob_dir.join("reservation.bson");
    let _ = fs::remove_file(receipt_path).await;

    sync_dir(blob_dir.as_ref()).await?;

    let reserved_size = params.total_size_bytes as i64;
    let actual_size = total_size_calculated as i64;
    let delta = actual_size - reserved_size;

    if delta != 0 {
        update_quota(&uid, delta).await?;
        info!("Quota adjustment for {}: {} bytes (Reserved: {}, Actual: {})", uid, delta, reserved_size, actual_size);
    }

    invalidate_cache(&fragment, &object_key, &uid).await;

    state.emit(&uid, StorageEventPayload::FileUploaded(object_meta.clone()));

    info!("Finalized object: {}/{} (Size: {})", fragment, object_key, total_size_calculated);

    Ok((StatusCode::OK, "Upload finalized."))
}

pub async fn cancel_upload_session(
    claims: Claims,
    AxumPath((fragment, version_id)): AxumPath<(String, String)>,
    Query(query): Query<ObjectKeyQuery>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let path_manager = ObjectPathManager::new();
    let blob_dir = path_manager.get_blob_dir(&uid, &fragment, &version_id);

    if !fs::try_exists(&blob_dir).await.unwrap_or(false) {
        return Err(AppError::NotFound);
    }

    let index_path = path_manager.get_metadata_path(&uid, &fragment, &query.object_key, &version_id);
    if fs::try_exists(&index_path).await.unwrap_or(false) {
        return Err(AppError::Conflict("Cannot cancel: Object is already finalized.".to_string()));
    }

    let receipt_path = blob_dir.join("reservation.bson");
    let mut refund_amount = 0;

    if let Ok(data) = fs::read(&receipt_path).await {
        if let Ok(receipt) = mongodb::bson::from_slice::<ReservationReceipt>(&data) {
            refund_amount = receipt.amount;
        }
    }

    fs::remove_dir_all(&blob_dir).await.map_err(|e| {
        error!("Failed to cancel upload session {}: {}", version_id, e);
        AppError::InternalServerError("Failed to cleanup upload session.".to_string())
    })?;

    if refund_amount > 0 {
        if let Err(e) = update_quota(&uid, -(refund_amount as i64)).await {
            error!("Failed to refund quota for cancelled upload {}: {}", uid, e);
        }
    }

    info!(
        "Cancelled upload session: {}/{} (Refunded: {} bytes)",
        fragment, version_id, refund_amount
    );

    Ok(StatusCode::NO_CONTENT)
}

pub async fn get_capacity_usage(claims: Claims) -> Result<impl IntoResponse, AppError> {
    let usage = calculate_storage_usage(&claims.uid).await?;
    Ok(AxumJson(CapacityUsageResponse {
        capacity_used: usage,
    }))
}
