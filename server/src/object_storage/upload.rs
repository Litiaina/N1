use crate::database::database_constants::DATABASE;
use crate::enums::app_error_enum::AppError;
use crate::fs::fragmented_upload::with_io_timeout;
use crate::object_storage::caching::{add_key_to_list_cache, invalidate_cache};
use crate::object_storage::constants::{OBJECT_META_FILE, PART_META_SUFFIX, PART_SUFFIX};
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
use axum::{
    Json as AxumJson,
    extract::{Path as AxumPath, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use chrono::Utc;
use futures_util::{StreamExt, stream};
use mongodb::bson::doc;
#[cfg(not(unix))]
use std::path::Path;
use std::sync::Arc;
use tokio::fs::{self, File};
use tokio::io::{AsyncWriteExt, BufWriter};
use tracing::{error, info, warn};
use ulid::Ulid;
use walkdir::WalkDir;

#[cfg(unix)]
pub async fn sync_dir(path: &Path) -> Result<(), AppError> {
    use std::os::unix::fs::OpenOptionsExt;
    use tokio::fs::OpenOptions;

    let dir = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY)
        .open(path)
        .await
        .map_err(|e| AppError::Io(e, "open dir for sync".into()))?;

    dir.sync_all()
        .await
        .map_err(|e| AppError::Io(e, "sync dir".into()))?;

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
    let version_dir = path_manager.get_version_dir(&uid, &fragment, &object_key, &version_id);

    if let Err(e) = with_io_timeout(
        "creating upload directory",
        fs::create_dir_all(&version_dir),
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
        let receipt_path = version_dir.join("reservation.bson");
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
    let object_key = key_query.object_key;
    let path_manager = ObjectPathManager::new();
    let version_dir = path_manager.get_version_dir(&uid, &fragment, &object_key, &version_id);

    if !fs::try_exists(&version_dir).await.unwrap_or(false) {
        return Err(AppError::NotFound);
    }

    let mut uploaded_parts = Vec::new();
    let mut entries = fs::read_dir(&version_dir)
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
    AxumPath((fragment, version_id, part_index)): AxumPath<(String, String, u64)>,
    Query(query): Query<UploadPartQuery>,
    body: Body,
) -> Result<impl IntoResponse, AppError> {
    let object_key = query.object_key;
    let buffer_size = query.buffer_size.unwrap_or(128 * 1024).max(4096);

    let _permit = CHUNK_UPLOAD_SEMAPHORE
        .acquire()
        .await
        .map_err(|_| AppError::InternalServerError("Server busy.".to_string()))?;

    let uid = claims.uid;
    let path_manager = ObjectPathManager::new();
    let version_dir = path_manager.get_version_dir(&uid, &fragment, &object_key, &version_id);

    if !fs::try_exists(&version_dir).await.unwrap_or(false) {
        return Err(AppError::NotFound);
    }

    let part_filename = format!("part_{}{}", part_index, PART_SUFFIX);
    let meta_filename = format!("part_{}{}", part_index, PART_META_SUFFIX);

    let part_path = version_dir.join(&part_filename);
    let meta_path = version_dir.join(&meta_filename);

    let file = File::create(&part_path)
        .await
        .map_err(|e| AppError::Io(e, "create part file".into()))?;

    let mut writer = BufWriter::with_capacity(buffer_size, file);
    let mut size_bytes = 0;

    let mut stream = body.into_data_stream();

    while let Some(chunk_res) = stream.next().await {
        let data =
            chunk_res.map_err(|e| AppError::BadRequest(format!("Stream read failed: {}", e)))?;
        size_bytes += data.len() as u64;
        writer
            .write_all(&data)
            .await
            .map_err(|e| AppError::Io(e, "write part".into()))?;
    }

    writer
        .flush()
        .await
        .map_err(|e| AppError::Io(e, "flush part".into()))?;

    let part_meta = PartMetadata {
        part_index,
        size_bytes,
        file_name: part_filename,
        created_at: Utc::now().timestamp(),
    };

    let bson_data = mongodb::bson::to_vec(&part_meta).map_err(|e| {
        AppError::InternalServerError(format!("Failed to serialize part metadata: {}", e))
    })?;

    fs::write(&meta_path, bson_data)
        .await
        .map_err(|e| AppError::Io(e, "write part meta".into()))?;

    Ok((StatusCode::OK, "Part uploaded."))
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
    let version_dir = path_manager.get_version_dir(&uid, &fragment, &object_key, &version_id);

    let version_dir_clone = version_dir.clone();

    let part_meta_paths =
        tokio::task::spawn_blocking(move || -> Result<Vec<std::path::PathBuf>, AppError> {
            let mut paths = Vec::new();
            for entry in WalkDir::new(&version_dir_clone)
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
        return Err(AppError::BadRequest(
            "No parts found to finalize.".to_string(),
        ));
    }

    let parts_result: Result<Vec<PartMetadata>, AppError> = stream::iter(part_meta_paths.clone())
        .map(|path| async move {
            let data = fs::read(&path)
                .await
                .map_err(|e| AppError::Io(e, "read part meta".into()))?;

            let part: PartMetadata = mongodb::bson::from_slice(&data)
                .map_err(|_| AppError::InternalServerError("Corrupt part meta".into()))?;

            Ok(part)
        })
        .buffer_unordered(50)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect();

    let mut parts = parts_result?;
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
            .unwrap_or("application/octet-stream".to_string()),
        total_size_bytes: total_size_calculated,
        created_at: Utc::now().timestamp(),
        parts,
        is_delete_marker: false,
    };

    let bson_data = mongodb::bson::to_vec(&object_meta).map_err(|e| {
        AppError::InternalServerError(format!("Failed to serialize object metadata: {}", e))
    })?;

    let meta_path = version_dir.join(OBJECT_META_FILE);
    let mut final_meta_file = File::create(&meta_path)
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

    let cleanup_futures = stream::iter(part_meta_paths)
        .map(|path| async move {
            if let Err(e) = fs::remove_file(&path).await {
                error!("Failed to cleanup part meta file {:?}: {}", path, e);
            }
        })
        .buffer_unordered(20);

    cleanup_futures.collect::<Vec<_>>().await;
    let receipt_path = version_dir.join("reservation.bson");
    let _ = fs::remove_file(receipt_path).await;

    sync_dir(&version_dir).await?;

    let reserved_size = params.total_size_bytes as i64;
    let actual_size = total_size_calculated as i64;
    let delta = actual_size - reserved_size;

    if delta != 0 {
        update_quota(&uid, delta).await?;
        info!(
            "Quota adjustment for {}: {} bytes (Reserved: {}, Actual: {})",
            uid, delta, reserved_size, actual_size
        );
    }

    invalidate_cache(&fragment, &object_key, &uid).await;

    state.emit(&uid, StorageEventPayload::FileUploaded(object_meta.clone()));

    info!(
        "Finalized object: {}/{} (Size: {})",
        fragment, object_key, total_size_calculated
    );

    Ok((StatusCode::OK, "Upload finalized."))
}

pub async fn get_capacity_usage(claims: Claims) -> Result<impl IntoResponse, AppError> {
    let usage = calculate_storage_usage(&claims.uid).await?;
    Ok(AxumJson(CapacityUsageResponse {
        capacity_used: usage,
    }))
}
