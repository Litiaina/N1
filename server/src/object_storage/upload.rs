use crate::database::database_constants::DATABASE;
use crate::enums::app_error_enum::AppError;
use crate::fs::fragmented_upload::with_io_timeout;
use crate::object_storage::caching::invalidate_cache;
use crate::object_storage::constants::{PARITY_SUFFIX, PART_META_SUFFIX, SHARD_SUFFIX};
use crate::object_storage::ec::ErasureStreamWriter;
use crate::object_storage::quota_manager::{init_quota_file, try_reserve_quota, update_quota};
use crate::object_storage::storage_capacity_parser::calculate_storage_usage;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{
    CapacityUsageResponse, InitiateUploadParams, InitiateUploadResponse, ObjectKeyQuery,
    ObjectMetadata, ObjectPathManager, PartMetadata, ReservationReceipt, ShardInfo,
    UploadPartQuery, UploadStatusResponse, UserRecord, default_stripe_size,
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
use futures_util::future::join_all;
use futures_util::{StreamExt, stream};
use mongodb::bson::doc;
use once_cell::sync::{Lazy, OnceCell};
#[cfg(not(unix))]
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};
use ulid::Ulid;
use walkdir::WalkDir;

pub static MAX_PART_SIZE_ATOMIC: OnceCell<AtomicU64> = OnceCell::new();
pub static DATA_SHARDS_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static PARITY_SHARDS_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static WRITER_BUFFER_SIZE_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static STRIPE_SIZE_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static MAX_IO_SEMAPHORE: OnceCell<usize> = OnceCell::new();

static IO_SEMAPHORE: Lazy<Semaphore> =
    Lazy::new(|| Semaphore::new(*MAX_IO_SEMAPHORE.get().unwrap()));

async fn create_optimized_file(path: &PathBuf) -> Result<fs::File, AppError> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);

    #[cfg(target_os = "linux")]
    {
        options.custom_flags(libc::O_DIRECT);
    }

    #[cfg(windows)]
    {
        const FILE_FLAG_NO_BUFFERING: u32 = 0x20000000;
        options.custom_flags(FILE_FLAG_NO_BUFFERING);
    }

    options
        .open(path)
        .await
        .map_err(|e| AppError::Io(e, format!("create {}", path.display())))
}

#[cfg(unix)]
pub async fn sync_dir(path: &std::path::Path) -> Result<(), AppError> {
    let dir = tokio::fs::OpenOptions::new()
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

#[cfg(windows)]
pub async fn sync_dir(path: &Path) -> Result<(), AppError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        use std::fs::OpenOptions;
        use std::os::windows::fs::OpenOptionsExt;

        const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x02000000;

        let result = OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(&path);

        match result {
            Ok(file) => {
                if let Err(e) = file.sync_all() {
                    warn!(
                        "Windows: Failed to sync directory (non-fatal): {} - {}",
                        path.display(),
                        e
                    );
                }
            }
            Err(e) => {
                warn!(
                    "Windows: Failed to open directory for sync (non-fatal): {} - {}",
                    path.display(),
                    e
                );
            }
        }

        Ok(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("sync dir task: {}", e)))?
}

#[cfg(not(any(unix, windows)))]
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
    let meta_dirs = path_manager.get_all_session_dirs(&uid, &fragment, &version_id);

    let create_futures = meta_dirs.iter().map(|path| async move {
        with_io_timeout(
            "creating upload session directory",
            fs::create_dir_all(path),
        )
        .await
    });

    let results = join_all(create_futures).await;
    let mut success_count = 0;

    for (i, result) in results.into_iter().enumerate() {
        if let Err(e) = result {
            warn!(
                "Failed to create session dir on index disk {}: {}. Continuing if possible.",
                i, e
            );
        } else {
            success_count += 1;
        }
    }

    if success_count == 0 {
        error!("CRITICAL: Failed to create session directory on ANY index disk.");
        let _ = update_quota(&uid, -(params.total_size_bytes as i64)).await;
        return Err(AppError::InternalServerError(
            "Storage backend unavailable".into(),
        ));
    }

    info!(
        "Initiated upload: {}/{} (v: {}) [Reserved: {} bytes]",
        fragment, object_key, version_id, params.total_size_bytes
    );

    let receipt = ReservationReceipt {
        amount: params.total_size_bytes,
    };

    if let Ok(bytes) = mongodb::bson::to_vec(&receipt) {
        let write_futures = meta_dirs.iter().map(|path| {
            let receipt_path = path.join("reservation.bson");
            let data = &bytes;
            async move { fs::write(&receipt_path, data).await }
        });

        let _ = join_all(write_futures).await;
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

    let meta_dirs = path_manager.get_all_session_dirs(&uid, &fragment, &version_id);
    let mut uploaded_parts = Vec::new();
    let mut read_success = false;

    for meta_dir in meta_dirs {
        if !fs::try_exists(&meta_dir).await.unwrap_or(false) {
            continue;
        }

        match fs::read_dir(&meta_dir).await {
            Ok(mut entries) => {
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
                read_success = true;
                break;
            }
            Err(e) => {
                warn!(
                    "Failed to read dir {:?}: {}, trying next mirror",
                    meta_dir, e
                );
            }
        }
    }

    if !read_success {
        return Err(AppError::NotFound);
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
    let _io_guard = IO_SEMAPHORE
        .acquire()
        .await
        .map_err(|_| AppError::InternalServerError("Server busy".into()))?;

    let max_part_size = MAX_PART_SIZE_ATOMIC.get().unwrap().load(Ordering::Relaxed);
    let data_shards = DATA_SHARDS_ATOMIC.get().unwrap().load(Ordering::Relaxed) as usize;
    let parity_shards = PARITY_SHARDS_ATOMIC.get().unwrap().load(Ordering::Relaxed) as usize;
    let total_shards = data_shards + parity_shards;

    let block_size = STRIPE_SIZE_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or_else(default_stripe_size);

    let content_length = headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .ok_or(AppError::BadRequest(
            "Content-Length header required".into(),
        ))?;

    if content_length > max_part_size {
        return Ok((StatusCode::PAYLOAD_TOO_LARGE, "Part too large").into_response());
    }

    let uid_arc = Arc::new(claims.uid);
    let fragment_arc = Arc::new(fragment);
    let version_id_arc = Arc::new(version_id);
    let path_manager = Arc::new(ObjectPathManager::new());

    let stripe_data_width = block_size as u64 * data_shards as u64;
    let stripe_count = (content_length + stripe_data_width - 1) / stripe_data_width;
    let final_shard_len = stripe_count * block_size as u64;

    let mut writers: Vec<Option<File>> = Vec::with_capacity(total_shards);
    let mut shard_paths = Vec::with_capacity(total_shards);
    let mut file_names = Vec::with_capacity(total_shards);
    let mut successful_opens = 0;

    let blob_roots = path_manager.get_blob_roots();
    let num_disks = blob_roots.len();

    if num_disks == 0 {
        return Err(AppError::InternalServerError(
            "No blob storage disks configured".into(),
        ));
    }

    for i in 0..total_shards {
        let is_parity = i >= data_shards;
        let ext = if is_parity {
            PARITY_SUFFIX
        } else {
            SHARD_SUFFIX
        };
        let file_name = format!("part_{}_shard_{}{}", part_index, i, ext);

        let disk_idx = (part_index as usize + i) % num_disks;
        let target_disk = &blob_roots[disk_idx];

        let shard_dir = path_manager.get_shard_dir_on_disk(
            target_disk,
            &uid_arc,
            &fragment_arc,
            &version_id_arc,
        );

        if !shard_dir.exists() {
            let _ = with_io_timeout("create shard dir", fs::create_dir_all(&shard_dir)).await;
        }

        let path = shard_dir.join(&file_name);

        match create_optimized_file(&path).await {
            Ok(file) => {
                if let Err(e) =
                    with_io_timeout("pre-allocate shard", file.set_len(final_shard_len)).await
                {
                    warn!("Failed to pre-allocate shard on disk {}: {}", disk_idx, e);
                    writers.push(None);
                } else {
                    writers.push(Some(file));
                    successful_opens += 1;
                }
            }
            Err(e) => {
                warn!(
                    "Failed to open shard file on disk {} ({:?}): {}",
                    disk_idx, path, e
                );
                writers.push(None);
            }
        }

        shard_paths.push(path.to_string_lossy().to_string());
        file_names.push(file_name);
    }

    if successful_opens < data_shards {
        return Err(AppError::InternalServerError(format!(
            "Not enough healthy disks to write data. Need {}, got {}.",
            data_shards, successful_opens
        )));
    }

    let mut stream_writer = ErasureStreamWriter::new(
        writers,
        data_shards,
        parity_shards,
        final_shard_len as usize,
        query.enable_checksum.unwrap_or(false),
        block_size,
    );

    let mut body_stream = body.into_data_stream();
    let mut total_bytes_read = 0u64;

    while let Some(chunk_result) = body_stream.next().await {
        let chunk = chunk_result.map_err(|e| {
            AppError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e),
                "stream read".into(),
            )
        })?;
        if chunk.is_empty() {
            continue;
        }
        total_bytes_read += chunk.len() as u64;

        stream_writer.write(&chunk).await?;
    }

    stream_writer.finalize().await?;

    if let Some(expected) = query.checksum {
        let calculated = stream_writer.get_total_checksum();
        if let Some(calc_str) = calculated {
            if expected.to_lowercase() != calc_str.to_lowercase() {
                return Ok((StatusCode::BAD_REQUEST, "Checksum mismatch").into_response());
            }
        }
    }

    let shard_checksums = stream_writer.get_shard_checksums();
    let mut shards_info = Vec::with_capacity(total_shards);

    for i in 0..total_shards {
        shards_info.push(ShardInfo {
            shard_index: i,
            file_name: file_names[i].clone(),
            absolute_path: shard_paths[i].clone(),
            size_bytes: final_shard_len,
            checksum: shard_checksums.get(i).cloned().unwrap_or_default(),
            is_parity: i >= data_shards,
        });
    }

    let part_meta = PartMetadata {
        part_index,
        size_bytes: total_bytes_read,
        created_at: Utc::now().timestamp(),
        checksum: stream_writer.get_total_checksum(),
        shards: shards_info,
    };

    let meta_dirs = path_manager.get_all_session_dirs(&uid_arc, &fragment_arc, &version_id_arc);

    let meta_bytes = Arc::new(
        mongodb::bson::to_vec(&part_meta)
            .map_err(|e| AppError::InternalServerError(format!("Serialize meta: {}", e)))?,
    );

    let write_futures = meta_dirs.into_iter().map(|dir| {
        let bytes = meta_bytes.clone();
        async move {
            if !dir.exists() {
                let _ = fs::create_dir_all(&dir).await;
            }
            let meta_path = dir.join(format!("part_{}{}", part_index, PART_META_SUFFIX));
            fs::write(&meta_path, &*bytes)
                .await
                .map_err(|e| AppError::Io(e, "write part meta".into()))
        }
    });

    let results = join_all(write_futures).await;
    let mut meta_success = 0;

    for result in results {
        if let Err(e) = result {
            warn!("Replication failed for part metadata: {}", e);
        } else {
            meta_success += 1;
        }
    }

    if meta_success == 0 {
        return Err(AppError::InternalServerError(
            "Failed to write part metadata to ANY index disk".into(),
        ));
    }

    Ok((StatusCode::OK, "Part uploaded").into_response())
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
    let meta_dirs = Arc::new(path_manager.get_all_session_dirs(&uid, &fragment, &version_id));

    let mut selected_index: Option<usize> = None;
    for (i, dir) in meta_dirs.iter().enumerate() {
        if fs::try_exists(dir).await.unwrap_or(false) {
            selected_index = Some(i);
            break;
        }
    }

    let index = selected_index.ok_or(AppError::NotFound)?;

    let meta_dirs_clone = meta_dirs.clone();

    let part_meta_paths =
        tokio::task::spawn_blocking(move || -> Result<Vec<std::path::PathBuf>, AppError> {
            let meta_dir = &meta_dirs_clone[index];
            let mut paths = Vec::new();
            for entry in WalkDir::new(meta_dir)
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

    let (mut parts, _): (Vec<_>, Vec<_>) = results.into_iter().unzip();

    parts.sort_by_key(|p| p.part_index);

    let mut total_size_calculated = 0;
    for (i, part) in parts.iter().enumerate() {
        if part.part_index != i as u64 {
            return Err(AppError::BadRequest(format!("Missing part index {}", i)));
        }
        total_size_calculated += part.size_bytes;
    }

    let stripe_size = STRIPE_SIZE_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or_else(default_stripe_size);

    let object_meta = ObjectMetadata {
        object_key,
        fragment,
        version_id,
        upload_session_id: session_id,
        owner_uid: uid,
        content_type: params
            .content_type
            .unwrap_or_else(|| "application/octet-stream".to_string()),
        total_size_bytes: total_size_calculated,
        created_at: Utc::now().timestamp(),
        parts,
        is_delete_marker: false,
        stripe_size_bytes: stripe_size,
    };

    let bson_data = mongodb::bson::to_vec(&object_meta).map_err(|e| {
        AppError::InternalServerError(format!("Failed to serialize object metadata: {}", e))
    })?;

    let index_paths = path_manager.get_all_metadata_paths(
        &object_meta.owner_uid,
        &object_meta.fragment,
        &object_meta.object_key,
        &object_meta.version_id,
    );

    let write_futures = index_paths.into_iter().map(|index_path| {
        let bson_data = &bson_data;
        async move {
            if let Some(parent) = index_path.parent() {
                if !fs::try_exists(parent).await.unwrap_or(false) {
                    let _ = fs::create_dir_all(parent).await;
                }
            }

            let mut final_meta_file = File::create(&index_path)
                .await
                .map_err(|e| AppError::Io(e, "create object meta".into()))?;

            final_meta_file
                .write_all(bson_data)
                .await
                .map_err(|e| AppError::Io(e, "write object meta".into()))?;

            final_meta_file
                .sync_all()
                .await
                .map_err(|e| AppError::Io(e, "sync object meta".into()))?;

            if let Some(parent) = index_path.parent() {
                let _ = sync_dir(parent).await;
            }
            Ok::<(), AppError>(())
        }
    });

    let write_results = join_all(write_futures).await;
    let mut success_count = 0;

    for result in write_results {
        if let Err(e) = result {
            warn!("Failed to write final metadata to one index disk: {}", e);
        } else {
            success_count += 1;
        }
    }

    if success_count == 0 {
        return Err(AppError::InternalServerError(
            "Failed to finalize upload on ANY index disk".into(),
        ));
    }

    let cleanup_futures = meta_dirs.iter().map(|dir| async move {
        if let Err(e) = fs::remove_dir_all(dir).await {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!("Failed to remove session dir {:?}: {}", dir, e);
            }
        }
    });
    join_all(cleanup_futures).await;

    let reserved_size = params.total_size_bytes as i64;
    let actual_size = total_size_calculated as i64;
    let delta = actual_size - reserved_size;

    if delta != 0 {
        update_quota(&object_meta.owner_uid, delta).await?;
        info!(
            "Quota adjustment for {}: {} bytes (Reserved: {}, Actual: {})",
            object_meta.owner_uid, delta, reserved_size, actual_size
        );
    }

    invalidate_cache(
        &object_meta.fragment,
        &object_meta.object_key,
        &object_meta.owner_uid,
    )
    .await;

    info!(
        "Finalized object: {}/{} (Size: {})",
        object_meta.fragment, object_meta.object_key, total_size_calculated
    );

    let owner_uid = object_meta.owner_uid.clone();

    let stripped_meta = ObjectMetadata {
        parts: Vec::new(),
        ..object_meta
    };

    state.emit(&owner_uid, StorageEventPayload::FileUploaded(stripped_meta));

    Ok((StatusCode::OK, "Upload finalized."))
}

pub async fn cancel_upload_session(
    claims: Claims,
    AxumPath((fragment, version_id)): AxumPath<(String, String)>,
    Query(key_query): Query<ObjectKeyQuery>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let path_manager = ObjectPathManager::new();

    let index_paths =
        path_manager.get_all_metadata_paths(&uid, &fragment, &key_query.object_key, &version_id);

    let mut any_finalized = false;
    for index_path in &index_paths {
        if fs::try_exists(index_path).await.unwrap_or(false) {
            any_finalized = true;
            break;
        }
    }

    if any_finalized {
        return Err(AppError::Conflict(
            "Cannot cancel: Object is already finalized.".to_string(),
        ));
    }

    let meta_dirs = path_manager.get_all_session_dirs(&uid, &fragment, &version_id);
    let mut refund_amount = 0;

    for meta_dir in &meta_dirs {
        let receipt_path = meta_dir.join("reservation.bson");
        if let Ok(data) = fs::read(&receipt_path).await {
            if let Ok(receipt) = mongodb::bson::from_slice::<ReservationReceipt>(&data) {
                refund_amount = receipt.amount;
                break;
            }
        }
    }

    let cleanup_meta_futures = meta_dirs.iter().map(|dir| {
        let dir = dir.clone();
        let version_id_clone = version_id.clone();
        async move {
            if let Err(e) = fs::remove_dir_all(&dir).await {
                if e.kind() != std::io::ErrorKind::NotFound {
                    warn!("Failed to clean session meta {}: {}", version_id_clone, e);
                }
            }
        }
    });
    join_all(cleanup_meta_futures).await;

    let blob_roots = path_manager.get_blob_roots();
    for disk in blob_roots {
        let shard_dir = path_manager.get_shard_dir_on_disk(&disk, &uid, &fragment, &version_id);
        if shard_dir.exists() {
            if let Err(e) = fs::remove_dir_all(&shard_dir).await {
                warn!("Failed to clean blob disk {:?}: {}", shard_dir, e);
            }
        }
    }

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
