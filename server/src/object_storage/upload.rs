use crate::database::database_constants::DATABASE;
use crate::enums::app_error_enum::AppError;
use crate::object_storage::caching::invalidate_cache;
use crate::object_storage::constants::{PARITY_SUFFIX, PART_META_SUFFIX, SHARD_SUFFIX};
use crate::object_storage::ec::{AlignedBuffer, ErasureStreamWriter};
use crate::object_storage::quota_manager::{init_quota_file, try_reserve_quota, update_quota};
use crate::object_storage::storage_capacity_parser::calculate_storage_usage;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{
    CapacityUsageResponse, FinalizeIncompleteResponse, InitiateUploadParams,
    InitiateUploadResponse, ObjectKeyQuery, ObjectMetadata, ObjectPathManager, PartMetadata,
    ReservationReceipt, ShardInfo, UploadPartQuery, UploadStatusResponse, UserRecord,
    default_stripe_size,
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
use std::collections::{BTreeSet, HashMap};
use std::fs::{self, OpenOptions};
use std::future::Future;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;
#[cfg(not(unix))]
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use tokio::fs as tokio_fs;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::Semaphore;
use tracing::{error, info, warn};
use ulid::Ulid;

pub static MAX_PART_SIZE_ATOMIC: OnceCell<AtomicU64> = OnceCell::new();
pub static DATA_SHARDS_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static PARITY_SHARDS_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static WRITER_BUFFER_SIZE_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static STRIPE_SIZE_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static MAX_IO_SEMAPHORE: OnceCell<usize> = OnceCell::new();

static IO_SEMAPHORE: Lazy<Semaphore> =
    Lazy::new(|| Semaphore::new(*MAX_IO_SEMAPHORE.get().unwrap()));

pub struct DirectWriter {
    file: Arc<fs::File>,
    write_task: Option<tokio::task::JoinHandle<Result<usize, std::io::Error>>>,
}

impl DirectWriter {
    pub fn new(file: fs::File) -> Self {
        Self {
            file: Arc::new(file),
            write_task: None,
        }
    }
}

impl AsyncWrite for DirectWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        if let Some(task) = &mut self.write_task {
            match Pin::new(task).poll(cx) {
                Poll::Ready(Ok(res)) => {
                    self.write_task = None;
                    match res {
                        Ok(_) => {}
                        Err(e) => return Poll::Ready(Err(e)),
                    }
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, e)));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        let file = self.file.clone();

        let mut aligned = AlignedBuffer::new(buf.len());
        aligned.extend_from_slice(buf);

        let task = tokio::task::spawn_blocking(move || {
            use std::io::Write;
            let mut f = &*file;
            f.write_all(aligned.as_slice())?;
            Ok(aligned.len())
        });

        self.write_task = Some(task);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if let Some(task) = &mut self.write_task {
            match Pin::new(task).poll(cx) {
                Poll::Ready(Ok(res)) => {
                    self.write_task = None;
                    match res {
                        Ok(_) => Poll::Ready(Ok(())),
                        Err(e) => Poll::Ready(Err(e)),
                    }
                }
                Poll::Ready(Err(e)) => {
                    Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, e)))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.poll_flush(cx)
    }
}

pub async fn create_direct_file(path: &PathBuf) -> Result<fs::File, AppError> {
    let path_clone = path.clone();

    if let Some(parent) = path.parent() {
        let _ = tokio_fs::create_dir_all(parent).await;
    }

    tokio::task::spawn_blocking(move || {
        let mut options = OpenOptions::new();
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
            .open(&path_clone)
            .map_err(|e| AppError::Io(e, format!("create {}", path_clone.display())))
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("Spawn blocking failed: {}", e)))?
}

#[cfg(unix)]
pub async fn sync_dir(path: &std::path::Path) -> Result<(), AppError> {
    let dir = tokio_fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY)
        .open(path)
        .await
        .map_err(|e| AppError::Io(e, format!("open dir for sync: {:?}", path)))?;

    dir.sync_all()
        .await
        .map_err(|e| AppError::Io(e, format!("sync dir: {:?}", path)))?;
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

    if params.expected_parts == 0 {
        return Err(AppError::BadRequest("expected_parts must be >= 1".into()));
    }

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
        let mut attempts = 0;
        loop {
            match tokio_fs::create_dir_all(path).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    attempts += 1;
                    if attempts >= 3 {
                        warn!(
                            "Failed to create session dir {:?} after 3 attempts: {}",
                            path, e
                        );
                        return Err(e);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        }
    });

    let results = join_all(create_futures).await;
    let success_count = results.iter().filter(|r| r.is_ok()).count();

    if success_count == 0 {
        error!("CRITICAL: Failed to create session directory on ANY index disk.");
        let _ = update_quota(&uid, -(params.total_size_bytes as i64)).await;
        return Err(AppError::InternalServerError(
            "Storage backend unavailable".into(),
        ));
    }

    let receipt = ReservationReceipt {
        amount: params.total_size_bytes,
    };

    if let Ok(bytes) = mongodb::bson::to_vec(&receipt) {
        let write_futures = meta_dirs.iter().map(|path| {
            let receipt_path = path.join("reservation.bson");
            let data = &bytes;
            async move {
                if tokio_fs::try_exists(path).await.unwrap_or(false) {
                    tokio_fs::write(&receipt_path, data).await
                } else {
                    Ok(())
                }
            }
        });
        let _ = join_all(write_futures).await;
    }

    info!(
        "Initiated upload: {}/{} (v: {})",
        fragment, object_key, version_id
    );

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

    let mut parts = BTreeSet::<u64>::new();
    let mut any_read = false;

    for meta_dir in meta_dirs {
        if !tokio_fs::try_exists(&meta_dir).await.unwrap_or(false) {
            continue;
        }
        let mut entries = match tokio_fs::read_dir(&meta_dir).await {
            Ok(e) => e,
            Err(_) => continue,
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            let name_str = entry.file_name().to_string_lossy().to_string();
            if name_str.ends_with(PART_META_SUFFIX) {
                if let Some(rest) = name_str.strip_prefix("part_") {
                    if let Some(index_str) = rest.strip_suffix(PART_META_SUFFIX) {
                        if let Ok(idx) = index_str.parse::<u64>() {
                            parts.insert(idx);
                            any_read = true;
                        }
                    }
                }
            }
        }
    }

    if !any_read {
        return Ok(AxumJson(UploadStatusResponse {
            uploaded_parts: vec![],
        }));
    }

    Ok(AxumJson(UploadStatusResponse {
        uploaded_parts: parts.into_iter().collect(),
    }))
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

    let mut writers: Vec<Option<DirectWriter>> = Vec::with_capacity(total_shards);
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
        let path = shard_dir.join(&file_name);

        let mut attempts = 0;
        loop {
            match tokio_fs::create_dir_all(&shard_dir).await {
                Ok(_) => break,
                Err(e) => {
                    attempts += 1;
                    if attempts >= 3 {
                        warn!(
                            "Failed to create shard dir {:?} after 3 attempts: {}",
                            shard_dir, e
                        );
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        }

        match create_direct_file(&path).await {
            Ok(file) => {
                let file_clone = file
                    .try_clone()
                    .map_err(|e| AppError::Io(e, "clone file handle".into()))?;
                let prealloc_result =
                    tokio::task::spawn_blocking(move || file_clone.set_len(final_shard_len)).await;

                if let Ok(Ok(_)) = prealloc_result {
                    writers.push(Some(DirectWriter::new(file)));
                    successful_opens += 1;
                } else {
                    writers.push(None);
                }
            }
            Err(_) => {
                writers.push(None);
            }
        }

        shard_paths.push(path.to_string_lossy().to_string());
        file_names.push(file_name);
    }

    if successful_opens < data_shards {
        return Err(AppError::InternalServerError(format!(
            "Insufficient healthy disks for write quorum. Need {}, got {}.",
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
            let mut attempts = 0;
            let dir_ready = loop {
                match tokio_fs::create_dir_all(&dir).await {
                    Ok(_) => break true,
                    Err(e) => {
                        attempts += 1;
                        if attempts >= 3 {
                            warn!("Failed to create part meta dir {:?}: {}", dir, e);
                            break false;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                }
            };

            if !dir_ready {
                return false;
            }

            let meta_path = dir.join(format!("part_{}{}", part_index, PART_META_SUFFIX));
            match tokio_fs::write(&meta_path, &*bytes).await {
                Ok(_) => {
                    let _ = sync_dir(&dir).await;
                    true
                }
                Err(e) => {
                    warn!("Failed to write part meta {:?}: {}", meta_path, e);
                    false
                }
            }
        }
    });

    let results = join_all(write_futures).await;

    if !results.into_iter().any(|ok| ok) {
        return Err(AppError::InternalServerError(
            "Failed to write part metadata to any index mirror".into(),
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

    if params.expected_parts == 0 {
        return Err(AppError::BadRequest("expected_parts must be >= 1".into()));
    }

    let path_manager = ObjectPathManager::new();

    let index_paths =
        path_manager.get_all_metadata_paths(&uid, &fragment, &object_key, &version_id);
    for p in &index_paths {
        if tokio_fs::try_exists(p).await.unwrap_or(false) {
            return Ok((StatusCode::OK, "Upload finalized.").into_response());
        }
    }

    let meta_dirs = Arc::new(path_manager.get_all_session_dirs(&uid, &fragment, &version_id));

    let mut all_part_paths = Vec::new();
    for meta_dir in meta_dirs.iter() {
        if !tokio_fs::try_exists(meta_dir).await.unwrap_or(false) {
            continue;
        }

        match tokio_fs::read_dir(meta_dir).await {
            Ok(mut entries) => {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let path = entry.path();
                    if path.is_file() {
                        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                            if name.ends_with(PART_META_SUFFIX) {
                                all_part_paths.push(path);
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Failed to list session dir {:?}: {}", meta_dir, e);
            }
        }
    }

    if all_part_paths.is_empty() {
        return Err(AppError::BadRequest(
            "No parts found to finalize.".to_string(),
        ));
    }

    let blob_roots = path_manager.get_blob_roots();
    let required_data_shards = DATA_SHARDS_ATOMIC.get().unwrap().load(Ordering::Relaxed) as usize;
    let mut found_data_roots = 0;

    for root in &blob_roots {
        let shard_dir_check =
            path_manager.get_shard_dir_on_disk(root, &uid, &fragment, &version_id);

        if tokio_fs::try_exists(&shard_dir_check)
            .await
            .unwrap_or(false)
        {
            found_data_roots += 1;
        }
    }

    if found_data_roots < required_data_shards {
        error!(
            "Finalization Aborted: Data directories missing. Found {}/{}. Likely GC collision.",
            found_data_roots, required_data_shards
        );
        return Err(AppError::InternalServerError(
            "Upload session expired or data missing.".into(),
        ));
    }

    let results: Vec<PartMetadata> = stream::iter(all_part_paths)
        .map(|path| async move {
            let data = tokio_fs::read(&path)
                .await
                .map_err(|e| AppError::Io(e, "read part meta".into()))?;
            let part: PartMetadata = mongodb::bson::from_slice(&data)
                .map_err(|_| AppError::InternalServerError("Corrupt part meta".into()))?;
            Ok::<_, AppError>(part)
        })
        .buffer_unordered(50)
        .collect::<Vec<Result<_, _>>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    let mut parts_map: HashMap<u64, PartMetadata> = HashMap::new();
    for part in results {
        parts_map.entry(part.part_index).or_insert(part);
    }

    let mut parts: Vec<PartMetadata> = parts_map.into_values().collect();
    parts.sort_by_key(|p| p.part_index);

    let expected_parts = params.expected_parts.max(1);

    let mut present = BTreeSet::new();
    for p in &parts {
        present.insert(p.part_index);
    }

    let mut missing = Vec::new();
    for i in 0..expected_parts {
        if !present.contains(&i) {
            missing.push(i);
        }
    }

    if !missing.is_empty() {
        return Ok((
            StatusCode::CONFLICT,
            AxumJson(FinalizeIncompleteResponse {
                uploaded_parts: present.into_iter().collect(),
                missing_parts: missing,
                expected_parts,
            }),
        )
            .into_response());
    }

    let mut total_size_calculated = 0;
    for part in &parts {
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
                if !tokio_fs::try_exists(parent).await.unwrap_or(false) {
                    let _ = tokio_fs::create_dir_all(parent).await;
                }
            }

            match tokio_fs::File::create(&index_path).await {
                Ok(mut final_meta_file) => {
                    if let Err(e) = final_meta_file.write_all(bson_data).await {
                        warn!("Failed to write metadata {:?}: {}", index_path, e);
                    }
                    let _ = final_meta_file.sync_all().await;
                }
                Err(e) => {
                    warn!("Failed to create metadata file {:?}: {}", index_path, e);
                }
            }

            if let Some(parent) = index_path.parent() {
                let _ = sync_dir(parent).await;
            }
            Ok::<(), AppError>(())
        }
    });

    join_all(write_futures).await;

    let cleanup_futures = meta_dirs.iter().map(|dir| async move {
        if !tokio_fs::try_exists(dir).await.unwrap_or(false) {
            return;
        }

        let mut entries = match tokio_fs::read_dir(dir).await {
            Ok(e) => e,
            Err(_) => return,
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let filename = entry.file_name();
            let name_str = filename.to_string_lossy();
            if name_str == "reservation.bson" || name_str.ends_with(PART_META_SUFFIX) {
                let _ = tokio_fs::remove_file(entry.path()).await;
            }
        }
    });
    join_all(cleanup_futures).await;

    let reserved_size = params.total_size_bytes as i64;
    let actual_size = total_size_calculated as i64;
    let delta = actual_size - reserved_size;

    if delta != 0 {
        update_quota(&object_meta.owner_uid, delta).await?;
    }

    invalidate_cache(
        &object_meta.fragment,
        &object_meta.object_key,
        &object_meta.owner_uid,
    )
    .await;

    let owner_uid = object_meta.owner_uid.clone();
    let stripped_meta = ObjectMetadata {
        parts: Vec::new(),
        ..object_meta
    };
    state.emit(&owner_uid, StorageEventPayload::FileUploaded(stripped_meta));

    Ok((StatusCode::OK, "Upload finalized.").into_response())
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
    for index_path in &index_paths {
        if tokio_fs::try_exists(index_path).await.unwrap_or(false) {
            return Err(AppError::Conflict(
                "Cannot cancel: Object is already finalized.".to_string(),
            ));
        }
    }

    let meta_dirs = path_manager.get_all_session_dirs(&uid, &fragment, &version_id);
    let mut refund_amount = 0;

    for meta_dir in &meta_dirs {
        if !tokio_fs::try_exists(meta_dir).await.unwrap_or(false) {
            continue;
        }
        let receipt_path = meta_dir.join("reservation.bson");
        if let Ok(data) = tokio_fs::read(&receipt_path).await {
            if let Ok(receipt) = mongodb::bson::from_slice::<ReservationReceipt>(&data) {
                refund_amount = receipt.amount;
                break;
            }
        }
    }

    let cleanup_meta_futures = meta_dirs.iter().map(|dir| {
        let dir = dir.clone();
        async move {
            let _ = tokio_fs::remove_dir_all(&dir).await;
        }
    });
    join_all(cleanup_meta_futures).await;

    if refund_amount > 0 {
        if let Err(e) = update_quota(&uid, -(refund_amount as i64)).await {
            error!("Failed to refund quota for cancelled upload {}: {}", uid, e);
        }
    }

    Ok(StatusCode::NO_CONTENT)
}

pub async fn get_capacity_usage(claims: Claims) -> Result<impl IntoResponse, AppError> {
    let usage = calculate_storage_usage(&claims.uid).await?;
    Ok(AxumJson(CapacityUsageResponse {
        capacity_used: usage,
    }))
}
