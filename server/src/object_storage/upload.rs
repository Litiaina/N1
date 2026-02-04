use std::collections::{BTreeSet, HashMap};
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll};

use axum::Json as AxumJson;
use axum::body::Body;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;

use chrono::Utc;
use crc32fast::Hasher;
use futures_util::StreamExt;
use futures_util::future::join_all;
use once_cell::sync::{Lazy, OnceCell};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::fs as tokio_fs;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Semaphore;
use tracing::{error, info, warn};
use ulid::Ulid;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::{
    PARITY_SUFFIX, PART_META_SUFFIX, RESERVATION_FILE, SESSION_MANIFEST_FILE, SHARD_SUFFIX,
};
use crate::object_storage::disk_manager::{
    FinalizeIncompleteResponse, InitiateUploadParams, InitiateUploadResponse, ObjectKeyQuery,
    ObjectMetadata, ObjectPathManager, PartMetadata, PrefixIndexEntry, PrefixIndexSegment,
    ReservationReceipt, ShardInfo, UploadPartQuery, UploadStatusResponse, default_stripe_size,
};
use crate::object_storage::ec::{AlignedBuffer, ErasureStreamWriter};
use crate::object_storage::quota_manager::{try_reserve_quota, update_quota};
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::ws_structs::StorageEventPayload;

pub static MAX_PART_SIZE_ATOMIC: OnceCell<AtomicU64> = OnceCell::new();
pub static DATA_SHARDS_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static PARITY_SHARDS_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static WRITER_BUFFER_SIZE_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static STRIPE_SIZE_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static MAX_IO_SEMAPHORE: OnceCell<usize> = OnceCell::new();

static IO_SEMAPHORE: Lazy<Semaphore> =
    Lazy::new(|| Semaphore::new(*MAX_IO_SEMAPHORE.get().unwrap()));

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadSessionManifest {
    pub fragment: String,
    pub object_key: String,
    pub version_id: String,
    pub session_id: String,
    pub total_size_bytes: u64,
    pub expected_parts: u64,
    pub content_type: String,
    pub created_at: i64,
    pub stripe_size_bytes: usize,
}

#[derive(Serialize)]
pub struct ActiveSessionInfo {
    pub session_id: String,
    pub version_id: String,
    pub object_key: Option<String>,
    pub created_at: i64,
    pub reserved_size: u64,
    pub total_size_bytes: Option<u64>,
    pub expected_parts: Option<u64>,
}

#[derive(Serialize)]
pub struct ListUploadsResponse {
    pub sessions: Vec<ActiveSessionInfo>,
    pub total: usize,
}

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
                Poll::Ready(Ok(Ok(n))) => {
                    self.write_task = None;
                    return Poll::Ready(Ok(n));
                }
                Poll::Ready(Ok(Err(e))) => {
                    self.write_task = None;
                    return Poll::Ready(Err(e));
                }
                Poll::Ready(Err(e)) => {
                    self.write_task = None;
                    return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, e)));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        let file = self.file.clone();

        let mut aligned = AlignedBuffer::new(buf.len());
        aligned.extend_from_slice(buf);
        let n = aligned.len();

        let task = tokio::task::spawn_blocking(move || {
            use std::io::Write;
            let mut f = &*file;
            f.write_all(aligned.as_slice())?;
            Ok::<usize, std::io::Error>(n)
        });

        self.write_task = Some(task);
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if let Some(task) = &mut self.write_task {
            match Pin::new(task).poll(cx) {
                Poll::Ready(Ok(Ok(_))) => {
                    self.write_task = None;
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Ok(Err(e))) => {
                    self.write_task = None;
                    Poll::Ready(Err(e))
                }
                Poll::Ready(Err(e)) => {
                    self.write_task = None;
                    Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, e)))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.as_mut().poll_flush(cx)
    }
}

fn prefixes_for_key(object_key: &str) -> BTreeSet<String> {
    let key = ObjectPathManager::canonical_prefix(object_key);
    let mut out = BTreeSet::new();
    out.insert(String::new());

    if key.is_empty() {
        return out;
    }

    let parts: Vec<&str> = key.split('/').filter(|p| !p.is_empty()).collect();
    let mut cur = String::new();

    for (i, p) in parts.iter().enumerate() {
        if i == 0 {
            cur.push_str(p);
        } else {
            cur.push('/');
            cur.push_str(p);
        }
        out.insert(cur.clone());
    }

    out
}

async fn write_prefix_segments(
    path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
    version_id: &str,
    created_at: i64,
) -> Result<(), AppError> {
    let prefixes = prefixes_for_key(object_key);
    let mut any_ok = false;

    for prefix in prefixes {
        let seg = PrefixIndexSegment {
            prefix: prefix.clone(),
            entries: vec![PrefixIndexEntry {
                object_key: object_key.to_string(),
                version_id: version_id.to_string(),
                created_at,
            }],
        };

        let bytes = rmp_serde::to_vec(&seg).map_err(|e| {
            AppError::InternalServerError(format!("Serialize prefix segment: {}", e))
        })?;

        let paths = path_manager.get_all_prefix_segment_paths(fragment, &prefix, version_id);

        let mut wrote_this_prefix = false;
        for p in paths {
            if let Some(parent) = p.parent() {
                let _ = tokio_fs::create_dir_all(parent).await;
            }
            if write_atomic(&p, &bytes).await.is_ok() {
                wrote_this_prefix = true;
            }
        }

        if wrote_this_prefix {
            any_ok = true;
        }
    }

    if !any_ok {
        return Err(AppError::InternalServerError(
            "Failed to write prefix index to any mirror".into(),
        ));
    }

    Ok(())
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
pub async fn sync_dir(path: &std::path::Path) -> Result<(), AppError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x02000000;

        let result = OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(&path);

        match result {
            Ok(file) => {
                let _ = file.sync_all();
            }
            Err(e) => {
                warn!(
                    "Windows: Failed to open directory for sync (non-fatal): {} - {}",
                    path.display(),
                    e
                );
            }
        }

        Ok::<(), AppError>(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("sync dir task: {}", e)))?
}

#[cfg(not(any(unix, windows)))]
pub async fn sync_dir(_path: &std::path::Path) -> Result<(), AppError> {
    Ok(())
}

pub async fn write_atomic(path: &std::path::Path, data: &[u8]) -> Result<(), AppError> {
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError(format!("No parent dir for {}", path.display()))
    })?;

    tokio_fs::create_dir_all(parent)
        .await
        .map_err(|e| AppError::Io(e, "create parent dir".into()))?;

    let tmp = parent.join(format!(
        ".{}.tmp.{}",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("file"),
        Ulid::new().to_string()
    ));

    let mut f = tokio_fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .await
        .map_err(|e| AppError::Io(e, format!("open tmp {}", tmp.display())))?;

    f.write_all(data)
        .await
        .map_err(|e| AppError::Io(e, "write tmp".into()))?;

    f.sync_all()
        .await
        .map_err(|e| AppError::Io(e, "sync tmp".into()))?;
    drop(f);

    tokio_fs::rename(&tmp, path)
        .await
        .map_err(|e| AppError::Io(e, format!("rename {} -> {}", tmp.display(), path.display())))?;

    sync_dir(parent).await?;
    Ok(())
}

async fn read_manifest_any(
    path_manager: &ObjectPathManager,
    fragment: &str,
    version_id: &str,
) -> Result<Option<UploadSessionManifest>, AppError> {
    let meta_dirs = path_manager.get_all_session_dirs(fragment, version_id);
    for d in meta_dirs {
        let p = d.join(SESSION_MANIFEST_FILE);
        if !tokio_fs::try_exists(&p).await.unwrap_or(false) {
            continue;
        }
        match tokio_fs::read(&p).await {
            Ok(bytes) => {
                if let Ok(m) = rmp_serde::from_slice::<UploadSessionManifest>(&bytes) {
                    return Ok(Some(m));
                }
            }
            Err(_) => {}
        }
    }
    Ok(None)
}

async fn read_part_meta_any(
    path_manager: &ObjectPathManager,
    fragment: &str,
    version_id: &str,
    part_index: u64,
) -> Option<PartMetadata> {
    let meta_dirs = path_manager.get_all_session_dirs(fragment, version_id);
    let name = format!("part_{}{}", part_index, PART_META_SUFFIX);

    for d in meta_dirs {
        let p = d.join(&name);
        if !tokio_fs::try_exists(&p).await.unwrap_or(false) {
            continue;
        }
        if let Ok(bytes) = tokio_fs::read(&p).await {
            if let Ok(pm) = rmp_serde::from_slice::<PartMetadata>(&bytes) {
                return Some(pm);
            }
        }
    }
    None
}

async fn cleanup_files_best_effort(paths: &[PathBuf]) {
    let futs = paths.iter().cloned().map(|p| async move {
        let _ = tokio_fs::remove_file(&p).await;
    });
    let _ = join_all(futs).await;
}

async fn commit_part_meta_lockless(
    path_manager: &ObjectPathManager,
    fragment: &str,
    version_id: &str,
    part_index: u64,
    meta_bytes: &[u8],
) -> Result<bool, AppError> {
    let meta_dirs = path_manager.get_all_session_dirs(fragment, version_id);
    let file_name = format!("part_{}{}", part_index, PART_META_SUFFIX);

    for d in &meta_dirs {
        let p = d.join(&file_name);
        if tokio_fs::try_exists(&p).await.unwrap_or(false) {
            if let Ok(bytes) = tokio_fs::read(&p).await {
                if rmp_serde::from_slice::<PartMetadata>(&bytes).is_ok() {
                    return Ok(false);
                }
            }
        }
    }

    for d in meta_dirs {
        if !tokio_fs::try_exists(&d).await.unwrap_or(false) {
            continue;
        }

        let p = d.join(&file_name);

        if tokio_fs::try_exists(&p).await.unwrap_or(false) {
            let mut valid = false;
            if let Ok(bytes) = tokio_fs::read(&p).await {
                valid = rmp_serde::from_slice::<PartMetadata>(&bytes).is_ok();
            }
            if valid {
                return Ok(false);
            } else {
                let _ = tokio_fs::remove_file(&p).await;
            }
        }

        let open = tokio_fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&p)
            .await;

        match open {
            Ok(mut f) => {
                if let Err(e) = f.write_all(meta_bytes).await {
                    warn!("Failed to write part meta {:?}: {}", p, e);
                    let _ = tokio_fs::remove_file(&p).await;
                    continue;
                }
                let _ = f.sync_all().await;
                drop(f);
                let _ = sync_dir(&d).await;
                return Ok(true);
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if let Ok(bytes) = tokio_fs::read(&p).await {
                    if rmp_serde::from_slice::<PartMetadata>(&bytes).is_ok() {
                        return Ok(false);
                    }
                }
                let _ = tokio_fs::remove_file(&p).await;
                continue;
            }
            Err(e) => {
                warn!("Failed to create part meta {:?}: {}", p, e);
                continue;
            }
        }
    }

    Err(AppError::InternalServerError(
        "Failed to commit part metadata to any index mirror".into(),
    ))
}

async fn fsync_file_path(path: &Path) -> Result<(), AppError> {
    let f = tokio_fs::File::open(path)
        .await
        .map_err(|e| AppError::Io(e, format!("open for fsync {}", path.display())))?;
    f.sync_all()
        .await
        .map_err(|e| AppError::Io(e, format!("fsync {}", path.display())))?;
    Ok(())
}

async fn crc32_file_hex(path: &Path) -> Result<String, AppError> {
    let mut f = tokio_fs::File::open(path)
        .await
        .map_err(|e| AppError::Io(e, format!("open for crc32 {}", path.display())))?;

    let mut hasher = Hasher::new();
    let mut buf = vec![0u8; 256 * 1024];

    loop {
        let n = f
            .read(&mut buf)
            .await
            .map_err(|e| AppError::Io(e, format!("read crc32 {}", path.display())))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    Ok(format!("{:08x}", hasher.finalize()))
}

async fn publish_one_shard(
    tmp_path: &Path,
    stable_path: &Path,
    expected_len: u64,
    expected_crc32: Option<&str>,
) -> Result<(), AppError> {
    if tokio_fs::try_exists(stable_path).await.unwrap_or(false) {
        let len_ok = tokio_fs::metadata(stable_path)
            .await
            .map(|m| m.len() == expected_len)
            .unwrap_or(false);

        if len_ok {
            if let Some(exp) = expected_crc32.filter(|s| !s.is_empty()) {
                let got = crc32_file_hex(stable_path).await?;
                if got.eq_ignore_ascii_case(exp) {
                    let _ = tokio_fs::remove_file(tmp_path).await;
                    return Ok(());
                }
            } else {
                let _ = tokio_fs::remove_file(tmp_path).await;
                return Ok(());
            }
        }

        let _ = tokio_fs::remove_file(stable_path).await;
    }

    tokio_fs::rename(tmp_path, stable_path).await.map_err(|e| {
        AppError::Io(
            e,
            format!(
                "rename tmp->stable {} -> {}",
                tmp_path.display(),
                stable_path.display()
            ),
        )
    })?;

    Ok(())
}

fn is_tmp_attempt_shard(name: &str) -> bool {
    if !name.starts_with("part_") || !name.contains("_shard_") || !name.contains(".tmp.") {
        return false;
    }

    name.contains(&format!("{}.tmp.", SHARD_SUFFIX))
        || name.contains(&format!("{}.tmp.", PARITY_SUFFIX))
}

async fn cleanup_blob_tmp_attempt_files_for_version(
    path_manager: &ObjectPathManager,
    fragment: &str,
    version_id: &str,
) {
    let blob_roots = path_manager.get_blob_roots();

    let futs = blob_roots.into_iter().map(|root| {
        let dir = path_manager.get_shard_dir_on_disk(&root, fragment, version_id);
        async move {
            if !tokio_fs::try_exists(&dir).await.unwrap_or(false) {
                return;
            }

            let mut entries = match tokio_fs::read_dir(&dir).await {
                Ok(e) => e,
                Err(_) => return,
            };

            while let Ok(Some(entry)) = entries.next_entry().await {
                let p = entry.path();
                if !p.is_file() {
                    continue;
                }

                let name = entry.file_name().to_string_lossy().to_string();
                if is_tmp_attempt_shard(&name) {
                    let _ = tokio_fs::remove_file(&p).await;
                }
            }

            let _ = sync_dir(&dir).await;
        }
    });

    let _ = join_all(futs).await;
}

pub async fn initiate_object_upload(
    claims: Claims,
    AxumPath((fragment, object_key)): AxumPath<(String, String)>,
    AxumJson(params): AxumJson<InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    if params.expected_parts == 0 {
        return Err(AppError::BadRequest("expected_parts must be >= 1".into()));
    }

    if let Err(e) = try_reserve_quota(&fragment, params.total_size_bytes).await {
        if let AppError::Unauthorized = e {
            return Err(AppError::Forbidden(
                "Fragment not initialized. Quota file missing.".into(),
            ));
        } else {
            return Err(e);
        }
    }

    let version_id = Ulid::new().to_string();
    let session_id = Ulid::new().to_string();

    let path_manager = ObjectPathManager::new();
    let meta_dirs = path_manager.get_all_session_dirs(&fragment, &version_id);

    let create_futures = meta_dirs.iter().map(|path| async move {
        let mut attempts = 0;
        loop {
            match tokio_fs::create_dir_all(path).await {
                Ok(_) => return Ok::<(), std::io::Error>(()),
                Err(e) => {
                    attempts += 1;
                    if attempts >= 3 {
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
        error!("Failed to create session directory on any index disk.");
        let _ = update_quota(&fragment, -(params.total_size_bytes as i64)).await;
        return Err(AppError::InternalServerError(
            "Storage backend unavailable".into(),
        ));
    }

    let receipt = ReservationReceipt {
        amount: params.total_size_bytes,
    };
    if let Ok(bytes) = rmp_serde::to_vec(&receipt) {
        let write_futures = meta_dirs.iter().map(|path| {
            let receipt_path = path.join(RESERVATION_FILE);
            let data = bytes.clone();
            async move {
                if tokio_fs::try_exists(path).await.unwrap_or(false) {
                    let _ = write_atomic(&receipt_path, &data).await;
                }
            }
        });
        let _ = join_all(write_futures).await;
    }

    let stripe_size = STRIPE_SIZE_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or_else(default_stripe_size);

    let manifest = UploadSessionManifest {
        fragment: fragment.clone(),
        object_key: object_key.clone(),
        version_id: version_id.clone(),
        session_id: session_id.clone(),
        total_size_bytes: params.total_size_bytes,
        expected_parts: params.expected_parts.max(1),
        content_type: params
            .content_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".to_string()),
        created_at: Utc::now().timestamp(),
        stripe_size_bytes: stripe_size,
    };

    if let Ok(bytes) = rmp_serde::to_vec(&manifest) {
        let write_futures = meta_dirs.iter().map(|path| {
            let p = path.join(SESSION_MANIFEST_FILE);
            let data = bytes.clone();
            async move {
                if tokio_fs::try_exists(path).await.unwrap_or(false) {
                    let _ = write_atomic(&p, &data).await;
                }
            }
        });
        let _ = join_all(write_futures).await;
    }

    info!(
        "Initiated upload: {}/{} (v: {}, s: {})",
        fragment, object_key, version_id, session_id
    );

    Ok((
        StatusCode::CREATED,
        AxumJson(InitiateUploadResponse {
            session_id,
            version_id,
        }),
    ))
}

pub async fn list_active_uploads(
    claims: Claims,
    AxumPath(fragment): AxumPath<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let limit: usize = params
        .get("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(50)
        .min(100);
    let offset: usize = params
        .get("offset")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();

    let mut session_root = None;
    for root in index_roots {
        let p = root.join(&fragment).join("index").join("sessions");
        if tokio_fs::try_exists(&p).await.unwrap_or(false) {
            session_root = Some(p);
            break;
        }
    }

    let root = match session_root {
        Some(r) => r,
        None => {
            return Ok(AxumJson(ListUploadsResponse {
                sessions: vec![],
                total: 0,
            }));
        }
    };

    let mut sessions = Vec::new();
    let mut entries = tokio_fs::read_dir(root)
        .await
        .map_err(|e| AppError::Io(e, "read sessions".into()))?;

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let version_id = entry.file_name().to_string_lossy().to_string();

        let mut created_at = 0i64;
        if let Ok(meta) = entry.metadata().await {
            if let Ok(created) = meta.created() {
                created_at = created
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
            } else if let Ok(modified) = meta.modified() {
                created_at = modified
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
            }
        }

        let mut reserved_size = 0u64;
        let res_path = path.join(RESERVATION_FILE);
        if let Ok(data) = tokio_fs::read(&res_path).await {
            if let Ok(receipt) = rmp_serde::from_slice::<ReservationReceipt>(&data) {
                reserved_size = receipt.amount;
            }
        }

        let mut session_id = version_id.clone();
        let mut object_key: Option<String> = None;
        let mut total_size_bytes: Option<u64> = None;
        let mut expected_parts: Option<u64> = None;

        let man_path = path.join(SESSION_MANIFEST_FILE);
        if let Ok(data) = tokio_fs::read(&man_path).await {
            if let Ok(m) = rmp_serde::from_slice::<UploadSessionManifest>(&data) {
                session_id = m.session_id;
                object_key = Some(m.object_key);
                total_size_bytes = Some(m.total_size_bytes);
                expected_parts = Some(m.expected_parts);
                if m.created_at > 0 {
                    created_at = m.created_at;
                }
            }
        }

        sessions.push(ActiveSessionInfo {
            session_id,
            version_id,
            object_key,
            created_at,
            reserved_size,
            total_size_bytes,
            expected_parts,
        });
    }

    sessions.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    let total = sessions.len();
    let paged_sessions = sessions.into_iter().skip(offset).take(limit).collect();

    Ok(AxumJson(ListUploadsResponse {
        sessions: paged_sessions,
        total,
    }))
}

pub async fn get_upload_status(
    claims: Claims,
    AxumPath((fragment, version_id)): AxumPath<(String, String)>,
    Query(_key_query): Query<ObjectKeyQuery>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let path_manager = ObjectPathManager::new();
    let meta_dirs = path_manager.get_all_session_dirs(&fragment, &version_id);

    let mut parts = BTreeSet::<u64>::new();
    let mut any_dir = false;

    for meta_dir in meta_dirs {
        if !tokio_fs::try_exists(&meta_dir).await.unwrap_or(false) {
            continue;
        }
        any_dir = true;

        let mut entries = match tokio_fs::read_dir(&meta_dir).await {
            Ok(e) => e,
            Err(_) => continue,
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            let name_str = entry.file_name().to_string_lossy().to_string();
            if !name_str.ends_with(PART_META_SUFFIX) {
                continue;
            }

            let Some(rest) = name_str.strip_prefix("part_") else {
                continue;
            };
            let Some(index_str) = rest.strip_suffix(PART_META_SUFFIX) else {
                continue;
            };
            let Ok(idx) = index_str.parse::<u64>() else {
                continue;
            };

            let p = entry.path();
            if let Ok(data) = tokio_fs::read(&p).await {
                if rmp_serde::from_slice::<PartMetadata>(&data).is_ok() {
                    parts.insert(idx);
                }
            }
        }
    }

    if !any_dir {
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
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

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

    if block_size % 4096 != 0 {
        return Err(AppError::InternalServerError(format!(
            "stripe_size_bytes must be multiple of 4096 for O_DIRECT (got {})",
            block_size
        )));
    }

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

    let path_manager = ObjectPathManager::new();
    if let Some(existing) =
        read_part_meta_any(&path_manager, &fragment, &version_id, part_index).await
    {
        if let Some(expected) = query.checksum.as_ref() {
            if existing
                .checksum
                .as_deref()
                .unwrap_or("")
                .eq_ignore_ascii_case(expected)
                == false
            {
                return Ok((
                    StatusCode::CONFLICT,
                    "Part already uploaded but checksum differs",
                )
                    .into_response());
            }
        }
        return Ok((StatusCode::OK, "Part already uploaded").into_response());
    }

    let attempt_id = Ulid::new().to_string();

    let fragment_arc = Arc::new(fragment);
    let version_id_arc = Arc::new(version_id);
    let path_manager = Arc::new(path_manager);

    let stripe_data_width = block_size as u64 * data_shards as u64;
    let stripe_count = (content_length + stripe_data_width - 1) / stripe_data_width;
    let final_shard_len = stripe_count * block_size as u64;

    let blob_roots = path_manager.get_blob_roots();
    let num_disks = blob_roots.len();
    if num_disks == 0 {
        return Err(AppError::InternalServerError(
            "No blob storage disks configured".into(),
        ));
    }

    let mut writers: Vec<Option<DirectWriter>> = Vec::with_capacity(total_shards);

    let mut tmp_paths: Vec<PathBuf> = Vec::with_capacity(total_shards);
    let mut stable_paths: Vec<PathBuf> = Vec::with_capacity(total_shards);
    let mut stable_names: Vec<String> = Vec::with_capacity(total_shards);

    let mut successful_opens = 0usize;

    for i in 0..total_shards {
        let is_parity = i >= data_shards;
        let ext = if is_parity {
            PARITY_SUFFIX
        } else {
            SHARD_SUFFIX
        };

        let stable_name = format!("part_{}_shard_{}{}", part_index, i, ext);
        let tmp_name = format!("{}.tmp.{}", stable_name, attempt_id);

        let disk_idx = (part_index as usize + i) % num_disks;
        let target_disk = &blob_roots[disk_idx];

        let shard_dir =
            path_manager.get_shard_dir_on_disk(target_disk, &fragment_arc, &version_id_arc);

        let _ = tokio_fs::create_dir_all(&shard_dir).await;

        let tmp_path = shard_dir.join(&tmp_name);
        let stable_path = shard_dir.join(&stable_name);

        match create_direct_file(&tmp_path).await {
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

        tmp_paths.push(tmp_path);
        stable_paths.push(stable_path);
        stable_names.push(stable_name);
    }

    if successful_opens < data_shards {
        cleanup_files_best_effort(&tmp_paths).await;
        return Err(AppError::InternalServerError(format!(
            "Insufficient healthy disks for write quorum. Need {}, got {}.",
            data_shards, successful_opens
        )));
    }

    let calc_checksum = query.enable_checksum.unwrap_or(false);

    let mut stream_writer = ErasureStreamWriter::new(
        writers,
        data_shards,
        parity_shards,
        final_shard_len as usize,
        calc_checksum,
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

        if total_bytes_read > content_length {
            cleanup_files_best_effort(&tmp_paths).await;
            return Err(AppError::BadRequest("Body exceeds Content-Length".into()));
        }

        stream_writer.write(&chunk).await?;
    }

    if total_bytes_read != content_length {
        cleanup_files_best_effort(&tmp_paths).await;
        return Err(AppError::BadRequest(format!(
            "Incomplete body: got {} bytes, expected {}",
            total_bytes_read, content_length
        )));
    }

    stream_writer.finalize().await?;

    if let Some(expected) = query.checksum.as_ref() {
        let calculated = stream_writer.get_total_checksum();
        if let Some(calc_str) = calculated.as_ref() {
            if !expected.eq_ignore_ascii_case(calc_str) {
                cleanup_files_best_effort(&tmp_paths).await;
                return Ok((StatusCode::BAD_REQUEST, "Checksum mismatch").into_response());
            }
        }
    }

    let shard_checksums = stream_writer.get_shard_checksums();
    let total_checksum = stream_writer.get_total_checksum();

    drop(stream_writer);

    let fsync_results = join_all(tmp_paths.iter().map(|p| fsync_file_path(p.as_path()))).await;
    for r in fsync_results {
        if let Err(e) = r {
            cleanup_files_best_effort(&tmp_paths).await;
            return Err(e);
        }
    }

    let mut dirs_to_sync: Vec<PathBuf> = Vec::new();

    for i in 0..total_shards {
        let expected_crc = if calc_checksum {
            shard_checksums.get(i).map(|s| s.as_str())
        } else {
            None
        };

        publish_one_shard(
            tmp_paths[i].as_path(),
            stable_paths[i].as_path(),
            final_shard_len,
            expected_crc,
        )
        .await?;

        if let Some(parent) = stable_paths[i].parent() {
            dirs_to_sync.push(parent.to_path_buf());
        }
    }

    dirs_to_sync.sort();
    dirs_to_sync.dedup();

    let _ = join_all(dirs_to_sync.iter().map(|d| sync_dir(d.as_path()))).await;

    let mut shards_info = Vec::with_capacity(total_shards);
    for i in 0..total_shards {
        shards_info.push(ShardInfo {
            shard_index: i,
            file_name: stable_names[i].clone(),
            absolute_path: stable_paths[i].to_string_lossy().to_string(),
            size_bytes: final_shard_len,
            checksum: shard_checksums.get(i).cloned().unwrap_or_default(),
            is_parity: i >= data_shards,
        });
    }

    let part_meta = PartMetadata {
        part_index,
        size_bytes: total_bytes_read,
        created_at: Utc::now().timestamp(),
        checksum: total_checksum,
        shards: shards_info,
    };

    let meta_bytes = rmp_serde::to_vec(&part_meta)
        .map_err(|e| AppError::InternalServerError(format!("Serialize part meta: {}", e)))?;

    let committed = commit_part_meta_lockless(
        &path_manager,
        &fragment_arc,
        &version_id_arc,
        part_index,
        &meta_bytes,
    )
    .await?;

    if !committed {
        return Ok((StatusCode::OK, "Part already uploaded").into_response());
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
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let object_key = key_query.object_key;
    let path_manager = ObjectPathManager::new();

    let index_paths = path_manager.get_all_metadata_paths(&fragment, &object_key, &version_id);
    for p in &index_paths {
        if tokio_fs::try_exists(p).await.unwrap_or(false) {
            return Ok((StatusCode::OK, "Upload finalized.").into_response());
        }
    }

    let manifest = read_manifest_any(&path_manager, &fragment, &version_id).await?;

    if let Some(m) = manifest.as_ref() {
        if m.session_id != session_id {
            return Err(AppError::Forbidden("Session id mismatch".into()));
        }
        if m.object_key != object_key {
            return Err(AppError::Forbidden("Object key mismatch".into()));
        }
    }

    let expected_parts = manifest
        .as_ref()
        .map(|m| m.expected_parts)
        .unwrap_or_else(|| params.expected_parts.max(1));

    if expected_parts == 0 {
        return Err(AppError::BadRequest("expected_parts must be >= 1".into()));
    }

    let blob_roots = path_manager.get_blob_roots();
    let required_data_shards = DATA_SHARDS_ATOMIC.get().unwrap().load(Ordering::Relaxed) as usize;
    let mut found_data_roots = 0usize;

    for root in &blob_roots {
        let shard_dir_check = path_manager.get_shard_dir_on_disk(root, &fragment, &version_id);
        if tokio_fs::try_exists(&shard_dir_check)
            .await
            .unwrap_or(false)
        {
            found_data_roots += 1;
        }
    }

    if found_data_roots < required_data_shards {
        error!(
            "Finalization aborted: data directories missing. Found {}/{}.",
            found_data_roots, required_data_shards
        );
        return Err(AppError::InternalServerError(
            "Upload session expired or data missing.".into(),
        ));
    }

    let mut present = BTreeSet::new();
    let mut parts: Vec<PartMetadata> = Vec::with_capacity(expected_parts as usize);

    for i in 0..expected_parts {
        if let Some(pm) = read_part_meta_any(&path_manager, &fragment, &version_id, i).await {
            present.insert(i);
            parts.push(pm);
        }
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

    parts.sort_by_key(|p| p.part_index);

    let mut total_size_calculated = 0u64;
    for part in &parts {
        total_size_calculated += part.size_bytes;
    }

    let stripe_size = STRIPE_SIZE_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or_else(default_stripe_size);

    let content_type = manifest
        .as_ref()
        .map(|m| m.content_type.clone())
        .or_else(|| params.content_type.clone())
        .unwrap_or_else(|| "application/octet-stream".to_string());

    let object_meta = ObjectMetadata {
        object_key: object_key.clone(),
        fragment: fragment.clone(),
        version_id: version_id.clone(),
        upload_session_id: session_id.clone(),
        owner_uid: fragment.clone(),
        content_type,
        total_size_bytes: total_size_calculated,
        created_at: Utc::now().timestamp(),
        parts,
        is_delete_marker: false,
        stripe_size_bytes: stripe_size,
    };

    let rmp_data = rmp_serde::to_vec(&object_meta).map_err(|e| {
        AppError::InternalServerError(format!("Failed to serialize object metadata: {}", e))
    })?;

    let index_paths = path_manager.get_all_metadata_paths(
        &object_meta.fragment,
        &object_meta.object_key,
        &object_meta.version_id,
    );

    let write_futures = index_paths.into_iter().map(|index_path| {
        let data = rmp_data.clone();
        async move {
            if let Some(parent) = index_path.parent() {
                let _ = tokio_fs::create_dir_all(parent).await;
            }
            match write_atomic(&index_path, &data).await {
                Ok(_) => true,
                Err(e) => {
                    warn!("Failed to write metadata {:?}: {}", index_path, e);
                    false
                }
            }
        }
    });

    let results = join_all(write_futures).await;
    if !results.into_iter().any(|ok| ok) {
        return Err(AppError::InternalServerError(
            "Failed to write final metadata to any index mirror".into(),
        ));
    }

    write_prefix_segments(
        &path_manager,
        &fragment,
        &object_key,
        &version_id,
        object_meta.created_at,
    )
    .await?;

    let meta_dirs = path_manager.get_all_session_dirs(&fragment, &version_id);
    let cleanup_futures = meta_dirs.iter().map(|dir| async move {
        if !tokio_fs::try_exists(dir).await.unwrap_or(false) {
            return;
        }
        let mut entries = match tokio_fs::read_dir(dir).await {
            Ok(e) => e,
            Err(_) => return,
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name_str = entry.file_name().to_string_lossy().to_string();
            if name_str == RESERVATION_FILE
                || name_str == SESSION_MANIFEST_FILE
                || name_str.ends_with(PART_META_SUFFIX)
            {
                let _ = tokio_fs::remove_file(entry.path()).await;
            }
        }
        let _ = sync_dir(dir).await;
    });
    let _ = join_all(cleanup_futures).await;

    let reserved_size = manifest
        .as_ref()
        .map(|m| m.total_size_bytes as i64)
        .unwrap_or(params.total_size_bytes as i64);

    let actual_size = total_size_calculated as i64;
    let delta = actual_size - reserved_size;

    if delta != 0 {
        update_quota(&fragment, delta).await?;
    }

    let stripped_meta = ObjectMetadata {
        parts: Vec::new(),
        ..object_meta.clone()
    };
    state.emit(&fragment, StorageEventPayload::FileUploaded(stripped_meta));

    Ok((StatusCode::OK, AxumJson(json!(object_meta))).into_response())
}

pub async fn cancel_upload_session(
    claims: Claims,
    AxumPath((fragment, version_id)): AxumPath<(String, String)>,
    Query(key_query): Query<ObjectKeyQuery>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let path_manager = ObjectPathManager::new();

    let index_paths =
        path_manager.get_all_metadata_paths(&fragment, &key_query.object_key, &version_id);
    for index_path in &index_paths {
        if tokio_fs::try_exists(index_path).await.unwrap_or(false) {
            return Err(AppError::Conflict(
                "Cannot cancel: Object is already finalized.".to_string(),
            ));
        }
    }

    let meta_dirs = path_manager.get_all_session_dirs(&fragment, &version_id);

    let mut refund_amount = 0u64;
    for meta_dir in &meta_dirs {
        if !tokio_fs::try_exists(meta_dir).await.unwrap_or(false) {
            continue;
        }
        let receipt_path = meta_dir.join(RESERVATION_FILE);
        if let Ok(data) = tokio_fs::read(&receipt_path).await {
            if let Ok(receipt) = rmp_serde::from_slice::<ReservationReceipt>(&data) {
                refund_amount = receipt.amount;
                break;
            }
        }
    }

    cleanup_blob_tmp_attempt_files_for_version(&path_manager, &fragment, &version_id).await;

    let cleanup_meta_futures = meta_dirs.iter().map(|dir| {
        let dir = dir.clone();
        async move {
            let _ = tokio_fs::remove_dir_all(&dir).await;
        }
    });
    let _ = join_all(cleanup_meta_futures).await;

    if refund_amount > 0 {
        if let Err(e) = update_quota(&fragment, -(refund_amount as i64)).await {
            error!(
                "Failed to refund quota for cancelled upload {}: {}",
                fragment, e
            );
        }
    }

    Ok(StatusCode::NO_CONTENT)
}
