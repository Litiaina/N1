use std::collections::{BTreeSet, HashMap};
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
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

use crate::n1_core::constants::{
    PREFIX_INDEX_DIR, PREFIX_INDEX_NAMESPACE, PREFIX_INDEX_VERSION, PREFIX_SEGMENT_EXT,
};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

use crate::enums::app_error_enum::AppError;
use crate::n1_core::constants::{PARITY_SUFFIX, SHARD_SUFFIX};
use crate::n1_core::disk_manager::{
    FinalizeIncompleteResponse, InitiateUploadParams, InitiateUploadResponse, ObjectKeyQuery,
    ObjectMetadata, ObjectPathManager, PartMetadata, PrefixIndexEntry, PrefixIndexSegment,
    ReservationReceipt, ShardInfo, UploadPartQuery, UploadStatusResponse, default_stripe_size,
};
use crate::n1_core::ec::{AlignedBuffer, ErasureStreamWriter};
use crate::n1_core::index;
use crate::n1_core::metadata_parser::get_metadata;
use crate::n1_core::prefix_compactor::schedule_prefix_compaction;
use crate::n1_core::prefix_delimiter::write_delimiter_delta;
use crate::n1_core::quota_manager::{try_reserve_quota, update_quota};
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::ws_structs::StorageEventPayload;

pub static MAX_PART_SIZE_ATOMIC: OnceCell<AtomicU64> = OnceCell::new();
pub static DATA_SHARDS_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static PARITY_SHARDS_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static WRITER_BUFFER_SIZE_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static STRIPE_SIZE_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static MAX_IO_SEMAPHORE: OnceCell<usize> = OnceCell::new();
pub static ADAPTIVE_IO_TARGET_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static UPLOAD_STRICT_DURABILITY_DEFAULT_ATOMIC: OnceCell<AtomicBool> = OnceCell::new();
pub static UPLOAD_IO_GATE_ENABLED_ATOMIC: OnceCell<AtomicBool> = OnceCell::new();

static IO_SEMAPHORE: Lazy<Semaphore> =
    Lazy::new(|| Semaphore::new(*MAX_IO_SEMAPHORE.get().unwrap()));
static ACTIVE_IO_INFLIGHT: Lazy<AtomicUsize> = Lazy::new(|| AtomicUsize::new(0));

struct AdaptiveIoGuard<'a> {
    _permit: tokio::sync::SemaphorePermit<'a>,
}

impl Drop for AdaptiveIoGuard<'_> {
    fn drop(&mut self) {
        ACTIVE_IO_INFLIGHT.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn acquire_adaptive_io_guard() -> Result<AdaptiveIoGuard<'static>, AppError> {
    'outer: loop {
        let permit = match IO_SEMAPHORE.acquire().await {
            Ok(permit) => permit,
            Err(_) => return Err(AppError::InternalServerError("Server busy".into())),
        };

        loop {
            let target = ADAPTIVE_IO_TARGET_ATOMIC
                .get()
                .map(|v| v.load(Ordering::Relaxed))
                .unwrap_or_else(|| *MAX_IO_SEMAPHORE.get().unwrap_or(&128usize))
                .max(1);

            let active = ACTIVE_IO_INFLIGHT.load(Ordering::Relaxed);
            if active >= target {
                drop(permit);
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                continue 'outer;
            }

            if ACTIVE_IO_INFLIGHT
                .compare_exchange(active, active + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(AdaptiveIoGuard { _permit: permit });
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UploadDurabilityMode {
    Strict,
    Batched,
}

impl UploadDurabilityMode {
    fn strict(self) -> bool {
        matches!(self, Self::Strict)
    }
}

fn parse_upload_durability_mode(query: &UploadPartQuery) -> UploadDurabilityMode {
    if let Some(v) = query.durability.as_deref() {
        match v.trim().to_ascii_lowercase().as_str() {
            "strict" | "sync" | "durable" => return UploadDurabilityMode::Strict,
            "batched" | "async" | "fast" | "relaxed" => return UploadDurabilityMode::Batched,
            _ => {}
        }
    }
    if let Some(strict) = query.strict_durability {
        if strict {
            UploadDurabilityMode::Strict
        } else {
            UploadDurabilityMode::Batched
        }
    } else if UPLOAD_STRICT_DURABILITY_DEFAULT_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(false)
    {
        UploadDurabilityMode::Strict
    } else {
        UploadDurabilityMode::Batched
    }
}

#[cfg(test)]
mod tests {
    use super::{UploadDurabilityMode, parse_upload_durability_mode};
    use crate::n1_core::disk_manager::UploadPartQuery;

    #[test]
    fn upload_durability_defaults_to_batched() {
        let q = UploadPartQuery {
            checksum: None,
            durability: None,
            strict_durability: None,
            io_gate: None,
        };
        assert!(matches!(
            parse_upload_durability_mode(&q),
            UploadDurabilityMode::Batched
        ));
    }

    #[test]
    fn upload_durability_accepts_string_and_bool_override() {
        let q_batched = UploadPartQuery {
            checksum: None,
            durability: Some("batched".to_string()),
            strict_durability: None,
            io_gate: None,
        };
        assert!(matches!(
            parse_upload_durability_mode(&q_batched),
            UploadDurabilityMode::Batched
        ));

        let q_strict = UploadPartQuery {
            checksum: None,
            durability: None,
            strict_durability: Some(true),
            io_gate: None,
        };
        assert!(matches!(
            parse_upload_durability_mode(&q_strict),
            UploadDurabilityMode::Strict
        ));
    }
}

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
    write_buffer: AlignedBuffer,
    write_task: Option<tokio::task::JoinHandle<Result<AlignedBuffer, std::io::Error>>>,
}

impl DirectWriter {
    pub fn new(file: fs::File) -> Self {
        let configured = WRITER_BUFFER_SIZE_ATOMIC
            .get()
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(256 * 1024);
        let aligned = ((configured.max(4096) + 4095) / 4096) * 4096;
        Self {
            file: Arc::new(file),
            write_buffer: AlignedBuffer::new(aligned),
            write_task: None,
        }
    }

    fn poll_write_task(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        if let Some(task) = &mut self.write_task {
            match Pin::new(task).poll(cx) {
                Poll::Ready(Ok(Ok(returned_buffer))) => {
                    self.write_buffer = returned_buffer;
                    self.write_task = None;
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
        Poll::Ready(Ok(()))
    }

    fn spawn_buffer_flush(&mut self) {
        if self.write_buffer.len() == 0 || self.write_task.is_some() {
            return;
        }

        let file = self.file.clone();
        let mut to_write = std::mem::replace(&mut self.write_buffer, AlignedBuffer::new(0));

        let task = tokio::task::spawn_blocking(move || {
            use std::io::Write;
            let mut f = &*file;
            f.write_all(to_write.as_slice())?;
            to_write.clear();
            Ok::<AlignedBuffer, std::io::Error>(to_write)
        });

        self.write_task = Some(task);
    }

    pub async fn sync_data(&self) -> Result<(), std::io::Error> {
        let file = self.file.clone();
        tokio::task::spawn_blocking(move || file.sync_all()).await?
    }
}

impl AsyncWrite for DirectWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        if matches!(self.poll_write_task(cx), Poll::Pending) {
            return Poll::Pending;
        }

        if self.write_buffer.len() == self.write_buffer.capacity() {
            self.spawn_buffer_flush();
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        let remaining = self.write_buffer.capacity() - self.write_buffer.len();
        let to_copy = std::cmp::min(remaining, buf.len());
        self.write_buffer.extend_from_slice(&buf[..to_copy]);

        if self.write_buffer.len() == self.write_buffer.capacity() {
            self.spawn_buffer_flush();
        }

        Poll::Ready(Ok(to_copy))
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if self.write_buffer.len() > 0 && self.write_task.is_none() {
            self.spawn_buffer_flush();
        }

        if matches!(self.poll_write_task(cx), Poll::Ready(Ok(()))) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
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
    fragment: &str,
    version_id: &str,
) -> Result<Option<UploadSessionManifest>, AppError> {
    if let Some(state) = index::load_session(fragment, version_id).await? {
        return Ok(Some(state.manifest));
    }
    Ok(None)
}

async fn read_part_meta_any(
    fragment: &str,
    version_id: &str,
    part_index: u64,
) -> Option<PartMetadata> {
    if let Ok(Some(part)) = index::read_session_part(fragment, version_id, part_index).await {
        return Some(part);
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
    _path_manager: &ObjectPathManager,
    fragment: &str,
    version_id: &str,
    part_index: u64,
    meta_bytes: &[u8],
    strict_durability: bool,
) -> Result<bool, AppError> {
    if index::read_session_part(fragment, version_id, part_index)
        .await?
        .is_some()
    {
        return Ok(false);
    }

    let part_meta: PartMetadata = rmp_serde::from_slice(meta_bytes).map_err(|e| {
        AppError::InternalServerError(format!("decode part metadata for index commit: {}", e))
    })?;

    let replica_mode = if strict_durability {
        index::IndexReplicaWriteMode::SyncBestEffort
    } else {
        index::IndexReplicaWriteMode::BackgroundBestEffort
    };

    index::commit_session_part(fragment, version_id, &part_meta, replica_mode).await?;
    Ok(true)
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

    let receipt = ReservationReceipt {
        amount: params.total_size_bytes,
    };

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

    if let Err(e) = index::open_session(&manifest, Some(receipt)).await {
        let _ = update_quota(&fragment, -(params.total_size_bytes as i64)).await;
        return Err(AppError::InternalServerError(format!(
            "Failed to initialize partitioned session index: {}",
            e
        )));
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

    let mut sessions: Vec<ActiveSessionInfo> = index::list_active_sessions(&fragment)
        .await?
        .into_iter()
        .filter(|state| state.finalized_at.is_none() && state.aborted_at.is_none())
        .map(|state| ActiveSessionInfo {
            session_id: state.manifest.session_id.clone(),
            version_id: state.manifest.version_id.clone(),
            object_key: Some(state.manifest.object_key.clone()),
            created_at: state.manifest.created_at,
            reserved_size: state.reservation.map(|receipt| receipt.amount).unwrap_or(0),
            total_size_bytes: Some(state.manifest.total_size_bytes),
            expected_parts: Some(state.manifest.expected_parts),
        })
        .collect();

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
) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    if let Some(state) = index::load_session(&fragment, &version_id).await? {
        let mut parts = state.parts.keys().copied().collect::<Vec<_>>();
        parts.sort_unstable();
        return Ok(AxumJson(UploadStatusResponse {
            uploaded_parts: parts,
        }));
    }

    Ok(AxumJson(UploadStatusResponse {
        uploaded_parts: vec![],
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

    let io_gate_enabled = query.io_gate.unwrap_or_else(|| {
        UPLOAD_IO_GATE_ENABLED_ATOMIC
            .get()
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(true)
    });
    let _io_guard = if io_gate_enabled {
        match tokio::time::timeout(
            std::time::Duration::from_secs(2),
            acquire_adaptive_io_guard(),
        )
        .await
        {
            Ok(Ok(guard)) => Some(guard),
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                warn!(
                    "Upload IO gate wait timed out (fragment={}, version={}, part={}); proceeding ungated",
                    fragment, version_id, part_index
                );
                None
            }
        }
    } else {
        None
    };
    let durability_mode = parse_upload_durability_mode(&query);
    let strict_durability = durability_mode.strict();

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
    let expected_checksum = query
        .checksum
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    if let Ok(Some(existing)) = index::read_session_part(&fragment, &version_id, part_index).await {
        if existing.size_bytes != content_length {
            return Ok((
                StatusCode::CONFLICT,
                "Part already uploaded but size differs",
            )
                .into_response());
        }
        if let Some(expected) = expected_checksum {
            if let Some(existing_checksum) = existing.checksum.as_deref() {
                if !existing_checksum.eq_ignore_ascii_case(expected) {
                    return Ok((
                        StatusCode::CONFLICT,
                        "Part already uploaded but checksum differs",
                    )
                        .into_response());
                }
            } else {
                return Ok((
                    StatusCode::CONFLICT,
                    "Part already uploaded without checksum metadata",
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
                if strict_durability {
                    let file_clone = file
                        .try_clone()
                        .map_err(|e| AppError::Io(e, "clone file handle".into()))?;
                    let prealloc_result =
                        tokio::task::spawn_blocking(move || file_clone.set_len(final_shard_len))
                            .await;

                    if let Ok(Ok(_)) = prealloc_result {
                        writers.push(Some(DirectWriter::new(file)));
                        successful_opens += 1;
                    } else {
                        writers.push(None);
                    }
                } else {
                    writers.push(Some(DirectWriter::new(file)));
                    successful_opens += 1;
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

    let calc_total_checksum = expected_checksum.is_some();

    let mut stream_writer = ErasureStreamWriter::new(
        writers,
        data_shards,
        parity_shards,
        final_shard_len as usize,
        calc_total_checksum,
        block_size,
    )?;

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

    if let Some(expected) = expected_checksum {
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
    let writers = stream_writer.into_writers();

    if strict_durability {
        let sync_futs = writers.into_iter().filter_map(|w_opt| {
            w_opt.map(|w| async move {
                if let Err(e) = w.sync_data().await {
                    warn!("Failed to fsync shard: {}", e);
                    return Err(AppError::Io(e, "fsync shard".into()));
                }
                Ok(())
            })
        });

        let sync_results = join_all(sync_futs).await;
        for r in sync_results {
            if let Err(e) = r {
                cleanup_files_best_effort(&tmp_paths).await;
                return Err(e);
            }
        }
    }

    let mut dirs_to_sync: Vec<PathBuf> = Vec::new();

    for i in 0..total_shards {
        let expected_crc = shard_checksums.get(i).map(|s| s.as_str());

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

    if strict_durability {
        let _ = join_all(dirs_to_sync.iter().map(|d| sync_dir(d.as_path()))).await;
    }

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
        checksum: if calc_total_checksum {
            total_checksum
        } else {
            None
        },
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
        strict_durability,
    )
    .await?;

    if !committed {
        return Ok((StatusCode::OK, "Part already uploaded").into_response());
    }

    Ok((StatusCode::OK, "Part uploaded").into_response())
}

async fn write_prefix_segments_on_roots(
    index_roots: &[PathBuf],
    fragment: &str,
    object_key: &str,
    version_id: &str,
    created_at: i64,
    child_count_delta: i64,
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
                is_delete_marker: false,
            }],
        };

        let bytes = rmp_serde::to_vec(&seg).map_err(|e| {
            AppError::InternalServerError(format!("Serialize prefix segment: {}", e))
        })?;

        let ph = ObjectPathManager::prefix_hash(&prefix);
        let p1 = &ph[0..2];
        let p2 = &ph[2..4];
        let file = format!("seg_{}{}", version_id, PREFIX_SEGMENT_EXT);

        let mut wrote_this_prefix = false;

        for root in index_roots {
            let p = root
                .join(fragment)
                .join("index")
                .join(PREFIX_INDEX_DIR)
                .join(PREFIX_INDEX_VERSION)
                .join(PREFIX_INDEX_NAMESPACE)
                .join(p1)
                .join(p2)
                .join(&ph)
                .join(&file);

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

    let path_manager = ObjectPathManager::new();
    write_delimiter_delta(
        &path_manager,
        fragment,
        object_key,
        version_id,
        Some(PrefixIndexEntry {
            object_key: object_key.to_string(),
            version_id: version_id.to_string(),
            created_at,
            is_delete_marker: false,
        }),
        child_count_delta,
    )
    .await?;

    Ok(())
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
    let had_live_before = matches!(get_metadata(&fragment, &object_key, &fragment).await, Ok(meta) if !meta.is_delete_marker);

    if index::read_object_version(&fragment, &object_key, &version_id)
        .await
        .is_ok()
    {
        return Ok((StatusCode::OK, "Upload finalized.").into_response());
    }

    let manifest = read_manifest_any(&fragment, &version_id).await?;

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
        if let Some(pm) = read_part_meta_any(&fragment, &version_id, i).await {
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

    index::publish_object(&object_meta).await?;

    let index_roots = path_manager.get_all_index_roots();
    write_prefix_segments_on_roots(
        &index_roots,
        &fragment,
        &object_key,
        &version_id,
        object_meta.created_at,
        if had_live_before { 0 } else { 1 },
    )
    .await?;

    schedule_prefix_compaction(
        fragment.clone(),
        prefixes_for_key(&object_key).into_iter().collect(),
    );

    let _ = index::remove_session(&fragment, &version_id, "finalized").await;

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

    if index::read_object_version(&fragment, &key_query.object_key, &version_id)
        .await
        .is_ok()
    {
        return Err(AppError::Conflict(
            "Cannot cancel: Object is already finalized.".to_string(),
        ));
    }

    let mut refund_amount = 0u64;
    if let Some(state) = index::load_session(&fragment, &version_id).await? {
        refund_amount = state.reservation.map(|r| r.amount).unwrap_or(0);
    }

    cleanup_blob_tmp_attempt_files_for_version(&path_manager, &fragment, &version_id).await;
    let _ = index::remove_session(&fragment, &version_id, "cancelled").await;

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
