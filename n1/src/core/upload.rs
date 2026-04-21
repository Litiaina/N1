use std::collections::{BTreeSet, HashMap};
use std::fs::{self, OpenOptions};
use std::io;
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
use tokio::fs as tokio_fs;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Semaphore;
use tracing::{error, info, warn};
use ulid::Ulid;

#[cfg(target_os = "linux")]
use io_uring::{IoUring, opcode, types};
#[cfg(target_os = "linux")]
use std::os::unix::fs::FileExt;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

use crate::core::constants::{PARITY_SUFFIX, SHARD_SUFFIX};
use crate::core::disk_manager::{
    FinalizeIncompleteResponse, InitiateUploadParams, InitiateUploadResponse, ObjectKeyQuery,
    ObjectMetadata, ObjectPathManager, PartMetadata, ReservationReceipt, ShardInfo,
    UploadPartQuery, UploadStatusResponse, default_stripe_size,
};
use crate::core::ec::{AlignedBuffer, ErasureStreamWriter};
use crate::core::index;
use crate::core::quota_manager::{
    ensure_blob_placement_capacity_available_for_parts, ensure_capacity_available,
};
use crate::enums::app_error_enum::AppError;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;

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
const UPLOAD_IO_GATE_TIMEOUT_SECS: u64 = 2;

#[cfg(target_os = "linux")]
static IO_URING_ENABLED: Lazy<bool> = Lazy::new(|| {
    let enabled = std::env::var("N1_IO_URING_ENABLED")
        .ok()
        .map(|v| {
            !matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "off" | "no"
            )
        })
        .unwrap_or(true);

    if !enabled {
        return false;
    }

    match IoUring::new(8) {
        Ok(_) => true,
        Err(err) => {
            warn!(
                "io_uring unavailable for Standard direct writer, falling back to O_DIRECT blocking writes: {}",
                err
            );
            false
        }
    }
});

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

async fn acquire_upload_io_guard_with_timeout(
    io_gate_enabled: bool,
    context: &str,
) -> Result<Option<AdaptiveIoGuard<'static>>, AppError> {
    if !io_gate_enabled {
        return Ok(None);
    }

    match tokio::time::timeout(
        std::time::Duration::from_secs(UPLOAD_IO_GATE_TIMEOUT_SECS),
        acquire_adaptive_io_guard(),
    )
    .await
    {
        Ok(Ok(guard)) => Ok(Some(guard)),
        Ok(Err(e)) => Err(e),
        Err(_) => {
            warn!(
                "Upload IO gate wait timed out ({}); rejecting request to preserve backpressure",
                context
            );
            Err(AppError::ServiceUnavailable(
                "Upload path is saturated; retry shortly.".into(),
            ))
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
    use super::{
        UploadDurabilityMode, acquire_upload_io_guard_with_timeout, parse_upload_durability_mode,
    };
    use crate::core::disk_manager::UploadPartQuery;

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

    #[tokio::test]
    async fn upload_io_gate_helper_bypasses_when_disabled() {
        let guard = acquire_upload_io_guard_with_timeout(false, "unit-test")
            .await
            .expect("gate bypass succeeds");
        assert!(guard.is_none());
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
    #[serde(default)]
    pub part_size_bytes: u64,
    pub content_type: String,
    pub created_at: i64,
    pub stripe_size_bytes: usize,
    #[serde(default)]
    pub data_shards: u16,
    #[serde(default)]
    pub parity_shards: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveSessionInfo {
    pub session_id: String,
    pub version_id: String,
    pub object_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub physical_object_key: Option<String>,
    pub created_at: i64,
    pub reserved_size: u64,
    pub total_size_bytes: Option<u64>,
    pub expected_parts: Option<u64>,
    pub committed_parts: u64,
    pub committed_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListUploadsResponse {
    pub sessions: Vec<ActiveSessionInfo>,
    pub total: usize,
}

fn committed_session_progress(state: &index::IndexSessionState) -> (u64, u64) {
    if !state.part_receipts.is_empty() {
        let committed_parts = state.part_receipts.len() as u64;
        let committed_bytes = state
            .part_receipts
            .values()
            .map(|receipt| receipt.size_bytes)
            .sum();
        return (committed_parts, committed_bytes);
    }

    let committed_parts = state.parts.len() as u64;
    let committed_bytes = state.parts.values().map(|part| part.size_bytes).sum();
    (committed_parts, committed_bytes)
}

pub struct DirectWriter {
    file: Arc<fs::File>,
    write_buffer: AlignedBuffer,
    next_offset: u64,
    write_task: Option<tokio::task::JoinHandle<Result<AlignedBuffer, std::io::Error>>>,
}

#[cfg(target_os = "linux")]
fn write_all_at_blocking(file: &fs::File, buf: &[u8], offset: u64) -> io::Result<()> {
    let mut written = 0usize;
    while written < buf.len() {
        let n = file.write_at(&buf[written..], offset + written as u64)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "write_at returned 0",
            ));
        }
        written += n;
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn write_all_at_blocking(file: &fs::File, buf: &[u8], offset: u64) -> io::Result<()> {
    use std::io::{Seek, SeekFrom, Write};

    let mut cloned = file.try_clone()?;
    cloned.seek(SeekFrom::Start(offset))?;
    cloned.write_all(buf)
}

#[cfg(target_os = "linux")]
fn write_all_at_io_uring(file: &fs::File, buf: &[u8], offset: u64) -> io::Result<()> {
    let mut ring = IoUring::new(8)?;
    let fd = types::Fd(file.as_raw_fd());
    let mut written = 0usize;

    while written < buf.len() {
        let ptr = unsafe { buf.as_ptr().add(written) };
        let len = (buf.len() - written) as u32;
        let entry = opcode::Write::new(fd, ptr, len)
            .offset(offset + written as u64)
            .build()
            .user_data(1);

        unsafe {
            ring.submission()
                .push(&entry)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "io_uring submission full"))?;
        }

        ring.submit_and_wait(1)?;
        let cqe = ring
            .completion()
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "io_uring completion missing"))?;
        let result = cqe.result();
        if result < 0 {
            return Err(io::Error::from_raw_os_error(-result));
        }
        if result == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "io_uring write returned 0",
            ));
        }
        written += result as usize;
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn write_all_direct(file: &fs::File, buf: &[u8], offset: u64) -> io::Result<()> {
    if *IO_URING_ENABLED {
        write_all_at_io_uring(file, buf, offset)
    } else {
        write_all_at_blocking(file, buf, offset)
    }
}

#[cfg(not(target_os = "linux"))]
fn write_all_direct(file: &fs::File, buf: &[u8], offset: u64) -> io::Result<()> {
    write_all_at_blocking(file, buf, offset)
}

#[cfg(target_os = "linux")]
fn sync_file_direct(file: &fs::File) -> io::Result<()> {
    if *IO_URING_ENABLED {
        let mut ring = IoUring::new(8)?;
        let entry = opcode::Fsync::new(types::Fd(file.as_raw_fd()))
            .build()
            .user_data(1);
        unsafe {
            ring.submission()
                .push(&entry)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "io_uring submission full"))?;
        }
        ring.submit_and_wait(1)?;
        let cqe = ring
            .completion()
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "io_uring completion missing"))?;
        let result = cqe.result();
        if result < 0 {
            return Err(io::Error::from_raw_os_error(-result));
        }
        Ok(())
    } else {
        file.sync_all()
    }
}

#[cfg(not(target_os = "linux"))]
fn sync_file_direct(file: &fs::File) -> io::Result<()> {
    file.sync_all()
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
            next_offset: 0,
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
        let offset = self.next_offset;
        let mut to_write = std::mem::replace(&mut self.write_buffer, AlignedBuffer::new(0));
        self.next_offset = self.next_offset.saturating_add(to_write.len() as u64);

        let task = tokio::task::spawn_blocking(move || {
            write_all_direct(&file, to_write.as_slice(), offset)?;
            to_write.clear();
            Ok::<AlignedBuffer, std::io::Error>(to_write)
        });

        self.write_task = Some(task);
    }

    pub async fn sync_data(&self) -> Result<(), std::io::Error> {
        let file = self.file.clone();
        tokio::task::spawn_blocking(move || sync_file_direct(&file)).await?
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

fn configured_data_shards() -> usize {
    DATA_SHARDS_ATOMIC.get().unwrap().load(Ordering::Relaxed) as usize
}

fn configured_parity_shards() -> usize {
    PARITY_SHARDS_ATOMIC.get().unwrap().load(Ordering::Relaxed) as usize
}

fn manifest_data_shards(manifest: &UploadSessionManifest) -> usize {
    let configured = usize::from(manifest.data_shards);
    if configured == 0 {
        configured_data_shards()
    } else {
        configured
    }
}

fn manifest_parity_shards(manifest: &UploadSessionManifest) -> usize {
    let configured = usize::from(manifest.parity_shards);
    if configured == 0 {
        configured_parity_shards()
    } else {
        configured
    }
}

fn manifest_total_shards(manifest: &UploadSessionManifest) -> usize {
    manifest_data_shards(manifest) + manifest_parity_shards(manifest)
}

fn configured_block_size() -> usize {
    STRIPE_SIZE_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or_else(default_stripe_size)
}

fn stable_shard_name(part_index: u64, shard_index: usize, data_shards: usize) -> String {
    let ext = if shard_index >= data_shards {
        PARITY_SUFFIX
    } else {
        SHARD_SUFFIX
    };
    format!("part_{}_shard_{}{}", part_index, shard_index, ext)
}

fn expected_part_size_bytes(manifest: &UploadSessionManifest, part_index: u64) -> Option<u64> {
    if manifest.expected_parts == 0 || part_index >= manifest.expected_parts {
        return None;
    }
    if manifest.expected_parts == 1 {
        return Some(manifest.total_size_bytes);
    }
    let part_size = manifest.part_size_bytes;
    if part_size == 0 {
        return None;
    }
    if part_index + 1 < manifest.expected_parts {
        return Some(part_size);
    }
    let prior_bytes = part_size.saturating_mul(manifest.expected_parts.saturating_sub(1));
    if prior_bytes > manifest.total_size_bytes {
        return None;
    }
    Some(manifest.total_size_bytes - prior_bytes)
}

fn planned_part_sizes(
    total_size_bytes: u64,
    expected_parts: u64,
    part_size_bytes: u64,
) -> Vec<u64> {
    let mut part_sizes = Vec::with_capacity(expected_parts as usize);
    if expected_parts == 0 {
        return part_sizes;
    }
    for part_index in 0..expected_parts {
        let size = if expected_parts == 1 {
            total_size_bytes
        } else if part_index + 1 < expected_parts {
            part_size_bytes
        } else {
            let prior_bytes = part_size_bytes.saturating_mul(expected_parts.saturating_sub(1));
            total_size_bytes.saturating_sub(prior_bytes)
        };
        part_sizes.push(size);
    }
    part_sizes
}

fn final_shard_len_for_part(part_size_bytes: u64, data_shards: usize, block_size: usize) -> u64 {
    let stripe_data_width = block_size as u64 * data_shards as u64;
    let stripe_count = if stripe_data_width == 0 {
        0
    } else {
        part_size_bytes.div_ceil(stripe_data_width)
    };
    stripe_count * block_size as u64
}

fn shard_specs_for_part(
    path_manager: &ObjectPathManager,
    manifest: &UploadSessionManifest,
    part_index: u64,
) -> Result<Vec<(usize, bool, PathBuf, String)>, AppError> {
    let blob_roots = path_manager.get_blob_roots();
    if blob_roots.is_empty() {
        return Err(AppError::InternalServerError(
            "No blob storage disks configured".into(),
        ));
    }

    let data_shards = manifest_data_shards(manifest);
    let total_shards = manifest_total_shards(manifest);
    let mut out = Vec::with_capacity(total_shards);

    for shard_index in 0..total_shards {
        let disk_idx = (part_index as usize + shard_index) % blob_roots.len();
        let disk_root = &blob_roots[disk_idx];
        let shard_dir =
            path_manager.get_shard_dir_on_disk(disk_root, &manifest.fragment, &manifest.version_id);
        let file_name = stable_shard_name(part_index, shard_index, data_shards);
        let stable_path = shard_dir.join(&file_name);
        out.push((
            shard_index,
            shard_index >= data_shards,
            stable_path,
            file_name,
        ));
    }

    Ok(out)
}

async fn part_exists_on_disk(
    path_manager: &ObjectPathManager,
    manifest: &UploadSessionManifest,
    part_index: u64,
) -> Result<bool, AppError> {
    let Some(part_size) = expected_part_size_bytes(manifest, part_index) else {
        return Ok(false);
    };

    let data_shards = manifest_data_shards(manifest);
    let block_size = configured_block_size();
    let expected_shard_len = final_shard_len_for_part(part_size, data_shards, block_size);

    for (_, _, stable_path, _) in shard_specs_for_part(path_manager, manifest, part_index)? {
        let meta = match tokio_fs::metadata(&stable_path).await {
            Ok(meta) => meta,
            Err(_) => return Ok(false),
        };
        if meta.len() != expected_shard_len {
            return Ok(false);
        }
    }

    Ok(true)
}

async fn ensure_version_shard_dirs(
    path_manager: &ObjectPathManager,
    manifest: &UploadSessionManifest,
) -> Result<(), AppError> {
    let create_futs = path_manager.get_blob_roots().into_iter().map(|disk_root| {
        let shard_dir = path_manager.get_shard_dir_on_disk(
            &disk_root,
            &manifest.fragment,
            &manifest.version_id,
        );
        async move {
            tokio_fs::create_dir_all(&shard_dir)
                .await
                .map_err(|e| AppError::Io(e, format!("mkdir shard dir {}", shard_dir.display())))
        }
    });

    for result in join_all(create_futs).await {
        result?;
    }

    Ok(())
}

async fn uploaded_parts_from_disk(
    path_manager: &ObjectPathManager,
    manifest: &UploadSessionManifest,
) -> Result<Vec<u64>, AppError> {
    let mut uploaded_parts = Vec::new();
    for part_index in 0..manifest.expected_parts {
        if part_exists_on_disk(path_manager, manifest, part_index).await? {
            uploaded_parts.push(part_index);
        }
    }
    Ok(uploaded_parts)
}

async fn reconstruct_part_meta_from_disk(
    path_manager: &ObjectPathManager,
    manifest: &UploadSessionManifest,
    part_index: u64,
) -> Result<Option<PartMetadata>, AppError> {
    let Some(part_size) = expected_part_size_bytes(manifest, part_index) else {
        return Ok(None);
    };

    let data_shards = manifest_data_shards(manifest);
    let block_size = configured_block_size();
    let expected_shard_len = final_shard_len_for_part(part_size, data_shards, block_size);

    let mut shards = Vec::with_capacity(manifest_total_shards(manifest));
    for (shard_index, is_parity, stable_path, file_name) in
        shard_specs_for_part(path_manager, manifest, part_index)?
    {
        let meta = match tokio_fs::metadata(&stable_path).await {
            Ok(meta) => meta,
            Err(_) => return Ok(None),
        };
        if meta.len() != expected_shard_len {
            return Ok(None);
        }
        let checksum = crc32_file_hex(&stable_path).await?;
        shards.push(ShardInfo {
            shard_index,
            file_name,
            absolute_path: stable_path.to_string_lossy().to_string(),
            size_bytes: expected_shard_len,
            checksum,
            is_parity,
        });
    }

    Ok(Some(PartMetadata {
        part_index,
        size_bytes: part_size,
        created_at: manifest.created_at,
        shards,
        checksum: None,
    }))
}

fn format_crc32_u32(value: u32) -> String {
    format!("{:08x}", value)
}

async fn reconstruct_part_meta_from_receipt(
    path_manager: &ObjectPathManager,
    manifest: &UploadSessionManifest,
    receipt: &index::SessionPartReceipt,
) -> Result<Option<PartMetadata>, AppError> {
    let Some(part_size) = expected_part_size_bytes(manifest, receipt.part_index) else {
        return Ok(None);
    };
    if receipt.size_bytes != part_size {
        return Ok(None);
    }

    let data_shards = manifest_data_shards(manifest);
    let total_shards = manifest_total_shards(manifest);
    if receipt.shard_checksums.len() != total_shards {
        return Ok(None);
    }

    let block_size = configured_block_size();
    let expected_shard_len = final_shard_len_for_part(part_size, data_shards, block_size);
    let shard_size_bytes = if receipt.shard_size_bytes == 0 {
        expected_shard_len
    } else {
        receipt.shard_size_bytes
    };

    let mut shards = Vec::with_capacity(total_shards);
    for ((shard_index, is_parity, stable_path, file_name), checksum) in
        shard_specs_for_part(path_manager, manifest, receipt.part_index)?
            .into_iter()
            .zip(receipt.shard_checksums.iter())
    {
        shards.push(ShardInfo {
            shard_index,
            file_name,
            absolute_path: stable_path.to_string_lossy().to_string(),
            size_bytes: shard_size_bytes,
            checksum: format_crc32_u32(*checksum),
            is_parity,
        });
    }

    Ok(Some(PartMetadata {
        part_index: receipt.part_index,
        size_bytes: receipt.size_bytes,
        created_at: receipt.created_at,
        shards,
        checksum: receipt.checksum.map(format_crc32_u32),
    }))
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

fn request_content_length(headers: &HeaderMap) -> Result<u64, AppError> {
    headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .ok_or(AppError::BadRequest(
            "Content-Length header required".into(),
        ))
}

fn request_content_type(headers: &HeaderMap) -> Option<String> {
    headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.split(';').next().unwrap_or(v).trim().to_string())
        .filter(|v| !v.is_empty())
}

async fn write_part_blob_to_disk(
    path_manager: &ObjectPathManager,
    fragment: &str,
    version_id: &str,
    part_index: u64,
    content_length: u64,
    body: Body,
    expected_checksum: Option<&str>,
    strict_durability: bool,
) -> Result<PartMetadata, AppError> {
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

    let attempt_id = Ulid::new().to_string();
    let stripe_data_width = block_size as u64 * data_shards as u64;
    let stripe_count = (content_length + stripe_data_width - 1) / stripe_data_width;
    let final_shard_len = stripe_count * block_size as u64;

    ensure_blob_placement_capacity_available_for_parts(
        part_index,
        &[content_length],
        data_shards,
        parity_shards,
        block_size,
    )
    .await?;

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
        let shard_dir = path_manager.get_shard_dir_on_disk(target_disk, fragment, version_id);

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
            Err(_) => writers.push(None),
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
                return Err(AppError::BadRequest("Checksum mismatch".into()));
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

    Ok(PartMetadata {
        part_index,
        size_bytes: total_bytes_read,
        created_at: Utc::now().timestamp(),
        checksum: if calc_total_checksum {
            total_checksum
        } else {
            None
        },
        shards: shards_info,
    })
}

async fn cleanup_files_best_effort(paths: &[PathBuf]) {
    let futs = paths.iter().cloned().map(|p| async move {
        let _ = tokio_fs::remove_file(&p).await;
    });
    let _ = join_all(futs).await;
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
    if !tokio_fs::try_exists(stable_path).await.unwrap_or(false) {
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
        return Ok(());
    }

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

    let part_size_bytes = if params.expected_parts <= 1 {
        params.part_size_bytes.unwrap_or(params.total_size_bytes)
    } else {
        params
            .part_size_bytes
            .filter(|v| *v > 0)
            .ok_or(AppError::BadRequest(
                "part_size_bytes is required for multipart uploads".into(),
            ))?
    };

    let computed_parts = if params.total_size_bytes == 0 {
        1
    } else if part_size_bytes == 0 {
        1
    } else {
        params.total_size_bytes.div_ceil(part_size_bytes)
    };

    if computed_parts != params.expected_parts.max(1) {
        return Err(AppError::BadRequest(
            "expected_parts does not match total_size_bytes and part_size_bytes".into(),
        ));
    }

    ensure_capacity_available(params.total_size_bytes).await?;

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
        part_size_bytes,
        content_type: params
            .content_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".to_string()),
        created_at: Utc::now().timestamp(),
        stripe_size_bytes: stripe_size,
        data_shards: configured_data_shards() as u16,
        parity_shards: configured_parity_shards() as u16,
    };

    ensure_blob_placement_capacity_available_for_parts(
        0,
        &planned_part_sizes(
            manifest.total_size_bytes,
            manifest.expected_parts,
            manifest.part_size_bytes,
        ),
        manifest_data_shards(&manifest),
        manifest_parity_shards(&manifest),
        manifest.stripe_size_bytes,
    )
    .await?;

    let path_manager = ObjectPathManager::new();
    ensure_version_shard_dirs(&path_manager, &manifest).await?;

    if let Err(e) = index::open_session(&manifest, Some(receipt)).await {
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
        .map(|state| {
            let (committed_parts, committed_bytes) = committed_session_progress(&state);
            ActiveSessionInfo {
                session_id: state.manifest.session_id.clone(),
                version_id: state.manifest.version_id.clone(),
                object_key: Some(state.manifest.object_key.clone()),
                physical_object_key: None,
                created_at: state.manifest.created_at,
                reserved_size: state.reservation.map(|receipt| receipt.amount).unwrap_or(0),
                total_size_bytes: Some(state.manifest.total_size_bytes),
                expected_parts: Some(state.manifest.expected_parts),
                committed_parts,
                committed_bytes,
            }
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
        let path_manager = ObjectPathManager::new();
        let (committed_parts, committed_bytes) = committed_session_progress(&state);
        let mut parts = if let Some(receipts) =
            index::load_session_part_receipts(&fragment, &version_id, &state.manifest).await?
        {
            receipts
                .iter()
                .enumerate()
                .filter_map(|(idx, receipt)| receipt.as_ref().map(|_| idx as u64))
                .collect()
        } else if state.manifest.part_size_bytes > 0 {
            uploaded_parts_from_disk(&path_manager, &state.manifest).await?
        } else {
            Vec::new()
        };
        parts.sort_unstable();
        return Ok(AxumJson(UploadStatusResponse {
            uploaded_parts: parts,
            committed_parts,
            committed_bytes,
            expected_parts: Some(state.manifest.expected_parts),
            total_size_bytes: Some(state.manifest.total_size_bytes),
            created_at: Some(state.manifest.created_at),
            session_id: Some(state.manifest.session_id),
            version_id: Some(state.manifest.version_id),
            object_key: Some(state.manifest.object_key),
            physical_object_key: None,
        }));
    }

    Ok(AxumJson(UploadStatusResponse::default()))
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
    let gate_context = format!(
        "fragment={}, version={}, part={}",
        fragment, version_id, part_index
    );
    let _io_guard = acquire_upload_io_guard_with_timeout(io_gate_enabled, &gate_context).await?;
    let durability_mode = parse_upload_durability_mode(&query);
    let strict_durability = durability_mode.strict();

    let max_part_size = MAX_PART_SIZE_ATOMIC.get().unwrap().load(Ordering::Relaxed);
    let content_length = request_content_length(&headers)?;

    if content_length > max_part_size {
        return Ok((StatusCode::PAYLOAD_TOO_LARGE, "Part too large").into_response());
    }

    let path_manager = ObjectPathManager::new();
    let manifest = read_manifest_any(&fragment, &version_id)
        .await?
        .ok_or(AppError::BadRequest("Upload session not found".into()))?;
    if part_index >= manifest.expected_parts {
        return Ok((StatusCode::BAD_REQUEST, "Part index out of range").into_response());
    }
    let expected_size = expected_part_size_bytes(&manifest, part_index);
    let expected_checksum = query
        .checksum
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    if part_exists_on_disk(&path_manager, &manifest, part_index).await? {
        if let Some(expected_size) = expected_size {
            if content_length != expected_size {
                return Ok((
                    StatusCode::CONFLICT,
                    "Part already uploaded but size differs",
                )
                    .into_response());
            }
        }
        if expected_checksum.is_some() {
            warn!(
                "Multipart resume for {}/{} part {} skipped checksum revalidation because session part metadata is not persisted",
                fragment, version_id, part_index
            );
        }
        return Ok((StatusCode::OK, "Part already uploaded").into_response());
    }
    if let Some(expected_size) = expected_size {
        if content_length != expected_size {
            return Ok((
                StatusCode::BAD_REQUEST,
                "Part size does not match upload session manifest",
            )
                .into_response());
        }
    }

    let part_meta = write_part_blob_to_disk(
        &path_manager,
        &fragment,
        &version_id,
        part_index,
        content_length,
        body,
        expected_checksum,
        strict_durability,
    )
    .await?;

    if let Err(err) =
        index::commit_session_part_receipt(&fragment, &version_id, &manifest, &part_meta).await
    {
        warn!(
            "Failed to persist compact multipart receipt for {}/{} part {}: {}",
            fragment, version_id, part_index, err
        );
    }

    Ok((StatusCode::OK, "Part uploaded").into_response())
}

pub async fn upload_object_one_shot(
    State(_state): State<Arc<AppState>>,
    claims: Claims,
    headers: HeaderMap,
    AxumPath((fragment, object_key)): AxumPath<(String, String)>,
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
    let gate_context = format!("fragment={}, object_key={}", fragment, object_key);
    let _io_guard = acquire_upload_io_guard_with_timeout(io_gate_enabled, &gate_context).await?;

    let durability_mode = parse_upload_durability_mode(&query);
    let strict_durability = durability_mode.strict();
    let content_length = request_content_length(&headers)?;
    let max_part_size = MAX_PART_SIZE_ATOMIC.get().unwrap().load(Ordering::Relaxed);
    if content_length > max_part_size {
        return Ok((
            StatusCode::PAYLOAD_TOO_LARGE,
            "Object too large for one-shot upload",
        )
            .into_response());
    }

    ensure_capacity_available(content_length).await?;

    let version_id = Ulid::new().to_string();
    let session_id = Ulid::new().to_string();
    let expected_checksum = query
        .checksum
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let content_type =
        request_content_type(&headers).unwrap_or_else(|| "application/octet-stream".to_string());

    let path_manager = ObjectPathManager::new();
    let part_meta = write_part_blob_to_disk(
        &path_manager,
        &fragment,
        &version_id,
        0,
        content_length,
        body,
        expected_checksum,
        strict_durability,
    )
    .await?;

    let stripe_size = STRIPE_SIZE_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or_else(default_stripe_size);

    let object_meta = ObjectMetadata {
        object_key: object_key.clone(),
        fragment: fragment.clone(),
        version_id: version_id.clone(),
        upload_session_id: session_id,
        owner_uid: fragment.clone(),
        content_type,
        total_size_bytes: content_length,
        created_at: Utc::now().timestamp(),
        parts: vec![part_meta],
        is_delete_marker: false,
        stripe_size_bytes: stripe_size,
    };

    let _object_lock = index::acquire_object_operation_lock(&fragment, &object_key).await?;
    index::publish_object(&object_meta).await?;

    let response_body = serde_json::to_value(&object_meta).map_err(|e| {
        AppError::InternalServerError(format!("serialize one-shot upload response: {}", e))
    })?;
    Ok((StatusCode::CREATED, AxumJson(response_body)).into_response())
}

pub async fn finalize_object_upload(
    State(_state): State<Arc<AppState>>,
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
    let _object_lock = index::acquire_object_operation_lock(&fragment, &object_key).await?;

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

    if let Some(m) = manifest.as_ref() {
        let receipts = index::load_session_part_receipts(&fragment, &version_id, m).await?;
        for i in 0..expected_parts {
            let mut resolved = None;

            if let Some(receipt) = receipts
                .as_ref()
                .and_then(|items| items.get(i as usize))
                .and_then(|entry| entry.as_ref())
            {
                resolved = reconstruct_part_meta_from_receipt(&path_manager, m, receipt).await?;
            }

            if resolved.is_none() && m.part_size_bytes > 0 {
                resolved = reconstruct_part_meta_from_disk(&path_manager, m, i).await?;
            }

            if resolved.is_none() {
                resolved = read_part_meta_any(&fragment, &version_id, i).await;
            }

            if let Some(pm) = resolved {
                present.insert(i);
                parts.push(pm);
            }
        }
    } else {
        for i in 0..expected_parts {
            if let Some(pm) = read_part_meta_any(&fragment, &version_id, i).await {
                present.insert(i);
                parts.push(pm);
            }
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
        created_at: manifest
            .as_ref()
            .map(|m| m.created_at)
            .unwrap_or_else(|| Utc::now().timestamp()),
        parts,
        is_delete_marker: false,
        stripe_size_bytes: stripe_size,
    };

    index::publish_object(&object_meta).await?;

    let _ = index::remove_session(&fragment, &version_id, "finalized").await;

    let response_body = serde_json::to_value(&object_meta).map_err(|e| {
        AppError::InternalServerError(format!("serialize finalized upload response: {}", e))
    })?;
    Ok((StatusCode::OK, AxumJson(response_body)).into_response())
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
    let _object_lock =
        index::acquire_object_operation_lock(&fragment, &key_query.object_key).await?;

    if index::read_object_version(&fragment, &key_query.object_key, &version_id)
        .await
        .is_ok()
    {
        return Err(AppError::Conflict(
            "Cannot cancel: Object is already finalized.".to_string(),
        ));
    }

    cleanup_blob_tmp_attempt_files_for_version(&path_manager, &fragment, &version_id).await;
    let _ = index::remove_session(&fragment, &version_id, "cancelled").await;

    Ok(StatusCode::NO_CONTENT)
}
