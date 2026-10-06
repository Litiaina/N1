use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs::File as StdFile;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::sync::{LazyLock, Mutex, OnceLock as StdOnceLock, RwLock};
use std::task::{Context, Poll};
use std::time::Instant;

use axum::Json as AxumJson;
use axum::body::Body;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use bytes::Bytes;
use chrono::Utc;
use crc32fast::Hasher;
use futures_util::Stream;
use futures_util::StreamExt;
use futures_util::future::join_all;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use tokio::fs as tokio_fs;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use tracing::{error, info, warn};
use ulid::Ulid;

use crate::core::constants::{PARITY_SUFFIX, SHARD_SUFFIX};
use crate::core::disk_manager::{
    FinalizeIncompleteResponse, InitiateUploadParams, InitiateUploadResponse, ObjectKeyQuery,
    ObjectMetadata, ObjectPathManager, PartMetadata, ShardInfo, UploadPartQuery,
    UploadStatusResponse, default_stripe_size,
};
use crate::core::ec::{AlignedBuffer, ErasureStreamWriter};
use crate::core::index;
use crate::core::io_scheduler::{DiskIoClass, DiskIoGuard, IoProgress, acquire_disk_io};
use crate::core::linux_io;
pub use crate::core::linux_io::sync_dir;
use crate::core::operation_gate::ensure_user_mutations_allowed;
use crate::core::root_identity::configured_root_identity_is_current;
use crate::enums::app_error_enum::AppError;
use crate::io_control::record_io_latency;
use crate::routes::storage_routes::AppState;
use crate::settings::config::CONFIG;
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

const MIN_DYNAMIC_STRIPE_SIZE: usize = 4 * 1024;
const TINY_OBJECT_STRIPE_4K_LIMIT: u64 = 64 * 1024;
const SMALL_OBJECT_STRIPE_16K_LIMIT: u64 = 1024 * 1024;
const SMALL_BUFFERED_WRITE_LIMIT: u64 = 1024 * 1024;

static MULTIPART_PART_LOCKS: LazyLock<
    Mutex<std::collections::HashMap<String, Weak<AsyncMutex<()>>>>,
> = LazyLock::new(|| Mutex::new(std::collections::HashMap::new()));

const STANDARD_SESSION_OPEN: u8 = 0;
const STANDARD_SESSION_FINALIZING: u8 = 1;
const STANDARD_SESSION_FINALIZED: u8 = 2;
const STANDARD_SESSION_CANCELLED: u8 = 3;
const STANDARD_PART_EMPTY: u8 = 0;
const STANDARD_PART_UPLOADING: u8 = 1;
const STANDARD_PART_COMPLETE: u8 = 2;

/// Process-local state for the standard high-throughput multipart API.
///
/// The registry lock is held only long enough to clone an `Arc` or insert/remove
/// a session. No request body, EC operation, disk I/O, or finalization work runs
/// while it is held.
static STANDARD_UPLOADS: LazyLock<RwLock<HashMap<String, Arc<StandardUploadSession>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

#[derive(Debug)]
struct StandardPartSlot {
    state: AtomicU8,
    record: StdOnceLock<StandardPartRecord>,
}

impl StandardPartSlot {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(STANDARD_PART_EMPTY),
            record: StdOnceLock::new(),
        }
    }
}

/// Compact in-memory completion record. Paths and filenames are deterministic
/// from the session manifest and are reconstructed only at finalization.
#[derive(Debug)]
struct StandardPartRecord {
    size_bytes: u64,
    created_at: i64,
    checksum: Option<u32>,
    shard_checksums: Box<[u32]>,
}

#[derive(Debug)]
struct StandardUploadSession {
    manifest: UploadSessionManifest,
    logical_path: String,
    lifecycle: AtomicU8,
    in_flight_parts: AtomicUsize,
    last_activity_at: AtomicI64,
    directories_ready: tokio::sync::OnceCell<()>,
    parts: Box<[StandardPartSlot]>,
}

fn standard_session_key(fragment: &str, version_id: &str) -> String {
    format!("{fragment}\u{001f}{version_id}")
}

fn standard_registry_read() -> Result<
    std::sync::RwLockReadGuard<'static, HashMap<String, Arc<StandardUploadSession>>>,
    AppError,
> {
    STANDARD_UPLOADS.read().map_err(|_| {
        AppError::InternalServerError("standard multipart registry poisoned".to_string())
    })
}

fn standard_registry_write() -> Result<
    std::sync::RwLockWriteGuard<'static, HashMap<String, Arc<StandardUploadSession>>>,
    AppError,
> {
    STANDARD_UPLOADS.write().map_err(|_| {
        AppError::InternalServerError("standard multipart registry poisoned".to_string())
    })
}

fn standard_session(
    fragment: &str,
    version_id: &str,
) -> Result<Option<Arc<StandardUploadSession>>, AppError> {
    Ok(standard_registry_read()?
        .get(&standard_session_key(fragment, version_id))
        .cloned())
}

struct StandardPartClaim<'a> {
    session: &'a StandardUploadSession,
    slot: &'a StandardPartSlot,
    completed: bool,
}

impl StandardPartClaim<'_> {
    fn complete(mut self, record: StandardPartRecord) -> Result<(), AppError> {
        self.slot.record.set(record).map_err(|_| {
            AppError::Conflict("Multipart part was completed concurrently".to_string())
        })?;
        self.session
            .last_activity_at
            .store(Utc::now().timestamp(), Ordering::Release);
        self.slot
            .state
            .store(STANDARD_PART_COMPLETE, Ordering::Release);
        self.completed = true;
        Ok(())
    }
}

impl Drop for StandardPartClaim<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.slot
                .state
                .store(STANDARD_PART_EMPTY, Ordering::Release);
        }
        self.session.in_flight_parts.fetch_sub(1, Ordering::AcqRel);
    }
}

struct MultipartPartGuard {
    _guard: OwnedMutexGuard<()>,
}

async fn acquire_multipart_part_lock(
    fragment: &str,
    version_id: &str,
    part_index: u64,
) -> Result<MultipartPartGuard, AppError> {
    let key = format!("{fragment}\u{001f}{version_id}\u{001f}{part_index}");
    let lock = {
        let mut locks = MULTIPART_PART_LOCKS.lock().map_err(|_| {
            AppError::InternalServerError("multipart part lock map poisoned".to_string())
        })?;
        if locks.len() > 4096 {
            locks.retain(|_, lock| lock.strong_count() > 0);
        }
        if let Some(existing) = locks.get(&key).and_then(Weak::upgrade) {
            existing
        } else {
            let lock = Arc::new(AsyncMutex::new(()));
            locks.insert(key, Arc::downgrade(&lock));
            lock
        }
    };
    Ok(MultipartPartGuard {
        _guard: lock.lock_owned().await,
    })
}

pub(crate) struct AdaptiveIoGuard {
    guard: DiskIoGuard,
}

impl AdaptiveIoGuard {
    pub(crate) fn progress_handle(&self) -> IoProgress {
        self.guard.progress_handle()
    }

    pub(crate) fn complete(self) {
        self.guard.complete();
    }
}

async fn acquire_adaptive_io_guard(
    fragment: &str,
    context: &str,
) -> Result<AdaptiveIoGuard, AppError> {
    Ok(AdaptiveIoGuard {
        guard: acquire_disk_io(DiskIoClass::ForegroundWrite, Some(fragment), context).await?,
    })
}

async fn acquire_upload_io_guard_with_timeout(
    io_gate_enabled: bool,
    fragment: &str,
    context: &str,
) -> Result<Option<AdaptiveIoGuard>, AppError> {
    if !io_gate_enabled {
        return Ok(None);
    }

    acquire_adaptive_io_guard(fragment, context).await.map(Some)
}

pub(crate) async fn acquire_upload_io_guard_for_query(
    io_gate_override: Option<bool>,
    fragment: &str,
    context: &str,
) -> Result<Option<AdaptiveIoGuard>, AppError> {
    let global_io_gate_enabled = UPLOAD_IO_GATE_ENABLED_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(true);
    let io_gate_enabled = io_gate_override.unwrap_or(global_io_gate_enabled);
    acquire_upload_io_guard_with_timeout(io_gate_enabled, fragment, context).await
}

async fn acquire_upload_io_guard_for_size(
    io_gate_override: Option<bool>,
    fragment: &str,
    context: &str,
    content_length: u64,
) -> Result<Option<AdaptiveIoGuard>, AppError> {
    if content_length <= SMALL_BUFFERED_WRITE_LIMIT && !io_gate_override.unwrap_or(false) {
        return Ok(None);
    }
    acquire_upload_io_guard_for_query(io_gate_override, fragment, context).await
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UploadDurabilityMode {
    Strict,
    Batched,
}

pub enum FinalizeUploadResult {
    Complete(ObjectMetadata),
    Incomplete(FinalizeIncompleteResponse),
}

struct ProgressReader<'a, R> {
    inner: &'a mut R,
    progress: Option<&'a IoProgress>,
}

impl<R> AsyncRead for ProgressReader<'_, R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut *self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &result {
            let read = buf.filled().len().saturating_sub(before);
            if read > 0
                && let Some(progress) = self.progress
            {
                progress.record(read as u64);
            }
        }
        result
    }
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

pub(crate) fn upload_uses_strict_durability(query: &UploadPartQuery) -> bool {
    parse_upload_durability_mode(query).strict()
}

mod activity;
mod data_plane;
mod sessions;
pub(crate) use activity::{UploadActivityGuard, UploadGcScan};

/// TTL reclamation is not session expiry. An idle resumable upload remains
/// protected until normal upload GC/cancellation removes its session.
pub(crate) fn standard_upload_object_ids(fragment: &str) -> Result<HashSet<String>, AppError> {
    Ok(standard_registry_read()?
        .values()
        .filter(|session| session.manifest.fragment == fragment)
        .filter(|session| session.lifecycle.load(Ordering::Acquire) != STANDARD_SESSION_CANCELLED)
        .map(|session| session.manifest.object_key.clone())
        .collect())
}

pub(crate) fn standard_namespace_activity(
    fragment: &str,
    version: &str,
) -> Result<UploadActivityGuard, AppError> {
    let session = standard_session(fragment, version)?
        .ok_or_else(|| AppError::BadRequest("Standard upload session not found".into()))?;
    UploadActivityGuard::begin(fragment, &session.manifest.object_key)
}

pub use data_plane::{
    ActiveSessionInfo, DirectWriter, ListUploadsResponse, UploadSessionManifest,
    create_direct_file, write_atomic,
};
pub use sessions::{
    active_standard_upload_versions, cancel_standard_upload_direct, complete_standard_upload,
    finalize_standard_upload_direct, find_reusable_standard_upload,
    get_standard_upload_status_direct, initiate_object_upload, initiate_object_upload_direct,
    initiate_standard_upload_direct, list_standard_uploads_direct,
    object_upload_part_standard_body_direct, object_upload_part_standard_reader_direct,
};

#[allow(unused_imports)]
pub(crate) use data_plane::{
    choose_upload_stripe_size_for_len_with_ceiling, target_stripe_size_for_metadata,
};

use data_plane::{
    choose_upload_stripe_size_for_len, choose_upload_stripe_size_for_parts,
    cleanup_blob_tmp_attempt_files_for_version, cleanup_blob_version_dirs_for_version,
    cleanup_files_best_effort, committed_session_progress, configured_block_size,
    configured_data_shards, configured_parity_shards, ensure_registered_placement_roots,
    ensure_version_shard_dirs, expected_part_size_bytes, final_shard_len_for_part,
    format_crc32_u32, manifest_data_shards, manifest_parity_shards, part_exists_on_disk,
    persist_missing_receipt_from_disk, planned_part_sizes, read_manifest_any, read_part_meta_any,
    reconstruct_part_meta_from_disk, reconstruct_part_meta_from_receipt, request_content_length,
    request_content_type, shard_specs_for_part, uploaded_part_bytes, uploaded_parts_from_disk,
    write_part_blob_to_disk, write_part_blob_to_disk_from_reader,
    write_part_blob_to_disk_from_stream,
};

#[cfg(test)]
use sessions::{StandardPartClaimResult, claim_standard_part};

#[cfg(test)]
mod tests;

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
        let (mut committed_parts, mut committed_bytes) = committed_session_progress(&state);
        let mut part_set = BTreeSet::new();
        if let Some(receipts) =
            index::load_session_part_receipts(&fragment, &version_id, &state.manifest).await?
        {
            part_set.extend(
                receipts
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, receipt)| receipt.as_ref().map(|_| idx as u64)),
            );
        }
        if state.manifest.part_size_bytes > 0
            && part_set.len() < state.manifest.expected_parts as usize
        {
            part_set.extend(uploaded_parts_from_disk(&path_manager, &state.manifest).await?);
        }
        let disk_backed_bytes = uploaded_part_bytes(&state.manifest, &part_set);
        if disk_backed_bytes > committed_bytes {
            committed_parts = part_set.len() as u64;
            committed_bytes = disk_backed_bytes;
        }
        let parts = part_set.into_iter().collect();
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

    let durability_mode = parse_upload_durability_mode(&query);
    let strict_durability = durability_mode.strict();

    let max_part_size = MAX_PART_SIZE_ATOMIC.get().unwrap().load(Ordering::Relaxed);
    let content_length = request_content_length(&headers)?;
    let gate_context = format!(
        "fragment={}, version={}, part={}",
        fragment, version_id, part_index
    );
    let io_guard =
        acquire_upload_io_guard_for_size(query.io_gate, &fragment, &gate_context, content_length)
            .await?;
    let io_progress = io_guard.as_ref().map(AdaptiveIoGuard::progress_handle);

    if content_length > max_part_size {
        return Ok((StatusCode::PAYLOAD_TOO_LARGE, "Part too large").into_response());
    }

    let path_manager = ObjectPathManager::new();
    let manifest = read_manifest_any(&fragment, &version_id)
        .await?
        .ok_or(AppError::BadRequest("Upload session not found".into()))?;
    let _upload_activity = UploadActivityGuard::begin(&fragment, &manifest.object_key)?;
    if part_index >= manifest.expected_parts {
        return Ok((StatusCode::BAD_REQUEST, "Part index out of range").into_response());
    }
    let _part_lock = acquire_multipart_part_lock(&fragment, &version_id, part_index).await?;
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
        if let Err(err) = persist_missing_receipt_from_disk(
            &path_manager,
            &fragment,
            &version_id,
            &manifest,
            part_index,
        )
        .await
        {
            warn!(
                "Multipart resume for {}/{} part {} could not refresh compact receipt: {}",
                fragment, version_id, part_index, err
            );
        }
        if let Some(guard) = io_guard {
            guard.complete();
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
        &manifest.object_key,
        &version_id,
        part_index,
        content_length,
        manifest_data_shards(&manifest),
        manifest_parity_shards(&manifest),
        manifest.stripe_size_bytes,
        Some(&manifest.placement_roots),
        body,
        expected_checksum,
        strict_durability,
        io_progress.as_ref(),
    )
    .await?;

    if index::load_session(&fragment, &version_id).await?.is_none() {
        cleanup_blob_version_dirs_for_version(
            &path_manager,
            &fragment,
            &manifest.object_key,
            &version_id,
        )
        .await;
        return Ok((StatusCode::CONFLICT, "Upload session was cancelled").into_response());
    }

    if let Err(err) =
        index::commit_session_part_receipt(&fragment, &version_id, &manifest, &part_meta).await
    {
        warn!(
            "Failed to persist compact multipart receipt for {}/{} part {}: {}",
            fragment, version_id, part_index, err
        );
    }

    if index::load_session(&fragment, &version_id).await?.is_none() {
        cleanup_blob_version_dirs_for_version(
            &path_manager,
            &fragment,
            &manifest.object_key,
            &version_id,
        )
        .await;
        return Ok((StatusCode::CONFLICT, "Upload session was cancelled").into_response());
    }

    if let Some(guard) = io_guard {
        guard.complete();
    }
    Ok((StatusCode::OK, "Part uploaded").into_response())
}

pub async fn object_upload_part_direct(
    claims: Claims,
    fragment: String,
    version_id: String,
    part_index: u64,
    query: UploadPartQuery,
    body: Vec<u8>,
) -> Result<(), AppError> {
    let content_length = body.len() as u64;
    object_upload_part_body_direct(
        claims,
        fragment,
        version_id,
        part_index,
        query,
        Body::from(body),
        content_length,
    )
    .await
}

pub async fn object_upload_part_body_direct(
    claims: Claims,
    fragment: String,
    version_id: String,
    part_index: u64,
    query: UploadPartQuery,
    body: Body,
    content_length: u64,
) -> Result<(), AppError> {
    let body_stream = body.into_data_stream().map(|chunk_result| {
        chunk_result.map_err(|e| {
            AppError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e),
                "stream read".into(),
            )
        })
    });
    object_upload_part_stream_direct(
        claims,
        fragment,
        version_id,
        part_index,
        query,
        body_stream,
        content_length,
    )
    .await
}

pub async fn object_upload_part_stream_direct<S>(
    claims: Claims,
    fragment: String,
    version_id: String,
    part_index: u64,
    query: UploadPartQuery,
    body_stream: S,
    content_length: u64,
) -> Result<(), AppError>
where
    S: Stream<Item = Result<Bytes, AppError>> + Unpin,
{
    ensure_user_mutations_allowed()?;
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let durability_mode = parse_upload_durability_mode(&query);
    let strict_durability = durability_mode.strict();
    let max_part_size = MAX_PART_SIZE_ATOMIC.get().unwrap().load(Ordering::Relaxed);
    let gate_context = format!(
        "fragment={}, version={}, part={}",
        fragment, version_id, part_index
    );
    let io_guard =
        acquire_upload_io_guard_for_size(query.io_gate, &fragment, &gate_context, content_length)
            .await?;
    let io_progress = io_guard.as_ref().map(AdaptiveIoGuard::progress_handle);

    if content_length > max_part_size {
        return Err(AppError::PayloadTooLarge("Part too large".into()));
    }

    let path_manager = ObjectPathManager::new();
    let manifest = read_manifest_any(&fragment, &version_id)
        .await?
        .ok_or(AppError::BadRequest("Upload session not found".into()))?;
    if part_index >= manifest.expected_parts {
        // Validate before admitting activity for this version.
        return Err(AppError::BadRequest("Part index out of range".into()));
    }
    let _upload_activity = UploadActivityGuard::begin(&fragment, &manifest.object_key)?;
    let _part_lock = acquire_multipart_part_lock(&fragment, &version_id, part_index).await?;
    let expected_size = expected_part_size_bytes(&manifest, part_index);
    let expected_checksum = query
        .checksum
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    if part_exists_on_disk(&path_manager, &manifest, part_index).await? {
        if let Some(expected_size) = expected_size {
            if content_length != expected_size {
                return Err(AppError::Conflict(
                    "Part already uploaded but size differs".into(),
                ));
            }
        }
        if expected_checksum.is_some() {
            warn!(
                "Multipart resume for {}/{} part {} skipped checksum revalidation because session part metadata is not persisted",
                fragment, version_id, part_index
            );
        }
        if let Err(err) = persist_missing_receipt_from_disk(
            &path_manager,
            &fragment,
            &version_id,
            &manifest,
            part_index,
        )
        .await
        {
            warn!(
                "Multipart resume for {}/{} part {} could not refresh compact receipt: {}",
                fragment, version_id, part_index, err
            );
        }
        if let Some(guard) = io_guard {
            guard.complete();
        }
        return Ok(());
    }
    if let Some(expected_size) = expected_size {
        if content_length != expected_size {
            return Err(AppError::BadRequest(
                "Part size does not match upload session manifest".into(),
            ));
        }
    }

    let part_meta = write_part_blob_to_disk_from_stream(
        &path_manager,
        &fragment,
        &manifest.object_key,
        &version_id,
        part_index,
        content_length,
        manifest_data_shards(&manifest),
        manifest_parity_shards(&manifest),
        manifest.stripe_size_bytes,
        Some(&manifest.placement_roots),
        body_stream,
        expected_checksum,
        strict_durability,
        io_progress.as_ref(),
    )
    .await?;

    if index::load_session(&fragment, &version_id).await?.is_none() {
        cleanup_blob_version_dirs_for_version(
            &path_manager,
            &fragment,
            &manifest.object_key,
            &version_id,
        )
        .await;
        return Err(AppError::Conflict("Upload session was cancelled".into()));
    }

    if let Err(err) =
        index::commit_session_part_receipt(&fragment, &version_id, &manifest, &part_meta).await
    {
        warn!(
            "Failed to persist compact multipart receipt for {}/{} part {}: {}",
            fragment, version_id, part_index, err
        );
    }

    if index::load_session(&fragment, &version_id).await?.is_none() {
        cleanup_blob_version_dirs_for_version(
            &path_manager,
            &fragment,
            &manifest.object_key,
            &version_id,
        )
        .await;
        return Err(AppError::Conflict("Upload session was cancelled".into()));
    }

    if let Some(guard) = io_guard {
        guard.complete();
    }
    Ok(())
}

pub async fn object_upload_part_reader_direct<R>(
    claims: Claims,
    fragment: String,
    version_id: String,
    part_index: u64,
    query: UploadPartQuery,
    reader: &mut R,
    content_length: u64,
) -> Result<(), AppError>
where
    R: AsyncRead + Unpin + Send,
{
    ensure_user_mutations_allowed()?;
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let durability_mode = parse_upload_durability_mode(&query);
    let strict_durability = durability_mode.strict();
    let max_part_size = MAX_PART_SIZE_ATOMIC.get().unwrap().load(Ordering::Relaxed);
    let gate_context = format!(
        "fragment={}, version={}, part={}",
        fragment, version_id, part_index
    );
    let io_guard =
        acquire_upload_io_guard_for_size(query.io_gate, &fragment, &gate_context, content_length)
            .await?;
    let io_progress = io_guard.as_ref().map(AdaptiveIoGuard::progress_handle);

    if content_length > max_part_size {
        return Err(AppError::PayloadTooLarge("Part too large".into()));
    }

    let path_manager = ObjectPathManager::new();
    let manifest = read_manifest_any(&fragment, &version_id)
        .await?
        .ok_or(AppError::BadRequest("Upload session not found".into()))?;
    if part_index >= manifest.expected_parts {
        // Validate before admitting activity for this version.
        return Err(AppError::BadRequest("Part index out of range".into()));
    }
    let _upload_activity = UploadActivityGuard::begin(&fragment, &manifest.object_key)?;
    let _part_lock = acquire_multipart_part_lock(&fragment, &version_id, part_index).await?;
    let expected_size = expected_part_size_bytes(&manifest, part_index);
    let expected_checksum = query
        .checksum
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    if part_exists_on_disk(&path_manager, &manifest, part_index).await? {
        if let Some(expected_size) = expected_size {
            if content_length != expected_size {
                return Err(AppError::Conflict(
                    "Part already uploaded but size differs".into(),
                ));
            }
        }
        if expected_checksum.is_some() {
            warn!(
                "Multipart resume for {}/{} part {} skipped checksum revalidation because session part metadata is not persisted",
                fragment, version_id, part_index
            );
        }
        if let Err(err) = persist_missing_receipt_from_disk(
            &path_manager,
            &fragment,
            &version_id,
            &manifest,
            part_index,
        )
        .await
        {
            warn!(
                "Multipart resume for {}/{} part {} could not refresh compact receipt: {}",
                fragment, version_id, part_index, err
            );
        }
        if let Some(guard) = io_guard {
            guard.complete();
        }
        return Ok(());
    }
    if let Some(expected_size) = expected_size {
        if content_length != expected_size {
            return Err(AppError::BadRequest(
                "Part size does not match upload session manifest".into(),
            ));
        }
    }

    let part_meta = write_part_blob_to_disk_from_reader(
        &path_manager,
        &fragment,
        &manifest.object_key,
        &version_id,
        part_index,
        content_length,
        manifest_data_shards(&manifest),
        manifest_parity_shards(&manifest),
        manifest.stripe_size_bytes,
        Some(&manifest.placement_roots),
        reader,
        expected_checksum,
        strict_durability,
        io_progress.as_ref(),
    )
    .await?;

    if index::load_session(&fragment, &version_id).await?.is_none() {
        cleanup_blob_version_dirs_for_version(
            &path_manager,
            &fragment,
            &manifest.object_key,
            &version_id,
        )
        .await;
        return Err(AppError::Conflict("Upload session was cancelled".into()));
    }

    if let Err(err) =
        index::commit_session_part_receipt(&fragment, &version_id, &manifest, &part_meta).await
    {
        warn!(
            "Failed to persist compact multipart receipt for {}/{} part {}: {}",
            fragment, version_id, part_index, err
        );
    }

    if index::load_session(&fragment, &version_id).await?.is_none() {
        cleanup_blob_version_dirs_for_version(
            &path_manager,
            &fragment,
            &manifest.object_key,
            &version_id,
        )
        .await;
        return Err(AppError::Conflict("Upload session was cancelled".into()));
    }

    if let Some(guard) = io_guard {
        guard.complete();
    }
    Ok(())
}

async fn upload_object_one_shot_inner(
    State(_state): State<Arc<AppState>>,
    claims: Claims,
    headers: HeaderMap,
    AxumPath((fragment, object_key)): AxumPath<(String, String)>,
    Query(query): Query<UploadPartQuery>,
    body: Body,
    acquire_io_gate: bool,
) -> Result<Response, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let durability_mode = parse_upload_durability_mode(&query);
    let strict_durability = durability_mode.strict();
    let content_length = request_content_length(&headers)?;
    let gate_context = format!("fragment={}, object_key={}", fragment, object_key);
    let io_guard = if acquire_io_gate {
        acquire_upload_io_guard_for_size(query.io_gate, &fragment, &gate_context, content_length)
            .await?
    } else {
        None
    };
    let io_progress = io_guard.as_ref().map(AdaptiveIoGuard::progress_handle);
    let max_part_size = MAX_PART_SIZE_ATOMIC.get().unwrap().load(Ordering::Relaxed);
    if content_length > max_part_size {
        return Ok((
            StatusCode::PAYLOAD_TOO_LARGE,
            "Object too large for one-shot upload",
        )
            .into_response());
    }

    let stripe_size = choose_upload_stripe_size_for_len(content_length);

    let version_id = Ulid::new().to_string();
    let _upload_activity = UploadActivityGuard::begin(&fragment, &object_key)?;
    let session_id = Ulid::new().to_string();
    let expected_checksum = query
        .checksum
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let content_type =
        request_content_type(&headers).unwrap_or_else(|| "application/octet-stream".to_string());

    let path_manager = ObjectPathManager::new();
    let (data_shards, parity_shards, placement_roots) =
        data_plane::upload_layout(&fragment, &object_key, &version_id)?;
    let created_at = Utc::now().timestamp();
    let part_meta = match write_part_blob_to_disk(
        &path_manager,
        &fragment,
        &object_key,
        &version_id,
        0,
        content_length,
        data_shards,
        parity_shards,
        stripe_size,
        Some(&placement_roots),
        body,
        expected_checksum,
        strict_durability,
        io_progress.as_ref(),
    )
    .await
    {
        Ok(part_meta) => part_meta,
        Err(err) => {
            cleanup_blob_version_dirs_for_version(
                &path_manager,
                &fragment,
                &object_key,
                &version_id,
            )
            .await;
            return Err(err);
        }
    };

    let object_meta = ObjectMetadata {
        object_key: object_key.clone(),
        fragment: fragment.clone(),
        version_id: version_id.clone(),
        upload_session_id: session_id,
        owner_uid: fragment.clone(),
        content_type,
        total_size_bytes: content_length,
        created_at,
        data_shards: data_shards as u16,
        parity_shards: parity_shards as u16,
        placement_generation: 0,
        parts: vec![part_meta],
        is_delete_marker: false,
        stripe_size_bytes: stripe_size,
    };

    let _object_lock = index::acquire_object_operation_lock(&fragment, &object_key).await?;
    let index_write_durability = if strict_durability {
        index::IndexWriteDurability::Strict
    } else {
        index::IndexWriteDurability::Relaxed
    };
    if let Err(err) =
        index::publish_object_with_durability(&object_meta, index_write_durability).await
    {
        cleanup_blob_version_dirs_for_version(&path_manager, &fragment, &object_key, &version_id)
            .await;
        return Err(err);
    }

    let response_body = serde_json::to_value(object_meta.clone().without_internal_layout())
        .map_err(|e| {
            AppError::InternalServerError(format!("serialize one-shot upload response: {}", e))
        })?;
    if let Some(guard) = io_guard {
        guard.complete();
    }
    Ok((StatusCode::CREATED, AxumJson(response_body)).into_response())
}

pub async fn upload_object_one_shot(
    state: State<Arc<AppState>>,
    claims: Claims,
    headers: HeaderMap,
    path: AxumPath<(String, String)>,
    query: Query<UploadPartQuery>,
    body: Body,
) -> Result<impl IntoResponse, AppError> {
    upload_object_one_shot_inner(state, claims, headers, path, query, body, true).await
}

pub async fn upload_object_one_shot_direct(
    claims: Claims,
    fragment: String,
    object_key: String,
    query: UploadPartQuery,
    content_type: Option<String>,
    body: Vec<u8>,
    acquire_io_gate: bool,
) -> Result<ObjectMetadata, AppError> {
    let content_length = body.len() as u64;
    upload_object_one_shot_body_direct(
        claims,
        fragment,
        object_key,
        query,
        content_type,
        Body::from(body),
        content_length,
        acquire_io_gate,
    )
    .await
}

pub async fn upload_object_one_shot_body_direct(
    claims: Claims,
    fragment: String,
    object_key: String,
    query: UploadPartQuery,
    content_type: Option<String>,
    body: Body,
    content_length: u64,
    acquire_io_gate: bool,
) -> Result<ObjectMetadata, AppError> {
    let body_stream = body.into_data_stream().map(|chunk_result| {
        chunk_result.map_err(|e| {
            AppError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e),
                "stream read".into(),
            )
        })
    });
    upload_object_one_shot_stream_direct(
        claims,
        fragment,
        object_key,
        query,
        content_type,
        body_stream,
        content_length,
        acquire_io_gate,
    )
    .await
}

pub async fn upload_object_one_shot_stream_direct<S>(
    claims: Claims,
    fragment: String,
    object_key: String,
    query: UploadPartQuery,
    content_type: Option<String>,
    body_stream: S,
    content_length: u64,
    acquire_io_gate: bool,
) -> Result<ObjectMetadata, AppError>
where
    S: Stream<Item = Result<Bytes, AppError>> + Unpin,
{
    ensure_user_mutations_allowed()?;
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let durability_mode = parse_upload_durability_mode(&query);
    let strict_durability = durability_mode.strict();
    let gate_context = format!("fragment={}, object_key={}", fragment, object_key);
    let io_guard = if acquire_io_gate {
        acquire_upload_io_guard_for_size(query.io_gate, &fragment, &gate_context, content_length)
            .await?
    } else {
        None
    };
    let io_progress = io_guard.as_ref().map(AdaptiveIoGuard::progress_handle);
    let max_part_size = MAX_PART_SIZE_ATOMIC.get().unwrap().load(Ordering::Relaxed);
    if content_length > max_part_size {
        return Err(AppError::PayloadTooLarge(
            "Object too large for one-shot upload".into(),
        ));
    }

    let stripe_size = choose_upload_stripe_size_for_len(content_length);

    let version_id = Ulid::new().to_string();
    let _upload_activity = UploadActivityGuard::begin(&fragment, &object_key)?;
    let session_id = Ulid::new().to_string();
    let expected_checksum = query
        .checksum
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let content_type = content_type.unwrap_or_else(|| "application/octet-stream".to_string());

    let path_manager = ObjectPathManager::new();
    let (data_shards, parity_shards, placement_roots) =
        data_plane::upload_layout(&fragment, &object_key, &version_id)?;
    let created_at = Utc::now().timestamp();
    let part_meta = match write_part_blob_to_disk_from_stream(
        &path_manager,
        &fragment,
        &object_key,
        &version_id,
        0,
        content_length,
        data_shards,
        parity_shards,
        stripe_size,
        Some(&placement_roots),
        body_stream,
        expected_checksum,
        strict_durability,
        io_progress.as_ref(),
    )
    .await
    {
        Ok(part_meta) => part_meta,
        Err(err) => {
            cleanup_blob_version_dirs_for_version(
                &path_manager,
                &fragment,
                &object_key,
                &version_id,
            )
            .await;
            return Err(err);
        }
    };

    let object_meta = ObjectMetadata {
        object_key: object_key.clone(),
        fragment: fragment.clone(),
        version_id: version_id.clone(),
        upload_session_id: session_id,
        owner_uid: fragment.clone(),
        content_type,
        total_size_bytes: content_length,
        created_at,
        data_shards: data_shards as u16,
        parity_shards: parity_shards as u16,
        placement_generation: 0,
        parts: vec![part_meta],
        is_delete_marker: false,
        stripe_size_bytes: stripe_size,
    };

    let _object_lock = index::acquire_object_operation_lock(&fragment, &object_key).await?;
    let index_write_durability = if strict_durability {
        index::IndexWriteDurability::Strict
    } else {
        index::IndexWriteDurability::Relaxed
    };
    if let Err(err) =
        index::publish_object_with_durability(&object_meta, index_write_durability).await
    {
        cleanup_blob_version_dirs_for_version(&path_manager, &fragment, &object_key, &version_id)
            .await;
        return Err(err);
    }

    if let Some(guard) = io_guard {
        guard.complete();
    }
    // Internal callers still need placement to publish the NCX in the exact
    // enrolled pool. HTTP/native response boundaries strip the layout only
    // after logical publication; stripping here loses the pool identity.
    Ok(object_meta)
}

pub async fn upload_object_one_shot_reader_direct<R>(
    claims: Claims,
    fragment: String,
    object_key: String,
    query: UploadPartQuery,
    content_type: Option<String>,
    reader: &mut R,
    content_length: u64,
    acquire_io_gate: bool,
) -> Result<ObjectMetadata, AppError>
where
    R: AsyncRead + Unpin + Send,
{
    ensure_user_mutations_allowed()?;
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let durability_mode = parse_upload_durability_mode(&query);
    let strict_durability = durability_mode.strict();
    let gate_context = format!("fragment={}, object_key={}", fragment, object_key);
    let io_guard = if acquire_io_gate {
        acquire_upload_io_guard_for_size(query.io_gate, &fragment, &gate_context, content_length)
            .await?
    } else {
        None
    };
    let io_progress = io_guard.as_ref().map(AdaptiveIoGuard::progress_handle);
    let max_part_size = MAX_PART_SIZE_ATOMIC.get().unwrap().load(Ordering::Relaxed);
    if content_length > max_part_size {
        return Err(AppError::PayloadTooLarge(
            "Object too large for one-shot upload".into(),
        ));
    }

    let stripe_size = choose_upload_stripe_size_for_len(content_length);

    let version_id = Ulid::new().to_string();
    let _upload_activity = UploadActivityGuard::begin(&fragment, &object_key)?;
    let session_id = Ulid::new().to_string();
    let expected_checksum = query
        .checksum
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let content_type = content_type.unwrap_or_else(|| "application/octet-stream".to_string());

    let path_manager = ObjectPathManager::new();
    let (data_shards, parity_shards, placement_roots) =
        data_plane::upload_layout(&fragment, &object_key, &version_id)?;
    let created_at = Utc::now().timestamp();
    let part_meta = match write_part_blob_to_disk_from_reader(
        &path_manager,
        &fragment,
        &object_key,
        &version_id,
        0,
        content_length,
        data_shards,
        parity_shards,
        stripe_size,
        Some(&placement_roots),
        reader,
        expected_checksum,
        strict_durability,
        io_progress.as_ref(),
    )
    .await
    {
        Ok(part_meta) => part_meta,
        Err(err) => {
            cleanup_blob_version_dirs_for_version(
                &path_manager,
                &fragment,
                &object_key,
                &version_id,
            )
            .await;
            return Err(err);
        }
    };

    let object_meta = ObjectMetadata {
        object_key: object_key.clone(),
        fragment: fragment.clone(),
        version_id: version_id.clone(),
        upload_session_id: session_id,
        owner_uid: fragment.clone(),
        content_type,
        total_size_bytes: content_length,
        created_at,
        data_shards: data_shards as u16,
        parity_shards: parity_shards as u16,
        placement_generation: 0,
        parts: vec![part_meta],
        is_delete_marker: false,
        stripe_size_bytes: stripe_size,
    };

    let _object_lock = index::acquire_object_operation_lock(&fragment, &object_key).await?;
    let index_write_durability = if strict_durability {
        index::IndexWriteDurability::Strict
    } else {
        index::IndexWriteDurability::Relaxed
    };
    if let Err(err) =
        index::publish_object_with_durability(&object_meta, index_write_durability).await
    {
        cleanup_blob_version_dirs_for_version(&path_manager, &fragment, &object_key, &version_id)
            .await;
        return Err(err);
    }

    if let Some(guard) = io_guard {
        guard.complete();
    }
    // Preserve placement until logical publication (see the stream variant).
    Ok(object_meta)
}

pub async fn finalize_object_upload(
    State(_state): State<Arc<AppState>>,
    claims: Claims,
    AxumPath((fragment, version_id, session_id)): AxumPath<(String, String, String)>,
    Query(key_query): Query<ObjectKeyQuery>,
    AxumJson(params): AxumJson<InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    match finalize_object_upload_direct(
        claims,
        fragment,
        version_id,
        session_id,
        key_query.object_key,
        params,
    )
    .await?
    {
        FinalizeUploadResult::Complete(object_meta) => {
            let response_body = serde_json::to_value(object_meta.without_internal_layout())
                .map_err(|e| {
                    AppError::InternalServerError(format!(
                        "serialize finalized upload response: {}",
                        e
                    ))
                })?;
            Ok((StatusCode::OK, AxumJson(response_body)).into_response())
        }
        FinalizeUploadResult::Incomplete(incomplete) => {
            Ok((StatusCode::CONFLICT, AxumJson(incomplete)).into_response())
        }
    }
}

pub async fn finalize_object_upload_direct(
    claims: Claims,
    fragment: String,
    version_id: String,
    session_id: String,
    object_key: String,
    params: InitiateUploadParams,
) -> Result<FinalizeUploadResult, AppError> {
    ensure_user_mutations_allowed()?;
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let path_manager = ObjectPathManager::new();
    let _object_lock = index::acquire_object_operation_lock(&fragment, &object_key).await?;

    let _upload_activity = UploadActivityGuard::begin(&fragment, &object_key)?;

    if let Ok(existing) = index::read_object_version(&fragment, &object_key, &version_id).await {
        return Ok(FinalizeUploadResult::Complete(existing));
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
    let required_data_shards = manifest
        .as_ref()
        .map(manifest_data_shards)
        .unwrap_or_else(configured_data_shards);
    let shard_dir_checks = blob_roots
        .iter()
        .map(|root| path_manager.get_shard_dir_on_disk(root, &fragment, &object_key, &version_id));
    let found_data_roots = join_all(shard_dir_checks.map(tokio_fs::try_exists))
        .await
        .into_iter()
        .filter(|result| matches!(result, Ok(true)))
        .count();

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
        let validation_concurrency = (expected_parts as usize).clamp(1, 8);
        let path_manager_ref = &path_manager;
        let fragment_ref = fragment.as_str();
        let version_id_ref = version_id.as_str();
        let resolved_parts = futures_util::stream::iter(0..expected_parts)
            .map(|i| {
                let receipt = receipts
                    .as_ref()
                    .and_then(|items| items.get(i as usize))
                    .and_then(|entry| entry.clone());
                async move {
                    let mut resolved = None;

                    if let Some(receipt) = receipt.as_ref() {
                        resolved = reconstruct_part_meta_from_receipt(
                            path_manager_ref,
                            m,
                            receipt,
                        )
                        .await?;
                    }

                    if resolved.is_none() && m.part_size_bytes > 0 {
                        resolved =
                            reconstruct_part_meta_from_disk(path_manager_ref, m, i).await?;
                    }

                    if resolved.is_none() {
                        resolved = read_part_meta_any(fragment_ref, version_id_ref, i).await;
                    }

                    if resolved.is_some() && !part_exists_on_disk(path_manager_ref, m, i).await? {
                        warn!(
                            "Finalization deferred for {}/{} part {} because its complete recorded EC shard set is not present",
                            fragment_ref, version_id_ref, i
                        );
                        resolved = None;
                    }

                    Ok::<_, AppError>((i, resolved))
                }
            })
            .buffer_unordered(validation_concurrency)
            .collect::<Vec<_>>()
            .await;

        for result in resolved_parts {
            let (i, resolved) = result?;
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
        return Ok(FinalizeUploadResult::Incomplete(
            FinalizeIncompleteResponse {
                uploaded_parts: present.into_iter().collect(),
                missing_parts: missing,
                expected_parts,
            },
        ));
    }

    parts.sort_by_key(|p| p.part_index);

    let mut total_size_calculated = 0u64;
    for part in &parts {
        total_size_calculated += part.size_bytes;
    }

    let stripe_size = manifest
        .as_ref()
        .map(|m| m.stripe_size_bytes)
        .unwrap_or_else(configured_block_size);

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
        data_shards: manifest
            .as_ref()
            .map(manifest_data_shards)
            .unwrap_or_else(configured_data_shards) as u16,
        parity_shards: manifest
            .as_ref()
            .map(manifest_parity_shards)
            .unwrap_or_else(configured_parity_shards) as u16,
        placement_generation: 0,
        parts,
        is_delete_marker: false,
        stripe_size_bytes: stripe_size,
    };

    index::publish_object(&object_meta).await?;

    // NDX publication is not the logical namespace commit. The HTTP/native
    // caller removes this durable session only after that commit succeeds.
    // Keeping its manifest and binding also makes a cancelled handoff retryable.

    Ok(FinalizeUploadResult::Complete(object_meta))
}

pub async fn cancel_upload_session(
    claims: Claims,
    AxumPath((fragment, version_id)): AxumPath<(String, String)>,
    Query(key_query): Query<ObjectKeyQuery>,
) -> Result<impl IntoResponse, AppError> {
    cancel_upload_session_direct(claims, fragment, version_id, key_query.object_key).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn cancel_upload_session_direct(
    claims: Claims,
    fragment: String,
    version_id: String,
    object_key_hint: String,
) -> Result<(), AppError> {
    ensure_user_mutations_allowed()?;
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let path_manager = ObjectPathManager::new();
    let session = index::load_session(&fragment, &version_id).await?;
    let object_key = session
        .as_ref()
        .map(|state| state.manifest.object_key.clone())
        .filter(|key| !key.is_empty())
        .unwrap_or_else(|| {
            if object_key_hint.is_empty() {
                version_id.clone()
            } else {
                object_key_hint
            }
        });
    let _object_lock = index::acquire_object_operation_lock(&fragment, &object_key).await?;

    if index::read_object_version(&fragment, &object_key, &version_id)
        .await
        .is_ok()
    {
        return Err(AppError::Conflict(
            "Cannot cancel: Object is already finalized.".to_string(),
        ));
    }

    cleanup_blob_tmp_attempt_files_for_version(&path_manager, &fragment, &object_key, &version_id)
        .await;
    let _ = index::remove_session(&fragment, &version_id, "cancelled").await;
    cleanup_blob_version_dirs_for_version(&path_manager, &fragment, &object_key, &version_id).await;

    Ok(())
}
