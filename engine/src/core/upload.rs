use std::collections::{BTreeSet, HashMap};
use std::fs::{self, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::sync::{LazyLock, Mutex};
use std::task::{Context, Poll};

use axum::Json as AxumJson;
use axum::body::Body;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use bytes::Bytes;
use chrono::Utc;
use futures_util::Stream;
use futures_util::StreamExt;
use futures_util::future::join_all;
use once_cell::sync::{Lazy, OnceCell};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::fs as tokio_fs;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use tracing::{error, info, warn};
use ulid::Ulid;
use walkdir::WalkDir;

use crate::core::checksum::format_sha256;

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

use crate::core::capacity_manager::{
    ensure_blob_placement_capacity_available_for_parts, ensure_capacity_available,
};
use crate::core::constants::{PARITY_SUFFIX, SHARD_SUFFIX};
use crate::core::disk_manager::{
    FinalizeIncompleteResponse, InitiateUploadParams, InitiateUploadResponse, ObjectKeyQuery,
    ObjectMetadata, ObjectPathManager, PartMetadata, ShardInfo, UploadPartQuery,
    UploadStatusResponse, default_stripe_size,
};
use crate::core::ec::{AlignedBuffer, ErasureStreamWriter};
use crate::core::index;
use crate::core::io_scheduler::{DiskIoClass, DiskIoGuard, acquire_disk_io};
use crate::distribution::membership::active_cluster_runtime;
use crate::enums::app_error_enum::AppError;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;

#[path = "upload/io.rs"]
mod direct_io;
#[path = "upload/finalization.rs"]
mod finalization;
#[path = "upload/one_shot.rs"]
mod one_shot;
#[path = "upload/part_writer.rs"]
mod part_writer;
#[path = "upload/planning.rs"]
mod planning;
#[path = "upload/session_api.rs"]
mod session_api;

pub use direct_io::{DirectWriter, create_direct_file, sync_dir, write_atomic};
use direct_io::{UploadShardWriter, create_buffered_file};
pub use finalization::*;
pub use one_shot::*;
use part_writer::*;
use planning::*;
pub use session_api::*;

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

fn current_pool_identity() -> Result<(String, u64), AppError> {
    let runtime = active_cluster_runtime()?;
    let pool = runtime.membership.newest_writable_pool().ok_or_else(|| {
        AppError::ServiceUnavailable("no writable N2 server pool is available".into())
    })?;
    Ok((pool.id.clone(), pool.generation))
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

#[cfg(target_os = "linux")]
static IO_URING_ENABLED: Lazy<bool> = Lazy::new(|| {
    // The current ring path constructs a short-lived ring per direct write. Keep
    // it opt-in until this writer owns a reusable ring per file/worker.
    let enabled = std::env::var("N2_IO_URING_ENABLED")
        .ok()
        .map(|v| {
            !matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "off" | "no"
            )
        })
        .unwrap_or(false);

    if !enabled {
        return false;
    }

    match IoUring::new(8) {
        Ok(_) => true,
        Err(err) => {
            warn!(
                "io_uring unavailable for N2 direct writer, falling back to O_DIRECT blocking writes: {}",
                err
            );
            false
        }
    }
});

pub(crate) struct AdaptiveIoGuard {
    _guard: DiskIoGuard,
}

async fn acquire_adaptive_io_guard(
    fragment: &str,
    context: &str,
) -> Result<AdaptiveIoGuard, AppError> {
    Ok(AdaptiveIoGuard {
        _guard: acquire_disk_io(DiskIoClass::ForegroundWrite, Some(fragment), context).await?,
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
        UPLOAD_STRICT_DURABILITY_DEFAULT_ATOMIC, UploadDurabilityMode,
        acquire_upload_io_guard_with_timeout, choose_upload_stripe_size_for_len_with_ceiling,
        parse_upload_durability_mode,
    };
    use crate::core::disk_manager::UploadPartQuery;
    use std::sync::atomic::Ordering;

    #[test]
    fn upload_durability_default_follows_runtime_config_or_batched_when_unset() {
        let q = UploadPartQuery {
            checksum: None,
            durability: None,
            strict_durability: None,
            io_gate: None,
        };
        let expected = if UPLOAD_STRICT_DURABILITY_DEFAULT_ATOMIC
            .get()
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(false)
        {
            UploadDurabilityMode::Strict
        } else {
            UploadDurabilityMode::Batched
        };
        assert_eq!(parse_upload_durability_mode(&q), expected);
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
        let guard = acquire_upload_io_guard_with_timeout(false, "unit-test-fragment", "unit-test")
            .await
            .expect("gate bypass succeeds");
        assert!(guard.is_none());
    }

    #[test]
    fn dynamic_stripe_uses_small_blocks_for_small_objects_and_configured_ceiling_for_large() {
        assert_eq!(
            choose_upload_stripe_size_for_len_with_ceiling(1, 262_144),
            4096
        );
        assert_eq!(
            choose_upload_stripe_size_for_len_with_ceiling(64 * 1024, 262_144),
            4096
        );
        assert_eq!(
            choose_upload_stripe_size_for_len_with_ceiling(64 * 1024 + 1, 262_144),
            16 * 1024
        );
        assert_eq!(
            choose_upload_stripe_size_for_len_with_ceiling(1024 * 1024 + 1, 262_144),
            262_144
        );
        assert_eq!(
            choose_upload_stripe_size_for_len_with_ceiling(1, 16 * 1024),
            4096
        );
        assert_eq!(
            choose_upload_stripe_size_for_len_with_ceiling(10 * 1024 * 1024, 16 * 1024),
            16 * 1024
        );
    }
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
    let _io_guard =
        acquire_upload_io_guard_for_size(query.io_gate, &fragment, &gate_context, content_length)
            .await?;

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
        body,
        expected_checksum,
        strict_durability,
    )
    .await?;

    if index::load_session(&fragment, &version_id).await?.is_none() {
        cleanup_blob_version_dirs_for_version(&path_manager, &fragment, &version_id).await;
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
        cleanup_blob_version_dirs_for_version(&path_manager, &fragment, &version_id).await;
        return Ok((StatusCode::CONFLICT, "Upload session was cancelled").into_response());
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
    let _io_guard =
        acquire_upload_io_guard_for_size(query.io_gate, &fragment, &gate_context, content_length)
            .await?;

    if content_length > max_part_size {
        return Err(AppError::PayloadTooLarge("Part too large".into()));
    }

    let path_manager = ObjectPathManager::new();
    let manifest = read_manifest_any(&fragment, &version_id)
        .await?
        .ok_or(AppError::BadRequest("Upload session not found".into()))?;
    if part_index >= manifest.expected_parts {
        return Err(AppError::BadRequest("Part index out of range".into()));
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
        body_stream,
        expected_checksum,
        strict_durability,
    )
    .await?;

    if index::load_session(&fragment, &version_id).await?.is_none() {
        cleanup_blob_version_dirs_for_version(&path_manager, &fragment, &version_id).await;
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
        cleanup_blob_version_dirs_for_version(&path_manager, &fragment, &version_id).await;
        return Err(AppError::Conflict("Upload session was cancelled".into()));
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
    let _io_guard =
        acquire_upload_io_guard_for_size(query.io_gate, &fragment, &gate_context, content_length)
            .await?;

    if content_length > max_part_size {
        return Err(AppError::PayloadTooLarge("Part too large".into()));
    }

    let path_manager = ObjectPathManager::new();
    let manifest = read_manifest_any(&fragment, &version_id)
        .await?
        .ok_or(AppError::BadRequest("Upload session not found".into()))?;
    if part_index >= manifest.expected_parts {
        return Err(AppError::BadRequest("Part index out of range".into()));
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
        reader,
        expected_checksum,
        strict_durability,
    )
    .await?;

    if index::load_session(&fragment, &version_id).await?.is_none() {
        cleanup_blob_version_dirs_for_version(&path_manager, &fragment, &version_id).await;
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
        cleanup_blob_version_dirs_for_version(&path_manager, &fragment, &version_id).await;
        return Err(AppError::Conflict("Upload session was cancelled".into()));
    }

    Ok(())
}
