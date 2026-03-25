use crate::core::disk_manager::{ObjectMetadata, ObjectPathManager, PartMetadata, ShardInfo};
use crate::core::header_parser::parse_range_header;
use crate::core::recovery::{ShardFileCache, ShardRepairContext, read_or_recover_chunk_into};
use crate::enums::app_error_enum::AppError;
use axum::body::Body;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures_util::stream;
use once_cell::sync::{Lazy, OnceCell};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Duration;

pub static STREAM_MAX_CONCURRENCY: OnceCell<usize> = OnceCell::new();
pub static STREAM_CHUNK_SIZE_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static ADAPTIVE_STREAM_TARGET_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static STREAM_READAHEAD_BYTES_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static STREAM_LOCALITY_BIAS_ENABLED_ATOMIC: OnceCell<AtomicBool> = OnceCell::new();

static STREAM_READ_SEMAPHORE: Lazy<Arc<Semaphore>> = Lazy::new(|| {
    Arc::new(Semaphore::new(
        *STREAM_MAX_CONCURRENCY.get().unwrap_or(&128usize),
    ))
});
static ACTIVE_STREAM_INFLIGHT: Lazy<AtomicUsize> = Lazy::new(|| AtomicUsize::new(0));

struct AdaptiveStreamGuard {
    _permit: OwnedSemaphorePermit,
}

impl Drop for AdaptiveStreamGuard {
    fn drop(&mut self) {
        ACTIVE_STREAM_INFLIGHT.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn acquire_adaptive_stream_guard() -> Result<AdaptiveStreamGuard, AppError> {
    loop {
        let target = ADAPTIVE_STREAM_TARGET_ATOMIC
            .get()
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or_else(|| *STREAM_MAX_CONCURRENCY.get().unwrap_or(&128usize))
            .max(1);
        let active = ACTIVE_STREAM_INFLIGHT.load(Ordering::Relaxed);
        if active < target {
            if ACTIVE_STREAM_INFLIGHT
                .compare_exchange(active, active + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        } else {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }

    match STREAM_READ_SEMAPHORE.clone().acquire_owned().await {
        Ok(permit) => Ok(AdaptiveStreamGuard { _permit: permit }),
        Err(_) => {
            ACTIVE_STREAM_INFLIGHT.fetch_sub(1, Ordering::Relaxed);
            Err(AppError::InternalServerError(
                "stream semaphore closed".into(),
            ))
        }
    }
}

fn stream_chunk_size_bytes() -> usize {
    STREAM_CHUNK_SIZE_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(256 * 1024)
        .clamp(4096, 4 * 1024 * 1024)
}

fn stream_readahead_bytes() -> usize {
    STREAM_READAHEAD_BYTES_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(512 * 1024)
        .clamp(64 * 1024, 8 * 1024 * 1024)
}

fn stream_locality_bias_enabled() -> bool {
    STREAM_LOCALITY_BIAS_ENABLED_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(true)
}

fn adaptive_emit_bytes() -> usize {
    let base = stream_chunk_size_bytes().min(stream_readahead_bytes());
    let target = ADAPTIVE_STREAM_TARGET_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or_else(|| *STREAM_MAX_CONCURRENCY.get().unwrap_or(&128usize))
        .max(1);
    let active = ACTIVE_STREAM_INFLIGHT.load(Ordering::Relaxed);
    let adjusted = if active >= target.saturating_mul(2) {
        base / 4
    } else if active >= target {
        base / 2
    } else if active.saturating_mul(2) >= target {
        base.saturating_mul(3) / 4
    } else {
        base
    };
    adjusted.clamp(16 * 1024, 4 * 1024 * 1024)
}

pub async fn get_object_logic(
    metadata: ObjectMetadata,
    req: axum::http::Request<Body>,
) -> Result<impl IntoResponse, AppError> {
    let (parts, _body) = req.into_parts();
    stream_object_logic(parts.headers, metadata, "inline").await
}

pub(crate) fn data_shards_count(part: &PartMetadata) -> usize {
    part.shards.iter().filter(|s| !s.is_parity).count()
}

fn nth_data_shard<'a>(part: &'a PartMetadata, nth: usize) -> Option<&'a ShardInfo> {
    let mut i = 0usize;
    for s in &part.shards {
        if s.is_parity {
            continue;
        }
        if i == nth {
            return Some(s);
        }
        i += 1;
    }
    None
}

pub(crate) fn shard_dir_with_locality_hint<'a>(
    part: &PartMetadata,
    shard_index: usize,
    shard_dirs: &'a [PathBuf],
) -> Option<&'a PathBuf> {
    if shard_dirs.is_empty() {
        return None;
    }
    if stream_locality_bias_enabled() {
        if let Some(shard) = nth_data_shard(part, shard_index) {
            let abs = PathBuf::from(&shard.absolute_path);
            if let Some(parent) = abs.parent() {
                for d in shard_dirs {
                    if parent.starts_with(d) {
                        return Some(d);
                    }
                }
            }
        }
    }
    let disk_idx = (part.part_index as usize + shard_index) % shard_dirs.len();
    shard_dirs.get(disk_idx)
}

pub(crate) fn shard_dirs_for_version(
    path_manager: &ObjectPathManager,
    blob_roots: &[PathBuf],
    fragment: &str,
    version_id: &str,
) -> Vec<PathBuf> {
    blob_roots
        .iter()
        .map(|disk| path_manager.get_shard_dir_on_disk(disk, fragment, version_id))
        .collect()
}

async fn read_chunk_with_retries(
    file_cache: &mut ShardFileCache,
    shard_dir: &PathBuf,
    part: Arc<PartMetadata>,
    shard_index: usize,
    file_offset: u64,
    to_read: usize,
    stripe_size: usize,
    repair_context: &ShardRepairContext,
    scratch: &mut Vec<u8>,
) -> Result<(), AppError> {
    let mut last_err: Option<AppError> = None;
    for attempt in 0..3usize {
        match read_or_recover_chunk_into(
            file_cache,
            shard_dir,
            Arc::clone(&part),
            shard_index,
            file_offset,
            to_read,
            stripe_size,
            Some(repair_context.clone()),
            scratch,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(e) => {
                last_err = Some(e);
                file_cache.clear();
                if attempt < 2 {
                    tokio::time::sleep(Duration::from_millis(4)).await;
                }
            }
        }
    }
    Err(last_err.unwrap_or_else(|| {
        AppError::InternalServerError("stream read failed after retries".into())
    }))
}

pub async fn stream_object_logic(
    headers: HeaderMap,
    metadata: ObjectMetadata,
    disposition_type: &str,
) -> Result<impl IntoResponse, AppError> {
    let file_size = metadata.total_size_bytes;

    let ObjectMetadata {
        content_type,
        parts,
        fragment,
        object_key,
        version_id,
        created_at,
        stripe_size_bytes,
        ..
    } = metadata;

    if file_size == 0 {
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, content_type)
            .header(header::CONTENT_LENGTH, "0")
            .body(Body::empty())
            .unwrap()
            .into_response());
    }

    let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    let parsed_range = parse_range_header(range_header, file_size);
    let (start, end) = parsed_range.unwrap_or((0, file_size.saturating_sub(1)));
    let is_explicit_range = parsed_range.is_some();

    if start >= file_size {
        return Err(AppError::RangeNotSatisfiable);
    }

    let content_length = end - start + 1;

    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    if blob_roots.is_empty() {
        return Err(AppError::InternalServerError(
            "No blob roots configured".into(),
        ));
    }

    let filename = std::path::Path::new(&object_key)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("download")
        .to_string();

    struct StreamState {
        pos: u64,
        end: u64,
        parts: Vec<Arc<PartMetadata>>,
        shard_dirs: Vec<PathBuf>,
        file_cache: ShardFileCache,
        scratch: Vec<u8>,
        current_part_idx: usize,
        current_part_offset: u64,
        stripe_size: usize,
        _stream_guard: AdaptiveStreamGuard,
    }

    let mut current_offset = 0u64;
    let mut start_part_index = 0usize;
    let mut start_part_offset = 0u64;

    for (idx, part) in parts.iter().enumerate() {
        let part_end = current_offset + part.size_bytes;
        if start >= current_offset && start < part_end {
            start_part_index = idx;
            start_part_offset = current_offset;
            break;
        }
        current_offset = part_end;
    }

    let checksum_header = if parts.len() == 1 {
        parts[0].checksum.clone()
    } else {
        None
    };
    let repair_context = ShardRepairContext::new(
        fragment.clone(),
        object_key.clone(),
        version_id.clone(),
        false,
    );
    let parts = parts.into_iter().map(Arc::new).collect::<Vec<_>>();
    let shard_dirs = shard_dirs_for_version(&path_manager, &blob_roots, &fragment, &version_id);

    let initial_state = StreamState {
        pos: start,
        end,
        parts,
        shard_dirs,
        file_cache: ShardFileCache::new(),
        scratch: Vec::new(),
        current_part_idx: start_part_index,
        current_part_offset: start_part_offset,
        stripe_size: stripe_size_bytes,
        _stream_guard: acquire_adaptive_stream_guard().await?,
    };

    let stream = stream::unfold(initial_state, move |mut state| {
        let repair_context = repair_context.clone();
        async move {
            if state.pos > state.end {
                return None;
            }

            while state.current_part_idx < state.parts.len() {
                let part = &state.parts[state.current_part_idx];
                let part_end_global = state.current_part_offset + part.size_bytes;

                if state.pos >= part_end_global {
                    state.current_part_idx += 1;
                    state.current_part_offset = part_end_global;
                    state.file_cache.clear();
                    continue;
                }

                let dcount = data_shards_count(part);
                if dcount == 0 {
                    return Some((
                        Err(AppError::InternalServerError("No data shards".into())),
                        state,
                    ));
                }

                let relative_pos = state.pos - state.current_part_offset;

                let stripe_block_size = state.stripe_size as u64;
                let stripe_size = stripe_block_size * (dcount as u64);

                let offset_in_stripe = relative_pos % stripe_size;

                let shard_index = (offset_in_stripe / stripe_block_size) as usize;
                if nth_data_shard(part, shard_index).is_none() {
                    return Some((
                        Err(AppError::InternalServerError(
                            "Shard index out of bounds".into(),
                        )),
                        state,
                    ));
                }

                let bytes_remaining_in_req_total = state.end - state.pos + 1;
                let bytes_remaining_in_part_total = part.size_bytes - relative_pos;
                let max_emit = adaptive_emit_bytes() as u64;
                let mut budget = bytes_remaining_in_req_total
                    .min(bytes_remaining_in_part_total)
                    .min(max_emit) as usize;
                if budget == 0 {
                    return None;
                }

                let mut local_pos = state.pos;
                let mut out = Vec::with_capacity(budget);

                while budget > 0 && local_pos <= state.end {
                    let rel = local_pos - state.current_part_offset;
                    if rel >= part.size_bytes {
                        break;
                    }

                    let stripe_block_size = state.stripe_size as u64;
                    let stripe_size = stripe_block_size * (dcount as u64);
                    let stripe_index = rel / stripe_size;
                    let offset_in_stripe = rel % stripe_size;
                    let shard_index = (offset_in_stripe / stripe_block_size) as usize;
                    let offset_in_shard_block = offset_in_stripe % stripe_block_size;
                    let file_offset = (stripe_index * stripe_block_size) + offset_in_shard_block;

                    if nth_data_shard(part, shard_index).is_none() {
                        return Some((
                            Err(AppError::InternalServerError(
                                "Shard index out of bounds".into(),
                            )),
                            state,
                        ));
                    }

                    let bytes_remaining_in_block = stripe_block_size - offset_in_shard_block;
                    let bytes_remaining_in_req = state.end - local_pos + 1;
                    let bytes_remaining_in_part = part.size_bytes - rel;
                    let to_read = bytes_remaining_in_block
                        .min(bytes_remaining_in_req)
                        .min(bytes_remaining_in_part)
                        .min(budget as u64) as usize;

                    if to_read == 0 {
                        break;
                    }

                    let Some(shard_dir) =
                        shard_dir_with_locality_hint(part, shard_index, &state.shard_dirs)
                    else {
                        return Some((
                            Err(AppError::InternalServerError(
                                "No shard dirs configured".into(),
                            )),
                            state,
                        ));
                    };

                    let data_result = read_chunk_with_retries(
                        &mut state.file_cache,
                        shard_dir,
                        Arc::clone(part),
                        shard_index,
                        file_offset,
                        to_read,
                        state.stripe_size,
                        &repair_context,
                        &mut state.scratch,
                    )
                    .await;

                    match data_result {
                        Ok(()) => {
                            out.extend_from_slice(&state.scratch);
                            local_pos += to_read as u64;
                            budget = budget.saturating_sub(to_read);
                        }
                        Err(e) => return Some((Err(e), state)),
                    }
                }

                if out.is_empty() {
                    return None;
                }

                state.pos = local_pos;
                return Some((Ok(Bytes::from(out)), state));
            }

            None
        }
    });

    let body = Body::from_stream(stream);

    let etag = format!("\"{}\"", version_id);
    let last_modified = Utc
        .timestamp_opt(created_at, 0)
        .single()
        .map(|dt| dt.to_rfc2822())
        .unwrap_or_else(|| "Thu, 01 Jan 1970 00:00:00 GMT".to_string());

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::ETAG, etag)
        .header(header::LAST_MODIFIED, last_modified)
        .header(
            header::CONTENT_DISPOSITION,
            format!("{}; filename=\"{}\"", disposition_type, filename),
        )
        .header(header::CACHE_CONTROL, "public, max-age=3600");

    if let Some(crc) = checksum_header {
        builder = builder.header("x-litiaina-checksum", crc);
    }

    if is_explicit_range || content_length < file_size {
        builder = builder
            .status(StatusCode::PARTIAL_CONTENT)
            .header(
                header::CONTENT_RANGE,
                format!("bytes {}-{}/{}", start, end, file_size),
            )
            .header(header::CONTENT_LENGTH, content_length.to_string());
    } else {
        builder = builder.header(header::CONTENT_LENGTH, file_size.to_string());
    }

    Ok(builder.body(body).unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locality_hint_falls_back_when_no_match() {
        let part = PartMetadata {
            part_index: 0,
            size_bytes: 1,
            created_at: 0,
            checksum: None,
            shards: vec![ShardInfo {
                file_name: "a.shard".into(),
                absolute_path: "/missing/root/a.shard".into(),
                raw_blob_address: None,
                checksum: "x".into(),
                size_bytes: 1,
                shard_index: 0,
                is_parity: false,
            }],
        };
        let dirs = vec![PathBuf::from("/d0"), PathBuf::from("/d1")];
        let got = shard_dir_with_locality_hint(&part, 0, &dirs).expect("dir");
        assert_eq!(got, &PathBuf::from("/d0"));
    }

    #[test]
    fn locality_hint_prefers_matching_root() {
        let part = PartMetadata {
            part_index: 1,
            size_bytes: 1,
            created_at: 0,
            checksum: None,
            shards: vec![ShardInfo {
                file_name: "a.shard".into(),
                absolute_path: "/d1/frag/blob/ver/a.shard".into(),
                raw_blob_address: None,
                checksum: "x".into(),
                size_bytes: 1,
                shard_index: 0,
                is_parity: false,
            }],
        };
        let dirs = vec![PathBuf::from("/d0"), PathBuf::from("/d1")];
        let got = shard_dir_with_locality_hint(&part, 0, &dirs).expect("dir");
        assert_eq!(got, &PathBuf::from("/d1"));
    }
}
