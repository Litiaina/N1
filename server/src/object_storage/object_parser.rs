use crate::enums::app_error_enum::AppError;
use crate::object_storage::header_parser::parse_range_header;
use crate::object_storage::recovery::read_or_recover_chunk;
use crate::structs::object_storage_structs::{
    ObjectMetadata, ObjectPathManager, PartMetadata, ShardInfo,
};
use axum::body::Body;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures_util::stream;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs::File;

pub async fn get_object_logic(
    metadata: ObjectMetadata,
    req: axum::http::Request<Body>,
) -> Result<impl IntoResponse, AppError> {
    let (parts, _body) = req.into_parts();
    stream_object_logic(parts.headers, metadata, "inline").await
}

fn data_shards_count(part: &PartMetadata) -> usize {
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

pub async fn stream_object_logic(
    headers: HeaderMap,
    metadata: ObjectMetadata,
    disposition_type: &str,
) -> Result<impl IntoResponse, AppError> {
    let file_size = metadata.total_size_bytes;

    let ObjectMetadata {
        content_type,
        parts,
        owner_uid,
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

    let parts: Vec<std::sync::Arc<PartMetadata>> =
        parts.into_iter().map(std::sync::Arc::new).collect();

    let disk_shard_dirs: Vec<PathBuf> = blob_roots
        .iter()
        .map(|disk_root| {
            disk_root
                .join(crate::object_storage::constants::OBJECT_DATA_ROOT)
                .join(&owner_uid)
                .join(&fragment)
                .join("blobs")
                .join(&version_id)
        })
        .collect();

    struct StreamState {
        pos: u64,
        end: u64,
        parts: Vec<std::sync::Arc<PartMetadata>>,
        disk_shard_dirs: Vec<PathBuf>,
        file_cache: HashMap<String, File>,
        current_part_idx: usize,
        current_part_offset: u64,
        stripe_size: usize,
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

    let initial_state = StreamState {
        pos: start,
        end,
        parts,
        disk_shard_dirs,
        file_cache: HashMap::new(),
        current_part_idx: start_part_index,
        current_part_offset: start_part_offset,
        stripe_size: stripe_size_bytes,
    };

    let stream = stream::unfold(initial_state, move |mut state| async move {
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

            let dcount = data_shards_count(part.as_ref());
            if dcount == 0 {
                return Some((
                    Err(AppError::InternalServerError("No data shards".into())),
                    state,
                ));
            }

            let relative_pos = state.pos - state.current_part_offset;

            let stripe_block_size = state.stripe_size as u64;
            let stripe_size = stripe_block_size * (dcount as u64);

            let stripe_index = relative_pos / stripe_size;
            let offset_in_stripe = relative_pos % stripe_size;

            let shard_ordinal = (offset_in_stripe / stripe_block_size) as usize;
            let offset_in_shard_block = offset_in_stripe % stripe_block_size;
            let file_offset = (stripe_index * stripe_block_size) + offset_in_shard_block;

            let target_shard = match nth_data_shard(part.as_ref(), shard_ordinal) {
                Some(s) => s,
                None => {
                    return Some((
                        Err(AppError::InternalServerError(
                            "Shard index out of bounds".into(),
                        )),
                        state,
                    ));
                }
            };

            let shard_index = target_shard.shard_index;

            let bytes_remaining_in_block = stripe_block_size - offset_in_shard_block;
            let bytes_remaining_in_req = state.end - state.pos + 1;
            let bytes_remaining_in_part = part.size_bytes - relative_pos;

            let to_read = bytes_remaining_in_block
                .min(bytes_remaining_in_req)
                .min(bytes_remaining_in_part) as usize;

            if to_read == 0 {
                return None;
            }

            let num_disks = state.disk_shard_dirs.len();
            let disk_idx = (part.part_index as usize + shard_index) % num_disks;
            let shard_dir = &state.disk_shard_dirs[disk_idx];

            let data_result = read_or_recover_chunk(
                &mut state.file_cache,
                shard_dir,
                part.clone(),
                shard_index,
                file_offset,
                to_read,
                state.stripe_size,
            )
            .await;

            match data_result {
                Ok(bytes) => {
                    state.pos += to_read as u64;
                    return Some((Ok(Bytes::from(bytes)), state));
                }
                Err(e) => return Some((Err(e), state)),
            }
        }

        None
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
