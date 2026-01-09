use crate::enums::app_error_enum::AppError;
use crate::object_storage::ec::STRIPE_BLOCK_SIZE;
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
use std::sync::Arc;
use tokio::fs::File;

pub const STREAM_BATCH_SIZE: usize = STRIPE_BLOCK_SIZE;

pub async fn get_object_logic(
    metadata: ObjectMetadata,
    req: axum::http::Request<Body>,
) -> Result<impl IntoResponse, AppError> {
    let (parts, _body) = req.into_parts();
    stream_object_logic(parts.headers, metadata, "inline").await
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
    let version_dir = path_manager.get_blob_dir(&owner_uid, &fragment, &version_id);

    let filename = std::path::Path::new(&object_key)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("download")
        .to_string();

    let parts_arc: Vec<Arc<PartMetadata>> = parts.into_iter().map(Arc::new).collect();

    struct StreamState {
        pos: u64,
        end: u64,
        parts: Vec<Arc<PartMetadata>>,
        version_dir: PathBuf,
        file_cache: HashMap<String, File>,
        current_part_idx: usize,
        current_part_offset: u64,
    }

    let mut current_offset = 0;
    let mut start_part_index = 0;
    let mut start_part_offset = 0;

    for (idx, part) in parts_arc.iter().enumerate() {
        let part_end = current_offset + part.size_bytes;
        if start >= current_offset && start < part_end {
            start_part_index = idx;
            start_part_offset = current_offset;
            break;
        }
        current_offset = part_end;
    }

    let checksum_header = if parts_arc.len() == 1 {
        parts_arc[0].checksum.clone()
    } else {
        None
    };

    let initial_state = StreamState {
        pos: start,
        end,
        parts: parts_arc,
        version_dir,
        file_cache: HashMap::new(),
        current_part_idx: start_part_index,
        current_part_offset: start_part_offset,
    };

    let stream = stream::unfold(initial_state, move |mut state| async move {
        let mut batch_buffer = Vec::with_capacity(STREAM_BATCH_SIZE);

        loop {
            if state.pos > state.end {
                break;
            }

            if batch_buffer.len() >= STREAM_BATCH_SIZE {
                break;
            }

            if state.current_part_idx >= state.parts.len() {
                break;
            }

            let mut part = &state.parts[state.current_part_idx];
            let part_end_global = state.current_part_offset + part.size_bytes;

            if state.pos >= part_end_global {
                state.current_part_idx += 1;
                if state.current_part_idx >= state.parts.len() {
                    break;
                }
                state.current_part_offset = part_end_global;
                state.file_cache.clear();
                part = &state.parts[state.current_part_idx];
            }

            let data_shards_info: Vec<&ShardInfo> =
                part.shards.iter().filter(|s| !s.is_parity).collect();

            let data_shards_count = data_shards_info.len() as u64;
            if data_shards_count == 0 {
                return Some((
                    Err(AppError::InternalServerError("No data shards".into())),
                    state,
                ));
            }

            let relative_pos = state.pos - state.current_part_offset;

            let stripe_block_size = STRIPE_BLOCK_SIZE as u64;
            let stripe_size = stripe_block_size * data_shards_count;

            let stripe_index = relative_pos / stripe_size;
            let offset_in_stripe = relative_pos % stripe_size;

            let shard_index = (offset_in_stripe / stripe_block_size) as usize;
            let offset_in_shard_block = offset_in_stripe % stripe_block_size;
            let file_offset = (stripe_index * stripe_block_size) + offset_in_shard_block;

            let target_shard_info = data_shards_info.get(shard_index);

            if target_shard_info.is_none() {
                return Some((
                    Err(AppError::InternalServerError(
                        "Shard index out of bounds".into(),
                    )),
                    state,
                ));
            }

            let bytes_remaining_in_block = stripe_block_size - offset_in_shard_block;
            let bytes_remaining_in_req = state.end - state.pos + 1;
            let bytes_remaining_in_part = part.size_bytes - relative_pos;
            let bytes_remaining_in_buffer = (STREAM_BATCH_SIZE - batch_buffer.len()) as u64;

            let to_read = bytes_remaining_in_block
                .min(bytes_remaining_in_req)
                .min(bytes_remaining_in_part)
                .min(bytes_remaining_in_buffer) as usize;

            if to_read == 0 {
                break;
            }

            let part_arc = Arc::clone(part);
            let data_result = read_or_recover_chunk(
                &mut state.file_cache,
                &state.version_dir,
                part_arc,
                shard_index,
                file_offset,
                to_read,
            )
            .await;

            match data_result {
                Ok(bytes) => {
                    batch_buffer.extend_from_slice(&bytes);
                    state.pos += to_read as u64;
                }
                Err(e) => return Some((Err(e), state)),
            }
        }

        if batch_buffer.is_empty() {
            return None;
        }

        Some((Ok(Bytes::from(batch_buffer)), state))
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
