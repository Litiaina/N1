use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::CHUNK_SIZE;
use crate::object_storage::header_parser::parse_range_header;
use crate::structs::object_storage_structs::{ObjectMetadata, ObjectPathManager, PartMetadata, ShardInfo};
use axum::body::Body;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures_util::stream;
use reed_solomon_simd::ReedSolomonDecoder;
use std::io::{Cursor, SeekFrom};
use std::path::{Path, PathBuf};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tracing::{info, warn};

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
    let clean_stored_key = object_key.trim_start_matches('/');

    let path_manager = ObjectPathManager::new();
    
    let version_dir = path_manager.get_blob_dir(&owner_uid, &fragment, &version_id);

    let filename = std::path::Path::new(clean_stored_key)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("download")
        .to_string();

    struct StreamState {
        pos: u64,
        end: u64,
        parts: Vec<PartMetadata>,
        version_dir: PathBuf,
        
        current_file: Option<File>,
        current_shard_data: Option<Cursor<Vec<u8>>>, 
        
        current_part_index: usize,
        current_part_start_offset: u64,
        current_shard_index: usize,
        current_shard_bytes_read: u64,
    }

    let mut current_offset = 0;
    let mut start_part_index = 0;
    let mut start_part_start_offset = 0;

    for (idx, part) in parts.iter().enumerate() {
        let part_end = current_offset + part.size_bytes;
        if start >= current_offset && start < part_end {
            start_part_index = idx;
            start_part_start_offset = current_offset;
            break;
        }
        current_offset = part_end;
    }

    let checksum_header = if parts.len() == 1 {
        parts[0].checksum.clone()
    } else {
        None
    };

    let start_relative_in_part = start - start_part_start_offset;
    
    let first_part_shards: Vec<&ShardInfo> = parts[start_part_index].shards.iter().filter(|s| !s.is_parity).collect();
    let shard_size = if !first_part_shards.is_empty() { first_part_shards[0].size_bytes } else { 0 };

    let start_shard_index = if shard_size > 0 { (start_relative_in_part / shard_size) as usize } else { 0 };
    let start_offset_in_shard = if shard_size > 0 { start_relative_in_part % shard_size } else { 0 };

    let initial_state = StreamState {
        pos: start,
        end,
        parts,
        version_dir,
        current_file: None,
        current_shard_data: None,
        current_part_index: start_part_index,
        current_part_start_offset: start_part_start_offset,
        current_shard_index: start_shard_index,
        current_shard_bytes_read: start_offset_in_shard, 
    };

    let stream = stream::unfold(initial_state, move |mut state| async move {
        if state.pos > state.end {
            return None;
        }

        if state.current_part_index >= state.parts.len() {
             return None;
        }

        let mut part = &state.parts[state.current_part_index];
        let part_end_global = state.current_part_start_offset + part.size_bytes;

        if state.pos >= part_end_global {
            state.current_part_index += 1;
            if state.current_part_index >= state.parts.len() {
                return None;
            }
            state.current_part_start_offset = part_end_global;
            state.current_file = None;
            state.current_shard_data = None;
            state.current_shard_index = 0;
            state.current_shard_bytes_read = 0;
            part = &state.parts[state.current_part_index];
        }

        let mut data_shards: Vec<&ShardInfo> = part.shards.iter().filter(|s| !s.is_parity).collect();
        data_shards.sort_by_key(|s| s.shard_index);
        
        if data_shards.is_empty() {
             return Some((Err(AppError::InternalServerError("Missing shards".into())), state));
        }
        
        let current_shard_info = if state.current_shard_index < data_shards.len() {
            data_shards[state.current_shard_index]
        } else {
             state.pos = part_end_global; 
             return Some((Ok(Bytes::new()), state));
        };

        if state.current_shard_bytes_read >= current_shard_info.size_bytes {
            state.current_shard_index += 1;
            state.current_shard_bytes_read = 0;
            state.current_file = None;
            state.current_shard_data = None;
            return Some((Ok(Bytes::new()), state));
        }

        if state.current_file.is_none() && state.current_shard_data.is_none() {
            let part_path = state.version_dir.join(&current_shard_info.file_name);
            
            match File::open(&part_path).await {
                Ok(mut f) => {
                    if state.current_shard_bytes_read > 0 {
                        if let Err(e) = f.seek(SeekFrom::Start(state.current_shard_bytes_read)).await {
                            return Some((Err(AppError::Io(e, "seek shard".into())), state));
                        }
                    }
                    state.current_file = Some(f);
                }
                Err(_) => {
                    match reconstruct_shard(part, state.current_shard_index, &state.version_dir).await {
                        Ok(data) => {
                            let mut cursor = Cursor::new(data);
                             if state.current_shard_bytes_read > 0 {
                                if let Err(e) = cursor.seek(SeekFrom::Start(state.current_shard_bytes_read)).await {
                                    return Some((Err(AppError::Io(e, "seek reconstructed shard".into())), state));
                                }
                            }
                            state.current_shard_data = Some(cursor);
                        }
                        Err(e) => {
                            return Some((Err(e), state));
                        }
                    }
                }
            }
        }

        let bytes_left_in_shard = current_shard_info.size_bytes - state.current_shard_bytes_read;
        let bytes_left_in_part_global = part_end_global - state.pos;
        let bytes_left_in_req = state.end - state.pos + 1;
        
        let to_read = (CHUNK_SIZE as u64)
            .min(bytes_left_in_shard)
            .min(bytes_left_in_part_global)
            .min(bytes_left_in_req) as usize;

        if to_read == 0 {
             return Some((Ok(Bytes::new()), state));
        }

        let mut buf = vec![0u8; to_read];

        let read_result = if let Some(file) = state.current_file.as_mut() {
            file.read_exact(&mut buf).await.map_err(|e| AppError::Io(e, "read shard file".into()))
        } else if let Some(cursor) = state.current_shard_data.as_mut() {
            cursor.read_exact(&mut buf).await.map_err(|e| AppError::Io(e, "read reconstructed shard".into()))
        } else {
            Err(AppError::InternalServerError("No reader available".into()))
        };

        match read_result {
            Ok(_) => {
                let bytes = Bytes::from(buf);
                state.pos += to_read as u64;
                state.current_shard_bytes_read += to_read as u64;
                Some((Ok(bytes), state))
            }
            Err(e) => Some((Err(e), state)),
        }
    });

    let body = Body::from_stream(stream);
    
    let etag = format!("\"{}\"", version_id);
    let last_modified = Utc.timestamp_opt(created_at, 0)
        .single()
        .map(|dt| dt.to_rfc2822())
        .unwrap_or_else(|| "Thu, 01 Jan 1970 00:00:00 GMT".to_string());

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::ETAG, etag) 
        .header(header::LAST_MODIFIED, last_modified)
        .header(header::CONTENT_DISPOSITION, format!("{}; filename=\"{}\"", disposition_type, filename))
        .header(header::CACHE_CONTROL, "public, max-age=3600");

    if let Some(crc) = checksum_header {
        builder = builder.header("x-litiaina-checksum", crc);
    }

    if is_explicit_range || content_length < file_size {
        builder = builder
            .status(StatusCode::PARTIAL_CONTENT)
            .header(header::CONTENT_RANGE, format!("bytes {}-{}/{}", start, end, file_size))
            .header(header::CONTENT_LENGTH, content_length.to_string());
    } else {
        builder = builder
            .header(header::CONTENT_LENGTH, file_size.to_string());
    }

    Ok(builder.body(body).unwrap())
}

pub async fn reconstruct_shard(
    part: &PartMetadata,
    target_shard_idx: usize,
    version_dir: &Path,
) -> Result<Vec<u8>, AppError> {
    let data_shards_count = part.shards.iter().filter(|s| !s.is_parity).count();
    let parity_shards_count = part.shards.iter().filter(|s| s.is_parity).count();
    
    if data_shards_count == 0 {
         return Err(AppError::InternalServerError("Invalid metadata: no data shards".into()));
    }

    let raw_shard_len = part.shards.first().map(|s| s.size_bytes as usize).unwrap_or(0);
    if raw_shard_len == 0 {
        return Err(AppError::InternalServerError("Invalid shard length".into()));
    }

    let decoder_shard_len = if raw_shard_len % 2 == 0 { raw_shard_len } else { raw_shard_len + 1 };

    let mut decoder = ReedSolomonDecoder::new(data_shards_count, parity_shards_count, decoder_shard_len)
        .map_err(|e| AppError::InternalServerError(format!("RS Init: {}", e)))?;

    let mut shards_present = 0;
    
    for shard_info in &part.shards {
        if shards_present >= data_shards_count {
             break;
        }

        let path = version_dir.join(&shard_info.file_name);
        if let Ok(mut data) = fs::read(&path).await {
             if data.len() < decoder_shard_len {
                 data.resize(decoder_shard_len, 0);
             }

             if shard_info.is_parity {
                 let recovery_idx = shard_info.shard_index - data_shards_count;
                 decoder.add_recovery_shard(recovery_idx, data)
                    .map_err(|e| AppError::InternalServerError(format!("Add recovery: {}", e)))?;
             } else {
                 decoder.add_original_shard(shard_info.shard_index, data)
                    .map_err(|e| AppError::InternalServerError(format!("Add original: {}", e)))?;
             }
             shards_present += 1;
        }
    }
    
    if shards_present < data_shards_count {
        return Err(AppError::InternalServerError("Not enough shards to reconstruct data".into()));
    }

    let result = decoder.decode()
        .map_err(|e| AppError::InternalServerError(format!("Reconstruct failed: {}", e)))?;

    for (id, data) in result.restored_original_iter() {
        if let Some(info) = part.shards.iter().find(|s| s.shard_index == id && !s.is_parity) {
            let path = version_dir.join(&info.file_name);
            let mut final_data = data.to_vec();
            final_data.truncate(raw_shard_len);
            
            if let Err(e) = tokio::fs::write(&path, final_data).await {
                warn!("Failed to write-back restored shard {}: {}", info.file_name, e);
            } else {
                info!("Self-healed shard {}", info.file_name);
            }
        }
    }

    for (id, data) in result.restored_original_iter() {
        if id == target_shard_idx {
            let mut final_data = data.to_vec();
            final_data.truncate(raw_shard_len);
            return Ok(final_data);
        }
    }

    Err(AppError::InternalServerError("Reconstruction target missing".into()))
}
