use crate::enums::app_error_enum::AppError;
use crate::object_storage::ec::STRIPE_BLOCK_SIZE;
use crate::object_storage::header_parser::parse_range_header;
use crate::structs::object_storage_structs::{
    ObjectMetadata, ObjectPathManager, PartMetadata, ShardInfo,
};
use axum::body::Body;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures_util::stream;
use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::{info, error};

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

    struct StreamState {
        pos: u64,
        end: u64,
        parts: Vec<PartMetadata>,
        version_dir: PathBuf,
        file_cache: HashMap<String, File>,
        current_part_idx: usize,
        current_part_offset: u64,
    }

    let mut current_offset = 0;
    let mut start_part_index = 0;
    let mut start_part_offset = 0;

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

            let data_shards_info: Vec<&ShardInfo> = part
                .shards
                .iter()
                .filter(|s| !s.is_parity)
                .collect();
                
            let data_shards_count = data_shards_info.len() as u64;
            if data_shards_count == 0 {
                return Some((Err(AppError::InternalServerError("No data shards".into())), state));
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
                 return Some((Err(AppError::InternalServerError("Shard index out of bounds".into())), state));
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

            let data_result = read_or_recover_chunk(
                &mut state.file_cache,
                &state.version_dir,
                part,
                shard_index,
                file_offset,
                to_read
            ).await;

            match data_result {
                Ok(bytes) => {
                    batch_buffer.extend_from_slice(&bytes);
                    state.pos += to_read as u64;
                },
                Err(e) => return Some((Err(e), state))
            }
        }

        if batch_buffer.is_empty() {
            return None;
        }

        Some((Ok(Bytes::from(batch_buffer)), state))
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

async fn read_or_recover_chunk(
    file_cache: &mut HashMap<String, File>,
    version_dir: &Path,
    part: &PartMetadata,
    shard_idx: usize,
    offset: u64,
    len: usize
) -> Result<Vec<u8>, AppError> {
    
    let mut direct_success = false;
    let mut buf = vec![0u8; len];

    {
        let target_shard = &part.shards[shard_idx];
        
        if !file_cache.contains_key(&target_shard.file_name) {
             let path = version_dir.join(&target_shard.file_name);
             if let Ok(f) = File::open(&path).await {
                 file_cache.insert(target_shard.file_name.clone(), f);
             }
        }

        if let Some(file) = file_cache.get_mut(&target_shard.file_name) {
            // Optimistic read
            if file.seek(SeekFrom::Start(offset)).await.is_ok() {
                if file.read_exact(&mut buf).await.is_ok() {
                    direct_success = true;
                }
            }
        }
    }

    if direct_success {
        return Ok(buf);
    }

    let data_shards: Vec<&ShardInfo> = part.shards.iter().filter(|s| !s.is_parity).collect();
    let target_shard = data_shards.get(shard_idx).ok_or(AppError::InternalServerError("Bad shard idx".into()))?;

    info!("Attempting reconstruction for shard {} at offset {}", shard_idx, offset);
    
    let data_count = data_shards.len();
    let parity_count = part.shards.len() - data_count;
    
    let mut decoder = ReedSolomonDecoder::new(data_count, parity_count, len)
        .map_err(|e| AppError::InternalServerError(format!("RS Decoder: {}", e)))?;

    let mut shards_found = 0;
    
    for s_info in &part.shards {
        if shards_found >= data_count { break; }
        if s_info.shard_index == target_shard.shard_index { continue; }

        if !file_cache.contains_key(&s_info.file_name) {
             let s_path = version_dir.join(&s_info.file_name);
             if let Ok(f) = File::open(&s_path).await {
                 file_cache.insert(s_info.file_name.clone(), f);
             }
        }

        if let Some(f) = file_cache.get_mut(&s_info.file_name) {
            if let Ok(metadata) = f.metadata().await {
                if metadata.len() < (offset + len as u64) {
                    continue; 
                }
            }

            if f.seek(SeekFrom::Start(offset)).await.is_ok() {
                let mut chunk = vec![0u8; len];
                if f.read_exact(&mut chunk).await.is_ok() {
                    let res = if s_info.is_parity {
                        decoder.add_recovery_shard(s_info.shard_index - data_count, chunk)
                    } else {
                        decoder.add_original_shard(s_info.shard_index, chunk)
                    };
                    
                    if res.is_ok() {
                        shards_found += 1;
                    }
                }
            }
        }
    }

    if shards_found < data_count {
        return Err(AppError::InternalServerError("Data lost: Insufficient shards for recovery".into()));
    }

    let result = decoder.decode().map_err(|e| AppError::InternalServerError(format!("RS Decode: {}", e)))?;
    
    if let Some(restored) = result.restored_original_iter().find(|(id, _)| *id == target_shard.shard_index) {
        
        let shard_path = version_dir.join(&target_shard.file_name);
        if !shard_path.exists() {
             let v_dir = version_dir.to_path_buf();
             let p_meta = part.clone();
             let t_idx = target_shard.shard_index;
             let t_name = target_shard.file_name.clone();
             
             tokio::spawn(async move {
                 if let Err(e) = repair_entire_shard(&v_dir, &p_meta, t_idx).await {
                     error!("Background repair failed for {}: {}", t_name, e);
                 } else {
                     info!("Successfully self-healed {}", t_name);
                 }
             });
        }

        return Ok(restored.1.to_vec());
    }

    Err(AppError::InternalServerError("Reconstruction failed to produce target".into()))
}

async fn repair_entire_shard(
    version_dir: &Path, 
    part: &PartMetadata, 
    target_shard_idx: usize
) -> Result<(), AppError> {
    let target_shard = part.shards.iter()
        .find(|s| s.shard_index == target_shard_idx)
        .ok_or(AppError::InternalServerError("Target shard not found".into()))?;

    let temp_name = format!("{}.repair", target_shard.file_name);
    let temp_path = version_dir.join(&temp_name);
    let final_path = version_dir.join(&target_shard.file_name);

    if temp_path.exists() || final_path.exists() {
        return Ok(()); 
    }

    let mut temp_file = File::create(&temp_path).await
        .map_err(|e| AppError::Io(e, "create repair file".into()))?;

    let data_shards: Vec<&ShardInfo> = part.shards.iter().filter(|s| !s.is_parity).collect();
    let data_count = data_shards.len();
    let parity_count = part.shards.len() - data_count;
    let block_size = STRIPE_BLOCK_SIZE;

    let total_shard_size = part.shards.first().map(|s| s.size_bytes).unwrap_or(0);
    
    let mut peer_files: Vec<Option<File>> = Vec::new();

    for s_info in &part.shards {
        if s_info.shard_index == target_shard_idx { 
            peer_files.push(None);
            continue; 
        }
        
        let s_path = version_dir.join(&s_info.file_name);
        match File::open(&s_path).await {
            Ok(f) => {
                peer_files.push(Some(f));
            },
            Err(_) => peer_files.push(None),
        }
    }

    let mut current_offset = 0u64;

    while current_offset < total_shard_size {
        let mut decoder = ReedSolomonDecoder::new(data_count, parity_count, block_size)
            .map_err(|e| AppError::InternalServerError(format!("RS Init: {}", e)))?;
        
        let mut original_data_stripe: Vec<Option<Vec<u8>>> = vec![None; data_count];
        let mut shards_found = 0;

        for (idx, file_opt) in peer_files.iter_mut().enumerate() {
            let s_info = &part.shards[idx]; 
            
            if let Some(f) = file_opt {
                let mut buf = vec![0u8; block_size];
                if f.read_exact(&mut buf).await.is_ok() {
                    if !s_info.is_parity {
                        if s_info.shard_index < data_count {
                            original_data_stripe[s_info.shard_index] = Some(buf.clone());
                        }
                    }

                    let res = if s_info.is_parity {
                        decoder.add_recovery_shard(s_info.shard_index - data_count, buf)
                    } else {
                        decoder.add_original_shard(s_info.shard_index, buf)
                    };
                    if res.is_ok() { shards_found += 1; }
                }
            }
        }

        if shards_found < data_count {
            let _ = fs::remove_file(&temp_path).await;
            return Err(AppError::InternalServerError("Not enough shards for repair".into()));
        }

        let result = decoder.decode().map_err(|e| AppError::InternalServerError(format!("Decode: {}", e)))?;
        
        for (idx, data) in result.restored_original_iter() {
            if idx < data_count {
                original_data_stripe[idx] = Some(data.to_vec());
            }
        }

        if target_shard_idx < data_count {
            if let Some(data) = &original_data_stripe[target_shard_idx] {
                temp_file.write_all(data).await
                    .map_err(|e| AppError::Io(e, "write repair chunk".into()))?;
            } else {
                return Err(AppError::InternalServerError("Failed to resolve target data shard".into()));
            }
        } else {
            if original_data_stripe.iter().any(|d| d.is_none()) {
                 return Err(AppError::InternalServerError("Failed to recover all data for parity gen".into()));
            }

            let mut encoder = ReedSolomonEncoder::new(data_count, parity_count, block_size)
                .map_err(|e| AppError::InternalServerError(format!("RS Enc Init: {}", e)))?;

            for data in original_data_stripe.iter().flatten() {
                encoder.add_original_shard(data)
                    .map_err(|e| AppError::InternalServerError(format!("RS Add: {}", e)))?;
            }

            let enc_result = encoder.encode()
                .map_err(|e| AppError::InternalServerError(format!("RS Encode: {}", e)))?;

            let parity_rel_idx = target_shard_idx - data_count;
            
            let mut found = false;
            for (idx, data) in enc_result.recovery_iter().enumerate() {
                if idx == parity_rel_idx {
                    temp_file.write_all(data).await
                        .map_err(|e| AppError::Io(e, "write repair chunk".into()))?;
                    found = true;
                    break;
                }
            }
            
            if !found {
                 return Err(AppError::InternalServerError("Generated parity index mismatch".into()));
            }
        }

        current_offset += block_size as u64;
    }

    temp_file.flush().await.map_err(|e| AppError::Io(e, "flush repair".into()))?;
    drop(temp_file); 

    fs::rename(&temp_path, &final_path).await
        .map_err(|e| AppError::Io(e, "finalize repair".into()))?;

    Ok(())
}
