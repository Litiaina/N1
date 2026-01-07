use crate::enums::app_error_enum::AppError;
use crate::object_storage::caching::{get_metadata_with_cache, get_version_metadata_with_cache};
use crate::object_storage::ec::STRIPE_BLOCK_SIZE;
use crate::object_storage::object_parser::stream_object_logic;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{
    ChunkQuery, ObjectKeyQuery, ObjectPathManager, PartMetadata, ShardInfo,
};
use axum::body::Body;
use axum::extract::{Path, Query};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use reed_solomon_simd::ReedSolomonDecoder;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::Path as StdPath;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

pub async fn download_object(
    claims: Claims,
    Path(fragment): Path<String>,
    Query(query): Query<ObjectKeyQuery>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, AppError> {
    let metadata = if let Some(version_id) = &query.version_id {
        get_version_metadata_with_cache(&fragment, &query.object_key, version_id, &claims.uid).await?
    } else {
        get_metadata_with_cache(&fragment, &query.object_key, &claims.uid).await?
    };

    if metadata.is_delete_marker {
        return Err(AppError::NotFound);
    }

    stream_object_logic(headers, metadata, "attachment").await
}

pub async fn download_object_chunk(
    claims: Claims,
    Path(fragment): Path<String>,
    Query(params): Query<ChunkQuery>,
) -> Result<impl IntoResponse, AppError> {
    let metadata = get_metadata_with_cache(&fragment, &params.object_key, &claims.uid).await?;

    if metadata.is_delete_marker {
        return Err(AppError::NotFound);
    }
    
    let path_manager = ObjectPathManager::new();
    let version_dir = path_manager.get_blob_dir(
        &metadata.owner_uid,
        &metadata.fragment,
        &metadata.version_id,
    );

    if params.offset >= metadata.total_size_bytes {
        return Ok(Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .unwrap()
            .into_response());
    }

    let mut result_buffer = Vec::with_capacity(params.chunk_size);
    let mut current_global_pos = 0u64;
    let mut checksum_header: Option<&str> = None;
    let mut file_cache: HashMap<String, File> = HashMap::new();

    for part in &metadata.parts {
        let part_len = part.size_bytes;
        let part_end = current_global_pos + part_len;

        let req_start = params.offset;
        let req_end = params.offset + params.chunk_size as u64;

        if req_start == current_global_pos && params.chunk_size as u64 == part_len {
            checksum_header = part.checksum.as_deref();
        }

        if req_end > current_global_pos && req_start < part_end {
            let part_rel_start = if req_start > current_global_pos {
                req_start - current_global_pos
            } else {
                0
            };

            let part_rel_end = std::cmp::min(req_end, part_end) - current_global_pos;
            let mut bytes_to_read = (part_rel_end - part_rel_start) as usize;
            let mut cursor_in_part = part_rel_start;

            let data_shards_info: Vec<&ShardInfo> = part.shards.iter().filter(|s| !s.is_parity).collect();
            let data_shards_count = data_shards_info.len() as u64;

            if data_shards_count == 0 {
                return Err(AppError::InternalServerError("Corrupt metadata: No data shards".into()));
            }

            while bytes_to_read > 0 {
                let stripe_block_size = STRIPE_BLOCK_SIZE as u64;
                let stripe_size = stripe_block_size * data_shards_count;
                let stripe_index = cursor_in_part / stripe_size;
                let offset_in_stripe = cursor_in_part % stripe_size;
                
                let shard_index = (offset_in_stripe / stripe_block_size) as usize;
                let offset_in_block = offset_in_stripe % stripe_block_size;
                
                let file_offset = (stripe_index * stripe_block_size) + offset_in_block;

                let bytes_left_in_block = stripe_block_size - offset_in_block;
                let chunk_len = std::cmp::min(bytes_to_read as u64, bytes_left_in_block) as usize;

                let shard_info = data_shards_info.get(shard_index)
                    .ok_or(AppError::InternalServerError("Shard index out of bounds".into()))?;

                let chunk_data = read_stripe_chunk_internal(
                    &mut file_cache,
                    &version_dir,
                    part,
                    shard_info,
                    file_offset,
                    chunk_len,
                    data_shards_count as usize
                ).await?;

                result_buffer.extend_from_slice(&chunk_data);

                bytes_to_read -= chunk_len;
                cursor_in_part += chunk_len as u64;
            }
        }

        current_global_pos += part_len;
        if result_buffer.len() >= params.chunk_size {
            break;
        }
    }

    let body = Body::from(result_buffer);
    let mut builder = Response::builder()
        .header(header::CONTENT_TYPE, "application/octet-stream");
        
    if let Some(crc) = checksum_header {
        builder = builder.header("x-litiaina-checksum", crc);
    }

    Ok(builder
        .body(body)
        .unwrap()
        .into_response())
}

async fn read_stripe_chunk_internal(
    file_cache: &mut HashMap<String, File>,
    version_dir: &StdPath,
    part: &PartMetadata,
    target_shard: &ShardInfo,
    offset: u64,
    len: usize,
    data_shards_count: usize,
) -> Result<Vec<u8>, AppError> {
    
    let mut direct_success = false;
    let mut buf = vec![0u8; len];

    {
        if !file_cache.contains_key(&target_shard.file_name) {
             let path = version_dir.join(&target_shard.file_name);
             if let Ok(f) = File::open(&path).await {
                 file_cache.insert(target_shard.file_name.clone(), f);
             }
        }

        if let Some(file) = file_cache.get_mut(&target_shard.file_name) {
            if let Ok(metadata) = file.metadata().await {
                if metadata.len() >= (offset + len as u64) {
                    if file.seek(SeekFrom::Start(offset)).await.is_ok() {
                        if file.read_exact(&mut buf).await.is_ok() {
                            direct_success = true;
                        }
                    }
                }
            }
        }
    }

    if direct_success {
        return Ok(buf);
    }

    let parity_count = part.shards.len() - data_shards_count;
    let mut decoder = ReedSolomonDecoder::new(data_shards_count, parity_count, len)
        .map_err(|e| AppError::InternalServerError(format!("RS Init: {}", e)))?;

    let mut shards_found = 0;

    for s_info in &part.shards {
        if shards_found >= data_shards_count { break; }
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
                        decoder.add_recovery_shard(s_info.shard_index - data_shards_count, chunk)
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

    if shards_found < data_shards_count {
        return Err(AppError::InternalServerError("Data chunk lost: Insufficient shards".into()));
    }

    let result = decoder.decode().map_err(|e| AppError::InternalServerError(format!("RS Decode: {}", e)))?;
    
    if let Some(restored) = result.restored_original_iter().find(|(id, _)| *id == target_shard.shard_index) {
        Ok(restored.1.to_vec())
    } else {
        Err(AppError::InternalServerError("Failed to reconstruct target chunk".into()))
    }
}
