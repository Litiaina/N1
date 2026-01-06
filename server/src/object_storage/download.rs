use crate::enums::app_error_enum::AppError;
use crate::object_storage::caching::{get_metadata_with_cache, get_version_metadata_with_cache};
use crate::object_storage::object_parser::{stream_object_logic, reconstruct_shard};
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{
    ChunkQuery, ObjectKeyQuery, ObjectPathManager,
};
use axum::body::Body;
use axum::extract::{Path, Query};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use std::io::SeekFrom;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

pub async fn download_object(
    claims: Claims,
    Path(fragment): Path<String>,
    Query(query): Query<ObjectKeyQuery>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, AppError> {
    let clean_key = query.object_key.trim_start_matches('/');

    let metadata = if let Some(version_id) = &query.version_id {
        get_version_metadata_with_cache(&fragment, clean_key, version_id, &claims.uid).await?
    } else {
        get_metadata_with_cache(&fragment, clean_key, &claims.uid).await?
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
    let clean_key = params.object_key.trim_start_matches('/');

    let metadata = get_metadata_with_cache(&fragment, clean_key, &claims.uid).await?;

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
    let mut current_pos = 0u64;
    let mut checksum_header: Option<&str> = None;

    for part in &metadata.parts {
        let part_len = part.size_bytes; 
        let part_end = current_pos + part_len;

        let req_start = params.offset;
        let req_end = params.offset + params.chunk_size as u64;

        if req_start == current_pos && params.chunk_size as u64 == part_len {
            checksum_header = part.checksum.as_deref();
        }

        if req_end > current_pos && req_start < part_end {
            let rel_start = if req_start > current_pos {
                req_start - current_pos
            } else {
                0
            };

            let rel_end = std::cmp::min(req_end, part_end) - current_pos;
            let mut to_read_total = (rel_end - rel_start) as usize;
            
            let mut data_shards: Vec<_> = part.shards.iter()
                .filter(|s| !s.is_parity)
                .collect();
            data_shards.sort_by_key(|s| s.shard_index);

            if data_shards.is_empty() {
                 return Err(AppError::InternalServerError("Corrupt metadata: No data shards".into()));
            }

            let shard_size = data_shards[0].size_bytes; 
            
            let mut cursor_in_part = rel_start;
            
            while to_read_total > 0 {
                let shard_idx = (cursor_in_part / shard_size) as usize;
                let offset_in_shard = cursor_in_part % shard_size;

                if shard_idx >= data_shards.len() {
                    break;
                }

                let shard_info = data_shards[shard_idx];
                let bytes_available_in_shard = shard_size - offset_in_shard;
                let chunk_to_read = std::cmp::min(to_read_total as u64, bytes_available_in_shard) as usize;

                let part_path = version_dir.join(&shard_info.file_name);
                
                let buffer = match File::open(&part_path).await {
                    Ok(mut file) => {
                        let mut temp_buf = vec![0u8; chunk_to_read];
                        if offset_in_shard > 0 {
                            file.seek(SeekFrom::Start(offset_in_shard))
                                .await
                                .map_err(|e| AppError::Io(e, "seek shard".into()))?;
                        }
                        file.read_exact(&mut temp_buf)
                            .await
                            .map_err(|e| AppError::Io(e, "read shard".into()))?;
                        temp_buf
                    },
                    Err(_) => {
                        let full_shard_data = reconstruct_shard(part, shard_info.shard_index, &version_dir).await?;
                        
                        let start = offset_in_shard as usize;
                        let end = start + chunk_to_read;
                        if end > full_shard_data.len() {
                             return Err(AppError::InternalServerError("Reconstructed shard too small".into()));
                        }
                        full_shard_data[start..end].to_vec()
                    }
                };

                let current_buf_len = result_buffer.len();
                result_buffer.resize(current_buf_len + chunk_to_read, 0);
                result_buffer[current_buf_len..].copy_from_slice(&buffer);

                to_read_total -= chunk_to_read;
                cursor_in_part += chunk_to_read as u64;
            }
        }

        current_pos += part_len;

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
