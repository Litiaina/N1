use crate::enums::app_error_enum::AppError;
use crate::n1_core::disk_manager::{ChunkQuery, ObjectKeyQuery, ObjectPathManager};
use crate::n1_core::metadata_parser::{get_metadata, get_version_metadata};
use crate::n1_core::object_parser::{
    data_shards_count, shard_dir_with_locality_hint, shard_dirs_for_version, stream_object_logic,
};
use crate::n1_core::recovery::{ShardFileCache, ShardRepairContext, read_or_recover_chunk_into};
use crate::structs::auth_structs::Claims;
use axum::body::Body;
use axum::extract::{Path, Query};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use std::sync::Arc;

pub async fn download_object(
    claims: Claims,
    Path(fragment): Path<String>,
    Query(query): Query<ObjectKeyQuery>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }

    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let metadata = if let Some(version_id) = &query.version_id {
        get_version_metadata(&fragment, &query.object_key, version_id, &claims.fragment).await?
    } else {
        get_metadata(&fragment, &query.object_key, &claims.fragment).await?
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
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }

    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let metadata = get_metadata(&fragment, &params.object_key, &claims.fragment).await?;

    if metadata.is_delete_marker {
        return Err(AppError::NotFound);
    }

    if params.offset >= metadata.total_size_bytes {
        return Ok(Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .unwrap()
            .into_response());
    }

    let stripe_block_size = metadata.stripe_size_bytes;
    let repair_context = ShardRepairContext::new(
        metadata.fragment.clone(),
        metadata.object_key.clone(),
        metadata.version_id.clone(),
        false,
    );
    let mut result_buffer = Vec::with_capacity(params.chunk_size);
    let mut current_global_pos = 0u64;
    let mut checksum_header: Option<&str> = None;
    let mut file_cache: ShardFileCache = ShardFileCache::new();
    let mut scratch = Vec::new();
    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    let shard_dirs = shard_dirs_for_version(
        &path_manager,
        &blob_roots,
        &metadata.fragment,
        &metadata.version_id,
    );
    let num_disks = shard_dirs.len();

    if num_disks == 0 {
        return Err(AppError::InternalServerError(
            "No blob storage configured".into(),
        ));
    }

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

            let data_shards_count = data_shards_count(part) as u64;

            if data_shards_count == 0 {
                return Err(AppError::InternalServerError(
                    "Corrupt metadata: No data shards".into(),
                ));
            }

            let part_arc = Arc::new(part.clone());

            while bytes_to_read > 0 {
                let stripe_size = (stripe_block_size as u64) * data_shards_count;
                let stripe_index = cursor_in_part / stripe_size;
                let offset_in_stripe = cursor_in_part % stripe_size;

                let shard_index = (offset_in_stripe / (stripe_block_size as u64)) as usize;
                let offset_in_block = offset_in_stripe % (stripe_block_size as u64);

                let file_offset = (stripe_index * (stripe_block_size as u64)) + offset_in_block;

                let bytes_left_in_block = (stripe_block_size as u64) - offset_in_block;
                let chunk_len = std::cmp::min(bytes_to_read as u64, bytes_left_in_block) as usize;

                let Some(shard_dir) = shard_dir_with_locality_hint(part, shard_index, &shard_dirs)
                else {
                    return Err(AppError::InternalServerError(
                        "No shard dirs configured".into(),
                    ));
                };

                read_or_recover_chunk_into(
                    &mut file_cache,
                    shard_dir,
                    part_arc.clone(),
                    shard_index,
                    file_offset,
                    chunk_len,
                    stripe_block_size,
                    Some(repair_context.clone()),
                    &mut scratch,
                )
                .await?;

                result_buffer.extend_from_slice(&scratch);

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
    let mut builder = Response::builder().header(header::CONTENT_TYPE, "application/octet-stream");

    if let Some(crc) = checksum_header {
        builder = builder.header("x-litiaina-checksum", crc);
    }

    Ok(builder.body(body).unwrap().into_response())
}
