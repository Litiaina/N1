use crate::core::disk_manager::{
    ChunkQuery, ObjectKeyQuery, ObjectMetadata, ObjectPathManager, ShardInfo,
};
use crate::core::io_scheduler::{DiskIoClass, acquire_disk_io};
use crate::core::metadata_parser::{get_metadata, get_version_metadata};
use crate::core::object_parser::stream_object_logic;
use crate::core::recovery::read_or_recover_chunk;
use crate::enums::app_error_enum::AppError;
use crate::structs::auth_structs::Claims;
use axum::body::Body;
use axum::extract::{Path, Query};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncWrite, AsyncWriteExt};

const MAX_DOWNLOAD_CHUNK_SIZE: usize = 8 * 1024 * 1024;

fn validate_chunk_query(params: &ChunkQuery) -> Result<(), AppError> {
    if params.chunk_size == 0 {
        return Err(AppError::BadRequest(
            "chunk_size must be greater than 0".into(),
        ));
    }
    if params.chunk_size > MAX_DOWNLOAD_CHUNK_SIZE {
        return Err(AppError::PayloadTooLarge(format!(
            "chunk_size exceeds server maximum of {} bytes; use streaming download or smaller chunks",
            MAX_DOWNLOAD_CHUNK_SIZE
        )));
    }
    params
        .offset
        .checked_add(params.chunk_size as u64)
        .ok_or_else(|| AppError::BadRequest("download chunk range overflows u64".into()))?;
    Ok(())
}

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

    stream_object_logic(headers, metadata, "attachment", query.filename.clone()).await
}

pub async fn download_object_chunk(
    claims: Claims,
    Path(fragment): Path<String>,
    Query(params): Query<ChunkQuery>,
) -> Result<impl IntoResponse, AppError> {
    validate_chunk_query(&params)?;
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

    let (result_buffer, checksum_header) =
        download_object_chunk_from_metadata(metadata, &params).await?;

    let body = Body::from(result_buffer);
    let mut builder = Response::builder().header(header::CONTENT_TYPE, "application/octet-stream");

    if let Some(crc) = checksum_header.as_deref() {
        builder = builder.header("x-litiaina-checksum", crc);
    }

    Ok(builder.body(body).unwrap().into_response())
}

pub async fn download_object_chunk_direct(
    fragment: &str,
    params: &ChunkQuery,
) -> Result<Vec<u8>, AppError> {
    validate_chunk_query(params)?;
    let metadata = get_metadata(fragment, &params.object_key, fragment).await?;
    if metadata.is_delete_marker {
        return Err(AppError::NotFound);
    }
    if params.offset >= metadata.total_size_bytes {
        return Ok(Vec::new());
    }

    let (result_buffer, _) = download_object_chunk_from_metadata(metadata, params).await?;
    Ok(result_buffer)
}

pub(crate) async fn download_object_chunk_from_metadata(
    metadata: ObjectMetadata,
    params: &ChunkQuery,
) -> Result<(Vec<u8>, Option<String>), AppError> {
    validate_chunk_query(params)?;
    let io_guard = acquire_disk_io(
        DiskIoClass::ForegroundRead,
        Some(&metadata.fragment),
        "download chunk",
    )
    .await?;
    let stripe_block_size = metadata.stripe_size_bytes;
    let mut result_buffer = Vec::with_capacity(params.chunk_size);
    let mut current_global_pos = 0u64;
    let mut checksum_header: Option<String> = None;
    let mut file_cache: HashMap<String, crate::core::recovery::ShardReadHandle> = HashMap::new();
    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    let num_disks = blob_roots.len();

    if num_disks == 0 {
        return Err(AppError::InternalServerError(
            "No blob storage configured".into(),
        ));
    }

    for part in &metadata.parts {
        let part_len = part.size_bytes;
        let part_end = current_global_pos + part_len;

        let req_start = params.offset;
        let req_end = params
            .offset
            .checked_add(params.chunk_size as u64)
            .ok_or_else(|| AppError::BadRequest("download chunk range overflows u64".into()))?;

        if req_start == current_global_pos && params.chunk_size as u64 == part_len {
            checksum_header = part.checksum.clone();
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

            let data_shards_info: Vec<&ShardInfo> =
                part.shards.iter().filter(|s| !s.is_parity).collect();
            let data_shards_count = data_shards_info.len() as u64;

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

                let disk_idx = (part.part_index as usize + shard_index) % num_disks;
                let target_disk = &blob_roots[disk_idx];
                let shard_dir = path_manager.get_shard_dir_on_disk(
                    target_disk,
                    &metadata.fragment,
                    &metadata.object_key,
                    &metadata.version_id,
                );

                let chunk_data = read_or_recover_chunk(
                    &mut file_cache,
                    &shard_dir,
                    part_arc.clone(),
                    shard_index,
                    file_offset,
                    chunk_len,
                    stripe_block_size,
                )
                .await?;

                result_buffer.extend_from_slice(&chunk_data);
                io_guard.record_progress(chunk_data.len() as u64);

                bytes_to_read -= chunk_len;
                cursor_in_part += chunk_len as u64;
            }
        }

        current_global_pos += part_len;
        if result_buffer.len() >= params.chunk_size {
            break;
        }
    }

    io_guard.complete();
    Ok((result_buffer, checksum_header))
}

pub(crate) async fn stream_object_range_to_writer<W>(
    metadata: ObjectMetadata,
    offset: u64,
    length: u64,
    writer: &mut W,
) -> Result<u64, AppError>
where
    W: AsyncWrite + Unpin + Send,
{
    if length == 0 || offset >= metadata.total_size_bytes {
        return Ok(0);
    }

    let end = offset
        .checked_add(length)
        .ok_or_else(|| AppError::BadRequest("download stream range overflows u64".into()))?
        .min(metadata.total_size_bytes);
    if end <= offset {
        return Ok(0);
    }

    let io_guard = acquire_disk_io(
        DiskIoClass::ForegroundRead,
        Some(&metadata.fragment),
        "native download stream",
    )
    .await?;

    let stripe_block_size = metadata.stripe_size_bytes;
    let mut current_global_pos = 0u64;
    let mut file_cache: HashMap<String, crate::core::recovery::ShardReadHandle> = HashMap::new();
    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    let num_disks = blob_roots.len();
    let mut written = 0u64;

    if num_disks == 0 {
        return Err(AppError::InternalServerError(
            "No blob storage configured".into(),
        ));
    }

    for part in &metadata.parts {
        let part_len = part.size_bytes;
        let part_end = current_global_pos + part_len;

        if end > current_global_pos && offset < part_end {
            let part_rel_start = offset.saturating_sub(current_global_pos);
            let part_rel_end = std::cmp::min(end, part_end) - current_global_pos;
            let mut bytes_to_read = (part_rel_end - part_rel_start) as usize;
            let mut cursor_in_part = part_rel_start;

            let data_shards_info: Vec<&ShardInfo> =
                part.shards.iter().filter(|s| !s.is_parity).collect();
            let data_shards_count = data_shards_info.len() as u64;

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

                let disk_idx = (part.part_index as usize + shard_index) % num_disks;
                let target_disk = &blob_roots[disk_idx];
                let shard_dir = path_manager.get_shard_dir_on_disk(
                    target_disk,
                    &metadata.fragment,
                    &metadata.object_key,
                    &metadata.version_id,
                );

                let chunk_data = read_or_recover_chunk(
                    &mut file_cache,
                    &shard_dir,
                    part_arc.clone(),
                    shard_index,
                    file_offset,
                    chunk_len,
                    stripe_block_size,
                )
                .await?;

                writer
                    .write_all(&chunk_data)
                    .await
                    .map_err(|err| AppError::Io(err, "download stream write".into()))?;

                written = written.saturating_add(chunk_data.len() as u64);
                io_guard.record_progress(chunk_data.len() as u64);
                bytes_to_read -= chunk_len;
                cursor_in_part += chunk_len as u64;
            }
        }

        current_global_pos += part_len;
        if current_global_pos >= end {
            break;
        }
    }

    writer
        .flush()
        .await
        .map_err(|err| AppError::Io(err, "download stream flush".into()))?;
    io_guard.complete();
    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::{MAX_DOWNLOAD_CHUNK_SIZE, validate_chunk_query};
    use crate::core::disk_manager::ChunkQuery;
    use crate::enums::app_error_enum::AppError;

    fn chunk(chunk_size: usize) -> ChunkQuery {
        ChunkQuery {
            object_key: "video.mp4".to_string(),
            offset: 0,
            chunk_size,
        }
    }

    #[test]
    fn chunk_query_rejects_zero_size() {
        assert!(matches!(
            validate_chunk_query(&chunk(0)),
            Err(AppError::BadRequest(_))
        ));
    }

    #[test]
    fn chunk_query_rejects_oversized_browser_buffers() {
        assert!(matches!(
            validate_chunk_query(&chunk(MAX_DOWNLOAD_CHUNK_SIZE + 1)),
            Err(AppError::PayloadTooLarge(_))
        ));
    }

    #[test]
    fn chunk_query_rejects_overflowing_ranges() {
        let query = ChunkQuery {
            object_key: "video.mp4".to_string(),
            offset: u64::MAX,
            chunk_size: 1,
        };
        assert!(matches!(
            validate_chunk_query(&query),
            Err(AppError::BadRequest(_))
        ));
    }
}
