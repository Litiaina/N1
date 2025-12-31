use crate::enums::app_error_enum::AppError;
use crate::object_storage::caching::{get_metadata_with_cache, get_version_metadata_with_cache};
use crate::object_storage::object_parser::stream_object_logic;
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
            let to_read = (rel_end - rel_start) as usize;

            if to_read > 0 {
                let part_path = version_dir.join(&part.file_name);
                let mut file = match File::open(&part_path).await {
                    Ok(f) => f,
                    Err(e) => {
                        tracing::error!("Failed to open part file {:?}: {}", part_path, e);
                        return Err(AppError::Io(e, "open part".into()));
                    }
                };

                if rel_start > 0 {
                    file.seek(SeekFrom::Start(rel_start))
                        .await
                        .map_err(|e| AppError::Io(e, "seek part".into()))?;
                }

                let current_len = result_buffer.len();
                result_buffer.resize(current_len + to_read, 0);
                
                file.read_exact(&mut result_buffer[current_len..])
                    .await
                    .map_err(|e| AppError::Io(e, "read part".into()))?;
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
