use crate::enums::app_error_enum::AppError;
use crate::object_storage::caching::get_metadata_with_cache;
use crate::object_storage::header_parser::parse_range_header;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{
    ChunkQuery, ObjectKeyQuery, ObjectMetadata, ObjectPathManager, PartMetadata,
};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures_util::stream;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

pub async fn download_object(
    State(_state): State<Arc<AppState>>,
    claims: Claims,
    Path(fragment): Path<String>,
    Query(query): Query<ObjectKeyQuery>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, AppError> {
    let clean_key = query.object_key.trim_start_matches('/');

    let metadata = get_metadata_with_cache(&fragment, clean_key, &claims.uid).await?;

    stream_object(metadata, headers, "attachment").await
}

pub async fn download_object_chunk(
    State(_state): State<Arc<AppState>>,
    claims: Claims,
    Path(fragment): Path<String>,
    Query(params): Query<ChunkQuery>,
) -> Result<impl IntoResponse, AppError> {
    let clean_key = params.object_key.trim_start_matches('/');

    let metadata = get_metadata_with_cache(&fragment, clean_key, &claims.uid).await?;

    let clean_stored_key = metadata.object_key.trim_start_matches('/');

    let path_manager = ObjectPathManager::new();
    let version_dir = path_manager.get_version_dir(
        &metadata.owner_uid,
        &metadata.fragment,
        clean_stored_key,
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

    for part in &metadata.parts {
        let part_len = part.size_bytes;
        let part_end = current_pos + part_len;

        let req_start = params.offset;
        let req_end = params.offset + params.chunk_size as u64;

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

                let mut chunk = vec![0u8; to_read];
                file.read_exact(&mut chunk)
                    .await
                    .map_err(|e| AppError::Io(e, "read part".into()))?;

                result_buffer.extend_from_slice(&chunk);
            }
        }

        current_pos += part_len;

        if result_buffer.len() >= params.chunk_size {
            break;
        }
    }

    let body = Body::from(result_buffer);
    Ok(Response::builder()
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(body)
        .unwrap()
        .into_response())
}

pub async fn stream_object(
    metadata: ObjectMetadata,
    headers: HeaderMap,
    disposition_type: &str,
) -> Result<impl IntoResponse, AppError> {
    let file_size = metadata.total_size_bytes;

    let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());

    let (start, end, is_partial) = if let Some(range) = range_header {
        match parse_range_header(Some(range), file_size) {
            Some((s, e)) => (s, e, true),
            None => return Err(AppError::RangeNotSatisfiable),
        }
    } else {
        (0, file_size.saturating_sub(1), false)
    };

    if start >= file_size {
        return Err(AppError::RangeNotSatisfiable);
    }

    let content_length = end - start + 1;

    let clean_stored_key = metadata.object_key.trim_start_matches('/');

    let path_manager = ObjectPathManager::new();
    let version_dir = path_manager.get_version_dir(
        &metadata.owner_uid,
        &metadata.fragment,
        clean_stored_key,
        &metadata.version_id,
    );

    if !tokio::fs::try_exists(&version_dir).await.unwrap_or(false) {
        tracing::error!("Physical directory missing for object: {:?}", version_dir);
        return Err(AppError::NotFound);
    }

    let filename = std::path::Path::new(&clean_stored_key)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("download")
        .to_string();

    let ObjectMetadata {
        content_type,
        parts,
        ..
    } = metadata;

    struct StreamState {
        pos: u64,
        end_pos: u64,
        parts: Vec<PartMetadata>,
        version_dir: PathBuf,
        current_file: Option<(usize, File)>,
    }

    let initial_state = StreamState {
        pos: start,
        end_pos: end,
        parts,
        version_dir,
        current_file: None,
    };

    let stream = stream::unfold(initial_state, move |mut state| async move {
        if state.pos > state.end_pos {
            return None;
        }

        let mut current_offset = 0;
        let mut target_part_idx = None;
        let mut part_relative_offset = 0;

        for (idx, part) in state.parts.iter().enumerate() {
            let part_end = current_offset + part.size_bytes;
            if state.pos >= current_offset && state.pos < part_end {
                target_part_idx = Some(idx);
                part_relative_offset = state.pos - current_offset;
                break;
            }
            current_offset = part_end;
        }

        let part_idx = match target_part_idx {
            Some(idx) => idx,
            None => return None,
        };

        let part = &state.parts[part_idx];

        let mut file = match state.current_file.take() {
            Some((idx, f)) if idx == part_idx => f,
            _ => {
                let part_path = state.version_dir.join(&part.file_name);
                let mut f = match File::open(&part_path).await {
                    Ok(f) => f,
                    Err(e) => {
                        tracing::error!("Failed to open part {:?}: {}", part_path, e);
                        return Some((Err(AppError::Io(e, "open part".into())), state));
                    }
                };
                if let Err(e) = f.seek(SeekFrom::Start(part_relative_offset)).await {
                    return Some((Err(AppError::Io(e, "seek part".into())), state));
                }
                f
            }
        };

        let part_remaining = part.size_bytes - part_relative_offset;
        let req_remaining = state.end_pos - state.pos + 1;
        let read_size = 64 * 1024;
        let to_read = (read_size as u64).min(part_remaining).min(req_remaining) as usize;

        let mut buf = vec![0u8; to_read];
        match file.read_exact(&mut buf).await {
            Ok(_) => {
                let bytes = Bytes::from(buf);
                state.pos += to_read as u64;
                state.current_file = Some((part_idx, file));
                Some((Ok(bytes), state))
            }
            Err(e) => Some((Err(AppError::Io(e, "read part".into())), state)),
        }
    });

    let body = Body::from_stream(stream);

    let mut builder = Response::builder()
        .header(header::CONTENT_TYPE, content_type)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(
            header::CONTENT_DISPOSITION,
            format!("{}; filename=\"{}\"", disposition_type, filename),
        );

    if is_partial {
        builder = builder
            .status(StatusCode::PARTIAL_CONTENT)
            .header(
                header::CONTENT_RANGE,
                format!("bytes {}-{}/{}", start, end, file_size),
            )
            .header(header::CONTENT_LENGTH, content_length.to_string());
    } else {
        builder = builder
            .status(StatusCode::OK)
            .header(header::CONTENT_LENGTH, file_size.to_string());
    }

    Ok(builder.body(body).unwrap())
}
