use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::CHUNK_SIZE;
use crate::object_storage::header_parser::parse_range_header;
use crate::structs::object_storage_structs::{ObjectMetadata, ObjectPathManager, PartMetadata};
use axum::body::Body;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures_util::stream;
use std::io::SeekFrom;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

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

    let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    let (start, end) =
        parse_range_header(range_header, file_size).unwrap_or((0, file_size.saturating_sub(1)));

    if start >= file_size {
        return Err(AppError::RangeNotSatisfiable);
    }

    let content_length = end - start + 1;
    let clean_stored_key = object_key.trim_start_matches('/');

    let path_manager = ObjectPathManager::new();
    let version_dir = path_manager.get_version_dir(&owner_uid, &fragment, clean_stored_key, &version_id);

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
        current_part_index: usize,
        current_part_start_offset: u64,
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

    let initial_state = StreamState {
        pos: start,
        end,
        parts,
        version_dir,
        current_file: None,
        current_part_index: start_part_index,
        current_part_start_offset: start_part_start_offset,
    };

    let stream = stream::unfold(initial_state, move |mut state| async move {
        if state.pos > state.end {
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
            part = &state.parts[state.current_part_index];
        }

        if state.current_file.is_none() {
            let part_path = state.version_dir.join(&part.file_name);
            match File::open(&part_path).await {
                Ok(mut f) => {
                    let relative_pos = state.pos - state.current_part_start_offset;
                    if let Err(e) = f.seek(SeekFrom::Start(relative_pos)).await {
                        return Some((Err(AppError::Io(e, "seek part".into())), state));
                    }
                    state.current_file = Some(f);
                }
                Err(e) => {
                    return Some((Err(AppError::Io(e, "open part".into())), state));
                }
            }
        }

        let part_remaining = part.size_bytes - (state.pos - state.current_part_start_offset);
        let req_remaining = state.end - state.pos + 1;
        let to_read = (CHUNK_SIZE as u64).min(part_remaining).min(req_remaining) as usize;

        let mut buf = vec![0u8; to_read];

        let file = state.current_file.as_mut().unwrap();

        match file.read_exact(&mut buf).await {
            Ok(_) => {
                let bytes = Bytes::from(buf);
                state.pos += to_read as u64;
                Some((Ok(bytes), state))
            }
            Err(e) => Some((Err(AppError::Io(e, "read part".into())), state)),
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

    if content_length < file_size {
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
