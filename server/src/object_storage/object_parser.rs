use crate::enums::app_error_enum::AppError;
use crate::object_storage::header_parser::parse_range_header;
use crate::structs::object_storage_structs::{ObjectMetadata, ObjectPathManager};
use axum::body::Body;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures_util::stream;
use std::io::SeekFrom;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

pub async fn get_object_logic(
    metadata: ObjectMetadata,
    req: axum::http::Request<Body>,
) -> Result<impl IntoResponse, AppError> {
    let (parts, _body) = req.into_parts();
    stream_object_logic(parts.headers, metadata).await
}

pub async fn stream_object_logic(
    headers: HeaderMap,
    metadata: ObjectMetadata,
) -> Result<impl IntoResponse, AppError> {
    let file_size = metadata.total_size_bytes;

    let ObjectMetadata {
        content_type,
        parts,
        owner_uid,
        fragment,
        object_key,
        version_id,
        ..
    } = metadata;

    let range_header = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    let (start, end) =
        parse_range_header(range_header, file_size).unwrap_or((0, file_size.saturating_sub(1)));

    if start >= file_size {
        return Err(AppError::RangeNotSatisfiable);
    }

    let content_length = end - start + 1;

    let path_manager = ObjectPathManager::new();
    let version_dir = path_manager.get_version_dir(&owner_uid, &fragment, &object_key, &version_id);

    let stream = stream::unfold(
        (start, end, parts, version_dir),
        move |(pos, end_pos, parts, dir)| async move {
            if pos > end_pos {
                return None;
            }

            let mut current_offset = 0;
            let mut target_part = None;
            let mut part_relative_offset = 0;

            for part in &parts {
                let part_end = current_offset + part.size_bytes;
                if pos >= current_offset && pos < part_end {
                    target_part = Some(part);
                    part_relative_offset = pos - current_offset;
                    break;
                }
                current_offset = part_end;
            }

            let part = match target_part {
                Some(p) => p,
                None => return None,
            };

            let part_path = dir.join(&part.file_name);
            let mut file = match File::open(&part_path).await {
                Ok(f) => f,
                Err(e) => {
                    return Some((
                        Err(AppError::Io(e, "open part".into())),
                        (pos, end_pos, parts, dir),
                    ));
                }
            };

            if let Err(e) = file.seek(SeekFrom::Start(part_relative_offset)).await {
                return Some((
                    Err(AppError::Io(e, "seek part".into())),
                    (pos, end_pos, parts, dir),
                ));
            }

            let part_remaining = part.size_bytes - part_relative_offset;
            let req_remaining = end_pos - pos + 1;
            let read_size = 64 * 1024;
            let to_read = (read_size as u64).min(part_remaining).min(req_remaining) as usize;

            let mut buf = vec![0u8; to_read];
            match file.read_exact(&mut buf).await {
                Ok(_) => {
                    let bytes = Bytes::from(buf);
                    Some((Ok(bytes), (pos + to_read as u64, end_pos, parts, dir)))
                }
                Err(e) => Some((
                    Err(AppError::Io(e, "read part".into())),
                    (pos, end_pos, parts, dir),
                )),
            }
        },
    );

    let body = Body::from_stream(stream);

    Ok(Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_LENGTH, content_length.to_string())
        .header(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", start, end, file_size),
        )
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CACHE_CONTROL, "public, max-age=3600")
        .header(header::CONTENT_DISPOSITION, "inline")
        .body(body)
        .unwrap())
}
