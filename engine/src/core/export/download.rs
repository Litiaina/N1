use std::collections::VecDeque;
use std::path::PathBuf;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::Response;
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::core::io_scheduler::{DiskIoClass, DiskIoGuard, acquire_disk_io};
use crate::enums::app_error_enum::AppError;

use super::ActiveDownloadLease;
use super::models::{ArtifactChunk, PersistedExport};
use super::store::path_is_on_configured_blob_root;

const DOWNLOAD_READ_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone)]
struct ArtifactSegment {
    path: PathBuf,
    file_offset: u64,
    size_bytes: u64,
    file_size_bytes: u64,
    expected_sha256: Option<String>,
}

struct ActiveSegment {
    file: File,
    remaining: u64,
    checksum: Option<(Sha256, String)>,
}

struct SegmentStreamState {
    segments: VecDeque<ArtifactSegment>,
    current: Option<ActiveSegment>,
    io_guard: Option<DiskIoGuard>,
    _download_lease: ActiveDownloadLease,
}

fn strict_range(value: Option<&str>, size: u64) -> Result<Option<(u64, u64)>, ()> {
    let Some(value) = value else {
        return Ok(None);
    };
    let Some(spec) = value.strip_prefix("bytes=") else {
        return Err(());
    };
    if spec.contains(',') || size == 0 {
        return Err(());
    }
    let Some((start, end)) = spec.split_once('-') else {
        return Err(());
    };
    if start.is_empty() {
        let suffix = end.parse::<u64>().map_err(|_| ())?;
        if suffix == 0 {
            return Err(());
        }
        let length = suffix.min(size);
        return Ok(Some((size - length, size - 1)));
    }
    let start = start.parse::<u64>().map_err(|_| ())?;
    if start >= size {
        return Err(());
    }
    let end = if end.is_empty() {
        size - 1
    } else {
        end.parse::<u64>().map_err(|_| ())?.min(size - 1)
    };
    if end < start {
        return Err(());
    }
    Ok(Some((start, end)))
}

pub(crate) fn safe_download_filename(filename: &str, format: &str) -> String {
    let fallback = format!("export.{format}");
    let cleaned = filename
        .chars()
        .map(|value| {
            if value.is_ascii_alphanumeric()
                || matches!(value, '.' | '_' | '-' | ' ' | '(' | ')' | '[' | ']')
            {
                value
            } else {
                '_'
            }
        })
        .collect::<String>();
    let cleaned = cleaned.trim().trim_matches('.');
    if cleaned.is_empty() {
        return fallback;
    }
    if cleaned
        .to_ascii_lowercase()
        .ends_with(&format!(".{format}"))
    {
        cleaned.to_string()
    } else {
        format!("{cleaned}.{format}")
    }
}

fn relevant_segments(
    chunks: &[ArtifactChunk],
    start: u64,
    end: u64,
) -> Result<Vec<ArtifactSegment>, AppError> {
    let mut segments = Vec::new();
    for chunk in chunks {
        let chunk_start = chunk.logical_offset;
        let chunk_end = chunk_start
            .checked_add(chunk.size_bytes)
            .and_then(|value| value.checked_sub(1))
            .ok_or_else(|| {
                AppError::InternalServerError("N1 export artifact chunk range overflow".to_string())
            })?;
        if end < chunk_start || start > chunk_end {
            continue;
        }
        let segment_start = start.max(chunk_start);
        let segment_end = end.min(chunk_end);
        let path = PathBuf::from(&chunk.path);
        if !path_is_on_configured_blob_root(&path) {
            return Err(AppError::InternalServerError(
                "N1 export artifact escaped configured blob roots".to_string(),
            ));
        }
        segments.push(ArtifactSegment {
            path,
            file_offset: segment_start - chunk_start,
            size_bytes: segment_end - segment_start + 1,
            file_size_bytes: chunk.size_bytes,
            expected_sha256: (segment_start == chunk_start
                && segment_end == chunk_end
                && !chunk.sha256.is_empty())
            .then(|| chunk.sha256.clone()),
        });
    }
    let covered = segments.iter().fold(0_u64, |total, segment| {
        total.saturating_add(segment.size_bytes)
    });
    if covered != end - start + 1 {
        return Err(AppError::ServiceUnavailable(
            "N1 export artifact is incomplete; rebuild the export".to_string(),
        ));
    }
    Ok(segments)
}

async fn preflight_segments(segments: &[ArtifactSegment]) -> Result<(), AppError> {
    let checks = futures_util::stream::iter(segments.iter().cloned())
        .map(|segment| async move {
            let metadata = tokio::fs::metadata(&segment.path).await.map_err(|error| {
                AppError::Io(error, "stat N1 export artifact chunk".to_string())
            })?;
            if metadata.len() != segment.file_size_bytes {
                return Err(AppError::ServiceUnavailable(
                    "N1 export artifact chunk length is invalid; rebuild the export".to_string(),
                ));
            }
            Ok(())
        })
        .buffer_unordered(32);
    tokio::pin!(checks);
    while let Some(result) = checks.next().await {
        result?;
    }
    Ok(())
}

fn artifact_body(
    segments: Vec<ArtifactSegment>,
    io_guard: DiskIoGuard,
    download_lease: ActiveDownloadLease,
) -> Body {
    let state = SegmentStreamState {
        segments: segments.into(),
        current: None,
        io_guard: Some(io_guard),
        _download_lease: download_lease,
    };
    let stream = futures_util::stream::try_unfold(state, |mut state| async move {
        loop {
            if let Some(current) = state.current.as_mut() {
                let mut buffer = vec![0_u8; (current.remaining as usize).min(DOWNLOAD_READ_BYTES)];
                let read = current.file.read(&mut buffer).await?;
                if read == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "N1 export artifact chunk ended early",
                    ));
                }
                current.remaining -= read as u64;
                if let Some((hasher, _)) = current.checksum.as_mut() {
                    hasher.update(&buffer[..read]);
                }
                if let Some(io_guard) = state.io_guard.as_ref() {
                    io_guard.record_progress(read as u64);
                }
                if current.remaining == 0 {
                    if let Some((hasher, expected)) = current.checksum.take() {
                        let actual = hex::encode(hasher.finalize());
                        if actual != expected {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "N1 export artifact chunk checksum mismatch",
                            ));
                        }
                    }
                    state.current = None;
                }
                buffer.truncate(read);
                return Ok(Some((Bytes::from(buffer), state)));
            }

            let Some(segment) = state.segments.pop_front() else {
                if let Some(io_guard) = state.io_guard.take() {
                    io_guard.complete();
                }
                return Ok(None);
            };
            let mut file = File::open(&segment.path).await?;
            file.seek(std::io::SeekFrom::Start(segment.file_offset))
                .await?;
            state.current = Some(ActiveSegment {
                file,
                remaining: segment.size_bytes,
                checksum: segment
                    .expected_sha256
                    .map(|expected| (Sha256::new(), expected)),
            });
        }
    });
    Body::from_stream(stream)
}

fn range_not_satisfiable(size: u64) -> Response {
    Response::builder()
        .status(StatusCode::RANGE_NOT_SATISFIABLE)
        .header(header::CONTENT_RANGE, format!("bytes */{size}"))
        .header(header::ACCEPT_RANGES, "bytes")
        .body(Body::empty())
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

pub(super) async fn build_download_response(
    state: &PersistedExport,
    headers: &HeaderMap,
) -> Result<Response, AppError> {
    let (state, download_lease) = super::lease_ready_export(&state.id).await?;
    let size = state.artifact_size_bytes.ok_or_else(|| {
        AppError::InternalServerError("ready export has no artifact size".to_string())
    })?;
    let etag = state.etag_sha256.as_ref().ok_or_else(|| {
        AppError::InternalServerError("ready export has no artifact checksum".to_string())
    })?;
    let quoted_etag = format!("\"{etag}\"");
    if headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|candidate| candidate.trim() == quoted_etag)
        })
    {
        return Ok(Response::builder()
            .status(StatusCode::NOT_MODIFIED)
            .header(header::ETAG, quoted_etag)
            .body(Body::empty())
            .unwrap_or_else(|_| Response::new(Body::empty())));
    }

    let requested_range = headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok());
    let if_range_matches = headers
        .get(header::IF_RANGE)
        .and_then(|value| value.to_str().ok())
        .is_none_or(|value| value.trim() == quoted_etag);
    let parsed_range = if if_range_matches {
        match strict_range(requested_range, size) {
            Ok(range) => range,
            Err(()) => return Ok(range_not_satisfiable(size)),
        }
    } else {
        None
    };

    let (start, end, status) = match parsed_range {
        Some((start, end)) => (start, end, StatusCode::PARTIAL_CONTENT),
        None if size > 0 => (0, size - 1, StatusCode::OK),
        None => (0, 0, StatusCode::OK),
    };
    let length = if size == 0 { 0 } else { end - start + 1 };
    let segments = if size == 0 {
        Vec::new()
    } else {
        relevant_segments(&state.chunks, start, end)?
    };
    let io_guard = if segments.is_empty() {
        None
    } else {
        let guard = acquire_disk_io(
            DiskIoClass::ForegroundRead,
            Some(&state.fragment),
            "bulk export download",
        )
        .await?;
        preflight_segments(&segments).await?;
        Some(guard)
    };

    let content_type = if state.format == "tar" {
        "application/x-tar"
    } else {
        "application/zip"
    };
    let filename = safe_download_filename(&state.filename, &state.format);
    let disposition = HeaderValue::from_str(&format!("attachment; filename=\"{filename}\""))
        .map_err(|error| {
            AppError::InternalServerError(format!("build export download filename: {error}"))
        })?;
    let last_modified = Utc
        .timestamp_opt(state.updated_at_unix, 0)
        .single()
        .map(|value| value.to_rfc2822())
        .unwrap_or_else(|| "Thu, 01 Jan 1970 00:00:00 +0000".to_string());
    let mut builder = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_LENGTH, length.to_string())
        .header(header::CONTENT_DISPOSITION, disposition)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::ETAG, quoted_etag)
        .header(header::LAST_MODIFIED, last_modified)
        .header(header::CACHE_CONTROL, "private, no-cache");
    if status == StatusCode::PARTIAL_CONTENT {
        builder = builder.header(header::CONTENT_RANGE, format!("bytes {start}-{end}/{size}"));
    }
    builder
        .body(if segments.is_empty() {
            Body::empty()
        } else {
            artifact_body(
                segments,
                io_guard.ok_or_else(|| {
                    AppError::InternalServerError(
                        "N1 export download lost disk scheduler admission".to_string(),
                    )
                })?,
                download_lease,
            )
        })
        .map_err(|error| {
            AppError::InternalServerError(format!("build N1 export download response: {error}"))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn range_parser_supports_browser_resume_forms() {
        assert_eq!(strict_range(Some("bytes=10-19"), 100), Ok(Some((10, 19))));
        assert_eq!(strict_range(Some("bytes=90-"), 100), Ok(Some((90, 99))));
        assert_eq!(strict_range(Some("bytes=-10"), 100), Ok(Some((90, 99))));
        assert_eq!(strict_range(Some("bytes=-200"), 100), Ok(Some((0, 99))));
        assert!(strict_range(Some("bytes=100-"), 100).is_err());
        assert!(strict_range(Some("bytes=0-1,4-5"), 100).is_err());
    }

    #[test]
    fn download_filename_cannot_inject_headers() {
        assert_eq!(
            safe_download_filename("report\r\nX-Evil: yes", "zip"),
            "report__X-Evil_ yes.zip"
        );
        assert_eq!(safe_download_filename(" ", "tar"), "export.tar");
    }

    #[tokio::test]
    async fn preflight_requires_the_exact_persisted_chunk_length() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "n1-export-preflight-{}-{unique}",
            std::process::id()
        ));
        tokio::fs::write(&path, b"artifact")
            .await
            .expect("write artifact");
        let valid = ArtifactSegment {
            path: path.clone(),
            file_offset: 0,
            size_bytes: 8,
            file_size_bytes: 8,
            expected_sha256: None,
        };
        assert!(
            preflight_segments(std::slice::from_ref(&valid))
                .await
                .is_ok()
        );
        let invalid = ArtifactSegment {
            file_size_bytes: 7,
            ..valid
        };
        assert!(preflight_segments(&[invalid]).await.is_err());
        let _ = tokio::fs::remove_file(path).await;
    }
}
