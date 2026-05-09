use crate::distribution_core::model::{ClusterCommitMode, PutObjectQuery};
use crate::enums::app_error_enum::AppError;
use axum::http::{
    HeaderMap,
    header::{CONTENT_TYPE, RANGE},
};

pub(crate) fn parse_single_byte_range(
    headers: &HeaderMap,
    total_size_bytes: u64,
) -> Result<Option<(u64, u64)>, AppError> {
    let Some(raw) = headers.get(RANGE).and_then(|v| v.to_str().ok()) else {
        return Ok(None);
    };
    let range = raw.trim();
    if !range.starts_with("bytes=") {
        return Err(AppError::RangeNotSatisfiable);
    }
    let spec = &range["bytes=".len()..];
    if spec.contains(',') {
        return Err(AppError::RangeNotSatisfiable);
    }
    let Some((start_raw, end_raw)) = spec.split_once('-') else {
        return Err(AppError::RangeNotSatisfiable);
    };
    if total_size_bytes == 0 {
        return Err(AppError::RangeNotSatisfiable);
    }

    let (start, end) = if start_raw.is_empty() {
        let suffix = end_raw
            .parse::<u64>()
            .map_err(|_| AppError::RangeNotSatisfiable)?;
        if suffix == 0 {
            return Err(AppError::RangeNotSatisfiable);
        }
        let len = suffix.min(total_size_bytes);
        let start = total_size_bytes.saturating_sub(len);
        (start, total_size_bytes - 1)
    } else {
        let start = start_raw
            .parse::<u64>()
            .map_err(|_| AppError::RangeNotSatisfiable)?;
        let end = if end_raw.is_empty() {
            total_size_bytes - 1
        } else {
            end_raw
                .parse::<u64>()
                .map_err(|_| AppError::RangeNotSatisfiable)?
        };
        (start, end.min(total_size_bytes - 1))
    };

    if start > end || start >= total_size_bytes {
        return Err(AppError::RangeNotSatisfiable);
    }

    Ok(Some((start, end)))
}

pub(crate) fn content_type_from_query_or_headers(
    query: &PutObjectQuery,
    headers: &HeaderMap,
) -> String {
    if let Some(v) = query.content_type.as_ref() {
        return v.to_string();
    }
    headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string()
}

pub(crate) fn required_cluster_commit_shards(
    commit_mode: ClusterCommitMode,
    kc: u8,
    planned_shards: usize,
) -> usize {
    match commit_mode {
        ClusterCommitMode::Quorum => usize::from(kc),
        ClusterCommitMode::StrictFullPlacement => planned_shards.max(usize::from(kc)),
    }
}
