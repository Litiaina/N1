use axum::http::{HeaderMap, StatusCode};
use crate::enums::app_error_enum::AppError;

pub fn extract_uid_from_header(header: &HeaderMap) -> Result<String, StatusCode> {
    header.get("uid")
        .and_then(|value| value.to_str().ok())
        .map(|s| s.to_string())
        .ok_or(StatusCode::BAD_REQUEST)
}

pub fn extract_uid_from_fragmented_header(header: &HeaderMap) -> Result<String, AppError> {
    header
        .get("uid")
        .and_then(|v| v.to_str().ok())
        .map(String::from)
        .ok_or(AppError::Unauthorized)
}
