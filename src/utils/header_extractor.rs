use axum::http::{HeaderMap, StatusCode};

pub fn extract_uid_from_header(header: &HeaderMap) -> Result<String, StatusCode> {
    header.get("uid")
        .and_then(|value| value.to_str().ok())
        .map(|s| s.to_string())
        .ok_or(StatusCode::BAD_REQUEST)
}
