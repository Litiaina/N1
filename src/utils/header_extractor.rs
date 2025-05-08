use axum::http::{HeaderMap, StatusCode};
use crate::validation::database_validation::validate_uid;

pub async fn extract_uid_from_header(header: &HeaderMap) -> Result<String, StatusCode> {
    let email = header
        .get("uid")
        .and_then(|value| value.to_str().ok());

    if let Some(key) = email {
        match validate_uid(&key).await {
            Ok(true) => Ok(key.to_string()),
            Ok(false) => Err(StatusCode::BAD_REQUEST),
            Err(_) => Err(StatusCode::BAD_REQUEST),
        }
    } else {
        Err(StatusCode::BAD_REQUEST)
    }
}
