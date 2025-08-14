use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use std::io;

#[derive(Debug)]
pub enum AppError {
    Io(io::Error, String),
    Timeout(String),
    BadRequest(String),
    Conflict(String),
    PayloadTooLarge(String),
    Unauthorized,
    Multipart(String),
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppError::Io(err, context) => write!(f, "IO error during '{}': {}", context, err),
            AppError::Timeout(context) => write!(f, "Operation timed out: {}", context),
            AppError::BadRequest(msg) => write!(f, "Bad request: {}", msg),
            AppError::Conflict(msg) => write!(f, "Conflict: {}", msg),
            AppError::PayloadTooLarge(msg) => write!(f, "Payload too large: {}", msg),
            AppError::Unauthorized => write!(f, "Unauthorized access"),
            AppError::Multipart(msg) => write!(f, "Multipart error: {}", msg),
        }
    }
}

impl std::error::Error for AppError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            AppError::Io(err, _) => Some(err),
            _ => None,
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AppError::Io(e, context) => {
                tracing::error!("IO error during '{}': {}", context, e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("IO operation failed: {}", context),
                )
            }
            AppError::Timeout(context) => {
                tracing::error!("Timeout during '{}'", context);
                (
                    StatusCode::REQUEST_TIMEOUT,
                    format!("Operation timed out: {}", context),
                )
            }
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            AppError::Conflict(msg) => (StatusCode::CONFLICT, msg),
            AppError::PayloadTooLarge(msg) => (StatusCode::PAYLOAD_TOO_LARGE, msg),
            AppError::Multipart(msg) => {
                tracing::error!("Multipart error: {}", msg);
                (StatusCode::BAD_REQUEST, msg)
            }
            AppError::Unauthorized => (StatusCode::UNAUTHORIZED, "Unauthorized".to_string()),
        };

        (status, message).into_response()
    }
}
