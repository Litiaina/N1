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
