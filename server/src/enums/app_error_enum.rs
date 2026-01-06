use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use mongodb::error::Error as MongoDbError;
use std::io;

use crate::handlers::error_handlers;

#[derive(Debug)]
pub enum AppError {
    Io(io::Error, String),
    Timeout(String),
    BadRequest(String),
    Conflict(String),
    PayloadTooLarge(String),
    Unauthorized,
    Multipart(String),
    RangeNotSatisfiable,
    InternalServerError(String),
    InsufficientStorage,
    MongoDb(MongoDbError),
    NotFound,
}

impl From<MongoDbError> for AppError {
    fn from(err: MongoDbError) -> Self {
        AppError::MongoDb(err)
    }
}

impl From<mongodb::bson::ser::Error> for AppError {
    fn from(err: mongodb::bson::ser::Error) -> Self {
        AppError::InternalServerError(format!("BSON serialization error: {}", err))
    }
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
            AppError::RangeNotSatisfiable => write!(f, "Range not satisfiable"),
            AppError::InternalServerError(msg) => write!(f, "Internal server error: {}", msg),
            AppError::InsufficientStorage => write!(f, "Insufficient storage capacity"),
            AppError::MongoDb(err) => write!(f, "Database error: {}", err),
            AppError::NotFound => write!(f, "Not found"),
        }
    }
}

impl std::error::Error for AppError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            AppError::Io(err, _) => Some(err),
            AppError::MongoDb(err) => Some(err),
            _ => None,
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        match self {
            AppError::NotFound => {
                (StatusCode::NOT_FOUND, Html(error_handlers::NOT_FOUND_HTML)).into_response()
            }
            AppError::Io(e, context) => {
                tracing::error!("IO error during '{}': {}", context, e);
                let message = format!("IO operation failed: {}", context);
                (StatusCode::INTERNAL_SERVER_ERROR, message).into_response()
            }
            AppError::Timeout(context) => {
                tracing::error!("Timeout during '{}'", context);
                let message = format!("Operation timed out: {}", context);
                (StatusCode::REQUEST_TIMEOUT, message).into_response()
            }
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg).into_response(),
            AppError::Conflict(msg) => (StatusCode::CONFLICT, msg).into_response(),
            AppError::PayloadTooLarge(msg) => (StatusCode::PAYLOAD_TOO_LARGE, msg).into_response(),
            AppError::Multipart(msg) => {
                tracing::error!("Multipart error: {}", msg);
                (StatusCode::BAD_REQUEST, msg).into_response()
            }
            AppError::Unauthorized => {
                (StatusCode::UNAUTHORIZED, "Unauthorized".to_string()).into_response()
            }
            AppError::RangeNotSatisfiable => (
                StatusCode::RANGE_NOT_SATISFIABLE,
                "Range Not Satisfiable".to_string(),
            )
                .into_response(),
            AppError::InternalServerError(msg) => {
                tracing::error!("Internal Server Error: {}", msg);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "An internal server error occurred.".to_string(),
                )
                    .into_response()
            }
            AppError::InsufficientStorage => (
                StatusCode::INSUFFICIENT_STORAGE,
                "Insufficient storage capacity.".to_string(),
            )
                .into_response(),
            AppError::MongoDb(err) => {
                tracing::error!("Database Error: {}", err);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "A database error occurred.".to_string(),
                )
                    .into_response()
            }
        }
    }
}
