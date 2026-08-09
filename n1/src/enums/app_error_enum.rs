use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use std::io;

use crate::handlers::error_handlers;

#[derive(Debug)]
pub enum AppError {
    Io(io::Error, String),
    Timeout(String),
    ServiceUnavailable(String),
    BadRequest(String),
    Conflict(String),
    PayloadTooLarge(String),
    Unauthorized,
    Forbidden(String),
    Multipart(String),
    RangeNotSatisfiable,
    InternalServerError(String),
    InsufficientStorage,
    NotFound,
}

impl AppError {
    pub fn is_capacity_error(&self) -> bool {
        match self {
            Self::InsufficientStorage => true,
            Self::Io(error, _) => matches!(
                error.raw_os_error(),
                Some(libc::ENOSPC) | Some(libc::EDQUOT)
            ),
            _ => false,
        }
    }

    pub fn status_code(&self) -> StatusCode {
        if self.is_capacity_error() {
            return StatusCode::INSUFFICIENT_STORAGE;
        }
        match self {
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Timeout(_) => StatusCode::REQUEST_TIMEOUT,
            Self::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::BadRequest(_) | Self::Multipart(_) => StatusCode::BAD_REQUEST,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::RangeNotSatisfiable => StatusCode::RANGE_NOT_SATISFIABLE,
            Self::Io(_, _) | Self::InternalServerError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InsufficientStorage => StatusCode::INSUFFICIENT_STORAGE,
        }
    }
}

impl From<rmp_serde::encode::Error> for AppError {
    fn from(err: rmp_serde::encode::Error) -> Self {
        AppError::InternalServerError(format!("MessagePack serialization error: {}", err))
    }
}

impl From<rmp_serde::decode::Error> for AppError {
    fn from(err: rmp_serde::decode::Error) -> Self {
        AppError::InternalServerError(format!("MessagePack deserialization error: {}", err))
    }
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppError::Io(err, context) => write!(f, "IO error during '{}': {}", context, err),
            AppError::Timeout(context) => write!(f, "Operation timed out: {}", context),
            AppError::ServiceUnavailable(msg) => write!(f, "Service unavailable: {}", msg),
            AppError::BadRequest(msg) => write!(f, "Bad request: {}", msg),
            AppError::Conflict(msg) => write!(f, "Conflict: {}", msg),
            AppError::PayloadTooLarge(msg) => write!(f, "Payload too large: {}", msg),
            AppError::Unauthorized => write!(f, "Unauthorized access"),
            AppError::Forbidden(msg) => write!(f, "Forbidden: {}", msg),
            AppError::Multipart(msg) => write!(f, "Multipart error: {}", msg),
            AppError::RangeNotSatisfiable => write!(f, "Range not satisfiable"),
            AppError::InternalServerError(msg) => write!(f, "Internal server error: {}", msg),
            AppError::InsufficientStorage => write!(f, "Insufficient storage capacity"),
            AppError::NotFound => write!(f, "Not found"),
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
        match self {
            AppError::NotFound => {
                (StatusCode::NOT_FOUND, Html(error_handlers::NOT_FOUND_HTML)).into_response()
            }
            AppError::Io(e, context) => {
                tracing::error!("IO error during '{}': {}", context, e);
                if matches!(e.raw_os_error(), Some(libc::ENOSPC) | Some(libc::EDQUOT)) {
                    return (
                        StatusCode::INSUFFICIENT_STORAGE,
                        format!("Insufficient storage capacity during {context}."),
                    )
                        .into_response();
                }
                let message = format!("IO operation failed: {}", context);
                (StatusCode::INTERNAL_SERVER_ERROR, message).into_response()
            }
            AppError::Timeout(context) => {
                tracing::error!("Timeout during '{}'", context);
                let message = format!("Operation timed out: {}", context);
                (StatusCode::REQUEST_TIMEOUT, message).into_response()
            }
            AppError::ServiceUnavailable(msg) => {
                tracing::warn!("Service unavailable: {}", msg);
                (StatusCode::SERVICE_UNAVAILABLE, msg).into_response()
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
            AppError::Forbidden(msg) => (StatusCode::FORBIDDEN, msg).into_response(),
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AppError;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    #[test]
    fn service_unavailable_maps_to_503() {
        let response = AppError::ServiceUnavailable("busy".to_string()).into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn kernel_capacity_errors_map_to_507() {
        for errno in [libc::ENOSPC, libc::EDQUOT] {
            let response = AppError::Io(
                std::io::Error::from_raw_os_error(errno),
                "write object shard".to_string(),
            )
            .into_response();
            assert_eq!(response.status(), StatusCode::INSUFFICIENT_STORAGE);
        }
    }
}
