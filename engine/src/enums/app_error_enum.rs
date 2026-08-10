use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use std::io;

#[derive(Serialize)]
struct ErrorResponseBody {
    error: &'static str,
    message: String,
}

fn error_response(status: StatusCode, error: &'static str, message: String) -> Response {
    (status, Json(ErrorResponseBody { error, message })).into_response()
}

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
            AppError::NotFound => error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                "Resource not found".into(),
            ),
            AppError::Io(e, context) => {
                tracing::error!("IO error during '{}': {}", context, e);
                let message = format!("IO operation failed: {}", context);
                error_response(StatusCode::INTERNAL_SERVER_ERROR, "io_error", message)
            }
            AppError::Timeout(context) => {
                tracing::error!("Timeout during '{}'", context);
                let message = format!("Operation timed out: {}", context);
                error_response(StatusCode::REQUEST_TIMEOUT, "timeout", message)
            }
            AppError::ServiceUnavailable(msg) => {
                tracing::warn!("Service unavailable: {}", msg);
                error_response(StatusCode::SERVICE_UNAVAILABLE, "service_unavailable", msg)
            }
            AppError::BadRequest(msg) => {
                error_response(StatusCode::BAD_REQUEST, "bad_request", msg)
            }
            AppError::Conflict(msg) => error_response(StatusCode::CONFLICT, "conflict", msg),
            AppError::PayloadTooLarge(msg) => {
                error_response(StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large", msg)
            }
            AppError::Multipart(msg) => {
                tracing::error!("Multipart error: {}", msg);
                error_response(StatusCode::BAD_REQUEST, "multipart_error", msg)
            }
            AppError::Unauthorized => error_response(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "Unauthorized".into(),
            ),
            AppError::Forbidden(msg) => error_response(StatusCode::FORBIDDEN, "forbidden", msg),
            AppError::RangeNotSatisfiable => error_response(
                StatusCode::RANGE_NOT_SATISFIABLE,
                "range_not_satisfiable",
                "Range not satisfiable".into(),
            ),
            AppError::InternalServerError(msg) => {
                tracing::error!("Internal Server Error: {}", msg);
                error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_server_error",
                    "An internal server error occurred".into(),
                )
            }
            AppError::InsufficientStorage => error_response(
                StatusCode::INSUFFICIENT_STORAGE,
                "insufficient_storage",
                "Insufficient storage capacity".into(),
            ),
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

    #[tokio::test]
    async fn not_found_is_a_structured_json_error() {
        let response = AppError::NotFound.into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("application/json")
        );
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .expect("error body");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).expect("json"),
            serde_json::json!({"error":"not_found","message":"Resource not found"})
        );
    }
}
