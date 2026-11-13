use crate::{Error, object_store::ObjectStoreError, service::ServiceError};
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};

#[derive(Debug)]
pub struct ApiError(pub StatusCode, pub String);
pub type ApiResult<T> = Result<T, ApiError>;
impl ApiError {
    pub fn invalid(message: impl Into<String>) -> Self {
        Self(StatusCode::BAD_REQUEST, message.into())
    }
    pub fn unauthorized() -> Self {
        Self(StatusCode::UNAUTHORIZED, "Authentication required".into())
    }
    pub fn forbidden() -> Self {
        Self(StatusCode::FORBIDDEN, "Permission denied".into())
    }
}
impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.1.fmt(f)
    }
}
impl std::error::Error for ApiError {}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(serde_json::json!({"error": self.1}))).into_response()
    }
}
impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        let status = match &error {
            Error::NotFound => StatusCode::NOT_FOUND,
            Error::Invalid(_) => StatusCode::BAD_REQUEST,
            Error::Conflict | Error::NameConflict(_) => StatusCode::CONFLICT,
            Error::Busy | Error::Closed => StatusCode::SERVICE_UNAVAILABLE,
            Error::Io(error) | Error::Storage(m1_storage::Error::Io(error))
                if storage_full(error) =>
            {
                StatusCode::INSUFFICIENT_STORAGE
            }
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let message = match status {
            StatusCode::INTERNAL_SERVER_ERROR => "Metadata storage operation failed".into(),
            StatusCode::INSUFFICIENT_STORAGE => "Metadata storage space exhausted".into(),
            _ => error.to_string(),
        };
        Self(status, message)
    }
}
impl From<ObjectStoreError> for ApiError {
    fn from(error: ObjectStoreError) -> Self {
        Self(
            StatusCode::from_u16(error.status()).unwrap_or(StatusCode::BAD_GATEWAY),
            error.to_string(),
        )
    }
}
impl From<ServiceError> for ApiError {
    fn from(error: ServiceError) -> Self {
        match error {
            ServiceError::Metadata(e) => e.into(),
            ServiceError::Object(e) => e.into(),
        }
    }
}
impl From<std::io::Error> for ApiError {
    fn from(error: std::io::Error) -> Self {
        if storage_full(&error) {
            Self(
                StatusCode::INSUFFICIENT_STORAGE,
                "Metadata staging storage space exhausted".into(),
            )
        } else {
            Self(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Metadata staging I/O failed".into(),
            )
        }
    }
}

fn storage_full(error: &std::io::Error) -> bool {
    matches!(error.raw_os_error(), Some(libc::ENOSPC | libc::EDQUOT))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_exhaustion_has_an_explicit_status_without_disclosing_private_paths() {
        for code in [libc::ENOSPC, libc::EDQUOT] {
            let raw = || std::io::Error::from_raw_os_error(code);
            for error in [
                ApiError::from(raw()),
                ApiError::from(Error::Io(raw())),
                ApiError::from(Error::Storage(m1_storage::Error::Io(raw()))),
            ] {
                assert_eq!(error.0, StatusCode::INSUFFICIENT_STORAGE);
                assert!(error.1.contains("space exhausted"));
            }
        }
        let other = ApiError::from(std::io::Error::other("/private/staging/path"));
        assert_eq!(other.0, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!other.1.contains("/private"));
    }
}
