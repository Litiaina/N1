use std::path::PathBuf;
use axum::extract::Request;
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::IntoResponse;
use crate::validation::directory_validation::{get_allocated_storage, get_dir_size};

pub async fn enforce_storage_limit(req: Request, next: Next) -> impl IntoResponse {
    let method = req.method();
    let headers = req.headers();

    if matches!(method, &Method::OPTIONS | &Method::HEAD) {
        return next.run(req).await;
    }

    let uid = headers
        .get("uid")
        .and_then(|v| v.to_str().ok());

    let uid = match uid {
        Some(key) if !key.is_empty() => key,
        Some(_) => return (StatusCode::BAD_REQUEST, "Header 'uid' cannot be empty").into_response(),
        None => return (StatusCode::BAD_REQUEST, "Missing required header: 'uid'").into_response(),
    };

    let allocated_storage = match get_allocated_storage(uid).await {
        Ok(Some(level)) => level,
        Ok(None) => return (StatusCode::FORBIDDEN, "Allocated storage not assigned for this user").into_response(),
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "Error fetching allocated storage").into_response(),
    };

    let path = PathBuf::from(format!("storage-files/{}", uid));
    let total_storage = get_dir_size(&path).await;

    if total_storage >= allocated_storage {
        return (StatusCode::INSUFFICIENT_STORAGE, "Your personal storage quota has been exceeded").into_response();
    }

    next.run(req).await
}
