use crate::structs::auth_structs::FileSystemClaims;
use crate::validation::directory_validation::{get_allocated_storage, get_dir_size};
use axum::extract::Request;
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::IntoResponse;
use std::path::PathBuf;

pub async fn enforce_storage_limit(
    claims: FileSystemClaims,
    req: Request,
    next: Next,
) -> impl IntoResponse {
    let method = req.method();

    if matches!(method, &Method::OPTIONS | &Method::HEAD) {
        return next.run(req).await;
    }

    let path = Some(claims.path.as_str());

    let path = match path {
        Some(key) if !key.is_empty() => key,
        Some(_) => {
            return (StatusCode::BAD_REQUEST, "Header 'path' cannot be empty").into_response();
        }
        None => {
            return (StatusCode::BAD_REQUEST, "Missing required header: 'path'").into_response();
        }
    };

    let allocated_storage = match get_allocated_storage(path).await {
        Ok(Some(level)) => level,
        Ok(None) => {
            return (
                StatusCode::FORBIDDEN,
                "Allocated storage not assigned for this user",
            )
                .into_response();
        }
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Error fetching allocated storage",
            )
                .into_response();
        }
    };

    let path = PathBuf::from(format!("{}", path));
    let total_storage = get_dir_size(&path).await;

    if total_storage >= allocated_storage {
        return (
            StatusCode::INSUFFICIENT_STORAGE,
            "Your personal storage capacity has been exceeded",
        )
            .into_response();
    }

    next.run(req).await
}
