use crate::structs::auth_structs::FileSystemClaims;
use crate::structs::capacity_struct::CapacityDetailParams;
use crate::validation::directory_validation::{get_allocated_storage, get_dir_size};
use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::json;
use std::path::PathBuf;

pub async fn get_storage_capacity(
    claims: FileSystemClaims,
    Query(params): Query<CapacityDetailParams>,
) -> impl IntoResponse {
    let allocated_storage = match get_allocated_storage(claims.path.as_str()).await {
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

    let path = PathBuf::from(format!("{}", params.uid));
    let total_storage = get_dir_size(&path).await;

    Json(json!({ "uid": params.uid, "capacity": allocated_storage, "used": total_storage }))
        .into_response()
}
