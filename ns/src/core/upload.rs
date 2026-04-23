use crate::core::disk_manager::InitiateUploadParams;
use crate::enums::app_error_enum::AppError;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use axum::Json as AxumJson;
use axum::body::Body;
use axum::extract::{Path as AxumPath, State};
use axum::response::Response;
use std::sync::Arc;

fn direct_multipart_unavailable() -> AppError {
    AppError::NotFound
}

pub async fn initiate_object_upload(
    _claims: Claims,
    _path: AxumPath<(String, String)>,
    _body: AxumJson<InitiateUploadParams>,
) -> Result<Response, AppError> {
    Err(direct_multipart_unavailable())
}

pub async fn object_upload_part(
    _claims: Claims,
    _headers: axum::http::HeaderMap,
    _path: AxumPath<(String, String, u64)>,
    _body: Body,
) -> Result<Response, AppError> {
    Err(direct_multipart_unavailable())
}

pub async fn finalize_object_upload(
    _state: State<Arc<AppState>>,
    _claims: Claims,
    _path: AxumPath<(String, String, String)>,
    _body: AxumJson<InitiateUploadParams>,
) -> Result<Response, AppError> {
    Err(direct_multipart_unavailable())
}

pub async fn cancel_upload_session(
    _claims: Claims,
    _path: AxumPath<(String, String)>,
) -> Result<Response, AppError> {
    Err(direct_multipart_unavailable())
}
