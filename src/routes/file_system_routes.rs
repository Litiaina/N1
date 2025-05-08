use std::collections::HashMap;
use std::sync::Arc;
use axum::{
    extract::DefaultBodyLimit,
    http::Method,
    routing::post,
    Router,
};
use axum::http::HeaderName;
use axum::routing::{delete, get, put};
use tokio::sync::Mutex;
use tower_http::cors::{Any, CorsLayer};
use crate::fs::direct_upload::upload_file;
use crate::fs::directory_manager::{create_directory, delete_directory, delete_file, rename_file_query};
use crate::fs::directory_viewer::{stream_file, view_directory, view_file};
use crate::fs::download_manager::download_file;
use crate::fs::fragmented_upload::{check_uploaded_fragments, finalize_assembled_fragments, upload_fragments};
use crate::types::shared_state_type::SharedState;

const MAX_FILE_SIZE: usize = 5 * 1024 * 1024 * 1024;

pub fn serve_file_system_routes() -> Router {

    let state: SharedState = Arc::new(Mutex::new(HashMap::new()));

    Router::new()
        .route("/files/{*path}", get(view_directory))
        .route("/files/directory/create/{*path}", post(create_directory))
        .route("/files/directory/delete/{*path}", delete(delete_directory))
        .route("/files/upload/{*path}", post(upload_file))
        .route("/files/upload/v2/{*path}", post(upload_fragments)).with_state(state)
        .route("/files/upload/check/{*path}", get(check_uploaded_fragments))
        .route("/files/finalize/{*path}", post(finalize_assembled_fragments))
        .route("/files/view/{*path}", get(view_file))
        .route("/files/stream/{*path}", get(stream_file))
        .route("/files/download/{*path}", get(download_file))
        .route("/files/rename/{*path}", put(rename_file_query))
        .route("/files/delete/{*path}", delete(delete_file))
        .layer(DefaultBodyLimit::max(MAX_FILE_SIZE))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_headers([
                    HeaderName::from_static("x-api-key"),
                    HeaderName::from_static("content-type"),
                ])
                .allow_methods([
                    Method::GET,
                    Method::POST,
                    Method::PATCH,
                    Method::PUT,
                    Method::DELETE,
                    Method::OPTIONS
                ])
        )
}
