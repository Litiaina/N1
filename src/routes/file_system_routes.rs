use std::collections::HashMap;
use std::sync::Arc;
use axum::{
    extract::DefaultBodyLimit,
    http::{header::CONTENT_TYPE, Method},
    routing::post,
    Router,
};
use tokio::sync::Mutex;
use tower_http::cors::{Any, CorsLayer};

use crate::fs::{direct_upload, fragmented_upload};
use crate::types::shared_state_type::SharedState;

const MAX_FILE_SIZE: usize = 5 * 1024 * 1024 * 1024;

pub fn serve_file_system_routes() -> Router {
    let state: SharedState = Arc::new(Mutex::new(HashMap::new()));
    Router::new()
        .route("/files/upload/{*path}", post(direct_upload::upload_file))
        .route("/files/upload/v2/{*path}", post(fragmented_upload::upload_fragments)).with_state(state)
        .route("/files/finalize/{*path}", post(fragmented_upload::finalize_merged_fragments))
        .layer(DefaultBodyLimit::max(MAX_FILE_SIZE))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([
                    Method::GET,
                    Method::POST,
                    Method::PATCH,
                    Method::PUT,
                    Method::DELETE,
                ])
                .allow_headers([CONTENT_TYPE]),
        )
}