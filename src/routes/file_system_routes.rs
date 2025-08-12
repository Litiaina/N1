use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use axum::{extract::DefaultBodyLimit, http::Method, routing::post, Router};
use axum::http::{HeaderName, StatusCode};
use axum::middleware::from_fn;
use axum::routing::{delete, get, put};
use mongodb::Client;
use mongodb::options::ClientOptions;
use tokio::sync::Mutex;
use tower_http::cors::{Any, CorsLayer};
use crate::database::database_constants::{DATABASE, DATABASE_URL};
use crate::fs::direct_upload::upload_file;
use crate::fs::directory_manager::{create_directory, delete_directory, delete_file, get_file_metadata, rename_file_query};
use crate::fs::directory_viewer::{stream_file, stream_file_v2, stream_hls_file, view_directory, view_file};
use crate::fs::download_manager::download_file;
use crate::fs::fragmented_upload::{check_uploaded_fragments, upload_fragments};
use crate::handlers::dir_info_handlers::get_storage_capacity;
use crate::handlers::transcoder_handler::{transcode_status_handler, transcode_video_handler};
use crate::middleware::auth_middleware::{auth, authorize};
use crate::middleware::storage_middleware::enforce_storage_limit;
use crate::types::shared_state_type::SharedState;

const MAX_FILE_SIZE: usize = 5 * 1024 * 1024 * 1024;

pub async fn serve_file_system_routes() -> Router {
    let state: SharedState = Arc::new(Mutex::new(HashMap::new()));

    let client_options = ClientOptions::parse(DATABASE_URL)
        .await
        .expect("Failed to parse MongoDB URI");
    let client = Client::with_options(client_options).expect("Failed to create MongoDB client");
    let db = client.database(DATABASE);

    let upload_routes = Router::new()
        .route("/dir/file/upload/", post(upload_file))
        .route("/dir/file/upload/{*path}", post(upload_file))
        .route("/dir/file/upload/v2/", post(upload_fragments)).with_state(Arc::clone(&state))
        .route("/dir/file/upload/v2/{*path}", post(upload_fragments)).with_state(Arc::clone(&state))
        .layer(from_fn(enforce_storage_limit));

    let protected_routes = Router::new()
        .route("/dir/", get(view_directory))
        .route("/dir/{*path}", get(view_directory))
        .route("/dir/create/{*path}", post(create_directory))
        .route("/dir/delete/{*path}", delete(delete_directory))
        .route("/dir/file/rename/", put(rename_file_query))
        .route("/dir/file/rename/{*path}", put(rename_file_query))
        .route("/dir/file/delete/", delete(delete_file))
        .route("/dir/file/delete/{*path}", delete(delete_file))
        .route("/dir/capacity", get(get_storage_capacity))
        .merge(upload_routes)
        .layer(from_fn(auth));

    let protected_transcoding_routes = Router::new()
        .route("/dir/file/transcode/video", post(transcode_video_handler))
        .route("/dir/file/transcode/status", get(transcode_status_handler))
        .with_state(db)
        .layer(from_fn(auth));

    Router::new()
        .route("/ping", get(ping).options(|| async { StatusCode::OK }))
        .route("/authenticate", post(authorize))
        .route("/dir/file/upload/check/{*path}", get(check_uploaded_fragments)).with_state(Arc::clone(&state))
        .route("/dir/file/view/{*path}", get(view_file))
        .route("/dir/file/stream/{*path}", get(stream_file))
        .route("/dir/file/stream/v2/{*path}", get(stream_file_v2))
        .route("/dir/file/transcode/stream/hls/{*path}", get(stream_hls_file))
        .route("/dir/file/metadata/", get(get_file_metadata))
        .route("/dir/file/metadata/{*path}", get(get_file_metadata))
        .route("/dir/file/download/", get(download_file))
        .route("/dir/file/download/{*path}", get(download_file))
        .merge(protected_transcoding_routes)
        .merge(protected_routes)
        .layer(DefaultBodyLimit::max(MAX_FILE_SIZE))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_headers([
                    HeaderName::from_static("authorization"),
                    HeaderName::from_static("uid"),
                    HeaderName::from_static("content-type"),
                ])
                .allow_methods([
                    Method::GET,
                    Method::POST,
                    Method::PATCH,
                    Method::PUT,
                    Method::DELETE,
                    Method::OPTIONS,
                ])
                .max_age(Duration::from_secs(86400)),
        )
}

async fn ping() -> &'static str {
    "pong"
}
