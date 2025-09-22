use std::sync::Arc;
use crate::fs::direct_upload::upload_file;
use crate::fs::directory_manager::{
    create_directory, delete_directory, delete_file, get_file_metadata, rename_file_query,
};
use crate::fs::directory_viewer::{
    stream_file, stream_file_v2, stream_hls_file, view_directory, view_file,
};
use crate::fs::download_manager::download_file;
use crate::fs::fragmented_upload::{check_uploaded_fragments, upload_fragments};
use crate::fs::stateless_fragmented_upload as legacy_upload;
use crate::handlers::dir_info_handlers::get_storage_capacity;
use crate::handlers::transcoder_handler::{transcode_status_handler, transcode_video_handler};
use crate::middleware::auth_middleware::{auth, authorize};
use crate::middleware::storage_middleware::enforce_storage_limit;
use crate::object_storage::garbage_collector::run_garbage_collector;
use crate::statics::database_statics::DATABASE_URL;
use axum::http::{HeaderName, Method, StatusCode};
use axum::middleware::from_fn;
use axum::routing::{delete, get, patch, post, put};
use axum::{extract::DefaultBodyLimit, Router};
use mongodb::options::ClientOptions;
use mongodb::Client;
use std::time::Duration;
use tower_http::cors::{Any, CorsLayer};
use crate::fs::fs_constants::MAX_FILE_SIZE;
use crate::object_storage::delete::{delete_object, delete_objects_by_prefix};
use crate::object_storage::download::{download_object, download_object_chunk};
use crate::object_storage::retrieve::{stream_object, get_object_metadata, list_objects, get_object, list_fragments};
use crate::object_storage::modify::rename_object;
use crate::object_storage::upload::{finalize_object_upload, get_capacity_usage, get_object_upload_status, initiate_object_upload, object_upload_chunk, object_upload_chunk_raw};
use crate::object_storage::sharing::{create_signed_url, get_shared_object};

pub struct AppState {
    pub db_client: Arc<Client>,
}

pub async fn serve_storage_routes() -> Router {
    let client_options = ClientOptions::parse(&*DATABASE_URL)
        .await
        .expect("Failed to parse MongoDB URI");
    let db_client = Arc::new(Client::with_options(client_options).expect("Failed to create MongoDB client"));
    let app_state = 
    Arc::new(AppState { db_client: Arc::clone(&db_client) });

    tokio::spawn(async move {
        run_garbage_collector(Arc::clone(&db_client)).await;
    });

    let cors_layer = CorsLayer::new()
        .allow_origin(Any)
        .allow_headers([
            HeaderName::from_static("authorization"),
            HeaderName::from_static("uid"),
            HeaderName::from_static("content-type"),
            HeaderName::from_static("digest"),
            HeaderName::from_static("buffer"),
        ])
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PATCH,
            Method::PUT,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .max_age(Duration::from_secs(86400));

    Router::new()
        .route("/ping", get(ping).options(options_handler))
        .route("/authenticate", post(authorize))
        .merge(create_object_storage_routes())
        .merge(create_legacy_fs_routes())
        .with_state(Arc::clone(&app_state))
        .layer(DefaultBodyLimit::max(MAX_FILE_SIZE))
        .layer(cors_layer)
}

fn create_object_storage_routes() -> Router<Arc<AppState>> {
    let protected_routes = Router::new()
        .route("/v4/capacity/usage", get(get_capacity_usage).options(options_handler))
        .route("/v4/fragment/object/share", post(create_signed_url))
        .route("/v4/fragment/metadata", get(list_fragments).options(options_handler))
        .route("/v4/fragment/object/metadata/{:fragment}", get(list_objects).options(options_handler))
        .route("/v4/fragment/object/get/{:fragment}/{*object_key}", get(get_object).options(options_handler))
        .route("/v4/fragment/object/stream/{:fragment}/{*object_key}", get(stream_object).options(options_handler))
        .route("/v4/fragment/object/get/metadata/{:fragment}/{*object_key}", get(get_object_metadata).options(options_handler))
        .route("/v4/fragment/object/download/{:fragment}/{*object_key}", get(download_object).options(options_handler))
        .route("/v4/fragment/object/download/v2/{:fragment}/{*object_key}", get(download_object_chunk).options(options_handler))
        .route("/v4/fragment/object/rename", patch(rename_object).options(options_handler))
        .route("/v4/fragment/object/delete/{:fragment}/{*object_key}", delete(delete_object).options(options_handler))
        .route("/v4/fragment/object/delete/prefix", delete(delete_objects_by_prefix).options(options_handler))
        .route("/v4/fragment/object/upload/init/{:fragment}/{*object_key}", post(initiate_object_upload).options(options_handler))
        .route("/v4/fragment/object/upload/chunk/{:file_id}/{:chunk_index}", put(object_upload_chunk).options(options_handler))
        .route("/v4/fragment/object/upload/chunk/raw/{:file_id}/{:chunk_index}", put(object_upload_chunk_raw).options(options_handler))
        .route("/v4/fragment/object/upload/status/{:file_id}", get(get_object_upload_status).options(options_handler))
        .route("/v4/fragment/object/upload/finalize/{:file_id}", post(finalize_object_upload).options(options_handler))
        .layer(from_fn(auth));

    Router::new()
        .route("/v4/public/share", get(get_shared_object))
        .merge(protected_routes)
}

fn create_legacy_fs_routes() -> Router<Arc<AppState>> {
    let upload_routes = Router::new()
        .route("/dir/file/upload/", post(upload_file).options(options_handler))
        .route("/dir/file/upload/{*path}", post(upload_file).options(options_handler))
        .route("/dir/file/upload/v2/", post(upload_fragments).options(options_handler))
        .route("/dir/file/upload/v2/{*path}", post(upload_fragments).options(options_handler))
        .route("/dir/file/upload/v3/initiate/", post(legacy_upload::initiate_upload_session).options(options_handler))
        .route("/dir/file/upload/v3/initiate/{*path}", post(legacy_upload::initiate_upload_session).options(options_handler))
        .route("/dir/file/upload/v3/{:file_id}/{:chunk_index}", put(legacy_upload::upload_chunk).options(options_handler))
        .route("/dir/file/upload/v3/{:file_id}/status", get(legacy_upload::get_upload_status).options(options_handler))
        .route("/dir/file/upload/v3/{:file_id}/finalize", post(legacy_upload::finalize_upload_session).options(options_handler))
        .layer(from_fn(enforce_storage_limit));

    let transcoding_routes = Router::new()
        .route("/dir/file/transcode/video", post(transcode_video_handler))
        .route("/dir/file/transcode/status", get(transcode_status_handler))
        .layer(from_fn(auth));

    let protected_management_routes = Router::new()
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

    Router::new()
        .route("/dir/file/upload/check/{*path}", get(check_uploaded_fragments))
        .route("/dir/file/view/{*path}", get(view_file))
        .route("/dir/file/stream/{*path}", get(stream_file))
        .route("/dir/file/stream/v2/{*path}", get(stream_file_v2))
        .route("/dir/file/transcode/stream/hls/{*path}", get(stream_hls_file))
        .route("/dir/file/metadata/", get(get_file_metadata))
        .route("/dir/file/metadata/{*path}", get(get_file_metadata))
        .route("/dir/file/download/", get(download_file))
        .route("/dir/file/download/{*path}", get(download_file))
        .merge(transcoding_routes)
        .merge(protected_management_routes)
}

async fn ping() -> &'static str {
    "pong"
}

async fn options_handler() -> StatusCode {
    StatusCode::OK
}
