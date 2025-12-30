use crate::fs::direct_upload::upload_file;
use crate::fs::directory_manager::{
    create_directory, delete_directory, delete_file, get_file_metadata, rename_file_query,
};
use crate::fs::directory_viewer::{
    stream_file, stream_file_v2, stream_hls_file, view_directory, view_file,
};
use crate::fs::download_manager::download_file;
use crate::fs::fragmented_upload::{check_uploaded_fragments, upload_fragments};
use crate::fs::fs_constants::MAX_FILE_SIZE;
use crate::fs::stateless_fragmented_upload as legacy_upload;
use crate::handlers::dir_info_handlers::get_storage_capacity;
use crate::handlers::transcoder_handler::{transcode_status_handler, transcode_video_handler};
use crate::middleware::auth_middleware::{auth, authorize};
use crate::middleware::rate_limiter_middleware::RateLimitLayers;
use crate::middleware::storage_middleware::enforce_storage_limit;
use crate::object_storage::delete::{
    delete_fragment, delete_object, delete_object_permanently, delete_object_version,
};
use crate::object_storage::download::{download_object, download_object_chunk};
use crate::object_storage::garbage_collector::run_lifecycle_manager;
use crate::object_storage::maintenance::sync_user_metadata;
use crate::object_storage::modify::{rename_fragment, rename_object};
use crate::object_storage::retrieve::{
    get_object, get_object_metadata, list_fragments, list_object_versions, list_objects,
    stream_object,
};
use crate::object_storage::sharing::{create_signed_url, get_shared_object};
use crate::object_storage::upload::{
    cancel_upload_session, finalize_object_upload, get_capacity_usage, get_upload_status, initiate_object_upload, object_upload_part
};
use crate::statics::database_statics::DATABASE_URL;
use crate::websocket::handler::{system_broadcast_handler, ws_handler};
use crate::websocket::manager::WsConnectionManager;
use crate::websocket::ws_structs::StorageEventPayload;
use axum::http::{HeaderName, Method};
use axum::middleware::from_fn;
use axum::routing::{delete, get, patch, post, put};
use axum::{Router, extract::DefaultBodyLimit};
use mongodb::Client;
use mongodb::options::ClientOptions;
use std::sync::Arc;
use std::time::Duration;
use tower_http::cors::{Any, CorsLayer};

pub struct AppState {
    pub db_client: Arc<Client>,
    pub ws_manager: WsConnectionManager,
}

impl AppState {
    pub fn emit(&self, uid: &str, payload: StorageEventPayload) {
        self.ws_manager.publish(uid, payload);
    }
}

pub async fn serve_storage_routes() -> (Router, Arc<AppState>) {
    let layers = RateLimitLayers::new();

    let client_options = ClientOptions::parse(&*DATABASE_URL)
        .await
        .expect("Failed to parse MongoDB URI");
    let db_client =
        Arc::new(Client::with_options(client_options).expect("Failed to create MongoDB client"));

    let ws_manager = WsConnectionManager::new();

    let app_state = Arc::new(AppState {
        db_client: Arc::clone(&db_client),
        ws_manager,
    });

    let lifecycle_state = Arc::clone(&app_state);
    tokio::spawn(async move {
        run_lifecycle_manager(lifecycle_state).await;
    });

    let cors_layer = CorsLayer::new()
        .allow_origin(Any)
        .allow_headers([
            HeaderName::from_static("authorization"),
            HeaderName::from_static("uid"),
            HeaderName::from_static("content-type"),
            HeaderName::from_static("digest"),
            HeaderName::from_static("buffer"),
            HeaderName::from_static("range"),
            HeaderName::from_static("upgrade"),
            HeaderName::from_static("connection"),
            HeaderName::from_static("sec-websocket-key"),
            HeaderName::from_static("sec-websocket-version"),
            HeaderName::from_static("sec-websocket-protocol"),
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

    let object_storage_routes = {
        let read_ops = Router::new()
            .route("/v4/capacity/usage", get(get_capacity_usage))
            .route("/v4/fragment/metadata", get(list_fragments))
            .route("/v4/fragment/object/metadata/{:fragment}", get(list_objects))
            .route("/v4/fragment/object/versions/{:fragment}/{*object_key}", get(list_object_versions))
            .route("/v4/fragment/object/get/{:fragment}/{*object_key}", get(get_object))
            .route("/v4/fragment/object/stream/{:fragment}/{*object_key}", get(stream_object))
            .route("/v4/fragment/object/get/metadata/{:fragment}/{*object_key}", get(get_object_metadata))
            .route("/v4/fragment/object/download/{:fragment}", get(download_object))
            .route("/v4/fragment/object/chunk/{:fragment}", get(download_object_chunk))
            .route("/v4/fragment/object/share", post(create_signed_url));

        let read_ops = layers.layer_view(read_ops);

        let write_ops = Router::new()
            .route("/v4/fragment/object/rename", patch(rename_object))
            .route("/v4/fragment/delete/{:fragment_name}", delete(delete_fragment))
            .route("/v4/fragment/object/delete/{:fragment}/{*object_key}", delete(delete_object))
            .route("/v4/fragment/object/delete/version/{:fragment}/{:version_id}/{*object_key}", delete(delete_object_version))
            .route("/v4/fragment/object/delete/permanent/{:fragment}/{*object_key}", delete(delete_object_permanently))
            .route("/v4/fragment/rename", patch(rename_fragment))
            .route("/v4/fragment/object/upload/init/{:fragment}/{*object_key}", post(initiate_object_upload))
            .route("/v4/fragment/object/upload/status/{:fragment}/{:version_id}", get(get_upload_status))
            .route("/v4/fragment/object/upload/chunk/{:fragment}/{:version_id}/{:part_index}", put(object_upload_part))
            .route("/v4/fragment/object/upload/finalize/{:fragment}/{:version_id}/{:session_id}", post(finalize_object_upload))
            .route("/v4/fragment/object/upload/cancel/{:fragment}/{:version_id}", delete(cancel_upload_session))
            .route("/v4/maintenance/sync/user", post(sync_user_metadata));

        let write_ops = layers.layer_upload(write_ops);

        let protected_routes = Router::new()
            .merge(read_ops)
            .merge(write_ops)
            .layer(from_fn(auth));

        let public_routes = Router::new()
            .route("/v4/public/share", get(get_shared_object));

        let public_routes = layers.layer_sharing(public_routes);

        Router::new()
            .merge(protected_routes)
            .merge(public_routes)
    };

    let legacy_fs_routes = {
        let upload_routes = Router::new()
            .route("/dir/file/upload/", post(upload_file))
            .route("/dir/file/upload/{*path}", post(upload_file))
            .route("/dir/file/upload/v2/", post(upload_fragments))
            .route("/dir/file/upload/v2/{*path}", post(upload_fragments))
            .route("/dir/file/upload/v3/initiate/", post(legacy_upload::initiate_upload_session))
            .route("/dir/file/upload/v3/initiate/{*path}", post(legacy_upload::initiate_upload_session))
            .route("/dir/file/upload/multipart/v3/{:file_id}/{:chunk_index}", put(legacy_upload::upload_chunk_multipart))
            .route("/dir/file/upload/v3/{:file_id}/{:chunk_index}", put(legacy_upload::upload_chunk_binary))
            .route("/dir/file/upload/v3/{:file_id}/status", get(legacy_upload::get_upload_status))
            .route("/dir/file/upload/v3/{:file_id}/finalize", post(legacy_upload::finalize_upload_session))
            .layer(from_fn(enforce_storage_limit));

        let upload_routes = layers.layer_upload(upload_routes);

        let transcoding_routes = Router::new()
            .route("/dir/file/transcode/video", post(transcode_video_handler))
            .route("/dir/file/transcode/status", get(transcode_status_handler))
            .layer(from_fn(auth));

        let transcoding_routes = layers.layer_transcode(transcoding_routes);

        let management_ops = Router::new()
            .route("/dir/create/{*path}", post(create_directory))
            .route("/dir/delete/{*path}", delete(delete_directory))
            .route("/dir/file/rename/", put(rename_file_query))
            .route("/dir/file/rename/{*path}", put(rename_file_query))
            .route("/dir/file/delete/", delete(delete_file))
            .route("/dir/file/delete/{*path}", delete(delete_file));

        let management_ops = layers.layer_upload(management_ops);

        let read_ops = Router::new()
            .route("/dir/", get(view_directory))
            .route("/dir/{*path}", get(view_directory))
            .route("/dir/capacity", get(get_storage_capacity))
            .route("/dir/file/upload/check/{*path}", get(check_uploaded_fragments))
            .route("/dir/file/view/{*path}", get(view_file))
            .route("/dir/file/stream/{*path}", get(stream_file))
            .route("/dir/file/stream/v2/{*path}", get(stream_file_v2))
            .route("/dir/file/transcode/stream/hls/{*path}", get(stream_hls_file))
            .route("/dir/file/metadata/", get(get_file_metadata))
            .route("/dir/file/metadata/{*path}", get(get_file_metadata))
            .route("/dir/file/download/", get(download_file))
            .route("/dir/file/download/{*path}", get(download_file));

        let read_ops = layers.layer_view(read_ops);

        Router::new()
            .merge(upload_routes)
            .merge(transcoding_routes)
            .merge(management_ops)
            .merge(read_ops)
            .layer(from_fn(auth))
    };

    let ws_routes = {
        let upgrade_route = Router::new()
            .route("/ws", get(ws_handler));

        let broadcast_route = Router::new()
            .route("/v4/system/broadcast", post(system_broadcast_handler))
            .layer(from_fn(auth));

        let broadcast_route = layers.layer_upload(broadcast_route);

        Router::new()
            .merge(upgrade_route)
            .merge(broadcast_route)
    };

    let auth_route = Router::new()
        .route("/authenticate", post(authorize));
    let auth_route = layers.layer_auth(auth_route);

    let ping_route = Router::new()
        .route("/ping", get(ping));
    let ping_route = layers.layer_global(ping_route);

    let router = Router::new()
        .merge(ping_route)
        .merge(auth_route)
        .merge(object_storage_routes)
        .merge(legacy_fs_routes)
        .merge(ws_routes)
        .layer(DefaultBodyLimit::max(MAX_FILE_SIZE))
        .layer(cors_layer)
        .with_state(Arc::clone(&app_state));

    (router, app_state)
}

async fn ping() -> &'static str {
    "pong"
}
