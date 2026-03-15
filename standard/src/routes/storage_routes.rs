use crate::enums::app_error_enum::AppError;
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
use crate::fs::stateless_fragmented_upload::{
    self, finalize_upload_session, initiate_upload_session, upload_chunk_binary,
    upload_chunk_multipart,
};
use crate::fuse::handlers::{
    fsd_delete, fsd_list_dir, fsd_mkdir, fsd_read, fsd_rename, fsd_rmdir, fsd_stat,
    fsd_write_abort, fsd_write_begin, fsd_write_chunk, fsd_write_commit,
};
use crate::handlers::dir_info_handlers::get_storage_capacity;
use crate::middleware::auth_middleware::{
    auth, authorize, authorize_fragment, logout_refresh_token, refresh_access_token,
    validate_access,
};
use crate::middleware::rate_limiter_middleware::RateLimitLayers;
use crate::middleware::storage_middleware::enforce_storage_limit;
use crate::n1_core::adaptive_concurrency::run_adaptive_concurrency_loop;
use crate::n1_core::archive::download_selected_archive as download_selected_archive_impl;
use crate::n1_core::delete::{
    delete_object as delete_object_impl,
    delete_object_permanently as delete_object_permanently_impl,
    delete_object_version as delete_object_version_impl,
    delete_objects_bulk as delete_objects_bulk_impl, empty_trash as empty_trash_impl,
    recover_all_trash as recover_all_trash_impl, recover_object as recover_object_impl,
    recover_object_version as recover_object_version_impl,
    recover_objects_bulk as recover_objects_bulk_impl,
};
use crate::n1_core::download::{
    download_object as download_object_impl, download_object_chunk as download_object_chunk_impl,
};
use crate::n1_core::fragment_manager::{
    create_fragment, delete_fragment, get_fragment_hash, list_public_fragments, update_fragment,
    verify_fragment_existence,
};
use crate::n1_core::garbage_collector::{
    get_gc_status_api, run_garbage_collector, run_lifecycle_manager, stop_garbage_collector,
};
use crate::n1_core::maintenance::sync_fragment_metadata;
use crate::n1_core::modify::rename_object;
use crate::n1_core::monitoring::{get_system_health, run_system_monitor};
use crate::n1_core::operation_gate::{
    enforce_quiesced_mode_on_reads, enforce_read_only_fast_mode_on_mutations,
};
use crate::n1_core::prefix_rebuilder::{
    get_prefix_rebuild_status_api, stop_prefix_rebuild_job_api, trigger_prefix_rebuild_job_api,
};
use crate::n1_core::redistribution::{
    get_redist_status_api, stop_redistribution_job_api, trigger_redistribution_job_api,
};
use crate::n1_core::repair_manager::{
    get_repair_status_api, stop_repair_job_api, trigger_repair_job_api,
};
use crate::n1_core::retrieve::{
    get_object as get_object_impl, get_object_metadata as get_object_metadata_impl, list_fragments,
    list_object_versions as list_object_versions_impl, list_objects as list_objects_impl,
    list_trash_objects as list_trash_objects_impl, stream_object as stream_object_impl,
};
use crate::n1_core::sharing::{create_signed_url, get_shared_object};
use crate::n1_core::storage_capacity_parser::get_capacity_usage as get_capacity_usage_impl;
use crate::n1_core::sync::initialize_fragment_storage_api;
use crate::n1_core::thumbnail::{
    ThumbnailQuery, get_object_thumbnail as get_object_thumbnail_impl,
};
use crate::n1_core::upload::{
    cancel_upload_session as cancel_upload_session_impl,
    finalize_object_upload as finalize_object_upload_impl,
    get_upload_status as get_upload_status_impl,
    initiate_object_upload as initiate_object_upload_impl,
    list_active_uploads as list_active_uploads_impl, object_upload_part as object_upload_part_impl,
    upload_object_one_shot as upload_object_one_shot_impl,
};
use crate::smb::server::list_transfer_progress_for_fragment;
use crate::structs::auth_structs::Claims;
use crate::websocket::handler::{system_broadcast_handler, ws_handler};
use crate::websocket::manager::{WsConnectionManager, register_global_ws_manager};
use crate::websocket::ws_structs::SmbTransferProgressPayload;
use crate::websocket::ws_structs::StorageEventPayload;
use axum::http::{HeaderName, Method};
use axum::middleware::from_fn;
use axum::response::IntoResponse;
use axum::routing::{delete, get, patch, post, put};
use axum::{
    Json as AxumJson, Router,
    extract::{DefaultBodyLimit, Path, Query, State},
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tower_http::cors::{Any, CorsLayer};

pub struct AppState {
    pub ws_manager: WsConnectionManager,
}

impl AppState {
    pub fn emit(&self, uid: &str, payload: StorageEventPayload) {
        self.ws_manager.publish(uid, payload);
    }
}

async fn get_capacity_usage_scoped(claims: Claims) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    get_capacity_usage_impl(claims, Path(fragment)).await
}

async fn list_objects_scoped(
    claims: Claims,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    list_objects_impl(claims, Path(fragment), Query(params)).await
}

async fn list_trash_objects_scoped(
    claims: Claims,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    list_trash_objects_impl(claims, Path(fragment), Query(params)).await
}

async fn list_object_versions_scoped(
    claims: Claims,
    Path(object_key): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    list_object_versions_impl(claims, Path((fragment, object_key))).await
}

async fn get_object_scoped(
    claims: Claims,
    Path(object_key): Path<String>,
    req: axum::http::Request<axum::body::Body>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    get_object_impl(claims, Path((fragment, object_key)), req).await
}

async fn stream_object_scoped(
    claims: Claims,
    headers: axum::http::HeaderMap,
    Path(object_key): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    stream_object_impl(claims, headers, Path((fragment, object_key))).await
}

async fn get_object_metadata_scoped(
    claims: Claims,
    Path(object_key): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    get_object_metadata_impl(claims, Path((fragment, object_key))).await
}

async fn get_object_thumbnail_scoped(
    claims: Claims,
    Path(object_key): Path<String>,
    Query(query): Query<ThumbnailQuery>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    get_object_thumbnail_impl(claims, Path((fragment, object_key)), Query(query)).await
}

async fn download_object_scoped(
    claims: Claims,
    Query(query): Query<crate::n1_core::disk_manager::ObjectKeyQuery>,
    headers: axum::http::HeaderMap,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    download_object_impl(claims, Path(fragment), Query(query), headers).await
}

async fn download_object_chunk_scoped(
    claims: Claims,
    Query(params): Query<crate::n1_core::disk_manager::ChunkQuery>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    download_object_chunk_impl(claims, Path(fragment), Query(params)).await
}

async fn download_selected_archive_scoped(
    claims: Claims,
    AxumJson(req): AxumJson<crate::n1_core::disk_manager::DownloadArchiveRequest>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    download_selected_archive_impl(claims, Path(fragment), AxumJson(req)).await
}

async fn empty_trash_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    empty_trash_impl(State(state), claims, Path(fragment)).await
}

async fn delete_object_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path(object_key): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    delete_object_impl(State(state), claims, Path((fragment, object_key))).await
}

async fn delete_object_version_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((version_id, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    delete_object_version_impl(
        State(state),
        claims,
        Path((fragment, version_id, object_key)),
    )
    .await
}

async fn delete_object_permanently_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path(object_key): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    delete_object_permanently_impl(State(state), claims, Path((fragment, object_key))).await
}

async fn recover_object_version_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((version_id, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    recover_object_version_impl(
        State(state),
        claims,
        Path((fragment, version_id, object_key)),
    )
    .await
}

async fn recover_object_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path(object_key): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    recover_object_impl(State(state), claims, Path((fragment, object_key))).await
}

async fn recover_all_trash_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    recover_all_trash_impl(State(state), claims, Path(fragment)).await
}

async fn initiate_object_upload_scoped(
    claims: Claims,
    Path(object_key): Path<String>,
    AxumJson(params): AxumJson<crate::n1_core::disk_manager::InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    initiate_object_upload_impl(claims, Path((fragment, object_key)), AxumJson(params)).await
}

async fn upload_object_one_shot_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    headers: axum::http::HeaderMap,
    Path(object_key): Path<String>,
    Query(query): Query<crate::n1_core::disk_manager::UploadPartQuery>,
    body: axum::body::Body,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    upload_object_one_shot_impl(
        State(state),
        claims,
        headers,
        Path((fragment, object_key)),
        Query(query),
        body,
    )
    .await
}

async fn get_upload_status_scoped(
    claims: Claims,
    Path(version_id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    get_upload_status_impl(claims, Path((fragment, version_id))).await
}

async fn list_active_uploads_scoped(
    claims: Claims,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    list_active_uploads_impl(claims, Path(fragment), Query(params)).await
}

async fn object_upload_part_scoped(
    claims: Claims,
    headers: axum::http::HeaderMap,
    Path((version_id, part_index)): Path<(String, u64)>,
    Query(query): Query<crate::n1_core::disk_manager::UploadPartQuery>,
    body: axum::body::Body,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    object_upload_part_impl(
        claims,
        headers,
        Path((fragment, version_id, part_index)),
        Query(query),
        body,
    )
    .await
}

async fn finalize_object_upload_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((version_id, session_id)): Path<(String, String)>,
    Query(key_query): Query<crate::n1_core::disk_manager::ObjectKeyQuery>,
    AxumJson(params): AxumJson<crate::n1_core::disk_manager::InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    finalize_object_upload_impl(
        State(state),
        claims,
        Path((fragment, version_id, session_id)),
        Query(key_query),
        AxumJson(params),
    )
    .await
}

async fn cancel_upload_session_scoped(
    claims: Claims,
    Path(version_id): Path<String>,
    Query(key_query): Query<crate::n1_core::disk_manager::ObjectKeyQuery>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    cancel_upload_session_impl(claims, Path((fragment, version_id)), Query(key_query)).await
}

fn build_cors_layer() -> CorsLayer {
    CorsLayer::new()
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
        .max_age(Duration::from_secs(86400))
}

pub fn apply_runtime_http_layers(router: Router<Arc<AppState>>) -> Router<Arc<AppState>> {
    router
        .layer(DefaultBodyLimit::max(MAX_FILE_SIZE))
        .layer(build_cors_layer())
}

pub fn build_storage_routes_unbound() -> (Router<Arc<AppState>>, Arc<AppState>) {
    let layers = RateLimitLayers::new();
    let ws_manager = WsConnectionManager::new();
    register_global_ws_manager(ws_manager.clone());

    let app_state = Arc::new(AppState { ws_manager });

    let lifecycle_state = Arc::clone(&app_state);
    tokio::spawn(async move {
        run_lifecycle_manager(lifecycle_state).await;
    });

    let monitoring_state = Arc::clone(&app_state);
    tokio::spawn(async move {
        run_system_monitor(monitoring_state).await;
    });

    tokio::spawn(async move {
        run_adaptive_concurrency_loop().await;
    });

    let object_storage_routes = {
        let read_ops = Router::new()
            .route("/v4/capacity/usage", get(get_capacity_usage_scoped))
            .route("/v4/fragment/list", get(list_fragments))
            .route("/v4/fragment/objects", get(list_objects_scoped))
            .route("/v4/fragment/trash", get(list_trash_objects_scoped))
            .route(
                "/v4/fragment/versions/{*object_key}",
                get(list_object_versions_scoped),
            )
            .route("/v4/fragment/get/{*object_key}", get(get_object_scoped))
            .route(
                "/v4/fragment/thumb/{*object_key}",
                get(get_object_thumbnail_scoped),
            )
            .route(
                "/v4/fragment/stream/{*object_key}",
                get(stream_object_scoped),
            )
            .route(
                "/v4/fragment/meta/{*object_key}",
                get(get_object_metadata_scoped),
            )
            .route("/v4/fragment/download", get(download_object_scoped))
            .route(
                "/v4/fragment/download/chunk",
                get(download_object_chunk_scoped),
            )
            .route(
                "/v4/fragment/archive",
                post(download_selected_archive_scoped),
            )
            .route("/v4/share/create", post(create_signed_url))
            .route("/v4/smb/progress", get(smb_progress))
            .route("/v4/fsd/list/{*mount_path}", get(fsd_list_dir))
            .route("/v4/fsd/stat/{*mount_path}", get(fsd_stat))
            .route("/v4/fsd/read/{*mount_path}", get(fsd_read));

        let read_ops = layers
            .layer_view(read_ops)
            .layer(from_fn(enforce_quiesced_mode_on_reads));

        let write_ops = Router::new()
            .route("/v4/fragment/delete/{:fragment}", delete(delete_fragment))
            .route("/v4/fragment/modify/{:fragment}", patch(update_fragment))
            .route("/v4/fragment/trash/empty", delete(empty_trash_scoped))
            .route("/v4/fragment/object/rename", patch(rename_object))
            .route(
                "/v4/fragment/object/bulk-delete",
                post(delete_objects_bulk_impl),
            )
            .route(
                "/v4/fragment/object/delete/{*object_key}",
                delete(delete_object_scoped),
            )
            .route(
                "/v4/fragment/version/delete/{:version_id}/{*object_key}",
                delete(delete_object_version_scoped),
            )
            .route(
                "/v4/fragment/permanent/delete/{*object_key}",
                delete(delete_object_permanently_scoped),
            )
            .route(
                "/v4/fragment/recover/version/{:version_id}/{*object_key}",
                post(recover_object_version_scoped),
            )
            .route(
                "/v4/fragment/object/bulk-recover",
                post(recover_objects_bulk_impl),
            )
            .route(
                "/v4/fragment/recover/object/{*object_key}",
                post(recover_object_scoped),
            )
            .route(
                "/v4/fragment/recover/trash/all",
                post(recover_all_trash_scoped),
            )
            .route(
                "/v4/upload/init/{*object_key}",
                post(initiate_object_upload_scoped),
            )
            .route(
                "/v4/upload/one-shot/{*object_key}",
                post(upload_object_one_shot_scoped),
            )
            .route(
                "/v4/upload/status/{:version_id}",
                get(get_upload_status_scoped),
            )
            .route("/v4/upload/status/list", get(list_active_uploads_scoped))
            .route(
                "/v4/upload/chunk/{:version_id}/{:part_index}",
                put(object_upload_part_scoped),
            )
            .route(
                "/v4/upload/finalize/{:version_id}/{:session_id}",
                post(finalize_object_upload_scoped),
            )
            .route(
                "/v4/upload/cancel/{:version_id}",
                delete(cancel_upload_session_scoped),
            )
            .route("/v4/fsd/write/begin/{*mount_path}", post(fsd_write_begin))
            .route("/v4/fsd/write/chunk/{:session_id}", put(fsd_write_chunk))
            .route("/v4/fsd/write/commit/{:session_id}", post(fsd_write_commit))
            .route("/v4/fsd/write/abort/{:session_id}", post(fsd_write_abort))
            .route("/v4/fsd/delete/{*mount_path}", delete(fsd_delete))
            .route("/v4/fsd/mkdir/{*mount_path}", post(fsd_mkdir))
            .route("/v4/fsd/rmdir/{*mount_path}", delete(fsd_rmdir))
            .route("/v4/fsd/rename", patch(fsd_rename));

        let maintenance_ops = Router::new()
            .route(
                "/v4/maintenance/cleanup/fragment",
                post(sync_fragment_metadata),
            )
            .route(
                "/v4/maintenance/init/fragment",
                post(initialize_fragment_storage_api),
            )
            .route("/v4/maintenance/repair/start", post(trigger_repair_job_api))
            .route("/v4/maintenance/repair/stop", post(stop_repair_job_api))
            .route("/v4/maintenance/repair/status", get(get_repair_status_api))
            .route("/v4/maintenance/gc/start", post(run_garbage_collector))
            .route("/v4/maintenance/gc/stop", post(stop_garbage_collector))
            .route("/v4/maintenance/gc/status", get(get_gc_status_api))
            .route(
                "/v4/maintenance/redistribution/start",
                post(trigger_redistribution_job_api),
            )
            .route(
                "/v4/maintenance/redistribution/stop",
                patch(stop_redistribution_job_api),
            )
            .route(
                "/v4/maintenance/redistribution/monitor",
                get(get_redist_status_api),
            )
            .route(
                "/v4/maintenance/prefix/rebuild/start",
                post(trigger_prefix_rebuild_job_api),
            )
            .route(
                "/v4/maintenance/prefix/rebuild/stop",
                post(stop_prefix_rebuild_job_api),
            )
            .route(
                "/v4/maintenance/prefix/rebuild/status",
                get(get_prefix_rebuild_status_api),
            )
            .route("/v4/maintenance/health", get(get_system_health));

        let maintenance_ops = layers.layer_global(maintenance_ops);

        let write_ops = layers
            .layer_upload(write_ops)
            .layer(from_fn(enforce_read_only_fast_mode_on_mutations));

        let protected_routes = Router::new()
            .merge(read_ops)
            .merge(write_ops)
            .merge(maintenance_ops)
            .layer(from_fn(validate_access));

        let public_routes = Router::new().route("/v4/public/share", get(get_shared_object));

        let public_routes = layers
            .layer_sharing(public_routes)
            .layer(from_fn(enforce_quiesced_mode_on_reads));

        Router::new().merge(protected_routes).merge(public_routes)
    };

    let ws_routes = {
        let upgrade_route = Router::new().route("/ws", get(ws_handler));

        let broadcast_route = Router::new()
            .route("/v4/system/broadcast", post(system_broadcast_handler))
            .layer(from_fn(validate_access));

        let broadcast_route = layers.layer_upload(broadcast_route);

        Router::new().merge(upgrade_route).merge(broadcast_route)
    };

    let legacy_fs_routes = {
        let upload_routes = Router::new()
            .route("/dir/file/upload/", post(upload_file))
            .route("/dir/file/upload/{*path}", post(upload_file))
            .route("/dir/file/upload/v2/", post(upload_fragments))
            .route("/dir/file/upload/v2/{*path}", post(upload_fragments))
            .route(
                "/dir/file/upload/v3/initiate/",
                post(initiate_upload_session),
            )
            .route(
                "/dir/file/upload/v3/initiate/{*path}",
                post(initiate_upload_session),
            )
            .route(
                "/dir/file/upload/multipart/v3/{:file_id}/{:chunk_index}",
                put(upload_chunk_multipart),
            )
            .route(
                "/dir/file/upload/v3/{:file_id}/{:chunk_index}",
                put(upload_chunk_binary),
            )
            .route(
                "/dir/file/upload/v3/{:file_id}/status",
                get(stateless_fragmented_upload::get_upload_status),
            )
            .route(
                "/dir/file/upload/v3/{:file_id}/finalize",
                post(finalize_upload_session),
            )
            .layer(from_fn(enforce_storage_limit));

        let upload_routes = layers
            .layer_upload(upload_routes)
            .layer(from_fn(enforce_read_only_fast_mode_on_mutations));

        let management_ops = Router::new()
            .route("/dir/create/{*path}", post(create_directory))
            .route("/dir/delete/{*path}", delete(delete_directory))
            .route("/dir/file/rename/", put(rename_file_query))
            .route("/dir/file/rename/{*path}", put(rename_file_query))
            .route("/dir/file/delete/", delete(delete_file))
            .route("/dir/file/delete/{*path}", delete(delete_file));

        let management_ops = layers
            .layer_upload(management_ops)
            .layer(from_fn(enforce_read_only_fast_mode_on_mutations));

        let read_ops = Router::new()
            .route("/dir/", get(view_directory))
            .route("/dir/{*path}", get(view_directory))
            .route("/dir/capacity", get(get_storage_capacity))
            .route(
                "/dir/file/upload/check/{*path}",
                get(check_uploaded_fragments),
            )
            .route("/dir/file/view/{*path}", get(view_file))
            .route("/dir/file/stream/{*path}", get(stream_file))
            .route("/dir/file/stream/v2/{*path}", get(stream_file_v2))
            .route(
                "/dir/file/transcode/stream/hls/{*path}",
                get(stream_hls_file),
            )
            .route("/dir/file/metadata/", get(get_file_metadata))
            .route("/dir/file/metadata/{*path}", get(get_file_metadata))
            .route("/dir/file/download/", get(download_file))
            .route("/dir/file/download/{*path}", get(download_file));

        let read_ops = layers
            .layer_view(read_ops)
            .layer(from_fn(enforce_quiesced_mode_on_reads));

        Router::new()
            .merge(upload_routes)
            .merge(management_ops)
            .merge(read_ops)
            .layer(from_fn(auth))
    };

    let list_fragments_route =
        Router::new().route("/v4/fragment/list/public", get(list_public_fragments));
    let list_fragments_route = layers.layer_global(list_fragments_route);

    let global_routes = Router::new()
        .route("/v4/fragment/check", get(verify_fragment_existence))
        .route("/v4/fragment/get/hash", get(get_fragment_hash));
    let global_routes = layers.layer_global(global_routes);

    let create_fragment_route = Router::new().route("/v4/fragment/create", post(create_fragment));
    let create_fragment_route = layers
        .layer_auth(create_fragment_route)
        .layer(from_fn(enforce_read_only_fast_mode_on_mutations));

    let fragment_auth_route =
        Router::new().route("/authenticate/fragment", post(authorize_fragment));
    let fragment_auth_route = layers.layer_auth(fragment_auth_route);

    let fs_auth_route = Router::new().route("/authenticate/fs", post(authorize));
    let fs_auth_route = layers.layer_auth(fs_auth_route);

    let refresh_auth_route = Router::new().route("/authenticate/refresh", post(refresh_access_token));
    let refresh_auth_route = layers.layer_auth(refresh_auth_route);

    let logout_auth_route = Router::new().route("/authenticate/logout", post(logout_refresh_token));
    let logout_auth_route = layers.layer_auth(logout_auth_route);

    let ping_route = Router::new().route("/ping", get(ping));
    let ping_route = layers.layer_global(ping_route);

    let router = Router::new()
        .merge(ping_route)
        .merge(fragment_auth_route)
        .merge(fs_auth_route)
        .merge(refresh_auth_route)
        .merge(logout_auth_route)
        .merge(list_fragments_route)
        .merge(global_routes)
        .merge(create_fragment_route)
        .merge(object_storage_routes)
        .merge(legacy_fs_routes)
        .merge(ws_routes);

    (router, app_state)
}

pub async fn serve_storage_routes() -> Router {
    let (router, app_state) = build_storage_routes_unbound();
    apply_runtime_http_layers(router).with_state(app_state)
}

async fn ping() -> &'static str {
    "pong"
}

async fn smb_progress(
    claims: Claims,
    Query(params): Query<HashMap<String, String>>,
) -> Result<AxumJson<Vec<SmbTransferProgressPayload>>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }

    let fragment = claims.fragment.clone();

    let include_completed = params
        .get("include_completed")
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false);
    let limit = params
        .get("limit")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(100)
        .min(1000);

    Ok(AxumJson(list_transfer_progress_for_fragment(
        fragment.as_str(),
        include_completed,
        limit,
    )))
}
