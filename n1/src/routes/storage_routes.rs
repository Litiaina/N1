use crate::core::adaptive_concurrency::run_adaptive_concurrency_loop;
use crate::core::archive::{
    download_manifest_archive as download_manifest_archive_impl,
    download_selected_archive as download_selected_archive_impl,
};
use crate::core::delete::{
    delete_object as delete_object_impl,
    delete_object_permanently as delete_object_permanently_impl,
    delete_object_version as delete_object_version_impl,
    delete_objects_bulk as delete_objects_bulk_impl,
    delete_objects_bulk_permanently as delete_objects_bulk_permanently_impl,
    empty_trash as empty_trash_impl, get_empty_trash_status_api, get_recover_all_trash_status_api,
    recover_all_trash as recover_all_trash_impl, recover_object as recover_object_impl,
    recover_object_version as recover_object_version_impl,
    recover_objects_bulk as recover_objects_bulk_impl, resume_persistent_trash_jobs,
};
use crate::core::download::{
    download_object as download_object_impl, download_object_chunk as download_object_chunk_impl,
};
use crate::core::fragment_manager::{
    create_fragment, delete_fragment, get_fragment_hash, update_fragment, verify_fragment_existence,
};
use crate::core::garbage_collector::{
    get_gc_status_api, run_garbage_collector, run_lifecycle_manager, stop_garbage_collector,
};
use crate::core::maintenance::sync_fragment_metadata;
use crate::core::modify::rename_object;
use crate::core::monitoring::{get_system_health, run_system_monitor};
use crate::core::operation_gate::{
    enforce_quiesced_mode_on_reads, enforce_read_only_fast_mode_on_mutations,
};
use crate::core::redistribution::{
    get_redist_status_api, stop_redistribution_job_api, trigger_redistribution_job_api,
};
use crate::core::repair_manager::{
    get_repair_status_api, stop_repair_job_api, trigger_repair_job_api,
};
use crate::core::retrieve::{
    get_object as get_object_impl, get_object_metadata as get_object_metadata_impl, list_fragments,
    list_object_versions as list_object_versions_impl, list_objects as list_objects_impl,
    list_trash_objects as list_trash_objects_impl, stream_object as stream_object_impl,
};
use crate::core::storage_capacity_parser::get_capacity_usage as get_capacity_usage_impl;
use crate::core::sync::initialize_fragment_storage_api;
use crate::core::upload::{
    cancel_upload_session as cancel_upload_session_impl,
    finalize_object_upload as finalize_object_upload_impl,
    get_upload_status as get_upload_status_impl,
    initiate_object_upload as initiate_object_upload_impl,
    list_active_uploads as list_active_uploads_impl, object_upload_part as object_upload_part_impl,
    upload_object_one_shot as upload_object_one_shot_impl,
};
use crate::enums::app_error_enum::AppError;
use crate::fs::fs_constants::MAX_FILE_SIZE;
use crate::middleware::auth_middleware::{
    authorize_fragment, logout_refresh_token, refresh_access_token, validate_access,
};
use crate::middleware::rate_limiter_middleware::RateLimitLayers;
use crate::structs::auth_structs::Claims;
use crate::websocket::handler::ws_handler;
use crate::websocket::manager::{WsConnectionManager, register_global_ws_manager};
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

async fn download_object_scoped(
    claims: Claims,
    Query(query): Query<crate::core::disk_manager::ObjectKeyQuery>,
    headers: axum::http::HeaderMap,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    download_object_impl(claims, Path(fragment), Query(query), headers).await
}

async fn download_object_chunk_scoped(
    claims: Claims,
    Query(params): Query<crate::core::disk_manager::ChunkQuery>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    download_object_chunk_impl(claims, Path(fragment), Query(params)).await
}

async fn download_selected_archive_scoped(
    claims: Claims,
    AxumJson(req): AxumJson<crate::core::disk_manager::DownloadArchiveRequest>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    download_selected_archive_impl(claims, Path(fragment), AxumJson(req)).await
}

async fn download_manifest_archive_scoped(
    claims: Claims,
    Query(query): Query<crate::core::disk_manager::DownloadArchiveManifestQuery>,
    body: axum::body::Body,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    download_manifest_archive_impl(claims, Path(fragment), Query(query), body).await
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
    AxumJson(params): AxumJson<crate::core::disk_manager::InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    initiate_object_upload_impl(claims, Path((fragment, object_key)), AxumJson(params)).await
}

async fn upload_object_one_shot_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    headers: axum::http::HeaderMap,
    Path(object_key): Path<String>,
    Query(query): Query<crate::core::disk_manager::UploadPartQuery>,
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
    Query(query): Query<crate::core::disk_manager::UploadPartQuery>,
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
    Query(key_query): Query<crate::core::disk_manager::ObjectKeyQuery>,
    AxumJson(params): AxumJson<crate::core::disk_manager::InitiateUploadParams>,
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
    Query(key_query): Query<crate::core::disk_manager::ObjectKeyQuery>,
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

#[derive(Clone, Copy, Debug)]
pub struct StorageRouteOptions {
    pub include_object_storage: bool,
}

impl Default for StorageRouteOptions {
    fn default() -> Self {
        Self {
            include_object_storage: true,
        }
    }
}

pub fn build_storage_routes_unbound_with_options(
    options: StorageRouteOptions,
) -> (Router<Arc<AppState>>, Arc<AppState>) {
    let layers = RateLimitLayers::new();
    let ws_manager = WsConnectionManager::new();
    register_global_ws_manager(ws_manager.clone());

    let app_state = Arc::new(AppState { ws_manager });

    let trash_resume_state = Arc::clone(&app_state);
    tokio::spawn(async move {
        match resume_persistent_trash_jobs(trash_resume_state).await {
            Ok(resumed) if resumed > 0 => {
                tracing::info!("Resumed {} persistent trash maintenance job(s)", resumed);
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(
                    "Failed to resume persistent trash maintenance jobs: {}",
                    err
                );
            }
        }
    });

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
            .route("/n1/capacity/usage", get(get_capacity_usage_scoped))
            .route("/n1/fragments", get(list_fragments))
            .route("/n1/objects", get(list_objects_scoped))
            .route("/n1/trash", get(list_trash_objects_scoped))
            .route(
                "/n1/objects/versions/{*object_key}",
                get(list_object_versions_scoped),
            )
            .route("/n1/objects/get/{*object_key}", get(get_object_scoped))
            .route(
                "/n1/objects/stream/{*object_key}",
                get(stream_object_scoped),
            )
            .route(
                "/n1/objects/meta/{*object_key}",
                get(get_object_metadata_scoped),
            )
            .route("/n1/objects/download", get(download_object_scoped))
            .route(
                "/n1/objects/download/chunk",
                get(download_object_chunk_scoped),
            )
            .route(
                "/n1/objects/archive",
                post(download_selected_archive_scoped),
            )
            .route(
                "/n1/objects/archive/manifest",
                post(download_manifest_archive_scoped),
            );

        let read_ops = layers
            .layer_view(read_ops)
            .layer(from_fn(enforce_quiesced_mode_on_reads));

        let write_ops = Router::new()
            .route("/n1/fragments/{:fragment}", delete(delete_fragment))
            .route("/n1/fragments/{:fragment}", patch(update_fragment))
            .route("/n1/trash/empty", delete(empty_trash_scoped))
            .route("/n1/objects/rename", post(rename_object))
            .route("/n1/objects/bulk-delete", post(delete_objects_bulk_impl))
            .route(
                "/n1/objects/permanent/bulk-delete",
                post(delete_objects_bulk_permanently_impl),
            )
            .route("/n1/objects/{*object_key}", delete(delete_object_scoped))
            .route(
                "/n1/objects/version/{:version_id}/{*object_key}",
                delete(delete_object_version_scoped),
            )
            .route(
                "/n1/objects/permanent/{*object_key}",
                delete(delete_object_permanently_scoped),
            )
            .route(
                "/n1/objects/recover/version/{:version_id}/{*object_key}",
                post(recover_object_version_scoped),
            )
            .route("/n1/objects/bulk-recover", post(recover_objects_bulk_impl))
            .route(
                "/n1/objects/recover/{*object_key}",
                post(recover_object_scoped),
            )
            .route("/n1/trash/recover-all", post(recover_all_trash_scoped))
            .route(
                "/n1/upload/multipart/init/{*object_key}",
                post(initiate_object_upload_scoped),
            )
            .route(
                "/n1/upload/one-shot/{*object_key}",
                post(upload_object_one_shot_scoped),
            )
            .route(
                "/n1/upload/multipart/{:version_id}",
                get(get_upload_status_scoped),
            )
            .route("/n1/upload/multipart", get(list_active_uploads_scoped))
            .route(
                "/n1/upload/multipart/{:version_id}/{:part_index}",
                put(object_upload_part_scoped),
            )
            .route(
                "/n1/upload/multipart/{:version_id}/{:session_id}/finalize",
                post(finalize_object_upload_scoped),
            )
            .route(
                "/n1/upload/multipart/{:version_id}",
                delete(cancel_upload_session_scoped),
            );

        let maintenance_ops = Router::new()
            .route(
                "/n1/maintenance/fragment/cleanup",
                post(sync_fragment_metadata),
            )
            .route(
                "/n1/maintenance/fragment/init",
                post(initialize_fragment_storage_api),
            )
            .route("/n1/maintenance/repair/start", post(trigger_repair_job_api))
            .route("/n1/maintenance/repair/stop", post(stop_repair_job_api))
            .route("/n1/maintenance/repair/status", get(get_repair_status_api))
            .route("/n1/maintenance/gc/start", post(run_garbage_collector))
            .route("/n1/maintenance/gc/stop", post(stop_garbage_collector))
            .route("/n1/maintenance/gc/status", get(get_gc_status_api))
            .route(
                "/n1/maintenance/trash/recover/status",
                get(get_recover_all_trash_status_api),
            )
            .route(
                "/n1/maintenance/trash/empty/status",
                get(get_empty_trash_status_api),
            )
            .route(
                "/n1/maintenance/redistribution/start",
                post(trigger_redistribution_job_api),
            )
            .route(
                "/n1/maintenance/redistribution/stop",
                post(stop_redistribution_job_api),
            )
            .route(
                "/n1/maintenance/redistribution/status",
                get(get_redist_status_api),
            )
            .route("/n1/maintenance/health", get(get_system_health));

        let maintenance_ops = layers.layer_global(maintenance_ops);

        let write_ops = layers
            .layer_upload(write_ops)
            .layer(from_fn(enforce_read_only_fast_mode_on_mutations));

        let protected_routes = Router::new()
            .merge(read_ops)
            .merge(write_ops)
            .merge(maintenance_ops)
            .layer(from_fn(validate_access));

        Router::new().merge(protected_routes)
    };

    let ws_routes = { Router::new().route("/ws", get(ws_handler)) };

    let global_routes = Router::new()
        .route("/n1/fragments/check", get(verify_fragment_existence))
        .route("/n1/fragments/hash", get(get_fragment_hash));
    let global_routes = layers.layer_global(global_routes);

    let create_fragment_route = Router::new().route("/n1/fragments", post(create_fragment));
    let create_fragment_route = layers
        .layer_auth(create_fragment_route)
        .layer(from_fn(enforce_read_only_fast_mode_on_mutations));

    let fragment_auth_route = Router::new().route("/n1/auth/fragment", post(authorize_fragment));
    let fragment_auth_route = layers.layer_auth(fragment_auth_route);

    let refresh_auth_route = Router::new().route("/n1/auth/refresh", post(refresh_access_token));
    let refresh_auth_route = layers.layer_auth(refresh_auth_route);

    let logout_auth_route = Router::new().route("/n1/auth/logout", post(logout_refresh_token));
    let logout_auth_route = layers.layer_auth(logout_auth_route);

    let ping_route = Router::new().route("/n1/ping", get(ping));
    let ping_route = layers.layer_global(ping_route);

    let mut router = Router::new()
        .merge(ping_route)
        .merge(fragment_auth_route)
        .merge(refresh_auth_route)
        .merge(logout_auth_route)
        .merge(global_routes)
        .merge(create_fragment_route)
        .merge(ws_routes);

    if options.include_object_storage {
        router = router.merge(object_storage_routes);
    }

    (router, app_state)
}

pub fn build_storage_routes_unbound() -> (Router<Arc<AppState>>, Arc<AppState>) {
    build_storage_routes_unbound_with_options(StorageRouteOptions::default())
}

pub async fn serve_storage_routes() -> Router {
    let (router, app_state) = build_storage_routes_unbound();
    apply_runtime_http_layers(router).with_state(app_state)
}

async fn ping() -> &'static str {
    "pong"
}
