use crate::enums::app_error_enum::AppError;
use crate::fuse::handlers::{
    fsd_delete, fsd_list_dir, fsd_mkdir, fsd_read, fsd_rename, fsd_rmdir, fsd_stat,
    fsd_write_abort, fsd_write_begin, fsd_write_chunk, fsd_write_commit,
};
use crate::middleware::auth_middleware::{authorize_fragment, validate_access};
use crate::middleware::rate_limiter_middleware::RateLimitLayers;
use crate::core::adaptive_concurrency::run_adaptive_concurrency_loop;
use crate::core::archive::download_selected_archive;
use crate::core::delete::{
    delete_object, delete_object_permanently, delete_object_version, empty_trash,
    recover_all_trash, recover_object, recover_object_version,
};
use crate::core::download::{download_object, download_object_chunk};
use crate::core::fragment_manager::{
    create_fragment, delete_fragment, get_fragment_hash, list_public_fragments, update_fragment,
    verify_fragment_existence,
};
use crate::core::garbage_collector::{
    get_gc_status_api, run_garbage_collector, run_lifecycle_manager, stop_garbage_collector,
};
use crate::core::maintenance::sync_fragment_metadata;
use crate::core::modify::rename_object;
use crate::core::monitoring::{
    get_raw_blob_backend_status, get_system_health, run_system_monitor,
};
use crate::core::operation_gate::enforce_read_only_fast_mode_on_mutations;
use crate::core::redistribution::{
    get_redist_status_api, stop_redistribution_job_api, trigger_redistribution_job_api,
};
use crate::core::repair_manager::{
    get_repair_status_api, get_scrub_status_api, stop_repair_job_api, stop_scrub_job_api,
    trigger_repair_job_api, trigger_scrub_job_api,
};
use crate::core::retrieve::{
    get_object, get_object_metadata, list_fragments, list_object_versions, list_objects,
    list_trash_objects, stream_object,
};
use crate::core::sharing::{create_signed_url, get_shared_object};
use crate::core::storage_capacity_parser::get_capacity_usage;
use crate::core::sync::{
    initialize_fragment_storage_api, sync_fragment_quota_api, sync_quota_api,
};
use crate::core::upload::{
    cancel_upload_session, finalize_object_upload, get_upload_status, initiate_object_upload,
    list_active_uploads, object_upload_part,
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

const MAX_FILE_SIZE: usize = 5 * 1024 * 1024 * 1024;

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
    crate::core::storage_capacity_parser::get_capacity_usage(claims, Path(fragment)).await
}

async fn list_objects_scoped(
    claims: Claims,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    crate::core::retrieve::list_objects(claims, Path(fragment), Query(params)).await
}

async fn list_trash_objects_scoped(
    claims: Claims,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    crate::core::retrieve::list_trash_objects(claims, Path(fragment), Query(params)).await
}

async fn list_object_versions_scoped(
    claims: Claims,
    Path(object_key): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    crate::core::retrieve::list_object_versions(claims, Path((fragment, object_key))).await
}

async fn get_object_scoped(
    claims: Claims,
    Path(object_key): Path<String>,
    req: axum::http::Request<axum::body::Body>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    crate::core::retrieve::get_object(claims, Path((fragment, object_key)), req).await
}

async fn stream_object_scoped(
    claims: Claims,
    headers: axum::http::HeaderMap,
    Path(object_key): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    crate::core::retrieve::stream_object(claims, headers, Path((fragment, object_key))).await
}

async fn get_object_metadata_scoped(
    claims: Claims,
    Path(object_key): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    crate::core::retrieve::get_object_metadata(claims, Path((fragment, object_key))).await
}

async fn download_object_scoped(
    claims: Claims,
    Query(query): Query<crate::core::disk_manager::ObjectKeyQuery>,
    headers: axum::http::HeaderMap,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    crate::core::download::download_object(claims, Path(fragment), Query(query), headers).await
}

async fn download_object_chunk_scoped(
    claims: Claims,
    Query(params): Query<crate::core::disk_manager::ChunkQuery>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    crate::core::download::download_object_chunk(claims, Path(fragment), Query(params)).await
}

async fn download_selected_archive_scoped(
    claims: Claims,
    AxumJson(req): AxumJson<crate::core::disk_manager::DownloadArchiveRequest>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    crate::core::archive::download_selected_archive(claims, Path(fragment), AxumJson(req)).await
}

async fn empty_trash_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    crate::core::delete::empty_trash(State(state), claims, Path(fragment)).await
}

async fn delete_object_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path(object_key): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    crate::core::delete::delete_object(State(state), claims, Path((fragment, object_key))).await
}

async fn delete_object_version_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((version_id, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    crate::core::delete::delete_object_version(
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
    crate::core::delete::delete_object_permanently(
        State(state),
        claims,
        Path((fragment, object_key)),
    )
    .await
}

async fn recover_object_version_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((version_id, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    crate::core::delete::recover_object_version(
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
    crate::core::delete::recover_object(State(state), claims, Path((fragment, object_key))).await
}

async fn recover_all_trash_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    crate::core::delete::recover_all_trash(State(state), claims, Path(fragment)).await
}

async fn initiate_object_upload_scoped(
    claims: Claims,
    Path(object_key): Path<String>,
    AxumJson(params): AxumJson<crate::core::disk_manager::InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    crate::core::upload::initiate_object_upload(
        claims,
        Path((fragment, object_key)),
        AxumJson(params),
    )
    .await
}

async fn get_upload_status_scoped(
    claims: Claims,
    Path(version_id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    crate::core::upload::get_upload_status(claims, Path((fragment, version_id))).await
}

async fn list_active_uploads_scoped(
    claims: Claims,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    crate::core::upload::list_active_uploads(claims, Path(fragment), Query(params)).await
}

async fn object_upload_part_scoped(
    claims: Claims,
    headers: axum::http::HeaderMap,
    Path((version_id, part_index)): Path<(String, u64)>,
    Query(query): Query<crate::core::disk_manager::UploadPartQuery>,
    body: axum::body::Body,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    crate::core::upload::object_upload_part(
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
    crate::core::upload::finalize_object_upload(
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
    crate::core::upload::cancel_upload_session(
        claims,
        Path((fragment, version_id)),
        Query(key_query),
    )
    .await
}

async fn smb_progress_scoped(
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

pub async fn serve_storage_routes() -> Router {
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
            .route("/v4/capacity/usage", get(get_capacity_usage_scoped))
            .route("/v4/capacity/usage/{:fragment}", get(get_capacity_usage))
            .route("/v4/fragment/list", get(list_fragments))
            .route("/v4/fragment/objects", get(list_objects_scoped))
            .route("/v4/fragment/objects/{:fragment}", get(list_objects))
            .route("/v4/fragment/trash", get(list_trash_objects_scoped))
            .route("/v4/fragment/trash/{:fragment}", get(list_trash_objects))
            .route(
                "/v4/fragment/versions/{*object_key}",
                get(list_object_versions_scoped),
            )
            .route(
                "/v4/fragment/versions/{:fragment}/{*object_key}",
                get(list_object_versions),
            )
            .route("/v4/fragment/get/{*object_key}", get(get_object_scoped))
            .route(
                "/v4/fragment/get/{:fragment}/{*object_key}",
                get(get_object),
            )
            .route(
                "/v4/fragment/stream/{*object_key}",
                get(stream_object_scoped),
            )
            .route(
                "/v4/fragment/stream/{:fragment}/{*object_key}",
                get(stream_object),
            )
            .route(
                "/v4/fragment/meta/{*object_key}",
                get(get_object_metadata_scoped),
            )
            .route(
                "/v4/fragment/meta/{:fragment}/{*object_key}",
                get(get_object_metadata),
            )
            .route("/v4/fragment/download", get(download_object_scoped))
            .route("/v4/fragment/download/{:fragment}", get(download_object))
            .route(
                "/v4/fragment/download/chunk",
                get(download_object_chunk_scoped),
            )
            .route(
                "/v4/fragment/download/chunk/{:fragment}",
                get(download_object_chunk),
            )
            .route(
                "/v4/fragment/archive",
                post(download_selected_archive_scoped),
            )
            .route(
                "/v4/fragment/archive/{:fragment}",
                post(download_selected_archive),
            )
            .route("/v4/share/create", post(create_signed_url))
            .route("/v4/smb/progress", get(smb_progress_scoped))
            .route("/v4/smb/progress/{:fragment}", get(smb_progress))
            .route("/v4/fsd/list/{*mount_path}", get(fsd_list_dir))
            .route("/v4/fsd/stat/{*mount_path}", get(fsd_stat))
            .route("/v4/fsd/read/{*mount_path}", get(fsd_read));

        let read_ops = layers
            .layer_view(read_ops)
            .layer(from_fn(enforce_read_only_fast_mode_on_mutations));

        let write_ops = Router::new()
            .route("/v4/fragment/delete/{:fragment}", delete(delete_fragment))
            .route("/v4/fragment/modify/{:fragment}", patch(update_fragment))
            .route("/v4/fragment/trash/empty", delete(empty_trash_scoped))
            .route("/v4/fragment/trash/empty/{:fragment}", delete(empty_trash))
            .route("/v4/fragment/object/rename", patch(rename_object))
            .route(
                "/v4/fragment/object/delete/{*object_key}",
                delete(delete_object_scoped),
            )
            .route(
                "/v4/fragment/object/delete/{:fragment}/{*object_key}",
                delete(delete_object),
            )
            .route(
                "/v4/fragment/version/delete/{:version_id}/{*object_key}",
                delete(delete_object_version_scoped),
            )
            .route(
                "/v4/fragment/version/delete/{:fragment}/{:version_id}/{*object_key}",
                delete(delete_object_version),
            )
            .route(
                "/v4/fragment/permanent/delete/{*object_key}",
                delete(delete_object_permanently_scoped),
            )
            .route(
                "/v4/fragment/permanent/delete/{:fragment}/{*object_key}",
                delete(delete_object_permanently),
            )
            .route(
                "/v4/fragment/recover/version/{:version_id}/{*object_key}",
                post(recover_object_version_scoped),
            )
            .route(
                "/v4/fragment/recover/version/{:fragment}/{:version_id}/{*object_key}",
                post(recover_object_version),
            )
            .route(
                "/v4/fragment/recover/object/{*object_key}",
                post(recover_object_scoped),
            )
            .route(
                "/v4/fragment/recover/object/{:fragment}/{*object_key}",
                post(recover_object),
            )
            .route(
                "/v4/fragment/recover/trash/all",
                post(recover_all_trash_scoped),
            )
            .route(
                "/v4/fragment/recover/trash/all/{:fragment}",
                post(recover_all_trash),
            )
            .route(
                "/v4/upload/init/{*object_key}",
                post(initiate_object_upload_scoped),
            )
            .route(
                "/v4/upload/init/{:fragment}/{*object_key}",
                post(initiate_object_upload),
            )
            .route(
                "/v4/upload/status/{:version_id}",
                get(get_upload_status_scoped),
            )
            .route(
                "/v4/upload/status/{:fragment}/{:version_id}",
                get(get_upload_status),
            )
            .route("/v4/upload/status/list", get(list_active_uploads_scoped))
            .route(
                "/v4/upload/status/list/{:fragment}",
                get(list_active_uploads),
            )
            .route(
                "/v4/upload/chunk/{:version_id}/{:part_index}",
                put(object_upload_part_scoped),
            )
            .route(
                "/v4/upload/chunk/{:fragment}/{:version_id}/{:part_index}",
                put(object_upload_part),
            )
            .route(
                "/v4/upload/finalize/{:version_id}/{:session_id}",
                post(finalize_object_upload_scoped),
            )
            .route(
                "/v4/upload/finalize/{:fragment}/{:version_id}/{:session_id}",
                post(finalize_object_upload),
            )
            .route(
                "/v4/upload/cancel/{:version_id}",
                delete(cancel_upload_session_scoped),
            )
            .route(
                "/v4/upload/cancel/{:fragment}/{:version_id}",
                delete(cancel_upload_session),
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
            .route("/v4/maintenance/sync/global", post(sync_quota_api))
            .route(
                "/v4/maintenance/sync/fragment",
                post(sync_fragment_quota_api),
            )
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
            .route("/v4/maintenance/scrub/start", post(trigger_scrub_job_api))
            .route("/v4/maintenance/scrub/stop", post(stop_scrub_job_api))
            .route("/v4/maintenance/scrub/status", get(get_scrub_status_api))
            .route("/v4/maintenance/gc/start", post(run_garbage_collector))
            .route("/v4/maintenance/gc/status", get(get_gc_status_api))
            .route("/v4/maintenance/gc/stop", post(stop_garbage_collector))
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
                "/v4/maintenance/blob/raw/status",
                get(get_raw_blob_backend_status),
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
            .layer(from_fn(enforce_read_only_fast_mode_on_mutations));

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

    let list_fragments_route =
        Router::new().route("/v4/fragment/list/public", get(list_public_fragments));
    let list_fragments_route = layers
        .layer_global(list_fragments_route)
        .layer(from_fn(enforce_read_only_fast_mode_on_mutations));

    let global_routes = Router::new()
        .route("/v4/fragment/check", get(verify_fragment_existence))
        .route("/v4/fragment/get/hash", get(get_fragment_hash));
    let global_routes = layers
        .layer_global(global_routes)
        .layer(from_fn(enforce_read_only_fast_mode_on_mutations));

    let create_fragment_route = Router::new().route("/v4/fragment/create", post(create_fragment));
    let create_fragment_route = layers
        .layer_auth(create_fragment_route)
        .layer(from_fn(enforce_read_only_fast_mode_on_mutations));

    let fragment_auth_route =
        Router::new().route("/authenticate/fragment", post(authorize_fragment));
    let fragment_auth_route = layers.layer_auth(fragment_auth_route);

    let ping_route = Router::new().route("/ping", get(ping));
    let ping_route = layers.layer_global(ping_route);

    let router = Router::new()
        .merge(ping_route)
        .merge(fragment_auth_route)
        .merge(list_fragments_route)
        .merge(global_routes)
        .merge(create_fragment_route)
        .merge(object_storage_routes)
        .merge(ws_routes)
        .layer(DefaultBodyLimit::max(MAX_FILE_SIZE))
        .layer(cors_layer)
        .with_state(Arc::clone(&app_state));

    router
}

async fn ping() -> &'static str {
    "pong"
}

async fn smb_progress(
    claims: Claims,
    Path(fragment): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<AxumJson<Vec<SmbTransferProgressPayload>>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }

    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

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
