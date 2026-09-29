use crate::core::adaptive_concurrency::run_adaptive_concurrency_loop;
use crate::core::archive::{
    download_logical_archive as download_logical_archive_impl,
    download_manifest_archive as download_manifest_archive_impl,
    download_selected_archive as download_selected_archive_impl,
};
use crate::core::delete::{BulkObjectFailure, BulkObjectKeysRequest, BulkObjectOperationResponse};
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
use crate::core::monitoring::{get_system_health, run_system_monitor};
use crate::core::operation_gate::{
    enforce_quiesced_mode_on_reads, enforce_read_only_fast_mode_on_mutations,
};
use crate::core::redistribution::{
    get_redist_status_api, stop_redistribution_job_api, trigger_redistribution_job_api,
};
use crate::core::repair_manager::{
    ObjectRepairStatus, get_repair_status_api, parse_check_mode, repair_object,
    stop_repair_job_api, trigger_repair_job_api,
};
use crate::core::retrieve::{
    get_object as get_object_impl, get_object_metadata_by_id, list_fragments,
    stream_object as stream_object_impl,
};
use crate::core::storage_capacity_parser::get_capacity_usage as get_capacity_usage_impl;
use crate::core::sync::initialize_fragment_storage_api;
use crate::core::upload::{
    FinalizeUploadResult, cancel_upload_session as cancel_upload_session_impl,
    get_upload_status as get_upload_status_impl, list_active_uploads as list_active_uploads_impl,
};
use crate::core::{index, index_store};
use crate::distribution::auto_healing::build_auto_healing_peer_routes;
use crate::distribution::download::part_has_remote_shards;
use crate::distribution::lifecycle::build_lifecycle_peer_routes;
use crate::distribution::maintenance::build_distributed_maintenance_peer_routes;
use crate::distribution::membership::active_cluster_runtime;
use crate::distribution::namespace::{
    NamespaceMutationKind, PeerNamespaceApplyRequest, commit_namespace_bulk,
    commit_namespace_summary, commit_upload_pointer,
};
use crate::distribution::ncnx::{
    NcnxPublishBatchItem, PEER_NCNX_BATCH_MAX, build_ncnx_peer_routes, publish_new_batch,
};
use crate::distribution::peer::build_peer_routes;
use crate::distribution::rebalance::{
    metadata_matches_current_layout, rewrite_object_to_current_layout,
};
use crate::distribution::reserve_set::build_reserve_peer_routes;
use crate::distribution::routes::build_cluster_routes;
use crate::distribution::status::inspect_cluster_object_status;
use crate::distribution::upload::{
    cancel_standard_cluster_upload, complete_standard_cluster_upload, finalize_cluster_upload,
    finalize_standard_cluster_upload, find_reusable_standard_cluster_upload,
    get_standard_cluster_upload_status, initiate_cluster_upload, initiate_standard_cluster_upload,
    list_standard_cluster_uploads, put_cluster_part, put_standard_cluster_part,
    standard_upload_binding, upload_cluster_one_shot,
};
use crate::enums::app_error_enum::AppError;
use crate::fs::fs_constants::MAX_FILE_SIZE;
use crate::io_control::{record_io_latency, upload_sha256_checksum};
use crate::middleware::auth_middleware::{
    authorize_fragment, logout_refresh_token, refresh_access_token, validate_access,
};
use crate::middleware::rate_limiter_middleware::RateLimitLayers;
use crate::namespace::ncnx_store::NcnxRecord;
use crate::namespace::runtime::{global_namespace_runtime, logical_not_found};
use crate::posix::routes::{build_posix_read_routes, build_posix_write_routes};
use crate::structs::auth_structs::Claims;
use crate::url::signed_read::{
    SignedReadUrlRequest, mint_signed_read_url, read_object_via_signed_url,
};
use crate::websocket::handler::ws_handler;
use crate::websocket::manager::{WsConnectionManager, register_global_ws_manager};
use crate::websocket::ws_structs::{NamespaceChangeEvent, StorageEventPayload};
use axum::body::to_bytes;
use axum::http::{HeaderName, Method, StatusCode};
use axum::middleware::from_fn;
use axum::response::IntoResponse;
use axum::routing::{delete, get, patch, post, put};
use axum::{
    Json as AxumJson, Router,
    extract::{DefaultBodyLimit, Multipart, Path, Query, State},
};
use bytes::BytesMut;
use futures_util::StreamExt;
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tower_http::cors::{Any, CorsLayer};
use ulid::Ulid;

mod batch;
mod logical;
mod multipart;
mod scoped;

use batch::*;
pub use logical::*;
use multipart::*;
use scoped::*;

fn build_cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(Any)
        .allow_headers([
            HeaderName::from_static("authorization"),
            HeaderName::from_static("uid"),
            HeaderName::from_static("content-type"),
            HeaderName::from_static("digest"),
            HeaderName::from_static("x-litiaina-checksum"),
            HeaderName::from_static("x-noa-checksum-sha256"),
            HeaderName::from_static("x-n2-checksum-sha256"),
            HeaderName::from_static("x-n2-coordinator-node"),
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

    let namespace_operation_state = Arc::clone(&app_state);
    tokio::spawn(async move {
        crate::posix::operations::recover_pending_namespace_operations(namespace_operation_state)
            .await;
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
            .route("/noa/v1/capacity/usage", get(get_capacity_usage_scoped))
            .route("/noa/v1/fragments", get(list_fragments))
            .route("/noa/v1/objects", get(list_objects_scoped))
            .route(
                "/noa/v1/objects/signed-url",
                post(mint_signed_read_url_scoped),
            )
            .route("/noa/v1/trash", get(list_trash_objects_scoped))
            .route(
                "/noa/v1/objects/versions/{*object_key}",
                get(list_object_versions_scoped),
            )
            .route("/noa/v1/objects/get/{*object_key}", get(get_object_scoped))
            .route(
                "/noa/v1/objects/stream/{*object_key}",
                get(stream_object_scoped),
            )
            .route(
                "/noa/v1/objects/meta/{*object_key}",
                get(get_object_metadata_scoped),
            )
            .route(
                "/noa/v1/objects/status/{*object_key}",
                get(get_object_repair_status_scoped),
            )
            .route("/noa/v1/objects/download", get(download_object_scoped))
            .route("/noa/v1/exports", post(crate::core::export::create_export))
            .route(
                "/noa/v1/exports/{id}",
                get(crate::core::export::export_status).delete(crate::core::export::cancel_export),
            )
            .route(
                "/noa/v1/exports/{id}/download",
                get(crate::core::export::download_export),
            )
            .route(
                "/noa/v1/exports/{id}/signed-url",
                post(crate::url::signed_export::mint_signed_export_url),
            )
            .route(
                "/noa/v1/objects/download/chunk",
                get(download_object_chunk_scoped),
            )
            .route(
                "/noa/v1/objects/archive",
                post(download_selected_archive_scoped),
            )
            .route(
                "/noa/v1/objects/archive/manifest",
                post(download_manifest_archive_scoped),
            )
            .merge(build_posix_read_routes());

        let read_ops = layers
            .layer_view(read_ops)
            .layer(from_fn(enforce_quiesced_mode_on_reads));

        let write_ops = Router::new()
            .route("/noa/v1/fragments/{:fragment}", delete(delete_fragment))
            .route("/noa/v1/fragments/{:fragment}", patch(update_fragment))
            .route("/noa/v1/objects/rename", post(rename_object_scoped))
            .route(
                "/noa/v1/objects/bulk-delete",
                post(bulk_delete_objects_scoped),
            )
            .route(
                "/noa/v1/maintenance/objects/redistribution/start/{*object_key}",
                post(redistribute_object_scoped),
            )
            .route(
                "/noa/v1/objects/{*object_key}",
                delete(delete_object_scoped),
            )
            .route(
                "/noa/v1/objects/repair/{*object_key}",
                post(repair_object_scoped),
            )
            .route(
                "/noa/v1/objects/bulk-recover",
                post(bulk_recover_objects_scoped),
            )
            .route(
                "/noa/v1/objects/recover/{*object_key}",
                post(recover_object_scoped),
            )
            .route(
                "/noa/v1/upload/multipart/init/{*object_key}",
                post(initiate_object_upload_scoped),
            )
            .route(
                "/noa/v1/upload/one-shot/{*object_key}",
                post(upload_object_one_shot_scoped),
            )
            .route("/noa/v1/upload/batch", post(upload_standard_batch_scoped))
            .route(
                "/noa/v1/upload/multipart/{:version_id}",
                get(get_upload_status_scoped),
            )
            .route("/noa/v1/upload/multipart", get(list_active_uploads_scoped))
            .route(
                "/noa/v1/upload/multipart/{:version_id}/{:part_index}",
                put(object_upload_part_scoped),
            )
            .route(
                "/noa/v1/upload/multipart/{:version_id}/{:session_id}/finalize",
                post(finalize_object_upload_scoped),
            )
            .route(
                "/noa/v1/upload/multipart/{:version_id}",
                delete(cancel_upload_session_scoped),
            )
            .route(
                "/noa/v1/upload/multipart/durable/init/{*object_key}",
                post(initiate_object_upload_durable_scoped),
            )
            .route(
                "/noa/v1/upload/multipart/durable/{:version_id}",
                get(get_upload_status_durable_scoped),
            )
            .route(
                "/noa/v1/upload/multipart/durable",
                get(list_active_uploads_durable_scoped),
            )
            .route(
                "/noa/v1/upload/multipart/durable/{:version_id}/{:part_index}",
                put(object_upload_part_durable_scoped),
            )
            .route(
                "/noa/v1/upload/multipart/durable/{:version_id}/{:session_id}/finalize",
                post(finalize_object_upload_durable_scoped),
            )
            .route(
                "/noa/v1/upload/multipart/durable/{:version_id}",
                delete(cancel_upload_session_durable_scoped),
            )
            .merge(build_posix_write_routes());

        let maintenance_ops = Router::new()
            .route(
                "/noa/v1/maintenance/fragment/cleanup",
                post(sync_fragment_metadata),
            )
            .route(
                "/noa/v1/maintenance/fragment/init",
                post(initialize_fragment_storage_api),
            )
            .route(
                "/noa/v1/maintenance/repair/start",
                post(trigger_repair_job_api),
            )
            .route("/noa/v1/maintenance/repair/stop", post(stop_repair_job_api))
            .route(
                "/noa/v1/maintenance/repair/status",
                get(get_repair_status_api),
            )
            .route("/noa/v1/maintenance/gc/start", post(run_garbage_collector))
            .route(
                "/noa/v1/maintenance/fragments/{:fragment}/gc/start",
                post(run_fragment_gc),
            )
            .route("/noa/v1/maintenance/gc/stop", post(stop_garbage_collector))
            .route("/noa/v1/maintenance/gc/status", get(get_gc_status_api))
            .route(
                "/noa/v1/maintenance/redistribution/start",
                post(trigger_redistribution_job_api),
            )
            .route(
                "/noa/v1/maintenance/fragments/{:fragment}/redistribution/start",
                post(trigger_fragment_redistribution_job),
            )
            .route(
                "/noa/v1/maintenance/redistribution/stop",
                post(stop_redistribution_job_api),
            )
            .route(
                "/noa/v1/maintenance/redistribution/status",
                get(get_redist_status_api),
            )
            .route(
                "/noa/v1/maintenance/fragments/{:fragment}/repair/start",
                post(trigger_fragment_repair_job),
            )
            .route(
                "/noa/v1/maintenance/fragments/{:fragment}/health",
                get(get_fragment_health),
            )
            .route("/noa/v1/maintenance/health", get(get_system_health))
            .merge(build_cluster_routes());

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

    let ws_routes = { Router::new().route("/noa/v1/ws", get(ws_handler)) };

    let public_global_routes = Router::new()
        .route(
            "/noa/v1/signed-exports/download",
            get(crate::url::signed_export::download_signed_export),
        )
        .route("/noa/v1/fragments/check", get(verify_fragment_existence))
        .route("/noa/v1/fragments/hash", get(get_fragment_hash))
        .route(
            "/noa/v1/objects/signed-read",
            get(read_object_via_signed_url),
        );
    let public_global_routes = layers.layer_global(public_global_routes);

    let peer_routes = Router::new()
        .merge(crate::core::export::pins::routes())
        .merge(build_peer_routes())
        .merge(build_ncnx_peer_routes())
        .merge(build_auto_healing_peer_routes())
        .merge(build_distributed_maintenance_peer_routes())
        .merge(build_reserve_peer_routes())
        .merge(build_lifecycle_peer_routes());

    let create_fragment_route = Router::new().route("/noa/v1/fragments", post(create_fragment));
    let create_fragment_route = layers
        .layer_auth(create_fragment_route)
        .layer(from_fn(enforce_read_only_fast_mode_on_mutations));

    let fragment_auth_route =
        Router::new().route("/noa/v1/auth/fragment", post(authorize_fragment));
    let fragment_auth_route = layers.layer_auth(fragment_auth_route);

    let refresh_auth_route =
        Router::new().route("/noa/v1/auth/refresh", post(refresh_access_token));
    let refresh_auth_route = layers.layer_auth(refresh_auth_route);

    let logout_auth_route = Router::new().route("/noa/v1/auth/logout", post(logout_refresh_token));
    let logout_auth_route = layers.layer_auth(logout_auth_route);

    let ping_route = Router::new().route("/noa/v1/ping", get(ping));
    let ping_route = layers.layer_global(ping_route);

    let mut router = Router::new()
        .merge(ping_route)
        .merge(fragment_auth_route)
        .merge(refresh_auth_route)
        .merge(logout_auth_route)
        .merge(public_global_routes)
        .merge(peer_routes)
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

#[cfg(test)]
#[path = "storage_routes/tests.rs"]
mod noa_route_tests;
