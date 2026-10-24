use crate::core::adaptive_concurrency::run_adaptive_concurrency_loop;

mod small_batch;
mod small_batch_request;
use crate::core::archive::{
    download_logical_archive as download_logical_archive_impl,
    download_manifest_archive as download_manifest_archive_impl,
    download_selected_archive as download_selected_archive_impl,
};
use crate::core::auto_healing::run_auto_healing_controller;
use crate::core::delete::{BulkObjectFailure, BulkObjectKeysRequest, BulkObjectOperationResponse};
use crate::core::download::{
    download_object as download_object_impl, download_object_chunk as download_object_chunk_impl,
};
use crate::core::export::{cancel_export, create_export, download_export, export_status};
use crate::core::fragment_manager::{
    create_fragment, delete_fragment, get_fragment_hash, update_fragment, verify_fragment_existence,
};
use crate::core::fragment_metadata::resolve_stable_fragment_id;
use crate::core::garbage_collector::{
    get_gc_status_api, run_garbage_collector, run_lifecycle_manager, stop_garbage_collector,
};
use crate::core::maintenance::sync_fragment_metadata;
use crate::core::monitoring::get_system_health;
use crate::core::operation_gate::{
    enforce_quiesced_mode_on_reads, enforce_read_only_fast_mode_on_mutations,
};
use crate::core::redistribution::{
    get_redist_status_api, stop_redistribution_job_api, trigger_redistribution_job_api,
};
use crate::core::repair_manager::{
    get_repair_status_api, inspect_object_status, parse_check_mode, repair_object,
    stop_repair_job_api, trigger_repair_job_api,
};
use crate::core::retrieve::{
    get_object as get_object_impl, get_object_metadata_direct, list_fragments,
    stream_object as stream_object_impl,
};
use crate::core::storage_capacity_parser::{
    get_capacity_usage as get_capacity_usage_impl, get_fragment_usage as get_fragment_usage_impl,
};
use crate::core::sync::initialize_fragment_storage_api;
use crate::core::upload::{
    AdaptiveIoGuard, acquire_upload_io_guard_for_query, cancel_standard_upload_direct,
    cancel_upload_session as durable_cancel_upload_session_impl, complete_standard_upload,
    finalize_object_upload_direct as durable_finalize_object_upload_direct,
    finalize_standard_upload_direct, find_reusable_standard_upload,
    get_standard_upload_status_direct, get_upload_status as durable_get_upload_status_impl,
    initiate_object_upload as durable_initiate_object_upload_impl,
    initiate_standard_upload_direct_with_reuse,
    list_active_uploads as durable_list_active_uploads_impl, list_standard_uploads_direct,
    object_upload_part as durable_object_upload_part_impl, object_upload_part_standard_body_direct,
    upload_object_one_shot_body_direct, upload_object_one_shot_metadata,
    upload_uses_strict_durability,
};
use crate::core::{index, index_store};
use crate::enums::app_error_enum::AppError;
use crate::io_control::record_io_latency;
use crate::middleware::auth_middleware::{
    authorize_fragment, logout_refresh_token, refresh_access_token, validate_access,
};
use crate::middleware::rate_limiter_middleware::RateLimitLayers;
use crate::namespace::runtime::{global_namespace_runtime, logical_not_found};
use crate::posix::routes::{build_posix_read_routes, build_posix_write_routes};
use crate::routes::accel_routes::proxy_namespace_search;
use crate::structs::auth_structs::Claims;
use crate::url::signed_export::{download_signed_export, mint_signed_export_url};
use crate::url::signed_read::{
    SignedReadUrlRequest, mint_signed_read_url, read_object_via_signed_url,
};
use crate::websocket::handler::ws_handler;
use crate::websocket::manager::{WsConnectionManager, register_global_ws_manager};
use crate::websocket::ws_structs::{NamespaceChangeEvent, StorageEventPayload};
use axum::body::{Body, to_bytes};
use axum::http::{HeaderName, Method, StatusCode};
use axum::middleware::{Next, from_fn};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, post, put};
use axum::{
    Json as AxumJson, Router,
    extract::{DefaultBodyLimit, Multipart, Path, Query, Request, State},
};
use bytes::{Bytes, BytesMut};
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tokio_stream::wrappers::ReceiverStream;
use tower_http::cors::{Any, CorsLayer};

const MAX_HTTP_BODY_SIZE: usize = 5 * 1024 * 1024 * 1024;
const MAX_EXPORT_REQUEST_BODY_SIZE: usize = 4 * 1024 * 1024;
const NAMESPACE_SNAPSHOT_CHANNEL_CAPACITY: usize = 128;

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum NamespaceSnapshotFrame {
    Entry {
        object_key: String,
        version_id: String,
        content_type: String,
        total_size_bytes: u64,
        created_at: i64,
    },
    Complete {
        entry_count: usize,
    },
}

pub struct AppState {
    pub ws_manager: WsConnectionManager,
}

impl AppState {
    pub fn emit(&self, uid: &str, payload: StorageEventPayload) {
        self.ws_manager.publish(uid, payload);
    }
}

async fn parse_json_response<T: DeserializeOwned>(
    response: axum::response::Response,
    context: &str,
) -> Result<T, AppError> {
    let bytes = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .map_err(|e| AppError::InternalServerError(format!("{context} body read failed: {e}")))?;
    serde_json::from_slice(&bytes)
        .map_err(|e| AppError::InternalServerError(format!("{context} json decode failed: {e}")))
}

pub(crate) async fn resolve_logical_read_key(
    fragment: &str,
    requested_path: &str,
) -> Result<String, AppError> {
    let requested = requested_path.trim_start_matches('/').trim();
    if let Some(entry) = global_namespace_runtime()
        .entry(fragment, requested)
        .await?
    {
        if entry.kind != crate::namespace::models::NamespaceEntryKind::File
            || !entry.lifecycle.is_live()
        {
            return Err(logical_not_found());
        }
        return entry.object_id.ok_or_else(|| {
            AppError::InternalServerError("logical file is missing physical object id".to_string())
        });
    }
    index_store::normalize_object_id(requested).map_err(|_| logical_not_found())
}

fn download_filename_for_logical_path(requested_path: &str, resolved_key: &str) -> Option<String> {
    let requested = requested_path.trim_start_matches('/').trim();
    let resolved = resolved_key.trim_start_matches('/').trim();
    if requested.is_empty() || requested == resolved {
        return None;
    }
    std::path::Path::new(requested)
        .file_name()
        .and_then(|name| name.to_str())
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(ToOwned::to_owned)
}

fn looks_like_flat_storage_object_id(requested_path: &str) -> bool {
    let normalized = requested_path.trim_start_matches('/');
    !normalized.is_empty()
        && !normalized.contains('/')
        && normalized.len() == 64
        && normalized.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(crate) async fn resolve_logical_write_key(
    fragment: &str,
    requested_path: &str,
) -> Result<String, AppError> {
    if looks_like_flat_storage_object_id(requested_path) {
        if let Some(entry) = global_namespace_runtime()
            .entry(fragment, requested_path)
            .await?
        {
            if entry.kind != crate::namespace::models::NamespaceEntryKind::File {
                return Err(AppError::Conflict(format!(
                    "logical path '{}' already exists as a directory",
                    requested_path.trim_start_matches('/')
                )));
            }
            return entry.object_id.ok_or_else(|| {
                AppError::InternalServerError(
                    "logical file is missing physical object id".to_string(),
                )
            });
        }
        return Ok(requested_path.trim_start_matches('/').to_string());
    }
    global_namespace_runtime()
        .select_object_id_for_write(fragment, requested_path)
        .await
}

fn upload_request_matches_session(
    params: &crate::core::disk_manager::InitiateUploadParams,
    state: &crate::core::index::IndexSessionState,
) -> bool {
    let part_size_matches = match params.part_size_bytes {
        Some(requested) if requested > 0 => {
            state.manifest.part_size_bytes == 0 || state.manifest.part_size_bytes == requested
        }
        _ => true,
    };
    let content_type_matches = params
        .content_type
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|requested| requested == state.manifest.content_type)
        .unwrap_or(true);

    state.finalized_at.is_none()
        && state.aborted_at.is_none()
        && state.manifest.total_size_bytes == params.total_size_bytes
        && state.manifest.expected_parts == params.expected_parts
        && part_size_matches
        && content_type_matches
}

pub(crate) async fn find_reusable_pending_upload(
    fragment: &str,
    logical_path: &str,
    params: &crate::core::disk_manager::InitiateUploadParams,
) -> Result<Option<crate::core::disk_manager::InitiateUploadResponse>, AppError> {
    let normalized = crate::namespace::paths::normalize_path(logical_path)
        .map_err(|err| AppError::BadRequest(err.to_string()))?;
    let mut bindings = global_namespace_runtime()
        .list_pending_uploads(fragment)
        .await?
        .into_iter()
        .filter(|binding| binding.logical_path == normalized)
        .collect::<Vec<_>>();
    bindings.sort_by(|a, b| b.created_at_unix.cmp(&a.created_at_unix));

    for binding in bindings {
        let Some(state) = index::load_session(fragment, &binding.version_id).await? else {
            let _ = global_namespace_runtime()
                .clear_pending_upload(fragment, &binding.version_id)
                .await;
            continue;
        };

        if upload_request_matches_session(params, &state) {
            return Ok(Some(crate::core::disk_manager::InitiateUploadResponse {
                session_id: binding.session_id,
                version_id: binding.version_id,
            }));
        }
    }
    Ok(None)
}

async fn apply_logical_upload_binding_to_status(
    fragment: &str,
    status: &mut crate::core::disk_manager::UploadStatusResponse,
) -> Result<(), AppError> {
    let Some(version_id) = status.version_id.as_deref() else {
        return Ok(());
    };
    if let Some(binding) = global_namespace_runtime()
        .pending_upload(fragment, version_id)
        .await?
    {
        status.physical_object_key = status
            .object_key
            .take()
            .or_else(|| Some(binding.physical_object_id.clone()));
        status.object_key = Some(binding.logical_path);
    }
    Ok(())
}

async fn apply_logical_upload_bindings_to_sessions(
    fragment: &str,
    sessions: &mut [crate::core::upload::ActiveSessionInfo],
) -> Result<(), AppError> {
    for session in sessions.iter_mut() {
        if let Some(binding) = global_namespace_runtime()
            .pending_upload(fragment, &session.version_id)
            .await?
        {
            session.physical_object_key = session
                .object_key
                .take()
                .or_else(|| Some(binding.physical_object_id.clone()));
            session.object_key = Some(binding.logical_path);
        }
    }
    Ok(())
}

pub(crate) async fn commit_logical_upload_from_meta(
    fragment: &str,
    logical_path: &str,
    physical_object_id: &str,
    metadata: &crate::core::disk_manager::ObjectMetadata,
) -> Result<String, AppError> {
    global_namespace_runtime()
        .finalize_logical_upload(fragment, logical_path, physical_object_id, metadata)
        .await
}

async fn commit_logical_upload_from_meta_with_durability(
    fragment: &str,
    logical_path: &str,
    physical_object_id: &str,
    metadata: &crate::core::disk_manager::ObjectMetadata,
    strict_durability: bool,
) -> Result<String, AppError> {
    global_namespace_runtime()
        .finalize_logical_upload_with_durability(
            fragment,
            logical_path,
            physical_object_id,
            metadata,
            strict_durability,
        )
        .await
}

pub(crate) fn emit_logical_file_uploaded(
    state: &Arc<AppState>,
    fragment: &str,
    logical_path: &str,
    physical_object_id: &str,
    metadata: &crate::core::disk_manager::ObjectMetadata,
) {
    state.emit(
        fragment,
        StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::file_upsert(
            fragment,
            logical_path,
            physical_object_id,
            metadata.total_size_bytes,
            &metadata.version_id,
            &metadata.content_type,
            metadata.created_at,
        )),
    );
}

async fn mint_signed_read_url_scoped(
    claims: Claims,
    AxumJson(mut req): AxumJson<SignedReadUrlRequest>,
) -> Result<impl IntoResponse, AppError> {
    let requested = req.object_key.clone();
    let resolved = resolve_logical_read_key(&claims.fragment, &requested).await?;
    if req.filename.is_none() {
        req.filename = download_filename_for_logical_path(&requested, &resolved);
    }
    req.object_key = resolved;
    mint_signed_read_url(claims, AxumJson(req)).await
}

async fn get_capacity_usage_scoped(claims: Claims) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    get_capacity_usage_impl(claims, Path(fragment)).await
}

async fn get_fragment_usage_scoped(claims: Claims) -> Result<impl IntoResponse, AppError> {
    get_fragment_usage_impl(claims).await
}

async fn run_fragment_gc(
    claims: Claims,
    State(state): State<Arc<AppState>>,
    Path(fragment): Path<String>,
    Query(mut params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let fragment = match resolve_stable_fragment_id(&fragment).await {
        Ok(stable) => stable,
        Err(err) => return err.into_response(),
    };
    params.insert("fragment".to_string(), fragment);
    run_garbage_collector(claims, State(state), Query(params))
        .await
        .into_response()
}

async fn trigger_fragment_repair_job(
    claims: Claims,
    State(state): State<Arc<AppState>>,
    Path(fragment): Path<String>,
    Query(mut params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = resolve_stable_fragment_id(&fragment).await?;
    params.insert("fragment".to_string(), fragment);
    trigger_repair_job_api(claims, State(state), Query(params)).await
}

async fn trigger_fragment_redistribution_job(
    claims: Claims,
    State(state): State<Arc<AppState>>,
    Path(fragment): Path<String>,
    Query(mut params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = resolve_stable_fragment_id(&fragment).await?;
    params.insert("fragment".to_string(), fragment);
    trigger_redistribution_job_api(claims, State(state), Query(params)).await
}

async fn trigger_object_redistribution_job(
    claims: Claims,
    State(state): State<Arc<AppState>>,
    Path(object_key): Path<String>,
    Query(mut params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    let object_id = resolve_logical_read_key(&fragment, &object_key).await?;
    params.insert("fragment".to_string(), fragment);
    params.insert("object_key".to_string(), object_id);
    params.insert("scope".to_string(), "object".to_string());
    trigger_redistribution_job_api(claims, State(state), Query(params)).await
}

async fn get_fragment_health(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path(fragment): Path<String>,
    Query(mut params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = resolve_stable_fragment_id(&fragment).await?;
    params.insert("fragment".to_string(), fragment);
    get_system_health(State(state), claims, Query(params)).await
}

async fn list_objects_scoped(
    claims: Claims,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    Ok(AxumJson(
        global_namespace_runtime()
            .list_objects_page(&fragment, &params, false)
            .await?,
    ))
}

async fn list_trash_objects_scoped(
    claims: Claims,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    Ok(AxumJson(
        global_namespace_runtime()
            .list_objects_page(&fragment, &params, true)
            .await?,
    ))
}

async fn stream_namespace_snapshot_scoped(claims: Claims) -> Result<Response<Body>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let fragment = claims.fragment;
    let mut source = global_namespace_runtime().stream_live_object_refs(
        &fragment,
        None,
        NAMESPACE_SNAPSHOT_CHANNEL_CAPACITY,
    );
    let (sender, receiver) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(
        NAMESPACE_SNAPSHOT_CHANNEL_CAPACITY,
    );
    tokio::spawn(async move {
        let mut entry_count = 0usize;
        while let Some(result) = source.recv().await {
            let object = match result {
                Ok(object) => object,
                Err(error) => {
                    let _ = sender
                        .send(Err(std::io::Error::other(error.to_string())))
                        .await;
                    return;
                }
            };
            let frame = NamespaceSnapshotFrame::Entry {
                object_key: object.path,
                version_id: object.version_id,
                content_type: object.content_type,
                total_size_bytes: object.total_size_bytes,
                created_at: object.created_at,
            };
            let mut encoded = match serde_json::to_vec(&frame) {
                Ok(encoded) => encoded,
                Err(error) => {
                    let _ = sender.send(Err(std::io::Error::other(error))).await;
                    return;
                }
            };
            encoded.push(b'\n');
            if sender.send(Ok(Bytes::from(encoded))).await.is_err() {
                return;
            }
            entry_count = entry_count.saturating_add(1);
        }
        let mut encoded =
            match serde_json::to_vec(&NamespaceSnapshotFrame::Complete { entry_count }) {
                Ok(encoded) => encoded,
                Err(error) => {
                    let _ = sender.send(Err(std::io::Error::other(error))).await;
                    return;
                }
            };
        encoded.push(b'\n');
        let _ = sender.send(Ok(Bytes::from(encoded))).await;
    });

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/x-ndjson")
        .header("cache-control", "private, no-store")
        .body(Body::from_stream(ReceiverStream::new(receiver)))
        .map_err(|error| AppError::InternalServerError(error.to_string()))
}

async fn list_object_versions_scoped(
    claims: Claims,
    Path(object_key): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let requested_path = object_key;
    let physical = resolve_logical_read_key(&fragment, &requested_path).await?;
    let mut versions = index::list_live_object_versions_metadata(&fragment, &physical).await?;
    versions.sort_by(|a, b| b.version_id.cmp(&a.version_id));
    for metadata in &mut versions {
        metadata.object_key = requested_path.clone();
        metadata.strip_internal_layout();
    }
    Ok(AxumJson(versions))
}

async fn get_object_scoped(
    claims: Claims,
    Path(object_key): Path<String>,
    req: axum::http::Request<axum::body::Body>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    let physical = resolve_logical_read_key(&fragment, &object_key).await?;
    get_object_impl(claims, Path((fragment, physical)), req).await
}

async fn stream_object_scoped(
    claims: Claims,
    headers: axum::http::HeaderMap,
    Path(object_key): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    let physical = resolve_logical_read_key(&fragment, &object_key).await?;
    stream_object_impl(claims, headers, Path((fragment, physical))).await
}

async fn get_object_metadata_scoped(
    claims: Claims,
    Path(object_key): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let requested_path = object_key;
    let physical = resolve_logical_read_key(&fragment, &requested_path).await?;
    let mut metadata = get_object_metadata_direct(&fragment, &physical)
        .await?
        .without_internal_layout();
    metadata.object_key = requested_path;
    Ok(AxumJson(metadata))
}

async fn download_object_scoped(
    claims: Claims,
    Query(mut query): Query<crate::core::disk_manager::ObjectKeyQuery>,
    headers: axum::http::HeaderMap,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    let requested = query.object_key.clone();
    let resolved = resolve_logical_read_key(&fragment, &requested).await?;
    if query.filename.is_none() {
        query.filename = download_filename_for_logical_path(&requested, &resolved);
    }
    query.object_key = resolved;
    download_object_impl(claims, Path(fragment), Query(query), headers).await
}

async fn download_object_chunk_scoped(
    claims: Claims,
    Query(mut params): Query<crate::core::disk_manager::ChunkQuery>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    params.object_key = resolve_logical_read_key(&fragment, &params.object_key).await?;
    download_object_chunk_impl(claims, Path(fragment), Query(params)).await
}

async fn download_selected_archive_scoped(
    claims: Claims,
    AxumJson(req): AxumJson<crate::core::disk_manager::DownloadArchiveRequest>,
) -> Result<axum::response::Response, AppError> {
    let fragment = claims.fragment.clone();
    if !req.export_all
        && (!req.keys.is_empty()
            || req
                .prefix
                .as_deref()
                .is_some_and(|prefix| !prefix.trim().is_empty())
            || req.prefixes.iter().any(|prefix| !prefix.trim().is_empty()))
    {
        return download_logical_archive_impl(fragment, req).await;
    }
    Ok(
        download_selected_archive_impl(claims, Path(fragment), AxumJson(req))
            .await?
            .into_response(),
    )
}

async fn download_manifest_archive_scoped(
    claims: Claims,
    Query(query): Query<crate::core::disk_manager::DownloadArchiveManifestQuery>,
    body: axum::body::Body,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    download_manifest_archive_impl(claims, Path(fragment), Query(query), body).await
}

async fn delete_object_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path(object_key): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.delete {
        return Err(AppError::Forbidden("Delete permission required".into()));
    }
    let fragment = claims.fragment.clone();
    let summary = global_namespace_runtime()
        .trash(&fragment, &object_key)
        .await?;
    state.emit(
        &fragment,
        StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::from_summary(
            &fragment,
            summary.clone(),
        )),
    );
    Ok((
        StatusCode::OK,
        AxumJson(serde_json::json!({
            "operation": summary.operation,
            "affected_paths": summary.affected_paths,
            "affected_objects": summary.affected_objects,
            "conflicted_paths": summary.conflicted_paths,
            "failed_paths": summary.failed_paths,
            "trash_entry_id": summary.trash_entry_id,
            "deleted_at_unix": summary.deleted_at_unix,
        })),
    ))
}

async fn recover_object_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path(object_key): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden(
            "Modify permission required to recover".into(),
        ));
    }
    let fragment = claims.fragment.clone();
    let summary = global_namespace_runtime()
        .recover(&fragment, &object_key)
        .await?;
    state.emit(
        &fragment,
        StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::from_summary(
            &fragment,
            summary.clone(),
        )),
    );
    Ok((
        StatusCode::OK,
        AxumJson(serde_json::json!({
            "operation": summary.operation,
            "affected_paths": summary.affected_paths,
            "affected_objects": summary.affected_objects,
            "conflicted_paths": summary.conflicted_paths,
            "failed_paths": summary.failed_paths,
        })),
    ))
}

async fn get_object_repair_status_scoped(
    claims: Claims,
    Path(object_key): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let fragment = claims.fragment.clone();
    let physical = resolve_logical_read_key(&fragment, &object_key).await?;
    let metadata = get_object_metadata_direct(&fragment, &physical).await?;
    let mut status = inspect_object_status(&metadata, parse_check_mode(&params)).await?;
    status.object_key = object_key.trim_start_matches('/').to_string();
    Ok(AxumJson(status))
}

async fn repair_object_scoped(
    claims: Claims,
    Path(object_key): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write || !claims.modify {
        return Err(AppError::Forbidden(
            "Write and Modify permission required".into(),
        ));
    }
    let fragment = claims.fragment.clone();
    let physical = resolve_logical_read_key(&fragment, &object_key).await?;
    let metadata = get_object_metadata_direct(&fragment, &physical).await?;
    let mut status = repair_object(&metadata, parse_check_mode(&params)).await?;
    status.object_key = object_key.trim_start_matches('/').to_string();
    Ok(AxumJson(status))
}

async fn bulk_delete_objects_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumJson(payload): AxumJson<BulkObjectKeysRequest>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.delete {
        return Err(AppError::Forbidden("Delete permission required".into()));
    }
    let fragment = claims.fragment.clone();
    let requested = payload.object_keys.len();
    let unique_keys = payload
        .object_keys
        .into_iter()
        .filter(|key| !key.trim().is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if unique_keys.is_empty() {
        return Err(AppError::BadRequest("object_keys must not be empty".into()));
    }

    let mut succeeded = 0usize;
    let mut not_found = 0usize;
    let mut failed = Vec::new();
    for object_key in &unique_keys {
        match global_namespace_runtime()
            .trash(&fragment, object_key)
            .await
        {
            Ok(_) => succeeded = succeeded.saturating_add(1),
            Err(AppError::NotFound) => not_found = not_found.saturating_add(1),
            Err(err) => failed.push(BulkObjectFailure {
                object_key: object_key.clone(),
                error: err.to_string(),
            }),
        }
    }
    if succeeded > 0 {
        state.emit(
            &fragment,
            StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::bulk(
                &fragment,
                "bulk_delete",
                succeeded,
            )),
        );
    }
    Ok((
        StatusCode::OK,
        AxumJson(BulkObjectOperationResponse {
            requested,
            unique: unique_keys.len(),
            succeeded,
            not_found,
            failed,
        }),
    ))
}

async fn bulk_recover_objects_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumJson(payload): AxumJson<BulkObjectKeysRequest>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden(
            "Modify permission required to recover".into(),
        ));
    }
    let fragment = claims.fragment.clone();
    let requested = payload.object_keys.len();
    let unique_keys = payload
        .object_keys
        .into_iter()
        .filter(|key| !key.trim().is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if unique_keys.is_empty() {
        return Err(AppError::BadRequest("object_keys must not be empty".into()));
    }

    let mut succeeded = 0usize;
    let mut not_found = 0usize;
    let mut failed = Vec::new();
    for object_key in &unique_keys {
        match global_namespace_runtime()
            .recover(&fragment, object_key)
            .await
        {
            Ok(_) => succeeded = succeeded.saturating_add(1),
            Err(AppError::NotFound) => not_found = not_found.saturating_add(1),
            Err(err) => failed.push(BulkObjectFailure {
                object_key: object_key.clone(),
                error: err.to_string(),
            }),
        }
    }
    if succeeded > 0 {
        state.emit(
            &fragment,
            StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::bulk(
                &fragment,
                "bulk_recover",
                succeeded,
            )),
        );
    }
    Ok((
        StatusCode::OK,
        AxumJson(BulkObjectOperationResponse {
            requested,
            unique: unique_keys.len(),
            succeeded,
            not_found,
            failed,
        }),
    ))
}

async fn recover_all_trash_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    if !claims.modify {
        return Err(AppError::Forbidden(
            "Modify permission required to recover all".into(),
        ));
    }
    let summary = global_namespace_runtime().recover_all(&fragment).await?;
    state.emit(
        &fragment,
        StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::from_summary(
            &fragment,
            summary.clone(),
        )),
    );
    Ok((
        StatusCode::OK,
        AxumJson(serde_json::json!({
            "operation": summary.operation,
            "affected_paths": summary.affected_paths,
            "affected_objects": summary.affected_objects,
            "conflicted_paths": summary.conflicted_paths,
            "failed_paths": summary.failed_paths,
        })),
    ))
}

async fn rename_object_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumJson(mut payload): AxumJson<crate::core::disk_manager::RenameObjectRequest>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden("Modify permission required".into()));
    }
    payload.source_fragment = resolve_stable_fragment_id(&payload.source_fragment).await?;
    payload.destination_fragment =
        resolve_stable_fragment_id(&payload.destination_fragment).await?;
    if payload.source_fragment != claims.fragment || payload.destination_fragment != claims.fragment
    {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let summary = global_namespace_runtime()
        .rename(
            &claims.fragment,
            &payload.source_key,
            &payload.destination_key,
        )
        .await?;

    state.emit(
        &claims.fragment,
        StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::from_summary(
            &claims.fragment,
            summary,
        )),
    );
    Ok(StatusCode::OK)
}

#[derive(Default, Deserialize)]
struct UploadInitiationQuery {
    /// Editors know their own session ID; never adopt another same-sized save.
    #[serde(default)]
    fresh: bool,
}

async fn initiate_object_upload_scoped(
    claims: Claims,
    Path(object_key): Path<String>,
    Query(options): Query<UploadInitiationQuery>,
    AxumJson(params): AxumJson<crate::core::disk_manager::InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    if !options.fresh {
        if let Some(existing) = find_reusable_standard_upload(&fragment, &object_key, &params)? {
            return Ok((StatusCode::CREATED, AxumJson(existing)).into_response());
        }
    }
    let physical = resolve_logical_write_key(&fragment, &object_key).await?;
    let response = initiate_standard_upload_direct_with_reuse(
        claims,
        fragment,
        physical,
        object_key,
        params,
        !options.fresh,
    )
    .await?;
    Ok((StatusCode::CREATED, AxumJson(response)).into_response())
}

async fn get_upload_status_scoped(
    claims: Claims,
    Path(version_id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    Ok(AxumJson(get_standard_upload_status_direct(
        &claims,
        &fragment,
        &version_id,
    )?))
}

async fn list_active_uploads_scoped(
    claims: Claims,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let limit = params
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(50)
        .min(100);
    let offset = params
        .get("offset")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    let fragment = claims.fragment.clone();
    Ok(AxumJson(list_standard_uploads_direct(
        &claims, &fragment, limit, offset,
    )?))
}

async fn object_upload_part_scoped(
    claims: Claims,
    headers: axum::http::HeaderMap,
    Path((version_id, part_index)): Path<(String, u64)>,
    Query(query): Query<crate::core::disk_manager::UploadPartQuery>,
    body: axum::body::Body,
) -> Result<impl IntoResponse, AppError> {
    let content_length = headers
        .get("content-length")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| AppError::BadRequest("Content-Length header required".into()))?;
    let fragment = claims.fragment.clone();
    object_upload_part_standard_body_direct(
        claims,
        fragment,
        version_id,
        part_index,
        query,
        body,
        content_length,
    )
    .await?;
    Ok((StatusCode::OK, "Part uploaded"))
}

async fn finalize_object_upload_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((version_id, session_id)): Path<(String, String)>,
    Query(key_query): Query<crate::core::disk_manager::ObjectKeyQuery>,
    AxumJson(_params): AxumJson<crate::core::disk_manager::InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    let gate_context = format!("standard-finalize fragment={fragment}, version={version_id}");
    let finalize_io_guard =
        acquire_upload_io_guard_for_query(Some(true), &fragment, &gate_context).await?;
    let _upload_activity =
        crate::core::upload::standard_namespace_activity(&fragment, &version_id)?;
    let (finalized, strict_durability) =
        finalize_standard_upload_direct(claims, fragment.clone(), version_id.clone(), session_id)
            .await?;
    let response = match finalized {
        crate::core::upload::FinalizeUploadResult::Incomplete(incomplete) => {
            (StatusCode::CONFLICT, AxumJson(incomplete)).into_response()
        }
        crate::core::upload::FinalizeUploadResult::Complete(mut metadata) => {
            let logical_path = key_query.object_key;
            let physical = metadata.object_key.clone();
            let namespace_started = Instant::now();
            let committed_result = commit_logical_upload_from_meta_with_durability(
                &fragment,
                &logical_path,
                &physical,
                &metadata,
                strict_durability,
            )
            .await;
            record_io_latency(
                "upload.standard.finalize.namespace",
                namespace_started.elapsed(),
                metadata.total_size_bytes,
                committed_result.is_ok(),
            );
            let committed_physical = committed_result?;
            complete_standard_upload(&fragment, &version_id)?;
            metadata.object_key = logical_path;
            emit_logical_file_uploaded(
                &state,
                &fragment,
                &metadata.object_key,
                &committed_physical,
                &metadata,
            );
            metadata.strip_internal_layout();
            (StatusCode::OK, AxumJson(metadata)).into_response()
        }
    };
    if let Some(guard) = finalize_io_guard {
        guard.complete();
    }
    Ok(response)
}

async fn cancel_upload_session_scoped(
    claims: Claims,
    Path(version_id): Path<String>,
    Query(_key_query): Query<crate::core::disk_manager::ObjectKeyQuery>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    cancel_standard_upload_direct(&claims, &fragment, &version_id)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn initiate_object_upload_durable_scoped(
    claims: Claims,
    Path(object_key): Path<String>,
    Query(options): Query<UploadInitiationQuery>,
    AxumJson(params): AxumJson<crate::core::disk_manager::InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    if !options.fresh {
        if let Some(existing) =
            find_reusable_pending_upload(&fragment, &object_key, &params).await?
        {
            return Ok((StatusCode::CREATED, AxumJson(existing)).into_response());
        }
    }
    let physical = resolve_logical_write_key(&fragment, &object_key).await?;
    let response = durable_initiate_object_upload_impl(
        claims,
        Path((fragment.clone(), physical.clone())),
        AxumJson(params),
    )
    .await?
    .into_response();
    if response.status() != StatusCode::CREATED {
        return Ok(response);
    }
    let parsed: crate::core::disk_manager::InitiateUploadResponse =
        parse_json_response(response, "initiate upload").await?;
    global_namespace_runtime()
        .bind_pending_upload(
            &fragment,
            &object_key,
            &physical,
            &parsed.version_id,
            &parsed.session_id,
        )
        .await?;
    Ok((StatusCode::CREATED, AxumJson(parsed)).into_response())
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
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    // Admit before choosing identity, so queued same-key writers resolve the
    // published identity instead of all staging independent provisional IDs.
    let io_guard =
        acquire_upload_io_guard_for_query(query.io_gate, &fragment, "logical one-shot upload")
            .await?;
    let io_progress = io_guard.as_ref().map(AdaptiveIoGuard::progress_handle);
    let physical = resolve_logical_write_key(&fragment, &object_key).await?;
    let strict_durability = upload_uses_strict_durability(&query);
    let _upload_activity = crate::core::upload::UploadActivityGuard::begin(&fragment, &physical)?;
    let mut parsed = upload_object_one_shot_metadata(
        claims,
        headers,
        Path((fragment.clone(), physical.clone())),
        Query(query),
        body,
        false,
        io_progress,
    )
    .await?;
    let committed_physical = commit_logical_upload_from_meta_with_durability(
        &fragment,
        &object_key,
        &physical,
        &parsed,
        strict_durability,
    )
    .await?;
    parsed.object_key = object_key;
    emit_logical_file_uploaded(
        &state,
        &fragment,
        &parsed.object_key,
        &committed_physical,
        &parsed,
    );
    if let Some(guard) = io_guard {
        guard.complete();
    }
    parsed.strip_internal_layout();
    Ok((StatusCode::CREATED, AxumJson(parsed)).into_response())
}

async fn guard_upload_lifecycle(
    claims: Claims,
    request: Request,
    next: Next,
) -> Result<Response, AppError> {
    if request.method() == Method::GET
        || !request.uri().path().starts_with("/noa/v1/upload/")
        || request.uri().path() == "/noa/v1/upload/batch"
    {
        return Ok(next.run(request).await);
    }
    let lifecycle = crate::core::fragment_lifecycle::acquire_live(&claims.fragment).await?;
    // Disk workers cannot be cancelled once started. Keep their shared
    // admission until the whole request's publication/cleanup has finished.
    tokio::spawn(async move {
        let _lifecycle = lifecycle;
        next.run(request).await
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("upload task failed: {e}")))
}

const STANDARD_BATCH_OBJECT_MAX_BYTES: usize = 1024 * 1024;
const STANDARD_BATCH_MAX_CONCURRENCY: usize = 128;
const STANDARD_BATCH_MAX_OBJECTS: usize = 4096;

#[derive(Debug, Deserialize)]
struct StandardBatchUploadQuery {
    #[serde(default)]
    concurrency: Option<usize>,
    #[serde(default)]
    durability: Option<String>,
    #[serde(default)]
    strict_durability: Option<bool>,
}

#[derive(Debug, Serialize)]
struct StandardBatchUploadEntryResult {
    object_key: String,
    status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    version_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug)]
struct IndexedStandardBatchUploadResult {
    index: usize,
    result: StandardBatchUploadEntryResult,
}

#[derive(Debug, Serialize)]
struct StandardBatchUploadResponse {
    requested: usize,
    succeeded: usize,
    failed: usize,
    results: Vec<StandardBatchUploadEntryResult>,
}

fn default_standard_batch_concurrency() -> usize {
    crate::core::io_scheduler::disk_io_scheduler_snapshot()
        .foreground_target
        .clamp(1, STANDARD_BATCH_MAX_CONCURRENCY)
}

fn decode_standard_batch_object_key(
    field_name: Option<&str>,
    file_name: Option<&str>,
) -> Result<String, AppError> {
    if field_name != Some("object") {
        return Err(AppError::BadRequest(
            "batch file fields must be named 'object'".to_string(),
        ));
    }
    let encoded = file_name.ok_or_else(|| {
        AppError::BadRequest(
            "batch object fields require a percent-encoded object key as filename".to_string(),
        )
    })?;
    let decoded = percent_decode_str(encoded)
        .decode_utf8()
        .map_err(|_| AppError::BadRequest("batch object key is not valid UTF-8".to_string()))?;
    let key = decoded.trim_start_matches('/').trim();
    if key.is_empty() {
        return Err(AppError::BadRequest(
            "batch object key cannot be empty".to_string(),
        ));
    }
    Ok(key.to_string())
}

fn batch_error_status(error: &AppError) -> StatusCode {
    error.status_code()
}

fn batch_error_message(error: &AppError) -> String {
    if error.is_capacity_error() {
        return "Insufficient storage capacity".to_string();
    }
    match error {
        AppError::Io(_, _) => "Object storage I/O failed".to_string(),
        AppError::InternalServerError(_) => "Internal object storage failure".to_string(),
        _ => error.to_string(),
    }
}

async fn upload_standard_batch_object(
    state: Arc<AppState>,
    claims: Claims,
    logical_path: String,
    content_type: String,
    bytes: bytes::Bytes,
    io_guard: Option<AdaptiveIoGuard>,
    query: crate::core::disk_manager::UploadPartQuery,
) -> StandardBatchUploadEntryResult {
    let size_bytes = bytes.len() as u64;
    let result_key = logical_path.clone();
    // Keep already-admitted small-object work alive through publication even
    // if the multipart request disconnects and drops its JoinSet.
    let operation = tokio::spawn(async move {
        let fragment = claims.fragment.clone();
        let _lifecycle = crate::core::fragment_lifecycle::acquire_live(&fragment).await?;
        let strict_durability = upload_uses_strict_durability(&query);
        let physical = resolve_logical_write_key(&fragment, &logical_path).await?;
        let _upload_activity =
            crate::core::upload::UploadActivityGuard::begin(&fragment, &physical)?;
        let io_progress = io_guard.as_ref().map(AdaptiveIoGuard::progress_handle);
        let payload_started = std::time::Instant::now();
        let payload_result = upload_object_one_shot_body_direct(
            claims,
            fragment.clone(),
            physical.clone(),
            query,
            Some(content_type),
            Body::from(bytes),
            size_bytes,
            false,
            io_progress,
        )
        .await;
        record_io_latency(
            "upload.batch.payload_ndx",
            payload_started.elapsed(),
            size_bytes,
            payload_result.is_ok(),
        );
        let mut metadata = payload_result?;
        // Namespace publication performs physical I/O and strict file/directory
        // syncs too. The detached operation owns admission through the complete
        // object commit, not just through payload and NDX publication.
        let namespace_started = std::time::Instant::now();
        let namespace_result = commit_logical_upload_from_meta_with_durability(
            &fragment,
            &logical_path,
            &physical,
            &metadata,
            strict_durability,
        )
        .await;
        record_io_latency(
            "upload.batch.ncx",
            namespace_started.elapsed(),
            0,
            namespace_result.is_ok(),
        );
        let committed_physical = namespace_result?;
        metadata.object_key = logical_path.clone();
        emit_logical_file_uploaded(
            &state,
            &fragment,
            &logical_path,
            &committed_physical,
            &metadata,
        );
        if let Some(guard) = io_guard {
            guard.complete();
        }
        Ok::<_, AppError>(metadata)
    })
    .await
    .unwrap_or_else(|e| {
        Err(AppError::InternalServerError(format!(
            "batch upload task: {e}"
        )))
    });

    match operation {
        Ok(metadata) => StandardBatchUploadEntryResult {
            object_key: result_key,
            status: StatusCode::CREATED.as_u16(),
            version_id: Some(metadata.version_id),
            size_bytes: Some(size_bytes),
            error: None,
        },
        Err(error) => {
            let status = batch_error_status(&error);
            StandardBatchUploadEntryResult {
                object_key: result_key,
                status: status.as_u16(),
                version_id: None,
                size_bytes: Some(size_bytes),
                error: Some(batch_error_message(&error)),
            }
        }
    }
}

async fn collect_standard_batch_result(
    workers: &mut JoinSet<IndexedStandardBatchUploadResult>,
    results: &mut Vec<IndexedStandardBatchUploadResult>,
) {
    if let Some(joined) = workers.join_next().await {
        match joined {
            Ok(result) => results.push(result),
            Err(error) => results.push(IndexedStandardBatchUploadResult {
                index: usize::MAX,
                result: StandardBatchUploadEntryResult {
                    object_key: "<batch-worker>".to_string(),
                    status: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                    version_id: None,
                    size_bytes: None,
                    error: Some(format!("Batch worker failed: {error}")),
                },
            }),
        }
    }
}

/// Streams small objects through bounded request-owned persistence groups.
/// Each multipart field is named `object`; its filename is the percent-encoded
/// logical object key. Objects commit independently; one failure never
/// publishes an incomplete object and does not cancel unrelated entries.
async fn upload_standard_batch_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Query(query): Query<StandardBatchUploadQuery>,
    multipart: Multipart,
) -> Result<Response, AppError> {
    let durability = crate::core::disk_manager::UploadPartQuery {
        checksum: None,
        durability: query.durability.clone(),
        strict_durability: query.strict_durability,
        io_gate: Some(true),
    };
    if upload_uses_strict_durability(&durability)
        && !crate::core::storage_pool::configured().is_empty()
    {
        return small_batch_request::upload(state, claims, query, multipart).await;
    }
    Ok(
        upload_standard_batch_legacy(State(state), claims, Query(query), multipart)
            .await?
            .into_response(),
    )
}

async fn upload_standard_batch_legacy(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Query(query): Query<StandardBatchUploadQuery>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }

    let concurrency = query
        .concurrency
        .unwrap_or_else(default_standard_batch_concurrency)
        .clamp(1, STANDARD_BATCH_MAX_CONCURRENCY);
    let mut workers = JoinSet::new();
    let mut results = Vec::<IndexedStandardBatchUploadResult>::new();
    let mut requested = 0usize;
    let mut admission_failure: Option<(StatusCode, String)> = None;
    let upload_query = crate::core::disk_manager::UploadPartQuery {
        checksum: None,
        durability: query.durability,
        strict_durability: query.strict_durability,
        // The parser admits buffering and payload/NDX work. Lifecycle and
        // activity protection continue through independent NCX publication.
        io_gate: Some(false),
    };

    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|error| AppError::Multipart(error.to_string()))?
    {
        let index = requested;
        if index >= STANDARD_BATCH_MAX_OBJECTS {
            return Err(AppError::BadRequest(format!(
                "Standard batch requests are limited to {STANDARD_BATCH_MAX_OBJECTS} objects"
            )));
        }
        requested = requested.saturating_add(1);
        let logical_path = match decode_standard_batch_object_key(field.name(), field.file_name()) {
            Ok(path) => path,
            Err(error) => {
                results.push(IndexedStandardBatchUploadResult {
                    index,
                    result: StandardBatchUploadEntryResult {
                        object_key: "<invalid>".to_string(),
                        status: batch_error_status(&error).as_u16(),
                        version_id: None,
                        size_bytes: None,
                        error: Some(batch_error_message(&error)),
                    },
                });
                continue;
            }
        };
        let content_type = field
            .content_type()
            .map(ToString::to_string)
            .unwrap_or_else(|| "application/octet-stream".to_string());
        if let Some((status, message)) = &admission_failure {
            results.push(IndexedStandardBatchUploadResult {
                index,
                result: StandardBatchUploadEntryResult {
                    object_key: logical_path,
                    status: status.as_u16(),
                    version_id: None,
                    size_bytes: None,
                    error: Some(message.clone()),
                },
            });
            continue;
        }
        while workers.len() >= concurrency {
            collect_standard_batch_result(&mut workers, &mut results).await;
        }
        // Admit before allocating a body. All batch clients share the existing
        // I/O limit; concurrency=128 must not buffer 128 waiting 1MiB objects.
        let context = format!(
            "standard-batch fragment={}, object={logical_path}",
            claims.fragment
        );
        let io_guard =
            match acquire_upload_io_guard_for_query(Some(true), &claims.fragment, &context).await {
                Ok(guard) => guard,
                Err(error) => {
                    let status = batch_error_status(&error);
                    let message = batch_error_message(&error);
                    results.push(IndexedStandardBatchUploadResult {
                        index,
                        result: StandardBatchUploadEntryResult {
                            object_key: logical_path,
                            status: status.as_u16(),
                            version_id: None,
                            size_bytes: None,
                            error: Some(message.clone()),
                        },
                    });
                    // Stop adding work after backpressure, but retain and join
                    // every admitted worker. The response reports committed
                    // versions plus explicit rejections instead of losing them
                    // behind a whole-request 503 halfway through the batch.
                    admission_failure = Some((status, message));
                    continue;
                }
            };
        let mut body = BytesMut::new();
        let mut too_large = false;
        while let Some(chunk) = field
            .chunk()
            .await
            .map_err(|error| AppError::Multipart(error.to_string()))?
        {
            if body.len().saturating_add(chunk.len()) > STANDARD_BATCH_OBJECT_MAX_BYTES {
                too_large = true;
                continue;
            }
            if !too_large {
                body.extend_from_slice(&chunk);
            }
        }
        if too_large {
            results.push(IndexedStandardBatchUploadResult {
                index,
                result: StandardBatchUploadEntryResult {
                    object_key: logical_path,
                    status: StatusCode::PAYLOAD_TOO_LARGE.as_u16(),
                    version_id: None,
                    size_bytes: None,
                    error: Some(format!(
                        "Standard batch objects are limited to {} bytes; use one-shot or multipart for larger files",
                        STANDARD_BATCH_OBJECT_MAX_BYTES
                    )),
                },
            });
            continue;
        }

        let task_state = Arc::clone(&state);
        let task_claims = claims.clone();
        let query = upload_query.clone();
        workers.spawn(async move {
            IndexedStandardBatchUploadResult {
                index,
                result: upload_standard_batch_object(
                    task_state,
                    task_claims,
                    logical_path,
                    content_type,
                    body.freeze(),
                    io_guard,
                    query,
                )
                .await,
            }
        });
    }

    while !workers.is_empty() {
        collect_standard_batch_result(&mut workers, &mut results).await;
    }
    if requested == 0 {
        return Err(AppError::BadRequest(
            "standard batch upload contained no object fields".to_string(),
        ));
    }

    results.sort_by_key(|entry| entry.index);
    let results = results
        .into_iter()
        .map(|entry| entry.result)
        .collect::<Vec<_>>();
    let succeeded = results
        .iter()
        .filter(|entry| entry.status == StatusCode::CREATED.as_u16())
        .count();
    let failed = results.len().saturating_sub(succeeded);
    let status = if failed == 0 {
        StatusCode::CREATED
    } else {
        StatusCode::MULTI_STATUS
    };
    Ok((
        status,
        AxumJson(StandardBatchUploadResponse {
            requested,
            succeeded,
            failed,
            results,
        }),
    ))
}

async fn get_upload_status_durable_scoped(
    claims: Claims,
    Path(version_id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    let response = durable_get_upload_status_impl(claims, Path((fragment.clone(), version_id)))
        .await?
        .into_response();
    let mut parsed: crate::core::disk_manager::UploadStatusResponse =
        parse_json_response(response, "upload status").await?;
    apply_logical_upload_binding_to_status(&fragment, &mut parsed).await?;
    Ok(AxumJson(parsed))
}

async fn list_active_uploads_durable_scoped(
    claims: Claims,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    let response = durable_list_active_uploads_impl(claims, Path(fragment.clone()), Query(params))
        .await?
        .into_response();
    let mut parsed: crate::core::upload::ListUploadsResponse =
        parse_json_response(response, "active uploads").await?;
    apply_logical_upload_bindings_to_sessions(&fragment, &mut parsed.sessions).await?;
    Ok(AxumJson(parsed))
}

async fn object_upload_part_durable_scoped(
    claims: Claims,
    headers: axum::http::HeaderMap,
    Path((version_id, part_index)): Path<(String, u64)>,
    Query(query): Query<crate::core::disk_manager::UploadPartQuery>,
    body: axum::body::Body,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    durable_object_upload_part_impl(
        claims,
        headers,
        Path((fragment, version_id, part_index)),
        Query(query),
        body,
    )
    .await
}

async fn finalize_object_upload_durable_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((version_id, session_id)): Path<(String, String)>,
    Query(key_query): Query<crate::core::disk_manager::ObjectKeyQuery>,
    AxumJson(params): AxumJson<crate::core::disk_manager::InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    let binding = global_namespace_runtime()
        .pending_upload(&fragment, &version_id)
        .await?;
    let physical = binding
        .as_ref()
        .map(|value| value.physical_object_id.clone())
        .unwrap_or_else(|| key_query.object_key.clone());
    let _upload_activity = crate::core::upload::UploadActivityGuard::begin(&fragment, &physical)?;
    let finalized = durable_finalize_object_upload_direct(
        claims,
        fragment.clone(),
        version_id.clone(),
        session_id,
        physical.clone(),
        params,
    )
    .await?;
    let mut parsed = match finalized {
        crate::core::upload::FinalizeUploadResult::Complete(metadata) => metadata,
        crate::core::upload::FinalizeUploadResult::Incomplete(incomplete) => {
            return Ok((StatusCode::CONFLICT, AxumJson(incomplete)).into_response());
        }
    };
    let logical_path = binding
        .as_ref()
        .map(|value| value.logical_path.clone())
        .unwrap_or(key_query.object_key);
    let committed_physical =
        commit_logical_upload_from_meta(&parsed.fragment, &logical_path, &physical, &parsed)
            .await?;
    index::remove_session(&parsed.fragment, &parsed.version_id, "finalized").await?;
    parsed.object_key = logical_path;
    emit_logical_file_uploaded(
        &state,
        &fragment,
        &parsed.object_key,
        &committed_physical,
        &parsed,
    );
    parsed.strip_internal_layout();
    Ok((StatusCode::OK, AxumJson(parsed)).into_response())
}

async fn cancel_upload_session_durable_scoped(
    claims: Claims,
    Path(version_id): Path<String>,
    Query(key_query): Query<crate::core::disk_manager::ObjectKeyQuery>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    let binding = global_namespace_runtime()
        .pending_upload(&fragment, &version_id)
        .await?;
    let physical = binding
        .as_ref()
        .map(|value| value.physical_object_id.clone())
        .unwrap_or(key_query.object_key);
    let response = durable_cancel_upload_session_impl(
        claims,
        Path((fragment.clone(), version_id.clone())),
        Query(crate::core::disk_manager::ObjectKeyQuery {
            object_key: physical,
            version_id: None,
            filename: None,
        }),
    )
    .await;
    if response.is_ok() {
        let _ = global_namespace_runtime()
            .clear_pending_upload(&fragment, &version_id)
            .await;
    }
    response
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
        .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_SIZE))
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

    let lifecycle_state = Arc::clone(&app_state);
    tokio::spawn(async move {
        run_lifecycle_manager(lifecycle_state).await;
    });

    let auto_healing_state = Arc::clone(&app_state);
    tokio::spawn(async move {
        run_auto_healing_controller(auto_healing_state).await;
    });

    tokio::spawn(async move {
        run_adaptive_concurrency_loop().await;
    });

    let object_storage_routes = {
        let read_ops = Router::new()
            .route("/noa/v1/capacity/usage", get(get_capacity_usage_scoped))
            .route("/noa/v1/capacity/fragment", get(get_fragment_usage_scoped))
            .route("/noa/v1/fragments", get(list_fragments))
            .route("/noa/v1/objects", get(list_objects_scoped))
            .route(
                "/noa/v1/namespace/snapshot",
                get(stream_namespace_snapshot_scoped),
            )
            .route("/noa/v1/namespace/search", get(proxy_namespace_search))
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
            .route(
                "/noa/v1/exports",
                post(create_export).layer(DefaultBodyLimit::max(MAX_EXPORT_REQUEST_BODY_SIZE)),
            )
            .route(
                "/noa/v1/exports/{:id}",
                get(export_status).delete(cancel_export),
            )
            .route("/noa/v1/exports/{:id}/download", get(download_export))
            .route(
                "/noa/v1/exports/{:id}/signed-url",
                post(mint_signed_export_url),
            )
            .merge(build_posix_read_routes());

        let read_ops = layers
            .layer_view(read_ops)
            .layer(DefaultBodyLimit::max(MAX_EXPORT_REQUEST_BODY_SIZE))
            .layer(from_fn(enforce_quiesced_mode_on_reads));

        let write_ops = Router::new()
            .route("/noa/v1/fragments/{:fragment}", delete(delete_fragment))
            .route("/noa/v1/fragments/{:fragment}", patch(update_fragment))
            .route(
                "/noa/v1/trash/empty",
                delete(removed_manual_reclamation_route).post(removed_manual_reclamation_route),
            )
            .route(
                "/noa/v1/posix/purge",
                post(removed_manual_reclamation_route),
            )
            .route(
                "/noa/v1/objects/permanent/bulk-delete",
                post(removed_manual_reclamation_route),
            )
            .route(
                "/noa/v1/objects/version/{:version_id}/{*object_key}",
                delete(removed_manual_reclamation_route),
            )
            .route(
                "/noa/v1/objects/permanent/{*object_key}",
                delete(removed_manual_reclamation_route),
            )
            .route("/noa/v1/objects/rename", post(rename_object_scoped))
            .route(
                "/noa/v1/objects/bulk-delete",
                post(bulk_delete_objects_scoped),
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
            .route("/noa/v1/trash/recover-all", post(recover_all_trash_scoped))
            .route(
                "/noa/v1/upload/multipart/init/{*object_key}",
                post(initiate_object_upload_scoped),
            )
            .route(
                "/noa/v1/upload/one-shot/{*object_key}",
                post(upload_object_one_shot_scoped)
                    .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_SIZE)),
            )
            .route(
                "/noa/v1/upload/batch",
                post(upload_standard_batch_scoped).layer(DefaultBodyLimit::max(MAX_HTTP_BODY_SIZE)),
            )
            .route(
                "/noa/v1/upload/multipart/{:version_id}",
                get(get_upload_status_scoped),
            )
            .route("/noa/v1/upload/multipart", get(list_active_uploads_scoped))
            .route(
                "/noa/v1/upload/multipart/{:version_id}/{:part_index}",
                put(object_upload_part_scoped).layer(DefaultBodyLimit::max(MAX_HTTP_BODY_SIZE)),
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
                put(object_upload_part_durable_scoped)
                    .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_SIZE)),
            )
            .route(
                "/noa/v1/upload/multipart/durable/{:version_id}/{:session_id}/finalize",
                post(finalize_object_upload_durable_scoped),
            )
            .route(
                "/noa/v1/upload/multipart/durable/{:version_id}",
                delete(cancel_upload_session_durable_scoped),
            )
            .merge(build_posix_write_routes())
            .layer(DefaultBodyLimit::max(MAX_EXPORT_REQUEST_BODY_SIZE));

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
                "/noa/v1/maintenance/objects/redistribution/start/{*object_key}",
                post(trigger_object_redistribution_job),
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
            .route(
                "/noa/v1/maintenance/pools/health",
                get(crate::core::storage_health::get_pool_health),
            )
            .route("/noa/v1/maintenance/health", get(get_system_health));

        let maintenance_ops = layers
            .layer_global(maintenance_ops)
            .layer(DefaultBodyLimit::max(MAX_EXPORT_REQUEST_BODY_SIZE));

        let write_ops = layers
            .layer_upload(write_ops)
            .layer(from_fn(guard_upload_lifecycle))
            .layer(from_fn(enforce_read_only_fast_mode_on_mutations));

        let protected_routes = Router::new()
            .merge(read_ops)
            .merge(write_ops)
            .merge(maintenance_ops)
            .layer(from_fn(validate_access));

        Router::new().merge(protected_routes)
    };

    let ws_routes = { Router::new().route("/noa/v1/ws", get(ws_handler)) };

    let global_routes = Router::new()
        .route(
            "/noa/v1/maintenance/backups/operation",
            post(crate::core::backup::network::live_operation),
        )
        .route("/noa/v1/fragments/check", get(verify_fragment_existence))
        .route("/noa/v1/fragments/hash", get(get_fragment_hash))
        .route(
            "/noa/v1/signed-exports/download",
            get(download_signed_export),
        )
        .route(
            "/noa/v1/objects/signed-read",
            get(read_object_via_signed_url),
        );
    let global_routes = layers
        .layer_global(global_routes)
        .layer(DefaultBodyLimit::max(64 * 1024));

    let create_fragment_route = Router::new().route("/noa/v1/fragments", post(create_fragment));
    let create_fragment_route = layers
        .layer_auth(create_fragment_route)
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(from_fn(enforce_read_only_fast_mode_on_mutations));

    let fragment_auth_route =
        Router::new().route("/noa/v1/auth/fragment", post(authorize_fragment));
    let fragment_auth_route = layers
        .layer_auth(fragment_auth_route)
        .layer(DefaultBodyLimit::max(64 * 1024));

    let refresh_auth_route =
        Router::new().route("/noa/v1/auth/refresh", post(refresh_access_token));
    let refresh_auth_route = layers
        .layer_auth(refresh_auth_route)
        .layer(DefaultBodyLimit::max(64 * 1024));

    let logout_auth_route = Router::new().route("/noa/v1/auth/logout", post(logout_refresh_token));
    let logout_auth_route = layers
        .layer_auth(logout_auth_route)
        .layer(DefaultBodyLimit::max(64 * 1024));

    let ping_route = Router::new().route("/noa/v1/ping", get(ping));
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

async fn removed_manual_reclamation_route() -> impl IntoResponse {
    (
        StatusCode::GONE,
        "N1 3.0 removed manual trash purge and version deletion; configured TTL lifecycle owns reclamation",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    pub(super) fn upload_test_fixture() -> (String, Claims, Arc<AppState>) {
        use crate::core::disk_manager::{BLOB_STORE_DISKS, FS_STORAGE_ROOT, INDEX_STORE_DISKS};
        use crate::core::upload::{
            DATA_SHARDS_ATOMIC, MAX_PART_SIZE_ATOMIC, PARITY_SHARDS_ATOMIC, STRIPE_SIZE_ATOMIC,
            WRITER_BUFFER_SIZE_ATOMIC,
        };
        use std::sync::atomic::{AtomicU64, AtomicUsize};
        let base = std::env::temp_dir().join(format!("n1-batch-admission-{}", ulid::Ulid::new()));
        for cell in [&INDEX_STORE_DISKS, &BLOB_STORE_DISKS] {
            for root in cell.get_or_init(|| {
                (0..2)
                    .map(|i| base.join(format!("member-{i}")).display().to_string())
                    .collect()
            }) {
                std::fs::create_dir_all(root).unwrap();
            }
        }
        let control = FS_STORAGE_ROOT.get_or_init(|| base.join("control").display().to_string());
        let roots = BLOB_STORE_DISKS
            .get()
            .unwrap()
            .iter()
            .map(std::path::PathBuf::from)
            .collect::<Vec<_>>();
        crate::core::root_identity::reconcile_roots_for_test(
            &std::path::Path::new(control).join(".n1/topology-roots.json"),
            &roots,
        )
        .unwrap();
        MAX_PART_SIZE_ATOMIC.get_or_init(|| AtomicU64::new(1024 * 1024));
        DATA_SHARDS_ATOMIC.get_or_init(|| AtomicUsize::new(1));
        PARITY_SHARDS_ATOMIC.get_or_init(|| AtomicUsize::new(0));
        WRITER_BUFFER_SIZE_ATOMIC.get_or_init(|| AtomicUsize::new(4096));
        STRIPE_SIZE_ATOMIC.get_or_init(|| AtomicUsize::new(4096));
        let fragment = ulid::Ulid::new().to_string();
        for root in INDEX_STORE_DISKS.get().unwrap() {
            let root = crate::core::fragment_identity::physical_fragment_root_on(
                std::path::Path::new(root),
                &fragment,
            );
            std::fs::create_dir_all(&root).unwrap();
            std::fs::write(root.join(".fragment.ndx"), b"test-owned live fragment").unwrap();
        }
        let claims = Claims {
            fragment: fragment.clone(),
            fragment_alias: None,
            operator: true,
            read: true,
            write: true,
            modify: true,
            delete: true,
            exp: usize::MAX,
        };
        let state = Arc::new(AppState {
            ws_manager: WsConnectionManager::new(),
        });
        (fragment, claims, state)
    }

    pub(super) fn remove_upload_test_fixture(fragment: &str) {
        use crate::core::disk_manager::{BLOB_STORE_DISKS, INDEX_STORE_DISKS};
        for root in INDEX_STORE_DISKS
            .get()
            .unwrap()
            .iter()
            .chain(BLOB_STORE_DISKS.get().unwrap())
        {
            let path = crate::core::fragment_identity::physical_fragment_root_on(
                std::path::Path::new(root),
                fragment,
            );
            if path.exists() {
                std::fs::remove_dir_all(path).unwrap();
            }
        }
    }

    #[tokio::test]
    async fn one_shot_publishes_typed_metadata_without_upload_workspace_or_public_layout() {
        use crate::core::disk_manager::{BLOB_STORE_DISKS, INDEX_STORE_DISKS, ObjectMetadata};
        let _test_guard = crate::test_support::global_test_lock().lock().await;
        let (fragment, claims, state) = upload_test_fixture();
        let payload = b"straight through strict object";
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("content-length", payload.len().to_string().parse().unwrap());
        headers.insert("content-type", "application/octet-stream".parse().unwrap());
        let query = crate::core::disk_manager::UploadPartQuery {
            checksum: Some(format!("{:08x}", crc32fast::hash(payload))),
            durability: None,
            strict_durability: Some(true),
            io_gate: Some(true),
        };
        let response = upload_object_one_shot_scoped(
            State(state),
            claims,
            headers,
            Path("direct.bin".into()),
            Query(query),
            Body::from(payload.as_slice()),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(response.status(), StatusCode::CREATED);
        let public: ObjectMetadata = parse_json_response(response, "one-shot test")
            .await
            .unwrap();
        assert_eq!(public.object_key, "direct.bin");
        assert_eq!(public.total_size_bytes, payload.len() as u64);
        assert!(
            public.parts.is_empty(),
            "public response must not expose placement"
        );
        let pointer = crate::namespace::flat::lookup(&fragment, "direct.bin")
            .unwrap()
            .unwrap();
        let committed =
            index::read_object_version(&fragment, &pointer.object_id, &public.version_id)
                .await
                .unwrap();
        assert_eq!(committed.parts.len(), 1);
        assert!(!committed.parts[0].shards.is_empty());
        for shard in &committed.parts[0].shards {
            let path = crate::core::storage_pool::resolve_address(&shard.absolute_path).unwrap();
            let bytes = std::fs::read(path).unwrap();
            assert_eq!(format!("{:08x}", crc32fast::hash(&bytes)), shard.checksum);
        }
        for root in INDEX_STORE_DISKS
            .get()
            .unwrap()
            .iter()
            .chain(BLOB_STORE_DISKS.get().unwrap())
        {
            let path = crate::core::fragment_identity::physical_fragment_root_on(
                std::path::Path::new(root),
                &fragment,
            );
            assert!(
                !path.join(".uploads").exists(),
                "one-shot must not create resumable state"
            );
        }
        remove_upload_test_fixture(&fragment);
    }

    #[tokio::test]
    async fn one_shot_rejections_never_publish_namespace_or_leave_payload_files() {
        use crate::core::disk_manager::BLOB_STORE_DISKS;
        let _test_guard = crate::test_support::global_test_lock().lock().await;
        let (fragment, claims, state) = upload_test_fixture();
        for (name, length, checksum, payload, expected) in [
            (
                "missing",
                None,
                None,
                b"x".as_slice(),
                StatusCode::BAD_REQUEST,
            ),
            (
                "invalid",
                Some("bad"),
                None,
                b"x".as_slice(),
                StatusCode::BAD_REQUEST,
            ),
            (
                "short",
                Some("2"),
                None,
                b"x".as_slice(),
                StatusCode::BAD_REQUEST,
            ),
            (
                "excess",
                Some("1"),
                None,
                b"xy".as_slice(),
                StatusCode::BAD_REQUEST,
            ),
            (
                "checksum",
                Some("1"),
                Some("00000000"),
                b"x".as_slice(),
                StatusCode::BAD_REQUEST,
            ),
            (
                "limit",
                Some("1048577"),
                None,
                b"".as_slice(),
                StatusCode::PAYLOAD_TOO_LARGE,
            ),
        ] {
            let mut headers = axum::http::HeaderMap::new();
            if let Some(length) = length {
                headers.insert("content-length", length.parse().unwrap());
            }
            let query = crate::core::disk_manager::UploadPartQuery {
                checksum: checksum.map(str::to_owned),
                durability: None,
                strict_durability: Some(true),
                io_gate: Some(true),
            };
            let result = upload_object_one_shot_scoped(
                State(state.clone()),
                claims.clone(),
                headers,
                Path(name.into()),
                Query(query),
                Body::from(payload),
            )
            .await;
            let response = match result {
                Ok(response) => response.into_response(),
                Err(error) => error.into_response(),
            };
            assert_eq!(response.status(), expected, "case {name}");
            assert!(
                crate::namespace::flat::lookup(&fragment, name)
                    .unwrap()
                    .is_none()
            );
        }
        for root in BLOB_STORE_DISKS.get().unwrap() {
            let path = crate::core::fragment_identity::physical_fragment_root_on(
                std::path::Path::new(root),
                &fragment,
            );
            for entry in walkdir::WalkDir::new(path.join(".objects")) {
                match entry {
                    Ok(entry) => assert!(
                        !entry.file_type().is_file(),
                        "leaked rejected payload: {}",
                        entry.path().display()
                    ),
                    Err(error)
                        if error
                            .io_error()
                            .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) => {}
                    Err(error) => panic!("failed to inspect rejection cleanup: {error}"),
                }
            }
            assert!(!path.join(".uploads").exists());
        }
        remove_upload_test_fixture(&fragment);
    }

    #[tokio::test]
    async fn batch_commit_keeps_disk_admission_until_ncx_after_caller_cancellation() {
        use crate::core::disk_manager::BLOB_STORE_DISKS;
        use crate::core::io_scheduler::disk_io_scheduler_snapshot;
        use std::time::Duration;
        let _test_guard = crate::test_support::global_test_lock().lock().await;
        let (fragment, claims, state) = upload_test_fixture();
        let key = "item.bin";
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let blocked_fragment = fragment.clone();
        let blocker = tokio::spawn(async move {
            crate::namespace::runtime::hold_publication_for_test(
                &blocked_fragment,
                key,
                ready_tx,
                release_rx,
            )
            .await;
        });
        ready_rx.await.unwrap();
        let context = format!("batch-commit-test-{fragment}");
        let guard = acquire_upload_io_guard_for_query(Some(true), &fragment, &context)
            .await
            .unwrap();
        let query = crate::core::disk_manager::UploadPartQuery {
            checksum: None,
            durability: None,
            strict_durability: Some(true),
            io_gate: Some(false),
        };
        let mut caller = tokio::spawn(upload_standard_batch_object(
            state,
            claims,
            key.into(),
            "application/octet-stream".into(),
            bytes::Bytes::from_static(b"strict payload"),
            guard,
            query,
        ));
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let has_ndx = BLOB_STORE_DISKS.get().unwrap().iter().any(|root| {
                    let fragment_root = crate::core::fragment_identity::physical_fragment_root_on(
                        std::path::Path::new(root),
                        &fragment,
                    );
                    walkdir::WalkDir::new(fragment_root.join(".objects"))
                        .into_iter()
                        .filter_map(Result::ok)
                        .any(|entry| {
                            entry.file_type().is_file()
                                && entry.file_name().to_string_lossy().ends_with(".ndx")
                        })
                });
                if has_ndx {
                    break;
                }
                if caller.is_finished() {
                    panic!(
                        "batch ended before publication gate: {:?}",
                        (&mut caller).await.unwrap()
                    );
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("strict payload/NDX must reach the blocked namespace publisher");
        assert!(!caller.is_finished());
        assert!(
            disk_io_scheduler_snapshot()
                .active_operations
                .iter()
                .any(|op| op.context == context),
            "namespace publication must still own foreground admission"
        );
        caller.abort();
        let _ = caller.await;
        assert!(
            disk_io_scheduler_snapshot()
                .active_operations
                .iter()
                .any(|op| op.context == context),
            "cancelling the caller must not release the detached commit's permit"
        );
        release_tx.send(()).unwrap();
        blocker.await.unwrap();
        tokio::time::timeout(Duration::from_secs(30), async {
            while disk_io_scheduler_snapshot()
                .active_operations
                .iter()
                .any(|op| op.context == context)
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("completed publication must release admission");
        assert!(
            crate::namespace::flat::lookup(&fragment, key)
                .unwrap()
                .is_some()
        );
        remove_upload_test_fixture(&fragment);
    }

    #[test]
    fn independent_upload_initiation_is_explicit_and_defaults_to_reuse() {
        let normal: UploadInitiationQuery = serde_json::from_str("{}").unwrap();
        let independent: UploadInitiationQuery = serde_json::from_str(r#"{"fresh":true}"#).unwrap();
        assert!(!normal.fresh);
        assert!(independent.fresh);
        assert!(serde_json::from_str::<UploadInitiationQuery>(r#"{"fresh":"yes"}"#).is_err());
    }

    #[tokio::test]
    async fn restricted_http_tokens_cannot_mutate_objects_or_start_global_jobs() {
        let _guard = crate::test_support::global_test_lock().lock().await;
        crate::middleware::auth_middleware::tests::ensure_test_env();
        let claims = Claims {
            operator: false,
            fragment: "permission-regression".into(),
            fragment_alias: None,
            read: true,
            write: false,
            modify: false,
            delete: false,
            exp: (chrono::Utc::now().timestamp() + 120) as usize,
        };
        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &claims,
            &crate::middleware::auth_middleware::JWT_KEYS.encoding,
        )
        .unwrap();
        let (router, state) = build_storage_routes_unbound();
        let router = router.with_state(state);
        for (method, uri, json) in [
            (Method::DELETE, "/noa/v1/objects/no-write", "{}"),
            (Method::POST, "/noa/v1/objects/recover/no-write", "{}"),
            (
                Method::POST,
                "/noa/v1/objects/bulk-delete",
                "{\"object_keys\":[\"no-write\"]}",
            ),
            (
                Method::POST,
                "/noa/v1/objects/bulk-recover",
                "{\"object_keys\":[\"no-write\"]}",
            ),
            (Method::POST, "/noa/v1/trash/recover-all", "{}"),
            (
                Method::POST,
                "/noa/v1/posix/trash",
                "{\"path\":\"no-write\"}",
            ),
            (
                Method::POST,
                "/noa/v1/posix/recover",
                "{\"path\":\"no-write\"}",
            ),
            (Method::POST, "/noa/v1/maintenance/gc/start", "{}"),
            (Method::POST, "/noa/v1/maintenance/repair/start", "{}"),
            (
                Method::POST,
                "/noa/v1/maintenance/redistribution/start",
                "{}",
            ),
        ] {
            let response = router
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .method(method)
                        .uri(uri)
                        .header("authorization", format!("Bearer {token}"))
                        .header("content-type", "application/json")
                        .extension(axum::extract::ConnectInfo(std::net::SocketAddr::from((
                            [127, 0, 0, 9],
                            32001,
                        ))))
                        .body(Body::from(json))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN, "{uri}");
        }
    }

    #[tokio::test]
    async fn public_auth_body_is_bounded_independently_of_streaming_upload_limit() {
        let (router, state) = build_storage_routes_unbound_with_options(StorageRouteOptions {
            include_object_storage: false,
        });
        let router = apply_runtime_http_layers(router).with_state(state);
        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/noa/v1/auth/fragment")
                    .header("content-type", "application/json")
                    .extension(axum::extract::ConnectInfo(std::net::SocketAddr::from((
                        [127, 0, 0, 10],
                        32001,
                    ))))
                    .body(Body::from(vec![b'x'; 65 * 1024]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn protected_json_control_is_bounded_before_deserialization() {
        let _guard = crate::test_support::global_test_lock().lock().await;
        crate::middleware::auth_middleware::tests::ensure_test_env();
        let fragment = format!("body-limit-test-{}", ulid::Ulid::new());
        crate::middleware::auth_middleware::tests::ensure_auth_test_fragment(&fragment).await;
        let claims = Claims {
            operator: false,
            fragment,
            fragment_alias: None,
            read: true,
            write: true,
            modify: true,
            delete: true,
            exp: (chrono::Utc::now().timestamp() + 120) as usize,
        };
        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &claims,
            &crate::middleware::auth_middleware::JWT_KEYS.encoding,
        )
        .unwrap();
        let (router, state) = build_storage_routes_unbound();
        let router = apply_runtime_http_layers(router).with_state(state);
        for uri in [
            "/noa/v1/posix/rename",
            "/noa/v1/objects/bulk-delete",
            "/noa/v1/upload/multipart/init/key",
        ] {
            let response = router
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .method(Method::POST)
                        .uri(uri)
                        .header("authorization", format!("Bearer {token}"))
                        .header("content-type", "application/json")
                        .extension(axum::extract::ConnectInfo(std::net::SocketAddr::from((
                            [127, 0, 0, 11],
                            32001,
                        ))))
                        .body(Body::from(vec![b'x'; MAX_EXPORT_REQUEST_BODY_SIZE + 1]))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE, "{uri}");
        }
    }

    #[test]
    fn namespace_snapshot_completion_frame_is_explicit_ndjson() {
        let encoded = serde_json::to_string(&NamespaceSnapshotFrame::Complete { entry_count: 7 })
            .expect("completion JSON");
        let frame: serde_json::Value = serde_json::from_str(&encoded).expect("completion JSON");
        assert_eq!(frame["kind"], "complete");
        assert_eq!(frame["entry_count"], 7);
    }

    #[tokio::test]
    async fn namespace_snapshot_requires_read_authority() {
        let result = stream_namespace_snapshot_scoped(Claims {
            operator: false,
            fragment: "@f_snapshot-test-forbidden".into(),
            fragment_alias: None,
            read: false,
            write: true,
            modify: true,
            delete: true,
            exp: usize::MAX,
        })
        .await;
        assert!(matches!(result, Err(AppError::Forbidden(_))));
    }

    #[test]
    fn standard_batch_object_keys_are_explicit_and_percent_decoded() {
        assert_eq!(
            decode_standard_batch_object_key(
                Some("object"),
                Some("records%2F2026%2Fscan%2001.dcm")
            )
            .expect("decode batch object key"),
            "records/2026/scan 01.dcm"
        );
        assert!(decode_standard_batch_object_key(Some("files"), Some("a.bin")).is_err());
        assert!(decode_standard_batch_object_key(Some("object"), Some("%2F")).is_err());
        assert!(decode_standard_batch_object_key(Some("object"), None).is_err());
    }

    #[test]
    fn standard_batch_concurrency_is_bounded_independently_of_topology_size() {
        assert!(
            (1..=STANDARD_BATCH_MAX_CONCURRENCY).contains(&default_standard_batch_concurrency())
        );
        assert_eq!(0usize.clamp(1, STANDARD_BATCH_MAX_CONCURRENCY), 1);
        assert_eq!(
            usize::MAX.clamp(1, STANDARD_BATCH_MAX_CONCURRENCY),
            STANDARD_BATCH_MAX_CONCURRENCY
        );
    }

    #[test]
    fn standard_batch_reports_kernel_capacity_failure_as_507() {
        let error = AppError::Io(
            std::io::Error::from_raw_os_error(libc::ENOSPC),
            "write batch shard".to_string(),
        );
        assert_eq!(batch_error_status(&error), StatusCode::INSUFFICIENT_STORAGE);
        assert_eq!(batch_error_message(&error), "Insufficient storage capacity");
    }

    #[test]
    fn object_redistribution_route_places_catch_all_last() {
        let _: Router<Arc<AppState>> = Router::new().route(
            "/noa/v1/maintenance/objects/redistribution/start/{*object_key}",
            post(trigger_object_redistribution_job),
        );
    }

    #[tokio::test]
    async fn noa_v1_is_the_canonical_http_namespace() {
        let (router, state) = build_storage_routes_unbound_with_options(StorageRouteOptions {
            include_object_storage: false,
        });
        let router = router.with_state(state);

        let noa_response = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::GET)
                    .uri("/noa/v1/ping")
                    .extension(axum::extract::ConnectInfo(std::net::SocketAddr::from((
                        [127, 0, 0, 1],
                        30_001,
                    ))))
                    .body(axum::body::Body::empty())
                    .expect("NOA ping request"),
            )
            .await
            .expect("NOA ping response");
        assert_eq!(noa_response.status(), StatusCode::OK);

        let legacy_response = router
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::GET)
                    .uri("/n1/ping")
                    .extension(axum::extract::ConnectInfo(std::net::SocketAddr::from((
                        [127, 0, 0, 1],
                        30_002,
                    ))))
                    .body(axum::body::Body::empty())
                    .expect("legacy ping request"),
            )
            .await
            .expect("legacy ping response");
        assert_eq!(legacy_response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn noa_v1_removed_destructive_routes_fail_closed() {
        let (router, state) = build_storage_routes_unbound();
        let router = router.with_state(state);
        for (method, uri) in [
            (Method::DELETE, "/noa/v1/trash/empty"),
            (Method::POST, "/noa/v1/trash/empty"),
            (Method::POST, "/noa/v1/posix/purge"),
            (Method::DELETE, "/noa/v1/objects/permanent/clinical/a.bin"),
            (Method::DELETE, "/noa/v1/objects/version/v1/clinical/a.bin"),
            (Method::POST, "/noa/v1/objects/permanent/bulk-delete"),
        ] {
            let response = router
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .method(method)
                        .uri(uri)
                        .body(axum::body::Body::empty())
                        .expect("removed route request"),
                )
                .await
                .expect("removed route response");
            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "authentication must remain ahead of retired route {uri}"
            );
        }

        let authenticated_handler_response =
            removed_manual_reclamation_route().await.into_response();
        assert_eq!(authenticated_handler_response.status(), StatusCode::GONE);
    }
}
