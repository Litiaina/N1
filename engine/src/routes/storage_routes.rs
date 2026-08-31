use crate::core::adaptive_concurrency::run_adaptive_concurrency_loop;
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
use crate::core::fragment_manager::{
    create_fragment, delete_fragment, get_fragment_hash, update_fragment, verify_fragment_existence,
};
use crate::core::fragment_metadata::resolve_stable_fragment_id;
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
    cancel_standard_upload_direct, cancel_upload_session as durable_cancel_upload_session_impl,
    complete_standard_upload,
    finalize_object_upload_direct as durable_finalize_object_upload_direct,
    finalize_standard_upload_direct, find_reusable_standard_upload,
    get_standard_upload_status_direct, get_upload_status as durable_get_upload_status_impl,
    initiate_object_upload as durable_initiate_object_upload_impl, initiate_standard_upload_direct,
    list_active_uploads as durable_list_active_uploads_impl, list_standard_uploads_direct,
    object_upload_part as durable_object_upload_part_impl, object_upload_part_standard_body_direct,
    upload_object_one_shot as upload_object_one_shot_impl, upload_object_one_shot_body_direct,
};
use crate::core::{index, index_store};
use crate::enums::app_error_enum::AppError;
use crate::middleware::auth_middleware::{
    authorize_fragment, logout_refresh_token, refresh_access_token, validate_access,
};
use crate::middleware::rate_limiter_middleware::RateLimitLayers;
use crate::namespace::runtime::{global_namespace_runtime, logical_not_found};
use crate::posix::routes::{build_posix_read_routes, build_posix_write_routes};
use crate::structs::auth_structs::Claims;
use crate::url::signed_read::{
    SignedReadUrlRequest, mint_signed_read_url, read_object_via_signed_url,
};
use crate::websocket::handler::ws_handler;
use crate::websocket::manager::{WsConnectionManager, register_global_ws_manager};
use crate::websocket::ws_structs::{NamespaceChangeEvent, StorageEventPayload};
use axum::body::{Body, to_bytes};
use axum::http::{HeaderName, Method, StatusCode};
use axum::middleware::from_fn;
use axum::response::IntoResponse;
use axum::routing::{delete, get, patch, post, put};
use axum::{
    Json as AxumJson, Router,
    extract::{DefaultBodyLimit, Multipart, Path, Query, State},
};
use bytes::BytesMut;
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tower_http::cors::{Any, CorsLayer};

const MAX_HTTP_BODY_SIZE: usize = 5 * 1024 * 1024 * 1024;

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
    State(state): State<Arc<AppState>>,
    Path(fragment): Path<String>,
    Query(mut params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let fragment = match resolve_stable_fragment_id(&fragment).await {
        Ok(stable) => stable,
        Err(err) => return err.into_response(),
    };
    params.insert("fragment".to_string(), fragment);
    run_garbage_collector(State(state), Query(params))
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
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
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
    if !claims.write {
        return Err(AppError::Forbidden(
            "Write permission required to recover".into(),
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
    if !claims.write {
        return Err(AppError::Forbidden(
            "Write permission required to recover all".into(),
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

async fn initiate_object_upload_scoped(
    claims: Claims,
    Path(object_key): Path<String>,
    AxumJson(params): AxumJson<crate::core::disk_manager::InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    if let Some(existing) = find_reusable_standard_upload(&fragment, &object_key, &params)? {
        return Ok((StatusCode::CREATED, AxumJson(existing)).into_response());
    }
    let physical = resolve_logical_write_key(&fragment, &object_key).await?;
    let response =
        initiate_standard_upload_direct(claims, fragment, physical, object_key, params).await?;
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
    match finalize_standard_upload_direct(claims, fragment.clone(), version_id.clone(), session_id)
        .await?
    {
        crate::core::upload::FinalizeUploadResult::Incomplete(incomplete) => {
            Ok((StatusCode::CONFLICT, AxumJson(incomplete)).into_response())
        }
        crate::core::upload::FinalizeUploadResult::Complete(mut metadata) => {
            let logical_path = key_query.object_key;
            let physical = metadata.object_key.clone();
            let committed_physical =
                commit_logical_upload_from_meta(&fragment, &logical_path, &physical, &metadata)
                    .await?;
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
            Ok((StatusCode::OK, AxumJson(metadata)).into_response())
        }
    }
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
    AxumJson(params): AxumJson<crate::core::disk_manager::InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    if let Some(existing) = find_reusable_pending_upload(&fragment, &object_key, &params).await? {
        return Ok((StatusCode::CREATED, AxumJson(existing)).into_response());
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
    let physical = resolve_logical_write_key(&fragment, &object_key).await?;
    let response = upload_object_one_shot_impl(
        State(state.clone()),
        claims,
        headers,
        Path((fragment.clone(), physical.clone())),
        Query(query),
        body,
    )
    .await?
    .into_response();
    if response.status() != StatusCode::CREATED {
        return Ok(response);
    }
    let public_meta: crate::core::disk_manager::ObjectMetadata =
        parse_json_response(response, "one-shot upload").await?;
    let mut parsed =
        index::read_object_version(&fragment, &physical, &public_meta.version_id).await?;
    let committed_physical =
        commit_logical_upload_from_meta(&fragment, &object_key, &physical, &parsed).await?;
    parsed.object_key = object_key;
    emit_logical_file_uploaded(
        &state,
        &fragment,
        &parsed.object_key,
        &committed_physical,
        &parsed,
    );
    parsed.strip_internal_layout();
    Ok((StatusCode::CREATED, AxumJson(parsed)).into_response())
}

const STANDARD_BATCH_OBJECT_MAX_BYTES: usize = 1024 * 1024;
const STANDARD_BATCH_MAX_CONCURRENCY: usize = 128;

#[derive(Debug, Deserialize)]
struct StandardBatchUploadQuery {
    #[serde(default)]
    concurrency: Option<usize>,
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
    std::thread::available_parallelism()
        .map(|cores| cores.get().saturating_mul(4))
        .unwrap_or(16)
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
) -> StandardBatchUploadEntryResult {
    let size_bytes = bytes.len() as u64;
    let operation = async {
        let fragment = claims.fragment.clone();
        let physical = resolve_logical_write_key(&fragment, &logical_path).await?;
        let query = crate::core::disk_manager::UploadPartQuery {
            checksum: None,
            durability: Some("batched".to_string()),
            strict_durability: Some(false),
            io_gate: Some(false),
        };
        let mut metadata = upload_object_one_shot_body_direct(
            claims,
            fragment.clone(),
            physical.clone(),
            query,
            Some(content_type),
            Body::from(bytes),
            size_bytes,
            false,
        )
        .await?;
        let committed_physical =
            commit_logical_upload_from_meta(&fragment, &logical_path, &physical, &metadata).await?;
        metadata.object_key = logical_path.clone();
        emit_logical_file_uploaded(
            &state,
            &fragment,
            &logical_path,
            &committed_physical,
            &metadata,
        );
        Ok::<_, AppError>(metadata)
    }
    .await;

    match operation {
        Ok(metadata) => StandardBatchUploadEntryResult {
            object_key: logical_path,
            status: StatusCode::CREATED.as_u16(),
            version_id: Some(metadata.version_id),
            size_bytes: Some(size_bytes),
            error: None,
        },
        Err(error) => {
            let status = batch_error_status(&error);
            StandardBatchUploadEntryResult {
                object_key: logical_path,
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

/// Streams a bounded set of small objects through the standard one-shot path.
/// Each multipart field is named `object`; its filename is the percent-encoded
/// logical object key. Objects commit independently; one failure never
/// publishes an incomplete object and does not cancel unrelated entries.
async fn upload_standard_batch_scoped(
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

    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|error| AppError::Multipart(error.to_string()))?
    {
        let index = requested;
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

        while workers.len() >= concurrency {
            collect_standard_batch_result(&mut workers, &mut results).await;
        }
        let task_state = Arc::clone(&state);
        let task_claims = claims.clone();
        workers.spawn(async move {
            IndexedStandardBatchUploadResult {
                index,
                result: upload_standard_batch_object(
                    task_state,
                    task_claims,
                    logical_path,
                    content_type,
                    body.freeze(),
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
    global_namespace_runtime()
        .clear_pending_upload(&parsed.fragment, &parsed.version_id)
        .await?;
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

    let monitoring_state = Arc::clone(&app_state);
    tokio::spawn(async move {
        run_system_monitor(monitoring_state).await;
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
            .merge(build_posix_read_routes());

        let read_ops = layers
            .layer_view(read_ops)
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
            .route("/noa/v1/maintenance/health", get(get_system_health));

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

    let global_routes = Router::new()
        .route("/noa/v1/fragments/check", get(verify_fragment_existence))
        .route("/noa/v1/fragments/hash", get(get_fragment_hash))
        .route(
            "/noa/v1/objects/signed-read",
            get(read_object_via_signed_url),
        );
    let global_routes = layers.layer_global(global_routes);

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
