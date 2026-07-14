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
    cancel_upload_session as cancel_upload_session_impl,
    finalize_object_upload as finalize_object_upload_impl,
    get_upload_status as get_upload_status_impl,
    initiate_object_upload as initiate_object_upload_impl,
    list_active_uploads as list_active_uploads_impl, object_upload_part as object_upload_part_impl,
    upload_object_one_shot as upload_object_one_shot_impl,
};
use crate::core::{index, index_store};
use crate::enums::app_error_enum::AppError;
use crate::fs::fs_constants::MAX_FILE_SIZE;
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
use axum::body::to_bytes;
use axum::http::{HeaderName, Method, StatusCode};
use axum::middleware::from_fn;
use axum::response::IntoResponse;
use axum::routing::{delete, get, patch, post, put};
use axum::{
    Json as AxumJson, Router,
    extract::{DefaultBodyLimit, Path, Query, State},
};
use serde::de::DeserializeOwned;
use std::collections::{BTreeSet, HashMap};
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
    if let Some(existing) = find_reusable_pending_upload(&fragment, &object_key, &params).await? {
        return Ok((StatusCode::CREATED, AxumJson(existing)).into_response());
    }
    let physical = resolve_logical_write_key(&fragment, &object_key).await?;
    let response = initiate_object_upload_impl(
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

async fn get_upload_status_scoped(
    claims: Claims,
    Path(version_id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    let response = get_upload_status_impl(claims, Path((fragment.clone(), version_id)))
        .await?
        .into_response();
    let mut parsed: crate::core::disk_manager::UploadStatusResponse =
        parse_json_response(response, "upload status").await?;
    apply_logical_upload_binding_to_status(&fragment, &mut parsed).await?;
    Ok(AxumJson(parsed))
}

async fn list_active_uploads_scoped(
    claims: Claims,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    let response = list_active_uploads_impl(claims, Path(fragment.clone()), Query(params))
        .await?
        .into_response();
    let mut parsed: crate::core::upload::ListUploadsResponse =
        parse_json_response(response, "active uploads").await?;
    apply_logical_upload_bindings_to_sessions(&fragment, &mut parsed.sessions).await?;
    Ok(AxumJson(parsed))
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
    let binding = global_namespace_runtime()
        .pending_upload(&fragment, &version_id)
        .await?;
    let physical = binding
        .as_ref()
        .map(|value| value.physical_object_id.clone())
        .unwrap_or_else(|| key_query.object_key.clone());
    let response = finalize_object_upload_impl(
        State(state.clone()),
        claims,
        Path((fragment.clone(), version_id.clone(), session_id)),
        Query(crate::core::disk_manager::ObjectKeyQuery {
            object_key: physical.clone(),
            version_id: key_query.version_id,
            filename: None,
        }),
        AxumJson(params),
    )
    .await?
    .into_response();
    if response.status() != StatusCode::OK {
        return Ok(response);
    }
    let logical_path = binding
        .as_ref()
        .map(|value| value.logical_path.clone())
        .unwrap_or(key_query.object_key);
    let mut parsed = index::read_object_version(&fragment, &physical, &version_id).await?;
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

async fn cancel_upload_session_scoped(
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
    let response = cancel_upload_session_impl(
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
            .route("/n1/capacity/usage", get(get_capacity_usage_scoped))
            .route("/n1/capacity/fragment", get(get_fragment_usage_scoped))
            .route("/n1/fragments", get(list_fragments))
            .route("/n1/objects", get(list_objects_scoped))
            .route("/n1/objects/signed-url", post(mint_signed_read_url_scoped))
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
            .route(
                "/n1/objects/status/{*object_key}",
                get(get_object_repair_status_scoped),
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
            )
            .merge(build_posix_read_routes());

        let read_ops = layers
            .layer_view(read_ops)
            .layer(from_fn(enforce_quiesced_mode_on_reads));

        let write_ops = Router::new()
            .route("/n1/fragments/{:fragment}", delete(delete_fragment))
            .route("/n1/fragments/{:fragment}", patch(update_fragment))
            .route(
                "/n1/trash/empty",
                delete(removed_manual_reclamation_route).post(removed_manual_reclamation_route),
            )
            .route("/n1/posix/purge", post(removed_manual_reclamation_route))
            .route(
                "/n1/objects/permanent/bulk-delete",
                post(removed_manual_reclamation_route),
            )
            .route(
                "/n1/objects/version/{:version_id}/{*object_key}",
                delete(removed_manual_reclamation_route),
            )
            .route(
                "/n1/objects/permanent/{*object_key}",
                delete(removed_manual_reclamation_route),
            )
            .route("/n1/objects/rename", post(rename_object_scoped))
            .route("/n1/objects/bulk-delete", post(bulk_delete_objects_scoped))
            .route("/n1/objects/{*object_key}", delete(delete_object_scoped))
            .route(
                "/n1/objects/repair/{*object_key}",
                post(repair_object_scoped),
            )
            .route(
                "/n1/objects/bulk-recover",
                post(bulk_recover_objects_scoped),
            )
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
            )
            .merge(build_posix_write_routes());

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
            .route(
                "/n1/maintenance/fragments/{:fragment}/gc/start",
                post(run_fragment_gc),
            )
            .route("/n1/maintenance/gc/stop", post(stop_garbage_collector))
            .route("/n1/maintenance/gc/status", get(get_gc_status_api))
            .route(
                "/n1/maintenance/redistribution/start",
                post(trigger_redistribution_job_api),
            )
            .route(
                "/n1/maintenance/fragments/{:fragment}/redistribution/start",
                post(trigger_fragment_redistribution_job),
            )
            .route(
                "/n1/maintenance/objects/redistribution/start/{*object_key}",
                post(trigger_object_redistribution_job),
            )
            .route(
                "/n1/maintenance/redistribution/stop",
                post(stop_redistribution_job_api),
            )
            .route(
                "/n1/maintenance/redistribution/status",
                get(get_redist_status_api),
            )
            .route(
                "/n1/maintenance/fragments/{:fragment}/repair/start",
                post(trigger_fragment_repair_job),
            )
            .route(
                "/n1/maintenance/fragments/{:fragment}/health",
                get(get_fragment_health),
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
        .route("/n1/fragments/hash", get(get_fragment_hash))
        .route("/n1/objects/signed-read", get(read_object_via_signed_url));
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
    fn object_redistribution_route_places_catch_all_last() {
        let _: Router<Arc<AppState>> = Router::new().route(
            "/n1/maintenance/objects/redistribution/start/{*object_key}",
            post(trigger_object_redistribution_job),
        );
    }

    #[tokio::test]
    async fn n1_3_removed_destructive_routes_fail_closed() {
        let (router, state) = build_storage_routes_unbound();
        let router = router.with_state(state);
        for (method, uri) in [
            (Method::DELETE, "/n1/trash/empty"),
            (Method::POST, "/n1/trash/empty"),
            (Method::POST, "/n1/posix/purge"),
            (Method::DELETE, "/n1/objects/permanent/clinical/a.bin"),
            (Method::DELETE, "/n1/objects/version/v1/clinical/a.bin"),
            (Method::POST, "/n1/objects/permanent/bulk-delete"),
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
