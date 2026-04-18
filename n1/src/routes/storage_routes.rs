use crate::core::adaptive_concurrency::run_adaptive_concurrency_loop;
use crate::core::archive::{
    download_manifest_archive as download_manifest_archive_impl, download_prepared_archive,
    download_selected_archive as download_selected_archive_impl,
};
use crate::core::delete::{
    delete_object_version as delete_object_version_impl,
    delete_objects_bulk as delete_objects_bulk_impl,
    delete_objects_bulk_permanently as delete_objects_bulk_permanently_impl,
    get_empty_trash_status_api, get_recover_all_trash_status_api,
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
use crate::core::index;
use crate::core::maintenance::sync_fragment_metadata;
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
    list_object_versions as list_object_versions_impl, stream_object as stream_object_impl,
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
use crate::namespace::runtime::{global_namespace_runtime, logical_not_found};
use crate::namespace::scoped_archive::prepare_logical_archive_entries;
use crate::posix::routes::{build_posix_read_routes, build_posix_write_routes};
use crate::structs::auth_structs::Claims;
use crate::url::signed_read::{
    SignedReadUrlRequest, mint_signed_read_url, read_object_via_signed_url,
};
use crate::websocket::handler::ws_handler;
use crate::websocket::manager::{WsConnectionManager, register_global_ws_manager};
use crate::websocket::ws_structs::StorageEventPayload;
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

async fn resolve_logical_read_key(
    fragment: &str,
    requested_path: &str,
) -> Result<String, AppError> {
    if let Some(entry) = global_namespace_runtime()
        .entry(fragment, requested_path)
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
    Ok(requested_path.to_string())
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

async fn resolve_logical_write_key(
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

async fn find_reusable_pending_upload(
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

async fn commit_logical_upload_from_meta(
    fragment: &str,
    logical_path: &str,
    physical_object_id: &str,
    metadata: &crate::core::disk_manager::ObjectMetadata,
) -> Result<(), AppError> {
    global_namespace_runtime()
        .finalize_logical_upload(fragment, logical_path, physical_object_id, metadata)
        .await
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
    let physical = resolve_logical_read_key(&fragment, &object_key).await?;
    list_object_versions_impl(claims, Path((fragment, physical))).await
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
    let physical = resolve_logical_read_key(&fragment, &object_key).await?;
    get_object_metadata_impl(claims, Path((fragment, physical))).await
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
    if let Some(prepared) = prepare_logical_archive_entries(&fragment, &req).await? {
        return download_prepared_archive(prepared, req).await;
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

async fn empty_trash_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    if !claims.delete {
        return Err(AppError::Forbidden("Delete permission required".into()));
    }
    let summary = global_namespace_runtime().empty_trash(&fragment).await?;
    state.emit(&fragment, StorageEventPayload::StateInvalidated);
    Ok((
        StatusCode::OK,
        AxumJson(serde_json::json!({
            "operation": summary.operation,
            "affected_paths": summary.affected_paths,
            "affected_objects": summary.affected_objects,
        })),
    ))
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
    state.emit(&fragment, StorageEventPayload::StateInvalidated);
    Ok((
        StatusCode::OK,
        AxumJson(serde_json::json!({
            "operation": summary.operation,
            "affected_paths": summary.affected_paths,
            "affected_objects": summary.affected_objects,
        })),
    ))
}

async fn delete_object_version_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((version_id, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    let physical = resolve_logical_read_key(&fragment, &object_key).await?;
    delete_object_version_impl(State(state), claims, Path((fragment, version_id, physical))).await
}

async fn delete_object_permanently_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path(object_key): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let _ = state;
    let _ = claims;
    let _ = object_key;
    Err::<axum::response::Response, AppError>(AppError::BadRequest(
        "Permanent delete was removed in N1 2.0; use lifecycle trash and garbage collection"
            .to_string(),
    ))
}

async fn recover_object_version_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((version_id, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    let physical = resolve_logical_read_key(&fragment, &object_key).await?;
    recover_object_version_impl(State(state), claims, Path((fragment, version_id, physical))).await
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
    state.emit(&fragment, StorageEventPayload::StateInvalidated);
    Ok((
        StatusCode::OK,
        AxumJson(serde_json::json!({
            "operation": summary.operation,
            "affected_paths": summary.affected_paths,
            "affected_objects": summary.affected_objects,
        })),
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
    state.emit(&fragment, StorageEventPayload::StateInvalidated);
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
    AxumJson(payload): AxumJson<crate::core::disk_manager::RenameObjectRequest>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden("Modify permission required".into()));
    }
    if payload.source_fragment != claims.fragment || payload.destination_fragment != claims.fragment
    {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    global_namespace_runtime()
        .rename(
            &claims.fragment,
            &payload.source_key,
            &payload.destination_key,
        )
        .await?;

    state.emit(
        &claims.fragment,
        StorageEventPayload::FileRenamed {
            source_fragment: claims.fragment.clone(),
            source_key: payload.source_key,
            destination_fragment: claims.fragment.clone(),
            destination_key: payload.destination_key,
        },
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
        State(state),
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
    let mut parsed: crate::core::disk_manager::ObjectMetadata =
        parse_json_response(response, "one-shot upload").await?;
    commit_logical_upload_from_meta(&fragment, &object_key, &physical, &parsed).await?;
    parsed.object_key = object_key;
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
        State(state),
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
    let mut parsed = index::read_object_version(&fragment, &physical, &version_id).await?;
    let logical_path = binding
        .as_ref()
        .map(|value| value.logical_path.clone())
        .unwrap_or(key_query.object_key);
    commit_logical_upload_from_meta(&parsed.fragment, &logical_path, &physical, &parsed).await?;
    global_namespace_runtime()
        .clear_pending_upload(&parsed.fragment, &parsed.version_id)
        .await?;
    parsed.object_key = logical_path;
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
    let _ = global_namespace_runtime()
        .clear_pending_upload(&fragment, &version_id)
        .await;
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
            .route("/n1/trash/empty", delete(empty_trash_scoped))
            .route("/n1/objects/rename", post(rename_object_scoped))
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

#[cfg(test)]
mod tests {
    use super::{
        AppState, download_filename_for_logical_path, download_selected_archive_scoped,
        get_upload_status_scoped, initiate_object_upload_scoped, list_active_uploads_scoped,
        looks_like_flat_storage_object_id, parse_json_response, upload_object_one_shot_scoped,
        upload_request_matches_session,
    };
    use crate::core::disk_manager::{
        BLOB_STORE_DISKS, DownloadArchiveRequest, FS_STORAGE_ROOT, INDEX_STORE_DISKS,
        InitiateUploadResponse,
    };
    use crate::core::disk_manager::{InitiateUploadParams, ReservationReceipt, UploadPartQuery};
    use crate::core::fragment_metadata::{
        DEFAULT_INDEX_PARTITION_COUNT, clear_fragment_partition_count_cache_for_test,
        init_fragment_metadata, set_configured_index_partition_count,
    };
    use crate::core::index::IndexSessionState;
    use crate::core::indexdb_store;
    use crate::core::quota_manager::init_quota_file;
    use crate::core::sync::ensure_fragment_blob_directories;
    use crate::core::upload::UploadSessionManifest;
    use crate::core::upload::{
        DATA_SHARDS_ATOMIC, ListUploadsResponse, MAX_IO_SEMAPHORE, MAX_PART_SIZE_ATOMIC,
        PARITY_SHARDS_ATOMIC, STRIPE_SIZE_ATOMIC, WRITER_BUFFER_SIZE_ATOMIC,
    };
    use crate::namespace::scoped_archive::prepare_logical_archive_entries;
    use crate::settings::constants::CONFIG_FILE;
    use crate::settings::create_config::create_default_config_ini;
    use crate::structs::auth_structs::Claims;
    use crate::websocket::manager::WsConnectionManager;
    use axum::body::Body;
    use axum::Json as AxumJson;
    use axum::extract::{Path, Query, State};
    use axum::response::IntoResponse;
    use once_cell::sync::Lazy;
    use sha2::{Digest, Sha256};
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use tokio::fs;
    use tokio::sync::Mutex;
    use ulid::Ulid;

    static TEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

    fn ensure_test_config_file() {
        let config_path = PathBuf::from(CONFIG_FILE);
        if !config_path.exists() {
            create_default_config_ini(&config_path).expect("create default test config");
        }
    }

    async fn ensure_storage_config() {
        ensure_test_config_file();
        set_configured_index_partition_count(DEFAULT_INDEX_PARTITION_COUNT)
            .expect("reset configured index partition count");
        clear_fragment_partition_count_cache_for_test();
        indexdb_store::clear_cached_dbs_for_test();

        let base = std::env::temp_dir().join(format!("n1-storage-routes-{}", std::process::id()));
        let index_roots = if let Some(existing) = INDEX_STORE_DISKS.get() {
            existing.iter().map(PathBuf::from).collect::<Vec<_>>()
        } else {
            vec![base.join("index_1"), base.join("index_2")]
        };
        let blob_roots = if let Some(existing) = BLOB_STORE_DISKS.get() {
            existing.iter().map(PathBuf::from).collect::<Vec<_>>()
        } else {
            vec![
                base.join("blob_1"),
                base.join("blob_2"),
                base.join("blob_3"),
            ]
        };
        let fs_root = if let Some(existing) = FS_STORAGE_ROOT.get() {
            PathBuf::from(existing)
        } else {
            base.join("fs")
        };

        for root in index_roots
            .iter()
            .chain(blob_roots.iter())
            .chain(std::iter::once(&fs_root))
        {
            let _ = fs::create_dir_all(root).await;
        }

        if INDEX_STORE_DISKS.get().is_none() {
            INDEX_STORE_DISKS
                .set(
                    index_roots
                        .iter()
                        .map(|path| path.to_string_lossy().to_string())
                        .collect(),
                )
                .expect("set INDEX_STORE_DISKS");
        }
        if BLOB_STORE_DISKS.get().is_none() {
            BLOB_STORE_DISKS
                .set(
                    blob_roots
                        .iter()
                        .map(|path| path.to_string_lossy().to_string())
                        .collect(),
                )
                .expect("set BLOB_STORE_DISKS");
        }
        if FS_STORAGE_ROOT.get().is_none() {
            FS_STORAGE_ROOT
                .set(fs_root.to_string_lossy().to_string())
                .expect("set FS_STORAGE_ROOT");
        }
    }

    fn ensure_upload_runtime_config() {
        if MAX_IO_SEMAPHORE.get().is_none() {
            MAX_IO_SEMAPHORE.set(128).expect("set MAX_IO_SEMAPHORE");
        }
        if MAX_PART_SIZE_ATOMIC.get().is_none() {
            MAX_PART_SIZE_ATOMIC
                .set(AtomicU64::new(16 * 1024 * 1024))
                .expect("set MAX_PART_SIZE_ATOMIC");
        }
        if DATA_SHARDS_ATOMIC.get().is_none() {
            DATA_SHARDS_ATOMIC
                .set(AtomicUsize::new(1))
                .expect("set DATA_SHARDS_ATOMIC");
        } else {
            DATA_SHARDS_ATOMIC
                .get()
                .expect("DATA_SHARDS_ATOMIC")
                .store(1, Ordering::Relaxed);
        }
        if PARITY_SHARDS_ATOMIC.get().is_none() {
            PARITY_SHARDS_ATOMIC
                .set(AtomicUsize::new(0))
                .expect("set PARITY_SHARDS_ATOMIC");
        } else {
            PARITY_SHARDS_ATOMIC
                .get()
                .expect("PARITY_SHARDS_ATOMIC")
                .store(0, Ordering::Relaxed);
        }
        if WRITER_BUFFER_SIZE_ATOMIC.get().is_none() {
            WRITER_BUFFER_SIZE_ATOMIC
                .set(AtomicUsize::new(64 * 1024))
                .expect("set WRITER_BUFFER_SIZE_ATOMIC");
        } else {
            WRITER_BUFFER_SIZE_ATOMIC
                .get()
                .expect("WRITER_BUFFER_SIZE_ATOMIC")
                .store(64 * 1024, Ordering::Relaxed);
        }
        if STRIPE_SIZE_ATOMIC.get().is_none() {
            STRIPE_SIZE_ATOMIC
                .set(AtomicUsize::new(256 * 1024))
                .expect("set STRIPE_SIZE_ATOMIC");
        } else {
            STRIPE_SIZE_ATOMIC
                .get()
                .expect("STRIPE_SIZE_ATOMIC")
                .store(256 * 1024, Ordering::Relaxed);
        }
    }

    async fn setup_fragment(fragment: &str) {
        init_quota_file(fragment, 64 * 1024 * 1024)
            .await
            .expect("init quota");
        ensure_fragment_blob_directories(fragment).await;
        init_fragment_metadata(fragment, "key")
            .await
            .expect("init metadata");
    }

    fn full_claims(fragment: &str) -> Claims {
        Claims {
            fragment: fragment.to_string(),
            read: true,
            write: true,
            modify: true,
            delete: true,
            exp: usize::MAX,
        }
    }

    fn sample_upload_params() -> InitiateUploadParams {
        InitiateUploadParams {
            content_type: Some("video/mp4".to_string()),
            total_size_bytes: 8 * 1024 * 1024,
            expected_parts: 4,
            part_size_bytes: Some(2 * 1024 * 1024),
        }
    }

    #[test]
    fn flat_storage_object_id_detection_matches_registry_shape() {
        assert!(looks_like_flat_storage_object_id(
            "a309986ad9486b636f2c7ffd3abd63a244a0bf14e32f9a05274c27209da687cb"
        ));
        assert!(!looks_like_flat_storage_object_id("images/archive.bin"));
        assert!(!looks_like_flat_storage_object_id(
            "01KNSFH1NYRERVTB8XT0SB9DD5"
        ));
        assert!(!looks_like_flat_storage_object_id(
            "a309986ad9486b636f2c7ffd3abd63a244a0bf14e32f9a05274c27209da687cb/child"
        ));
    }

    #[test]
    fn logical_download_filename_uses_requested_basename_only_for_logical_paths() {
        assert_eq!(
            download_filename_for_logical_path(
                "videos/demo.mp4",
                "a309986ad9486b636f2c7ffd3abd63a244a0bf14e32f9a05274c27209da687cb"
            )
            .as_deref(),
            Some("demo.mp4")
        );
        assert_eq!(
            download_filename_for_logical_path(
                "IMG_0001.mov",
                "f32cf9271c2517798966d4970207d463f9a3277ceb5b0e3520f9d5dba8747e7e"
            )
            .as_deref(),
            Some("IMG_0001.mov")
        );
        assert!(
            download_filename_for_logical_path(
                "a309986ad9486b636f2c7ffd3abd63a244a0bf14e32f9a05274c27209da687cb",
                "a309986ad9486b636f2c7ffd3abd63a244a0bf14e32f9a05274c27209da687cb"
            )
            .is_none()
        );
    }

    fn sample_session_state(
        total_size_bytes: u64,
        expected_parts: u64,
        part_size_bytes: u64,
        content_type: &str,
    ) -> IndexSessionState {
        IndexSessionState {
            manifest: UploadSessionManifest {
                fragment: "frag".to_string(),
                object_key: "01TESTOBJECTID".to_string(),
                version_id: "01TESTVERSION".to_string(),
                session_id: "01TESTSESSION".to_string(),
                total_size_bytes,
                expected_parts,
                part_size_bytes,
                content_type: content_type.to_string(),
                created_at: 0,
                stripe_size_bytes: 262_144,
                data_shards: 1,
                parity_shards: 0,
            },
            reservation: Some(ReservationReceipt {
                amount: total_size_bytes,
            }),
            parts: BTreeMap::new(),
            part_receipts: BTreeMap::new(),
            last_activity_at: 0,
            finalized_at: None,
            aborted_at: None,
        }
    }

    #[test]
    fn upload_request_matching_accepts_same_session_shape() {
        let params = InitiateUploadParams {
            content_type: Some("video/mp4".to_string()),
            total_size_bytes: 8_388_608,
            expected_parts: 4,
            part_size_bytes: Some(2_097_152),
        };
        let state = sample_session_state(8_388_608, 4, 2_097_152, "video/mp4");
        assert!(upload_request_matches_session(&params, &state));
    }

    #[test]
    fn upload_request_matching_rejects_size_or_type_mismatch() {
        let params = InitiateUploadParams {
            content_type: Some("video/mp4".to_string()),
            total_size_bytes: 8_388_609,
            expected_parts: 4,
            part_size_bytes: Some(2_097_152),
        };
        let state = sample_session_state(8_388_608, 4, 2_097_152, "application/octet-stream");
        assert!(!upload_request_matches_session(&params, &state));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scoped_multipart_resume_surfaces_logical_path_and_reuses_pending_session() {
        let _guard = TEST_LOCK.lock().await;
        ensure_storage_config().await;
        ensure_upload_runtime_config();

        let fragment = {
            let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
            hex::encode(digest)
        };
        setup_fragment(&fragment).await;
        let logical_path = "videos/video.mp4".to_string();

        let first = initiate_object_upload_scoped(
            full_claims(&fragment),
            Path(logical_path.clone()),
            AxumJson(sample_upload_params()),
        )
        .await
        .expect("first scoped init")
        .into_response();
        let first_json: InitiateUploadResponse = parse_json_response(first, "scoped init first")
            .await
            .expect("decode first init");

        let second = initiate_object_upload_scoped(
            full_claims(&fragment),
            Path(logical_path.clone()),
            AxumJson(sample_upload_params()),
        )
        .await
        .expect("second scoped init")
        .into_response();
        let second_json: InitiateUploadResponse = parse_json_response(second, "scoped init second")
            .await
            .expect("decode second init");

        assert_eq!(second_json.version_id, first_json.version_id);
        assert_eq!(second_json.session_id, first_json.session_id);

        let listed = list_active_uploads_scoped(full_claims(&fragment), Query(HashMap::new()))
            .await
            .expect("list active uploads")
            .into_response();
        let listed_json: ListUploadsResponse = parse_json_response(listed, "scoped active uploads")
            .await
            .expect("decode uploads");
        let session = listed_json
            .sessions
            .iter()
            .find(|entry| entry.version_id == first_json.version_id)
            .expect("pending session in scoped listing");
        assert_eq!(session.object_key.as_deref(), Some(logical_path.as_str()));
        assert!(
            session
                .physical_object_key
                .as_deref()
                .is_some_and(|value| !value.is_empty() && value != logical_path)
        );

        let status =
            get_upload_status_scoped(full_claims(&fragment), Path(first_json.version_id.clone()))
                .await
                .expect("get upload status")
                .into_response();
        let status_json: crate::core::disk_manager::UploadStatusResponse =
            parse_json_response(status, "scoped upload status")
                .await
                .expect("decode status");
        assert_eq!(
            status_json.object_key.as_deref(),
            Some(logical_path.as_str())
        );
        assert!(
            status_json
                .physical_object_key
                .as_deref()
                .is_some_and(|value| !value.is_empty() && value != logical_path)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scoped_folder_archive_preparation_resolves_logical_prefixes() {
        let _guard = TEST_LOCK.lock().await;
        ensure_storage_config().await;
        ensure_upload_runtime_config();

        let fragment = {
            let digest = Sha256::digest(format!("fragment-{}", Ulid::new()).as_bytes());
            hex::encode(digest)
        };
        setup_fragment(&fragment).await;

        let state = Arc::new(AppState {
            ws_manager: WsConnectionManager::new(),
        });

        let payload = "demo-video-payload";
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::CONTENT_LENGTH,
            axum::http::HeaderValue::from_str(&payload.len().to_string())
                .expect("content-length header"),
        );

        let upload = upload_object_one_shot_scoped(
            State(state),
            full_claims(&fragment),
            headers,
            Path("videos/demo.mp4".to_string()),
            Query(UploadPartQuery {
                checksum: None,
                durability: None,
                strict_durability: None,
                io_gate: None,
            }),
            Body::from(payload),
        )
        .await
        .expect("scoped one-shot upload")
        .into_response();
        assert_eq!(upload.status(), axum::http::StatusCode::CREATED);

        let request = DownloadArchiveRequest {
            keys: Vec::new(),
            prefix: None,
            prefixes: vec!["videos/".to_string()],
            format: "tar".to_string(),
            filename: Some("videos".to_string()),
            export_all: false,
            include_internal: false,
        };

        let prepared = prepare_logical_archive_entries(&fragment, &request)
            .await
            .expect("prepare folder archive")
            .expect("prepared entries");
        assert_eq!(prepared.len(), 1);
        assert_eq!(prepared[0].archive_path, "videos/demo.mp4");
        assert_ne!(
            prepared[0].metadata.object_key, prepared[0].archive_path,
            "prepared archive should resolve the logical path to the physical object id"
        );

        let response = download_selected_archive_scoped(full_claims(&fragment), AxumJson(request))
            .await
            .expect("download folder archive");
        assert_eq!(response.status(), axum::http::StatusCode::OK);
    }
}
