use super::*;

pub(super) fn request_content_length(headers: &axum::http::HeaderMap) -> Result<u64, AppError> {
    headers
        .get("content-length")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| AppError::BadRequest("Content-Length header required".into()))
}

pub(super) fn request_content_type(headers: &axum::http::HeaderMap) -> String {
    headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.split(';').next().unwrap_or(value).trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "application/octet-stream".to_string())
}

pub struct AppState {
    pub ws_manager: WsConnectionManager,
}

impl AppState {
    pub fn emit(&self, uid: &str, payload: StorageEventPayload) {
        self.ws_manager.publish(uid, payload);
    }
}

pub(super) async fn parse_json_response<T: DeserializeOwned>(
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

pub(super) fn download_filename_for_logical_path(
    requested_path: &str,
    resolved_key: &str,
) -> Option<String> {
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

pub(super) fn looks_like_flat_storage_object_id(requested_path: &str) -> bool {
    let normalized = requested_path.trim_start_matches('/');
    !normalized.is_empty()
        && !normalized.contains('/')
        && normalized.len() == 64
        && normalized.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(super) async fn read_versioned_metadata_for_request(
    fragment: &str,
    requested_path: &str,
    version_id: &str,
    physical_hint: Option<&str>,
) -> Result<(String, crate::core::disk_manager::ObjectMetadata), AppError> {
    let requested = requested_path.trim_start_matches('/').trim().to_string();
    let version_id = version_id.trim();
    if requested.is_empty() || version_id.is_empty() {
        return Err(AppError::NotFound);
    }

    if let Some(physical) = physical_hint
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|value| index_store::normalize_object_id(value).ok())
    {
        if let Ok(mut metadata) = index::read_object_version(fragment, &physical, version_id).await
        {
            metadata.object_key = requested;
            return Ok((physical, metadata));
        }
    }

    if let Ok(physical) = resolve_logical_read_key(fragment, &requested).await {
        if let Ok(mut metadata) = index::read_object_version(fragment, &physical, version_id).await
        {
            metadata.object_key = requested;
            return Ok((physical, metadata));
        }
    }

    if let Ok(physical) = index_store::normalize_object_id(&requested) {
        if let Ok(mut metadata) = index::read_object_version(fragment, &physical, version_id).await
        {
            metadata.object_key = requested;
            return Ok((physical, metadata));
        }
    }

    if let Some(mut metadata) = index::find_live_object_version_by_id(fragment, version_id).await? {
        let physical = metadata.object_key.clone();
        metadata.object_key = requested;
        return Ok((physical, metadata));
    }

    Err(AppError::NotFound)
}

pub(super) fn query_bool(params: &HashMap<String, String>, key: &str) -> bool {
    params
        .get(key)
        .map(|value| value.trim().to_ascii_lowercase())
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
}

pub(crate) async fn resolve_logical_write_key(
    fragment: &str,
    requested_path: &str,
) -> Result<String, AppError> {
    if looks_like_flat_storage_object_id(requested_path) {
        if let Some(entry) = global_namespace_runtime()
            .entry_for_write(fragment, requested_path)
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

pub(super) fn upload_request_matches_session(
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

pub(super) async fn apply_logical_upload_binding_to_status(
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

pub(super) async fn apply_logical_upload_bindings_to_sessions(
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
    let started = std::time::Instant::now();
    let result = commit_upload_pointer(fragment, logical_path, physical_object_id, metadata).await;
    record_io_latency(
        "cluster.upload.publish_ncnx",
        started.elapsed(),
        metadata.total_size_bytes,
        result.is_ok(),
    );
    result
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

pub(super) async fn mint_signed_read_url_scoped(
    claims: Claims,
    AxumJson(mut req): AxumJson<SignedReadUrlRequest>,
) -> Result<impl IntoResponse, AppError> {
    let requested = req.object_key.clone();
    if req.filename.is_none() {
        req.filename = std::path::Path::new(requested.trim_start_matches('/'))
            .file_name()
            .and_then(|name| name.to_str())
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(ToOwned::to_owned);
    }
    if req
        .version_id
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
    {
        req.object_key = requested;
    } else {
        let resolved = resolve_logical_read_key(&claims.fragment, &requested).await?;
        if req.filename.is_none() {
            req.filename = download_filename_for_logical_path(&requested, &resolved);
        }
        req.object_key = resolved;
    }
    mint_signed_read_url(claims, AxumJson(req)).await
}
