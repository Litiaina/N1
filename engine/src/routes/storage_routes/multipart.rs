use super::*;

pub(super) async fn get_upload_status_scoped(
    claims: Claims,
    Path(version_id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    Ok(AxumJson(get_standard_cluster_upload_status(
        &claims,
        &fragment,
        &version_id,
    )?))
}

pub(super) async fn get_upload_status_durable_scoped(
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

pub(super) async fn list_active_uploads_scoped(
    claims: Claims,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    let limit = params
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(50)
        .min(100);
    let offset = params
        .get("offset")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    Ok(AxumJson(list_standard_cluster_uploads(
        &claims, &fragment, limit, offset,
    )?))
}

pub(super) async fn list_active_uploads_durable_scoped(
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

pub(super) async fn object_upload_part_scoped(
    claims: Claims,
    headers: axum::http::HeaderMap,
    Path((version_id, part_index)): Path<(String, u64)>,
    Query(mut query): Query<crate::core::disk_manager::UploadPartQuery>,
    body: axum::body::Body,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    let content_length = request_content_length(&headers)?;
    query.checksum = upload_sha256_checksum(&headers, query.checksum.as_deref())?;
    let body_stream = body.into_data_stream().map(|chunk_result| {
        chunk_result.map_err(|e| {
            AppError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e),
                "cluster multipart body read".into(),
            )
        })
    });
    put_standard_cluster_part(
        claims,
        fragment,
        version_id,
        part_index,
        query,
        body_stream,
        content_length,
    )
    .await?;
    Ok(StatusCode::OK)
}

pub(super) async fn object_upload_part_durable_scoped(
    claims: Claims,
    headers: axum::http::HeaderMap,
    Path((version_id, part_index)): Path<(String, u64)>,
    Query(mut query): Query<crate::core::disk_manager::UploadPartQuery>,
    body: axum::body::Body,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    let content_length = request_content_length(&headers)?;
    query.checksum = upload_sha256_checksum(&headers, query.checksum.as_deref())?;
    let body_stream = body.into_data_stream().map(|chunk_result| {
        chunk_result.map_err(|e| {
            AppError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e),
                "durable cluster multipart body read".into(),
            )
        })
    });
    put_cluster_part(
        claims,
        fragment,
        version_id,
        part_index,
        query,
        body_stream,
        content_length,
    )
    .await?;
    Ok(StatusCode::OK)
}

pub(super) async fn finalize_object_upload_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((version_id, session_id)): Path<(String, String)>,
    AxumJson(_params): AxumJson<crate::core::disk_manager::InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    let (logical_path, physical) = standard_upload_binding(&fragment, &version_id)
        .ok_or_else(|| AppError::BadRequest("Standard upload session not found".into()))?;
    let result =
        finalize_standard_cluster_upload(claims, fragment.clone(), version_id.clone(), session_id)
            .await?;
    let mut parsed = match result {
        FinalizeUploadResult::Complete(metadata) => metadata,
        FinalizeUploadResult::Incomplete(incomplete) => {
            return Ok((StatusCode::CONFLICT, AxumJson(incomplete)).into_response());
        }
    };
    let committed_physical =
        commit_logical_upload_from_meta(&fragment, &logical_path, &physical, &parsed).await?;
    complete_standard_cluster_upload(&fragment, &version_id);
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

pub(super) async fn finalize_object_upload_durable_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((version_id, session_id)): Path<(String, String)>,
    Query(key_query): Query<HashMap<String, String>>,
    AxumJson(params): AxumJson<crate::core::disk_manager::InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    let binding = global_namespace_runtime()
        .pending_upload(&fragment, &version_id)
        .await?;
    let requested_key = key_query.get("object_key").cloned();
    let physical = binding
        .as_ref()
        .map(|value| value.physical_object_id.clone())
        .or_else(|| requested_key.clone())
        .ok_or_else(|| {
            AppError::BadRequest(
                "object_key is required when the durable upload binding is unavailable".into(),
            )
        })?;
    let result = finalize_cluster_upload(
        claims,
        fragment.clone(),
        version_id.clone(),
        session_id,
        physical.clone(),
        params,
    )
    .await?;
    let mut parsed = match result {
        FinalizeUploadResult::Complete(metadata) => metadata,
        FinalizeUploadResult::Incomplete(incomplete) => {
            return Ok((StatusCode::CONFLICT, AxumJson(incomplete)).into_response());
        }
    };
    let logical_path = binding
        .as_ref()
        .map(|value| value.logical_path.clone())
        .or(requested_key)
        .ok_or_else(|| {
            AppError::BadRequest(
                "object_key is required when the durable upload binding is unavailable".into(),
            )
        })?;
    let committed_physical = commit_logical_upload_from_meta(
        &parsed.fragment,
        &logical_path,
        &parsed.object_key,
        &parsed,
    )
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

pub(super) async fn cancel_upload_session_scoped(
    claims: Claims,
    Path(version_id): Path<String>,
    Query(_key_query): Query<crate::core::disk_manager::ObjectKeyQuery>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    cancel_standard_cluster_upload(&claims, &fragment, &version_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn cancel_upload_session_durable_scoped(
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
