use super::*;

pub(super) async fn get_capacity_usage_scoped(
    claims: Claims,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    get_capacity_usage_impl(claims, Path(fragment)).await
}

pub(super) async fn run_fragment_gc(
    State(state): State<Arc<AppState>>,
    Path(fragment): Path<String>,
    Query(mut params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    params.insert("fragment".to_string(), fragment);
    run_garbage_collector(State(state), Query(params)).await
}

pub(super) async fn trigger_fragment_repair_job(
    claims: Claims,
    State(state): State<Arc<AppState>>,
    Path(fragment): Path<String>,
    Query(mut params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    params.insert("fragment".to_string(), fragment);
    trigger_repair_job_api(claims, State(state), Query(params)).await
}

pub(super) async fn trigger_fragment_redistribution_job(
    claims: Claims,
    State(state): State<Arc<AppState>>,
    Path(fragment): Path<String>,
    Query(mut params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    params.insert("fragment".to_string(), fragment);
    trigger_redistribution_job_api(claims, State(state), Query(params)).await
}

pub(super) async fn get_fragment_health(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path(fragment): Path<String>,
    Query(mut params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    params.insert("fragment".to_string(), fragment);
    get_system_health(State(state), claims, Query(params)).await
}

pub(super) async fn list_objects_scoped(
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

pub(super) async fn list_trash_objects_scoped(
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

pub(super) async fn list_object_versions_scoped(
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
    for metadata in &mut versions {
        metadata.object_key = requested_path.clone();
        metadata.strip_internal_layout();
    }
    Ok(AxumJson(versions))
}

pub(super) async fn get_object_scoped(
    claims: Claims,
    Path(object_key): Path<String>,
    req: axum::http::Request<axum::body::Body>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    get_object_impl(claims, Path((fragment, object_key)), req).await
}

pub(super) async fn stream_object_scoped(
    claims: Claims,
    headers: axum::http::HeaderMap,
    Path(object_key): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    stream_object_impl(claims, headers, Path((fragment, object_key))).await
}

pub(super) async fn get_object_metadata_scoped(
    claims: Claims,
    Path(object_key): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let requested_path = object_key;
    let physical = resolve_logical_read_key(&fragment, &requested_path).await?;
    let mut metadata = get_object_metadata_by_id(&fragment, &physical)
        .await?
        .without_internal_layout();
    metadata.object_key = requested_path;
    Ok(AxumJson(metadata))
}

pub(super) async fn download_object_scoped(
    claims: Claims,
    Query(mut query): Query<crate::core::disk_manager::ObjectKeyQuery>,
    headers: axum::http::HeaderMap,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    let requested = query.object_key.clone();
    if query.filename.is_none() {
        query.filename = std::path::Path::new(requested.trim_start_matches('/'))
            .file_name()
            .and_then(|name| name.to_str())
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(ToOwned::to_owned);
    }
    download_object_impl(claims, Path(fragment), Query(query), headers).await
}

pub(super) async fn download_object_chunk_scoped(
    claims: Claims,
    Query(params): Query<crate::core::disk_manager::ChunkQuery>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    download_object_chunk_impl(claims, Path(fragment), Query(params)).await
}

pub(super) async fn download_selected_archive_scoped(
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

pub(super) async fn download_manifest_archive_scoped(
    claims: Claims,
    Query(query): Query<crate::core::disk_manager::DownloadArchiveManifestQuery>,
    body: axum::body::Body,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    download_manifest_archive_impl(claims, Path(fragment), Query(query), body).await
}

pub(super) async fn delete_object_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path(object_key): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    let summary = commit_namespace_summary(PeerNamespaceApplyRequest {
        mutation_id: None,
        kind: NamespaceMutationKind::Trash,
        fragment: fragment.clone(),
        source_path: Some(object_key.clone()),
        destination_path: None,
        object_id: None,
        version_id: None,
        metadata: None,
    })
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

pub(super) async fn recover_object_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path(object_key): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    let summary = commit_namespace_summary(PeerNamespaceApplyRequest {
        mutation_id: None,
        kind: NamespaceMutationKind::Recover,
        fragment: fragment.clone(),
        source_path: Some(object_key.clone()),
        destination_path: None,
        object_id: None,
        version_id: None,
        metadata: None,
    })
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

pub(super) async fn get_object_repair_status_scoped(
    claims: Claims,
    Path(object_key): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let fragment = claims.fragment.clone();
    let requested_version_id = params
        .get("version_id")
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let metadata = if let Some(version_id) = params
        .get("version_id")
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        read_versioned_metadata_for_request(
            &fragment,
            &object_key,
            version_id,
            params.get("physical_object_id").map(String::as_str),
        )
        .await?
        .1
    } else {
        let physical = resolve_logical_read_key(&fragment, &object_key).await?;
        get_object_metadata_by_id(&fragment, &physical).await?
    };
    let mut status = inspect_cluster_object_status(&metadata, parse_check_mode(&params)).await?;
    status.object_key = object_key.trim_start_matches('/').to_string();
    if requested_version_id.is_some() {
        status.version_id = metadata.version_id;
    }
    Ok(AxumJson(status))
}

#[derive(Debug, Serialize)]
pub(super) struct ObjectRedistributeResponse {
    object_key: String,
    physical_object_id: String,
    requested_version_id: Option<String>,
    dry_run: bool,
    force: bool,
    rewritten: bool,
    skipped: bool,
    reason: Option<String>,
    before: ObjectRepairStatus,
    after: Option<ObjectRepairStatus>,
}

pub(super) async fn redistribute_object_scoped(
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
    let logical = object_key.trim_start_matches('/').to_string();
    let requested_version_id = params
        .get("version_id")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let (physical, metadata) = if let Some(version_id) = requested_version_id.as_deref() {
        read_versioned_metadata_for_request(
            &fragment,
            &logical,
            version_id,
            params.get("physical_object_id").map(String::as_str),
        )
        .await?
    } else {
        let physical = resolve_logical_read_key(&fragment, &logical).await?;
        let metadata = get_object_metadata_by_id(&fragment, &physical).await?;
        (physical, metadata)
    };
    let check_mode = parse_check_mode(&params);
    let mut before = inspect_cluster_object_status(&metadata, check_mode).await?;
    before.object_key = logical.clone();

    let runtime = active_cluster_runtime()?;
    let dry_run = query_bool(&params, "dry_run");
    let force = query_bool(&params, "force");
    let already_current = metadata_matches_current_layout(&metadata, &runtime);
    if already_current && !force {
        return Ok(AxumJson(ObjectRedistributeResponse {
            object_key: logical,
            physical_object_id: physical,
            requested_version_id,
            dry_run,
            force,
            rewritten: false,
            skipped: true,
            reason: Some("object version already uses the current topology".to_string()),
            before: before.clone(),
            after: Some(before),
        }));
    }

    if dry_run {
        return Ok(AxumJson(ObjectRedistributeResponse {
            object_key: logical,
            physical_object_id: physical,
            requested_version_id,
            dry_run,
            force,
            rewritten: false,
            skipped: false,
            reason: Some("dry_run: object version would be redistributed".to_string()),
            before,
            after: None,
        }));
    }

    rewrite_object_to_current_layout(logical.clone(), physical.clone(), metadata.clone(), false)
        .await?;
    let rewritten_metadata = get_object_metadata_by_id(&fragment, &physical).await?;
    let mut after = inspect_cluster_object_status(&rewritten_metadata, check_mode).await?;
    after.object_key = logical.clone();
    Ok(AxumJson(ObjectRedistributeResponse {
        object_key: logical,
        physical_object_id: physical,
        requested_version_id,
        dry_run,
        force,
        rewritten: true,
        skipped: false,
        reason: None,
        before,
        after: Some(after),
    }))
}

pub(super) async fn repair_object_scoped(
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
    let requested_version_id = params
        .get("version_id")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let (_physical, metadata) = if let Some(version_id) = requested_version_id.as_deref() {
        read_versioned_metadata_for_request(
            &fragment,
            &object_key,
            version_id,
            params.get("physical_object_id").map(String::as_str),
        )
        .await?
    } else {
        let physical = resolve_logical_read_key(&fragment, &object_key).await?;
        let metadata = get_object_metadata_by_id(&fragment, &physical).await?;
        (physical, metadata)
    };
    let check_mode = parse_check_mode(&params);
    let mut status = if metadata.parts.iter().any(part_has_remote_shards) {
        let status = inspect_cluster_object_status(&metadata, check_mode).await?;
        if status.status != "healthy" && status.repairable {
            crate::core::repair_manager::repair_cluster_object_recorded_layout(
                &metadata, check_mode,
            )
            .await?
        } else {
            status
        }
    } else {
        repair_object(&metadata, check_mode).await?
    };
    status.object_key = object_key.trim_start_matches('/').to_string();
    Ok(AxumJson(status))
}

pub(super) async fn bulk_delete_objects_scoped(
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
    for (object_key, result) in
        commit_namespace_bulk(&fragment, NamespaceMutationKind::Trash, unique_keys.clone()).await
    {
        match result {
            Ok(_) => {
                succeeded = succeeded.saturating_add(1);
            }
            Err(AppError::NotFound) => not_found = not_found.saturating_add(1),
            Err(err) => failed.push(BulkObjectFailure {
                object_key,
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

pub(super) async fn bulk_recover_objects_scoped(
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
    for (object_key, result) in commit_namespace_bulk(
        &fragment,
        NamespaceMutationKind::Recover,
        unique_keys.clone(),
    )
    .await
    {
        match result {
            Ok(_) => {
                succeeded = succeeded.saturating_add(1);
            }
            Err(AppError::NotFound) => not_found = not_found.saturating_add(1),
            Err(err) => failed.push(BulkObjectFailure {
                object_key,
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

pub(super) async fn rename_object_scoped(
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

    let summary = commit_namespace_summary(PeerNamespaceApplyRequest {
        mutation_id: None,
        kind: NamespaceMutationKind::Rename,
        fragment: claims.fragment.clone(),
        source_path: Some(payload.source_key.clone()),
        destination_path: Some(payload.destination_key.clone()),
        object_id: None,
        version_id: None,
        metadata: None,
    })
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

pub(super) async fn initiate_object_upload_scoped(
    claims: Claims,
    Path(object_key): Path<String>,
    AxumJson(mut params): AxumJson<crate::core::disk_manager::InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    if let Some(existing) = find_reusable_standard_cluster_upload(&fragment, &object_key, &params) {
        return Ok((StatusCode::CREATED, AxumJson(existing)).into_response());
    }
    params.packed_ext_u64 = index_store::packed_extension_key(&object_key);
    let physical = resolve_logical_write_key(&fragment, &object_key).await?;
    let parsed =
        initiate_standard_cluster_upload(claims, fragment, physical, object_key, params).await?;
    Ok((StatusCode::CREATED, AxumJson(parsed)).into_response())
}

pub(super) async fn initiate_object_upload_durable_scoped(
    claims: Claims,
    Path(object_key): Path<String>,
    AxumJson(mut params): AxumJson<crate::core::disk_manager::InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    if let Some(existing) = find_reusable_pending_upload(&fragment, &object_key, &params).await? {
        return Ok((StatusCode::CREATED, AxumJson(existing)).into_response());
    }
    params.packed_ext_u64 = index_store::packed_extension_key(&object_key);
    let physical = resolve_logical_write_key(&fragment, &object_key).await?;
    let parsed =
        initiate_cluster_upload(claims, fragment.clone(), physical.clone(), params).await?;
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

pub(super) async fn upload_object_one_shot_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    headers: axum::http::HeaderMap,
    Path(object_key): Path<String>,
    Query(mut query): Query<crate::core::disk_manager::UploadPartQuery>,
    body: axum::body::Body,
) -> Result<impl IntoResponse, AppError> {
    let fragment = claims.fragment.clone();
    let physical = resolve_logical_write_key(&fragment, &object_key).await?;
    let content_length = request_content_length(&headers)?;
    let content_type = request_content_type(&headers);
    query.checksum = upload_sha256_checksum(&headers, query.checksum.as_deref())?;
    let body_stream = body.into_data_stream().map(|chunk_result| {
        chunk_result.map_err(|e| {
            AppError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e),
                "cluster upload body read".into(),
            )
        })
    });
    let mut parsed = upload_cluster_one_shot(
        claims,
        fragment.clone(),
        physical.clone(),
        index_store::packed_extension_key(&object_key),
        content_type,
        query,
        body_stream,
        content_length,
    )
    .await?;
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
