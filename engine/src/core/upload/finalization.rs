use super::*;

pub async fn finalize_object_upload(
    State(_state): State<Arc<AppState>>,
    claims: Claims,
    AxumPath((fragment, version_id, session_id)): AxumPath<(String, String, String)>,
    Query(key_query): Query<ObjectKeyQuery>,
    AxumJson(params): AxumJson<InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    match finalize_object_upload_direct(
        claims,
        fragment,
        version_id,
        session_id,
        key_query.object_key,
        params,
    )
    .await?
    {
        FinalizeUploadResult::Complete(object_meta) => {
            let response_body = serde_json::to_value(object_meta.without_internal_layout())
                .map_err(|e| {
                    AppError::InternalServerError(format!(
                        "serialize finalized upload response: {}",
                        e
                    ))
                })?;
            Ok((StatusCode::OK, AxumJson(response_body)).into_response())
        }
        FinalizeUploadResult::Incomplete(incomplete) => {
            Ok((StatusCode::CONFLICT, AxumJson(incomplete)).into_response())
        }
    }
}

pub async fn finalize_object_upload_direct(
    claims: Claims,
    fragment: String,
    version_id: String,
    session_id: String,
    object_key: String,
    params: InitiateUploadParams,
) -> Result<FinalizeUploadResult, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let path_manager = ObjectPathManager::new();
    let _object_lock = index::acquire_object_operation_lock(&fragment, &object_key).await?;

    if let Ok(existing) = index::read_object_version(&fragment, &object_key, &version_id).await {
        return Ok(FinalizeUploadResult::Complete(existing));
    }

    let manifest = read_manifest_any(&fragment, &version_id).await?;

    if let Some(m) = manifest.as_ref() {
        if m.session_id != session_id {
            return Err(AppError::Forbidden("Session id mismatch".into()));
        }
        if m.object_key != object_key {
            return Err(AppError::Forbidden("Object key mismatch".into()));
        }
    }

    let expected_parts = manifest
        .as_ref()
        .map(|m| m.expected_parts)
        .unwrap_or_else(|| params.expected_parts.max(1));

    if expected_parts == 0 {
        return Err(AppError::BadRequest("expected_parts must be >= 1".into()));
    }

    let blob_roots = path_manager.get_storage_roots();
    let required_data_shards = DATA_SHARDS_ATOMIC.get().unwrap().load(Ordering::Relaxed) as usize;
    let mut found_data_roots = 0usize;

    for root in &blob_roots {
        let shard_dir_check =
            path_manager.get_shard_dir_on_disk(root, &fragment, &object_key, &version_id);
        if tokio_fs::try_exists(&shard_dir_check)
            .await
            .unwrap_or(false)
        {
            found_data_roots += 1;
        }
    }

    if found_data_roots < required_data_shards {
        error!(
            "Finalization aborted: data directories missing. Found {}/{}.",
            found_data_roots, required_data_shards
        );
        return Err(AppError::InternalServerError(
            "Upload session expired or data missing.".into(),
        ));
    }

    let mut present = BTreeSet::new();
    let mut parts: Vec<PartMetadata> = Vec::with_capacity(expected_parts as usize);

    if let Some(m) = manifest.as_ref() {
        let receipts = index::load_session_part_receipts(&fragment, &version_id, m).await?;
        for i in 0..expected_parts {
            let mut resolved = None;

            if let Some(receipt) = receipts
                .as_ref()
                .and_then(|items| items.get(i as usize))
                .and_then(|entry| entry.as_ref())
            {
                resolved = reconstruct_part_meta_from_receipt(&path_manager, m, receipt).await?;
            }

            if resolved.is_none() && m.part_size_bytes > 0 {
                resolved = reconstruct_part_meta_from_disk(&path_manager, m, i).await?;
            }

            if resolved.is_none() {
                resolved = read_part_meta_any(&fragment, &version_id, i).await;
            }

            if let Some(pm) = resolved {
                present.insert(i);
                parts.push(pm);
            }
        }
    } else {
        for i in 0..expected_parts {
            if let Some(pm) = read_part_meta_any(&fragment, &version_id, i).await {
                present.insert(i);
                parts.push(pm);
            }
        }
    }

    let mut missing = Vec::new();
    for i in 0..expected_parts {
        if !present.contains(&i) {
            missing.push(i);
        }
    }

    if !missing.is_empty() {
        return Ok(FinalizeUploadResult::Incomplete(
            FinalizeIncompleteResponse {
                uploaded_parts: present.into_iter().collect(),
                missing_parts: missing,
                expected_parts,
            },
        ));
    }

    parts.sort_by_key(|p| p.part_index);

    let mut total_size_calculated = 0u64;
    for part in &parts {
        total_size_calculated += part.size_bytes;
    }

    let stripe_size = manifest
        .as_ref()
        .map(|m| m.stripe_size_bytes)
        .unwrap_or_else(configured_block_size);
    let membership_epoch = manifest.as_ref().map(|m| m.membership_epoch).unwrap_or(0);
    let data_shards = manifest
        .as_ref()
        .map(|m| m.data_shards)
        .unwrap_or_else(|| configured_data_shards() as u16);
    let parity_shards = manifest
        .as_ref()
        .map(|m| m.parity_shards)
        .unwrap_or_else(|| configured_parity_shards() as u16);
    let (pool_id, pool_generation) = manifest
        .as_ref()
        .map(|manifest| (manifest.pool_id.clone(), manifest.pool_generation))
        .ok_or_else(|| {
            AppError::Conflict(
                "N2 3.4 cannot finalize an upload without its pinned server-pool identity".into(),
            )
        })?;

    let content_type = manifest
        .as_ref()
        .map(|m| m.content_type.clone())
        .or_else(|| params.content_type.clone())
        .unwrap_or_else(|| "application/octet-stream".to_string());

    let object_meta = ObjectMetadata {
        object_key: object_key.clone(),
        packed_ext_u64: manifest
            .as_ref()
            .map(|value| value.packed_ext_u64)
            .unwrap_or_else(|| crate::core::index_store::packed_extension_key(&object_key)),
        fragment: fragment.clone(),
        version_id: version_id.clone(),
        upload_session_id: session_id.clone(),
        owner_uid: fragment.clone(),
        content_type,
        total_size_bytes: total_size_calculated,
        created_at: manifest
            .as_ref()
            .map(|m| m.created_at)
            .unwrap_or_else(|| Utc::now().timestamp()),
        pool_id,
        pool_generation,
        membership_epoch,
        data_shards,
        parity_shards,
        parts,
        is_delete_marker: false,
        stripe_size_bytes: stripe_size,
    };

    index::publish_object(&object_meta).await?;

    let _ = index::remove_session(&fragment, &version_id, "finalized").await;

    Ok(FinalizeUploadResult::Complete(object_meta))
}

pub async fn cancel_upload_session(
    claims: Claims,
    AxumPath((fragment, version_id)): AxumPath<(String, String)>,
    Query(key_query): Query<ObjectKeyQuery>,
) -> Result<impl IntoResponse, AppError> {
    cancel_upload_session_direct(claims, fragment, version_id, key_query.object_key).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn cancel_upload_session_direct(
    claims: Claims,
    fragment: String,
    version_id: String,
    object_key_hint: String,
) -> Result<(), AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let path_manager = ObjectPathManager::new();
    let session = index::load_session(&fragment, &version_id).await?;
    let object_key = session
        .as_ref()
        .map(|state| state.manifest.object_key.clone())
        .filter(|key| !key.is_empty())
        .unwrap_or_else(|| {
            if object_key_hint.is_empty() {
                version_id.clone()
            } else {
                object_key_hint
            }
        });
    let _object_lock = index::acquire_object_operation_lock(&fragment, &object_key).await?;

    if index::read_object_version(&fragment, &object_key, &version_id)
        .await
        .is_ok()
    {
        return Err(AppError::Conflict(
            "Cannot cancel: Object is already finalized.".to_string(),
        ));
    }

    cleanup_blob_tmp_attempt_files_for_version(&path_manager, &fragment, &version_id).await;
    let _ = index::remove_session(&fragment, &version_id, "cancelled").await;
    cleanup_blob_version_dirs_for_version(&path_manager, &fragment, &version_id).await;

    Ok(())
}
