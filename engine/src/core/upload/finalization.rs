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
    _object_key_hint: String,
) -> Result<(), AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let path_manager = ObjectPathManager::new();
    let session = index::load_session(&fragment, &version_id)
        .await?
        .ok_or_else(|| {
            AppError::BadRequest("Upload session not found; no payload removed".into())
        })?;
    let object_key = session.manifest.object_key.clone();
    validate_cancel_payload_identity(&fragment, &object_key, &version_id)?;
    let _object_lock = index::acquire_object_operation_lock(&fragment, &object_key).await?;

    if let Some(runtime) = crate::distribution::membership::try_active_cluster_runtime() {
        let identity = crate::distribution::upload_lifecycle::UploadIdentity::from_manifest(
            &runtime,
            &session.manifest,
        );
        crate::distribution::upload_ownership::abort_and_reclaim(&runtime, &identity).await?;
        index::remove_session(&fragment, &version_id, "cancelled").await?;
        return Ok(());
    }

    // Lookup failure is not absence. In particular, the ordinary distributed
    // resolver can establish visibility from a subset of metadata holders but
    // cannot fence a concurrent finalizer. Until holder-side upload ownership
    // is implemented, cancellation must retain the manifest and peer payload
    // rather than acknowledge a destructive, unproven abort.
    cancel_lookup_allows_cleanup(
        index::read_object_version(&fragment, &object_key, &version_id).await,
        crate::distribution::membership::try_active_cluster_runtime().is_some(),
    )?;
    let current = index::load_session(&fragment, &version_id)
        .await?
        .ok_or_else(|| AppError::Conflict("Upload session changed during cancellation".into()))?;
    if current.manifest.session_id != session.manifest.session_id
        || current.manifest.object_key != object_key
    {
        return Err(AppError::Conflict(
            "Upload ownership changed during cancellation".into(),
        ));
    }

    cleanup_cancelled_local_payload(&path_manager, &fragment, &object_key, &version_id).await?;
    // Retain retry/ownership evidence until every local deletion and its parent
    // durability barrier succeeded. This is local cleanup, not a distributed
    // peer-payload reclamation acknowledgement.
    index::remove_session(&fragment, &version_id, "cancelled").await?;

    Ok(())
}

fn cancel_lookup_allows_cleanup(
    lookup: Result<ObjectMetadata, AppError>,
    distributed: bool,
) -> Result<(), AppError> {
    match lookup {
        Ok(_) => Err(AppError::Conflict(
            "Cannot cancel: Object is already finalized.".into(),
        )),
        Err(AppError::NotFound) if !distributed => Ok(()),
        Err(AppError::NotFound) => Err(AppError::ServiceUnavailable(
            "Distributed upload cancellation is deferred: holder-side ownership and finalization fencing are not yet proven; the session and payload were retained".into(),
        )),
        Err(error) => Err(error),
    }
}

fn validate_cancel_payload_identity(
    fragment: &str,
    object_id: &str,
    version_id: &str,
) -> Result<(), AppError> {
    let stable_fragment = fragment.strip_prefix("@f_").unwrap_or(fragment);
    let component = |value: &str| {
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    };
    if !component(stable_fragment)
        || crate::core::index_store::normalize_object_id(object_id)? != object_id
        || version_id.len() != 26
        || Ulid::from_string(version_id).is_err()
    {
        return Err(AppError::BadRequest(
            "Invalid upload payload identity; no payload removed".into(),
        ));
    }
    Ok(())
}

async fn cleanup_cancelled_local_payload(
    path_manager: &ObjectPathManager,
    fragment: &str,
    object_id: &str,
    version_id: &str,
) -> Result<(), AppError> {
    validate_cancel_payload_identity(fragment, object_id, version_id)?;
    let roots = path_manager.get_storage_roots();
    if roots.is_empty() {
        return Err(AppError::ServiceUnavailable(
            "No local upload holders are available for cancellation".into(),
        ));
    }
    for root in roots {
        let root_meta = tokio_fs::metadata(&root)
            .await
            .map_err(|error| AppError::Io(error, "inspect cancellation storage root".into()))?;
        if !root_meta.is_dir() {
            return Err(AppError::Conflict(
                "Cancellation storage root is not a directory".into(),
            ));
        }
        let target = path_manager.get_shard_dir_on_disk(&root, fragment, object_id, version_id);
        let relative = target.strip_prefix(&root).map_err(|_| {
            AppError::Conflict("Cancellation payload escaped its storage root".into())
        })?;
        let mut current = root.clone();
        let mut absent = false;
        for component in relative.components() {
            if !matches!(component, std::path::Component::Normal(_)) {
                return Err(AppError::Conflict(
                    "Invalid cancellation payload path".into(),
                ));
            }
            current.push(component);
            match tokio_fs::symlink_metadata(&current).await {
                Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
                Ok(_) => {
                    return Err(AppError::Conflict(format!(
                        "Cancellation payload path is not a real directory: {}",
                        current.display()
                    )));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    absent = true;
                    break;
                }
                Err(error) => {
                    return Err(AppError::Io(
                        error,
                        "inspect exact cancellation path".into(),
                    ));
                }
            }
        }
        if absent {
            continue;
        }
        // The known identity is enough to address this one version directly;
        // never traverse other objects looking for a matching version name.
        tokio_fs::remove_dir_all(&target)
            .await
            .map_err(|error| AppError::Io(error, "remove exact cancelled local payload".into()))?;
        if let Some(parent) = target.parent() {
            sync_dir(parent).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "finalization/tests.rs"]
mod tests;
