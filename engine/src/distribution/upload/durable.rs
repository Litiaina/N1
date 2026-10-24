use super::*;

pub async fn put_cluster_part<S>(
    claims: Claims,
    fragment: String,
    version_id: String,
    part_index: u64,
    query: UploadPartQuery,
    body_stream: S,
    content_length: u64,
) -> Result<(), AppError>
where
    S: Stream<Item = Result<Bytes, AppError>>,
{
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    let state = index::load_session(&fragment, &version_id)
        .await?
        .ok_or_else(|| AppError::BadRequest("Upload session not found".into()))?;
    if state.part_receipts.contains_key(&part_index) || state.parts.contains_key(&part_index) {
        return Ok(());
    }
    if part_index >= state.manifest.expected_parts {
        return Err(AppError::BadRequest("Part index out of range".into()));
    }
    let expected = expected_part_size(&state.manifest, part_index);
    if expected != content_length {
        return Err(AppError::BadRequest(
            "Part size does not match upload session manifest".into(),
        ));
    }
    let started = Instant::now();
    let part = write_cluster_part_from_stream(
        &fragment,
        &state.manifest.object_key,
        &version_id,
        part_index,
        content_length,
        usize::from(state.manifest.data_shards),
        usize::from(state.manifest.parity_shards),
        &state.manifest.pool_id,
        state.manifest.pool_generation,
        state.manifest.membership_epoch,
        state.manifest.stripe_size_bytes,
        body_stream,
        query.checksum.as_deref(),
        false,
    )
    .await;
    record_io_latency(
        "cluster.upload.part",
        started.elapsed(),
        content_length,
        part.is_ok(),
    );
    let part = part?;
    let session_commit_started = Instant::now();
    index::commit_session_part(&fragment, &version_id, &part.part).await?;
    index::commit_session_part_receipt(&fragment, &version_id, &state.manifest, &part.part).await?;
    record_io_latency(
        "cluster.upload.commit_part",
        session_commit_started.elapsed(),
        content_length,
        true,
    );
    Ok(())
}

pub async fn upload_cluster_one_shot<S>(
    claims: Claims,
    fragment: String,
    object_key: String,
    packed_ext_u64: u64,
    content_type: String,
    query: UploadPartQuery,
    body_stream: S,
    content_length: u64,
) -> Result<ObjectMetadata, AppError>
where
    S: Stream<Item = Result<Bytes, AppError>>,
{
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    validate_cluster_one_shot_size(content_length)?;
    // write_cluster_part_from_stream performs the authoritative participant
    // reachability check immediately before opening the exact shard writers.
    // Avoid probing the same fixed server-pool layout twice for one-shot I/O.
    let (runtime, layout) = configured_write_context()?;
    let write_pool = runtime
        .membership
        .newest_writable_pool()
        .expect("write context requires a writable pool");
    let version_id = Ulid::new().to_string();
    let session_id = Ulid::new().to_string();
    let stripe_size = choose_upload_stripe_size(content_length);
    let created_at = Utc::now().timestamp();
    crate::distribution::upload_ownership::register_upload(
        &runtime,
        &crate::distribution::upload_lifecycle::UploadIdentity {
            cluster_id: runtime.membership.cluster_id.clone(),
            pool_id: write_pool.id.clone(),
            pool_generation: write_pool.generation,
            fragment: fragment.clone(),
            object_id: object_key.clone(),
            version_id: version_id.clone(),
            session_id: session_id.clone(),
            data_shards: layout.data_shards as u16,
            parity_shards: layout.parity_shards as u16,
            created_at_unix: created_at,
        },
    )
    .await?;
    let started = Instant::now();
    let part = write_cluster_part_from_stream(
        &fragment,
        &object_key,
        &version_id,
        0,
        content_length,
        layout.data_shards,
        layout.parity_shards,
        &write_pool.id,
        write_pool.generation,
        runtime.membership.epoch,
        stripe_size,
        body_stream,
        query.checksum.as_deref(),
        false,
    )
    .await;
    record_io_latency(
        "cluster.upload.one_shot",
        started.elapsed(),
        content_length,
        part.is_ok(),
    );
    let part = part?;
    let object_meta = ObjectMetadata {
        object_key: object_key.clone(),
        packed_ext_u64,
        fragment: fragment.clone(),
        version_id: version_id.clone(),
        upload_session_id: session_id,
        owner_uid: fragment.clone(),
        content_type,
        total_size_bytes: content_length,
        created_at,
        pool_id: part.pool_id.clone(),
        pool_generation: part.pool_generation,
        membership_epoch: part.membership_epoch,
        data_shards: part.data_shards,
        parity_shards: part.parity_shards,
        parts: vec![part.part],
        is_delete_marker: false,
        stripe_size_bytes: stripe_size,
    };
    publish_cluster_object_metadata(&object_meta).await?;
    Ok(object_meta)
}

pub async fn finalize_cluster_upload(
    claims: Claims,
    fragment: String,
    version_id: String,
    session_id: String,
    _object_key: String,
    params: InitiateUploadParams,
) -> Result<FinalizeUploadResult, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    let state = index::load_session(&fragment, &version_id)
        .await?
        .ok_or_else(|| AppError::BadRequest("Upload session not found".into()))?;
    if state.manifest.session_id != session_id {
        return Err(AppError::Forbidden("Session id mismatch".into()));
    }
    let mut parts = Vec::new();
    let mut missing = Vec::new();
    for part_index in 0..state.manifest.expected_parts {
        if let Some(part) = state.parts.get(&part_index) {
            parts.push(part.clone());
        } else {
            missing.push(part_index);
        }
    }
    if !missing.is_empty() {
        return Ok(FinalizeUploadResult::Incomplete(
            crate::core::disk_manager::FinalizeIncompleteResponse {
                uploaded_parts: parts.iter().map(|part| part.part_index).collect(),
                missing_parts: missing,
                expected_parts: state.manifest.expected_parts,
            },
        ));
    }
    parts.sort_by_key(|part| part.part_index);
    let total_size_bytes = parts.iter().map(|part| part.size_bytes).sum();
    let object_meta = ObjectMetadata {
        // The immutable upload manifest owns the physical object identity.
        // Using it here makes a finalize retry independent of the temporary
        // logical-path binding once NCDX has already been published.
        object_key: state.manifest.object_key.clone(),
        packed_ext_u64: state.manifest.packed_ext_u64,
        fragment: fragment.clone(),
        version_id: version_id.clone(),
        upload_session_id: session_id,
        owner_uid: fragment.clone(),
        content_type: state.manifest.content_type.clone().if_empty(
            params
                .content_type
                .unwrap_or_else(|| "application/octet-stream".into()),
        ),
        total_size_bytes,
        created_at: state.manifest.created_at,
        pool_id: state.manifest.pool_id.clone(),
        pool_generation: state.manifest.pool_generation,
        membership_epoch: state.manifest.membership_epoch,
        data_shards: state.manifest.data_shards,
        parity_shards: state.manifest.parity_shards,
        parts,
        is_delete_marker: false,
        stripe_size_bytes: state.manifest.stripe_size_bytes,
    };
    let ncdx_started = Instant::now();
    let publication = publish_cluster_object_metadata(&object_meta).await;
    record_io_latency(
        "cluster.upload.publish_ncdx",
        ncdx_started.elapsed(),
        total_size_bytes,
        publication.is_ok(),
    );
    publication?;
    Ok(FinalizeUploadResult::Complete(object_meta))
}
