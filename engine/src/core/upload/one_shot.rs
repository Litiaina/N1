use super::*;

async fn upload_object_one_shot_inner(
    State(_state): State<Arc<AppState>>,
    claims: Claims,
    headers: HeaderMap,
    AxumPath((fragment, object_key)): AxumPath<(String, String)>,
    Query(query): Query<UploadPartQuery>,
    body: Body,
    acquire_io_gate: bool,
) -> Result<Response, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let durability_mode = parse_upload_durability_mode(&query);
    let strict_durability = durability_mode.strict();
    let content_length = request_content_length(&headers)?;
    let gate_context = format!("fragment={}, object_key={}", fragment, object_key);
    let _io_guard = if acquire_io_gate {
        acquire_upload_io_guard_for_size(query.io_gate, &fragment, &gate_context, content_length)
            .await?
    } else {
        None
    };
    let max_part_size = MAX_PART_SIZE_ATOMIC.get().unwrap().load(Ordering::Relaxed);
    if content_length > max_part_size {
        return Ok((
            StatusCode::PAYLOAD_TOO_LARGE,
            "Object too large for one-shot upload",
        )
            .into_response());
    }

    preflight_upload_capacity(content_length).await?;
    let stripe_size = choose_upload_stripe_size_for_len(content_length);

    let version_id = Ulid::new().to_string();
    let session_id = Ulid::new().to_string();
    let expected_checksum = query
        .checksum
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let content_type =
        request_content_type(&headers).unwrap_or_else(|| "application/octet-stream".to_string());
    let (pool_id, pool_generation) = current_pool_identity()?;

    let path_manager = ObjectPathManager::new();
    let created_at = Utc::now().timestamp();
    let part_meta = match write_part_blob_to_disk(
        &path_manager,
        &fragment,
        &object_key,
        &version_id,
        0,
        content_length,
        configured_data_shards(),
        configured_parity_shards(),
        stripe_size,
        body,
        expected_checksum,
        strict_durability,
    )
    .await
    {
        Ok(part_meta) => part_meta,
        Err(err) => {
            cleanup_blob_version_dirs_for_version(&path_manager, &fragment, &version_id).await;
            return Err(err);
        }
    };

    let object_meta = ObjectMetadata {
        object_key: object_key.clone(),
        packed_ext_u64: crate::core::index_store::packed_extension_key(&object_key),
        fragment: fragment.clone(),
        version_id: version_id.clone(),
        upload_session_id: session_id,
        owner_uid: fragment.clone(),
        content_type,
        total_size_bytes: content_length,
        created_at,
        pool_id,
        pool_generation,
        membership_epoch: 0,
        data_shards: configured_data_shards() as u16,
        parity_shards: configured_parity_shards() as u16,
        parts: vec![part_meta],
        is_delete_marker: false,
        stripe_size_bytes: stripe_size,
    };

    let _object_lock = index::acquire_object_operation_lock(&fragment, &object_key).await?;
    if let Err(err) = index::publish_object(&object_meta).await {
        cleanup_blob_version_dirs_for_version(&path_manager, &fragment, &version_id).await;
        return Err(err);
    }

    let response_body = serde_json::to_value(object_meta.clone().without_internal_layout())
        .map_err(|e| {
            AppError::InternalServerError(format!("serialize one-shot upload response: {}", e))
        })?;
    Ok((StatusCode::CREATED, AxumJson(response_body)).into_response())
}

pub async fn upload_object_one_shot(
    state: State<Arc<AppState>>,
    claims: Claims,
    headers: HeaderMap,
    path: AxumPath<(String, String)>,
    query: Query<UploadPartQuery>,
    body: Body,
) -> Result<impl IntoResponse, AppError> {
    upload_object_one_shot_inner(state, claims, headers, path, query, body, true).await
}

pub async fn upload_object_one_shot_direct(
    claims: Claims,
    fragment: String,
    object_key: String,
    query: UploadPartQuery,
    content_type: Option<String>,
    body: Vec<u8>,
    acquire_io_gate: bool,
) -> Result<ObjectMetadata, AppError> {
    let content_length = body.len() as u64;
    upload_object_one_shot_body_direct(
        claims,
        fragment,
        object_key,
        query,
        content_type,
        Body::from(body),
        content_length,
        acquire_io_gate,
    )
    .await
}

pub async fn upload_object_one_shot_body_direct(
    claims: Claims,
    fragment: String,
    object_key: String,
    query: UploadPartQuery,
    content_type: Option<String>,
    body: Body,
    content_length: u64,
    acquire_io_gate: bool,
) -> Result<ObjectMetadata, AppError> {
    let body_stream = body.into_data_stream().map(|chunk_result| {
        chunk_result.map_err(|e| {
            AppError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e),
                "stream read".into(),
            )
        })
    });
    upload_object_one_shot_stream_direct(
        claims,
        fragment,
        object_key,
        query,
        content_type,
        body_stream,
        content_length,
        acquire_io_gate,
    )
    .await
}

pub async fn upload_object_one_shot_stream_direct<S>(
    claims: Claims,
    fragment: String,
    object_key: String,
    query: UploadPartQuery,
    content_type: Option<String>,
    body_stream: S,
    content_length: u64,
    acquire_io_gate: bool,
) -> Result<ObjectMetadata, AppError>
where
    S: Stream<Item = Result<Bytes, AppError>> + Unpin,
{
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let durability_mode = parse_upload_durability_mode(&query);
    let strict_durability = durability_mode.strict();
    let gate_context = format!("fragment={}, object_key={}", fragment, object_key);
    let _io_guard = if acquire_io_gate {
        acquire_upload_io_guard_for_size(query.io_gate, &fragment, &gate_context, content_length)
            .await?
    } else {
        None
    };
    let max_part_size = MAX_PART_SIZE_ATOMIC.get().unwrap().load(Ordering::Relaxed);
    if content_length > max_part_size {
        return Err(AppError::PayloadTooLarge(
            "Object too large for one-shot upload".into(),
        ));
    }

    preflight_upload_capacity(content_length).await?;
    let stripe_size = choose_upload_stripe_size_for_len(content_length);

    let version_id = Ulid::new().to_string();
    let session_id = Ulid::new().to_string();
    let expected_checksum = query
        .checksum
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let content_type = content_type.unwrap_or_else(|| "application/octet-stream".to_string());
    let (pool_id, pool_generation) = current_pool_identity()?;

    let path_manager = ObjectPathManager::new();
    let created_at = Utc::now().timestamp();
    let part_meta = match write_part_blob_to_disk_from_stream(
        &path_manager,
        &fragment,
        &object_key,
        &version_id,
        0,
        content_length,
        configured_data_shards(),
        configured_parity_shards(),
        stripe_size,
        body_stream,
        expected_checksum,
        strict_durability,
    )
    .await
    {
        Ok(part_meta) => part_meta,
        Err(err) => {
            cleanup_blob_version_dirs_for_version(&path_manager, &fragment, &version_id).await;
            return Err(err);
        }
    };

    let object_meta = ObjectMetadata {
        object_key: object_key.clone(),
        packed_ext_u64: crate::core::index_store::packed_extension_key(&object_key),
        fragment: fragment.clone(),
        version_id: version_id.clone(),
        upload_session_id: session_id,
        owner_uid: fragment.clone(),
        content_type,
        total_size_bytes: content_length,
        created_at,
        pool_id,
        pool_generation,
        membership_epoch: 0,
        data_shards: configured_data_shards() as u16,
        parity_shards: configured_parity_shards() as u16,
        parts: vec![part_meta],
        is_delete_marker: false,
        stripe_size_bytes: stripe_size,
    };

    let _object_lock = index::acquire_object_operation_lock(&fragment, &object_key).await?;
    if let Err(err) = index::publish_object(&object_meta).await {
        cleanup_blob_version_dirs_for_version(&path_manager, &fragment, &version_id).await;
        return Err(err);
    }

    Ok(object_meta.without_internal_layout())
}

pub async fn upload_object_one_shot_reader_direct<R>(
    claims: Claims,
    fragment: String,
    object_key: String,
    query: UploadPartQuery,
    content_type: Option<String>,
    reader: &mut R,
    content_length: u64,
    acquire_io_gate: bool,
) -> Result<ObjectMetadata, AppError>
where
    R: AsyncRead + Unpin + Send,
{
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let durability_mode = parse_upload_durability_mode(&query);
    let strict_durability = durability_mode.strict();
    let gate_context = format!("fragment={}, object_key={}", fragment, object_key);
    let _io_guard = if acquire_io_gate {
        acquire_upload_io_guard_for_size(query.io_gate, &fragment, &gate_context, content_length)
            .await?
    } else {
        None
    };
    let max_part_size = MAX_PART_SIZE_ATOMIC.get().unwrap().load(Ordering::Relaxed);
    if content_length > max_part_size {
        return Err(AppError::PayloadTooLarge(
            "Object too large for one-shot upload".into(),
        ));
    }

    preflight_upload_capacity(content_length).await?;
    let stripe_size = choose_upload_stripe_size_for_len(content_length);

    let version_id = Ulid::new().to_string();
    let session_id = Ulid::new().to_string();
    let expected_checksum = query
        .checksum
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let content_type = content_type.unwrap_or_else(|| "application/octet-stream".to_string());
    let (pool_id, pool_generation) = current_pool_identity()?;

    let path_manager = ObjectPathManager::new();
    let created_at = Utc::now().timestamp();
    let part_meta = match write_part_blob_to_disk_from_reader(
        &path_manager,
        &fragment,
        &object_key,
        &version_id,
        0,
        content_length,
        configured_data_shards(),
        configured_parity_shards(),
        stripe_size,
        reader,
        expected_checksum,
        strict_durability,
    )
    .await
    {
        Ok(part_meta) => part_meta,
        Err(err) => {
            cleanup_blob_version_dirs_for_version(&path_manager, &fragment, &version_id).await;
            return Err(err);
        }
    };

    let object_meta = ObjectMetadata {
        object_key: object_key.clone(),
        packed_ext_u64: crate::core::index_store::packed_extension_key(&object_key),
        fragment: fragment.clone(),
        version_id: version_id.clone(),
        upload_session_id: session_id,
        owner_uid: fragment.clone(),
        content_type,
        total_size_bytes: content_length,
        created_at,
        pool_id,
        pool_generation,
        membership_epoch: 0,
        data_shards: configured_data_shards() as u16,
        parity_shards: configured_parity_shards() as u16,
        parts: vec![part_meta],
        is_delete_marker: false,
        stripe_size_bytes: stripe_size,
    };

    let _object_lock = index::acquire_object_operation_lock(&fragment, &object_key).await?;
    if let Err(err) = index::publish_object(&object_meta).await {
        cleanup_blob_version_dirs_for_version(&path_manager, &fragment, &version_id).await;
        return Err(err);
    }

    Ok(object_meta.without_internal_layout())
}
