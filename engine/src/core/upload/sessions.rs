//! Durable and high-performance multipart session workflows.

use super::*;

pub async fn initiate_object_upload(
    claims: Claims,
    AxumPath((fragment, object_key)): AxumPath<(String, String)>,
    AxumJson(params): AxumJson<InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    let response = initiate_object_upload_direct(claims, fragment, object_key, params).await?;
    Ok((StatusCode::CREATED, AxumJson(response)))
}

async fn prepare_upload_manifest(
    fragment: &str,
    object_key: &str,
    params: &InitiateUploadParams,
) -> Result<UploadSessionManifest, AppError> {
    if params.expected_parts == 0 {
        return Err(AppError::BadRequest("expected_parts must be >= 1".into()));
    }

    let part_size_bytes = if params.expected_parts <= 1 {
        params.part_size_bytes.unwrap_or(params.total_size_bytes)
    } else {
        params
            .part_size_bytes
            .filter(|value| *value > 0)
            .ok_or(AppError::BadRequest(
                "part_size_bytes is required for multipart uploads".into(),
            ))?
    };

    let computed_parts = if params.total_size_bytes == 0 || part_size_bytes == 0 {
        1
    } else {
        params.total_size_bytes.div_ceil(part_size_bytes)
    };
    if computed_parts != params.expected_parts {
        return Err(AppError::BadRequest(
            "expected_parts does not match total_size_bytes and part_size_bytes".into(),
        ));
    }

    usize::try_from(params.expected_parts)
        .map_err(|_| AppError::BadRequest("expected_parts is too large".into()))?;

    let version_id = Ulid::new().to_string();
    let session_id = Ulid::new().to_string();
    let planned_sizes = planned_part_sizes(
        params.total_size_bytes,
        params.expected_parts,
        part_size_bytes,
    );
    let stripe_size = choose_upload_stripe_size_for_parts(&planned_sizes);
    let data_shards = configured_data_shards();
    let parity_shards = configured_parity_shards();

    let manifest = UploadSessionManifest {
        fragment: fragment.to_string(),
        object_key: object_key.to_string(),
        version_id: version_id.clone(),
        session_id,
        total_size_bytes: params.total_size_bytes,
        expected_parts: params.expected_parts,
        part_size_bytes,
        content_type: params
            .content_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".to_string()),
        created_at: Utc::now().timestamp(),
        stripe_size_bytes: stripe_size,
        data_shards: data_shards as u16,
        parity_shards: parity_shards as u16,
        placement_roots: ObjectPathManager::new()
            .select_blob_roots_for_object(
                fragment,
                object_key,
                &version_id,
                data_shards.saturating_add(parity_shards),
            )?
            .into_iter()
            .map(|root| root.to_string_lossy().to_string())
            .collect(),
    };

    ensure_registered_placement_roots(
        &manifest
            .placement_roots
            .iter()
            .map(PathBuf::from)
            .collect::<Vec<_>>(),
    )?;
    Ok(manifest)
}

pub async fn initiate_object_upload_direct(
    claims: Claims,
    fragment: String,
    object_key: String,
    params: InitiateUploadParams,
) -> Result<InitiateUploadResponse, AppError> {
    ensure_user_mutations_allowed()?;
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let manifest = prepare_upload_manifest(&fragment, &object_key, &params).await?;
    let _upload_activity = UploadActivityGuard::begin(&fragment, &object_key)?;
    let version_id = manifest.version_id.clone();
    let session_id = manifest.session_id.clone();

    let path_manager = ObjectPathManager::new();
    ensure_version_shard_dirs(&path_manager, &manifest).await?;

    index::open_session(&manifest, None).await?;

    info!(
        "Initiated upload: {}/{} (v: {}, s: {})",
        fragment, object_key, version_id, session_id
    );

    Ok(InitiateUploadResponse {
        session_id,
        version_id,
    })
}

fn standard_request_matches(
    session: &StandardUploadSession,
    logical_path: &str,
    params: &InitiateUploadParams,
) -> bool {
    let manifest = &session.manifest;
    let content_type_matches = params
        .content_type
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|requested| requested == manifest.content_type)
        .unwrap_or(true);
    let part_size_matches = params
        .part_size_bytes
        .filter(|value| *value > 0)
        .map(|requested| requested == manifest.part_size_bytes)
        .unwrap_or(true);

    session.lifecycle.load(Ordering::Acquire) == STANDARD_SESSION_OPEN
        && session.logical_path == logical_path.trim_start_matches('/')
        && manifest.total_size_bytes == params.total_size_bytes
        && manifest.expected_parts == params.expected_parts
        && part_size_matches
        && content_type_matches
}

pub fn find_reusable_standard_upload(
    fragment: &str,
    logical_path: &str,
    params: &InitiateUploadParams,
) -> Result<Option<InitiateUploadResponse>, AppError> {
    let registry = standard_registry_read()?;
    Ok(registry
        .values()
        .filter(|session| session.manifest.fragment == fragment)
        .filter(|session| standard_request_matches(session, logical_path, params))
        .max_by_key(|session| session.manifest.created_at)
        .map(|session| InitiateUploadResponse {
            session_id: session.manifest.session_id.clone(),
            version_id: session.manifest.version_id.clone(),
        }))
}

/// Starts the standard multipart upload entirely in memory. The first durable
/// write is the first part's EC shard stream; no manifest, namespace binding,
/// upload directory, or receipt is written here.
pub async fn initiate_standard_upload_direct(
    claims: Claims,
    fragment: String,
    physical_object_key: String,
    logical_path: String,
    params: InitiateUploadParams,
) -> Result<InitiateUploadResponse, AppError> {
    ensure_user_mutations_allowed()?;
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    if let Some(existing) = find_reusable_standard_upload(&fragment, &logical_path, &params)? {
        return Ok(existing);
    }

    let manifest = prepare_upload_manifest(&fragment, &physical_object_key, &params).await?;
    let _upload_activity = UploadActivityGuard::begin(&fragment, &physical_object_key)?;
    let expected_parts = usize::try_from(manifest.expected_parts)
        .map_err(|_| AppError::BadRequest("expected_parts is too large".into()))?;
    let mut parts = Vec::new();
    parts
        .try_reserve_exact(expected_parts)
        .map_err(|_| AppError::BadRequest("expected_parts is too large".into()))?;
    parts.resize_with(expected_parts, StandardPartSlot::new);

    let response = InitiateUploadResponse {
        session_id: manifest.session_id.clone(),
        version_id: manifest.version_id.clone(),
    };
    let normalized_logical = logical_path.trim_start_matches('/').to_string();
    let session = Arc::new(StandardUploadSession {
        manifest,
        logical_path: normalized_logical,
        lifecycle: AtomicU8::new(STANDARD_SESSION_OPEN),
        in_flight_parts: AtomicUsize::new(0),
        last_activity_at: AtomicI64::new(Utc::now().timestamp()),
        directories_ready: tokio::sync::OnceCell::new(),
        parts: parts.into_boxed_slice(),
    });

    let mut registry = standard_registry_write()?;
    if let Some(existing) = registry
        .values()
        .filter(|existing| existing.manifest.fragment == fragment)
        .find(|existing| standard_request_matches(existing, &logical_path, &params))
    {
        return Ok(InitiateUploadResponse {
            session_id: existing.manifest.session_id.clone(),
            version_id: existing.manifest.version_id.clone(),
        });
    }
    registry.insert(
        standard_session_key(&fragment, &response.version_id),
        session,
    );
    Ok(response)
}

fn standard_part_record(metadata: &PartMetadata) -> Result<StandardPartRecord, AppError> {
    let checksum = metadata
        .checksum
        .as_deref()
        .map(|value| u32::from_str_radix(value, 16))
        .transpose()
        .map_err(|_| AppError::InternalServerError("invalid part CRC32".to_string()))?;
    let shard_checksums = metadata
        .shards
        .iter()
        .map(|shard| u32::from_str_radix(&shard.checksum, 16))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| AppError::InternalServerError("invalid shard CRC32".to_string()))?
        .into_boxed_slice();
    Ok(StandardPartRecord {
        size_bytes: metadata.size_bytes,
        created_at: metadata.created_at,
        checksum,
        shard_checksums,
    })
}

fn standard_part_metadata(
    path_manager: &ObjectPathManager,
    session: &StandardUploadSession,
    part_index: u64,
) -> Result<PartMetadata, AppError> {
    let slot = session
        .parts
        .get(part_index as usize)
        .ok_or_else(|| AppError::BadRequest("Part index out of range".into()))?;
    let record = slot
        .record
        .get()
        .ok_or_else(|| AppError::Conflict(format!("Multipart part {part_index} is incomplete")))?;
    let expected_shard_len = final_shard_len_for_part(
        record.size_bytes,
        manifest_data_shards(&session.manifest),
        session.manifest.stripe_size_bytes,
    );
    let specs = shard_specs_for_part(path_manager, &session.manifest, part_index)?;
    if specs.len() != record.shard_checksums.len() {
        return Err(AppError::InternalServerError(
            "standard multipart shard record is incomplete".to_string(),
        ));
    }
    let shards = specs
        .into_iter()
        .zip(record.shard_checksums.iter())
        .map(
            |((shard_index, is_parity, absolute_path, file_name), checksum)| ShardInfo {
                shard_index,
                file_name,
                absolute_path: absolute_path.to_string_lossy().to_string(),
                size_bytes: expected_shard_len,
                checksum: format_crc32_u32(*checksum),
                is_parity,
            },
        )
        .collect();
    Ok(PartMetadata {
        part_index,
        size_bytes: record.size_bytes,
        created_at: record.created_at,
        shards,
        checksum: record.checksum.map(format_crc32_u32),
    })
}

pub(super) enum StandardPartClaimResult<'a> {
    Claimed(StandardPartClaim<'a>),
    AlreadyComplete,
}

pub(super) fn claim_standard_part(
    session: &StandardUploadSession,
    part_index: u64,
) -> Result<StandardPartClaimResult<'_>, AppError> {
    if session.lifecycle.load(Ordering::Acquire) != STANDARD_SESSION_OPEN {
        return Err(AppError::Conflict(
            "Multipart session is not accepting parts".to_string(),
        ));
    }
    let slot = session
        .parts
        .get(part_index as usize)
        .ok_or_else(|| AppError::BadRequest("Part index out of range".into()))?;
    match slot.state.compare_exchange(
        STANDARD_PART_EMPTY,
        STANDARD_PART_UPLOADING,
        Ordering::AcqRel,
        Ordering::Acquire,
    ) {
        Ok(_) => {
            session.in_flight_parts.fetch_add(1, Ordering::AcqRel);
            session
                .last_activity_at
                .store(Utc::now().timestamp(), Ordering::Release);
            if session.lifecycle.load(Ordering::Acquire) != STANDARD_SESSION_OPEN {
                slot.state.store(STANDARD_PART_EMPTY, Ordering::Release);
                session.in_flight_parts.fetch_sub(1, Ordering::AcqRel);
                return Err(AppError::Conflict(
                    "Multipart session is finalizing".to_string(),
                ));
            }
            Ok(StandardPartClaimResult::Claimed(StandardPartClaim {
                session,
                slot,
                completed: false,
            }))
        }
        Err(STANDARD_PART_COMPLETE) => Ok(StandardPartClaimResult::AlreadyComplete),
        Err(STANDARD_PART_UPLOADING) => Err(AppError::Conflict(
            "Multipart part is already uploading".to_string(),
        )),
        Err(_) => Err(AppError::Conflict(
            "Multipart part is unavailable".to_string(),
        )),
    }
}

fn validate_standard_part_request(
    session: &StandardUploadSession,
    part_index: u64,
    content_length: u64,
) -> Result<(), AppError> {
    let expected = expected_part_size_bytes(&session.manifest, part_index)
        .ok_or_else(|| AppError::BadRequest("Part index out of range".into()))?;
    if content_length != expected {
        return Err(AppError::BadRequest(format!(
            "Part size does not match upload session: got {content_length}, expected {expected}"
        )));
    }
    let max_part_size = MAX_PART_SIZE_ATOMIC.get().unwrap().load(Ordering::Relaxed);
    if content_length > max_part_size {
        return Err(AppError::PayloadTooLarge("Part too large".into()));
    }
    Ok(())
}

async fn prepare_standard_part(session: &StandardUploadSession) -> Result<(), AppError> {
    // Every part in a version shares the same K+M parent directories.
    session
        .directories_ready
        .get_or_try_init(|| async {
            ensure_version_shard_dirs(&ObjectPathManager::new(), &session.manifest).await
        })
        .await
        .map(|_| ())
}

pub async fn object_upload_part_standard_body_direct(
    claims: Claims,
    fragment: String,
    version_id: String,
    part_index: u64,
    query: UploadPartQuery,
    body: Body,
    content_length: u64,
) -> Result<(), AppError> {
    ensure_user_mutations_allowed()?;
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    let session = standard_session(&fragment, &version_id)?
        .ok_or_else(|| AppError::BadRequest("Standard upload session not found".into()))?;
    validate_standard_part_request(&session, part_index, content_length)?;
    let _upload_activity = UploadActivityGuard::begin(&fragment, &session.manifest.object_key)?;
    let claim = match claim_standard_part(&session, part_index)? {
        StandardPartClaimResult::AlreadyComplete => return Ok(()),
        StandardPartClaimResult::Claimed(claim) => claim,
    };

    prepare_standard_part(&session).await?;
    let gate_context = format!(
        "standard fragment={}, version={}, part={}",
        fragment, version_id, part_index
    );
    let io_guard = acquire_upload_io_guard_for_size(
        Some(query.io_gate.unwrap_or(false)),
        &fragment,
        &gate_context,
        content_length,
    )
    .await?;
    let io_progress = io_guard.as_ref().map(AdaptiveIoGuard::progress_handle);
    let expected_checksum = query
        .checksum
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    let metadata = write_part_blob_to_disk(
        &ObjectPathManager::new(),
        &fragment,
        &session.manifest.object_key,
        &version_id,
        part_index,
        content_length,
        manifest_data_shards(&session.manifest),
        manifest_parity_shards(&session.manifest),
        session.manifest.stripe_size_bytes,
        Some(&session.manifest.placement_roots),
        body,
        expected_checksum,
        false,
        io_progress.as_ref(),
    )
    .await?;

    if session.lifecycle.load(Ordering::Acquire) == STANDARD_SESSION_CANCELLED {
        cleanup_files_best_effort(
            &metadata
                .shards
                .iter()
                .map(|shard| PathBuf::from(&shard.absolute_path))
                .collect::<Vec<_>>(),
        )
        .await;
        return Err(AppError::Conflict("Upload session was cancelled".into()));
    }
    claim.complete(standard_part_record(&metadata)?)?;
    if let Some(guard) = io_guard {
        guard.complete();
    }
    Ok(())
}

pub async fn object_upload_part_standard_reader_direct<R>(
    claims: Claims,
    fragment: String,
    version_id: String,
    part_index: u64,
    query: UploadPartQuery,
    reader: &mut R,
    content_length: u64,
) -> Result<(), AppError>
where
    R: AsyncRead + Unpin + Send,
{
    ensure_user_mutations_allowed()?;
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    let session = standard_session(&fragment, &version_id)?
        .ok_or_else(|| AppError::BadRequest("Standard upload session not found".into()))?;
    validate_standard_part_request(&session, part_index, content_length)?;
    let _upload_activity = UploadActivityGuard::begin(&fragment, &session.manifest.object_key)?;
    let claim = match claim_standard_part(&session, part_index)? {
        StandardPartClaimResult::AlreadyComplete => return Ok(()),
        StandardPartClaimResult::Claimed(claim) => claim,
    };
    prepare_standard_part(&session).await?;
    let gate_context = format!(
        "standard-native fragment={}, version={}, part={}",
        fragment, version_id, part_index
    );
    let io_guard = acquire_upload_io_guard_for_size(
        Some(query.io_gate.unwrap_or(false)),
        &fragment,
        &gate_context,
        content_length,
    )
    .await?;
    let io_progress = io_guard.as_ref().map(AdaptiveIoGuard::progress_handle);
    let expected_checksum = query
        .checksum
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let metadata = write_part_blob_to_disk_from_reader(
        &ObjectPathManager::new(),
        &fragment,
        &session.manifest.object_key,
        &version_id,
        part_index,
        content_length,
        manifest_data_shards(&session.manifest),
        manifest_parity_shards(&session.manifest),
        session.manifest.stripe_size_bytes,
        Some(&session.manifest.placement_roots),
        reader,
        expected_checksum,
        false,
        io_progress.as_ref(),
    )
    .await?;
    if session.lifecycle.load(Ordering::Acquire) == STANDARD_SESSION_CANCELLED {
        cleanup_files_best_effort(
            &metadata
                .shards
                .iter()
                .map(|shard| PathBuf::from(&shard.absolute_path))
                .collect::<Vec<_>>(),
        )
        .await;
        return Err(AppError::Conflict("Upload session was cancelled".into()));
    }
    claim.complete(standard_part_record(&metadata)?)?;
    if let Some(guard) = io_guard {
        guard.complete();
    }
    Ok(())
}

pub fn get_standard_upload_status_direct(
    claims: &Claims,
    fragment: &str,
    version_id: &str,
) -> Result<UploadStatusResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    let Some(session) = standard_session(fragment, version_id)? else {
        return Ok(UploadStatusResponse::default());
    };
    let uploaded_parts = session
        .parts
        .iter()
        .enumerate()
        .filter_map(|(index, slot)| {
            (slot.state.load(Ordering::Acquire) == STANDARD_PART_COMPLETE).then_some(index as u64)
        })
        .collect::<Vec<_>>();
    let committed_bytes = uploaded_parts
        .iter()
        .filter_map(|index| session.parts[*index as usize].record.get())
        .map(|record| record.size_bytes)
        .sum();
    Ok(UploadStatusResponse {
        committed_parts: uploaded_parts.len() as u64,
        committed_bytes,
        uploaded_parts,
        expected_parts: Some(session.manifest.expected_parts),
        total_size_bytes: Some(session.manifest.total_size_bytes),
        created_at: Some(session.manifest.created_at),
        session_id: Some(session.manifest.session_id.clone()),
        version_id: Some(session.manifest.version_id.clone()),
        object_key: Some(session.logical_path.clone()),
        physical_object_key: Some(session.manifest.object_key.clone()),
    })
}

pub fn list_standard_uploads_direct(
    claims: &Claims,
    fragment: &str,
    limit: usize,
    offset: usize,
) -> Result<ListUploadsResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    let registry = standard_registry_read()?;
    let mut sessions = registry
        .values()
        .filter(|session| session.manifest.fragment == fragment)
        .filter(|session| session.lifecycle.load(Ordering::Acquire) == STANDARD_SESSION_OPEN)
        .map(|session| {
            let committed_parts = session
                .parts
                .iter()
                .filter(|slot| slot.state.load(Ordering::Acquire) == STANDARD_PART_COMPLETE)
                .count() as u64;
            let committed_bytes = session
                .parts
                .iter()
                .filter_map(|slot| slot.record.get())
                .map(|record| record.size_bytes)
                .sum();
            ActiveSessionInfo {
                session_id: session.manifest.session_id.clone(),
                version_id: session.manifest.version_id.clone(),
                object_key: Some(session.logical_path.clone()),
                physical_object_key: Some(session.manifest.object_key.clone()),
                created_at: session.manifest.created_at,
                reserved_size: session.manifest.total_size_bytes,
                total_size_bytes: Some(session.manifest.total_size_bytes),
                expected_parts: Some(session.manifest.expected_parts),
                committed_parts,
                committed_bytes,
            }
        })
        .collect::<Vec<_>>();
    sessions.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    let total = sessions.len();
    Ok(ListUploadsResponse {
        sessions: sessions
            .into_iter()
            .skip(offset)
            .take(limit.min(100))
            .collect(),
        total,
    })
}

/// Dropping a request future is also a finalization exit. Reset only our busy
/// state, never a successfully finalized session, so retries can proceed.
struct StandardFinalizeGuard<'a>(&'a StandardUploadSession);

impl Drop for StandardFinalizeGuard<'_> {
    fn drop(&mut self) {
        let _ = self.0.lifecycle.compare_exchange(
            STANDARD_SESSION_FINALIZING,
            STANDARD_SESSION_OPEN,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
}

pub async fn finalize_standard_upload_direct(
    claims: Claims,
    fragment: String,
    version_id: String,
    session_id: String,
) -> Result<FinalizeUploadResult, AppError> {
    let finalize_started = Instant::now();
    ensure_user_mutations_allowed()?;
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    let session = standard_session(&fragment, &version_id)?
        .ok_or_else(|| AppError::BadRequest("Standard upload session not found".into()))?;
    if session.manifest.session_id != session_id {
        return Err(AppError::Forbidden("Session id mismatch".into()));
    }
    let physical_object_key = session.manifest.object_key.clone();
    let _upload_activity = UploadActivityGuard::begin(&fragment, &physical_object_key)?;
    if session.lifecycle.load(Ordering::Acquire) == STANDARD_SESSION_FINALIZED {
        return Ok(FinalizeUploadResult::Complete(
            index::read_object_version(&fragment, &physical_object_key, &version_id).await?,
        ));
    }
    session
        .lifecycle
        .compare_exchange(
            STANDARD_SESSION_OPEN,
            STANDARD_SESSION_FINALIZING,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .map_err(|_| AppError::Conflict("Multipart session is busy".to_string()))?;
    let _finalize_guard = StandardFinalizeGuard(&session);

    let present = session
        .parts
        .iter()
        .enumerate()
        .filter_map(|(index, slot)| {
            (slot.state.load(Ordering::Acquire) == STANDARD_PART_COMPLETE).then_some(index as u64)
        })
        .collect::<BTreeSet<_>>();
    let missing = (0..session.manifest.expected_parts)
        .filter(|index| !present.contains(index))
        .collect::<Vec<_>>();
    if !missing.is_empty() || session.in_flight_parts.load(Ordering::Acquire) != 0 {
        return Ok(FinalizeUploadResult::Incomplete(
            FinalizeIncompleteResponse {
                uploaded_parts: present.into_iter().collect(),
                missing_parts: missing,
                expected_parts: session.manifest.expected_parts,
            },
        ));
    }

    let path_manager = ObjectPathManager::new();
    let finalize_result = async {
        let mut parts = (0..session.manifest.expected_parts)
            .map(|index| standard_part_metadata(&path_manager, &session, index))
            .collect::<Result<Vec<_>, _>>()?;
        parts.sort_by_key(|part| part.part_index);
        let total_size_bytes = parts.iter().map(|part| part.size_bytes).sum();
        let metadata = ObjectMetadata {
            object_key: physical_object_key.clone(),
            fragment: fragment.clone(),
            version_id: version_id.clone(),
            upload_session_id: session_id.clone(),
            owner_uid: fragment.clone(),
            content_type: session.manifest.content_type.clone(),
            total_size_bytes,
            created_at: session.manifest.created_at,
            data_shards: session.manifest.data_shards,
            parity_shards: session.manifest.parity_shards,
            placement_generation: 0,
            parts,
            is_delete_marker: false,
            stripe_size_bytes: session.manifest.stripe_size_bytes,
        };
        let _object_lock =
            index::acquire_object_operation_lock(&fragment, &physical_object_key).await?;
        let publish_started = Instant::now();
        let publish_result =
            index::publish_object_with_durability(&metadata, index::IndexWriteDurability::Relaxed)
                .await;
        record_io_latency(
            "upload.standard.finalize.publish",
            publish_started.elapsed(),
            metadata.total_size_bytes,
            publish_result.is_ok(),
        );
        publish_result?;
        Ok::<_, AppError>(metadata)
    }
    .await;

    match finalize_result {
        Ok(metadata) => {
            record_io_latency(
                "upload.standard.finalize.total",
                finalize_started.elapsed(),
                metadata.total_size_bytes,
                true,
            );
            session
                .last_activity_at
                .store(Utc::now().timestamp(), Ordering::Release);
            session
                .lifecycle
                .store(STANDARD_SESSION_FINALIZED, Ordering::Release);
            Ok(FinalizeUploadResult::Complete(metadata))
        }
        Err(err) => {
            record_io_latency(
                "upload.standard.finalize.total",
                finalize_started.elapsed(),
                session.manifest.total_size_bytes,
                false,
            );
            Err(err)
        }
    }
}

pub fn complete_standard_upload(fragment: &str, version_id: &str) -> Result<(), AppError> {
    let key = standard_session_key(fragment, version_id);
    let mut registry = standard_registry_write()?;
    if registry.get(&key).is_some_and(|session| {
        session.lifecycle.load(Ordering::Acquire) == STANDARD_SESSION_FINALIZED
    }) {
        registry.remove(&key);
    }
    Ok(())
}

pub fn cancel_standard_upload_direct(
    claims: &Claims,
    fragment: &str,
    version_id: &str,
) -> Result<(), AppError> {
    ensure_user_mutations_allowed()?;
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    let key = standard_session_key(fragment, version_id);
    let mut registry = standard_registry_write()?;
    let session = registry
        .get(&key)
        .cloned()
        .ok_or_else(|| AppError::BadRequest("Standard upload session not found".into()))?;
    session
        .lifecycle
        .compare_exchange(
            STANDARD_SESSION_OPEN,
            STANDARD_SESSION_CANCELLED,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .map_err(|_| AppError::Conflict("Multipart session is busy".to_string()))?;
    registry.remove(&key);
    Ok(())
}

pub fn active_standard_upload_versions(fragment: &str) -> Result<HashSet<String>, AppError> {
    active_standard_upload_versions_at(
        fragment,
        Utc::now().timestamp(),
        i64::try_from(CONFIG.garbage_collector.file_ttl_seconds).unwrap_or(i64::MAX),
    )
}

pub(super) fn active_standard_upload_versions_at(
    fragment: &str,
    now: i64,
    ttl: i64,
) -> Result<HashSet<String>, AppError> {
    let mut registry = standard_registry_write()?;
    let protected_objects = registry
        .values()
        .filter(|session| session.manifest.fragment == fragment)
        .map(|session| {
            let object = &session.manifest.object_key;
            Ok((
                object.clone(),
                UploadActivityGuard::active(fragment, object)?,
            ))
        })
        .collect::<Result<HashMap<_, _>, AppError>>()?;
    registry.retain(|_, session| {
        if session.manifest.fragment != fragment {
            return true;
        }
        let lifecycle = session.lifecycle.load(Ordering::Acquire);
        let idle = now.saturating_sub(session.last_activity_at.load(Ordering::Acquire));
        let expired = idle >= ttl
            && !protected_objects
                .get(&session.manifest.object_key)
                .copied()
                .unwrap_or(false)
            && session.in_flight_parts.load(Ordering::Acquire) == 0
            && lifecycle != STANDARD_SESSION_FINALIZING;
        if expired {
            return session
                .lifecycle
                .compare_exchange(
                    lifecycle,
                    STANDARD_SESSION_CANCELLED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err();
        }
        true
    });
    Ok(registry
        .values()
        .filter(|session| session.manifest.fragment == fragment)
        .filter(|session| {
            matches!(
                session.lifecycle.load(Ordering::Acquire),
                // Publication of NDX precedes the namespace commit. Keep the
                // finalized session protected until complete_standard_upload
                // removes it after that commit (or the abandoned session TTL).
                STANDARD_SESSION_OPEN | STANDARD_SESSION_FINALIZING | STANDARD_SESSION_FINALIZED
            )
        })
        .map(|session| session.manifest.version_id.clone())
        .collect())
}
