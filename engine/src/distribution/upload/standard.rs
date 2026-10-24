use super::*;

pub fn find_reusable_standard_cluster_upload(
    fragment: &str,
    logical_path: &str,
    params: &InitiateUploadParams,
) -> Option<InitiateUploadResponse> {
    STANDARD_UPLOADS
        .iter()
        .filter(|entry| entry.value().manifest.fragment == fragment)
        .filter(|entry| standard_request_matches(entry.value(), logical_path, params))
        .max_by_key(|entry| entry.value().manifest.created_at)
        .map(|entry| InitiateUploadResponse {
            session_id: entry.value().manifest.session_id.clone(),
            version_id: entry.value().manifest.version_id.clone(),
        })
}

/// Start the high-throughput multipart family without writing a session
/// manifest, pending NCNX binding, or per-part receipt. The coordinator owns
/// this compact state until finalize or restart.
pub async fn initiate_standard_cluster_upload(
    claims: Claims,
    fragment: String,
    physical_object_key: String,
    logical_path: String,
    params: InitiateUploadParams,
) -> Result<InitiateUploadResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    prune_standard_cluster_uploads(&fragment);
    if let Some(existing) = find_reusable_standard_cluster_upload(&fragment, &logical_path, &params)
    {
        return Ok(existing);
    }
    let manifest = prepare_cluster_upload_manifest(&fragment, physical_object_key, &params).await?;
    let response = InitiateUploadResponse {
        session_id: manifest.session_id.clone(),
        version_id: manifest.version_id.clone(),
    };
    let session = Arc::new(StandardUploadSession {
        manifest,
        logical_path: logical_path.trim_start_matches('/').to_string(),
        lifecycle: AtomicU8::new(STANDARD_SESSION_OPEN),
        in_flight_parts: AtomicUsize::new(0),
        last_activity_at: AtomicI64::new(Utc::now().timestamp()),
        parts: DashMap::new(),
        finalized_metadata: OnceLock::new(),
    });
    let key = standard_session_key(&fragment, &response.version_id);
    match STANDARD_UPLOADS.entry(key) {
        dashmap::mapref::entry::Entry::Occupied(existing) => Ok(InitiateUploadResponse {
            session_id: existing.get().manifest.session_id.clone(),
            version_id: existing.get().manifest.version_id.clone(),
        }),
        dashmap::mapref::entry::Entry::Vacant(entry) => {
            entry.insert(session);
            Ok(response)
        }
    }
}

pub fn standard_upload_binding(fragment: &str, version_id: &str) -> Option<(String, String)> {
    standard_session(fragment, version_id).map(|session| {
        (
            session.logical_path.clone(),
            session.manifest.object_key.clone(),
        )
    })
}

pub async fn put_standard_cluster_part<S>(
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
    let session = standard_session(&fragment, &version_id)
        .ok_or_else(|| AppError::BadRequest("Standard upload session not found".into()))?;
    let expected = expected_part_size(&session.manifest, part_index);
    if part_index >= session.manifest.expected_parts {
        return Err(AppError::BadRequest("Part index out of range".into()));
    }
    if expected != content_length {
        return Err(AppError::BadRequest(format!(
            "Part size does not match upload session: got {content_length}, expected {expected}"
        )));
    }
    let claim = match claim_standard_part(Arc::clone(&session), part_index)? {
        StandardPartClaimResult::AlreadyComplete => return Ok(()),
        StandardPartClaimResult::Claimed(claim) => claim,
    };
    let started = Instant::now();
    let part = write_cluster_part_from_stream(
        &fragment,
        &session.manifest.object_key,
        &version_id,
        part_index,
        content_length,
        usize::from(session.manifest.data_shards),
        usize::from(session.manifest.parity_shards),
        &session.manifest.pool_id,
        session.manifest.pool_generation,
        session.manifest.membership_epoch,
        session.manifest.stripe_size_bytes,
        body_stream,
        query.checksum.as_deref(),
        true,
    )
    .await;
    record_io_latency(
        "cluster.upload.standard_part",
        started.elapsed(),
        content_length,
        part.is_ok(),
    );
    let part = part?;
    claim.complete(part.part)
}

pub fn get_standard_cluster_upload_status(
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
    let Some(session) = standard_session(fragment, version_id) else {
        return Ok(UploadStatusResponse::default());
    };
    let mut uploaded_parts = session
        .parts
        .iter()
        .filter_map(|entry| {
            (entry.value().state.load(Ordering::Acquire) == STANDARD_PART_COMPLETE)
                .then_some(*entry.key())
        })
        .collect::<Vec<_>>();
    uploaded_parts.sort_unstable();
    let committed_bytes = uploaded_parts
        .iter()
        .filter_map(|index| session.parts.get(index))
        .filter_map(|slot| slot.record.get().map(|part| part.size_bytes))
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

pub(super) fn standard_session_is_expired(
    session: &StandardUploadSession,
    now: i64,
    ttl: i64,
) -> bool {
    let lifecycle = session.lifecycle.load(Ordering::Acquire);
    let idle = now.saturating_sub(session.last_activity_at.load(Ordering::Acquire));
    // A finalized session can mean NCDX is durable while NCNX publication
    // is awaiting an idempotent retry. Never prune away that binding.
    lifecycle == STANDARD_SESSION_OPEN
        && idle >= ttl
        && session.in_flight_parts.load(Ordering::Acquire) == 0
}

pub(super) fn prune_standard_cluster_uploads(fragment: &str) {
    let now = Utc::now().timestamp();
    let ttl = CONFIG.garbage_collector.file_ttl_seconds as i64;
    STANDARD_UPLOADS.retain(|_, session| {
        if session.manifest.fragment != fragment {
            return true;
        }
        let expired = standard_session_is_expired(session, now, ttl);
        if expired {
            session
                .lifecycle
                .store(STANDARD_SESSION_CANCELLED, Ordering::Release);
        }
        !expired
    });
}

pub fn list_standard_cluster_uploads(
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
    prune_standard_cluster_uploads(fragment);
    let mut sessions = STANDARD_UPLOADS
        .iter()
        .filter(|entry| entry.value().manifest.fragment == fragment)
        .filter(|entry| entry.value().lifecycle.load(Ordering::Acquire) == STANDARD_SESSION_OPEN)
        .map(|entry| {
            let session = entry.value();
            let (committed_parts, committed_bytes) =
                session
                    .parts
                    .iter()
                    .fold((0u64, 0u64), |(count, bytes), slot| {
                        if slot.value().state.load(Ordering::Acquire) != STANDARD_PART_COMPLETE {
                            return (count, bytes);
                        }
                        match slot.value().record.get() {
                            Some(part) => (
                                count.saturating_add(1),
                                bytes.saturating_add(part.size_bytes),
                            ),
                            None => (count, bytes),
                        }
                    });
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
    sessions.sort_by(|left, right| right.created_at.cmp(&left.created_at));
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

pub async fn finalize_standard_cluster_upload(
    claims: Claims,
    fragment: String,
    version_id: String,
    session_id: String,
) -> Result<FinalizeUploadResult, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    let session = standard_session(&fragment, &version_id)
        .ok_or_else(|| AppError::BadRequest("Standard upload session not found".into()))?;
    if session.manifest.session_id != session_id {
        return Err(AppError::Forbidden("Session id mismatch".into()));
    }
    if session.lifecycle.load(Ordering::Acquire) == STANDARD_SESSION_FINALIZED {
        return session
            .finalized_metadata
            .get()
            .cloned()
            .map(FinalizeUploadResult::Complete)
            .ok_or_else(|| {
                AppError::InternalServerError(
                    "finalized standard upload is missing in-memory metadata".into(),
                )
            });
    }
    session
        .lifecycle
        .compare_exchange(
            STANDARD_SESSION_OPEN,
            STANDARD_SESSION_FINALIZING,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .map_err(|_| AppError::Conflict("Multipart session is busy".into()))?;

    let mut uploaded_parts = session
        .parts
        .iter()
        .filter_map(|entry| {
            (entry.value().state.load(Ordering::Acquire) == STANDARD_PART_COMPLETE)
                .then_some(*entry.key())
        })
        .collect::<Vec<_>>();
    uploaded_parts.sort_unstable();
    let present = uploaded_parts.iter().copied().collect::<BTreeSet<_>>();
    let missing_parts = (0..session.manifest.expected_parts)
        .filter(|part| !present.contains(part))
        .collect::<Vec<_>>();
    if !missing_parts.is_empty() || session.in_flight_parts.load(Ordering::Acquire) != 0 {
        session
            .lifecycle
            .store(STANDARD_SESSION_OPEN, Ordering::Release);
        return Ok(FinalizeUploadResult::Incomplete(
            FinalizeIncompleteResponse {
                uploaded_parts,
                missing_parts,
                expected_parts: session.manifest.expected_parts,
            },
        ));
    }

    let finalize_result = async {
        let mut parts = Vec::with_capacity(uploaded_parts.len());
        for part_index in uploaded_parts {
            let slot = session.parts.get(&part_index).ok_or_else(|| {
                AppError::Conflict(format!("Multipart part {part_index} disappeared"))
            })?;
            let part = slot.record.get().cloned().ok_or_else(|| {
                AppError::Conflict(format!("Multipart part {part_index} is incomplete"))
            })?;
            parts.push(part);
        }
        parts.sort_by_key(|part| part.part_index);
        let total_size_bytes = parts.iter().map(|part| part.size_bytes).sum::<u64>();
        if total_size_bytes != session.manifest.total_size_bytes {
            return Err(AppError::Conflict(format!(
                "Multipart byte total is {total_size_bytes}, expected {}",
                session.manifest.total_size_bytes
            )));
        }
        let metadata = ObjectMetadata {
            object_key: session.manifest.object_key.clone(),
            packed_ext_u64: session.manifest.packed_ext_u64,
            fragment: fragment.clone(),
            version_id: version_id.clone(),
            upload_session_id: session_id.clone(),
            owner_uid: fragment.clone(),
            content_type: session.manifest.content_type.clone(),
            total_size_bytes,
            created_at: session.manifest.created_at,
            pool_id: session.manifest.pool_id.clone(),
            pool_generation: session.manifest.pool_generation,
            membership_epoch: session.manifest.membership_epoch,
            data_shards: session.manifest.data_shards,
            parity_shards: session.manifest.parity_shards,
            parts,
            is_delete_marker: false,
            stripe_size_bytes: session.manifest.stripe_size_bytes,
        };
        let started = Instant::now();
        let publication = publish_cluster_object_metadata_after_shard_sync(&metadata).await;
        record_io_latency(
            "cluster.upload.standard_finalize",
            started.elapsed(),
            total_size_bytes,
            publication.is_ok(),
        );
        publication?;
        Ok::<_, AppError>(metadata)
    }
    .await;

    match finalize_result {
        Ok(metadata) => {
            session
                .finalized_metadata
                .set(metadata.clone())
                .map_err(|_| {
                    AppError::InternalServerError(
                        "standard multipart finalized metadata was set twice".into(),
                    )
                })?;
            session
                .last_activity_at
                .store(Utc::now().timestamp(), Ordering::Release);
            session
                .lifecycle
                .store(STANDARD_SESSION_FINALIZED, Ordering::Release);
            Ok(FinalizeUploadResult::Complete(metadata))
        }
        Err(error) => {
            session
                .lifecycle
                .store(STANDARD_SESSION_OPEN, Ordering::Release);
            Err(error)
        }
    }
}

pub fn complete_standard_cluster_upload(fragment: &str, version_id: &str) {
    let key = standard_session_key(fragment, version_id);
    if STANDARD_UPLOADS.get(&key).is_some_and(|session| {
        session.lifecycle.load(Ordering::Acquire) == STANDARD_SESSION_FINALIZED
    }) {
        STANDARD_UPLOADS.remove(&key);
    }
}

pub async fn cancel_standard_cluster_upload(
    claims: &Claims,
    fragment: &str,
    version_id: &str,
) -> Result<(), AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    let key = standard_session_key(fragment, version_id);
    let session = STANDARD_UPLOADS
        .get(&key)
        .map(|entry| Arc::clone(entry.value()))
        .ok_or_else(|| AppError::BadRequest("Standard upload session not found".into()))?;
    let prior = session
        .lifecycle
        .compare_exchange(
            STANDARD_SESSION_OPEN,
            STANDARD_SESSION_CANCELLED,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .err();
    if prior.is_some_and(|state| state != STANDARD_SESSION_CANCELLED) {
        return Err(AppError::Conflict("Multipart session is busy".into()));
    }
    if session.in_flight_parts.load(Ordering::Acquire) != 0 {
        session
            .lifecycle
            .store(STANDARD_SESSION_OPEN, Ordering::Release);
        return Err(AppError::Conflict(
            "Multipart session has an in-flight part".into(),
        ));
    }
    let runtime = active_cluster_runtime()?;
    let identity = crate::distribution::upload_lifecycle::UploadIdentity::from_manifest(
        &runtime,
        &session.manifest,
    );
    crate::distribution::upload_ownership::abort_and_reclaim(&runtime, &identity).await?;
    STANDARD_UPLOADS.remove(&key);
    Ok(())
}

pub fn active_standard_cluster_upload_versions(fragment: &str) -> BTreeSet<String> {
    prune_standard_cluster_uploads(fragment);
    STANDARD_UPLOADS
        .iter()
        .filter(|entry| entry.value().manifest.fragment == fragment)
        .filter(|entry| {
            matches!(
                entry.value().lifecycle.load(Ordering::Acquire),
                STANDARD_SESSION_OPEN | STANDARD_SESSION_FINALIZING
            )
        })
        .map(|entry| entry.value().manifest.version_id.clone())
        .collect()
}
