use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadSessionManifest {
    pub fragment: String,
    pub object_key: String,
    #[serde(default)]
    pub packed_ext_u64: u64,
    pub version_id: String,
    pub session_id: String,
    pub total_size_bytes: u64,
    pub expected_parts: u64,
    #[serde(default)]
    pub part_size_bytes: u64,
    pub content_type: String,
    pub created_at: i64,
    pub pool_id: String,
    pub pool_generation: u64,
    pub stripe_size_bytes: usize,
    pub membership_epoch: u64,
    #[serde(default)]
    pub data_shards: u16,
    #[serde(default)]
    pub parity_shards: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveSessionInfo {
    pub session_id: String,
    pub version_id: String,
    pub object_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub physical_object_key: Option<String>,
    pub created_at: i64,
    pub reserved_size: u64,
    pub total_size_bytes: Option<u64>,
    pub expected_parts: Option<u64>,
    pub committed_parts: u64,
    pub committed_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListUploadsResponse {
    pub sessions: Vec<ActiveSessionInfo>,
    pub total: usize,
}

fn committed_session_progress(state: &index::IndexSessionState) -> (u64, u64) {
    if !state.part_receipts.is_empty() {
        let committed_parts = state.part_receipts.len() as u64;
        let committed_bytes = state
            .part_receipts
            .values()
            .map(|receipt| receipt.size_bytes)
            .sum();
        return (committed_parts, committed_bytes);
    }

    let committed_parts = state.parts.len() as u64;
    let committed_bytes = state.parts.values().map(|part| part.size_bytes).sum();
    (committed_parts, committed_bytes)
}

pub async fn initiate_object_upload(
    claims: Claims,
    AxumPath((fragment, object_key)): AxumPath<(String, String)>,
    AxumJson(params): AxumJson<InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    let response = initiate_object_upload_direct(claims, fragment, object_key, params).await?;
    Ok((StatusCode::CREATED, AxumJson(response)))
}

pub async fn initiate_object_upload_direct(
    claims: Claims,
    fragment: String,
    object_key: String,
    params: InitiateUploadParams,
) -> Result<InitiateUploadResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    if params.expected_parts == 0 {
        return Err(AppError::BadRequest("expected_parts must be >= 1".into()));
    }

    let part_size_bytes = if params.expected_parts <= 1 {
        params.part_size_bytes.unwrap_or(params.total_size_bytes)
    } else {
        params
            .part_size_bytes
            .filter(|v| *v > 0)
            .ok_or(AppError::BadRequest(
                "part_size_bytes is required for multipart uploads".into(),
            ))?
    };

    let computed_parts = if params.total_size_bytes == 0 {
        1
    } else if part_size_bytes == 0 {
        1
    } else {
        params.total_size_bytes.div_ceil(part_size_bytes)
    };

    if computed_parts != params.expected_parts.max(1) {
        return Err(AppError::BadRequest(
            "expected_parts does not match total_size_bytes and part_size_bytes".into(),
        ));
    }

    preflight_upload_capacity(params.total_size_bytes).await?;

    let version_id = Ulid::new().to_string();
    let session_id = Ulid::new().to_string();

    let planned_sizes = planned_part_sizes(
        params.total_size_bytes,
        params.expected_parts.max(1),
        part_size_bytes,
    );
    let stripe_size = choose_upload_stripe_size_for_parts(&planned_sizes);
    let (pool_id, pool_generation) = current_pool_identity()?;

    let manifest = UploadSessionManifest {
        fragment: fragment.clone(),
        object_key: object_key.clone(),
        packed_ext_u64: params.packed_ext_u64,
        version_id: version_id.clone(),
        session_id: session_id.clone(),
        total_size_bytes: params.total_size_bytes,
        expected_parts: params.expected_parts.max(1),
        part_size_bytes,
        content_type: params
            .content_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".to_string()),
        created_at: Utc::now().timestamp(),
        pool_id,
        pool_generation,
        stripe_size_bytes: stripe_size,
        membership_epoch: 0,
        data_shards: configured_data_shards() as u16,
        parity_shards: configured_parity_shards() as u16,
    };

    preflight_blob_placement_capacity_for_parts(
        0,
        &planned_sizes,
        manifest_data_shards(&manifest),
        manifest_parity_shards(&manifest),
        manifest.stripe_size_bytes,
    )
    .await?;

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

pub async fn list_active_uploads(
    claims: Claims,
    AxumPath(fragment): AxumPath<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let limit: usize = params
        .get("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(50)
        .min(100);
    let offset: usize = params
        .get("offset")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let mut sessions: Vec<ActiveSessionInfo> = index::list_active_sessions(&fragment)
        .await?
        .into_iter()
        .filter(|state| state.finalized_at.is_none() && state.aborted_at.is_none())
        .map(|state| {
            let (committed_parts, committed_bytes) = committed_session_progress(&state);
            ActiveSessionInfo {
                session_id: state.manifest.session_id.clone(),
                version_id: state.manifest.version_id.clone(),
                object_key: Some(state.manifest.object_key.clone()),
                physical_object_key: None,
                created_at: state.manifest.created_at,
                reserved_size: state.reservation.map(|receipt| receipt.amount).unwrap_or(0),
                total_size_bytes: Some(state.manifest.total_size_bytes),
                expected_parts: Some(state.manifest.expected_parts),
                committed_parts,
                committed_bytes,
            }
        })
        .collect();

    sessions.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    let total = sessions.len();
    let paged_sessions = sessions.into_iter().skip(offset).take(limit).collect();

    Ok(AxumJson(ListUploadsResponse {
        sessions: paged_sessions,
        total,
    }))
}

pub async fn get_upload_status(
    claims: Claims,
    AxumPath((fragment, version_id)): AxumPath<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    if let Some(state) = index::load_session(&fragment, &version_id).await? {
        let path_manager = ObjectPathManager::new();
        let (mut committed_parts, mut committed_bytes) = committed_session_progress(&state);
        let mut part_set = BTreeSet::new();
        if let Some(receipts) =
            index::load_session_part_receipts(&fragment, &version_id, &state.manifest).await?
        {
            part_set.extend(
                receipts
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, receipt)| receipt.as_ref().map(|_| idx as u64)),
            );
        }
        if state.manifest.part_size_bytes > 0
            && part_set.len() < state.manifest.expected_parts as usize
        {
            part_set.extend(uploaded_parts_from_disk(&path_manager, &state.manifest).await?);
        }
        let disk_backed_bytes = uploaded_part_bytes(&state.manifest, &part_set);
        if disk_backed_bytes > committed_bytes {
            committed_parts = part_set.len() as u64;
            committed_bytes = disk_backed_bytes;
        }
        let parts = part_set.into_iter().collect();
        return Ok(AxumJson(UploadStatusResponse {
            uploaded_parts: parts,
            committed_parts,
            committed_bytes,
            expected_parts: Some(state.manifest.expected_parts),
            total_size_bytes: Some(state.manifest.total_size_bytes),
            created_at: Some(state.manifest.created_at),
            session_id: Some(state.manifest.session_id),
            version_id: Some(state.manifest.version_id),
            object_key: Some(state.manifest.object_key),
            physical_object_key: None,
        }));
    }

    Ok(AxumJson(UploadStatusResponse::default()))
}
