use super::*;

pub(super) async fn prepare_cluster_upload_manifest(
    fragment: &str,
    object_key: String,
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
            .ok_or_else(|| AppError::BadRequest("part_size_bytes is required".into()))?
    };
    let computed_parts = if params.total_size_bytes == 0 || part_size_bytes == 0 {
        1
    } else {
        params.total_size_bytes.div_ceil(part_size_bytes)
    };
    if computed_parts != params.expected_parts.max(1) {
        return Err(AppError::BadRequest(
            "expected_parts does not match total_size_bytes and part_size_bytes".into(),
        ));
    }
    let (runtime, _active, layout) = current_write_context().await?;
    let write_pool = runtime
        .membership
        .newest_writable_pool()
        .expect("write context requires a writable pool");
    let version_id = Ulid::new().to_string();
    let session_id = Ulid::new().to_string();
    let stripe_size = choose_upload_stripe_size(params.total_size_bytes);
    let manifest = UploadSessionManifest {
        fragment: fragment.to_string(),
        object_key,
        packed_ext_u64: params.packed_ext_u64,
        version_id: version_id.clone(),
        session_id: session_id.clone(),
        total_size_bytes: params.total_size_bytes,
        expected_parts: params.expected_parts.max(1),
        part_size_bytes,
        content_type: params
            .content_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".into()),
        created_at: Utc::now().timestamp(),
        pool_id: write_pool.id.clone(),
        pool_generation: write_pool.generation,
        stripe_size_bytes: stripe_size,
        membership_epoch: runtime.membership.epoch,
        data_shards: layout.data_shards as u16,
        parity_shards: layout.parity_shards as u16,
    };
    crate::distribution::upload_ownership::register_upload(
        &runtime,
        &crate::distribution::upload_lifecycle::UploadIdentity::from_manifest(&runtime, &manifest),
    )
    .await?;
    Ok(manifest)
}

pub async fn initiate_cluster_upload(
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
    let manifest = prepare_cluster_upload_manifest(&fragment, object_key, &params).await?;
    let version_id = manifest.version_id.clone();
    let session_id = manifest.session_id.clone();
    index::open_session(&manifest, None).await?;
    Ok(InitiateUploadResponse {
        session_id,
        version_id,
    })
}
