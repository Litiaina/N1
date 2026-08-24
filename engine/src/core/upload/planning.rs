use super::*;

pub(super) async fn read_manifest_any(
    fragment: &str,
    version_id: &str,
) -> Result<Option<UploadSessionManifest>, AppError> {
    if let Some(state) = index::load_session(fragment, version_id).await? {
        return Ok(Some(state.manifest));
    }
    Ok(None)
}

pub(super) fn configured_data_shards() -> usize {
    DATA_SHARDS_ATOMIC.get().unwrap().load(Ordering::Relaxed) as usize
}

pub(super) fn configured_parity_shards() -> usize {
    PARITY_SHARDS_ATOMIC.get().unwrap().load(Ordering::Relaxed) as usize
}

pub(super) fn manifest_data_shards(manifest: &UploadSessionManifest) -> usize {
    let configured = usize::from(manifest.data_shards);
    if configured == 0 {
        configured_data_shards()
    } else {
        configured
    }
}

pub(super) fn manifest_parity_shards(manifest: &UploadSessionManifest) -> usize {
    usize::from(manifest.parity_shards)
}

pub(super) fn manifest_total_shards(manifest: &UploadSessionManifest) -> usize {
    manifest_data_shards(manifest) + manifest_parity_shards(manifest)
}

pub(super) fn configured_block_size() -> usize {
    STRIPE_SIZE_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or_else(default_stripe_size)
}

pub(super) fn choose_upload_stripe_size_for_len_with_ceiling(
    content_length: u64,
    configured: usize,
) -> usize {
    if configured <= MIN_DYNAMIC_STRIPE_SIZE {
        return configured;
    }

    let candidate = if content_length <= TINY_OBJECT_STRIPE_4K_LIMIT {
        MIN_DYNAMIC_STRIPE_SIZE
    } else if content_length <= SMALL_OBJECT_STRIPE_16K_LIMIT {
        16 * 1024
    } else {
        configured
    };

    candidate.min(configured)
}

pub(super) fn choose_upload_stripe_size_for_len(content_length: u64) -> usize {
    choose_upload_stripe_size_for_len_with_ceiling(content_length, configured_block_size())
}

pub(super) fn choose_upload_stripe_size_for_parts(part_sizes: &[u64]) -> usize {
    let largest_part = part_sizes.iter().copied().max().unwrap_or(0);
    choose_upload_stripe_size_for_len(largest_part)
}

pub(super) async fn preflight_upload_capacity(required_bytes: u64) -> Result<(), AppError> {
    ensure_capacity_available(required_bytes).await
}

pub(super) async fn preflight_blob_placement_capacity_for_parts(
    start_part_index: u64,
    part_sizes: &[u64],
    data_shards: usize,
    parity_shards: usize,
    block_size: usize,
) -> Result<(), AppError> {
    ensure_blob_placement_capacity_available_for_parts(
        start_part_index,
        part_sizes,
        data_shards,
        parity_shards,
        block_size,
    )
    .await
}

pub(super) fn stable_shard_name(part_index: u64, shard_index: usize, data_shards: usize) -> String {
    let ext = if shard_index >= data_shards {
        PARITY_SUFFIX
    } else {
        SHARD_SUFFIX
    };
    format!("part_{}_shard_{}{}", part_index, shard_index, ext)
}

pub(super) fn expected_part_size_bytes(
    manifest: &UploadSessionManifest,
    part_index: u64,
) -> Option<u64> {
    if manifest.expected_parts == 0 || part_index >= manifest.expected_parts {
        return None;
    }
    if manifest.expected_parts == 1 {
        return Some(manifest.total_size_bytes);
    }
    let part_size = manifest.part_size_bytes;
    if part_size == 0 {
        return None;
    }
    if part_index + 1 < manifest.expected_parts {
        return Some(part_size);
    }
    let prior_bytes = part_size.saturating_mul(manifest.expected_parts.saturating_sub(1));
    if prior_bytes > manifest.total_size_bytes {
        return None;
    }
    Some(manifest.total_size_bytes - prior_bytes)
}

pub(super) fn planned_part_sizes(
    total_size_bytes: u64,
    expected_parts: u64,
    part_size_bytes: u64,
) -> Vec<u64> {
    let mut part_sizes = Vec::with_capacity(expected_parts as usize);
    if expected_parts == 0 {
        return part_sizes;
    }
    for part_index in 0..expected_parts {
        let size = if expected_parts == 1 {
            total_size_bytes
        } else if part_index + 1 < expected_parts {
            part_size_bytes
        } else {
            let prior_bytes = part_size_bytes.saturating_mul(expected_parts.saturating_sub(1));
            total_size_bytes.saturating_sub(prior_bytes)
        };
        part_sizes.push(size);
    }
    part_sizes
}

pub(super) fn final_shard_len_for_part(
    part_size_bytes: u64,
    data_shards: usize,
    block_size: usize,
) -> u64 {
    let stripe_data_width = block_size as u64 * data_shards as u64;
    let stripe_count = if stripe_data_width == 0 {
        0
    } else {
        part_size_bytes.div_ceil(stripe_data_width)
    };
    stripe_count * block_size as u64
}

pub(super) fn shard_specs_for_part(
    path_manager: &ObjectPathManager,
    manifest: &UploadSessionManifest,
    part_index: u64,
) -> Result<Vec<(usize, bool, PathBuf, String)>, AppError> {
    let blob_roots = path_manager.get_storage_roots();
    if blob_roots.is_empty() {
        return Err(AppError::InternalServerError(
            "No blob storage disks configured".into(),
        ));
    }

    let data_shards = manifest_data_shards(manifest);
    let total_shards = manifest_total_shards(manifest);
    let mut out = Vec::with_capacity(total_shards);

    for shard_index in 0..total_shards {
        let disk_idx = (part_index as usize + shard_index) % blob_roots.len();
        let disk_root = &blob_roots[disk_idx];
        let shard_dir = path_manager.get_shard_dir_on_disk(
            disk_root,
            &manifest.fragment,
            &manifest.object_key,
            &manifest.version_id,
        );
        let file_name = stable_shard_name(part_index, shard_index, data_shards);
        let stable_path = shard_dir.join(&file_name);
        out.push((
            shard_index,
            shard_index >= data_shards,
            stable_path,
            file_name,
        ));
    }

    Ok(out)
}

pub(super) async fn part_exists_on_disk(
    path_manager: &ObjectPathManager,
    manifest: &UploadSessionManifest,
    part_index: u64,
) -> Result<bool, AppError> {
    let Some(part_size) = expected_part_size_bytes(manifest, part_index) else {
        return Ok(false);
    };

    let data_shards = manifest_data_shards(manifest);
    let block_size = manifest.stripe_size_bytes;
    let expected_shard_len = final_shard_len_for_part(part_size, data_shards, block_size);

    for (_, _, stable_path, _) in shard_specs_for_part(path_manager, manifest, part_index)? {
        let meta = match tokio_fs::metadata(&stable_path).await {
            Ok(meta) => meta,
            Err(_) => return Ok(false),
        };
        if meta.len() != expected_shard_len {
            return Ok(false);
        }
    }

    Ok(true)
}

pub(super) async fn ensure_version_shard_dirs(
    path_manager: &ObjectPathManager,
    manifest: &UploadSessionManifest,
) -> Result<(), AppError> {
    let create_futs = path_manager
        .get_storage_roots()
        .into_iter()
        .map(|disk_root| {
            let shard_dir = path_manager.get_shard_dir_on_disk(
                &disk_root,
                &manifest.fragment,
                &manifest.object_key,
                &manifest.version_id,
            );
            async move {
                tokio_fs::create_dir_all(&shard_dir).await.map_err(|e| {
                    AppError::Io(e, format!("mkdir shard dir {}", shard_dir.display()))
                })
            }
        });

    for result in join_all(create_futs).await {
        result?;
    }

    Ok(())
}

pub(super) async fn uploaded_parts_from_disk(
    path_manager: &ObjectPathManager,
    manifest: &UploadSessionManifest,
) -> Result<Vec<u64>, AppError> {
    let mut uploaded_parts = Vec::new();
    for part_index in 0..manifest.expected_parts {
        if part_exists_on_disk(path_manager, manifest, part_index).await? {
            uploaded_parts.push(part_index);
        }
    }
    Ok(uploaded_parts)
}

pub(super) fn uploaded_part_bytes(manifest: &UploadSessionManifest, parts: &BTreeSet<u64>) -> u64 {
    parts
        .iter()
        .filter_map(|part_index| expected_part_size_bytes(manifest, *part_index))
        .sum()
}

pub(super) async fn persist_missing_receipt_from_disk(
    path_manager: &ObjectPathManager,
    fragment: &str,
    version_id: &str,
    manifest: &UploadSessionManifest,
    part_index: u64,
) -> Result<(), AppError> {
    let receipt_exists = index::load_session_part_receipts(fragment, version_id, manifest)
        .await?
        .and_then(|receipts| receipts.get(part_index as usize).cloned().flatten())
        .is_some();
    if receipt_exists {
        return Ok(());
    }

    if let Some(part_meta) =
        reconstruct_part_meta_from_disk(path_manager, manifest, part_index).await?
    {
        index::commit_session_part_receipt(fragment, version_id, manifest, &part_meta).await?;
    }

    Ok(())
}

pub(super) async fn reconstruct_part_meta_from_disk(
    path_manager: &ObjectPathManager,
    manifest: &UploadSessionManifest,
    part_index: u64,
) -> Result<Option<PartMetadata>, AppError> {
    let Some(part_size) = expected_part_size_bytes(manifest, part_index) else {
        return Ok(None);
    };

    let data_shards = manifest_data_shards(manifest);
    let block_size = manifest.stripe_size_bytes;
    let expected_shard_len = final_shard_len_for_part(part_size, data_shards, block_size);

    let mut shards = Vec::with_capacity(manifest_total_shards(manifest));
    for (shard_index, is_parity, stable_path, file_name) in
        shard_specs_for_part(path_manager, manifest, part_index)?
    {
        let meta = match tokio_fs::metadata(&stable_path).await {
            Ok(meta) => meta,
            Err(_) => return Ok(None),
        };
        if meta.len() != expected_shard_len {
            return Ok(None);
        }
        let checksum = sha256_file_hex(&stable_path).await?;
        shards.push(ShardInfo {
            shard_index,
            file_name,
            absolute_path: stable_path.to_string_lossy().to_string(),
            size_bytes: expected_shard_len,
            checksum,
            block_checksums: Vec::new(),
            checksum_block_size: 0,
            is_parity,
        });
    }

    Ok(Some(PartMetadata {
        part_index,
        size_bytes: part_size,
        created_at: manifest.created_at,
        shards,
        checksum: None,
    }))
}

pub(super) async fn reconstruct_part_meta_from_receipt(
    path_manager: &ObjectPathManager,
    manifest: &UploadSessionManifest,
    receipt: &index::SessionPartReceipt,
) -> Result<Option<PartMetadata>, AppError> {
    let Some(part_size) = expected_part_size_bytes(manifest, receipt.part_index) else {
        return Ok(None);
    };
    if receipt.size_bytes != part_size {
        return Ok(None);
    }

    let data_shards = manifest_data_shards(manifest);
    let total_shards = manifest_total_shards(manifest);
    if receipt.shard_checksums.len() != total_shards {
        return Ok(None);
    }

    let block_size = manifest.stripe_size_bytes;
    let expected_shard_len = final_shard_len_for_part(part_size, data_shards, block_size);
    let shard_size_bytes = if receipt.shard_size_bytes == 0 {
        expected_shard_len
    } else {
        receipt.shard_size_bytes
    };

    let mut shards = Vec::with_capacity(total_shards);
    for ((shard_index, is_parity, stable_path, file_name), checksum) in
        shard_specs_for_part(path_manager, manifest, receipt.part_index)?
            .into_iter()
            .zip(receipt.shard_checksums.iter())
    {
        shards.push(ShardInfo {
            shard_index,
            file_name,
            absolute_path: stable_path.to_string_lossy().to_string(),
            size_bytes: shard_size_bytes,
            checksum: format_sha256(checksum),
            block_checksums: Vec::new(),
            checksum_block_size: 0,
            is_parity,
        });
    }

    Ok(Some(PartMetadata {
        part_index: receipt.part_index,
        size_bytes: receipt.size_bytes,
        created_at: receipt.created_at,
        shards,
        checksum: receipt.checksum.as_ref().map(format_sha256),
    }))
}

pub(super) async fn read_part_meta_any(
    fragment: &str,
    version_id: &str,
    part_index: u64,
) -> Option<PartMetadata> {
    if let Ok(Some(part)) = index::read_session_part(fragment, version_id, part_index).await {
        return Some(part);
    }
    None
}

pub(super) fn request_content_length(headers: &HeaderMap) -> Result<u64, AppError> {
    headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .ok_or(AppError::BadRequest(
            "Content-Length header required".into(),
        ))
}

pub(super) fn request_content_type(headers: &HeaderMap) -> Option<String> {
    headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.split(';').next().unwrap_or(v).trim().to_string())
        .filter(|v| !v.is_empty())
}
