use super::*;

pub async fn prepare_native_download_stream(
    session: &NativeSession,
    payload: Value,
) -> Result<NativeDownloadPlan, AppError> {
    let claims = require_claims(session)?;
    require_read(&claims)?;
    let fragment = claims.fragment.clone();
    let logical = required_string(&payload, "object_key")?;
    let physical = resolve_logical_read_key(&fragment, &logical).await?;
    let metadata = if let Some(version_id) = string_field(&payload, "version_id") {
        get_version_metadata_by_id(&fragment, &physical, &version_id).await?
    } else {
        get_object_metadata_by_id(&fragment, &physical).await?
    };
    if metadata.is_delete_marker {
        return Err(AppError::NotFound);
    }

    let total_size = metadata.total_size_bytes;
    let offset = u64_field(&payload, "offset").unwrap_or(0).min(total_size);
    let remaining = total_size.saturating_sub(offset);
    let requested_len = u64_field(&payload, "length").unwrap_or(remaining);
    let body_len = remaining.min(requested_len);
    let requested_chunk_size = usize_field(&payload, "chunk_size")
        .unwrap_or(NATIVE_STREAM_DOWNLOAD_CHUNK_BYTES)
        .clamp(64 * 1024, 8 * 1024 * 1024);
    let chunk_size = requested_chunk_size.min(NATIVE_STREAM_DOWNLOAD_CHUNK_BYTES);

    let response_payload = json!({
        "object_key": logical.trim_start_matches('/'),
        "version_id": metadata.version_id,
        "content_type": metadata.content_type,
        "total_size_bytes": total_size,
        "offset": offset,
        "length": body_len,
        "chunk_size_bytes": chunk_size,
        "stream": true,
    });

    Ok(NativeDownloadPlan {
        payload: response_payload,
        body_len,
        metadata,
        offset,
    })
}

pub async fn stream_native_download_plan<W>(
    writer: &mut W,
    plan: NativeDownloadPlan,
) -> Result<u64, AppError>
where
    W: AsyncWrite + Unpin + Send,
{
    let written = crate::core::download::stream_object_range_to_writer(
        plan.metadata,
        plan.offset,
        plan.body_len,
        writer,
    )
    .await?;

    if written != plan.body_len {
        return Err(AppError::InternalServerError(format!(
            "native download stream ended early: wrote {} of {} bytes",
            written, plan.body_len
        )));
    }

    writer
        .flush()
        .await
        .map_err(|err| AppError::Io(err, "native download stream flush".to_string()))?;
    Ok(written)
}
