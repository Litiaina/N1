use super::*;

pub async fn execute_native_streaming_op(
    state: Arc<AppState>,
    session: &NativeSession,
    op: &str,
    payload: Value,
    body: Body,
    body_len: u64,
) -> Result<Option<(NativeOpResponse, Vec<u8>)>, NativeExecutionError> {
    let body_stream = body.into_data_stream().map(|chunk_result| {
        chunk_result.map_err(|e| {
            AppError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e),
                "native stream read".into(),
            )
        })
    });
    execute_native_streaming_source(state, session, op, payload, body_stream, body_len).await
}

pub async fn execute_native_streaming_source<S>(
    state: Arc<AppState>,
    session: &NativeSession,
    op: &str,
    payload: Value,
    body_stream: S,
    body_len: u64,
) -> Result<Option<(NativeOpResponse, Vec<u8>)>, NativeExecutionError>
where
    S: Stream<Item = Result<Bytes, AppError>> + Unpin,
{
    let result =
        execute_native_streaming_op_inner(state, session, op, payload, body_stream, body_len).await;
    match result {
        Ok(value) => Ok(value),
        Err(err) => Err(native_app_error(err)),
    }
}

pub async fn execute_native_streaming_reader<R>(
    state: Arc<AppState>,
    session: &NativeSession,
    op: &str,
    payload: Value,
    reader: &mut R,
    body_len: u64,
) -> Result<Option<(NativeOpResponse, Vec<u8>)>, NativeExecutionError>
where
    R: AsyncRead + Unpin + Send,
{
    let result =
        execute_native_streaming_reader_inner(state, session, op, payload, reader, body_len).await;
    match result {
        Ok(value) => Ok(value),
        Err(err) => Err(native_app_error(err)),
    }
}

pub(super) async fn execute_native_streaming_reader_inner<R>(
    state: Arc<AppState>,
    session: &NativeSession,
    op: &str,
    payload: Value,
    reader: &mut R,
    body_len: u64,
) -> Result<Option<(NativeOpResponse, Vec<u8>)>, AppError>
where
    R: AsyncRead + Unpin + Send,
{
    let claims = require_claims(session)?;
    let response = match op {
        "upload.one_shot" | "object.upload.one_shot" => {
            require_write(&claims)?;
            let fragment = claims.fragment.clone();
            let logical = required_string(&payload, "object_key")?;
            let physical = resolve_logical_write_key(&fragment, &logical).await?;
            let body_stream = ReaderStream::new(reader).map(|chunk| {
                chunk.map_err(|err| AppError::Io(err, "native cluster upload body read".into()))
            });
            let mut metadata = upload_cluster_one_shot(
                claims,
                fragment.clone(),
                physical.clone(),
                crate::core::index_store::packed_extension_key(&logical),
                string_field(&payload, "content_type")
                    .unwrap_or_else(|| "application/octet-stream".to_string()),
                upload_part_query_from_payload(&payload)?,
                body_stream,
                body_len,
            )
            .await?;
            let committed_physical =
                commit_logical_upload_from_meta(&fragment, &logical, &physical, &metadata).await?;
            metadata.object_key = logical;
            emit_logical_file_uploaded(
                &state,
                &fragment,
                &metadata.object_key,
                &committed_physical,
                &metadata,
            );
            metadata.strip_internal_layout();
            Some((op_response(201), native_body_bytes(&metadata)))
        }
        "multipart.put_part"
        | "upload.multipart.put_part"
        | "upload.multipart.durable.put_part"
        | "upload.multipart.standard.put_part" => {
            require_write(&claims)?;
            let fragment = claims.fragment.clone();
            let version_id = required_string(&payload, "version_id")?;
            let part_index = u64_field(&payload, "part_index")
                .ok_or_else(|| AppError::BadRequest("part_index is required".into()))?;
            let body_stream = ReaderStream::new(reader).map(|chunk| {
                chunk.map_err(|err| AppError::Io(err, "native cluster multipart body read".into()))
            });
            if op == "upload.multipart.standard.put_part" {
                put_standard_cluster_part(
                    claims,
                    fragment,
                    version_id.clone(),
                    part_index,
                    upload_part_query_from_payload(&payload)?,
                    body_stream,
                    body_len,
                )
                .await?;
            } else {
                put_cluster_part(
                    claims,
                    fragment,
                    version_id.clone(),
                    part_index,
                    upload_part_query_from_payload(&payload)?,
                    body_stream,
                    body_len,
                )
                .await?;
            }
            Some(native_body(json!({
                "version_id": version_id,
                "part_index": part_index,
                "uploaded": true,
            })))
        }
        _ => None,
    };

    if let Some(response) = response {
        return Ok(Some(response));
    }
    execute_native_op_inner(state, session, op, payload, None).await
}

pub(super) async fn execute_native_streaming_op_inner<S>(
    state: Arc<AppState>,
    session: &NativeSession,
    op: &str,
    payload: Value,
    body_stream: S,
    body_len: u64,
) -> Result<Option<(NativeOpResponse, Vec<u8>)>, AppError>
where
    S: Stream<Item = Result<Bytes, AppError>> + Unpin,
{
    let claims = require_claims(session)?;
    let response = match op {
        "upload.one_shot" | "object.upload.one_shot" => {
            require_write(&claims)?;
            let fragment = claims.fragment.clone();
            let logical = required_string(&payload, "object_key")?;
            let physical = resolve_logical_write_key(&fragment, &logical).await?;
            let mut metadata = upload_cluster_one_shot(
                claims,
                fragment.clone(),
                physical.clone(),
                crate::core::index_store::packed_extension_key(&logical),
                string_field(&payload, "content_type")
                    .unwrap_or_else(|| "application/octet-stream".to_string()),
                upload_part_query_from_payload(&payload)?,
                body_stream,
                body_len,
            )
            .await?;
            let committed_physical =
                commit_logical_upload_from_meta(&fragment, &logical, &physical, &metadata).await?;
            metadata.object_key = logical;
            emit_logical_file_uploaded(
                &state,
                &fragment,
                &metadata.object_key,
                &committed_physical,
                &metadata,
            );
            metadata.strip_internal_layout();
            Some((op_response(201), native_body_bytes(&metadata)))
        }
        "multipart.put_part"
        | "upload.multipart.put_part"
        | "upload.multipart.durable.put_part"
        | "upload.multipart.standard.put_part" => {
            require_write(&claims)?;
            let fragment = claims.fragment.clone();
            let version_id = required_string(&payload, "version_id")?;
            let part_index = u64_field(&payload, "part_index")
                .ok_or_else(|| AppError::BadRequest("part_index is required".into()))?;
            if op == "upload.multipart.standard.put_part" {
                put_standard_cluster_part(
                    claims,
                    fragment,
                    version_id.clone(),
                    part_index,
                    upload_part_query_from_payload(&payload)?,
                    body_stream,
                    body_len,
                )
                .await?;
            } else {
                put_cluster_part(
                    claims,
                    fragment,
                    version_id.clone(),
                    part_index,
                    upload_part_query_from_payload(&payload)?,
                    body_stream,
                    body_len,
                )
                .await?;
            }
            Some(native_body(json!({
                "version_id": version_id,
                "part_index": part_index,
                "uploaded": true,
            })))
        }
        _ => None,
    };
    Ok(response)
}
