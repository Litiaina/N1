use super::*;

pub(super) const STANDARD_BATCH_OBJECT_MAX_BYTES: usize = 1024 * 1024;
pub(super) const STANDARD_BATCH_MAX_OBJECTS: usize = 256;
pub(super) const STANDARD_BATCH_MAX_CONCURRENCY: usize = 128;

pub(super) static STANDARD_BATCH_GLOBAL_GATE: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(default_standard_batch_concurrency())));

#[derive(Debug, Deserialize)]
pub(super) struct StandardBatchUploadQuery {
    #[serde(default)]
    concurrency: Option<usize>,
}

#[derive(Debug, Serialize)]
pub(super) struct StandardBatchUploadEntryResult {
    object_key: String,
    status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    version_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug)]
pub(super) struct IndexedStandardBatchUploadResult {
    index: usize,
    result: Result<StandardBatchStagedUpload, StandardBatchUploadEntryResult>,
}

#[derive(Debug)]
pub(super) struct StandardBatchStagedUpload {
    logical_path: String,
    physical_object_id: String,
    size_bytes: u64,
    metadata: crate::core::disk_manager::ObjectMetadata,
}

#[derive(Debug, Serialize)]
pub(super) struct StandardBatchUploadResponse {
    requested: usize,
    succeeded: usize,
    failed: usize,
    results: Vec<StandardBatchUploadEntryResult>,
}

pub(super) fn default_standard_batch_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|cores| cores.get().saturating_mul(4))
        .unwrap_or(16)
        .clamp(1, STANDARD_BATCH_MAX_CONCURRENCY)
}

pub(super) fn decode_standard_batch_object_key(
    field_name: Option<&str>,
    file_name: Option<&str>,
) -> Result<String, AppError> {
    if field_name != Some("object") {
        return Err(AppError::BadRequest(
            "batch file fields must be named 'object'".to_string(),
        ));
    }
    let encoded = file_name.ok_or_else(|| {
        AppError::BadRequest(
            "batch object fields require a percent-encoded object key as filename".to_string(),
        )
    })?;
    let decoded = percent_decode_str(encoded)
        .decode_utf8()
        .map_err(|_| AppError::BadRequest("batch object key is not valid UTF-8".to_string()))?;
    let key = decoded.trim_start_matches('/').trim();
    if key.is_empty() {
        return Err(AppError::BadRequest(
            "batch object key cannot be empty".to_string(),
        ));
    }
    Ok(key.to_string())
}

pub(super) fn batch_error_status(error: &AppError) -> StatusCode {
    match error {
        AppError::BadRequest(_) | AppError::Multipart(_) => StatusCode::BAD_REQUEST,
        AppError::Unauthorized => StatusCode::UNAUTHORIZED,
        AppError::Forbidden(_) => StatusCode::FORBIDDEN,
        AppError::NotFound => StatusCode::NOT_FOUND,
        AppError::Conflict(_) => StatusCode::CONFLICT,
        AppError::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
        AppError::RangeNotSatisfiable => StatusCode::RANGE_NOT_SATISFIABLE,
        AppError::Timeout(_) => StatusCode::REQUEST_TIMEOUT,
        AppError::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        AppError::InsufficientStorage => StatusCode::INSUFFICIENT_STORAGE,
        AppError::Io(error, _)
            if matches!(error.raw_os_error(), Some(libc::ENOSPC | libc::EDQUOT)) =>
        {
            StatusCode::INSUFFICIENT_STORAGE
        }
        AppError::Io(_, _) | AppError::InternalServerError(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

pub(super) fn batch_error_message(error: &AppError) -> String {
    if batch_error_status(error) == StatusCode::INSUFFICIENT_STORAGE {
        return "Insufficient storage capacity".to_string();
    }
    match error {
        AppError::Io(_, _) => "Object storage I/O failed".to_string(),
        AppError::InternalServerError(_) => "Internal object storage failure".to_string(),
        _ => error.to_string(),
    }
}

pub(super) async fn upload_standard_batch_object(
    claims: Claims,
    logical_path: String,
    content_type: String,
    bytes: bytes::Bytes,
) -> Result<StandardBatchStagedUpload, StandardBatchUploadEntryResult> {
    let size_bytes = bytes.len() as u64;
    let operation = async {
        let _batch_permit = Arc::clone(&STANDARD_BATCH_GLOBAL_GATE)
            .acquire_owned()
            .await
            .map_err(|_| {
                AppError::ServiceUnavailable("standard batch upload gate is closed".to_string())
            })?;
        let fragment = claims.fragment.clone();
        let physical = resolve_logical_write_key(&fragment, &logical_path).await?;
        let query = crate::core::disk_manager::UploadPartQuery {
            checksum: None,
            durability: Some("batched".to_string()),
            strict_durability: Some(false),
            io_gate: Some(false),
        };
        let body_stream =
            futures_util::stream::once(async move { Ok::<bytes::Bytes, AppError>(bytes) });
        let metadata = upload_cluster_one_shot(
            claims,
            fragment.clone(),
            physical.clone(),
            index_store::packed_extension_key(&logical_path),
            content_type,
            query,
            body_stream,
            size_bytes,
        )
        .await?;
        Ok::<_, AppError>((physical, metadata))
    }
    .await;

    match operation {
        Ok((physical_object_id, metadata)) => Ok(StandardBatchStagedUpload {
            logical_path,
            physical_object_id,
            size_bytes,
            metadata,
        }),
        Err(error) => Err(StandardBatchUploadEntryResult {
            object_key: logical_path,
            status: batch_error_status(&error).as_u16(),
            version_id: None,
            size_bytes: Some(size_bytes),
            error: Some(batch_error_message(&error)),
        }),
    }
}

pub(super) async fn collect_standard_batch_result(
    workers: &mut JoinSet<IndexedStandardBatchUploadResult>,
    results: &mut Vec<IndexedStandardBatchUploadResult>,
) {
    if let Some(joined) = workers.join_next().await {
        match joined {
            Ok(result) => results.push(result),
            Err(error) => results.push(IndexedStandardBatchUploadResult {
                index: usize::MAX,
                result: Err(StandardBatchUploadEntryResult {
                    object_key: "<batch-worker>".to_string(),
                    status: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                    version_id: None,
                    size_bytes: None,
                    error: Some(format!("Batch worker failed: {error}")),
                }),
            }),
        }
    }
}

/// Streams small standard uploads through N2's existing distributed one-shot
/// commit path. Each object retains an independent EC, NCDX, NCNX, survivor
/// write decision, and result; one failed object never cancels another.
pub(super) async fn upload_standard_batch_scoped(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Query(query): Query<StandardBatchUploadQuery>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }

    let concurrency = query
        .concurrency
        .unwrap_or_else(default_standard_batch_concurrency)
        .clamp(1, STANDARD_BATCH_MAX_CONCURRENCY);
    let mut workers = JoinSet::new();
    let mut results = Vec::<IndexedStandardBatchUploadResult>::new();
    let mut requested = 0usize;

    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|error| AppError::Multipart(error.to_string()))?
    {
        let index = requested;
        requested = requested.saturating_add(1);
        let logical_path = match decode_standard_batch_object_key(field.name(), field.file_name()) {
            Ok(path) => path,
            Err(error) => {
                results.push(IndexedStandardBatchUploadResult {
                    index,
                    result: Err(StandardBatchUploadEntryResult {
                        object_key: "<invalid>".to_string(),
                        status: batch_error_status(&error).as_u16(),
                        version_id: None,
                        size_bytes: None,
                        error: Some(batch_error_message(&error)),
                    }),
                });
                continue;
            }
        };
        let content_type = field
            .content_type()
            .map(ToString::to_string)
            .unwrap_or_else(|| "application/octet-stream".to_string());
        let mut body = BytesMut::new();
        let mut too_large = index >= STANDARD_BATCH_MAX_OBJECTS;
        while let Some(chunk) = field
            .chunk()
            .await
            .map_err(|error| AppError::Multipart(error.to_string()))?
        {
            if body.len().saturating_add(chunk.len()) > STANDARD_BATCH_OBJECT_MAX_BYTES {
                too_large = true;
                continue;
            }
            if !too_large {
                body.extend_from_slice(&chunk);
            }
        }
        if too_large {
            let error = if index >= STANDARD_BATCH_MAX_OBJECTS {
                format!(
                    "Standard batch requests are limited to {} objects",
                    STANDARD_BATCH_MAX_OBJECTS
                )
            } else {
                format!(
                    "Standard batch objects are limited to {} bytes; use one-shot or multipart for larger files",
                    STANDARD_BATCH_OBJECT_MAX_BYTES
                )
            };
            results.push(IndexedStandardBatchUploadResult {
                index,
                result: Err(StandardBatchUploadEntryResult {
                    object_key: logical_path,
                    status: StatusCode::PAYLOAD_TOO_LARGE.as_u16(),
                    version_id: None,
                    size_bytes: None,
                    error: Some(error),
                }),
            });
            continue;
        }

        while workers.len() >= concurrency {
            collect_standard_batch_result(&mut workers, &mut results).await;
        }
        let task_claims = claims.clone();
        workers.spawn(async move {
            IndexedStandardBatchUploadResult {
                index,
                result: upload_standard_batch_object(
                    task_claims,
                    logical_path,
                    content_type,
                    body.freeze(),
                )
                .await,
            }
        });
    }

    while !workers.is_empty() {
        collect_standard_batch_result(&mut workers, &mut results).await;
    }
    if requested == 0 {
        return Err(AppError::BadRequest(
            "standard batch upload contained no object fields".to_string(),
        ));
    }

    let mut completed = Vec::new();
    let mut staged = Vec::new();
    for entry in results {
        match entry.result {
            Ok(upload) => staged.push((entry.index, upload)),
            Err(result) => completed.push((entry.index, result)),
        }
    }
    staged.sort_by_key(|entry| entry.0);
    for chunk in staged.chunks(PEER_NCNX_BATCH_MAX) {
        let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(1);
        let mut publications = Vec::with_capacity(chunk.len());
        let mut record_errors = Vec::with_capacity(chunk.len());
        for (_, upload) in chunk {
            match NcnxRecord::new(
                &claims.fragment,
                &upload.physical_object_id,
                Ulid::new().to_string(),
                now_nanos,
            ) {
                Ok(record) => {
                    publications.push(Some(NcnxPublishBatchItem {
                        logical_key: upload.logical_path.clone(),
                        record,
                    }));
                    record_errors.push(None);
                }
                Err(error) => {
                    publications.push(None);
                    record_errors.push(Some(error));
                }
            }
        }
        let valid = publications
            .iter()
            .filter_map(Clone::clone)
            .collect::<Vec<_>>();
        let started = std::time::Instant::now();
        let mut published = publish_new_batch(&claims.fragment, &valid)
            .await
            .into_iter();
        let mut all_ok = true;
        for ((index, upload), record_error) in chunk.iter().zip(record_errors) {
            let outcome = match record_error {
                Some(error) => Err(error),
                None => published.next().unwrap_or_else(|| {
                    Err(AppError::InternalServerError(
                        "batch NCNX publication omitted a result".into(),
                    ))
                }),
            };
            match outcome {
                Ok(located) => {
                    emit_logical_file_uploaded(
                        &state,
                        &claims.fragment,
                        &upload.logical_path,
                        &located.record.object_id,
                        &upload.metadata,
                    );
                    completed.push((
                        *index,
                        StandardBatchUploadEntryResult {
                            object_key: upload.logical_path.clone(),
                            status: StatusCode::CREATED.as_u16(),
                            version_id: Some(upload.metadata.version_id.clone()),
                            size_bytes: Some(upload.size_bytes),
                            error: None,
                        },
                    ));
                }
                Err(error) => {
                    all_ok = false;
                    completed.push((
                        *index,
                        StandardBatchUploadEntryResult {
                            object_key: upload.logical_path.clone(),
                            status: batch_error_status(&error).as_u16(),
                            version_id: None,
                            size_bytes: Some(upload.size_bytes),
                            error: Some(batch_error_message(&error)),
                        },
                    ));
                }
            }
        }
        record_io_latency(
            "cluster.upload.publish_ncnx_batch",
            started.elapsed(),
            chunk.iter().map(|(_, upload)| upload.size_bytes).sum(),
            all_ok,
        );
    }
    completed.sort_by_key(|entry| entry.0);
    let results = completed
        .into_iter()
        .map(|(_, result)| result)
        .collect::<Vec<_>>();
    let succeeded = results
        .iter()
        .filter(|entry| entry.status == StatusCode::CREATED.as_u16())
        .count();
    let failed = results.len().saturating_sub(succeeded);
    let status = if failed == 0 {
        StatusCode::CREATED
    } else {
        StatusCode::MULTI_STATUS
    };
    Ok((
        status,
        AxumJson(StandardBatchUploadResponse {
            requested,
            succeeded,
            failed,
            results,
        }),
    ))
}
