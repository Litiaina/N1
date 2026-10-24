//! Incremental HTTP parsing for bounded, request-local strict commit groups.
use super::*;

pub(super) const MAX_GROUP_OBJECTS: usize = 64;
pub(super) const MAX_GROUP_BYTES: usize = 8 * 1024 * 1024;

fn canonical_key(key: &str) -> Result<String, AppError> {
    let normalized = crate::namespace::paths::normalize_path(key)
        .map_err(|error| AppError::BadRequest(error.to_string()))?;
    if normalized.is_empty() {
        return Err(AppError::BadRequest("root cannot be an object".into()));
    }
    index_store::reject_reserved_user_path(&normalized)?;
    Ok(normalized)
}

fn rejected(
    index: usize,
    key: String,
    size: Option<u64>,
    status: StatusCode,
    message: String,
) -> IndexedStandardBatchUploadResult {
    IndexedStandardBatchUploadResult {
        index,
        result: StandardBatchUploadEntryResult {
            object_key: key,
            status: status.as_u16(),
            version_id: None,
            size_bytes: size,
            error: Some(message),
        },
    }
}

async fn finish(
    state: &Arc<AppState>,
    claims: &Claims,
    pending: &mut Vec<small_batch::Input>,
    guard: &mut Option<AdaptiveIoGuard>,
    concurrency: usize,
    results: &mut Vec<IndexedStandardBatchUploadResult>,
) {
    if !pending.is_empty() {
        results.extend(
            small_batch::commit(
                Arc::clone(state),
                claims.clone(),
                std::mem::take(pending),
                guard.take(),
                concurrency,
            )
            .await,
        );
    }
}

pub(super) async fn upload(
    state: Arc<AppState>,
    claims: Claims,
    query: StandardBatchUploadQuery,
    mut multipart: Multipart,
) -> Result<Response, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    crate::core::operation_gate::ensure_user_mutations_allowed()?;
    let concurrency = query
        .concurrency
        .unwrap_or_else(default_standard_batch_concurrency)
        .clamp(1, STANDARD_BATCH_MAX_CONCURRENCY);
    let mut pending = Vec::<small_batch::Input>::new();
    let mut bytes = 0usize;
    let mut guard = None;
    let mut admitted = false;
    let mut results = Vec::new();
    let mut requested = 0usize;
    let mut admission_failure: Option<(StatusCode, String)> = None;
    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|error| AppError::Multipart(error.to_string()))?
    {
        let index = requested;
        if index >= STANDARD_BATCH_MAX_OBJECTS {
            return Err(AppError::BadRequest(format!(
                "Standard batch requests are limited to {STANDARD_BATCH_MAX_OBJECTS} objects"
            )));
        }
        requested += 1;
        let response_key = match decode_standard_batch_object_key(field.name(), field.file_name()) {
            Ok(key) => key,
            Err(error) => {
                results.push(rejected(
                    index,
                    "<invalid>".into(),
                    None,
                    batch_error_status(&error),
                    batch_error_message(&error),
                ));
                continue;
            }
        };
        let logical_path = match canonical_key(&response_key) {
            Ok(key) => key,
            Err(error) => {
                results.push(rejected(
                    index,
                    response_key,
                    None,
                    batch_error_status(&error),
                    batch_error_message(&error),
                ));
                continue;
            }
        };
        if let Some((status, message)) = &admission_failure {
            results.push(rejected(
                index,
                response_key,
                None,
                *status,
                message.clone(),
            ));
            continue;
        }
        // Leave space for the next bounded body before reading any of it.
        // Repeated/aliased keys start a new group, retaining version ordering
        // and avoiding holding the same per-key publication gate twice.
        if pending.len() == MAX_GROUP_OBJECTS
            || bytes > MAX_GROUP_BYTES - STANDARD_BATCH_OBJECT_MAX_BYTES
            || pending.iter().any(|item| item.logical_path == logical_path)
        {
            finish(
                &state,
                &claims,
                &mut pending,
                &mut guard,
                concurrency,
                &mut results,
            )
            .await;
            bytes = 0;
            admitted = false;
        }
        if !admitted {
            let context = format!("standard-batch-group fragment={}", claims.fragment);
            match acquire_upload_io_guard_for_query(Some(true), &claims.fragment, &context).await {
                Ok(acquired) => {
                    guard = acquired;
                    admitted = true;
                }
                Err(error) => {
                    let status = batch_error_status(&error);
                    let message = batch_error_message(&error);
                    results.push(rejected(index, response_key, None, status, message.clone()));
                    admission_failure = Some((status, message));
                    continue;
                }
            }
        }
        let content_type = field
            .content_type()
            .map(ToString::to_string)
            .unwrap_or_else(|| "application/octet-stream".into());
        let mut body = BytesMut::new();
        let mut too_large = false;
        while let Some(chunk) = field
            .chunk()
            .await
            .map_err(|error| AppError::Multipart(error.to_string()))?
        {
            if body.len().saturating_add(chunk.len()) > STANDARD_BATCH_OBJECT_MAX_BYTES {
                too_large = true;
            }
            if !too_large {
                body.extend_from_slice(&chunk);
            }
        }
        if too_large {
            results.push(rejected(index, response_key, None, StatusCode::PAYLOAD_TOO_LARGE,
                format!("Standard batch objects are limited to {STANDARD_BATCH_OBJECT_MAX_BYTES} bytes; use one-shot or multipart for larger files")));
            continue;
        }
        bytes += body.len();
        debug_assert!(bytes <= MAX_GROUP_BYTES && pending.len() < MAX_GROUP_OBJECTS);
        pending.push(small_batch::Input {
            index,
            logical_path,
            response_key,
            content_type,
            bytes: body.freeze(),
        });
    }
    finish(
        &state,
        &claims,
        &mut pending,
        &mut guard,
        concurrency,
        &mut results,
    )
    .await;
    if requested == 0 {
        return Err(AppError::BadRequest(
            "standard batch upload contained no object fields".into(),
        ));
    }
    results.sort_by_key(|entry| entry.index);
    let results = results
        .into_iter()
        .map(|entry| entry.result)
        .collect::<Vec<_>>();
    let succeeded = results
        .iter()
        .filter(|entry| entry.status == StatusCode::CREATED.as_u16())
        .count();
    let failed = results.len().saturating_sub(succeeded);
    Ok((
        if failed == 0 {
            StatusCode::CREATED
        } else {
            StatusCode::MULTI_STATUS
        },
        AxumJson(StandardBatchUploadResponse {
            requested,
            succeeded,
            failed,
            results,
        }),
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn group_memory_and_key_bounds_do_not_depend_on_folder_population() {
        assert_eq!(MAX_GROUP_OBJECTS, 64);
        assert_eq!(MAX_GROUP_BYTES, 8 * STANDARD_BATCH_OBJECT_MAX_BYTES);
        assert_eq!(canonical_key("a//b/file.txt").unwrap(), "a/b/file.txt");
        assert!(canonical_key("a/./file.txt").is_err());
        assert!(canonical_key("/").is_err());
        assert!(canonical_key("../escape").is_err());
    }
}
