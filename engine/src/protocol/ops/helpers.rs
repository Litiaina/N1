use super::*;

pub fn op_response(status: u16) -> NativeOpResponse {
    NativeOpResponse { status }
}

pub(super) fn native_body<T: Serialize>(value: T) -> (NativeOpResponse, Vec<u8>) {
    (op_response(200), native_body_bytes(&value))
}

pub(super) fn typed_payload<T: DeserializeOwned>(payload: &Value) -> Result<T, AppError> {
    serde_json::from_value(payload.clone())
        .map_err(|err| AppError::BadRequest(format!("invalid native payload: {err}")))
}

pub(super) fn upload_part_query_from_payload(payload: &Value) -> Result<UploadPartQuery, AppError> {
    typed_payload(payload)
}

#[derive(Clone, Copy)]
pub(super) enum BulkNamespaceMode {
    Trash,
    Recover,
}

pub(super) async fn native_bulk_namespace_operation(
    state: &Arc<AppState>,
    fragment: &str,
    object_keys: Vec<String>,
    event: &str,
    mode: BulkNamespaceMode,
) -> Result<Value, AppError> {
    let requested = object_keys.len();
    let unique_keys = object_keys
        .into_iter()
        .filter(|key| !key.trim().is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if unique_keys.is_empty() {
        return Err(AppError::BadRequest("object_keys must not be empty".into()));
    }

    let mut succeeded = 0usize;
    let mut not_found = 0usize;
    let mut failed = Vec::new();
    let kind = match mode {
        BulkNamespaceMode::Trash => NamespaceMutationKind::Trash,
        BulkNamespaceMode::Recover => NamespaceMutationKind::Recover,
    };
    for (object_key, result) in commit_namespace_bulk(fragment, kind, unique_keys.clone()).await {
        match result {
            Ok(_) => succeeded = succeeded.saturating_add(1),
            Err(AppError::NotFound) => not_found = not_found.saturating_add(1),
            Err(err) => failed.push(json!({
                "object_key": object_key,
                "error": err.to_string(),
            })),
        }
    }

    if succeeded > 0 {
        state.emit(
            fragment,
            StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::bulk(
                fragment, event, succeeded,
            )),
        );
    }

    Ok(json!({
        "requested": requested,
        "unique": unique_keys.len(),
        "succeeded": succeeded,
        "not_found": not_found,
        "failed": failed,
    }))
}

pub(super) async fn native_download_metadata(
    metadata: ObjectMetadata,
    physical_object_key: String,
) -> Result<(NativeOpResponse, Vec<u8>), AppError> {
    let total_size = usize::try_from(metadata.total_size_bytes).map_err(|_| {
        AppError::PayloadTooLarge("native download size exceeds platform usize".into())
    })?;
    if total_size > crate::protocol::types::MAX_NATIVE_FULL_DOWNLOAD_BYTES {
        return Err(AppError::PayloadTooLarge(format!(
            "native full download exceeds {} bytes; use object.download_chunk",
            crate::protocol::types::MAX_NATIVE_FULL_DOWNLOAD_BYTES
        )));
    }

    let mut offset = 0u64;
    let mut out = Vec::with_capacity(total_size);
    while offset < metadata.total_size_bytes {
        let chunk_size = ((metadata.total_size_bytes - offset) as usize).min(8 * 1024 * 1024);
        let (chunk, _) = crate::core::download::download_object_chunk_from_metadata(
            metadata.clone(),
            &ChunkQuery {
                object_key: physical_object_key.clone(),
                offset,
                chunk_size,
            },
        )
        .await?;
        if chunk.is_empty() {
            break;
        }
        offset = offset.saturating_add(chunk.len() as u64);
        out.extend_from_slice(&chunk);
    }

    Ok((op_response(200), out))
}

pub(super) async fn native_from_response(
    response: impl IntoResponse,
) -> (NativeOpResponse, Vec<u8>) {
    let response = response.into_response();
    let status = response.status().as_u16();
    let raw_body = to_bytes(
        response.into_body(),
        crate::protocol::types::MAX_NATIVE_FRAME_BYTES,
    )
    .await
    .map(|bytes| bytes.to_vec())
    .unwrap_or_default();
    let body = serde_json::from_slice::<Value>(&raw_body)
        .ok()
        .and_then(|value| rmp_serde::to_vec_named(&value).ok())
        .unwrap_or(raw_body);
    (op_response(status), body)
}

pub(super) fn native_app_error(err: AppError) -> NativeExecutionError {
    let status = app_error_status(&err).as_u16();
    let body = native_body_bytes(&json!({"error": err.to_string()}));
    NativeExecutionError(op_response(status), body)
}

pub fn auth_error_response(err: &AuthError) -> NativeOpResponse {
    op_response(auth_error_status(err).as_u16())
}

pub fn native_body_bytes<T: Serialize>(value: &T) -> Vec<u8> {
    rmp_serde::to_vec_named(value).unwrap_or_default()
}

pub fn auth_error_message(err: &AuthError) -> &'static str {
    match err {
        AuthError::WrongCredentials => "Wrong credentials",
        AuthError::MissingCredentials => "Missing credentials",
        AuthError::InvalidToken => "Invalid token",
        AuthError::InternalError => "An internal error occurred",
        AuthError::FragmentNotFound => "Fragment not found",
    }
}

pub(super) fn auth_error_status(err: &AuthError) -> StatusCode {
    match err {
        AuthError::WrongCredentials | AuthError::InvalidToken => StatusCode::UNAUTHORIZED,
        AuthError::MissingCredentials | AuthError::FragmentNotFound => StatusCode::BAD_REQUEST,
        AuthError::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

pub(super) fn app_error_status(err: &AppError) -> StatusCode {
    match err {
        AppError::NotFound => StatusCode::NOT_FOUND,
        AppError::Timeout(_) => StatusCode::REQUEST_TIMEOUT,
        AppError::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
        AppError::Conflict(_) => StatusCode::CONFLICT,
        AppError::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
        AppError::Unauthorized => StatusCode::UNAUTHORIZED,
        AppError::Forbidden(_) => StatusCode::FORBIDDEN,
        AppError::Multipart(_) => StatusCode::BAD_REQUEST,
        AppError::RangeNotSatisfiable => StatusCode::RANGE_NOT_SATISFIABLE,
        AppError::InsufficientStorage => StatusCode::INSUFFICIENT_STORAGE,
        AppError::Io(_, _) | AppError::InternalServerError(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

pub(super) fn require_claims(session: &NativeSession) -> Result<Claims, AppError> {
    session.claims().cloned().ok_or(AppError::Unauthorized)
}

pub(super) fn claims_required(claims: Option<Claims>) -> Result<Claims, AppError> {
    claims.ok_or(AppError::Unauthorized)
}

pub(super) fn require_read(claims: &Claims) -> Result<(), AppError> {
    if claims.read {
        Ok(())
    } else {
        Err(AppError::Forbidden("Read permission required".into()))
    }
}

pub(super) fn require_write(claims: &Claims) -> Result<(), AppError> {
    crate::core::operation_gate::ensure_client_mutation_allowed()?;
    if claims.write {
        Ok(())
    } else {
        Err(AppError::Forbidden("Write permission required".into()))
    }
}

pub(super) fn require_write_modify(claims: &Claims) -> Result<(), AppError> {
    crate::core::operation_gate::ensure_client_mutation_allowed()?;
    if claims.write && claims.modify {
        Ok(())
    } else {
        Err(AppError::Forbidden(
            "Write and Modify permission required".into(),
        ))
    }
}

pub(super) fn require_write_modify_delete(claims: &Claims) -> Result<(), AppError> {
    crate::core::operation_gate::ensure_client_mutation_allowed()?;
    if claims.write && claims.modify && claims.delete {
        Ok(())
    } else {
        Err(AppError::Forbidden(
            "Write, Modify and Delete permission required".into(),
        ))
    }
}

pub(super) async fn resolve_logical_read_key(
    fragment: &str,
    requested_path: &str,
) -> Result<String, AppError> {
    let requested = requested_path.trim_start_matches('/').trim();
    if let Some(entry) = global_namespace_runtime()
        .entry(fragment, requested)
        .await?
    {
        if entry.kind != NamespaceEntryKind::File || !entry.lifecycle.is_live() {
            return Err(logical_not_found());
        }
        return entry.object_id.ok_or_else(|| {
            AppError::InternalServerError("logical file is missing physical object id".to_string())
        });
    }
    crate::core::index_store::normalize_object_id(requested).map_err(|_| logical_not_found())
}

pub(super) async fn resolve_any_logical_object_id(
    fragment: &str,
    requested_path: &str,
) -> Result<String, AppError> {
    let requested = requested_path.trim_start_matches('/').trim();
    if let Some(object_id) = global_namespace_runtime()
        .resolve_any_object_id(fragment, requested)
        .await?
    {
        return Ok(object_id);
    }
    crate::core::index_store::normalize_object_id(requested).map_err(|_| logical_not_found())
}

pub(super) fn params_from_payload(payload: &Value) -> HashMap<String, String> {
    payload
        .as_object()
        .map(|obj| {
            obj.iter()
                .filter_map(|(key, value)| value_to_string(value).map(|value| (key.clone(), value)))
                .collect()
        })
        .unwrap_or_default()
}

pub(super) fn params_with_claim_fragment(
    claims: &Claims,
    payload: &Value,
) -> HashMap<String, String> {
    let mut params = params_from_payload(payload);
    params
        .entry("fragment".to_string())
        .or_insert_with(|| claims.fragment.clone());
    params
}

pub(super) fn check_mode_payload(payload: &Value) -> crate::core::repair_manager::RepairCheckMode {
    parse_check_mode(&params_from_payload(payload))
}

pub(super) fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(value) => Some(value.clone()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Number(value) => Some(value.to_string()),
        _ => Some(value.to_string()),
    }
}

pub(super) fn string_field(payload: &Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(value_to_string)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(super) fn required_string(payload: &Value, key: &str) -> Result<String, AppError> {
    string_field(payload, key).ok_or_else(|| AppError::BadRequest(format!("{key} is required")))
}

pub(super) fn string_array_field(payload: &Value, key: &str) -> Result<Vec<String>, AppError> {
    payload
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| AppError::BadRequest(format!("{key} must be an array")))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| AppError::BadRequest(format!("{key} must contain strings")))
        })
        .collect()
}

pub(super) fn bool_field(payload: &Value, key: &str) -> Option<bool> {
    match payload.get(key)? {
        Value::Bool(value) => Some(*value),
        Value::String(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
        .then_some(true)
        .or_else(|| Some(false)),
        Value::Number(value) => value.as_u64().map(|value| value != 0),
        _ => None,
    }
}

pub(super) fn usize_field(payload: &Value, key: &str) -> Option<usize> {
    payload.get(key).and_then(|value| match value {
        Value::Number(value) => value.as_u64().and_then(|value| usize::try_from(value).ok()),
        Value::String(value) => value.trim().parse::<usize>().ok(),
        _ => None,
    })
}

pub(super) fn u64_field(payload: &Value, key: &str) -> Option<u64> {
    payload.get(key).and_then(|value| match value {
        Value::Number(value) => value.as_u64(),
        Value::String(value) => value.trim().parse::<u64>().ok(),
        _ => None,
    })
}

pub(super) fn fragment_hash(fragment: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(fragment.as_bytes());
    hex::encode(hasher.finalize())
}

#[allow(dead_code)]
pub(super) fn encode_query(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}
