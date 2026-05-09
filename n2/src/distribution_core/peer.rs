use crate::distribution_core::config::n2_runtime_config;
use crate::distribution_core::state::{
    N2_CLUSTER_FINGERPRINT_HEADER, N2_EXPECTED_EPOCH_HEADER, current_cluster_epoch_and_fingerprint,
    validate_cluster_view_proof,
};
use crate::enums::app_error_enum::AppError;
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};

pub(crate) const INTERNAL_TOKEN_HEADER: &str = "x-n2-internal-token";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InternalShardSessionResponse {
    pub session_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InternalShardChunkAck {
    pub session_id: String,
    pub written_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InternalShardAbortResponse {
    pub session_id: String,
    pub aborted: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct InternalMapReadQuery {
    pub object_key: String,
    pub version_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct InternalMapDeleteQuery {
    pub object_key: String,
    pub version_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InternalMapUpsertResponse {
    pub local_mirrors_written: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InternalMapDeleteResponse {
    pub local_map_files_deleted: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InternalShardDeleteResponse {
    pub deleted_paths: usize,
}

pub(crate) fn normalize_endpoint(endpoint: &str) -> String {
    endpoint.trim().trim_end_matches('/').to_ascii_lowercase()
}

pub(crate) fn validate_internal_token(headers: &HeaderMap) -> Result<(), AppError> {
    let cfg = n2_runtime_config();
    let Some(token) = headers
        .get(INTERNAL_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
    else {
        return Err(AppError::Forbidden("missing internal n2 token".to_string()));
    };

    if token != cfg.internal_token {
        return Err(AppError::Forbidden("invalid internal n2 token".to_string()));
    }
    Ok(())
}

pub(crate) async fn validate_internal_peer_request(headers: &HeaderMap) -> Result<(), AppError> {
    validate_internal_token(headers)?;
    let expected_epoch = headers
        .get(N2_EXPECTED_EPOCH_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .ok_or_else(|| {
            AppError::Conflict("missing or invalid internal n2 expected epoch".to_string())
        })?;
    let expected_fingerprint = headers
        .get(N2_CLUSTER_FINGERPRINT_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| AppError::Conflict("missing internal n2 cluster fingerprint".to_string()))?;
    validate_cluster_view_proof(expected_epoch, expected_fingerprint).await
}

pub(crate) async fn add_internal_peer_headers(
    request: reqwest::RequestBuilder,
) -> Result<reqwest::RequestBuilder, AppError> {
    let cfg = n2_runtime_config();
    let (expected_epoch, cluster_fingerprint) = current_cluster_epoch_and_fingerprint().await?;
    Ok(request
        .header(INTERNAL_TOKEN_HEADER, cfg.internal_token.as_str())
        .header(N2_EXPECTED_EPOCH_HEADER, expected_epoch.to_string())
        .header(N2_CLUSTER_FINGERPRINT_HEADER, cluster_fingerprint))
}
