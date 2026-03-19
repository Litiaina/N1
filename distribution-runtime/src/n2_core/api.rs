use crate::enums::app_error_enum::AppError;
use crate::middleware::auth_middleware::ADMIN_KEY;
use crate::n2_core::config::effective_cross_params;
use crate::n2_core::config::n2_runtime_config;
use crate::n2_core::model::{
    ClusterMembership, ClusterViewQuery, ObjectIdentity, ResolvePlacementQuery,
    UpsertMembershipRequest,
};
use crate::n2_core::state::{
    apply_membership_snapshot, get_cluster_view, get_membership, resolve_placement,
    stage_rollback_membership, stage_upsert_membership,
};
use crate::structs::auth_structs::Claims;
use axum::Json as AxumJson;
use axum::extract::{Path, Query};
use axum::http::HeaderMap;
use std::collections::HashSet;
use std::time::Duration;
use ulid::Ulid;

const N2_INTERNAL_TOKEN_HEADER: &str = "x-n2-internal-token";

fn is_admin_claim(claims: &Claims) -> bool {
    claims.fragment == *ADMIN_KEY
}

fn validate_internal_token(headers: &HeaderMap) -> Result<(), AppError> {
    let cfg = n2_runtime_config();
    let Some(token) = headers
        .get(N2_INTERNAL_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
    else {
        return Err(AppError::Forbidden("missing internal n2 token".to_string()));
    };

    if token != cfg.internal_token {
        return Err(AppError::Forbidden("invalid internal n2 token".to_string()));
    }
    Ok(())
}

fn normalize_endpoint(endpoint: &str) -> String {
    endpoint.trim().trim_end_matches('/').to_ascii_lowercase()
}

async fn propagate_membership_snapshot(
    previous: &ClusterMembership,
    next: &ClusterMembership,
) -> Result<(), AppError> {
    let cfg = n2_runtime_config();
    let local_endpoint = normalize_endpoint(&cfg.node_endpoint);
    let target_majority = next.nodes.iter().filter(|n| n.active).count() / 2 + 1;

    if target_majority <= 1 {
        return Ok(());
    }

    let mut endpoints = previous
        .nodes
        .iter()
        .chain(next.nodes.iter())
        .filter(|node| node.active)
        .map(|node| node.endpoint.clone())
        .filter(|endpoint| normalize_endpoint(endpoint) != local_endpoint)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    endpoints.sort();

    if endpoints.is_empty() {
        return Err(AppError::Conflict(
            "membership update requires remote peers but none are reachable from the proposed active set"
                .to_string(),
        ));
    }

    let http_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_millis(cfg.remote_request_timeout_ms))
        .build()
        .map_err(|e| {
            AppError::InternalServerError(format!("build n2 membership client failed: {}", e))
        })?;

    let mut acknowledged = Vec::new();
    for endpoint in &endpoints {
        let url = format!(
            "{}/v4/n2/internal/membership",
            endpoint.trim_end_matches('/')
        );
        let response = http_client
            .put(url)
            .header(N2_INTERNAL_TOKEN_HEADER, cfg.internal_token.as_str())
            .json(next)
            .send()
            .await;
        match response {
            Ok(resp) if resp.status().is_success() => acknowledged.push(endpoint.clone()),
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                tracing::warn!(
                    "n2 membership propagation failed endpoint={} status={} body={}",
                    endpoint,
                    status,
                    body
                );
            }
            Err(err) => {
                tracing::warn!(
                    "n2 membership propagation request failed endpoint={} err={}",
                    endpoint,
                    err
                );
            }
        }
    }

    let total_acks = 1usize.saturating_add(acknowledged.len());
    if total_acks >= target_majority {
        return Ok(());
    }

    for endpoint in &acknowledged {
        let url = format!(
            "{}/v4/n2/internal/membership",
            endpoint.trim_end_matches('/')
        );
        let _ = http_client
            .put(url)
            .header(N2_INTERNAL_TOKEN_HEADER, cfg.internal_token.as_str())
            .json(previous)
            .send()
            .await;
    }

    Err(AppError::Conflict(format!(
        "membership propagation reached only {} acknowledgements, need majority {}",
        total_acks, target_majority
    )))
}

pub async fn get_n2_membership(
    claims: Claims,
) -> Result<AxumJson<crate::n2_core::model::ClusterMembership>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".to_string()));
    }
    Ok(AxumJson(get_membership().await?))
}

pub async fn get_n2_cluster_view(
    claims: Claims,
    Query(query): Query<ClusterViewQuery>,
) -> Result<AxumJson<crate::n2_core::model::N2ClusterViewResponse>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".to_string()));
    }
    Ok(AxumJson(get_cluster_view(query.expected_epoch).await?))
}

pub async fn get_n2_cluster_view_public(
    Query(query): Query<ClusterViewQuery>,
) -> Result<AxumJson<crate::n2_core::model::N2ClusterViewResponse>, AppError> {
    Ok(AxumJson(get_cluster_view(query.expected_epoch).await?))
}

pub async fn put_n2_membership(
    claims: Claims,
    AxumJson(payload): AxumJson<UpsertMembershipRequest>,
) -> Result<AxumJson<crate::n2_core::model::ClusterMembership>, AppError> {
    if !claims.modify || !is_admin_claim(&claims) {
        return Err(AppError::Forbidden(
            "Admin modify permission required".to_string(),
        ));
    }
    let (previous, next) = stage_upsert_membership(payload).await?;
    propagate_membership_snapshot(&previous, &next).await?;
    Ok(AxumJson(apply_membership_snapshot(next).await?))
}

pub async fn rollback_n2_membership(
    claims: Claims,
    Path(epoch): Path<u64>,
) -> Result<AxumJson<crate::n2_core::model::ClusterMembership>, AppError> {
    if !claims.modify || !is_admin_claim(&claims) {
        return Err(AppError::Forbidden(
            "Admin modify permission required".to_string(),
        ));
    }
    let (previous, next) = stage_rollback_membership(epoch).await?;
    propagate_membership_snapshot(&previous, &next).await?;
    Ok(AxumJson(apply_membership_snapshot(next).await?))
}

pub async fn put_n2_internal_membership(
    headers: HeaderMap,
    AxumJson(payload): AxumJson<ClusterMembership>,
) -> Result<AxumJson<crate::n2_core::model::ClusterMembership>, AppError> {
    validate_internal_token(&headers)?;
    Ok(AxumJson(apply_membership_snapshot(payload).await?))
}

pub async fn resolve_n2_placement(
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
    Query(query): Query<ResolvePlacementQuery>,
) -> Result<AxumJson<crate::n2_core::model::ResolvePlacementResponse>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".to_string()));
    }
    if !is_admin_claim(&claims) && claims.fragment != fragment {
        return Err(AppError::Forbidden(
            "Access denied to this fragment".to_string(),
        ));
    }

    let cross_params = effective_cross_params();
    let kc = query.kc.unwrap_or(cross_params.kc);
    let mc = query.mc.unwrap_or(cross_params.mc);
    let uid = query.uid.unwrap_or_else(|| claims.fragment.clone());
    let version_id = query.version_id.unwrap_or_else(|| Ulid::new().to_string());

    let identity = ObjectIdentity {
        uid,
        fragment,
        object_key: object_key.trim_start_matches('/').to_string(),
        version_id,
    };
    let response = resolve_placement(identity, kc, mc).await?;
    Ok(AxumJson(response))
}
