use crate::distribution_core::config::effective_cross_params;
use crate::distribution_core::data_plane::schedule_n2_auto_maintenance;
use crate::distribution_core::governance::{
    commit_membership_change, governance_state_view, handle_governance_abort,
    handle_governance_append, handle_governance_vote,
};
use crate::distribution_core::model::{
    ClusterViewQuery, GovernanceAbortRequest, GovernanceAppendRequest, GovernanceVoteRequest,
    ObjectIdentity, ResolvePlacementQuery, UpsertMembershipRequest,
};
use crate::distribution_core::object_index::resolve_n2_object_id_local;
use crate::distribution_core::state::{
    get_cluster_view, get_membership, resolve_placement, stage_rollback_membership,
    stage_upsert_membership,
};
use crate::enums::app_error_enum::AppError;
use crate::middleware::auth_middleware::ADMIN_KEY;
use crate::structs::auth_structs::Claims;
use axum::Json as AxumJson;
use axum::extract::{Path, Query};
use axum::http::HeaderMap;
use sha2::{Digest, Sha256};
use ulid::Ulid;

fn is_admin_claim(claims: &Claims) -> bool {
    claims.fragment == *ADMIN_KEY
}

fn preview_object_id(uid: &str, fragment: &str, object_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"n2-preview-object-id|");
    hasher.update(uid.as_bytes());
    hasher.update(b"|");
    hasher.update(fragment.as_bytes());
    hasher.update(b"|");
    hasher.update(object_key.as_bytes());
    hex::encode(hasher.finalize())
}

pub async fn get_n2_membership(
    claims: Claims,
) -> Result<AxumJson<crate::distribution_core::model::ClusterMembership>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".to_string()));
    }
    Ok(AxumJson(get_membership().await?))
}

pub async fn get_n2_cluster_view(
    claims: Claims,
    Query(query): Query<ClusterViewQuery>,
) -> Result<AxumJson<crate::distribution_core::model::N2ClusterViewResponse>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".to_string()));
    }
    Ok(AxumJson(get_cluster_view(query.expected_epoch).await?))
}

pub async fn get_n2_cluster_view_public(
    Query(query): Query<ClusterViewQuery>,
) -> Result<AxumJson<crate::distribution_core::model::N2ClusterViewResponse>, AppError> {
    Ok(AxumJson(get_cluster_view(query.expected_epoch).await?))
}

pub async fn put_n2_membership(
    claims: Claims,
    AxumJson(payload): AxumJson<UpsertMembershipRequest>,
) -> Result<AxumJson<crate::distribution_core::model::ClusterMembership>, AppError> {
    if !claims.modify || !is_admin_claim(&claims) {
        return Err(AppError::Forbidden(
            "Admin modify permission required".to_string(),
        ));
    }
    let (_, next) = stage_upsert_membership(payload).await?;
    let applied = commit_membership_change(next).await?;
    schedule_n2_auto_maintenance("membership_apply", applied.epoch);
    Ok(AxumJson(applied))
}

pub async fn rollback_n2_membership(
    claims: Claims,
    Path(epoch): Path<u64>,
) -> Result<AxumJson<crate::distribution_core::model::ClusterMembership>, AppError> {
    if !claims.modify || !is_admin_claim(&claims) {
        return Err(AppError::Forbidden(
            "Admin modify permission required".to_string(),
        ));
    }
    let (_, next) = stage_rollback_membership(epoch).await?;
    let applied = commit_membership_change(next).await?;
    schedule_n2_auto_maintenance("membership_rollback", applied.epoch);
    Ok(AxumJson(applied))
}

pub async fn get_n2_governance_state(
    claims: Claims,
) -> Result<AxumJson<crate::distribution_core::model::N2GovernanceStateResponse>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".to_string()));
    }
    Ok(AxumJson(governance_state_view().await?))
}

pub async fn post_n2_internal_governance_vote(
    headers: HeaderMap,
    AxumJson(payload): AxumJson<GovernanceVoteRequest>,
) -> Result<AxumJson<crate::distribution_core::model::GovernanceVoteResponse>, AppError> {
    Ok(AxumJson(handle_governance_vote(&headers, payload).await?))
}

pub async fn post_n2_internal_governance_append(
    headers: HeaderMap,
    AxumJson(payload): AxumJson<GovernanceAppendRequest>,
) -> Result<AxumJson<crate::distribution_core::model::GovernanceAppendResponse>, AppError> {
    let committed_index = payload
        .entry
        .as_ref()
        .map(|entry| entry.index)
        .filter(|entry_index| payload.leader_commit_index >= *entry_index);
    let response = handle_governance_append(&headers, payload).await?;
    if response.accepted && committed_index.is_some() {
        schedule_n2_auto_maintenance("governance_append_commit", response.commit_index);
    }
    Ok(AxumJson(response))
}

pub async fn post_n2_internal_governance_abort(
    headers: HeaderMap,
    AxumJson(payload): AxumJson<GovernanceAbortRequest>,
) -> Result<AxumJson<crate::distribution_core::model::GovernanceAbortResponse>, AppError> {
    Ok(AxumJson(handle_governance_abort(&headers, payload).await?))
}

pub async fn resolve_n2_placement(
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
    Query(query): Query<ResolvePlacementQuery>,
) -> Result<AxumJson<crate::distribution_core::model::ResolvePlacementResponse>, AppError> {
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
    let object_key = object_key.trim_start_matches('/').to_string();
    let object_id = resolve_n2_object_id_local(fragment.as_str(), object_key.as_str())
        .await?
        .unwrap_or_else(|| preview_object_id(uid.as_str(), fragment.as_str(), object_key.as_str()));

    let identity = ObjectIdentity {
        uid,
        fragment,
        object_id,
        object_key,
        version_id,
    };
    let response = resolve_placement(identity, kc, mc).await?;
    Ok(AxumJson(response))
}
