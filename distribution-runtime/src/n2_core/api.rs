use crate::enums::app_error_enum::AppError;
use crate::middleware::auth_middleware::ADMIN_KEY;
use crate::n2_core::config::effective_cross_params;
use crate::n2_core::model::{
    ClusterViewQuery, ObjectIdentity, ResolvePlacementQuery, UpsertMembershipRequest,
};
use crate::n2_core::state::{
    get_cluster_view, get_membership, resolve_placement, rollback_membership, upsert_membership,
};
use crate::structs::auth_structs::Claims;
use axum::Json as AxumJson;
use axum::extract::{Path, Query};
use ulid::Ulid;

fn is_admin_claim(claims: &Claims) -> bool {
    claims.fragment == *ADMIN_KEY
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
    Ok(AxumJson(upsert_membership(payload).await?))
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
    Ok(AxumJson(rollback_membership(epoch).await?))
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
