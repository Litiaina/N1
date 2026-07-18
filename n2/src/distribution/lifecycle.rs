use crate::core::index;
use crate::distribution::membership::{ClusterNode, active_cluster_runtime};
use crate::distribution::peer::{
    authorize_peer, cluster_secret, parse_peer_shard_locator, peer_http_client,
};
use crate::enums::app_error_enum::AppError;
use crate::routes::storage_routes::AppState;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::post;
use axum::{Json, Router};
use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::time::{Duration, timeout};

const LIFECYCLE_PURGE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerObjectPurgeRequest {
    pub fragment: String,
    pub object_id: String,
    pub lifecycle_mutation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerObjectPurgeAck {
    pub node_id: String,
    pub fragment: String,
    pub object_id: String,
}

pub async fn purge_object_from_holders(
    fragment: &str,
    object_id: &str,
    lifecycle_mutation_id: &str,
) -> Result<(), AppError> {
    let versions = index::list_live_object_versions_metadata(fragment, object_id).await?;
    if versions.is_empty() {
        return Ok(());
    }
    let runtime = active_cluster_runtime()?;
    let mut holder_ids = BTreeSet::new();
    for version in &versions {
        for part in &version.parts {
            for shard in &part.shards {
                if let Some(locator) = parse_peer_shard_locator(&shard.absolute_path) {
                    holder_ids.insert(locator.node_id);
                }
            }
        }
    }
    if holder_ids.is_empty() {
        return Err(AppError::InternalServerError(format!(
            "immutable object {fragment}/{object_id} has no shard-holder identities"
        )));
    }
    let holders = holder_ids
        .iter()
        .map(|node_id| {
            runtime
                .membership
                .nodes
                .iter()
                .find(|node| &node.id == node_id)
                .cloned()
                .ok_or_else(|| {
                    AppError::ServiceUnavailable(format!(
                        "object holder '{node_id}' is absent from membership"
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let request = PeerObjectPurgeRequest {
        fragment: fragment.to_string(),
        object_id: object_id.to_string(),
        lifecycle_mutation_id: lifecycle_mutation_id.to_string(),
    };
    let results = join_all(
        holders
            .into_iter()
            .map(|node| purge_on_node(runtime.local_node_id.clone(), node, request.clone())),
    )
    .await;
    let failures = results
        .into_iter()
        .filter_map(Result::err)
        .map(|err| err.to_string())
        .collect::<Vec<_>>();
    if !failures.is_empty() {
        return Err(AppError::ServiceUnavailable(format!(
            "object lifecycle purge is incomplete: {}",
            failures.join("; ")
        )));
    }
    Ok(())
}

async fn purge_on_node(
    local_node_id: String,
    node: ClusterNode,
    request: PeerObjectPurgeRequest,
) -> Result<PeerObjectPurgeAck, AppError> {
    if node.id == local_node_id {
        return local_purge(&local_node_id, request).await;
    }
    let client = peer_http_client()?;
    let mut send = client
        .post(format!(
            "{}/n2/peer/lifecycle/purge-object",
            node.endpoint.trim_end_matches('/')
        ))
        .json(&request);
    if let Some(secret) = cluster_secret() {
        send = send.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(LIFECYCLE_PURGE_TIMEOUT, send.send())
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!("object purge timed out at {}", node.id))
        })?
        .map_err(|err| {
            AppError::ServiceUnavailable(format!("object purge failed at {}: {err}", node.id))
        })?;
    if !response.status().is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "object purge returned {} from {}",
            response.status(),
            node.id
        )));
    }
    response.json().await.map_err(|err| {
        AppError::InternalServerError(format!("decode object purge ack from {}: {err}", node.id))
    })
}

async fn local_purge(
    local_node_id: &str,
    request: PeerObjectPurgeRequest,
) -> Result<PeerObjectPurgeAck, AppError> {
    crate::core::operation_gate::ensure_client_mutation_allowed()?;
    let _ = index::delete_object_fast(&request.fragment, &request.object_id).await?;
    Ok(PeerObjectPurgeAck {
        node_id: local_node_id.to_string(),
        fragment: request.fragment,
        object_id: request.object_id,
    })
}

async fn peer_purge_object(
    headers: HeaderMap,
    State(_state): State<Arc<AppState>>,
    Json(request): Json<PeerObjectPurgeRequest>,
) -> Result<Json<PeerObjectPurgeAck>, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    Ok(Json(local_purge(&runtime.local_node_id, request).await?))
}

pub fn build_lifecycle_peer_routes() -> Router<Arc<AppState>> {
    Router::new().route("/n2/peer/lifecycle/purge-object", post(peer_purge_object))
}
