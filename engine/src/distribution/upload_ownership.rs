//! Distributed upload lifecycle authority. K committed metadata acknowledgements
//! intersect M+1 irreversible abort votes; neither age nor namespace absence is
//! a deletion certificate. Peer votes are re-read, never trusted from a caller.
use super::membership::{ClusterNode, ClusterRuntime, active_cluster_runtime};
use super::peer::{cluster_secret, peer_http_client};
use super::upload_lifecycle::{self, UploadIdentity, UploadLifecycleRecord, UploadLifecycleState};
use crate::core::disk_manager::ObjectMetadata;
use crate::enums::app_error_enum::AppError;
use crate::routes::storage_routes::AppState;
use axum::extract::DefaultBodyLimit;
use axum::http::HeaderMap;
use axum::routing::post;
use axum::{Json, Router};
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::{Arc, LazyLock};
use tokio::sync::Semaphore;
use tokio::time::Duration;

const PATH: &str = "/n2/peer/upload-ownership";
static REQUESTS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(128)));

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Action {
    Open,
    Inspect,
    Abort,
    Cleanup,
    ProbeCommitted,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Request {
    action: Action,
    target_slot_id: String,
    target_instance_id: String,
    binding_epoch: u64,
    identity: UploadIdentity,
    #[serde(default)]
    abort_voters: Vec<String>,
    #[serde(default)]
    idle_cutoff_unix: Option<i64>,
    #[serde(default)]
    expected_metadata: Option<ObjectMetadata>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Response {
    node_id: String,
    instance_id: String,
    binding_epoch: u64,
    record: Option<UploadLifecycleRecord>,
    cleaned: bool,
    #[serde(default)]
    committed_match: bool,
}

fn secret() -> Result<String, AppError> {
    cluster_secret()
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| {
            AppError::ServiceUnavailable(
                "upload ownership requires a configured cluster secret".into(),
            )
        })
}

fn holders(
    runtime: &ClusterRuntime,
    identity: &UploadIdentity,
) -> Result<Vec<ClusterNode>, AppError> {
    let pool = runtime
        .membership
        .pool(&identity.pool_id)
        .ok_or_else(|| AppError::Conflict("unknown upload ownership pool".into()))?;
    if identity.cluster_id != runtime.membership.cluster_id
        || pool.generation != identity.pool_generation
        || pool.policy.object_data_shards != usize::from(identity.data_shards)
        || pool.policy.object_parity_shards != usize::from(identity.parity_shards)
    {
        return Err(AppError::Conflict(
            "upload ownership pool contract changed".into(),
        ));
    }
    Ok(runtime.membership.nodes_for_pool(&pool.id))
}

fn request(
    runtime: &ClusterRuntime,
    node: &ClusterNode,
    identity: &UploadIdentity,
    action: Action,
    voters: &[String],
) -> Request {
    Request {
        action,
        target_slot_id: node.id.clone(),
        target_instance_id: node.instance_id.clone(),
        binding_epoch: runtime.membership.binding_epoch,
        identity: identity.clone(),
        abort_voters: voters.to_vec(),
        idle_cutoff_unix: None,
        expected_metadata: None,
    }
}

async fn send(
    runtime: &ClusterRuntime,
    node: &ClusterNode,
    request: Request,
) -> Result<Response, AppError> {
    let identity = request.identity.clone();
    let response = if node.id == runtime.local_node_id {
        apply(runtime, request).await?
    } else {
        let response = peer_http_client()?
            .post(format!("{}{PATH}", node.endpoint.trim_end_matches('/')))
            .header("x-n2-cluster-secret", secret()?)
            .json(&request)
            .timeout(Duration::from_secs(60))
            .send()
            .await
            .map_err(|e| {
                AppError::ServiceUnavailable(format!("upload ownership peer request: {e}"))
            })?;
        if !response.status().is_success() {
            let status = response.status();
            return Err(AppError::ServiceUnavailable(format!(
                "upload ownership peer returned {status}: {}",
                response.text().await.unwrap_or_default()
            )));
        }
        response.json::<Response>().await.map_err(|e| {
            AppError::ServiceUnavailable(format!("decode upload ownership peer: {e}"))
        })?
    };
    if response.node_id != node.id
        || response.instance_id != node.instance_id
        || response.binding_epoch != runtime.membership.binding_epoch
    {
        return Err(AppError::Conflict(
            "upload ownership response binding changed".into(),
        ));
    }
    if response
        .record
        .as_ref()
        .is_some_and(|record| record.identity != identity)
    {
        return Err(AppError::Conflict(
            "upload ownership peer returned another version".into(),
        ));
    }
    Ok(response)
}

// Separate non-recursive inspect transport: cleanup verifies votes before
// taking its local exclusive guard, avoiding cross-holder callback deadlocks.
async fn inspect_vote(
    runtime: &ClusterRuntime,
    node: &ClusterNode,
    identity: &UploadIdentity,
) -> Result<bool, AppError> {
    if node.id == runtime.local_node_id {
        return Ok(upload_lifecycle::inspect_upload(runtime, identity)
            .await?
            .is_some_and(|r| r.state == UploadLifecycleState::Aborted));
    }
    let req = request(runtime, node, identity, Action::Inspect, &[]);
    let response = peer_http_client()?
        .post(format!("{}{PATH}", node.endpoint.trim_end_matches('/')))
        .header("x-n2-cluster-secret", secret()?)
        .json(&req)
        .timeout(Duration::from_secs(20))
        .send()
        .await
        .map_err(|e| AppError::ServiceUnavailable(format!("inspect abort vote: {e}")))?;
    if !response.status().is_success() {
        return Ok(false);
    }
    let response = response
        .json::<Response>()
        .await
        .map_err(|e| AppError::ServiceUnavailable(format!("decode abort vote: {e}")))?;
    Ok(response.node_id == node.id
        && response.instance_id == node.instance_id
        && response.binding_epoch == runtime.membership.binding_epoch
        && response
            .record
            .is_some_and(|r| r.identity == *identity && r.state == UploadLifecycleState::Aborted))
}

async fn apply(runtime: &ClusterRuntime, req: Request) -> Result<Response, AppError> {
    if req.target_slot_id != runtime.local_node_id
        || req.target_instance_id != runtime.local_instance_id
        || req.binding_epoch != runtime.membership.binding_epoch
    {
        return Err(AppError::Conflict(
            "upload ownership receiver binding changed".into(),
        ));
    }
    let nodes = holders(runtime, &req.identity)?;
    if req.action == Action::ProbeCommitted {
        let expected = req
            .expected_metadata
            .ok_or_else(|| AppError::BadRequest("commit probe requires metadata".into()))?;
        if UploadIdentity::from_metadata(runtime, &expected) != req.identity {
            return Err(AppError::Conflict("commit probe identity differs".into()));
        }
        let committed_match =
            upload_lifecycle::probe_committed_metadata(runtime, &expected).await?;
        return Ok(Response {
            node_id: runtime.local_node_id.clone(),
            instance_id: runtime.local_instance_id.clone(),
            binding_epoch: runtime.membership.binding_epoch,
            record: None,
            cleaned: false,
            committed_match,
        });
    }
    let (record, cleaned) = match req.action {
        Action::Open => (
            Some(upload_lifecycle::ensure_open(runtime, &req.identity).await?),
            false,
        ),
        Action::Inspect => (
            upload_lifecycle::inspect_upload(runtime, &req.identity).await?,
            false,
        ),
        Action::Abort => (
            Some(
                upload_lifecycle::abort_upload_if_idle(
                    runtime,
                    &req.identity,
                    req.idle_cutoff_unix,
                )
                .await?,
            ),
            false,
        ),
        Action::Cleanup => {
            let voters = req.abort_voters.iter().collect::<BTreeSet<_>>();
            let required = usize::from(req.identity.parity_shards) + 1;
            if voters.len() != req.abort_voters.len()
                || voters.len() > nodes.len()
                || voters.iter().any(|id| !nodes.iter().any(|n| &n.id == *id))
            {
                return Err(AppError::Conflict("invalid distinct abort voters".into()));
            }
            let mut confirmed = 0;
            for node in nodes.iter().filter(|n| voters.contains(&n.id)) {
                if inspect_vote(runtime, node, &req.identity).await? {
                    confirmed += 1;
                }
            }
            if confirmed < required {
                return Err(AppError::ServiceUnavailable(
                    "M+1 durable abort votes are not proven".into(),
                ));
            }
            // A holder that missed the first vote round is fenced now before
            // any physical deletion. Committed local state always refuses.
            upload_lifecycle::abort_upload(runtime, &req.identity).await?;
            let guard = upload_lifecycle::guard_aborted_cleanup(runtime, &req.identity).await?;
            guard.cleanup_aborted_payload().await?;
            (Some(guard.record.clone()), true)
        }
        Action::ProbeCommitted => unreachable!("handled above"),
    };
    Ok(Response {
        node_id: runtime.local_node_id.clone(),
        instance_id: runtime.local_instance_id.clone(),
        binding_epoch: runtime.membership.binding_epoch,
        record,
        cleaned,
        committed_match: false,
    })
}

async fn peer(headers: HeaderMap, Json(req): Json<Request>) -> Result<Json<Response>, AppError> {
    if headers
        .get("x-n2-cluster-secret")
        .and_then(|v| v.to_str().ok())
        != Some(secret()?.as_str())
    {
        return Err(AppError::Forbidden(
            "invalid upload ownership peer authorization".into(),
        ));
    }
    let _permit = REQUESTS
        .clone()
        .try_acquire_owned()
        .map_err(|_| AppError::ServiceUnavailable("upload ownership admission is full".into()))?;
    let runtime = active_cluster_runtime()?;
    apply(&runtime, req).await.map(Json)
}

pub fn build_upload_ownership_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route(PATH, post(peer))
        .layer(DefaultBodyLimit::max(16 * 1024 * 1024))
}

/// Register one version on reachable exact pool holders before any shard PUT.
pub async fn register_upload(
    runtime: &ClusterRuntime,
    identity: &UploadIdentity,
) -> Result<(), AppError> {
    secret()?;
    let nodes = holders(runtime, identity)?;
    let calls = nodes
        .iter()
        .map(|n| send(runtime, n, request(runtime, n, identity, Action::Open, &[])))
        .collect::<Vec<_>>();
    let results = stream::iter(calls)
        .buffer_unordered(16)
        .collect::<Vec<_>>()
        .await;
    let count = results.iter().filter(|r| matches!(r, Ok(v) if v.record.as_ref().is_some_and(|r| r.state == UploadLifecycleState::Open))).count();
    if count < usize::from(identity.data_shards) {
        return Err(AppError::ServiceUnavailable(
            "upload ownership did not reach K holders".into(),
        ));
    }
    Ok(())
}

/// Explicit cancellation or GC use the same terminal quorum, then require all
/// holders' exact cleanup acknowledgements. Partial cleanup remains retryable.
pub async fn abort_and_reclaim(
    runtime: &ClusterRuntime,
    identity: &UploadIdentity,
) -> Result<(), AppError> {
    abort_and_reclaim_if_idle(runtime, identity, None).await
}

pub async fn abort_and_reclaim_if_idle(
    runtime: &ClusterRuntime,
    identity: &UploadIdentity,
    cutoff: Option<i64>,
) -> Result<(), AppError> {
    secret()?;
    let nodes = holders(runtime, identity)?;
    let calls = nodes
        .iter()
        .map(|n| async move {
            let mut req = request(runtime, n, identity, Action::Abort, &[]);
            req.idle_cutoff_unix = cutoff;
            let response = send(runtime, n, req).await?;
            if response
                .record
                .is_some_and(|r| r.state == UploadLifecycleState::Aborted)
            {
                Ok(n.id.clone())
            } else {
                Err(AppError::Conflict("holder did not vote aborted".into()))
            }
        })
        .collect::<Vec<_>>();
    let votes = stream::iter(calls)
        .buffer_unordered(16)
        .collect::<Vec<Result<String, AppError>>>()
        .await;
    let voters = votes.into_iter().filter_map(Result::ok).collect::<Vec<_>>();
    if voters.len() < usize::from(identity.parity_shards) + 1 {
        return Err(AppError::ServiceUnavailable(
            "upload retained: M+1 abort votes unavailable (possibly committed)".into(),
        ));
    }
    let calls = nodes
        .iter()
        .map(|n| {
            send(
                runtime,
                n,
                request(runtime, n, identity, Action::Cleanup, &voters),
            )
        })
        .collect::<Vec<_>>();
    let cleaned = stream::iter(calls)
        .buffer_unordered(16)
        .collect::<Vec<_>>()
        .await;
    for result in cleaned {
        if !result?.cleaned {
            return Err(AppError::ServiceUnavailable(
                "upload cleanup incomplete".into(),
            ));
        }
    }
    Ok(())
}

/// Bootstrap replacement/legacy metadata only from K exact immutable NCDX
/// copies, not a caller assertion or the existence of a namespace pointer.
pub async fn verify_committed_metadata(
    runtime: &ClusterRuntime,
    metadata: &ObjectMetadata,
) -> Result<(), AppError> {
    let identity = UploadIdentity::from_metadata(runtime, metadata);
    let nodes = holders(runtime, &identity)?;
    let mut count = 0;
    for node in nodes {
        let mut req = request(runtime, &node, &identity, Action::ProbeCommitted, &[]);
        req.expected_metadata = Some(metadata.clone());
        if send(runtime, &node, req)
            .await
            .is_ok_and(|r| r.committed_match)
        {
            count += 1;
        }
    }
    if count < usize::from(identity.data_shards) {
        return Err(AppError::ServiceUnavailable(
            "K matching committed NCDX copies are not proven".into(),
        ));
    }
    Ok(())
}
