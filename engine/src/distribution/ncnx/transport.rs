use super::*;

pub(super) async fn send_to_replica(
    runtime: &ClusterRuntime,
    replica: &NcnxReplica,
    request: PeerNcnxRequest,
) -> Result<PeerNcnxResponse, AppError> {
    if replica.node_id == runtime.local_node_id {
        return run_local_apply(Arc::new(runtime.clone()), request).await;
    }
    let client = peer_http_client()?;
    let mut call = client
        .post(format!(
            "{}{}",
            replica.endpoint.trim_end_matches('/'),
            PEER_NCNX_PATH
        ))
        .timeout(PEER_NCNX_TIMEOUT)
        .json(&request);
    if let Some(secret) = cluster_secret() {
        call = call.header("x-n2-cluster-secret", secret);
    }
    let response = call.send().await.map_err(|err| {
        AppError::ServiceUnavailable(format!(
            "NCNX peer '{}' request failed: {err}",
            replica.node_id
        ))
    })?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        if status == StatusCode::CONFLICT {
            return Err(AppError::Conflict(format!(
                "NCNX peer '{}' rejected a conflicting mutation: {body}",
                replica.node_id
            )));
        }
        return Err(AppError::ServiceUnavailable(format!(
            "NCNX peer '{}' returned {status}: {body}",
            replica.node_id
        )));
    }
    response.json().await.map_err(|err| {
        AppError::InternalServerError(format!(
            "decode NCNX peer '{}' response: {err}",
            replica.node_id
        ))
    })
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct PeerNcnxBatchTarget {
    pub(super) pool_id: String,
    pub(super) pool_generation: u64,
    pub(super) node_id: String,
    pub(super) instance_id: String,
    pub(super) binding_epoch: u64,
    pub(super) root_id: String,
    pub(super) endpoint: String,
}

pub(super) async fn send_batch_to_target(
    runtime: &ClusterRuntime,
    target: &PeerNcnxBatchTarget,
    action: PeerNcnxBatchAction,
    fragment_id: &str,
    items: Vec<PeerNcnxBatchItem>,
) -> Result<PeerNcnxBatchResponse, AppError> {
    let request = PeerNcnxBatchRequest {
        action,
        pool_id: target.pool_id.clone(),
        pool_generation: target.pool_generation,
        target_slot_id: target.node_id.clone(),
        target_instance_id: target.instance_id.clone(),
        binding_epoch: target.binding_epoch,
        fragment_id: fragment_id.to_string(),
        root_id: target.root_id.clone(),
        items,
    };
    if target.node_id == runtime.local_node_id {
        return run_local_batch(Arc::new(runtime.clone()), request).await;
    }

    let client = peer_http_client()?;
    let mut call = client
        .post(format!(
            "{}{}",
            target.endpoint.trim_end_matches('/'),
            PEER_NCNX_BATCH_PATH
        ))
        .timeout(PEER_NCNX_TIMEOUT)
        .json(&request);
    if let Some(secret) = cluster_secret() {
        call = call.header("x-n2-cluster-secret", secret);
    }
    let response = call.send().await.map_err(|error| {
        AppError::ServiceUnavailable(format!(
            "NCNX peer '{}' batch request failed: {error}",
            target.node_id
        ))
    })?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        if status == StatusCode::CONFLICT {
            return Err(AppError::Conflict(format!(
                "NCNX peer '{}' rejected a conflicting batch mutation: {body}",
                target.node_id
            )));
        }
        return Err(AppError::ServiceUnavailable(format!(
            "NCNX peer '{}' returned {status} for batch mutation: {body}",
            target.node_id
        )));
    }
    response.json().await.map_err(|error| {
        AppError::InternalServerError(format!(
            "decode NCNX peer '{}' batch response: {error}",
            target.node_id
        ))
    })
}
