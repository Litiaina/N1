use super::*;

async fn apply_fragment_control(request: &PeerFragmentControlRequest) -> Result<(), AppError> {
    crate::core::operation_gate::ensure_client_mutation_allowed()?;
    match request {
        PeerFragmentControlRequest::Provision {
            fragment_hash,
            stable_fragment_id,
            key,
        } => {
            crate::core::fragment_manager::provision_fragment_local(
                fragment_hash,
                stable_fragment_id,
                key,
            )
            .await
        }
        PeerFragmentControlRequest::Rename {
            current_hash,
            new_hash,
            stable_fragment_id,
        } => {
            crate::core::fragment_manager::rename_fragment_alias_local(
                current_hash,
                new_hash,
                stable_fragment_id,
            )
            .await
        }
        PeerFragmentControlRequest::RotateKey { fragment_hash, key } => {
            crate::core::fragment_manager::rotate_fragment_key_local(fragment_hash, key).await
        }
        PeerFragmentControlRequest::Delete { stable_fragment_id } => {
            crate::core::fragment_manager::delete_fragment_local(stable_fragment_id)
        }
    }
}

pub async fn replicate_fragment_control(
    request: PeerFragmentControlRequest,
) -> Result<(), AppError> {
    let runtime = active_cluster_runtime()?;
    let client = peer_http_client()?;
    for node in runtime
        .membership
        .nodes
        .iter()
        .filter(|node| node.id != runtime.local_node_id)
    {
        let mut builder = client
            .post(format!(
                "{}/n2/peer/fragments/control",
                node.endpoint.trim_end_matches('/')
            ))
            .json(&request);
        if let Some(secret) = cluster_secret() {
            builder = builder.header("x-n2-cluster-secret", secret);
        }
        let response = timeout(Duration::from_secs(20), builder.send())
            .await
            .map_err(|_| {
                AppError::ServiceUnavailable(format!("fragment control timed out at {}", node.id))
            })?
            .map_err(|err| {
                AppError::ServiceUnavailable(format!(
                    "fragment control failed at {}: {err}",
                    node.id
                ))
            })?;
        if !response.status().is_success() {
            return Err(AppError::ServiceUnavailable(format!(
                "fragment control returned {} from {}",
                response.status(),
                node.id
            )));
        }
    }
    apply_fragment_control(&request).await
}

pub(super) async fn peer_fragment_snapshot(
    headers: HeaderMap,
) -> Result<impl IntoResponse, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(
        crate::core::fragment_manager::local_fragment_control_snapshot().await?,
    ))
}

pub async fn sync_fragment_controls_from_cluster() -> Result<usize, AppError> {
    if crate::core::operation_gate::is_cluster_safety_read_only() {
        return Ok(0);
    }
    let runtime = active_cluster_runtime()?;
    let client = peer_http_client()?;
    let mut by_alias =
        BTreeMap::<String, crate::core::fragment_manager::FragmentControlSnapshot>::new();

    for node in runtime
        .membership
        .nodes
        .iter()
        .filter(|node| node.id != runtime.local_node_id)
    {
        let mut request = client.get(format!(
            "{}/n2/peer/fragments/snapshot",
            node.endpoint.trim_end_matches('/')
        ));
        if let Some(secret) = cluster_secret() {
            request = request.header("x-n2-cluster-secret", secret);
        }
        let response = match timeout(Duration::from_secs(10), request.send()).await {
            Ok(Ok(response)) if response.status().is_success() => response,
            _ => continue,
        };
        let snapshots = response
            .json::<Vec<crate::core::fragment_manager::FragmentControlSnapshot>>()
            .await
            .map_err(|err| {
                AppError::InternalServerError(format!(
                    "decode fragment control snapshot from {}: {err}",
                    node.id
                ))
            })?;
        for snapshot in snapshots {
            if let Some(existing) = by_alias.get(&snapshot.fragment_hash)
                && existing.metadata.fragment != snapshot.metadata.fragment
            {
                return Err(AppError::Conflict(format!(
                    "fragment alias {} resolves to conflicting stable identities {} and {}",
                    snapshot.fragment_hash, existing.metadata.fragment, snapshot.metadata.fragment
                )));
            }
            by_alias
                .entry(snapshot.fragment_hash.clone())
                .or_insert(snapshot);
        }
    }

    let mut applied = 0usize;
    for snapshot in by_alias.into_values() {
        if let Ok(local) =
            crate::core::fragment_metadata::read_fragment_metadata(&snapshot.fragment_hash).await
            && local.fragment == snapshot.metadata.fragment
            && local.updated >= snapshot.metadata.updated
        {
            continue;
        }
        crate::core::fragment_manager::apply_fragment_control_snapshot_local(&snapshot).await?;
        applied += 1;
    }
    Ok(applied)
}

#[derive(Clone, Debug, Serialize)]
struct PeerFragmentSyncResponse {
    applied: usize,
}

pub(super) async fn peer_fragment_sync(headers: HeaderMap) -> Result<impl IntoResponse, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(PeerFragmentSyncResponse {
        applied: sync_fragment_controls_from_cluster().await?,
    }))
}

pub fn spawn_fragment_control_sync_task() {
    tokio::spawn(async {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            match sync_fragment_controls_from_cluster().await {
                Ok(0) => {}
                Ok(count) => {
                    tracing::debug!("synchronized {count} N2 fragment control record(s)")
                }
                Err(err) => tracing::warn!("fragment control synchronization failed: {err}"),
            }
        }
    });
}

pub(super) async fn peer_fragment_control(
    headers: HeaderMap,
    Json(request): Json<PeerFragmentControlRequest>,
) -> Result<impl IntoResponse, AppError> {
    authorize_peer(&headers)?;
    apply_fragment_control(&request).await?;
    Ok(StatusCode::NO_CONTENT)
}
