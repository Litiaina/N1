//! Coordinator ownership of a complete replicated NCNX stage. No bytes are
//! published until every exact holder has granted its receiver-issued fence.
use super::*;
use futures_util::StreamExt;
use lease::{
    LEASE_PROTOCOL_VERSION, MAX_LEASE_TTL_MS, PEER_NCNX_LEASE_PATH, PeerNcnxLeaseAction,
    PeerNcnxLeaseKey, PeerNcnxLeaseRequest, PeerNcnxLeaseResponse,
};
use tokio::sync::OwnedSemaphorePermit;

const MAX_ACTIVE_STAGES: usize = 64;
static STAGES: once_cell::sync::Lazy<Arc<Semaphore>> =
    once_cell::sync::Lazy::new(|| Arc::new(Semaphore::new(MAX_ACTIVE_STAGES)));

pub(super) async fn join_calls<F, I>(calls: I) -> Vec<F::Output>
where
    F: std::future::Future + Send,
    F::Output: Send,
    I: IntoIterator<Item = F> + Send,
    I::IntoIter: Send,
{
    // Materialize the bounded target list before awaiting. Besides avoiding
    // lifetime-erased lazy iterator captures, only sixteen RPCs are polled.
    let calls = calls.into_iter().collect::<Vec<_>>();
    futures_util::stream::iter(calls)
        .buffer_unordered(16)
        .collect()
        .await
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Target {
    pool_id: String,
    generation: u64,
    node_id: String,
    instance_id: String,
    binding_epoch: u64,
    endpoint: String,
}

impl Target {
    fn for_replica(layout: &NcnxLayout, replica: &NcnxReplica) -> Self {
        Self {
            pool_id: layout.pool_id.clone(),
            generation: layout.pool_generation,
            node_id: replica.node_id.clone(),
            instance_id: replica.instance_id.clone(),
            binding_epoch: replica.binding_epoch,
            endpoint: replica.endpoint.clone(),
        }
    }
}

pub(super) struct StageOwnership {
    runtime: Arc<ClusterRuntime>,
    fragment: String,
    owner: String,
    held: BTreeMap<Target, Vec<PeerNcnxLeaseKey>>,
    permit: Option<OwnedSemaphorePermit>,
}

async fn call(
    runtime: &ClusterRuntime,
    target: &Target,
    request: PeerNcnxLeaseRequest,
) -> Result<PeerNcnxLeaseResponse, AppError> {
    if target.node_id == runtime.local_node_id {
        return lease::run_local_lease(Arc::new(runtime.clone()), request).await;
    }
    let client = peer_http_client()?;
    let mut call = client
        .post(format!(
            "{}{}",
            target.endpoint.trim_end_matches('/'),
            PEER_NCNX_LEASE_PATH
        ))
        .timeout(PEER_NCNX_TIMEOUT)
        .json(&request);
    if let Some(secret) = cluster_secret() {
        call = call.header("x-n2-cluster-secret", secret);
    }
    let response = call.send().await.map_err(|error| {
        AppError::ServiceUnavailable(format!(
            "NCNX stage peer '{}' is unavailable: {error}",
            target.node_id
        ))
    })?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(if status == StatusCode::CONFLICT {
            AppError::Conflict(format!(
                "NCNX stage is busy or stale on '{}': {body}",
                target.node_id
            ))
        } else {
            AppError::ServiceUnavailable(format!(
                "NCNX stage peer '{}' returned {status}: {body}",
                target.node_id
            ))
        });
    }
    response.json().await.map_err(|error| {
        AppError::ServiceUnavailable(format!(
            "invalid NCNX stage reply from '{}': {error}",
            target.node_id
        ))
    })
}

fn request(
    runtime: &ClusterRuntime,
    target: &Target,
    fragment: &str,
    owner: &str,
    action: PeerNcnxLeaseAction,
    items: Vec<PeerNcnxLeaseKey>,
) -> PeerNcnxLeaseRequest {
    PeerNcnxLeaseRequest {
        action,
        cluster_id: runtime.membership.cluster_id.clone(),
        pool_id: target.pool_id.clone(),
        pool_generation: target.generation,
        target_slot_id: target.node_id.clone(),
        target_instance_id: target.instance_id.clone(),
        binding_epoch: target.binding_epoch,
        fragment_id: fragment.to_string(),
        owner_id: owner.to_string(),
        ttl_ms: MAX_LEASE_TTL_MS,
        items,
    }
}

impl StageOwnership {
    pub(super) async fn acquire(
        runtime: Arc<ClusterRuntime>,
        fragment: &str,
        keys: &[(NcnxLayout, String)],
    ) -> Result<Self, AppError> {
        if keys.is_empty() || keys.len() > PEER_NCNX_BATCH_MAX {
            return Err(AppError::BadRequest(format!(
                "NCNX stage requires 1..={PEER_NCNX_BATCH_MAX} keys"
            )));
        }
        let permit = STAGES
            .clone()
            .try_acquire_owned()
            .map_err(|_| AppError::ServiceUnavailable("NCNX active-stage budget is full".into()))?;
        let mut groups = BTreeMap::<Target, BTreeMap<String, String>>::new();
        for (layout, key) in keys {
            let key = validate_logical_key(key)?;
            if layout.replicas.is_empty() {
                return Err(AppError::BadRequest(
                    "NCNX stage has no exact holders".into(),
                ));
            }
            for replica in &layout.replicas {
                groups
                    .entry(Target::for_replica(layout, replica))
                    .or_default()
                    .insert(key.clone(), replica.root_id.clone());
            }
        }
        let mut owned = Self {
            runtime,
            fragment: fragment.to_string(),
            owner: ulid::Ulid::new().to_string(),
            held: BTreeMap::new(),
            permit: Some(permit),
        };
        let calls = groups.into_iter().map(|(target, keys)| {
            let items = keys
                .into_iter()
                .map(|(logical_key, root_id)| PeerNcnxLeaseKey {
                    logical_key,
                    root_id,
                    token: None,
                })
                .collect::<Vec<_>>();
            let request = request(
                &owned.runtime,
                &target,
                fragment,
                &owned.owner,
                PeerNcnxLeaseAction::Acquire,
                items.clone(),
            );
            let runtime = owned.runtime.clone();
            async move {
                let result = call(&runtime, &target, request).await;
                (target, items, result)
            }
        });
        let mut first_error = None;
        for (target, wanted, response) in join_calls(calls).await {
            match response {
                Ok(response) => {
                    let supplied = response
                        .items
                        .iter()
                        .map(|grant| (grant.logical_key.clone(), grant.root_id.clone()))
                        .collect::<BTreeSet<_>>();
                    let expected = wanted
                        .iter()
                        .map(|item| (item.logical_key.clone(), item.root_id.clone()))
                        .collect::<BTreeSet<_>>();
                    if response.protocol_version != LEASE_PROTOCOL_VERSION
                        || response.ttl_ms == 0
                        || supplied != expected
                        || response.items.len() != wanted.len()
                        || response
                            .items
                            .iter()
                            .any(|grant| grant.token.is_empty() || grant.token.len() > 96)
                    {
                        first_error.get_or_insert_with(|| {
                            AppError::ServiceUnavailable(
                                "incomplete or incompatible NCNX stage grant".into(),
                            )
                        });
                    } else {
                        owned.held.insert(
                            target,
                            response
                                .items
                                .into_iter()
                                .map(|grant| PeerNcnxLeaseKey {
                                    logical_key: grant.logical_key,
                                    root_id: grant.root_id,
                                    token: Some(grant.token),
                                })
                                .collect(),
                        );
                    }
                }
                Err(error) => {
                    first_error.get_or_insert_with(|| {
                        AppError::ServiceUnavailable(format!(
                            "NCNX stage acquisition deferred: {error}"
                        ))
                    });
                }
            }
        }
        if let Some(error) = first_error {
            owned.release().await;
            return Err(error);
        }
        Ok(owned)
    }

    pub(super) fn token(
        &self,
        layout: &NcnxLayout,
        replica: &NcnxReplica,
        key: &str,
    ) -> Result<String, AppError> {
        let key = validate_logical_key(key)?;
        self.held
            .get(&Target::for_replica(layout, replica))
            .and_then(|items| {
                items
                    .iter()
                    .find(|item| item.logical_key == key && item.root_id == replica.root_id)
            })
            .and_then(|item| item.token.clone())
            .ok_or_else(|| {
                AppError::Conflict("NCNX stage does not own this exact holder/key".into())
            })
    }

    pub(super) fn attach(
        &self,
        request: &mut PeerNcnxRequest,
        layout: &NcnxLayout,
        replica: &NcnxReplica,
    ) -> Result<(), AppError> {
        request.lease_token = Some(self.token(layout, replica, &request.logical_key)?);
        Ok(())
    }

    pub(super) fn attach_batch(
        &self,
        target: &PeerNcnxBatchTarget,
        items: &mut [PeerNcnxBatchItem],
    ) -> Result<(), AppError> {
        let layout = NcnxLayout {
            pool_id: target.pool_id.clone(),
            pool_generation: target.pool_generation,
            replicas: Vec::new(),
        };
        let replica = NcnxReplica {
            node_id: target.node_id.clone(),
            instance_id: target.instance_id.clone(),
            binding_epoch: target.binding_epoch,
            root_id: target.root_id.clone(),
            endpoint: target.endpoint.clone(),
        };
        for item in items {
            item.lease_token = Some(self.token(&layout, &replica, &item.logical_key)?);
        }
        Ok(())
    }

    pub(super) async fn release(mut self) {
        let held = std::mem::take(&mut self.held);
        release_all(&self.runtime, &self.fragment, &self.owner, held).await;
    }
}

async fn release_all(
    runtime: &ClusterRuntime,
    fragment: &str,
    owner: &str,
    held: BTreeMap<Target, Vec<PeerNcnxLeaseKey>>,
) {
    join_calls(held.into_iter().map(|(target, items)| async move {
        let request = request(
            runtime,
            &target,
            fragment,
            owner,
            PeerNcnxLeaseAction::Release,
            items,
        );
        if let Err(error) = call(runtime, &target, request).await {
            tracing::debug!(%error, "NCNX stage release deferred to receiver expiry");
        }
    }))
    .await;
}

impl Drop for StageOwnership {
    fn drop(&mut self) {
        if self.held.is_empty() {
            return;
        }
        let held = std::mem::take(&mut self.held);
        let runtime = self.runtime.clone();
        let fragment = self.fragment.clone();
        let owner = self.owner.clone();
        let permit = self.permit.take();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _permit = permit;
                release_all(&runtime, &fragment, &owner, held).await;
            });
        }
        // With no executor, exact grants expire on their receivers. No stale
        // request is allowed to bypass that fence after release or expiry.
    }
}
