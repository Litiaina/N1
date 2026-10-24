//! Receiver-side ownership of one replicated namespace stage. Grants are
//! ephemeral fences, not namespace records or a durable mutation history.
use super::*;
use crate::core::fragment_identity::{PHYSICAL_FRAGMENT_PREFIX, physical_fragment_dir_name};
use crate::distribution::membership::try_active_cluster_runtime;
use axum::extract::DefaultBodyLimit;
use std::sync::Mutex;
use std::time::Instant;
use ulid::Ulid;

pub(super) const PEER_NCNX_LEASE_PATH: &str = "/n2/peer/ncnx-v4/lease";
pub(super) const LEASE_PROTOCOL_VERSION: u32 = 1;
pub(super) const MAX_LEASE_TTL_MS: u64 = 60_000;
const MAX_ACTIVE_GRANTS: usize = 8192;
const MAX_OWNER_BYTES: usize = 96;
const MAX_TOKEN_BYTES: usize = 96;
const MAX_LEASE_BODY_BYTES: usize = 1024 * 1024;
const MAX_LEASE_RPC_CONCURRENCY: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum PeerNcnxLeaseAction {
    Acquire,
    Renew,
    Release,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct PeerNcnxLeaseKey {
    pub logical_key: String,
    pub root_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct PeerNcnxLeaseRequest {
    pub action: PeerNcnxLeaseAction,
    pub cluster_id: String,
    pub pool_id: String,
    pub pool_generation: u64,
    pub target_slot_id: String,
    pub target_instance_id: String,
    pub binding_epoch: u64,
    pub fragment_id: String,
    pub owner_id: String,
    pub ttl_ms: u64,
    pub items: Vec<PeerNcnxLeaseKey>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct PeerNcnxLeaseGrant {
    pub logical_key: String,
    pub root_id: String,
    pub token: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct PeerNcnxLeaseResponse {
    pub protocol_version: u32,
    pub boot_id: String,
    pub ttl_ms: u64,
    pub items: Vec<PeerNcnxLeaseGrant>,
}

type Key = (String, String);

#[derive(Clone, Debug, Eq, PartialEq)]
struct Binding {
    cluster_id: String,
    pool_id: String,
    pool_generation: u64,
    node_id: String,
    instance_id: String,
    binding_epoch: u64,
    root_id: String,
    physical_root_id: String,
    root_path: String,
}

struct Grant {
    binding: Binding,
    owner: String,
    token: String,
    expires: Instant,
}

struct PreparedKey {
    key: Key,
    binding: Binding,
    token: Option<String>,
}

struct LeaseRegistry {
    boot_id: String,
    grants: BTreeMap<Key, Grant>,
    capacity: usize,
}

impl LeaseRegistry {
    fn new(capacity: usize) -> Self {
        Self {
            boot_id: Ulid::new().to_string(),
            grants: BTreeMap::new(),
            capacity,
        }
    }

    fn apply_at(
        &mut self,
        request: &PeerNcnxLeaseRequest,
        keys: &[PreparedKey],
        now: Instant,
    ) -> Result<PeerNcnxLeaseResponse, AppError> {
        // This traversal is strictly bounded by capacity. It never visits the
        // namespace or spawns expiry tasks. A replacement grant still needs
        // the same key guard retained by every admitted filesystem worker.
        self.grants.retain(|_, grant| grant.expires > now);
        let mut additions = 0usize;
        for item in keys {
            match request.action {
                PeerNcnxLeaseAction::Acquire => {
                    if let Some(existing) = self.grants.get(&item.key) {
                        if existing.binding != item.binding || existing.owner != request.owner_id {
                            return Err(AppError::Conflict(
                                "NCNX key stage is already owned".into(),
                            ));
                        }
                    } else {
                        additions += 1;
                    }
                }
                PeerNcnxLeaseAction::Renew | PeerNcnxLeaseAction::Release => {
                    self.validate_at(
                        &item.key,
                        &item.binding,
                        item.token.as_deref().unwrap_or(""),
                        now,
                    )?;
                    if self.grants[&item.key].owner != request.owner_id {
                        return Err(AppError::Conflict("NCNX stage owner does not match".into()));
                    }
                }
            }
        }
        if self.grants.len().saturating_add(additions) > self.capacity {
            return Err(AppError::ServiceUnavailable(
                "NCNX stage grant budget is full".into(),
            ));
        }
        let mut items = Vec::with_capacity(keys.len());
        for item in keys {
            let grant = match request.action {
                PeerNcnxLeaseAction::Acquire => {
                    self.grants
                        .entry(item.key.clone())
                        .or_insert_with(|| Grant {
                            binding: item.binding.clone(),
                            owner: request.owner_id.clone(),
                            token: format!("{}:{}", self.boot_id, Ulid::new()),
                            expires: now + Duration::from_millis(request.ttl_ms),
                        })
                }
                PeerNcnxLeaseAction::Renew => {
                    let grant = self.grants.get_mut(&item.key).expect("validated grant");
                    grant.expires = now + Duration::from_millis(request.ttl_ms);
                    grant
                }
                PeerNcnxLeaseAction::Release => {
                    let grant = self.grants.remove(&item.key).expect("validated grant");
                    items.push(PeerNcnxLeaseGrant {
                        logical_key: item.key.1.clone(),
                        root_id: item.binding.root_id.clone(),
                        token: grant.token,
                    });
                    continue;
                }
            };
            items.push(PeerNcnxLeaseGrant {
                logical_key: item.key.1.clone(),
                root_id: item.binding.root_id.clone(),
                token: grant.token.clone(),
            });
        }
        // Retrying an acquisition does not renew or resurrect it. Report a
        // conservative lifetime for the earliest grant in a grouped reply.
        let ttl_ms = keys
            .iter()
            .filter_map(|item| self.grants.get(&item.key))
            .map(|grant| grant.expires.saturating_duration_since(now).as_millis() as u64)
            .min()
            .unwrap_or(0);
        Ok(PeerNcnxLeaseResponse {
            protocol_version: LEASE_PROTOCOL_VERSION,
            boot_id: self.boot_id.clone(),
            ttl_ms,
            items,
        })
    }

    fn validate_at(
        &self,
        key: &Key,
        binding: &Binding,
        token: &str,
        now: Instant,
    ) -> Result<(), AppError> {
        let grant = self
            .grants
            .get(key)
            .ok_or_else(|| AppError::Conflict("NCNX stage grant is absent or expired".into()))?;
        if token.is_empty()
            || grant.token != token
            || grant.binding != *binding
            || grant.expires <= now
        {
            return Err(AppError::Conflict(
                "NCNX stage token is stale, expired, or misbound".into(),
            ));
        }
        Ok(())
    }
}

static LEASES: once_cell::sync::Lazy<Mutex<LeaseRegistry>> =
    once_cell::sync::Lazy::new(|| Mutex::new(LeaseRegistry::new(MAX_ACTIVE_GRANTS)));
static LEASE_RPC_BUDGET: once_cell::sync::Lazy<Arc<Semaphore>> =
    once_cell::sync::Lazy::new(|| Arc::new(Semaphore::new(MAX_LEASE_RPC_CONCURRENCY)));

fn canonical_fragment(fragment_id: &str) -> Result<String, AppError> {
    let stable = fragment_id
        .strip_prefix(PHYSICAL_FRAGMENT_PREFIX)
        .unwrap_or(fragment_id);
    let canonical = physical_fragment_dir_name(fragment_id);
    if stable.trim().is_empty()
        || matches!(stable, "." | "..")
        || fragment_id.contains(['/', '\\', '\0'])
        || canonical.len() > 255
    {
        return Err(AppError::BadRequest(
            "NCNX fragment must be a valid single directory identity".into(),
        ));
    }
    Ok(canonical)
}

fn ensure_current_runtime(runtime: &ClusterRuntime) -> Result<(), AppError> {
    if let Some(active) = try_active_cluster_runtime()
        && (active.membership.cluster_id != runtime.membership.cluster_id
            || active.membership.binding_epoch != runtime.membership.binding_epoch
            || active.local_node_id != runtime.local_node_id
            || active.local_instance_id != runtime.local_instance_id
            || active.local_is_reserve != runtime.local_is_reserve
            || active.local_storage_roots != runtime.local_storage_roots
            || active.membership.pools != runtime.membership.pools)
    {
        return Err(AppError::Conflict(
            "NCNX stage request captured a stale cluster binding".into(),
        ));
    }
    Ok(())
}

fn binding_for(
    runtime: &ClusterRuntime,
    pool_id: &str,
    pool_generation: u64,
    fragment_id: &str,
    key: &str,
    root_id: &str,
) -> Result<Binding, AppError> {
    ensure_current_runtime(runtime)?;
    let pool = runtime
        .membership
        .pool(pool_id)
        .ok_or_else(|| AppError::Conflict("unknown NCNX stage pool".into()))?;
    let node = runtime
        .membership
        .nodes
        .iter()
        .find(|node| node.id == runtime.local_node_id)
        .ok_or_else(|| AppError::Conflict("NCNX stage receiver has no bound slot".into()))?;
    if runtime.local_is_reserve
        || pool.generation != pool_generation
        || !pool.node_ids.contains(&runtime.local_node_id)
        || node.instance_id != runtime.local_instance_id
        || node.binding_epoch > runtime.membership.binding_epoch
    {
        return Err(AppError::Conflict(
            "NCNX stage receiver is stale or not bound to the requested pool".into(),
        ));
    }
    let layout = build_ncnx_layout(runtime, pool, fragment_id, key)?;
    let assigned = layout
        .replicas
        .iter()
        .find(|replica| replica.node_id == runtime.local_node_id)
        .ok_or_else(|| AppError::Forbidden("NCNX receiver is not an exact key holder".into()))?;
    if assigned.root_id != root_id
        || assigned.instance_id != runtime.local_instance_id
        || assigned.binding_epoch != runtime.membership.binding_epoch
    {
        return Err(AppError::Conflict(
            "NCNX stage root or instance does not match exact placement".into(),
        ));
    }
    let root = runtime
        .local_storage_roots
        .iter()
        .find(|root| root.id == root_id)
        .ok_or_else(|| AppError::Conflict("NCNX stage root is not enrolled on receiver".into()))?;
    Ok(Binding {
        cluster_id: runtime.membership.cluster_id.clone(),
        pool_id: pool_id.to_string(),
        pool_generation,
        node_id: runtime.local_node_id.clone(),
        instance_id: runtime.local_instance_id.clone(),
        binding_epoch: runtime.membership.binding_epoch,
        root_id: root.id.clone(),
        physical_root_id: root.physical_id().to_string(),
        root_path: root.path.clone(),
    })
}

fn prepare(
    runtime: &ClusterRuntime,
    request: &PeerNcnxLeaseRequest,
) -> Result<Vec<PreparedKey>, AppError> {
    if request.items.is_empty() || request.items.len() > PEER_NCNX_BATCH_MAX {
        return Err(AppError::BadRequest(format!(
            "NCNX stage requires 1..={PEER_NCNX_BATCH_MAX} keys"
        )));
    }
    if request.owner_id.is_empty()
        || request.owner_id.len() > MAX_OWNER_BYTES
        || request.owner_id.chars().any(char::is_control)
    {
        return Err(AppError::BadRequest("invalid NCNX stage owner".into()));
    }
    if request.action != PeerNcnxLeaseAction::Release
        && !(1..=MAX_LEASE_TTL_MS).contains(&request.ttl_ms)
    {
        return Err(AppError::BadRequest(format!(
            "NCNX stage duration must be 1..={MAX_LEASE_TTL_MS} milliseconds"
        )));
    }
    if request.cluster_id != runtime.membership.cluster_id
        || request.target_slot_id != runtime.local_node_id
        || request.target_instance_id != runtime.local_instance_id
        || request.binding_epoch != runtime.membership.binding_epoch
    {
        return Err(AppError::Conflict(
            "NCNX stage targets a different cluster or receiver binding".into(),
        ));
    }
    let fragment = canonical_fragment(&request.fragment_id)?;
    let stable_fragment = fragment
        .strip_prefix(PHYSICAL_FRAGMENT_PREFIX)
        .expect("canonical fragment");
    let mut seen = BTreeSet::new();
    request
        .items
        .iter()
        .map(|item| {
            let key = validate_logical_key(&item.logical_key)?;
            if !seen.insert(key.clone()) {
                return Err(AppError::BadRequest(
                    "duplicate normalized NCNX stage key".into(),
                ));
            }
            match request.action {
                PeerNcnxLeaseAction::Acquire if item.token.is_some() => {
                    return Err(AppError::BadRequest(
                        "NCNX acquisition cannot choose its grant token".into(),
                    ));
                }
                PeerNcnxLeaseAction::Renew | PeerNcnxLeaseAction::Release
                    if item
                        .token
                        .as_ref()
                        .is_none_or(|token| token.is_empty() || token.len() > MAX_TOKEN_BYTES) =>
                {
                    return Err(AppError::BadRequest(
                        "NCNX renewal or release requires its exact token".into(),
                    ));
                }
                _ => {}
            }
            Ok(PreparedKey {
                binding: binding_for(
                    runtime,
                    &request.pool_id,
                    request.pool_generation,
                    stable_fragment,
                    &key,
                    &item.root_id,
                )?,
                key: (fragment.clone(), key),
                token: item.token.clone(),
            })
        })
        .collect()
}

/// Caller must retain the receiver-local guards for these keys across this
/// validation and all admitted filesystem work. Validation is synchronous so
/// it can run inside that guarded blocking worker immediately before mutation.
pub(super) fn validate_tokens<'a>(
    runtime: &ClusterRuntime,
    pool_id: &str,
    pool_generation: u64,
    fragment_id: &str,
    items: impl IntoIterator<Item = (&'a str, &'a str, &'a str)>,
) -> Result<(), AppError> {
    let fragment = canonical_fragment(fragment_id)?;
    let stable_fragment = fragment
        .strip_prefix(PHYSICAL_FRAGMENT_PREFIX)
        .expect("canonical fragment");
    let mut prepared = Vec::new();
    for (key, root, token) in items {
        if prepared.len() >= PEER_NCNX_BATCH_MAX
            || token.is_empty()
            || token.len() > MAX_TOKEN_BYTES
        {
            return Err(AppError::Conflict(
                "missing or invalid NCNX stage token".into(),
            ));
        }
        let key = validate_logical_key(key)?;
        let binding = binding_for(
            runtime,
            pool_id,
            pool_generation,
            stable_fragment,
            &key,
            root,
        )?;
        prepared.push(((fragment.clone(), key), binding, token));
    }
    if prepared.is_empty() {
        return Err(AppError::BadRequest(
            "NCNX stage validation requires keys".into(),
        ));
    }
    let registry = LEASES
        .lock()
        .map_err(|_| AppError::ServiceUnavailable("NCNX stage registry is poisoned".into()))?;
    let now = Instant::now();
    for (key, binding, token) in prepared {
        registry.validate_at(&key, &binding, token, now)?;
    }
    Ok(())
}

pub(super) async fn run_local_lease(
    runtime: Arc<ClusterRuntime>,
    request: PeerNcnxLeaseRequest,
) -> Result<PeerNcnxLeaseResponse, AppError> {
    let _budget = LEASE_RPC_BUDGET
        .clone()
        .try_acquire_owned()
        .map_err(|_| AppError::ServiceUnavailable("NCNX stage RPC budget is full".into()))?;
    let keys = prepare(&runtime, &request)?;
    let _guards = coordination::lock_keys(
        &request.fragment_id,
        keys.iter().map(|item| item.key.1.as_str()),
    )
    .await?;
    let _binding = crate::distribution::membership::guard_cluster_binding()?;
    // Binding can have changed while waiting for an admitted old worker.
    ensure_current_runtime(&runtime)?;
    let mut registry = LEASES
        .lock()
        .map_err(|_| AppError::ServiceUnavailable("NCNX stage registry is poisoned".into()))?;
    registry.apply_at(&request, &keys, Instant::now())
}

async fn peer_lease_handler(
    headers: HeaderMap,
    State(_state): State<Arc<AppState>>,
    Json(request): Json<PeerNcnxLeaseRequest>,
) -> Result<Json<PeerNcnxLeaseResponse>, AppError> {
    let secret = cluster_secret();
    authorize_configured_lease_peer(&headers, secret.as_deref())?;
    Ok(Json(
        run_local_lease(active_cluster_runtime()?, request).await?,
    ))
}

fn authorize_configured_lease_peer(
    headers: &HeaderMap,
    secret: Option<&str>,
) -> Result<(), AppError> {
    let secret = secret.filter(|secret| !secret.is_empty()).ok_or_else(|| {
        AppError::ServiceUnavailable(
            "NCNX stage coordination requires a cluster peer secret".into(),
        )
    })?;
    let provided = headers
        .get("x-n2-cluster-secret")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if provided != secret {
        return Err(AppError::Forbidden(
            "NCNX stage peer authentication failed".into(),
        ));
    }
    Ok(())
}

pub(super) fn build_lease_peer_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route(PEER_NCNX_LEASE_PATH, post(peer_lease_handler))
        .layer(DefaultBodyLimit::max(MAX_LEASE_BODY_BYTES))
}

#[cfg(test)]
mod tests;
