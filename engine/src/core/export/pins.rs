//! Short-lived, durable fragment fences. Every current member acknowledges a
//! pin before capture starts. Holder-side purge and pin publication share one
//! lock; a partially admitted job is harmless and its pins expire automatically.
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use axum::{Json, Router, http::HeaderMap, routing::post};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, MutexGuard};

use crate::core::disk_manager::FS_STORAGE_ROOT;
use crate::core::fragment_metadata::{durable_atomic_write, resolve_stable_fragment_id};
use crate::distribution::membership::active_cluster_runtime;
use crate::distribution::peer::{authorize_peer, cluster_secret, peer_http_client};
use crate::enums::app_error_enum::AppError;
use crate::routes::storage_routes::AppState;

use super::models::PersistedExport;

static PURGE_GATE: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
const MAX_BUILD_LEASE_SECONDS: i64 = 30 * 24 * 60 * 60;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct SourcePin {
    pub id: String,
    pub fragment: String,
    pub expires_at_unix: i64,
    pub membership_epoch: u64,
    pub binding_epoch: u64,
    pub cluster_id: String,
}

pub(crate) fn identity_hash(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

pub(super) fn new_export_id() -> Result<String, AppError> {
    let runtime = active_cluster_runtime()?;
    if runtime.local_is_reserve {
        return Err(AppError::ServiceUnavailable(
            "reserve nodes cannot own exports".into(),
        ));
    }
    Ok(format!(
        "{}.{}.{}",
        identity_hash(&runtime.local_node_id),
        identity_hash(&runtime.local_instance_id),
        ulid::Ulid::new()
    ))
}

pub(super) fn parse_id(id: &str) -> Result<(&str, &str), AppError> {
    let parts = id.split('.').collect::<Vec<_>>();
    let valid_hash = |s: &str| {
        s.len() == 64
            && s.bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    };
    if parts.len() != 3
        || !valid_hash(parts[0])
        || !valid_hash(parts[1])
        || parts[2].len() != 26
        || ulid::Ulid::from_string(parts[2]).is_err()
    {
        return Err(AppError::BadRequest("invalid export id".into()));
    }
    Ok((parts[0], parts[1]))
}

pub(super) fn ensure_owner(id: &str) -> Result<(), AppError> {
    let (node, instance) = parse_id(id)?;
    let runtime = active_cluster_runtime()?;
    if runtime.local_is_reserve
        || node != identity_hash(&runtime.local_node_id)
        || instance != identity_hash(&runtime.local_instance_id)
    {
        return Err(AppError::ServiceUnavailable(
            "export belongs to another physical coordinator; use its original node or the gateway"
                .into(),
        ));
    }
    Ok(())
}

fn pin_root() -> Result<PathBuf, AppError> {
    FS_STORAGE_ROOT
        .get()
        .map(|root| PathBuf::from(root).join(".n2-export-pins"))
        .ok_or_else(|| AppError::InternalServerError("export pin root is not initialized".into()))
}

async fn install_pin(pin: &SourcePin) -> Result<(), AppError> {
    let _guard = PURGE_GATE.lock().await;
    let now = super::now_unix();
    let (owner, instance) = parse_id(&pin.id)?;
    let runtime = active_cluster_runtime()?;
    if pin.expires_at_unix <= now
        || pin.expires_at_unix > now.saturating_add(MAX_BUILD_LEASE_SECONDS)
        || pin.membership_epoch != runtime.membership.epoch
        || pin.binding_epoch != runtime.membership.binding_epoch
        || pin.cluster_id != runtime.membership.cluster_id
        || !runtime.membership.nodes.iter().any(|node| {
            identity_hash(&node.id) == owner && identity_hash(&node.instance_id) == instance
        })
    {
        return Err(AppError::Conflict(
            "export pin authority expired or membership changed".into(),
        ));
    }
    if pin.fragment.is_empty() {
        return Err(AppError::BadRequest("empty export fragment".into()));
    }
    let bytes =
        serde_json::to_vec(pin).map_err(|e| AppError::InternalServerError(e.to_string()))?;
    let root = pin_root()?;
    durable_atomic_write(&root.join(format!("{}.json", pin.id)), &bytes).await?;
    if let Some(parent) = root.parent() {
        tokio::fs::File::open(parent)
            .await
            .map_err(|e| AppError::Io(e, "open export pin parent".into()))?
            .sync_all()
            .await
            .map_err(|e| AppError::Io(e, "sync export pin parent".into()))?;
    }
    Ok(())
}

// Keep the guard through the actual holder-side purge. Unreadable/corrupt pins
// fail closed: reclaiming space is less important than an in-flight export.
pub(crate) async fn guard_purge(fragment: &str) -> Result<MutexGuard<'static, ()>, AppError> {
    let guard = PURGE_GATE.lock().await;
    let stable = resolve_stable_fragment_id(fragment).await?;
    if pinned_at(&pin_root()?, &stable, super::now_unix()).await? {
        return Err(AppError::Conflict(
            "fragment is retained by an active export".into(),
        ));
    }
    Ok(guard)
}

pub(crate) async fn fragment_is_pinned(fragment: &str) -> Result<bool, AppError> {
    let _guard = PURGE_GATE.lock().await;
    let stable = resolve_stable_fragment_id(fragment).await?;
    pinned_at(&pin_root()?, &stable, super::now_unix()).await
}

#[derive(Serialize, Deserialize)]
struct ReleasePin {
    id: String,
}

async fn remove_pin(id: &str) -> Result<(), AppError> {
    parse_id(id)?;
    let _guard = PURGE_GATE.lock().await;
    match tokio::fs::remove_file(pin_root()?.join(format!("{id}.json"))).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(AppError::Io(e, "remove export pin".into())),
    }
}

// Best effort release; durable expiry is the fallback if a member is offline.
pub(super) async fn release(id: String) {
    let _ = remove_pin(&id).await;
    let (Ok(runtime), Ok(client), Some(secret)) = (
        active_cluster_runtime(),
        peer_http_client(),
        cluster_secret(),
    ) else {
        return;
    };
    for node in &runtime.membership.nodes {
        if node.id == runtime.local_node_id {
            continue;
        }
        let _ = client
            .post(format!(
                "{}/n2/peer/exports/unpin",
                node.endpoint.trim_end_matches('/')
            ))
            .header("x-n2-cluster-secret", &secret)
            .json(&ReleasePin { id: id.clone() })
            .timeout(std::time::Duration::from_secs(3))
            .send()
            .await;
    }
}

async fn peer_unpin(
    headers: HeaderMap,
    Json(request): Json<ReleasePin>,
) -> Result<Json<serde_json::Value>, AppError> {
    if cluster_secret().is_none_or(|s| s.trim().is_empty()) {
        return Err(AppError::Unauthorized);
    }
    authorize_peer(&headers)?;
    remove_pin(&request.id).await?;
    Ok(Json(serde_json::json!({"released": true})))
}

async fn pinned_at(root: &std::path::Path, fragment: &str, now: i64) -> Result<bool, AppError> {
    let mut entries = match tokio::fs::read_dir(root).await {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(AppError::Io(e, "scan export pins".into())),
    };
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| AppError::Io(e, "read export pin entry".into()))?
    {
        if entry
            .path()
            .extension()
            .is_none_or(|extension| extension != "json")
        {
            continue;
        }
        let bytes = tokio::fs::read(entry.path())
            .await
            .map_err(|e| AppError::Io(e, "read export pin".into()))?;
        let pin: SourcePin = serde_json::from_slice(&bytes)
            .map_err(|e| AppError::InternalServerError(format!("decode export pin: {e}")))?;
        if pin.expires_at_unix <= now {
            tokio::fs::remove_file(entry.path())
                .await
                .map_err(|e| AppError::Io(e, "expire export pin".into()))?;
        } else if pin.fragment == fragment {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) async fn acquire(state: &PersistedExport) -> Result<(), AppError> {
    let secret = cluster_secret()
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| {
            AppError::ServiceUnavailable("N2_CLUSTER_SECRET is required for durable exports".into())
        })?;
    let runtime = active_cluster_runtime()?;
    let pin = SourcePin {
        id: state.id.clone(),
        fragment: resolve_stable_fragment_id(&state.fragment).await?,
        expires_at_unix: state.expires_at_unix,
        membership_epoch: state.membership_epoch,
        binding_epoch: state.binding_epoch,
        cluster_id: state.cluster_id.clone(),
    };
    let client = peer_http_client()?;
    for node in &runtime.membership.nodes {
        if node.id == runtime.local_node_id {
            install_pin(&pin).await?;
            continue;
        }
        let response = client
            .post(format!(
                "{}/n2/peer/exports/pin",
                node.endpoint.trim_end_matches('/')
            ))
            .header("x-n2-cluster-secret", &secret)
            .json(&pin)
            .timeout(std::time::Duration::from_secs(15))
            .send()
            .await
            .map_err(|e| {
                AppError::ServiceUnavailable(format!(
                    "export source retention could not reach {}: {e}",
                    node.id
                ))
            })?;
        if !response.status().is_success() {
            return Err(AppError::ServiceUnavailable(format!(
                "export source retention rejected by {} ({})",
                node.id,
                response.status()
            )));
        }
    }
    Ok(())
}

async fn peer_pin(
    headers: HeaderMap,
    Json(pin): Json<SourcePin>,
) -> Result<Json<serde_json::Value>, AppError> {
    if cluster_secret().is_none_or(|s| s.trim().is_empty()) {
        return Err(AppError::Unauthorized);
    }
    authorize_peer(&headers)?;
    install_pin(&pin).await?;
    Ok(Json(serde_json::json!({"pinned": true})))
}

pub(crate) fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/n2/peer/exports/pin", post(peer_pin))
        .route("/n2/peer/exports/unpin", post(peer_unpin))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_ids_are_safe_and_include_both_coordinator_identities() {
        let id = format!(
            "{}.{}.{}",
            identity_hash("slot-a"),
            identity_hash("physical-a"),
            ulid::Ulid::new()
        );
        let (slot, instance) = parse_id(&id).unwrap();
        assert_eq!(slot, identity_hash("slot-a"));
        assert_ne!(instance, identity_hash("replacement-a"));
        assert!(id.len() < 255);
        for bad in ["../secret", "01J00000000000000000000000", "a.b.c", ""] {
            assert!(parse_id(bad).is_err());
        }
    }

    #[tokio::test]
    async fn persisted_pins_survive_reload_expire_and_fail_closed_on_corruption() {
        let root = std::env::temp_dir().join(format!("n2-export-pin-test-{}", ulid::Ulid::new()));
        let pin = SourcePin {
            id: "test".into(),
            fragment: "f".into(),
            expires_at_unix: 200,
            membership_epoch: 1,
            binding_epoch: 1,
            cluster_id: "c".into(),
        };
        durable_atomic_write(&root.join("pin.json"), &serde_json::to_vec(&pin).unwrap())
            .await
            .unwrap();
        assert!(pinned_at(&root, "f", 100).await.unwrap());
        assert!(!pinned_at(&root, "other", 100).await.unwrap());
        assert!(!pinned_at(&root, "f", 200).await.unwrap());
        durable_atomic_write(&root.join("broken.json"), b"{")
            .await
            .unwrap();
        assert!(pinned_at(&root, "f", 201).await.is_err());
        tokio::fs::remove_dir_all(root).await.unwrap();
    }
}
