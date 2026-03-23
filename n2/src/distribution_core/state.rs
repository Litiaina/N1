use crate::enums::app_error_enum::AppError;
use crate::distribution_core::config::n2_runtime_config;
use crate::distribution_core::cross::rendezvous_placement;
use crate::distribution_core::governance::governance_cluster_fields;
use crate::distribution_core::model::{
    ClusterMembership, N2ClusterViewResponse, ObjectIdentity, ResolvePlacementResponse,
    UpsertMembershipRequest,
};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::path::Path;
use std::sync::OnceLock;
use tokio::fs;
use tokio::sync::RwLock;

pub const N2_EXPECTED_EPOCH_HEADER: &str = "x-n2-expected-epoch";
pub const N2_CLUSTER_FINGERPRINT_HEADER: &str = "x-n2-cluster-fingerprint";

fn default_membership() -> ClusterMembership {
    let cfg = n2_runtime_config();
    ClusterMembership {
        epoch: 1,
        nodes: vec![crate::distribution_core::model::NodeDescriptor {
            node_id: cfg.node_id.clone(),
            endpoint: cfg.node_endpoint.clone(),
            epoch: 1,
            active: true,
        }],
    }
}

fn normalize_endpoint(endpoint: &str) -> String {
    endpoint.trim().trim_end_matches('/').to_ascii_lowercase()
}

fn is_loopback_host(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<std::net::IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(false)
}

fn validate_node_endpoint(endpoint: &str) -> Result<String, AppError> {
    let parsed = reqwest::Url::parse(endpoint).map_err(|e| {
        AppError::BadRequest(format!("invalid node endpoint '{}': {}", endpoint, e))
    })?;
    let host = parsed.host_str().ok_or_else(|| {
        AppError::BadRequest(format!("node endpoint '{}' is missing a host", endpoint))
    })?;
    let allow_loopback_http = parsed.scheme() == "http" && is_loopback_host(host);
    if parsed.scheme() != "https" && !allow_loopback_http {
        return Err(AppError::BadRequest(format!(
            "node endpoint '{}' must use https (loopback http is only allowed for in-process tests)",
            endpoint
        )));
    }
    if host.trim().is_empty() {
        return Err(AppError::BadRequest(format!(
            "node endpoint '{}' is missing a usable host",
            endpoint
        )));
    }
    if parsed.port_or_known_default().is_none() {
        return Err(AppError::BadRequest(format!(
            "node endpoint '{}' is missing a port",
            endpoint
        )));
    }
    Ok(normalize_endpoint(endpoint))
}

fn build_cluster_view(
    membership: &ClusterMembership,
    expected_epoch: Option<u64>,
    governance_term: u64,
    governance_commit_index: u64,
    governance_leader_id: Option<String>,
    governance_leader_endpoint: Option<String>,
    governance_pending_index: Option<u64>,
    governance_healthy: bool,
) -> N2ClusterViewResponse {
    let cfg = n2_runtime_config();
    let cluster_fingerprint = cluster_fingerprint_for_membership(membership);
    let local_endpoint = normalize_endpoint(&cfg.node_endpoint);
    let mut local_node_present = false;
    let mut local_node_active = false;
    let mut local_epoch_matches = false;
    let active_nodes = membership.nodes.iter().filter(|n| n.active).count();

    for node in &membership.nodes {
        let by_id = node.node_id == cfg.node_id;
        let by_endpoint = normalize_endpoint(&node.endpoint) == local_endpoint;
        if by_id || by_endpoint {
            local_node_present = true;
            local_node_active = node.active;
            local_epoch_matches = node.epoch == membership.epoch;
            break;
        }
    }

    let mut reasons = Vec::new();
    if let Some(expected) = expected_epoch {
        if expected != membership.epoch {
            reasons.push(format!(
                "membership epoch mismatch: expected {}, local {}",
                expected, membership.epoch
            ));
        }
    }
    if !local_node_present {
        reasons.push(format!(
            "local node is missing from membership (node_id={}, endpoint={})",
            cfg.node_id, cfg.node_endpoint
        ));
    } else if !local_node_active {
        reasons.push("local node is marked inactive in membership".to_string());
    }
    if local_node_present && !local_epoch_matches {
        reasons.push(format!(
            "local node epoch does not match membership epoch {}",
            membership.epoch
        ));
    }
    if active_nodes == 0 {
        reasons.push("membership has zero active nodes".to_string());
    }

    N2ClusterViewResponse {
        node_id: cfg.node_id.clone(),
        node_endpoint: cfg.node_endpoint.clone(),
        membership_epoch: membership.epoch,
        cluster_fingerprint,
        expected_epoch,
        total_nodes: membership.nodes.len(),
        active_nodes,
        local_node_present,
        local_node_active,
        governance_term,
        governance_commit_index,
        governance_leader_id,
        governance_leader_endpoint,
        governance_pending_index,
        governance_healthy,
        write_ready: reasons.is_empty(),
        reasons,
    }
}

fn cluster_fingerprint_for_membership(membership: &ClusterMembership) -> String {
    let cfg = n2_runtime_config();
    let mut normalized_nodes = membership
        .nodes
        .iter()
        .map(|node| {
            serde_json::json!({
                "node_id": node.node_id,
                "endpoint": normalize_endpoint(&node.endpoint),
                "epoch": node.epoch,
                "active": node.active,
            })
        })
        .collect::<Vec<_>>();
    normalized_nodes.sort_by(|a, b| {
        let a_id = a
            .get("node_id")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let b_id = b
            .get("node_id")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        a_id.cmp(b_id)
    });

    let payload = serde_json::json!({
        "membership_epoch": membership.epoch,
        "nodes": normalized_nodes,
        "cluster_commit_mode": cfg.cluster_commit_mode.as_str(),
        "auto_cross_params": cfg.auto_cross_params,
        "configured_cross_params": {
            "kc": cfg.cross_params.kc,
            "mc": cfg.cross_params.mc,
            "kd": cfg.cross_params.kd,
            "md": cfg.cross_params.md,
        },
        "cluster_stripe_block_size": cfg.cluster_stripe_block_size,
    });
    let encoded = serde_json::to_vec(&payload).unwrap_or_default();
    let digest = Sha256::digest(encoded);
    hex::encode(digest)
}

async fn read_membership_file(path: &Path) -> Result<Option<ClusterMembership>, AppError> {
    let exists = fs::try_exists(path)
        .await
        .map_err(|e| AppError::Io(e, "check n2 membership file".to_string()))?;
    if !exists {
        return Ok(None);
    }
    let bytes = fs::read(path)
        .await
        .map_err(|e| AppError::Io(e, "read n2 membership file".to_string()))?;
    let parsed = serde_json::from_slice::<ClusterMembership>(&bytes).map_err(|e| {
        AppError::InternalServerError(format!("decode n2 membership file failed: {}", e))
    })?;
    Ok(Some(parsed))
}

async fn write_membership_file(
    path: &Path,
    membership: &ClusterMembership,
) -> Result<(), AppError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| AppError::Io(e, "create n2 membership parent".to_string()))?;
    }

    let payload = serde_json::to_vec_pretty(membership).map_err(|e| {
        AppError::InternalServerError(format!("encode n2 membership failed: {}", e))
    })?;
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, payload)
        .await
        .map_err(|e| AppError::Io(e, "write n2 membership temp file".to_string()))?;
    fs::rename(&tmp, path)
        .await
        .map_err(|e| AppError::Io(e, "replace n2 membership file".to_string()))?;

    let snap = membership_snapshot_path(path, membership.epoch);
    fs::write(
        &snap,
        serde_json::to_vec_pretty(membership).map_err(|e| {
            AppError::InternalServerError(format!("encode n2 membership snapshot failed: {}", e))
        })?,
    )
    .await
    .map_err(|e| AppError::Io(e, "write n2 membership snapshot file".to_string()))?;
    Ok(())
}

pub async fn ensure_membership_artifacts(membership: &ClusterMembership) -> Result<(), AppError> {
    let cfg = n2_runtime_config();
    let membership_path = cfg.membership_file.as_path();
    let snapshot_path = membership_snapshot_path(membership_path, membership.epoch);
    let membership_exists = fs::try_exists(membership_path)
        .await
        .map_err(|e| AppError::Io(e, "check n2 membership file".to_string()))?;
    let snapshot_exists = fs::try_exists(&snapshot_path)
        .await
        .map_err(|e| AppError::Io(e, "check n2 membership snapshot file".to_string()))?;
    if membership_exists && snapshot_exists {
        return Ok(());
    }
    write_membership_file(membership_path, membership).await
}

fn membership_snapshot_path(path: &Path, epoch: u64) -> std::path::PathBuf {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("n2_cluster_membership");
    parent.join(format!("{}.epoch-{}.json", stem, epoch))
}

fn validate_membership(membership: &ClusterMembership) -> Result<(), AppError> {
    if membership.nodes.is_empty() {
        return Err(AppError::BadRequest(
            "membership must include at least one node".to_string(),
        ));
    }

    let mut ids = HashSet::new();
    let mut endpoints = HashSet::new();
    for node in &membership.nodes {
        if node.node_id.trim().is_empty() {
            return Err(AppError::BadRequest("node_id cannot be empty".to_string()));
        }
        if node.endpoint.trim().is_empty() {
            return Err(AppError::BadRequest(
                "node endpoint cannot be empty".to_string(),
            ));
        }
        if !ids.insert(node.node_id.clone()) {
            return Err(AppError::BadRequest(format!(
                "duplicate node_id in membership: {}",
                node.node_id
            )));
        }
        let normalized = validate_node_endpoint(&node.endpoint)?;
        if !endpoints.insert(normalized) {
            return Err(AppError::BadRequest(format!(
                "duplicate node endpoint in membership: {}",
                node.endpoint
            )));
        }
    }

    if !membership.nodes.iter().any(|n| n.active) {
        return Err(AppError::BadRequest(
            "membership must contain at least one active node".to_string(),
        ));
    }

    Ok(())
}

static MEMBERSHIP: OnceLock<RwLock<ClusterMembership>> = OnceLock::new();

async fn initialize_membership() -> ClusterMembership {
    let cfg = n2_runtime_config();
    match read_membership_file(cfg.membership_file.as_path()).await {
        Ok(Some(from_disk)) => from_disk,
        _ => default_membership(),
    }
}

async fn membership_lock() -> &'static RwLock<ClusterMembership> {
    if let Some(lock) = MEMBERSHIP.get() {
        return lock;
    }

    let initial = initialize_membership().await;
    let _ = MEMBERSHIP.set(RwLock::new(initial));
    MEMBERSHIP.get().expect("membership lock initialized")
}

pub async fn get_membership() -> Result<ClusterMembership, AppError> {
    let lock = membership_lock().await;
    let snapshot = lock.read().await.clone();
    validate_membership(&snapshot)?;
    Ok(snapshot)
}

pub async fn current_cluster_epoch_and_fingerprint() -> Result<(u64, String), AppError> {
    let membership = get_membership().await?;
    Ok((
        membership.epoch,
        cluster_fingerprint_for_membership(&membership),
    ))
}

pub async fn validate_cluster_view_proof(
    expected_epoch: u64,
    expected_fingerprint: &str,
) -> Result<(), AppError> {
    let membership = get_membership().await?;
    if membership.epoch != expected_epoch {
        return Err(AppError::Conflict(format!(
            "membership epoch mismatch: expected {}, local {}",
            expected_epoch, membership.epoch
        )));
    }
    let local_fingerprint = cluster_fingerprint_for_membership(&membership);
    if local_fingerprint != expected_fingerprint.trim() {
        return Err(AppError::Conflict(format!(
            "cluster fingerprint mismatch: expected {}, local {}",
            expected_fingerprint.trim(),
            local_fingerprint
        )));
    }
    Ok(())
}

async fn replace_membership(snapshot: ClusterMembership) -> Result<ClusterMembership, AppError> {
    validate_membership(&snapshot)?;
    let lock = membership_lock().await;
    let mut guard = lock.write().await;
    let cfg = n2_runtime_config();
    write_membership_file(cfg.membership_file.as_path(), &snapshot).await?;
    *guard = snapshot.clone();
    Ok(snapshot)
}

pub async fn get_cluster_view(
    expected_epoch: Option<u64>,
) -> Result<N2ClusterViewResponse, AppError> {
    let membership = get_membership().await?;
    let governance = governance_cluster_fields().await?;
    let governance_healthy =
        governance.last_applied_membership_epoch == membership.epoch && governance.commit_index > 0;
    Ok(build_cluster_view(
        &membership,
        expected_epoch,
        governance.current_term,
        governance.commit_index,
        governance.leader_id,
        governance.leader_endpoint,
        governance.pending_index,
        governance_healthy,
    ))
}

pub async fn ensure_cluster_write_ready(
    expected_epoch: Option<u64>,
) -> Result<N2ClusterViewResponse, AppError> {
    let view = get_cluster_view(expected_epoch).await?;
    if !view.write_ready {
        let reason = if view.reasons.is_empty() {
            "cluster view is not write-ready".to_string()
        } else {
            view.reasons.join("; ")
        };
        return Err(AppError::Conflict(format!(
            "stale n2 cluster view on node {}: {}",
            view.node_id, reason
        )));
    }
    Ok(view)
}

#[allow(dead_code)]
pub async fn upsert_membership(
    request: UpsertMembershipRequest,
) -> Result<ClusterMembership, AppError> {
    let (_, next) = stage_upsert_membership(request).await?;
    replace_membership(next).await
}

pub async fn stage_upsert_membership(
    request: UpsertMembershipRequest,
) -> Result<(ClusterMembership, ClusterMembership), AppError> {
    let lock = membership_lock().await;
    let guard = lock.read().await;
    let next_epoch = match request.epoch {
        Some(epoch) => {
            if epoch <= guard.epoch {
                return Err(AppError::Conflict(format!(
                    "stale membership epoch {}, current is {}",
                    epoch, guard.epoch
                )));
            }
            epoch
        }
        None => guard.epoch.saturating_add(1),
    };

    let mut next = ClusterMembership {
        epoch: next_epoch,
        nodes: request.nodes,
    };
    for node in &mut next.nodes {
        node.epoch = next_epoch;
    }
    validate_membership(&next)?;
    Ok((guard.clone(), next))
}

#[allow(dead_code)]
pub async fn rollback_membership(epoch: u64) -> Result<ClusterMembership, AppError> {
    let (_, next) = stage_rollback_membership(epoch).await?;
    replace_membership(next).await
}

pub async fn stage_rollback_membership(
    epoch: u64,
) -> Result<(ClusterMembership, ClusterMembership), AppError> {
    let lock = membership_lock().await;
    let guard = lock.read().await;
    if epoch == 0 {
        return Err(AppError::BadRequest("epoch must be >= 1".to_string()));
    }
    if epoch >= guard.epoch {
        return Err(AppError::Conflict(format!(
            "rollback epoch {} must be lower than current epoch {}",
            epoch, guard.epoch
        )));
    }

    let cfg = n2_runtime_config();
    let snap_path = membership_snapshot_path(cfg.membership_file.as_path(), epoch);
    let bytes = fs::read(&snap_path)
        .await
        .map_err(|e| AppError::Io(e, format!("read membership snapshot {:?}", snap_path)))?;
    let next = serde_json::from_slice::<ClusterMembership>(&bytes).map_err(|e| {
        AppError::InternalServerError(format!("decode n2 membership snapshot failed: {}", e))
    })?;
    validate_membership(&next)?;
    Ok((guard.clone(), next))
}

pub async fn apply_membership_snapshot(
    membership: ClusterMembership,
) -> Result<ClusterMembership, AppError> {
    replace_membership(membership).await
}

pub async fn resolve_placement(
    identity: ObjectIdentity,
    kc: u8,
    mc: u8,
) -> Result<ResolvePlacementResponse, AppError> {
    if kc == 0 {
        return Err(AppError::BadRequest("kc must be at least 1".to_string()));
    }
    let target_count = usize::from(kc) + usize::from(mc);
    if target_count == 0 {
        return Err(AppError::BadRequest("kc+mc must be at least 1".to_string()));
    }

    let membership = get_membership().await?;
    let active_count = membership.nodes.iter().filter(|n| n.active).count();
    if active_count < target_count {
        return Err(AppError::BadRequest(format!(
            "insufficient active nodes: need {}, have {}",
            target_count, active_count
        )));
    }

    let targets = rendezvous_placement(&membership.nodes, &identity, target_count);
    if targets.len() < target_count {
        return Err(AppError::InternalServerError(
            "placement produced fewer targets than requested".to_string(),
        ));
    }

    Ok(ResolvePlacementResponse {
        epoch: membership.epoch,
        object: identity,
        kc,
        mc,
        nc: target_count,
        write_quorum: kc,
        targets,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution_core::model::NodeDescriptor;

    fn node(node_id: &str, endpoint: &str) -> NodeDescriptor {
        NodeDescriptor {
            node_id: node_id.to_string(),
            endpoint: endpoint.to_string(),
            epoch: 1,
            active: true,
        }
    }

    #[test]
    fn validate_membership_rejects_duplicate_normalized_endpoints() {
        let membership = ClusterMembership {
            epoch: 1,
            nodes: vec![
                node("node-a", "https://192.168.0.10:50001"),
                node("node-b", "https://192.168.0.10:50001/"),
            ],
        };

        let err = validate_membership(&membership).expect_err("duplicate endpoints rejected");
        match err {
            AppError::BadRequest(message) => {
                assert!(message.contains("duplicate node endpoint"));
            }
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[test]
    fn validate_membership_rejects_non_https_endpoints() {
        let membership = ClusterMembership {
            epoch: 1,
            nodes: vec![node("node-a", "http://192.168.0.10:50001")],
        };

        let err = validate_membership(&membership).expect_err("non-https endpoint rejected");
        match err {
            AppError::BadRequest(message) => {
                assert!(message.contains("must use https"));
            }
            other => panic!("unexpected error: {:?}", other),
        }
    }
}
