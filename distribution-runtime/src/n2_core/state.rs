use crate::enums::app_error_enum::AppError;
use crate::n2_core::config::n2_runtime_config;
use crate::n2_core::cross::rendezvous_placement;
use crate::n2_core::model::{
    ClusterMembership, N2ClusterViewResponse, ObjectIdentity, ResolvePlacementResponse,
    UpsertMembershipRequest,
};
use std::collections::HashSet;
use std::path::Path;
use std::sync::OnceLock;
use tokio::fs;
use tokio::sync::RwLock;

fn default_membership() -> ClusterMembership {
    let cfg = n2_runtime_config();
    ClusterMembership {
        epoch: 1,
        nodes: vec![crate::n2_core::model::NodeDescriptor {
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

fn build_cluster_view(
    membership: &ClusterMembership,
    expected_epoch: Option<u64>,
) -> N2ClusterViewResponse {
    let cfg = n2_runtime_config();
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
        expected_epoch,
        total_nodes: membership.nodes.len(),
        active_nodes,
        local_node_present,
        local_node_active,
        write_ready: reasons.is_empty(),
        reasons,
    }
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

pub async fn get_cluster_view(
    expected_epoch: Option<u64>,
) -> Result<N2ClusterViewResponse, AppError> {
    let membership = get_membership().await?;
    Ok(build_cluster_view(&membership, expected_epoch))
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

pub async fn upsert_membership(
    request: UpsertMembershipRequest,
) -> Result<ClusterMembership, AppError> {
    let lock = membership_lock().await;
    let mut guard = lock.write().await;

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

    let cfg = n2_runtime_config();
    write_membership_file(cfg.membership_file.as_path(), &next).await?;

    *guard = next.clone();
    Ok(next)
}

pub async fn rollback_membership(epoch: u64) -> Result<ClusterMembership, AppError> {
    let lock = membership_lock().await;
    let mut guard = lock.write().await;
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
    write_membership_file(cfg.membership_file.as_path(), &next).await?;

    *guard = next.clone();
    Ok(next)
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
