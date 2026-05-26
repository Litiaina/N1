use crate::core::disk_manager::ObjectPathManager;
use crate::distribution_core::cluster_shards::{
    collect_readable_cluster_shard_refs, delete_local_cluster_shard, delete_remote_cluster_shard,
    store_cluster_shard_from_available_sources,
};
use crate::distribution_core::config::{effective_cross_params, n2_runtime_config};
use crate::distribution_core::maintenance_gate::acquire_object_repair_permit;
use crate::distribution_core::model::{
    ClusterHealthQuery, FragmentHealthQuery, N2ClusterHealthResponse, N2ClusterShardHealth,
    N2DiskRootHealth, N2FragmentHealthResponse, N2HealthLevel, N2NodeHealthResponse,
    N2ObjectHealthResponse, N2ObjectHealthSummary, N2ObjectRecord, N2ReconcileObjectResult,
    N2RepairObjectResponse, NodeDescriptor, ObjectHealthQuery, RepairN2ObjectQuery,
};
use crate::distribution_core::object_index::{
    list_fragment_n2_records, list_n2_fragments, load_n2_object_record, persist_n2_object_record,
};
use crate::distribution_core::peer::{
    INTERNAL_TOKEN_HEADER, normalize_endpoint, validate_internal_token,
};
use crate::distribution_core::state::{get_cluster_view, get_membership, resolve_placement};
use crate::enums::app_error_enum::AppError;
use crate::middleware::auth_middleware::ADMIN_KEY;
use crate::structs::auth_structs::Claims;
use axum::Json as AxumJson;
use axum::extract::{Path, Query};
use axum::http::HeaderMap;
use chrono::Utc;
use std::collections::HashMap;
use std::ffi::CString;
use std::path::{Path as StdPath, PathBuf};
use std::time::Duration;
use tokio::fs;
use tracing::warn;
use ulid::Ulid;

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;

fn is_admin_claim(claims: &Claims) -> bool {
    claims.fragment == *ADMIN_KEY
}

fn normalize_object_key_for_health(object_key: &str) -> String {
    object_key.trim_start_matches('/').to_string()
}

fn rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        let Some(rest) = line.strip_prefix("VmRSS:") else {
            continue;
        };
        let kb = rest
            .split_whitespace()
            .next()
            .and_then(|v| v.parse::<u64>().ok())?;
        return Some(kb.saturating_mul(1024));
    }
    None
}

#[cfg(unix)]
fn statvfs_bytes(path: &StdPath) -> Result<(u64, u64), String> {
    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| "path contains an interior nul byte".to_string())?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::zeroed();
    let rc = unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    let stat = unsafe { stat.assume_init() };
    let fragment_size = stat.f_frsize.max(1);
    let total = stat.f_blocks.saturating_mul(fragment_size);
    let available = stat.f_bavail.saturating_mul(fragment_size);
    Ok((total, available))
}

#[cfg(not(unix))]
fn statvfs_bytes(_path: &StdPath) -> Result<(u64, u64), String> {
    Err("statvfs is not available on this platform".to_string())
}

async fn inspect_disk_root(kind: &str, path: PathBuf) -> N2DiskRootHealth {
    let path_string = path.to_string_lossy().to_string();
    let mut health = N2DiskRootHealth {
        kind: kind.to_string(),
        path: path_string.clone(),
        exists: false,
        readable: false,
        writable: false,
        total_bytes: None,
        available_bytes: None,
        used_bytes: None,
        error: None,
    };

    match fs::metadata(&path).await {
        Ok(meta) => {
            health.exists = meta.is_dir();
        }
        Err(err) => {
            health.error = Some(format!("metadata failed: {err}"));
            return health;
        }
    }

    match fs::read_dir(&path).await {
        Ok(_) => health.readable = true,
        Err(err) => {
            health.error = Some(format!("read_dir failed: {err}"));
        }
    }

    let probe = path.join(format!(".n2-health-{}.tmp", Ulid::new()));
    match fs::write(&probe, b"n2-health").await {
        Ok(_) => {
            health.writable = true;
            let _ = fs::remove_file(&probe).await;
        }
        Err(err) => {
            health.error = Some(format!("write probe failed: {err}"));
        }
    }

    match statvfs_bytes(&path) {
        Ok((total, available)) => {
            health.total_bytes = Some(total);
            health.available_bytes = Some(available);
            health.used_bytes = Some(total.saturating_sub(available));
        }
        Err(err) => {
            if health.error.is_none() {
                health.error = Some(format!("statvfs failed: {err}"));
            }
        }
    }

    health
}

fn storage_root_ready(root: &N2DiskRootHealth) -> bool {
    root.exists && root.readable && root.writable
}

async fn inspect_local_node_health(active: bool) -> N2NodeHealthResponse {
    let cfg = n2_runtime_config();
    let path_manager = ObjectPathManager::new();
    let mut reasons = Vec::new();

    let mut index_roots = Vec::new();
    for root in path_manager.get_all_index_roots() {
        let health = inspect_disk_root("index", root).await;
        if !storage_root_ready(&health) {
            reasons.push(format!("index root is not ready: {}", health.path));
        }
        index_roots.push(health);
    }

    let mut blob_roots = Vec::new();
    for root in path_manager.get_blob_roots() {
        let health = inspect_disk_root("blob", root).await;
        if !storage_root_ready(&health) {
            reasons.push(format!("blob root is not ready: {}", health.path));
        }
        blob_roots.push(health);
    }

    if index_roots.is_empty() {
        reasons.push("no index roots configured".to_string());
    }
    if blob_roots.is_empty() {
        reasons.push("no blob roots configured".to_string());
    }

    let level = if reasons.is_empty() {
        N2HealthLevel::Healthy
    } else if index_roots.iter().any(storage_root_ready)
        && blob_roots.iter().any(storage_root_ready)
    {
        N2HealthLevel::Degraded
    } else {
        N2HealthLevel::Critical
    };

    N2NodeHealthResponse {
        node_id: cfg.node_id.clone(),
        endpoint: cfg.node_endpoint.clone(),
        active,
        local: true,
        reachable: true,
        level,
        observed_at_unix: Utc::now().timestamp(),
        rss_bytes: rss_bytes(),
        index_roots,
        blob_roots,
        reasons,
    }
}

fn failed_remote_node_health(
    node: &NodeDescriptor,
    error: impl std::fmt::Display,
) -> N2NodeHealthResponse {
    N2NodeHealthResponse {
        node_id: node.node_id.clone(),
        endpoint: node.endpoint.clone(),
        active: node.active,
        local: false,
        reachable: false,
        level: if node.active {
            N2HealthLevel::Critical
        } else {
            N2HealthLevel::Degraded
        },
        observed_at_unix: Utc::now().timestamp(),
        rss_bytes: None,
        index_roots: Vec::new(),
        blob_roots: Vec::new(),
        reasons: vec![format!("node health request failed: {error}")],
    }
}

async fn remote_node_health(
    client: &reqwest::Client,
    node: &NodeDescriptor,
) -> N2NodeHealthResponse {
    let cfg = n2_runtime_config();
    let url = format!(
        "{}/n2/internal/health/node",
        node.endpoint.trim_end_matches('/')
    );
    let response = match client
        .get(url)
        .header(INTERNAL_TOKEN_HEADER, cfg.internal_token.as_str())
        .send()
        .await
    {
        Ok(response) => response,
        Err(err) => return failed_remote_node_health(node, err),
    };
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return failed_remote_node_health(node, format!("status={status} body={body}"));
    }
    match response.json::<N2NodeHealthResponse>().await {
        Ok(mut health) => {
            health.active = node.active;
            health.local = false;
            health.reachable = true;
            health
        }
        Err(err) => failed_remote_node_health(node, err),
    }
}

fn shard_path(
    fragment: &str,
    version_id: &str,
    cluster_shard_index: usize,
    root: &StdPath,
) -> PathBuf {
    root.join(fragment)
        .join("n2")
        .join("shards")
        .join(version_id)
        .join(format!("cshard_{}", cluster_shard_index))
        .join("disk_shard_0.bin")
}

pub async fn inspect_local_cluster_shard_health(
    fragment: &str,
    version_id: &str,
    cluster_shard_index: usize,
) -> N2ClusterShardHealth {
    let cfg = n2_runtime_config();
    let path_manager = ObjectPathManager::new();
    let mut error = None;
    let mut size_bytes = None;
    let mut present = false;

    for root in path_manager.get_blob_roots() {
        let path = shard_path(fragment, version_id, cluster_shard_index, &root);
        match fs::metadata(&path).await {
            Ok(meta) if meta.is_file() => {
                present = true;
                size_bytes = Some(meta.len());
                break;
            }
            Ok(_) => {
                error = Some(format!(
                    "cluster shard path is not a file: {}",
                    path.display()
                ));
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                error = Some(format!(
                    "cluster shard stat failed {}: {err}",
                    path.display()
                ));
            }
        }
    }

    N2ClusterShardHealth {
        cluster_shard_index,
        node_id: cfg.node_id.clone(),
        endpoint: cfg.node_endpoint.clone(),
        expected_node_id: None,
        expected_endpoint: None,
        reachable: true,
        present,
        stale_placement: false,
        size_bytes,
        expected_size_bytes: None,
        checksum: None,
        error,
    }
}

async fn remote_shard_health(
    client: &reqwest::Client,
    node: &NodeDescriptor,
    fragment: &str,
    version_id: &str,
    cluster_shard_index: usize,
) -> N2ClusterShardHealth {
    let cfg = n2_runtime_config();
    let url = format!(
        "{}/n2/internal/health/shard/{}/{}/{}",
        node.endpoint.trim_end_matches('/'),
        fragment,
        version_id,
        cluster_shard_index
    );
    let response = match client
        .get(url)
        .header(INTERNAL_TOKEN_HEADER, cfg.internal_token.as_str())
        .send()
        .await
    {
        Ok(response) => response,
        Err(err) => {
            return N2ClusterShardHealth {
                cluster_shard_index,
                node_id: node.node_id.clone(),
                endpoint: node.endpoint.clone(),
                expected_node_id: None,
                expected_endpoint: None,
                reachable: false,
                present: false,
                stale_placement: false,
                size_bytes: None,
                expected_size_bytes: None,
                checksum: None,
                error: Some(format!("remote shard health request failed: {err}")),
            };
        }
    };
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return N2ClusterShardHealth {
            cluster_shard_index,
            node_id: node.node_id.clone(),
            endpoint: node.endpoint.clone(),
            expected_node_id: None,
            expected_endpoint: None,
            reachable: true,
            present: false,
            stale_placement: false,
            size_bytes: None,
            expected_size_bytes: None,
            checksum: None,
            error: Some(format!("remote shard health status={status} body={body}")),
        };
    }
    response
        .json::<N2ClusterShardHealth>()
        .await
        .unwrap_or_else(|err| N2ClusterShardHealth {
            cluster_shard_index,
            node_id: node.node_id.clone(),
            endpoint: node.endpoint.clone(),
            expected_node_id: None,
            expected_endpoint: None,
            reachable: false,
            present: false,
            stale_placement: false,
            size_bytes: None,
            expected_size_bytes: None,
            checksum: None,
            error: Some(format!("decode remote shard health failed: {err}")),
        })
}

fn shard_matches_target(shard: &N2ClusterShardHealth, target: &NodeDescriptor) -> bool {
    shard.node_id == target.node_id
        || normalize_endpoint(shard.endpoint.as_str())
            == normalize_endpoint(target.endpoint.as_str())
}

async fn inspect_record_health(
    record: N2ObjectRecord,
    client: &reqwest::Client,
) -> Result<N2ObjectHealthResponse, AppError> {
    let cfg = n2_runtime_config();
    let local_endpoint = normalize_endpoint(&cfg.node_endpoint);
    let required_shards = usize::from(record.params.kc);
    let expected_shards = usize::from(record.params.kc) + usize::from(record.params.mc);
    let placement =
        resolve_placement(record.object.clone(), record.params.kc, record.params.mc).await;
    let mut reasons = Vec::new();
    let mut targets = Vec::new();
    let mut target_epoch = None;
    match placement {
        Ok(placement) => {
            target_epoch = Some(placement.epoch);
            targets = placement.targets;
            if placement.epoch != record.epoch {
                reasons.push(format!(
                    "object record epoch {} differs from target membership epoch {}",
                    record.epoch, placement.epoch
                ));
            }
        }
        Err(err) => {
            reasons.push(format!("placement cannot be resolved: {err}"));
        }
    }

    let membership = get_membership().await?;
    let node_by_endpoint = membership
        .nodes
        .iter()
        .map(|node| (normalize_endpoint(node.endpoint.as_str()), node.clone()))
        .collect::<HashMap<_, _>>();
    let mut shards = Vec::with_capacity(expected_shards);

    for idx in 0..expected_shards {
        let expected_target = targets.get(idx).cloned();
        let shard_ref = record
            .shards
            .iter()
            .find(|shard| shard.cluster_shard_index == idx)
            .cloned();
        let Some(shard_ref) = shard_ref else {
            shards.push(N2ClusterShardHealth {
                cluster_shard_index: idx,
                node_id: expected_target
                    .as_ref()
                    .map(|n| n.node_id.clone())
                    .unwrap_or_default(),
                endpoint: expected_target
                    .as_ref()
                    .map(|n| n.endpoint.clone())
                    .unwrap_or_default(),
                expected_node_id: expected_target.as_ref().map(|n| n.node_id.clone()),
                expected_endpoint: expected_target.as_ref().map(|n| n.endpoint.clone()),
                reachable: false,
                present: false,
                stale_placement: false,
                size_bytes: None,
                expected_size_bytes: None,
                checksum: None,
                error: Some(
                    "object record has no shard reference for this cluster shard".to_string(),
                ),
            });
            continue;
        };

        let node = NodeDescriptor {
            node_id: shard_ref.node_id.clone(),
            endpoint: shard_ref.endpoint.clone(),
            epoch: record.epoch,
            active: node_by_endpoint
                .get(&normalize_endpoint(shard_ref.endpoint.as_str()))
                .map(|n| n.active)
                .unwrap_or(true),
        };

        let mut health = if normalize_endpoint(shard_ref.endpoint.as_str()) == local_endpoint {
            inspect_local_cluster_shard_health(
                record.object.fragment.as_str(),
                record.object.version_id.as_str(),
                idx,
            )
            .await
        } else {
            remote_shard_health(
                client,
                &node,
                record.object.fragment.as_str(),
                record.object.version_id.as_str(),
                idx,
            )
            .await
        };
        health.node_id = shard_ref.node_id.clone();
        health.endpoint = shard_ref.endpoint.clone();
        health.expected_node_id = expected_target.as_ref().map(|n| n.node_id.clone());
        health.expected_endpoint = expected_target.as_ref().map(|n| n.endpoint.clone());
        health.expected_size_bytes = Some(shard_ref.size_bytes);
        health.checksum = Some(shard_ref.checksum.clone());
        health.stale_placement = expected_target
            .as_ref()
            .map(|target| !shard_matches_target(&health, target))
            .unwrap_or(false);
        if health.present && health.size_bytes != Some(shard_ref.size_bytes) {
            health.error = Some(format!(
                "cluster shard size mismatch: expected {}, observed {:?}",
                shard_ref.size_bytes, health.size_bytes
            ));
        }
        shards.push(health);
    }

    let available_shards = shards
        .iter()
        .filter(|shard| shard.reachable && shard.present)
        .count();
    let stale_shards = shards.iter().filter(|shard| shard.stale_placement).count();
    let missing_shards = expected_shards.saturating_sub(available_shards);
    let readable = available_shards >= required_shards;
    let repairable = readable;
    let needs_redistribution = target_epoch
        .map(|epoch| epoch != record.epoch)
        .unwrap_or(false)
        || stale_shards > 0;

    if missing_shards > 0 {
        reasons.push(format!(
            "{} of {} cluster shards are missing or unreachable",
            missing_shards, expected_shards
        ));
    }
    if stale_shards > 0 {
        reasons.push(format!(
            "{} cluster shards are not on their current placement targets",
            stale_shards
        ));
    }

    let level = if available_shards >= expected_shards && stale_shards == 0 && !needs_redistribution
    {
        N2HealthLevel::Healthy
    } else if readable {
        N2HealthLevel::Degraded
    } else if available_shards > 0 {
        N2HealthLevel::Critical
    } else {
        N2HealthLevel::Lost
    };

    Ok(N2ObjectHealthResponse {
        fragment: record.object.fragment.clone(),
        object_key: record.object.object_key.clone(),
        object_id: record.object.object_id.clone(),
        version_id: record.object.version_id.clone(),
        epoch: record.epoch,
        target_epoch,
        level,
        readable,
        repairable,
        needs_repair: level != N2HealthLevel::Healthy,
        needs_redistribution,
        required_shards,
        expected_shards,
        available_shards,
        missing_shards,
        stale_shards,
        total_size_bytes: record.total_size_bytes,
        shards,
        reasons,
    })
}

async fn inspect_object_health(
    fragment: &str,
    object_key: &str,
    version_id: Option<&str>,
) -> Result<N2ObjectHealthResponse, AppError> {
    let cfg = n2_runtime_config();
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_millis(cfg.remote_request_timeout_ms))
        .build()
        .map_err(|e| {
            AppError::InternalServerError(format!("build n2 health client failed: {}", e))
        })?;
    let record = load_n2_object_record(fragment, object_key, version_id).await?;
    inspect_record_health(record, &client).await
}

pub(crate) async fn inspect_fragment_health(
    fragment: &str,
    max_objects: usize,
) -> Result<N2FragmentHealthResponse, AppError> {
    let cfg = n2_runtime_config();
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_millis(cfg.remote_request_timeout_ms))
        .build()
        .map_err(|e| {
            AppError::InternalServerError(format!("build n2 health client failed: {}", e))
        })?;
    let records = list_fragment_n2_records(fragment, max_objects).await?;
    let mut response = N2FragmentHealthResponse {
        fragment: fragment.to_string(),
        level: N2HealthLevel::Healthy,
        max_objects,
        scanned_objects: 0,
        healthy_objects: 0,
        degraded_objects: 0,
        critical_objects: 0,
        lost_objects: 0,
        repairable_objects: 0,
        needs_repair_objects: 0,
        failed_objects: 0,
        objects: Vec::with_capacity(records.len()),
        reasons: Vec::new(),
    };

    for record in records {
        response.scanned_objects = response.scanned_objects.saturating_add(1);
        match inspect_record_health(record, &client).await {
            Ok(health) => {
                response.level = response.level.worst(health.level);
                match health.level {
                    N2HealthLevel::Healthy => {
                        response.healthy_objects = response.healthy_objects.saturating_add(1)
                    }
                    N2HealthLevel::Degraded => {
                        response.degraded_objects = response.degraded_objects.saturating_add(1)
                    }
                    N2HealthLevel::Critical => {
                        response.critical_objects = response.critical_objects.saturating_add(1)
                    }
                    N2HealthLevel::Lost => {
                        response.lost_objects = response.lost_objects.saturating_add(1)
                    }
                }
                if health.repairable {
                    response.repairable_objects = response.repairable_objects.saturating_add(1);
                }
                if health.needs_repair {
                    response.needs_repair_objects = response.needs_repair_objects.saturating_add(1);
                }
                response.objects.push(N2ObjectHealthSummary {
                    object_key: health.object_key,
                    version_id: health.version_id,
                    level: health.level,
                    readable: health.readable,
                    repairable: health.repairable,
                    needs_repair: health.needs_repair,
                    needs_redistribution: health.needs_redistribution,
                });
            }
            Err(err) => {
                response.failed_objects = response.failed_objects.saturating_add(1);
                response.level = response.level.worst(N2HealthLevel::Critical);
                response
                    .reasons
                    .push(format!("object health inspection failed: {err}"));
            }
        }
    }

    if response.failed_objects > 0 {
        response.reasons.push(format!(
            "{} object health inspections failed",
            response.failed_objects
        ));
    }
    Ok(response)
}

async fn reconcile_object_record_once(
    mut record: N2ObjectRecord,
    expected_epoch: Option<u64>,
    dry_run: bool,
    cleanup_orphans: bool,
) -> Result<N2ReconcileObjectResult, AppError> {
    let membership = get_membership().await?;
    if let Some(expected) = expected_epoch {
        if expected != membership.epoch {
            return Err(AppError::Conflict(format!(
                "membership epoch mismatch: expected {}, local {}",
                expected, membership.epoch
            )));
        }
    }

    let cfg = n2_runtime_config();
    let http_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_millis(cfg.remote_request_timeout_ms))
        .build()
        .map_err(|e| {
            AppError::InternalServerError(format!("build n2 repair client failed: {}", e))
        })?;

    let old_epoch = record.epoch;
    let placement =
        resolve_placement(record.object.clone(), record.params.kc, record.params.mc).await?;
    let kc = usize::from(record.params.kc);
    let nc = usize::from(record.params.kc) + usize::from(record.params.mc);
    let previous_shards = record.shards.clone();
    let mut readable_shards = collect_readable_cluster_shard_refs(
        &http_client,
        record.object.fragment.as_str(),
        record.object.version_id.as_str(),
        &record.shards,
    )
    .await;

    if readable_shards.len() < kc {
        return Err(AppError::InternalServerError(format!(
            "insufficient readable shards for object repair: need {}, have {}",
            kc,
            readable_shards.len()
        )));
    }

    let mut next_shards = Vec::with_capacity(nc);
    let mut planned_shards = 0usize;
    let mut repaired_shards = 0usize;
    let mut cleaned_orphans = 0usize;
    let mut cleanup_failures = 0usize;

    for idx in 0..nc {
        let target = &placement.targets[idx];
        if let Some(existing) = record.shards.iter().find(|shard| {
            shard.cluster_shard_index == idx
                && (shard.node_id == target.node_id
                    || normalize_endpoint(shard.endpoint.as_str())
                        == normalize_endpoint(target.endpoint.as_str()))
                && readable_shards
                    .iter()
                    .any(|readable| readable.cluster_shard_index == idx)
        }) {
            next_shards.push(existing.clone());
            continue;
        }

        planned_shards = planned_shards.saturating_add(1);
        if dry_run {
            continue;
        }

        let stored = store_cluster_shard_from_available_sources(
            &http_client,
            target,
            &record.object,
            idx,
            record.params.kd,
            record.params.md,
            record.stripe_block_size,
            usize::from(record.params.kc),
            usize::from(record.params.mc),
            &readable_shards,
        )
        .await?;
        next_shards.push(stored);
        readable_shards.push(next_shards.last().cloned().ok_or_else(|| {
            AppError::InternalServerError("stored shard ref missing".to_string())
        })?);
        repaired_shards = repaired_shards.saturating_add(1);
    }

    if !dry_run {
        next_shards.sort_by_key(|shard| shard.cluster_shard_index);
        if next_shards.len() < kc {
            return Err(AppError::InternalServerError(format!(
                "object repair produced insufficient shard refs: need {}, have {}",
                kc,
                next_shards.len()
            )));
        }
        let stale_shards = previous_shards
            .iter()
            .filter(|old| {
                !next_shards.iter().any(|next| {
                    old.cluster_shard_index == next.cluster_shard_index
                        && (old.node_id == next.node_id
                            || normalize_endpoint(old.endpoint.as_str())
                                == normalize_endpoint(next.endpoint.as_str()))
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        record.shards = next_shards;
        record.epoch = placement.epoch;
        record.committed_at = Utc::now().timestamp();
        persist_n2_object_record(&record).await?;

        if cleanup_orphans {
            for stale in stale_shards {
                let cleanup = if normalize_endpoint(stale.endpoint.as_str())
                    == normalize_endpoint(cfg.node_endpoint.as_str())
                {
                    delete_local_cluster_shard(
                        record.object.fragment.as_str(),
                        record.object.version_id.as_str(),
                        stale.cluster_shard_index,
                    )
                    .await
                } else {
                    delete_remote_cluster_shard(
                        &http_client,
                        stale.endpoint.as_str(),
                        record.object.fragment.as_str(),
                        record.object.version_id.as_str(),
                        stale.cluster_shard_index,
                    )
                    .await
                };
                match cleanup {
                    Ok(paths) => cleaned_orphans = cleaned_orphans.saturating_add(paths),
                    Err(err) => {
                        cleanup_failures = cleanup_failures.saturating_add(1);
                        warn!(
                            "n2 object repair cleanup failed fragment={} object_key={} version={} shard={} err={}",
                            record.object.fragment,
                            record.object.object_key,
                            record.object.version_id,
                            stale.cluster_shard_index,
                            err
                        );
                    }
                }
            }
        }
    }

    Ok(N2ReconcileObjectResult {
        object_key: record.object.object_key.clone(),
        version_id: record.object.version_id.clone(),
        old_epoch,
        target_epoch: placement.epoch,
        planned_shards,
        repaired_shards,
        cleaned_orphans,
        cleanup_failures,
        status: if dry_run {
            "planned".to_string()
        } else {
            "reconciled".to_string()
        },
        error: None,
    })
}

pub async fn get_n2_node_health(
    claims: Claims,
) -> Result<AxumJson<N2NodeHealthResponse>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".to_string()));
    }
    let membership = get_membership().await?;
    let cfg = n2_runtime_config();
    let active = membership.nodes.iter().any(|node| {
        node.active
            && (node.node_id == cfg.node_id
                || normalize_endpoint(node.endpoint.as_str())
                    == normalize_endpoint(cfg.node_endpoint.as_str()))
    });
    Ok(AxumJson(inspect_local_node_health(active).await))
}

pub async fn get_n2_internal_node_health(
    headers: HeaderMap,
) -> Result<AxumJson<N2NodeHealthResponse>, AppError> {
    validate_internal_token(&headers)?;
    let membership = get_membership().await?;
    let cfg = n2_runtime_config();
    let active = membership.nodes.iter().any(|node| {
        node.active
            && (node.node_id == cfg.node_id
                || normalize_endpoint(node.endpoint.as_str())
                    == normalize_endpoint(cfg.node_endpoint.as_str()))
    });
    Ok(AxumJson(inspect_local_node_health(active).await))
}

pub async fn get_n2_internal_shard_health(
    headers: HeaderMap,
    Path((fragment, version_id, cluster_shard_index)): Path<(String, String, usize)>,
) -> Result<AxumJson<N2ClusterShardHealth>, AppError> {
    validate_internal_token(&headers)?;
    Ok(AxumJson(
        inspect_local_cluster_shard_health(
            fragment.as_str(),
            version_id.as_str(),
            cluster_shard_index,
        )
        .await,
    ))
}

pub async fn get_n2_object_health(
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
    Query(query): Query<ObjectHealthQuery>,
) -> Result<AxumJson<N2ObjectHealthResponse>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".to_string()));
    }
    if !is_admin_claim(&claims) && claims.fragment != fragment {
        return Err(AppError::Forbidden(
            "Access denied to this fragment".to_string(),
        ));
    }
    let object_key = normalize_object_key_for_health(object_key.as_str());
    Ok(AxumJson(
        inspect_object_health(&fragment, &object_key, query.version_id.as_deref()).await?,
    ))
}

pub async fn get_n2_fragment_health(
    claims: Claims,
    Path(fragment): Path<String>,
    Query(query): Query<FragmentHealthQuery>,
) -> Result<AxumJson<N2FragmentHealthResponse>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".to_string()));
    }
    if !is_admin_claim(&claims) && claims.fragment != fragment {
        return Err(AppError::Forbidden(
            "Access denied to this fragment".to_string(),
        ));
    }
    let max_objects = query.max_objects.unwrap_or(256).clamp(1, 100_000);
    Ok(AxumJson(
        inspect_fragment_health(fragment.as_str(), max_objects).await?,
    ))
}

pub async fn repair_n2_object(
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
    Query(query): Query<RepairN2ObjectQuery>,
) -> Result<AxumJson<N2RepairObjectResponse>, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden(
            "Modify permission required".to_string(),
        ));
    }
    if !is_admin_claim(&claims) && claims.fragment != fragment {
        return Err(AppError::Forbidden(
            "Access denied to this fragment".to_string(),
        ));
    }

    let object_key = normalize_object_key_for_health(object_key.as_str());
    let _repair_permit = if query.dry_run.unwrap_or(false) {
        None
    } else {
        Some(acquire_object_repair_permit(&fragment, &object_key).await?)
    };
    let before = inspect_object_health(&fragment, &object_key, query.version_id.as_deref()).await?;
    let record = load_n2_object_record(&fragment, &object_key, query.version_id.as_deref()).await?;
    let reconcile = reconcile_object_record_once(
        record,
        query.expected_epoch,
        query.dry_run.unwrap_or(false),
        query.cleanup_orphans.unwrap_or(true),
    )
    .await?;
    let after = if query.dry_run.unwrap_or(false) {
        None
    } else {
        Some(inspect_object_health(&fragment, &object_key, query.version_id.as_deref()).await?)
    };
    Ok(AxumJson(N2RepairObjectResponse {
        before,
        reconcile,
        after,
    }))
}

pub async fn get_n2_cluster_health(
    claims: Claims,
    Query(query): Query<ClusterHealthQuery>,
) -> Result<AxumJson<N2ClusterHealthResponse>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".to_string()));
    }
    let view = get_cluster_view(query.expected_epoch).await?;
    let membership = get_membership().await?;
    let cfg = n2_runtime_config();
    let local_endpoint = normalize_endpoint(cfg.node_endpoint.as_str());
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_millis(cfg.remote_request_timeout_ms))
        .build()
        .map_err(|e| {
            AppError::InternalServerError(format!("build n2 health client failed: {}", e))
        })?;

    let mut nodes = Vec::with_capacity(membership.nodes.len());
    for node in &membership.nodes {
        if node.node_id == cfg.node_id
            || normalize_endpoint(node.endpoint.as_str()) == local_endpoint
        {
            nodes.push(inspect_local_node_health(node.active).await);
        } else {
            nodes.push(remote_node_health(&client, node).await);
        }
    }

    let params = effective_cross_params();
    let required_shards = usize::from(params.kc);
    let expected_shards = usize::from(params.kc) + usize::from(params.mc);
    let reachable_active_nodes = nodes
        .iter()
        .filter(|node| node.active && node.reachable)
        .count();
    let unreachable_active_nodes = nodes
        .iter()
        .filter(|node| node.active && !node.reachable)
        .count();
    let storage_ready_nodes = nodes
        .iter()
        .filter(|node| {
            node.active
                && node.reachable
                && node.index_roots.iter().any(storage_root_ready)
                && node.blob_roots.iter().any(storage_root_ready)
        })
        .count();

    let mut reasons = Vec::new();
    let mut level = N2HealthLevel::Healthy;
    if !view.write_ready {
        level = level.worst(N2HealthLevel::Degraded);
        reasons.push("cluster view is not write-ready".to_string());
    }
    if !view.governance_healthy {
        level = level.worst(N2HealthLevel::Degraded);
        reasons.push("governance is not fully aligned with membership".to_string());
    }
    if reachable_active_nodes < required_shards {
        level = level.worst(if reachable_active_nodes == 0 {
            N2HealthLevel::Lost
        } else {
            N2HealthLevel::Critical
        });
        reasons.push(format!(
            "reachable active nodes below read quorum: need {}, have {}",
            required_shards, reachable_active_nodes
        ));
    } else if reachable_active_nodes < expected_shards {
        level = level.worst(N2HealthLevel::Degraded);
        reasons.push(format!(
            "reachable active nodes below full placement: need {}, have {}",
            expected_shards, reachable_active_nodes
        ));
    }
    if storage_ready_nodes < reachable_active_nodes {
        level = level.worst(N2HealthLevel::Degraded);
        reasons.push("one or more reachable active nodes have storage root issues".to_string());
    }

    for node in &nodes {
        level = level.worst(node.level);
        if node.active && !node.reachable {
            reasons.push(format!("active node is unreachable: {}", node.node_id));
        }
    }

    let deep = query.deep.unwrap_or(false);
    let mut fragments = Vec::new();
    if deep {
        let max_fragments = query.max_fragments.unwrap_or(32).clamp(1, 4_096);
        let max_objects = query
            .max_objects_per_fragment
            .unwrap_or(128)
            .clamp(1, 100_000);
        for fragment in list_n2_fragments(max_fragments).await? {
            let health = inspect_fragment_health(fragment.as_str(), max_objects).await?;
            level = level.worst(health.level);
            fragments.push(health);
        }
    }

    Ok(AxumJson(N2ClusterHealthResponse {
        level,
        view,
        expected_shards,
        required_shards,
        reachable_active_nodes,
        unreachable_active_nodes,
        storage_ready_nodes,
        nodes,
        deep,
        fragments,
        reasons,
    }))
}
