use crate::core::disk_manager::FS_STORAGE_ROOT;
use crate::core::disk_manager::{ChunkQuery, ObjectMetadata, UploadPartQuery};
use crate::core::download::download_object_chunk_from_metadata;
use crate::core::index;
use crate::distribution::download::{part_has_remote_shards, shard_probe_readable};
use crate::distribution::layout::resolve_layout;
use crate::distribution::membership::{ClusterRuntime, active_cluster_runtime};
use crate::distribution::namespace::commit_upload_quorum;
use crate::distribution::upload::upload_cluster_one_shot;
use crate::enums::app_error_enum::AppError;
use crate::namespace::runtime::global_namespace_runtime;
use crate::structs::auth_structs::Claims;
use bytes::Bytes;
use futures_util::stream;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::sync::{LazyLock, Mutex};

const REWRITE_CHUNK_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Deserialize)]
pub struct ClusterRebalanceRequest {
    pub fragment: String,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ClusterRebalancePlan {
    pub membership_epoch: u64,
    pub active_nodes: usize,
    pub data_shards: usize,
    pub parity_shards: usize,
    pub automatic_rebalance: bool,
    pub recommendation: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ClusterRewriteJobStatus {
    pub running: bool,
    pub job: String,
    pub fragment: String,
    pub dry_run: bool,
    pub resumable: bool,
    pub scanned: u64,
    pub rewritten: u64,
    pub skipped: u64,
    pub failed: u64,
    pub started_at_unix: Option<i64>,
    pub finished_at_unix: Option<i64>,
    pub last_object: Option<String>,
    pub continuation_token: Option<String>,
    pub last_error: Option<String>,
}

static REBALANCE_STATUS: LazyLock<Mutex<ClusterRewriteJobStatus>> =
    LazyLock::new(|| Mutex::new(ClusterRewriteJobStatus::default()));
static REPAIR_STATUS: LazyLock<Mutex<ClusterRewriteJobStatus>> =
    LazyLock::new(|| Mutex::new(ClusterRewriteJobStatus::default()));

pub fn plan_rebalance(runtime: &ClusterRuntime) -> Result<ClusterRebalancePlan, AppError> {
    let active_nodes = runtime.membership.active_write_nodes();
    let layout =
        resolve_layout(active_nodes.len(), &runtime.layout_policy).map_err(AppError::BadRequest)?;
    Ok(ClusterRebalancePlan {
        membership_epoch: runtime.membership.epoch,
        active_nodes: active_nodes.len(),
        data_shards: layout.data_shards,
        parity_shards: layout.parity_shards,
        automatic_rebalance: runtime.auto_rebalance,
        recommendation: if runtime.auto_rebalance {
            "automatic rebalance is enabled for background rewrite jobs; new objects always use the current layout"
                .to_string()
        } else {
            "new objects use the current layout; old objects stay on their recorded layout until manual rebalance"
                .to_string()
        },
    })
}

fn job_status_path(job: &str) -> Option<PathBuf> {
    FS_STORAGE_ROOT.get().map(|root| {
        PathBuf::from(root)
            .join(".n2")
            .join("jobs")
            .join(format!("{job}.json"))
    })
}

fn persist_job_status(snapshot: &ClusterRewriteJobStatus) {
    if snapshot.job.is_empty() {
        return;
    }
    let Some(path) = job_status_path(&snapshot.job) else {
        return;
    };
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(bytes) = serde_json::to_vec_pretty(snapshot) {
        let tmp = path.with_extension("json.tmp");
        if fs::write(&tmp, bytes).is_ok() {
            let _ = fs::rename(tmp, path);
        }
    }
}

fn load_job_status(job: &str) -> Option<ClusterRewriteJobStatus> {
    let path = job_status_path(job)?;
    let bytes = fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn status_snapshot(
    status: &LazyLock<Mutex<ClusterRewriteJobStatus>>,
    job: &str,
) -> ClusterRewriteJobStatus {
    let current = status.lock().map(|guard| guard.clone()).unwrap_or_default();
    if current.running || !current.job.is_empty() {
        current
    } else {
        load_job_status(job).unwrap_or_default()
    }
}

pub fn rebalance_status() -> ClusterRewriteJobStatus {
    status_snapshot(&REBALANCE_STATUS, "cluster_rebalance")
}

pub fn repair_status() -> ClusterRewriteJobStatus {
    status_snapshot(&REPAIR_STATUS, "cluster_repair")
}

fn begin_job(
    status: &LazyLock<Mutex<ClusterRewriteJobStatus>>,
    job: &str,
    req: &ClusterRebalanceRequest,
) -> Result<(), AppError> {
    let mut guard = status.lock().map_err(|_| {
        AppError::InternalServerError("cluster rewrite status lock poisoned".into())
    })?;
    if guard.running {
        return Err(AppError::Conflict(format!("{job} is already running")));
    }
    *guard = ClusterRewriteJobStatus {
        running: true,
        job: job.to_string(),
        fragment: req.fragment.clone(),
        dry_run: req.dry_run,
        resumable: true,
        started_at_unix: Some(chrono::Utc::now().timestamp()),
        ..ClusterRewriteJobStatus::default()
    };
    persist_job_status(&guard);
    Ok(())
}

fn update_job(
    status: &LazyLock<Mutex<ClusterRewriteJobStatus>>,
    f: impl FnOnce(&mut ClusterRewriteJobStatus),
) {
    let snapshot = if let Ok(mut guard) = status.lock() {
        f(&mut guard);
        Some(guard.clone())
    } else {
        None
    };
    if let Some(snapshot) = snapshot {
        persist_job_status(&snapshot);
    }
}

fn finish_job(status: &LazyLock<Mutex<ClusterRewriteJobStatus>>) {
    update_job(status, |guard| {
        guard.running = false;
        guard.finished_at_unix = Some(chrono::Utc::now().timestamp());
    });
}

fn metadata_matches_current_layout(meta: &ObjectMetadata, runtime: &ClusterRuntime) -> bool {
    let Ok(layout) = resolve_layout(
        runtime.membership.active_write_nodes().len(),
        &runtime.layout_policy,
    ) else {
        return false;
    };
    let cluster_has_remote = runtime.membership.active_write_nodes().len() > 1;
    meta.parts.iter().all(|part| {
        let data = part.shards.iter().filter(|shard| !shard.is_parity).count();
        let parity = part.shards.iter().filter(|shard| shard.is_parity).count();
        data == layout.data_shards
            && parity == layout.parity_shards
            && (!cluster_has_remote || part_has_remote_shards(part))
    })
}

async fn object_has_unreadable_shard(meta: &ObjectMetadata) -> bool {
    for part in &meta.parts {
        for shard in &part.shards {
            if !shard_probe_readable(shard).await {
                return true;
            }
        }
    }
    false
}

async fn rewrite_object_to_current_layout(
    logical_path: String,
    object_id: String,
    source: ObjectMetadata,
    dry_run: bool,
) -> Result<bool, AppError> {
    if dry_run {
        return Ok(true);
    }
    let fragment = source.fragment.clone();
    let content_type = source.content_type.clone();
    let total = source.total_size_bytes;
    let source_for_stream = source.clone();
    let stream = stream::unfold(0u64, move |offset| {
        let meta = source_for_stream.clone();
        async move {
            if offset >= meta.total_size_bytes {
                return None;
            }
            let chunk_size =
                (meta.total_size_bytes - offset).min(REWRITE_CHUNK_BYTES as u64) as usize;
            let query = ChunkQuery {
                object_key: meta.object_key.clone(),
                offset,
                chunk_size,
            };
            let next = offset.saturating_add(chunk_size as u64);
            let result = download_object_chunk_from_metadata(meta, &query)
                .await
                .map(|(bytes, _)| Bytes::from(bytes));
            Some((result, next))
        }
    });
    let claims = Claims {
        fragment: fragment.clone(),
        read: true,
        write: true,
        modify: true,
        delete: true,
        exp: usize::MAX,
    };
    let query = UploadPartQuery {
        checksum: None,
        durability: None,
        strict_durability: None,
        io_gate: None,
    };
    let rewritten = upload_cluster_one_shot(
        claims,
        fragment.clone(),
        object_id.clone(),
        content_type,
        query,
        stream,
        total,
    )
    .await?;
    commit_upload_quorum(&fragment, &logical_path, &object_id, &rewritten).await?;
    Ok(true)
}

async fn run_rewrite_job(
    status: &'static LazyLock<Mutex<ClusterRewriteJobStatus>>,
    req: ClusterRebalanceRequest,
    repair_only: bool,
) {
    let runtime = match active_cluster_runtime() {
        Ok(runtime) => runtime,
        Err(err) => {
            update_job(status, |guard| {
                guard.failed = guard.failed.saturating_add(1);
                guard.last_error = Some(err.to_string());
            });
            finish_job(status);
            return;
        }
    };
    let mut continuation = None;
    let mut remaining = req.limit.unwrap_or(usize::MAX).max(1);
    while remaining > 0 {
        let page = match global_namespace_runtime()
            .list_live_object_refs_page(
                &req.fragment,
                req.prefix.as_deref(),
                continuation.as_deref(),
                remaining.min(256),
            )
            .await
        {
            Ok(page) => page,
            Err(err) => {
                update_job(status, |guard| {
                    guard.failed = guard.failed.saturating_add(1);
                    guard.last_error = Some(err.to_string());
                });
                break;
            }
        };
        if page.entries.is_empty() {
            break;
        }
        for entry in page.entries {
            if remaining == 0 {
                break;
            }
            remaining = remaining.saturating_sub(1);
            update_job(status, |guard| {
                guard.scanned = guard.scanned.saturating_add(1);
                guard.last_object = Some(entry.path.clone());
            });
            let meta = match index::read_object_version(
                &req.fragment,
                &entry.object_id,
                &entry.version_id,
            )
            .await
            {
                Ok(meta) => meta,
                Err(err) => {
                    update_job(status, |guard| {
                        guard.failed = guard.failed.saturating_add(1);
                        guard.last_error = Some(err.to_string());
                    });
                    continue;
                }
            };
            let should_rewrite = if repair_only {
                object_has_unreadable_shard(&meta).await
            } else {
                !metadata_matches_current_layout(&meta, &runtime)
            };
            if !should_rewrite {
                update_job(status, |guard| {
                    guard.skipped = guard.skipped.saturating_add(1)
                });
                continue;
            }
            match rewrite_object_to_current_layout(entry.path, entry.object_id, meta, req.dry_run)
                .await
            {
                Ok(true) => {
                    update_job(status, |guard| {
                        guard.rewritten = guard.rewritten.saturating_add(1)
                    });
                }
                Ok(false) => update_job(status, |guard| {
                    guard.skipped = guard.skipped.saturating_add(1)
                }),
                Err(err) => update_job(status, |guard| {
                    guard.failed = guard.failed.saturating_add(1);
                    guard.last_error = Some(err.to_string());
                }),
            }
        }
        continuation = page.continuation_token;
        update_job(status, |guard| {
            guard.continuation_token = continuation.clone();
        });
        if continuation.is_none() {
            break;
        }
    }
    finish_job(status);
}

pub fn start_rebalance(req: ClusterRebalanceRequest) -> Result<ClusterRewriteJobStatus, AppError> {
    begin_job(&REBALANCE_STATUS, "cluster_rebalance", &req)?;
    tokio::spawn(run_rewrite_job(&REBALANCE_STATUS, req, false));
    Ok(rebalance_status())
}

pub fn start_repair(req: ClusterRebalanceRequest) -> Result<ClusterRewriteJobStatus, AppError> {
    begin_job(&REPAIR_STATUS, "cluster_repair", &req)?;
    tokio::spawn(run_rewrite_job(&REPAIR_STATUS, req, true));
    Ok(repair_status())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::disk_manager::{PartMetadata, ShardInfo};

    #[test]
    fn layout_match_requires_current_shape_and_remote_cluster_holders() {
        let runtime = ClusterRuntime {
            local_node_id: "a".into(),
            membership_path: "membership.json".into(),
            layout_policy: crate::distribution::layout::ClusterLayoutPolicy::default(),
            membership: crate::distribution::membership::ClusterMembership {
                cluster_id: "test".into(),
                epoch: 1,
                nodes: vec![
                    crate::distribution::membership::ClusterNode {
                        id: "a".into(),
                        endpoint: "http://a".into(),
                        native_endpoint: None,
                        state: Some(crate::distribution::membership::NodeState::Online),
                        weight: 1,
                        index_roots: vec![],
                        blob_roots: vec![],
                        capacity_bytes: None,
                        used_bytes: None,
                        last_seen_unix: None,
                    },
                    crate::distribution::membership::ClusterNode {
                        id: "b".into(),
                        endpoint: "http://b".into(),
                        native_endpoint: None,
                        state: Some(crate::distribution::membership::NodeState::Online),
                        weight: 1,
                        index_roots: vec![],
                        blob_roots: vec![],
                        capacity_bytes: None,
                        used_bytes: None,
                        last_seen_unix: None,
                    },
                ],
            },
            auto_rebalance: false,
        };
        let meta = ObjectMetadata {
            object_key: "obj".into(),
            fragment: "frag".into(),
            version_id: "v".into(),
            upload_session_id: "s".into(),
            owner_uid: "u".into(),
            content_type: "application/octet-stream".into(),
            total_size_bytes: 1,
            created_at: 0,
            stripe_size_bytes: 4096,
            is_delete_marker: false,
            parts: vec![PartMetadata {
                part_index: 0,
                size_bytes: 1,
                created_at: 0,
                checksum: None,
                shards: vec![ShardInfo {
                    shard_index: 0,
                    file_name: "s".into(),
                    absolute_path: "/local".into(),
                    size_bytes: 1,
                    checksum: String::new(),
                    is_parity: false,
                }],
            }],
        };
        assert!(!metadata_matches_current_layout(&meta, &runtime));
    }
}
