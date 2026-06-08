use crate::core::disk_manager::FS_STORAGE_ROOT;
use crate::core::disk_manager::{
    ChunkQuery, InitiateUploadParams, ObjectMetadata, ShardInfo, UploadPartQuery,
};
use crate::core::download::download_object_chunk_from_metadata;
use crate::core::index;
use crate::core::repair_manager::ObjectLayoutStatus;
use crate::distribution::download::shard_probe_readable;
use crate::distribution::layout::resolve_layout;
use crate::distribution::membership::{ClusterRuntime, active_cluster_runtime};
use crate::distribution::namespace::commit_upload_quorum;
use crate::distribution::peer::parse_peer_shard_locator;
use crate::distribution::placement::{
    ClusterShardPlacement, ClusterShardRole, build_placement_plan,
};
use crate::distribution::upload::{
    finalize_cluster_upload, initiate_cluster_upload, put_cluster_part,
};
use crate::enums::app_error_enum::AppError;
use crate::io_control::{
    IoJobKind, IoPriority, MaintenanceBudget, MaintenanceBudgetConfig, MaintenanceBudgetSnapshot,
    register_io_job, request_io_job_cancel,
};
use crate::namespace::runtime::global_namespace_runtime;
use crate::structs::auth_structs::Claims;
use bytes::Bytes;
use futures_util::stream;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const REWRITE_CHUNK_BYTES: usize = 1024 * 1024;
const REWRITE_PART_BYTES: u64 = 64 * 1024 * 1024;

fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize)]
pub struct ClusterRebalanceRequest {
    pub fragment: String,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default = "default_true")]
    pub mismatched_only: bool,
    #[serde(default)]
    pub max_duration_seconds: Option<u64>,
    #[serde(default)]
    pub max_objects: Option<u64>,
    #[serde(default)]
    pub max_bytes: Option<u64>,
    #[serde(default)]
    pub max_directories: Option<u64>,
    #[serde(default)]
    pub continuation_token: Option<String>,
    #[serde(default)]
    pub priority: Option<IoPriority>,
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
#[serde(default)]
pub struct ClusterRewriteJobStatus {
    pub running: bool,
    pub job: String,
    pub fragment: String,
    pub dry_run: bool,
    pub mismatched_only: bool,
    pub resumable: bool,
    pub priority: String,
    pub cancel_requested: bool,
    pub budget_exhausted: bool,
    pub budget_reason: Option<String>,
    pub budget: Option<MaintenanceBudgetSnapshot>,
    pub scanned: u64,
    pub rewritten: u64,
    pub skipped: u64,
    pub skipped_current_layout: u64,
    pub skipped_healthy: u64,
    pub failed: u64,
    pub bytes_processed: u64,
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

fn priority_label(priority: IoPriority) -> String {
    format!("{priority:?}").to_ascii_lowercase()
}

fn request_priority(req: &ClusterRebalanceRequest) -> IoPriority {
    req.priority.unwrap_or(IoPriority::Low)
}

fn budget_for_request(req: &ClusterRebalanceRequest) -> MaintenanceBudget {
    MaintenanceBudget::new(MaintenanceBudgetConfig {
        max_duration: req.max_duration_seconds.map(Duration::from_secs),
        max_objects: req.max_objects.or(req.limit.map(|limit| limit as u64)),
        max_bytes: req.max_bytes,
        max_directories: req.max_directories,
    })
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
        mismatched_only: req.mismatched_only,
        resumable: true,
        priority: priority_label(request_priority(req)),
        budget: Some(budget_for_request(req).snapshot()),
        continuation_token: req.continuation_token.clone(),
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

fn mark_cancel_requested(status: &'static LazyLock<Mutex<ClusterRewriteJobStatus>>) {
    update_job(status, |guard| {
        guard.cancel_requested = true;
        guard.last_error = Some("job cancellation requested".to_string());
    });
}

fn mark_budget(
    status: &'static LazyLock<Mutex<ClusterRewriteJobStatus>>,
    budget: &MaintenanceBudget,
) {
    let snapshot = budget.snapshot();
    update_job(status, |guard| {
        guard.budget = Some(snapshot.clone());
        guard.budget_exhausted = snapshot.exhausted;
        guard.budget_reason = snapshot.exhaustion_reason.clone();
    });
}

fn should_stop_job(
    status: &'static LazyLock<Mutex<ClusterRewriteJobStatus>>,
    cancel: &CancellationToken,
    budget: &MaintenanceBudget,
) -> bool {
    if cancel.is_cancelled() {
        mark_cancel_requested(status);
        return true;
    }
    if budget.exhausted() {
        mark_budget(status, budget);
        return true;
    }
    false
}

fn shard_holder_node(runtime: &ClusterRuntime, shard: &ShardInfo) -> String {
    parse_peer_shard_locator(&shard.absolute_path)
        .map(|locator| locator.node_id)
        .unwrap_or_else(|| runtime.local_node_id.clone())
}

fn placement_role_matches(shard: &ShardInfo, placement: &ClusterShardPlacement) -> bool {
    let expected_parity = matches!(placement.role, ClusterShardRole::Parity);
    shard.is_parity == expected_parity
}

pub fn object_layout_status(
    meta: &ObjectMetadata,
    runtime: &ClusterRuntime,
) -> Result<ObjectLayoutStatus, AppError> {
    let active = runtime.membership.active_write_nodes();
    let layout =
        resolve_layout(active.len(), &runtime.layout_policy).map_err(AppError::BadRequest)?;
    let recorded_total_shards = meta
        .parts
        .first()
        .map(|part| part.shards.len())
        .unwrap_or_else(|| (meta.data_shards as usize).saturating_add(meta.parity_shards as usize));
    let mut mismatch_reasons = Vec::new();

    if meta.membership_epoch != runtime.membership.epoch {
        mismatch_reasons.push(format!(
            "membership_epoch recorded={} current={}",
            meta.membership_epoch, runtime.membership.epoch
        ));
    }
    if meta.data_shards as usize != layout.data_shards
        || meta.parity_shards as usize != layout.parity_shards
    {
        mismatch_reasons.push(format!(
            "ec_layout recorded={}+{} current={}+{}",
            meta.data_shards, meta.parity_shards, layout.data_shards, layout.parity_shards
        ));
    }

    let mut placement_matches_current = true;
    if meta.parts.is_empty() {
        if meta.total_size_bytes != 0 {
            placement_matches_current = false;
            mismatch_reasons.push("non-empty object has no recorded shard placement".to_string());
        }
    } else {
        let plan = build_placement_plan(
            &meta.fragment,
            &meta.object_key,
            &meta.version_id,
            runtime.membership.epoch,
            layout,
            &active,
        )?;

        for part in &meta.parts {
            if part.shards.len() != plan.placements.len() {
                placement_matches_current = false;
                mismatch_reasons.push(format!(
                    "part {} shard_count recorded={} current={}",
                    part.part_index,
                    part.shards.len(),
                    plan.placements.len()
                ));
            }
            for expected in &plan.placements {
                let Some(shard) = part
                    .shards
                    .iter()
                    .find(|shard| shard.shard_index == expected.shard_index)
                else {
                    placement_matches_current = false;
                    mismatch_reasons.push(format!(
                        "part {} missing shard {} for node {}",
                        part.part_index, expected.shard_index, expected.node_id
                    ));
                    continue;
                };
                let holder = shard_holder_node(runtime, shard);
                if holder != expected.node_id {
                    placement_matches_current = false;
                    mismatch_reasons.push(format!(
                        "part {} shard {} node recorded={} current={}",
                        part.part_index, expected.shard_index, holder, expected.node_id
                    ));
                }
                if !placement_role_matches(shard, expected) {
                    placement_matches_current = false;
                    let recorded_role = if shard.is_parity { "parity" } else { "data" };
                    let current_role = match expected.role {
                        ClusterShardRole::Data => "data",
                        ClusterShardRole::Parity => "parity",
                    };
                    mismatch_reasons.push(format!(
                        "part {} shard {} role recorded={} current={}",
                        part.part_index, expected.shard_index, recorded_role, current_role
                    ));
                }
            }
        }
    }

    let layout_matches_current = meta.membership_epoch == runtime.membership.epoch
        && meta.data_shards as usize == layout.data_shards
        && meta.parity_shards as usize == layout.parity_shards;
    let matches_current = layout_matches_current && placement_matches_current;
    let state = if matches_current {
        "current_layout"
    } else if meta.membership_epoch < runtime.membership.epoch
        || meta.data_shards as usize != layout.data_shards
        || meta.parity_shards as usize != layout.parity_shards
    {
        "older_recorded_layout"
    } else {
        "placement_mismatch"
    }
    .to_string();

    Ok(ObjectLayoutStatus {
        recorded_membership_epoch: meta.membership_epoch,
        current_membership_epoch: runtime.membership.epoch,
        recorded_data_shards: meta.data_shards,
        recorded_parity_shards: meta.parity_shards,
        current_data_shards: layout.data_shards,
        current_parity_shards: layout.parity_shards,
        recorded_total_shards,
        current_total_shards: layout.total_shards(),
        active_write_nodes: active.len(),
        layout_matches_current,
        placement_matches_current,
        state,
        redistributable: !matches_current,
        mismatch_reasons,
    })
}

pub fn metadata_matches_current_layout(meta: &ObjectMetadata, runtime: &ClusterRuntime) -> bool {
    object_layout_status(meta, runtime)
        .map(|status| status.layout_matches_current && status.placement_matches_current)
        .unwrap_or(false)
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

pub async fn rewrite_object_to_current_layout(
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
    let claims = Claims {
        fragment: fragment.clone(),
        fragment_alias: None,
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
    let part_size_bytes = REWRITE_PART_BYTES.min(total.max(1));
    let expected_parts = if total == 0 {
        1
    } else {
        total.div_ceil(part_size_bytes)
    };
    let params = InitiateUploadParams {
        content_type: Some(content_type.clone()),
        total_size_bytes: total,
        expected_parts,
        part_size_bytes: Some(part_size_bytes),
        packed_ext_u64: crate::core::index_store::packed_extension_key(&logical_path),
    };
    let init = initiate_cluster_upload(
        claims.clone(),
        fragment.clone(),
        object_id.clone(),
        params.clone(),
    )
    .await?;
    for part_index in 0..expected_parts {
        let offset = part_index.saturating_mul(part_size_bytes);
        let part_len = if total == 0 {
            0
        } else {
            (total - offset).min(part_size_bytes)
        };
        let source_for_stream = source.clone();
        let stream = stream::unfold(0u64, move |part_offset| {
            let meta = source_for_stream.clone();
            async move {
                if part_offset >= part_len {
                    return None;
                }
                let chunk_size = (part_len - part_offset).min(REWRITE_CHUNK_BYTES as u64) as usize;
                let query = ChunkQuery {
                    object_key: meta.object_key.clone(),
                    offset: offset.saturating_add(part_offset),
                    chunk_size,
                };
                let next = part_offset.saturating_add(chunk_size as u64);
                let result = download_object_chunk_from_metadata(meta, &query)
                    .await
                    .map(|(bytes, _)| Bytes::from(bytes));
                Some((result, next))
            }
        });
        put_cluster_part(
            claims.clone(),
            fragment.clone(),
            init.version_id.clone(),
            part_index,
            query.clone(),
            stream,
            part_len,
        )
        .await?;
    }
    let rewritten = match finalize_cluster_upload(
        claims,
        fragment.clone(),
        init.version_id.clone(),
        init.session_id,
        object_id.clone(),
        params,
    )
    .await?
    {
        crate::core::upload::FinalizeUploadResult::Complete(meta) => meta,
        crate::core::upload::FinalizeUploadResult::Incomplete(incomplete) => {
            return Err(AppError::Conflict(format!(
                "cluster rewrite incomplete: missing parts {:?}",
                incomplete.missing_parts
            )));
        }
    };
    commit_upload_quorum(&fragment, &logical_path, &object_id, &rewritten).await?;
    Ok(true)
}

async fn run_rewrite_job(
    status: &'static LazyLock<Mutex<ClusterRewriteJobStatus>>,
    req: ClusterRebalanceRequest,
    repair_only: bool,
) {
    let job_name = if repair_only {
        "cluster_repair"
    } else {
        "cluster_rebalance"
    };
    let job_kind = if repair_only {
        IoJobKind::ClusterRepair
    } else {
        IoJobKind::ClusterRebalance
    };
    let priority = request_priority(&req);
    let job_guard = match register_io_job(job_name, job_kind, priority) {
        Ok(guard) => guard,
        Err(err) => {
            update_job(status, |guard| {
                guard.failed = guard.failed.saturating_add(1);
                guard.last_error = Some(err.to_string());
            });
            finish_job(status);
            return;
        }
    };
    let cancel = job_guard.token();
    let mut budget = budget_for_request(&req);
    mark_budget(status, &budget);

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
    let mut continuation = req.continuation_token.clone();
    let mut remaining = req.limit.unwrap_or(usize::MAX).max(1);
    while remaining > 0 {
        if should_stop_job(status, &cancel, &budget) {
            break;
        }
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
            if should_stop_job(status, &cancel, &budget) {
                break;
            }
            remaining = remaining.saturating_sub(1);
            budget.record_object();
            update_job(status, |guard| {
                guard.scanned = guard.scanned.saturating_add(1);
                guard.last_object = Some(entry.path.clone());
                guard.budget = Some(budget.snapshot());
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
            } else if req.mismatched_only {
                !metadata_matches_current_layout(&meta, &runtime)
            } else {
                true
            };
            if !should_rewrite {
                update_job(status, |guard| {
                    guard.skipped = guard.skipped.saturating_add(1);
                    if repair_only {
                        guard.skipped_healthy = guard.skipped_healthy.saturating_add(1);
                    } else if req.mismatched_only {
                        guard.skipped_current_layout =
                            guard.skipped_current_layout.saturating_add(1);
                    }
                });
                continue;
            }
            let rewrite_bytes = meta.total_size_bytes;
            budget.record_bytes(rewrite_bytes);
            mark_budget(status, &budget);
            match rewrite_object_to_current_layout(entry.path, entry.object_id, meta, req.dry_run)
                .await
            {
                Ok(true) => {
                    update_job(status, |guard| {
                        guard.rewritten = guard.rewritten.saturating_add(1);
                        if !req.dry_run {
                            guard.bytes_processed =
                                guard.bytes_processed.saturating_add(rewrite_bytes);
                        }
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
            guard.budget = Some(budget.snapshot());
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

pub async fn run_rewrite_fragment_once(
    req: ClusterRebalanceRequest,
    repair_only: bool,
) -> ClusterRewriteJobStatus {
    let job_name = if repair_only {
        format!("cluster_repair:{}", req.fragment)
    } else {
        format!("cluster_rebalance:{}", req.fragment)
    };
    let job_kind = if repair_only {
        IoJobKind::ClusterRepair
    } else {
        IoJobKind::ClusterRebalance
    };
    let priority = request_priority(&req);
    let mut out = ClusterRewriteJobStatus {
        running: true,
        job: job_name.clone(),
        fragment: req.fragment.clone(),
        dry_run: req.dry_run,
        mismatched_only: req.mismatched_only,
        resumable: true,
        priority: priority_label(priority),
        continuation_token: req.continuation_token.clone(),
        started_at_unix: Some(chrono::Utc::now().timestamp()),
        ..ClusterRewriteJobStatus::default()
    };
    let job_guard = match register_io_job(&job_name, job_kind, priority) {
        Ok(guard) => guard,
        Err(err) => {
            out.running = false;
            out.failed = out.failed.saturating_add(1);
            out.last_error = Some(err.to_string());
            out.finished_at_unix = Some(chrono::Utc::now().timestamp());
            return out;
        }
    };
    let cancel = job_guard.token();
    let mut budget = budget_for_request(&req);
    out.budget = Some(budget.snapshot());

    let runtime = match active_cluster_runtime() {
        Ok(runtime) => runtime,
        Err(err) => {
            out.running = false;
            out.failed = out.failed.saturating_add(1);
            out.last_error = Some(err.to_string());
            out.finished_at_unix = Some(chrono::Utc::now().timestamp());
            return out;
        }
    };
    let mut continuation = req.continuation_token.clone();
    let mut remaining = req.limit.unwrap_or(usize::MAX).max(1);
    while remaining > 0 {
        if cancel.is_cancelled() || budget.exhausted() {
            out.cancel_requested = cancel.is_cancelled();
            out.budget_exhausted = budget.exhausted();
            out.budget_reason = budget.snapshot().exhaustion_reason;
            break;
        }
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
                out.failed = out.failed.saturating_add(1);
                out.last_error = Some(err.to_string());
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
            if cancel.is_cancelled() || budget.exhausted() {
                out.cancel_requested = cancel.is_cancelled();
                out.budget_exhausted = budget.exhausted();
                out.budget_reason = budget.snapshot().exhaustion_reason;
                break;
            }
            remaining = remaining.saturating_sub(1);
            budget.record_object();
            out.scanned = out.scanned.saturating_add(1);
            out.last_object = Some(entry.path.clone());
            out.budget = Some(budget.snapshot());
            let meta =
                match index::read_object_version(&req.fragment, &entry.object_id, &entry.version_id)
                    .await
                {
                    Ok(meta) => meta,
                    Err(err) => {
                        out.failed = out.failed.saturating_add(1);
                        out.last_error = Some(err.to_string());
                        continue;
                    }
                };
            let should_rewrite = if repair_only {
                object_has_unreadable_shard(&meta).await
            } else if req.mismatched_only {
                !metadata_matches_current_layout(&meta, &runtime)
            } else {
                true
            };
            if !should_rewrite {
                out.skipped = out.skipped.saturating_add(1);
                if repair_only {
                    out.skipped_healthy = out.skipped_healthy.saturating_add(1);
                } else if req.mismatched_only {
                    out.skipped_current_layout = out.skipped_current_layout.saturating_add(1);
                }
                continue;
            }
            let rewrite_bytes = meta.total_size_bytes;
            budget.record_bytes(rewrite_bytes);
            out.budget = Some(budget.snapshot());
            out.budget_exhausted = budget.exhausted();
            out.budget_reason = budget.snapshot().exhaustion_reason;
            match rewrite_object_to_current_layout(entry.path, entry.object_id, meta, req.dry_run)
                .await
            {
                Ok(true) => {
                    out.rewritten = out.rewritten.saturating_add(1);
                    if !req.dry_run {
                        out.bytes_processed = out.bytes_processed.saturating_add(rewrite_bytes);
                    }
                }
                Ok(false) => out.skipped = out.skipped.saturating_add(1),
                Err(err) => {
                    out.failed = out.failed.saturating_add(1);
                    out.last_error = Some(err.to_string());
                }
            }
        }
        continuation = page.continuation_token;
        out.continuation_token = continuation.clone();
        out.budget = Some(budget.snapshot());
        if continuation.is_none() {
            break;
        }
    }
    out.running = false;
    out.finished_at_unix = Some(chrono::Utc::now().timestamp());
    out
}

pub fn stop_rebalance() -> ClusterRewriteJobStatus {
    if request_io_job_cancel("cluster_rebalance") {
        mark_cancel_requested(&REBALANCE_STATUS);
    }
    rebalance_status()
}

pub fn stop_repair() -> ClusterRewriteJobStatus {
    if request_io_job_cancel("cluster_repair") {
        mark_cancel_requested(&REPAIR_STATUS);
    }
    repair_status()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::disk_manager::{PartMetadata, ShardInfo};
    use crate::distribution::membership::{ClusterMembership, ClusterNode, NodeState};
    use crate::distribution::peer::{PeerShardTarget, encode_peer_shard_locator};

    fn node(id: &str) -> ClusterNode {
        ClusterNode {
            id: id.into(),
            endpoint: format!("http://{id}"),
            native_endpoint: Some(format!("n2://{id}:50002")),
            state: Some(NodeState::Online),
            weight: 1,
            index_roots: vec![],
            blob_roots: vec![],
            capacity_bytes: None,
            used_bytes: None,
            last_seen_unix: None,
        }
    }

    fn runtime(epoch: u64) -> ClusterRuntime {
        ClusterRuntime {
            local_node_id: "a".into(),
            membership_path: "membership.json".into(),
            layout_policy: crate::distribution::layout::ClusterLayoutPolicy::default(),
            membership: ClusterMembership {
                cluster_id: "test".into(),
                epoch,
                nodes: vec![node("a"), node("b"), node("c")],
            },
            auto_rebalance: false,
        }
    }

    fn base_meta(epoch: u64, data_shards: u16, parity_shards: u16) -> ObjectMetadata {
        ObjectMetadata {
            object_key: "obj".into(),
            packed_ext_u64: 0,
            fragment: "frag".into(),
            version_id: "v".into(),
            upload_session_id: "s".into(),
            owner_uid: "u".into(),
            content_type: "application/octet-stream".into(),
            total_size_bytes: 1,
            created_at: 0,
            membership_epoch: epoch,
            data_shards,
            parity_shards,
            stripe_size_bytes: 4096,
            is_delete_marker: false,
            parts: vec![],
        }
    }

    fn meta_matching_runtime(runtime: &ClusterRuntime) -> ObjectMetadata {
        let active = runtime.membership.active_write_nodes();
        let layout = resolve_layout(active.len(), &runtime.layout_policy).unwrap();
        let plan = build_placement_plan(
            "frag",
            "obj",
            "v",
            runtime.membership.epoch,
            layout,
            &active,
        )
        .unwrap();
        let shards = plan
            .placements
            .iter()
            .map(|placement| {
                let is_parity = matches!(placement.role, ClusterShardRole::Parity);
                let absolute_path = if placement.node_id == runtime.local_node_id {
                    format!("/tmp/part_0_shard_{}", placement.shard_index)
                } else {
                    encode_peer_shard_locator(&PeerShardTarget {
                        node_id: placement.node_id.clone(),
                        endpoint: placement
                            .native_endpoint
                            .clone()
                            .unwrap_or_else(|| placement.endpoint.clone()),
                        fragment: "frag".into(),
                        object_id: "obj".into(),
                        version_id: "v".into(),
                        part_index: 0,
                        shard_index: placement.shard_index,
                        is_parity,
                        expected_len: 1,
                    })
                };
                ShardInfo {
                    shard_index: placement.shard_index,
                    file_name: format!("part_0_shard_{}", placement.shard_index),
                    absolute_path,
                    size_bytes: 1,
                    checksum: String::new(),
                    is_parity,
                }
            })
            .collect::<Vec<_>>();
        let mut meta = base_meta(
            runtime.membership.epoch,
            layout.data_shards as u16,
            layout.parity_shards as u16,
        );
        meta.parts = vec![PartMetadata {
            part_index: 0,
            size_bytes: 1,
            created_at: 0,
            checksum: None,
            shards,
        }];
        meta
    }

    #[test]
    fn layout_match_accepts_exact_current_epoch_shape_and_placement() {
        let runtime = runtime(4);
        let meta = meta_matching_runtime(&runtime);
        let status = object_layout_status(&meta, &runtime).unwrap();
        assert_eq!(status.state, "current_layout");
        assert!(status.layout_matches_current);
        assert!(status.placement_matches_current);
        assert!(metadata_matches_current_layout(&meta, &runtime));
    }

    #[test]
    fn layout_match_rejects_older_recorded_epoch_even_when_shards_are_readable() {
        let runtime = runtime(4);
        let mut meta = meta_matching_runtime(&runtime);
        meta.membership_epoch = 2;
        let status = object_layout_status(&meta, &runtime).unwrap();
        assert_eq!(status.state, "older_recorded_layout");
        assert!(status.placement_matches_current);
        assert!(!status.layout_matches_current);
        assert!(!metadata_matches_current_layout(&meta, &runtime));
    }

    #[test]
    fn layout_match_rejects_missing_remote_cluster_holders() {
        let runtime = runtime(1);
        let mut meta = base_meta(0, 1, 1);
        meta.parts = vec![PartMetadata {
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
        }];
        assert!(!metadata_matches_current_layout(&meta, &runtime));
    }
}
