use crate::core::checksum::Sha256Digest;
use crate::core::constants::{PARITY_SUFFIX, SHARD_SUFFIX};
use crate::core::disk_manager::{
    FinalizeIncompleteResponse, InitiateUploadParams, InitiateUploadResponse, ObjectMetadata,
    ObjectPathManager, PartMetadata, ShardInfo, UploadPartQuery, UploadStatusResponse,
};
use crate::core::ec::ErasureStreamWriter;
use crate::core::index;
use crate::core::upload::{
    ActiveSessionInfo, FinalizeUploadResult, ListUploadsResponse, UploadSessionManifest,
};
use crate::core::upload::{DirectWriter, create_direct_file};
use crate::distribution::membership::{
    active_cluster_runtime, live_active_write_nodes_for_pool, live_newest_pool_write_nodes,
};
use crate::distribution::peer::{
    PeerShardTarget, RemoteShardWriter, encode_peer_shard_locator, invalidate_local_usage_cache,
    publish_cluster_object_metadata, publish_cluster_object_metadata_after_shard_sync,
};
use crate::distribution::placement::{ClusterPlacementPlan, build_placement_plan};
use crate::distribution::root_routing::writable_local_roots;
use crate::distribution::{
    ClusterEcLayout, resolve_configured_layout, resolve_survival_write_layout,
};
use crate::enums::app_error_enum::AppError;
use crate::io_control::record_io_latency;
use crate::settings::config::CONFIG;
use crate::structs::auth_structs::Claims;
use bytes::Bytes;
use chrono::Utc;
use dashmap::DashMap;
use futures_util::Stream;
use futures_util::StreamExt;
use futures_util::future::join_all;
use serde::Serialize;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::fs;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tracing::warn;
use ulid::Ulid;

const MIN_DYNAMIC_STRIPE_SIZE: usize = 4 * 1024;
const TINY_OBJECT_STRIPE_4K_LIMIT: u64 = 64 * 1024;
const SMALL_OBJECT_STRIPE_16K_LIMIT: u64 = 1024 * 1024;
pub const CLUSTER_ONE_SHOT_MAX_BYTES: u64 = 16 * 1024 * 1024;

const STANDARD_SESSION_OPEN: u8 = 0;
const STANDARD_SESSION_FINALIZING: u8 = 1;
const STANDARD_SESSION_FINALIZED: u8 = 2;
const STANDARD_SESSION_CANCELLED: u8 = 3;
const STANDARD_PART_EMPTY: u8 = 0;
const STANDARD_PART_UPLOADING: u8 = 1;
const STANDARD_PART_COMPLETE: u8 = 2;

/// Coordinator-local state for N2's speed-first multipart family. DashMap
/// shards only the tiny registry/part maps; no request body, EC work, peer I/O,
/// fsync, NCDX publication, or NCNX publication runs under a map guard.
static STANDARD_UPLOADS: LazyLock<DashMap<String, Arc<StandardUploadSession>>> =
    LazyLock::new(DashMap::new);

#[derive(Debug)]
struct StandardPartSlot {
    state: AtomicU8,
    record: OnceLock<PartMetadata>,
}

impl StandardPartSlot {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(STANDARD_PART_EMPTY),
            record: OnceLock::new(),
        }
    }
}

#[derive(Debug)]
struct StandardUploadSession {
    manifest: UploadSessionManifest,
    logical_path: String,
    lifecycle: AtomicU8,
    in_flight_parts: AtomicUsize,
    last_activity_at: AtomicI64,
    parts: DashMap<u64, Arc<StandardPartSlot>>,
    finalized_metadata: OnceLock<ObjectMetadata>,
}

fn standard_session_key(fragment: &str, version_id: &str) -> String {
    format!("{fragment}\u{001f}{version_id}")
}

fn standard_session(fragment: &str, version_id: &str) -> Option<Arc<StandardUploadSession>> {
    STANDARD_UPLOADS
        .get(&standard_session_key(fragment, version_id))
        .map(|entry| Arc::clone(entry.value()))
}

struct StandardPartClaim {
    session: Arc<StandardUploadSession>,
    slot: Arc<StandardPartSlot>,
    completed: bool,
}

impl StandardPartClaim {
    fn complete(mut self, part: PartMetadata) -> Result<(), AppError> {
        if self.session.lifecycle.load(Ordering::Acquire) == STANDARD_SESSION_CANCELLED {
            return Err(AppError::Conflict("Upload session was cancelled".into()));
        }
        self.slot
            .record
            .set(part)
            .map_err(|_| AppError::Conflict("Multipart part was completed concurrently".into()))?;
        self.session
            .last_activity_at
            .store(Utc::now().timestamp(), Ordering::Release);
        self.slot
            .state
            .store(STANDARD_PART_COMPLETE, Ordering::Release);
        self.completed = true;
        Ok(())
    }
}

impl Drop for StandardPartClaim {
    fn drop(&mut self) {
        if !self.completed {
            self.slot
                .state
                .store(STANDARD_PART_EMPTY, Ordering::Release);
        }
        self.session.in_flight_parts.fetch_sub(1, Ordering::AcqRel);
    }
}

enum StandardPartClaimResult {
    Claimed(StandardPartClaim),
    AlreadyComplete,
}

fn standard_request_matches(
    session: &StandardUploadSession,
    logical_path: &str,
    params: &InitiateUploadParams,
) -> bool {
    let content_type_matches = params
        .content_type
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|requested| requested == session.manifest.content_type)
        .unwrap_or(true);
    let part_size_matches = params
        .part_size_bytes
        .filter(|value| *value > 0)
        .map(|requested| requested == session.manifest.part_size_bytes)
        .unwrap_or(true);

    session.lifecycle.load(Ordering::Acquire) == STANDARD_SESSION_OPEN
        && session.logical_path == logical_path.trim_start_matches('/')
        && session.manifest.total_size_bytes == params.total_size_bytes
        && session.manifest.expected_parts == params.expected_parts
        && part_size_matches
        && content_type_matches
}

fn claim_standard_part(
    session: Arc<StandardUploadSession>,
    part_index: u64,
) -> Result<StandardPartClaimResult, AppError> {
    if part_index >= session.manifest.expected_parts {
        return Err(AppError::BadRequest("Part index out of range".into()));
    }
    if session.lifecycle.load(Ordering::Acquire) != STANDARD_SESSION_OPEN {
        return Err(AppError::Conflict(
            "Multipart session is not accepting parts".into(),
        ));
    }
    let slot = session
        .parts
        .entry(part_index)
        .or_insert_with(|| Arc::new(StandardPartSlot::new()))
        .clone();
    match slot.state.compare_exchange(
        STANDARD_PART_EMPTY,
        STANDARD_PART_UPLOADING,
        Ordering::AcqRel,
        Ordering::Acquire,
    ) {
        Ok(_) => {
            session.in_flight_parts.fetch_add(1, Ordering::AcqRel);
            session
                .last_activity_at
                .store(Utc::now().timestamp(), Ordering::Release);
            if session.lifecycle.load(Ordering::Acquire) != STANDARD_SESSION_OPEN {
                slot.state.store(STANDARD_PART_EMPTY, Ordering::Release);
                session.in_flight_parts.fetch_sub(1, Ordering::AcqRel);
                return Err(AppError::Conflict("Multipart session is finalizing".into()));
            }
            Ok(StandardPartClaimResult::Claimed(StandardPartClaim {
                session,
                slot,
                completed: false,
            }))
        }
        Err(STANDARD_PART_COMPLETE) => Ok(StandardPartClaimResult::AlreadyComplete),
        Err(STANDARD_PART_UPLOADING) => Err(AppError::Conflict(
            "Multipart part is already uploading".into(),
        )),
        Err(_) => Err(AppError::Conflict("Multipart part is unavailable".into())),
    }
}

fn validate_cluster_one_shot_size(content_length: u64) -> Result<(), AppError> {
    if content_length > CLUSTER_ONE_SHOT_MAX_BYTES {
        return Err(AppError::PayloadTooLarge(format!(
            "Object too large for one-shot upload; use multipart for objects over {} bytes",
            CLUSTER_ONE_SHOT_MAX_BYTES
        )));
    }
    Ok(())
}

#[derive(Debug, Serialize)]
pub struct DistributedPartWrite {
    pub part: PartMetadata,
    pub pool_id: String,
    pub pool_generation: u64,
    pub membership_epoch: u64,
    pub data_shards: u16,
    pub parity_shards: u16,
}

enum ClusterShardWriter {
    Local(LocalShardWriter),
    Remote(RemoteShardWriter),
}

struct LocalShardWriter {
    file: DirectWriter,
    tmp_path: PathBuf,
    stable_path: PathBuf,
    file_name: String,
    shard_index: usize,
    is_parity: bool,
    expected_len: u64,
    locator: PeerShardTarget,
    deferred_durability: bool,
}

impl AsyncWrite for LocalShardWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.file).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.file).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.file).poll_shutdown(cx)
    }
}

impl LocalShardWriter {
    async fn finish(
        mut self,
        checksum: String,
        block_checksums: Vec<Sha256Digest>,
        checksum_block_size: usize,
    ) -> Result<ShardInfo, AppError> {
        self.file.flush().await.map_err(|e| {
            AppError::Io(
                e,
                format!("flush cluster shard {}", self.tmp_path.display()),
            )
        })?;
        if !self.deferred_durability {
            self.file.sync_data().await.map_err(|e| {
                AppError::Io(e, format!("sync cluster shard {}", self.tmp_path.display()))
            })?;
        }
        drop(self.file);
        if fs::try_exists(&self.stable_path).await.unwrap_or(false) {
            let _ = fs::remove_file(&self.stable_path).await;
        }
        fs::rename(&self.tmp_path, &self.stable_path)
            .await
            .map_err(|e| {
                AppError::Io(
                    e,
                    format!(
                        "publish cluster shard {} -> {}",
                        self.tmp_path.display(),
                        self.stable_path.display()
                    ),
                )
            })?;
        if !self.deferred_durability {
            crate::core::upload::sync_dir(self.stable_path.parent().ok_or_else(|| {
                AppError::InternalServerError("cluster shard path has no parent".into())
            })?)
            .await?;
        }
        invalidate_local_usage_cache(Some(&self.locator.fragment));
        Ok(ShardInfo {
            shard_index: self.shard_index,
            file_name: self.file_name,
            absolute_path: encode_peer_shard_locator(&self.locator),
            size_bytes: self.expected_len,
            checksum,
            block_checksums,
            checksum_block_size,
            is_parity: self.is_parity,
        })
    }
}

fn configured_layout(
    runtime: &crate::distribution::membership::ClusterRuntime,
) -> Result<ClusterEcLayout, AppError> {
    let pool = runtime.membership.newest_writable_pool().ok_or_else(|| {
        AppError::ServiceUnavailable("cluster has no writable server pool".into())
    })?;
    resolve_configured_layout(pool.node_ids.len(), &runtime.layout_policy)
        .map_err(AppError::BadRequest)
}

fn validate_write_admission(
    runtime: &crate::distribution::membership::ClusterRuntime,
    reachable_namespace_nodes: usize,
    reachable_blob_nodes: usize,
    write_layout: ClusterEcLayout,
) -> Result<(), AppError> {
    let write_pool = runtime.membership.newest_writable_pool().ok_or_else(|| {
        AppError::ServiceUnavailable("cluster has no writable server pool".into())
    })?;
    let configured = configured_layout(runtime)?;
    if write_layout != configured {
        return Err(AppError::ServiceUnavailable(format!(
            "N2 survival writes preserve the configured {}+{} layout",
            configured.data_shards, configured.parity_shards
        )));
    }
    if reachable_namespace_nodes < configured.data_shards {
        return Err(AppError::ServiceUnavailable(format!(
            "N2 survival namespace publication requires at least K={} reachable pool nodes, but only {} are reachable",
            configured.data_shards, reachable_namespace_nodes
        )));
    }
    if reachable_blob_nodes < configured.data_shards {
        return Err(AppError::ServiceUnavailable(format!(
            "N2 survival object writes preserve configured EC {}+{} and require at least K={} distinct writable server-pool nodes",
            configured.data_shards, configured.parity_shards, configured.data_shards
        )));
    }
    debug_assert_eq!(write_pool.policy.object_data_shards, configured.data_shards);
    Ok(())
}

fn validate_pinned_upload_layout(
    layout: ClusterEcLayout,
    active_nodes: usize,
) -> Result<(), AppError> {
    if layout.data_shards == 0 || layout.total_shards() > active_nodes {
        return Err(AppError::ServiceUnavailable(format!(
            "upload session layout {}+{} requires {} writable node(s), but only {} are reachable",
            layout.data_shards,
            layout.parity_shards,
            layout.total_shards(),
            active_nodes
        )));
    }
    Ok(())
}

fn configured_write_context() -> Result<
    (
        std::sync::Arc<crate::distribution::membership::ClusterRuntime>,
        ClusterEcLayout,
    ),
    AppError,
> {
    crate::core::operation_gate::ensure_client_mutation_allowed()?;
    let runtime = active_cluster_runtime()?;
    if runtime.membership.newest_writable_pool().is_none() {
        return Err(AppError::ServiceUnavailable(
            "cluster has no writable server pool".into(),
        ));
    }
    let layout = configured_layout(&runtime)?;
    Ok((runtime, layout))
}

async fn current_write_context() -> Result<
    (
        std::sync::Arc<crate::distribution::membership::ClusterRuntime>,
        Vec<crate::distribution::membership::ClusterNode>,
        ClusterEcLayout,
    ),
    AppError,
> {
    let (runtime, layout) = configured_write_context()?;
    let active = live_newest_pool_write_nodes(&runtime).await;
    let configured = runtime
        .membership
        .newest_writable_pool()
        .map(|pool| pool.node_ids.len())
        .unwrap_or_default();
    let live_layout =
        resolve_survival_write_layout(active.len(), configured, &runtime.layout_policy)
            .map_err(AppError::ServiceUnavailable)?;
    validate_write_admission(&runtime, active.len(), active.len(), layout)?;
    debug_assert_eq!(live_layout, layout);
    Ok((runtime, active, layout))
}

impl AsyncWrite for ClusterShardWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match &mut *self {
            Self::Local(writer) => Pin::new(writer).poll_write(cx, buf),
            Self::Remote(writer) => Pin::new(writer).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match &mut *self {
            Self::Local(writer) => Pin::new(writer).poll_flush(cx),
            Self::Remote(writer) => Pin::new(writer).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match &mut *self {
            Self::Local(writer) => Pin::new(writer).poll_shutdown(cx),
            Self::Remote(writer) => Pin::new(writer).poll_shutdown(cx),
        }
    }
}

impl ClusterShardWriter {
    async fn finish(
        self,
        checksum: String,
        block_checksums: Vec<Sha256Digest>,
        checksum_block_size: usize,
    ) -> Result<ShardInfo, AppError> {
        match self {
            Self::Local(writer) => {
                writer
                    .finish(checksum, block_checksums, checksum_block_size)
                    .await
            }
            Self::Remote(writer) => {
                let mut info = writer.finish().await?;
                if !info.checksum.eq_ignore_ascii_case(&checksum) {
                    return Err(AppError::BadRequest(format!(
                        "remote shard checksum mismatch for shard {}",
                        info.shard_index
                    )));
                }
                info.checksum = checksum;
                info.block_checksums = block_checksums;
                info.checksum_block_size = checksum_block_size;
                Ok(info)
            }
        }
    }
}

fn configured_block_size() -> usize {
    crate::core::upload::STRIPE_SIZE_ATOMIC
        .get()
        .map(|value| value.load(std::sync::atomic::Ordering::Relaxed))
        .unwrap_or_else(crate::core::disk_manager::default_stripe_size)
}

fn choose_upload_stripe_size(content_length: u64) -> usize {
    let configured = configured_block_size();
    if configured <= MIN_DYNAMIC_STRIPE_SIZE {
        return configured;
    }
    let candidate = if content_length <= TINY_OBJECT_STRIPE_4K_LIMIT {
        MIN_DYNAMIC_STRIPE_SIZE
    } else if content_length <= SMALL_OBJECT_STRIPE_16K_LIMIT {
        16 * 1024
    } else {
        configured
    };
    candidate.min(configured)
}

fn planned_part_size(total_size: u64, expected_parts: u64, part_size: u64, index: u64) -> u64 {
    if expected_parts <= 1 {
        return total_size;
    }
    let start = index.saturating_mul(part_size);
    if start >= total_size {
        return 0;
    }
    let end = start.saturating_add(part_size).min(total_size);
    end.saturating_sub(start)
}

fn expected_part_size(manifest: &UploadSessionManifest, part_index: u64) -> u64 {
    let part_size = if manifest.part_size_bytes == 0 {
        manifest.total_size_bytes
    } else {
        manifest.part_size_bytes
    };
    planned_part_size(
        manifest.total_size_bytes,
        manifest.expected_parts.max(1),
        part_size,
        part_index,
    )
}

fn stable_name(part_index: u64, shard_index: usize, is_parity: bool) -> String {
    let ext = if is_parity {
        PARITY_SUFFIX
    } else {
        SHARD_SUFFIX
    };
    format!("part_{part_index}_shard_{shard_index}{ext}")
}

fn target_for_placement(
    plan: &ClusterPlacementPlan,
    placement: &crate::distribution::placement::ClusterShardPlacement,
    part_index: u64,
    final_shard_len: u64,
    deferred_durability: bool,
) -> PeerShardTarget {
    let is_parity = matches!(
        placement.role,
        crate::distribution::placement::ClusterShardRole::Parity
    );
    PeerShardTarget {
        pool_id: plan.pool_id.clone(),
        pool_generation: plan.pool_generation,
        node_id: placement.node_id.clone(),
        instance_id: placement.instance_id.clone(),
        binding_epoch: plan.binding_epoch,
        root_id: placement.root_id.clone(),
        endpoint: placement
            .native_endpoint
            .clone()
            .unwrap_or_else(|| placement.endpoint.clone()),
        fragment: plan.fragment.clone(),
        object_id: plan.object_key.clone(),
        version_id: plan.version_id.clone(),
        part_index,
        shard_index: placement.shard_index,
        is_parity,
        expected_len: final_shard_len,
        deferred_durability,
    }
}

fn planned_shard_info(
    target: &PeerShardTarget,
    checksum: String,
    block_checksums: Vec<Sha256Digest>,
    checksum_block_size: usize,
) -> ShardInfo {
    ShardInfo {
        shard_index: target.shard_index,
        file_name: stable_name(target.part_index, target.shard_index, target.is_parity),
        absolute_path: encode_peer_shard_locator(target),
        size_bytes: target.expected_len,
        checksum,
        block_checksums,
        checksum_block_size,
        is_parity: target.is_parity,
    }
}

async fn open_local_writer(target: PeerShardTarget) -> Result<ClusterShardWriter, AppError> {
    let runtime = active_cluster_runtime()?;
    if runtime.local_is_reserve
        || runtime.local_node_id != target.node_id
        || runtime.local_instance_id != target.instance_id
        || runtime.membership.binding_epoch != target.binding_epoch
    {
        return Err(AppError::Conflict(format!(
            "local logical slot '{}' changed binding before shard writer creation",
            target.node_id
        )));
    }
    let pm = ObjectPathManager::new();
    let file_name = stable_name(target.part_index, target.shard_index, target.is_parity);
    let mut failures = Vec::new();
    for root in writable_local_roots(&runtime, &target.root_id, target.expected_len)? {
        let dir = pm.get_shard_dir_on_disk(
            &PathBuf::from(&root.path),
            &target.fragment,
            &target.object_id,
            &target.version_id,
        );
        if let Err(error) = fs::create_dir_all(&dir).await {
            failures.push(format!("{}: create directory: {}", root.id, error));
            continue;
        }
        let tmp_path = dir.join(format!("{file_name}.tmp.{}", Ulid::new()));
        let stable_path = dir.join(&file_name);
        match create_direct_file(&tmp_path).await {
            Ok(file) => {
                return Ok(ClusterShardWriter::Local(LocalShardWriter {
                    file: DirectWriter::new(file),
                    tmp_path,
                    stable_path,
                    file_name,
                    shard_index: target.shard_index,
                    is_parity: target.is_parity,
                    expected_len: target.expected_len,
                    deferred_durability: target.deferred_durability,
                    // The immutable locator keeps the deterministic preferred
                    // root. The node resolves it across current local disks.
                    locator: target,
                }));
            }
            Err(error) => failures.push(format!("{}: {}", root.id, error)),
        }
    }
    Err(AppError::ServiceUnavailable(format!(
        "node '{}' could not open shard {} on any healthy disk: {}",
        runtime.local_node_id,
        target.shard_index,
        failures.join("; ")
    )))
}

async fn open_writers_for_plan(
    plan: &ClusterPlacementPlan,
    part_index: u64,
    final_shard_len: u64,
    reachable: &BTreeSet<String>,
    deferred_durability: bool,
) -> Result<Vec<Option<ClusterShardWriter>>, AppError> {
    let runtime = active_cluster_runtime()?;
    let mut writers = Vec::with_capacity(plan.placements.len());
    for placement in &plan.placements {
        if !reachable.contains(&placement.node_id) {
            writers.push(None);
            continue;
        }
        let target = target_for_placement(
            plan,
            placement,
            part_index,
            final_shard_len,
            deferred_durability,
        );
        let writer = if placement.node_id == runtime.local_node_id {
            open_local_writer(target).await
        } else {
            RemoteShardWriter::spawn(target).map(ClusterShardWriter::Remote)
        };
        match writer {
            Ok(writer) => writers.push(Some(writer)),
            Err(error) => {
                warn!(
                    node_id = %placement.node_id,
                    root_id = %placement.root_id,
                    shard_index = placement.shard_index,
                    "N2 survival upload could not open an assigned shard writer: {error}"
                );
                writers.push(None);
            }
        }
    }
    if writers.iter().filter(|writer| writer.is_some()).count() < plan.data_shards {
        return Err(AppError::ServiceUnavailable(format!(
            "N2 survival upload requires K={} shard writers, but only {} opened",
            plan.data_shards,
            writers.iter().filter(|writer| writer.is_some()).count()
        )));
    }
    Ok(writers)
}

pub async fn write_cluster_part_from_stream<S>(
    fragment: &str,
    object_key: &str,
    version_id: &str,
    part_index: u64,
    content_length: u64,
    data_shards: usize,
    parity_shards: usize,
    pool_id: &str,
    pool_generation: u64,
    membership_epoch: u64,
    block_size: usize,
    body_stream: S,
    expected_checksum: Option<&str>,
    deferred_durability: bool,
) -> Result<DistributedPartWrite, AppError>
where
    S: Stream<Item = Result<Bytes, AppError>>,
{
    let runtime = active_cluster_runtime()?;
    let pool = runtime.membership.pool(pool_id).ok_or_else(|| {
        AppError::Conflict(format!(
            "upload is pinned to unknown server pool '{pool_id}'"
        ))
    })?;
    if pool.generation != pool_generation
        || pool.policy.object_data_shards != data_shards
        || pool.policy.object_parity_shards != parity_shards
    {
        return Err(AppError::Conflict(
            "upload session pool generation or static EC contract changed".into(),
        ));
    }
    let configured_nodes = runtime.membership.nodes_for_pool(pool_id);
    let layout = ClusterEcLayout {
        data_shards,
        parity_shards,
    };
    validate_pinned_upload_layout(layout, configured_nodes.len())?;
    let placement = build_placement_plan(
        fragment,
        object_key,
        version_id,
        pool_id,
        pool_generation,
        membership_epoch,
        runtime.membership.binding_epoch,
        layout,
        &configured_nodes,
    )?;
    let reachable = live_active_write_nodes_for_pool(&runtime, pool_id)
        .await
        .into_iter()
        .map(|node| node.id)
        .collect::<BTreeSet<_>>();
    let unavailable = placement
        .placements
        .iter()
        .filter(|placement| !reachable.contains(&placement.node_id))
        .map(|placement| placement.node_id.clone())
        .collect::<Vec<_>>();
    if reachable.len() < data_shards {
        return Err(AppError::ServiceUnavailable(format!(
            "N2 survival placement for {fragment}/{object_key} requires K={data_shards} reachable participants; unavailable participant(s): {}",
            unavailable.join(", ")
        )));
    }
    let stripe_data_width = block_size as u64 * data_shards as u64;
    let stripe_count = content_length.div_ceil(stripe_data_width.max(1));
    let final_shard_len = stripe_count.saturating_mul(block_size as u64);
    let open_started = Instant::now();
    let writers = open_writers_for_plan(
        &placement,
        part_index,
        final_shard_len,
        &reachable,
        deferred_durability,
    )
    .await?;
    record_io_latency(
        "cluster.upload.open_writers",
        open_started.elapsed(),
        0,
        true,
    );
    let mut stream_writer = ErasureStreamWriter::new(
        writers,
        data_shards,
        parity_shards,
        final_shard_len as usize,
        expected_checksum.is_some(),
        block_size,
    )?;
    let mut total_bytes_read = 0u64;
    futures_util::pin_mut!(body_stream);
    while let Some(chunk) = body_stream.next().await {
        let chunk = chunk?;
        if chunk.is_empty() {
            continue;
        }
        total_bytes_read = total_bytes_read.saturating_add(chunk.len() as u64);
        if total_bytes_read > content_length {
            return Err(AppError::BadRequest("Body exceeds Content-Length".into()));
        }
        stream_writer.write(&chunk).await?;
    }
    if total_bytes_read != content_length {
        return Err(AppError::BadRequest(format!(
            "Incomplete body: got {} bytes, expected {}",
            total_bytes_read, content_length
        )));
    }
    stream_writer.finalize().await?;
    if let Some(expected) = expected_checksum {
        let calculated = stream_writer.get_total_checksum();
        if calculated
            .as_deref()
            .is_some_and(|value| !value.eq_ignore_ascii_case(expected))
        {
            return Err(AppError::BadRequest("Checksum mismatch".into()));
        }
    }
    let shard_checksums = stream_writer.get_shard_checksums();
    let shard_block_checksums = stream_writer.get_shard_block_checksums();
    let total_checksum = stream_writer.get_total_checksum();
    let writers = stream_writer.into_writers();
    let finish_started = Instant::now();
    let finish_tasks = writers.into_iter().enumerate().map(|(idx, writer)| {
        let checksum = shard_checksums.get(idx).cloned().unwrap_or_default();
        let block_checksums = shard_block_checksums.get(idx).cloned().unwrap_or_default();
        async move {
            match writer {
                Some(writer) => writer
                    .finish(checksum, block_checksums, block_size)
                    .await
                    .map(Some),
                None => Ok(None),
            }
            .map(|shard| (idx, shard))
        }
    });
    let finish_results = join_all(finish_tasks).await;
    let successful_finishes = finish_results
        .iter()
        .filter(|result| matches!(result, Ok((_, Some(_)))))
        .count();
    let finish_ok = successful_finishes >= data_shards;
    record_io_latency(
        "cluster.upload.finish_shards",
        finish_started.elapsed(),
        content_length,
        finish_ok,
    );
    let mut finished = vec![None; placement.placements.len()];
    for result in finish_results {
        match result {
            Ok((index, shard)) => finished[index] = shard,
            Err(error) => warn!(
                fragment,
                object_key,
                part_index,
                "N2 survival upload lost a shard while finishing the part: {error}"
            ),
        }
    }
    let written = finished.iter().filter(|shard| shard.is_some()).count();
    if written < data_shards {
        return Err(AppError::ServiceUnavailable(format!(
            "N2 survival upload wrote {written} of {} EC pieces; K={data_shards} is required",
            layout.total_shards()
        )));
    }
    if written < layout.total_shards() {
        warn!(
            fragment,
            object_key,
            part_index,
            written_pieces = written,
            target_pieces = layout.total_shards(),
            "N2 committed a reconstructable degraded part; autonomous repair owns the exact missing slots"
        );
    }
    let mut shards = Vec::with_capacity(placement.placements.len());
    for (index, assigned) in placement.placements.iter().enumerate() {
        let checksum = shard_checksums.get(index).cloned().ok_or_else(|| {
            AppError::InternalServerError(format!(
                "N2 EC writer omitted checksum for shard {index}"
            ))
        })?;
        let block_checksums = shard_block_checksums.get(index).cloned().ok_or_else(|| {
            AppError::InternalServerError(format!(
                "N2 EC writer omitted block checksums for shard {index}"
            ))
        })?;
        let target = target_for_placement(
            &placement,
            assigned,
            part_index,
            final_shard_len,
            deferred_durability,
        );
        shards.push(
            finished[index].take().unwrap_or_else(|| {
                planned_shard_info(&target, checksum, block_checksums, block_size)
            }),
        );
    }
    shards.sort_by_key(|shard| shard.shard_index);
    Ok(DistributedPartWrite {
        pool_id: pool_id.to_string(),
        pool_generation,
        membership_epoch,
        data_shards: layout.data_shards as u16,
        parity_shards: layout.parity_shards as u16,
        part: PartMetadata {
            part_index,
            size_bytes: total_bytes_read,
            created_at: Utc::now().timestamp(),
            shards,
            checksum: total_checksum,
        },
    })
}

async fn prepare_cluster_upload_manifest(
    fragment: &str,
    object_key: String,
    params: &InitiateUploadParams,
) -> Result<UploadSessionManifest, AppError> {
    if params.expected_parts == 0 {
        return Err(AppError::BadRequest("expected_parts must be >= 1".into()));
    }
    let part_size_bytes = if params.expected_parts <= 1 {
        params.part_size_bytes.unwrap_or(params.total_size_bytes)
    } else {
        params
            .part_size_bytes
            .filter(|value| *value > 0)
            .ok_or_else(|| AppError::BadRequest("part_size_bytes is required".into()))?
    };
    let computed_parts = if params.total_size_bytes == 0 || part_size_bytes == 0 {
        1
    } else {
        params.total_size_bytes.div_ceil(part_size_bytes)
    };
    if computed_parts != params.expected_parts.max(1) {
        return Err(AppError::BadRequest(
            "expected_parts does not match total_size_bytes and part_size_bytes".into(),
        ));
    }
    let (runtime, _active, layout) = current_write_context().await?;
    let write_pool = runtime
        .membership
        .newest_writable_pool()
        .expect("write context requires a writable pool");
    let version_id = Ulid::new().to_string();
    let session_id = Ulid::new().to_string();
    let stripe_size = choose_upload_stripe_size(params.total_size_bytes);
    Ok(UploadSessionManifest {
        fragment: fragment.to_string(),
        object_key,
        packed_ext_u64: params.packed_ext_u64,
        version_id: version_id.clone(),
        session_id: session_id.clone(),
        total_size_bytes: params.total_size_bytes,
        expected_parts: params.expected_parts.max(1),
        part_size_bytes,
        content_type: params
            .content_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".into()),
        created_at: Utc::now().timestamp(),
        pool_id: write_pool.id.clone(),
        pool_generation: write_pool.generation,
        stripe_size_bytes: stripe_size,
        membership_epoch: runtime.membership.epoch,
        data_shards: layout.data_shards as u16,
        parity_shards: layout.parity_shards as u16,
    })
}

pub async fn initiate_cluster_upload(
    claims: Claims,
    fragment: String,
    object_key: String,
    params: InitiateUploadParams,
) -> Result<InitiateUploadResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    let manifest = prepare_cluster_upload_manifest(&fragment, object_key, &params).await?;
    let version_id = manifest.version_id.clone();
    let session_id = manifest.session_id.clone();
    index::open_session(&manifest, None).await?;
    Ok(InitiateUploadResponse {
        session_id,
        version_id,
    })
}

pub fn find_reusable_standard_cluster_upload(
    fragment: &str,
    logical_path: &str,
    params: &InitiateUploadParams,
) -> Option<InitiateUploadResponse> {
    STANDARD_UPLOADS
        .iter()
        .filter(|entry| entry.value().manifest.fragment == fragment)
        .filter(|entry| standard_request_matches(entry.value(), logical_path, params))
        .max_by_key(|entry| entry.value().manifest.created_at)
        .map(|entry| InitiateUploadResponse {
            session_id: entry.value().manifest.session_id.clone(),
            version_id: entry.value().manifest.version_id.clone(),
        })
}

/// Start the high-throughput multipart family without writing a session
/// manifest, pending NCNX binding, or per-part receipt. The coordinator owns
/// this compact state until finalize or restart.
pub async fn initiate_standard_cluster_upload(
    claims: Claims,
    fragment: String,
    physical_object_key: String,
    logical_path: String,
    params: InitiateUploadParams,
) -> Result<InitiateUploadResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    prune_standard_cluster_uploads(&fragment);
    if let Some(existing) = find_reusable_standard_cluster_upload(&fragment, &logical_path, &params)
    {
        return Ok(existing);
    }
    let manifest = prepare_cluster_upload_manifest(&fragment, physical_object_key, &params).await?;
    let response = InitiateUploadResponse {
        session_id: manifest.session_id.clone(),
        version_id: manifest.version_id.clone(),
    };
    let session = Arc::new(StandardUploadSession {
        manifest,
        logical_path: logical_path.trim_start_matches('/').to_string(),
        lifecycle: AtomicU8::new(STANDARD_SESSION_OPEN),
        in_flight_parts: AtomicUsize::new(0),
        last_activity_at: AtomicI64::new(Utc::now().timestamp()),
        parts: DashMap::new(),
        finalized_metadata: OnceLock::new(),
    });
    let key = standard_session_key(&fragment, &response.version_id);
    match STANDARD_UPLOADS.entry(key) {
        dashmap::mapref::entry::Entry::Occupied(existing) => Ok(InitiateUploadResponse {
            session_id: existing.get().manifest.session_id.clone(),
            version_id: existing.get().manifest.version_id.clone(),
        }),
        dashmap::mapref::entry::Entry::Vacant(entry) => {
            entry.insert(session);
            Ok(response)
        }
    }
}

pub fn standard_upload_binding(fragment: &str, version_id: &str) -> Option<(String, String)> {
    standard_session(fragment, version_id).map(|session| {
        (
            session.logical_path.clone(),
            session.manifest.object_key.clone(),
        )
    })
}

pub async fn put_standard_cluster_part<S>(
    claims: Claims,
    fragment: String,
    version_id: String,
    part_index: u64,
    query: UploadPartQuery,
    body_stream: S,
    content_length: u64,
) -> Result<(), AppError>
where
    S: Stream<Item = Result<Bytes, AppError>>,
{
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    let session = standard_session(&fragment, &version_id)
        .ok_or_else(|| AppError::BadRequest("Standard upload session not found".into()))?;
    let expected = expected_part_size(&session.manifest, part_index);
    if part_index >= session.manifest.expected_parts {
        return Err(AppError::BadRequest("Part index out of range".into()));
    }
    if expected != content_length {
        return Err(AppError::BadRequest(format!(
            "Part size does not match upload session: got {content_length}, expected {expected}"
        )));
    }
    let claim = match claim_standard_part(Arc::clone(&session), part_index)? {
        StandardPartClaimResult::AlreadyComplete => return Ok(()),
        StandardPartClaimResult::Claimed(claim) => claim,
    };
    let started = Instant::now();
    let part = write_cluster_part_from_stream(
        &fragment,
        &session.manifest.object_key,
        &version_id,
        part_index,
        content_length,
        usize::from(session.manifest.data_shards),
        usize::from(session.manifest.parity_shards),
        &session.manifest.pool_id,
        session.manifest.pool_generation,
        session.manifest.membership_epoch,
        session.manifest.stripe_size_bytes,
        body_stream,
        query.checksum.as_deref(),
        true,
    )
    .await;
    record_io_latency(
        "cluster.upload.standard_part",
        started.elapsed(),
        content_length,
        part.is_ok(),
    );
    claim.complete(part?.part)
}

pub fn get_standard_cluster_upload_status(
    claims: &Claims,
    fragment: &str,
    version_id: &str,
) -> Result<UploadStatusResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    let Some(session) = standard_session(fragment, version_id) else {
        return Ok(UploadStatusResponse::default());
    };
    let mut uploaded_parts = session
        .parts
        .iter()
        .filter_map(|entry| {
            (entry.value().state.load(Ordering::Acquire) == STANDARD_PART_COMPLETE)
                .then_some(*entry.key())
        })
        .collect::<Vec<_>>();
    uploaded_parts.sort_unstable();
    let committed_bytes = uploaded_parts
        .iter()
        .filter_map(|index| session.parts.get(index))
        .filter_map(|slot| slot.record.get().map(|part| part.size_bytes))
        .sum();
    Ok(UploadStatusResponse {
        committed_parts: uploaded_parts.len() as u64,
        committed_bytes,
        uploaded_parts,
        expected_parts: Some(session.manifest.expected_parts),
        total_size_bytes: Some(session.manifest.total_size_bytes),
        created_at: Some(session.manifest.created_at),
        session_id: Some(session.manifest.session_id.clone()),
        version_id: Some(session.manifest.version_id.clone()),
        object_key: Some(session.logical_path.clone()),
        physical_object_key: Some(session.manifest.object_key.clone()),
    })
}

fn standard_session_is_expired(session: &StandardUploadSession, now: i64, ttl: i64) -> bool {
    let lifecycle = session.lifecycle.load(Ordering::Acquire);
    let idle = now.saturating_sub(session.last_activity_at.load(Ordering::Acquire));
    // A finalized session can mean NCDX is durable while NCNX publication
    // is awaiting an idempotent retry. Never prune away that binding.
    lifecycle == STANDARD_SESSION_OPEN
        && idle >= ttl
        && session.in_flight_parts.load(Ordering::Acquire) == 0
}

fn prune_standard_cluster_uploads(fragment: &str) {
    let now = Utc::now().timestamp();
    let ttl = CONFIG.garbage_collector.file_ttl_seconds as i64;
    STANDARD_UPLOADS.retain(|_, session| {
        if session.manifest.fragment != fragment {
            return true;
        }
        let expired = standard_session_is_expired(session, now, ttl);
        if expired {
            session
                .lifecycle
                .store(STANDARD_SESSION_CANCELLED, Ordering::Release);
        }
        !expired
    });
}

pub fn list_standard_cluster_uploads(
    claims: &Claims,
    fragment: &str,
    limit: usize,
    offset: usize,
) -> Result<ListUploadsResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    prune_standard_cluster_uploads(fragment);
    let mut sessions = STANDARD_UPLOADS
        .iter()
        .filter(|entry| entry.value().manifest.fragment == fragment)
        .filter(|entry| entry.value().lifecycle.load(Ordering::Acquire) == STANDARD_SESSION_OPEN)
        .map(|entry| {
            let session = entry.value();
            let (committed_parts, committed_bytes) =
                session
                    .parts
                    .iter()
                    .fold((0u64, 0u64), |(count, bytes), slot| {
                        if slot.value().state.load(Ordering::Acquire) != STANDARD_PART_COMPLETE {
                            return (count, bytes);
                        }
                        match slot.value().record.get() {
                            Some(part) => (
                                count.saturating_add(1),
                                bytes.saturating_add(part.size_bytes),
                            ),
                            None => (count, bytes),
                        }
                    });
            ActiveSessionInfo {
                session_id: session.manifest.session_id.clone(),
                version_id: session.manifest.version_id.clone(),
                object_key: Some(session.logical_path.clone()),
                physical_object_key: Some(session.manifest.object_key.clone()),
                created_at: session.manifest.created_at,
                reserved_size: session.manifest.total_size_bytes,
                total_size_bytes: Some(session.manifest.total_size_bytes),
                expected_parts: Some(session.manifest.expected_parts),
                committed_parts,
                committed_bytes,
            }
        })
        .collect::<Vec<_>>();
    sessions.sort_by(|left, right| right.created_at.cmp(&left.created_at));
    let total = sessions.len();
    Ok(ListUploadsResponse {
        sessions: sessions
            .into_iter()
            .skip(offset)
            .take(limit.min(100))
            .collect(),
        total,
    })
}

pub async fn finalize_standard_cluster_upload(
    claims: Claims,
    fragment: String,
    version_id: String,
    session_id: String,
) -> Result<FinalizeUploadResult, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    let session = standard_session(&fragment, &version_id)
        .ok_or_else(|| AppError::BadRequest("Standard upload session not found".into()))?;
    if session.manifest.session_id != session_id {
        return Err(AppError::Forbidden("Session id mismatch".into()));
    }
    if session.lifecycle.load(Ordering::Acquire) == STANDARD_SESSION_FINALIZED {
        return session
            .finalized_metadata
            .get()
            .cloned()
            .map(FinalizeUploadResult::Complete)
            .ok_or_else(|| {
                AppError::InternalServerError(
                    "finalized standard upload is missing in-memory metadata".into(),
                )
            });
    }
    session
        .lifecycle
        .compare_exchange(
            STANDARD_SESSION_OPEN,
            STANDARD_SESSION_FINALIZING,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .map_err(|_| AppError::Conflict("Multipart session is busy".into()))?;

    let mut uploaded_parts = session
        .parts
        .iter()
        .filter_map(|entry| {
            (entry.value().state.load(Ordering::Acquire) == STANDARD_PART_COMPLETE)
                .then_some(*entry.key())
        })
        .collect::<Vec<_>>();
    uploaded_parts.sort_unstable();
    let present = uploaded_parts.iter().copied().collect::<BTreeSet<_>>();
    let missing_parts = (0..session.manifest.expected_parts)
        .filter(|part| !present.contains(part))
        .collect::<Vec<_>>();
    if !missing_parts.is_empty() || session.in_flight_parts.load(Ordering::Acquire) != 0 {
        session
            .lifecycle
            .store(STANDARD_SESSION_OPEN, Ordering::Release);
        return Ok(FinalizeUploadResult::Incomplete(
            FinalizeIncompleteResponse {
                uploaded_parts,
                missing_parts,
                expected_parts: session.manifest.expected_parts,
            },
        ));
    }

    let finalize_result = async {
        let mut parts = Vec::with_capacity(uploaded_parts.len());
        for part_index in uploaded_parts {
            let slot = session.parts.get(&part_index).ok_or_else(|| {
                AppError::Conflict(format!("Multipart part {part_index} disappeared"))
            })?;
            let part = slot.record.get().cloned().ok_or_else(|| {
                AppError::Conflict(format!("Multipart part {part_index} is incomplete"))
            })?;
            parts.push(part);
        }
        parts.sort_by_key(|part| part.part_index);
        let total_size_bytes = parts.iter().map(|part| part.size_bytes).sum::<u64>();
        if total_size_bytes != session.manifest.total_size_bytes {
            return Err(AppError::Conflict(format!(
                "Multipart byte total is {total_size_bytes}, expected {}",
                session.manifest.total_size_bytes
            )));
        }
        let metadata = ObjectMetadata {
            object_key: session.manifest.object_key.clone(),
            packed_ext_u64: session.manifest.packed_ext_u64,
            fragment: fragment.clone(),
            version_id: version_id.clone(),
            upload_session_id: session_id.clone(),
            owner_uid: fragment.clone(),
            content_type: session.manifest.content_type.clone(),
            total_size_bytes,
            created_at: session.manifest.created_at,
            pool_id: session.manifest.pool_id.clone(),
            pool_generation: session.manifest.pool_generation,
            membership_epoch: session.manifest.membership_epoch,
            data_shards: session.manifest.data_shards,
            parity_shards: session.manifest.parity_shards,
            parts,
            is_delete_marker: false,
            stripe_size_bytes: session.manifest.stripe_size_bytes,
        };
        let started = Instant::now();
        let publication = publish_cluster_object_metadata_after_shard_sync(&metadata).await;
        record_io_latency(
            "cluster.upload.standard_finalize",
            started.elapsed(),
            total_size_bytes,
            publication.is_ok(),
        );
        publication?;
        Ok::<_, AppError>(metadata)
    }
    .await;

    match finalize_result {
        Ok(metadata) => {
            session
                .finalized_metadata
                .set(metadata.clone())
                .map_err(|_| {
                    AppError::InternalServerError(
                        "standard multipart finalized metadata was set twice".into(),
                    )
                })?;
            session
                .last_activity_at
                .store(Utc::now().timestamp(), Ordering::Release);
            session
                .lifecycle
                .store(STANDARD_SESSION_FINALIZED, Ordering::Release);
            Ok(FinalizeUploadResult::Complete(metadata))
        }
        Err(error) => {
            session
                .lifecycle
                .store(STANDARD_SESSION_OPEN, Ordering::Release);
            Err(error)
        }
    }
}

pub fn complete_standard_cluster_upload(fragment: &str, version_id: &str) {
    let key = standard_session_key(fragment, version_id);
    if STANDARD_UPLOADS.get(&key).is_some_and(|session| {
        session.lifecycle.load(Ordering::Acquire) == STANDARD_SESSION_FINALIZED
    }) {
        STANDARD_UPLOADS.remove(&key);
    }
}

pub fn cancel_standard_cluster_upload(
    claims: &Claims,
    fragment: &str,
    version_id: &str,
) -> Result<(), AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    let key = standard_session_key(fragment, version_id);
    let session = STANDARD_UPLOADS
        .get(&key)
        .map(|entry| Arc::clone(entry.value()))
        .ok_or_else(|| AppError::BadRequest("Standard upload session not found".into()))?;
    session
        .lifecycle
        .compare_exchange(
            STANDARD_SESSION_OPEN,
            STANDARD_SESSION_CANCELLED,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .map_err(|_| AppError::Conflict("Multipart session is busy".into()))?;
    if session.in_flight_parts.load(Ordering::Acquire) != 0 {
        session
            .lifecycle
            .store(STANDARD_SESSION_OPEN, Ordering::Release);
        return Err(AppError::Conflict(
            "Multipart session has an in-flight part".into(),
        ));
    }
    STANDARD_UPLOADS.remove(&key);
    Ok(())
}

pub fn active_standard_cluster_upload_versions(fragment: &str) -> BTreeSet<String> {
    prune_standard_cluster_uploads(fragment);
    STANDARD_UPLOADS
        .iter()
        .filter(|entry| entry.value().manifest.fragment == fragment)
        .filter(|entry| {
            matches!(
                entry.value().lifecycle.load(Ordering::Acquire),
                STANDARD_SESSION_OPEN | STANDARD_SESSION_FINALIZING
            )
        })
        .map(|entry| entry.value().manifest.version_id.clone())
        .collect()
}

pub async fn put_cluster_part<S>(
    claims: Claims,
    fragment: String,
    version_id: String,
    part_index: u64,
    query: UploadPartQuery,
    body_stream: S,
    content_length: u64,
) -> Result<(), AppError>
where
    S: Stream<Item = Result<Bytes, AppError>>,
{
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    let state = index::load_session(&fragment, &version_id)
        .await?
        .ok_or_else(|| AppError::BadRequest("Upload session not found".into()))?;
    if state.part_receipts.contains_key(&part_index) || state.parts.contains_key(&part_index) {
        return Ok(());
    }
    if part_index >= state.manifest.expected_parts {
        return Err(AppError::BadRequest("Part index out of range".into()));
    }
    let expected = expected_part_size(&state.manifest, part_index);
    if expected != content_length {
        return Err(AppError::BadRequest(
            "Part size does not match upload session manifest".into(),
        ));
    }
    let started = Instant::now();
    let part = write_cluster_part_from_stream(
        &fragment,
        &state.manifest.object_key,
        &version_id,
        part_index,
        content_length,
        usize::from(state.manifest.data_shards),
        usize::from(state.manifest.parity_shards),
        &state.manifest.pool_id,
        state.manifest.pool_generation,
        state.manifest.membership_epoch,
        state.manifest.stripe_size_bytes,
        body_stream,
        query.checksum.as_deref(),
        false,
    )
    .await;
    record_io_latency(
        "cluster.upload.part",
        started.elapsed(),
        content_length,
        part.is_ok(),
    );
    let part = part?;
    let session_commit_started = Instant::now();
    index::commit_session_part(&fragment, &version_id, &part.part).await?;
    index::commit_session_part_receipt(&fragment, &version_id, &state.manifest, &part.part).await?;
    record_io_latency(
        "cluster.upload.commit_part",
        session_commit_started.elapsed(),
        content_length,
        true,
    );
    Ok(())
}

pub async fn upload_cluster_one_shot<S>(
    claims: Claims,
    fragment: String,
    object_key: String,
    packed_ext_u64: u64,
    content_type: String,
    query: UploadPartQuery,
    body_stream: S,
    content_length: u64,
) -> Result<ObjectMetadata, AppError>
where
    S: Stream<Item = Result<Bytes, AppError>>,
{
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    validate_cluster_one_shot_size(content_length)?;
    // write_cluster_part_from_stream performs the authoritative participant
    // reachability check immediately before opening the exact shard writers.
    // Avoid probing the same fixed server-pool layout twice for one-shot I/O.
    let (runtime, layout) = configured_write_context()?;
    let write_pool = runtime
        .membership
        .newest_writable_pool()
        .expect("write context requires a writable pool");
    let version_id = Ulid::new().to_string();
    let session_id = Ulid::new().to_string();
    let stripe_size = choose_upload_stripe_size(content_length);
    let started = Instant::now();
    let part = write_cluster_part_from_stream(
        &fragment,
        &object_key,
        &version_id,
        0,
        content_length,
        layout.data_shards,
        layout.parity_shards,
        &write_pool.id,
        write_pool.generation,
        runtime.membership.epoch,
        stripe_size,
        body_stream,
        query.checksum.as_deref(),
        false,
    )
    .await;
    record_io_latency(
        "cluster.upload.one_shot",
        started.elapsed(),
        content_length,
        part.is_ok(),
    );
    let part = part?;
    let object_meta = ObjectMetadata {
        object_key: object_key.clone(),
        packed_ext_u64,
        fragment: fragment.clone(),
        version_id: version_id.clone(),
        upload_session_id: session_id,
        owner_uid: fragment.clone(),
        content_type,
        total_size_bytes: content_length,
        created_at: Utc::now().timestamp(),
        pool_id: part.pool_id.clone(),
        pool_generation: part.pool_generation,
        membership_epoch: part.membership_epoch,
        data_shards: part.data_shards,
        parity_shards: part.parity_shards,
        parts: vec![part.part],
        is_delete_marker: false,
        stripe_size_bytes: stripe_size,
    };
    publish_cluster_object_metadata(&object_meta).await?;
    Ok(object_meta)
}

pub async fn finalize_cluster_upload(
    claims: Claims,
    fragment: String,
    version_id: String,
    session_id: String,
    _object_key: String,
    params: InitiateUploadParams,
) -> Result<FinalizeUploadResult, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    let state = index::load_session(&fragment, &version_id)
        .await?
        .ok_or_else(|| AppError::BadRequest("Upload session not found".into()))?;
    if state.manifest.session_id != session_id {
        return Err(AppError::Forbidden("Session id mismatch".into()));
    }
    let mut parts = Vec::new();
    let mut missing = Vec::new();
    for part_index in 0..state.manifest.expected_parts {
        if let Some(part) = state.parts.get(&part_index) {
            parts.push(part.clone());
        } else {
            missing.push(part_index);
        }
    }
    if !missing.is_empty() {
        return Ok(FinalizeUploadResult::Incomplete(
            crate::core::disk_manager::FinalizeIncompleteResponse {
                uploaded_parts: parts.iter().map(|part| part.part_index).collect(),
                missing_parts: missing,
                expected_parts: state.manifest.expected_parts,
            },
        ));
    }
    parts.sort_by_key(|part| part.part_index);
    let total_size_bytes = parts.iter().map(|part| part.size_bytes).sum();
    let object_meta = ObjectMetadata {
        // The immutable upload manifest owns the physical object identity.
        // Using it here makes a finalize retry independent of the temporary
        // logical-path binding once NCDX has already been published.
        object_key: state.manifest.object_key.clone(),
        packed_ext_u64: state.manifest.packed_ext_u64,
        fragment: fragment.clone(),
        version_id: version_id.clone(),
        upload_session_id: session_id,
        owner_uid: fragment.clone(),
        content_type: state.manifest.content_type.clone().if_empty(
            params
                .content_type
                .unwrap_or_else(|| "application/octet-stream".into()),
        ),
        total_size_bytes,
        created_at: state.manifest.created_at,
        pool_id: state.manifest.pool_id.clone(),
        pool_generation: state.manifest.pool_generation,
        membership_epoch: state.manifest.membership_epoch,
        data_shards: state.manifest.data_shards,
        parity_shards: state.manifest.parity_shards,
        parts,
        is_delete_marker: false,
        stripe_size_bytes: state.manifest.stripe_size_bytes,
    };
    let ncdx_started = Instant::now();
    let publication = publish_cluster_object_metadata(&object_meta).await;
    record_io_latency(
        "cluster.upload.publish_ncdx",
        ncdx_started.elapsed(),
        total_size_bytes,
        publication.is_ok(),
    );
    publication?;
    Ok(FinalizeUploadResult::Complete(object_meta))
}

trait EmptyFallback {
    fn if_empty(self, fallback: String) -> String;
}

impl EmptyFallback for String {
    fn if_empty(self, fallback: String) -> String {
        if self.trim().is_empty() {
            fallback
        } else {
            self
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::layout::ClusterLayoutPolicy;
    use crate::distribution::membership::{
        ClusterMembership, ClusterNode, ClusterPolicy, ClusterRuntime,
        N2_MEMBERSHIP_FORMAT_VERSION, NodeState, PoolState, ServerPool, StorageRoot,
    };
    use std::path::PathBuf;

    fn manifest(
        total_size_bytes: u64,
        expected_parts: u64,
        part_size_bytes: u64,
    ) -> UploadSessionManifest {
        UploadSessionManifest {
            fragment: "fragment".into(),
            object_key: "video.bin".into(),
            packed_ext_u64: crate::core::index_store::packed_extension_key("video.bin"),
            version_id: "01TESTVERSION000000000000".into(),
            session_id: "01TESTSESSION000000000000".into(),
            total_size_bytes,
            expected_parts,
            part_size_bytes,
            content_type: "application/octet-stream".into(),
            created_at: 0,
            pool_id: "pool-a".into(),
            pool_generation: 1,
            stripe_size_bytes: 256 * 1024,
            membership_epoch: 0,
            data_shards: 1,
            parity_shards: 1,
        }
    }

    fn standard_test_session() -> Arc<StandardUploadSession> {
        Arc::new(StandardUploadSession {
            manifest: manifest(128, 2, 64),
            logical_path: "videos/video.bin".into(),
            lifecycle: AtomicU8::new(STANDARD_SESSION_OPEN),
            in_flight_parts: AtomicUsize::new(0),
            last_activity_at: AtomicI64::new(0),
            parts: DashMap::new(),
            finalized_metadata: OnceLock::new(),
        })
    }

    #[test]
    fn standard_multipart_claims_distinct_parts_without_a_session_lock() {
        let session = standard_test_session();
        let first = match claim_standard_part(Arc::clone(&session), 0).unwrap() {
            StandardPartClaimResult::Claimed(claim) => claim,
            StandardPartClaimResult::AlreadyComplete => panic!("part was unexpectedly complete"),
        };
        let second = match claim_standard_part(Arc::clone(&session), 1).unwrap() {
            StandardPartClaimResult::Claimed(claim) => claim,
            StandardPartClaimResult::AlreadyComplete => panic!("part was unexpectedly complete"),
        };

        assert_eq!(session.in_flight_parts.load(Ordering::Acquire), 2);
        assert!(claim_standard_part(Arc::clone(&session), 0).is_err());

        drop(first);
        drop(second);
        assert_eq!(session.in_flight_parts.load(Ordering::Acquire), 0);
        assert!(matches!(
            claim_standard_part(session, 0),
            Ok(StandardPartClaimResult::Claimed(_))
        ));
    }

    #[test]
    fn completed_standard_part_retry_is_idempotent() {
        let session = standard_test_session();
        let claim = match claim_standard_part(Arc::clone(&session), 0).unwrap() {
            StandardPartClaimResult::Claimed(claim) => claim,
            StandardPartClaimResult::AlreadyComplete => panic!("part was unexpectedly complete"),
        };
        claim
            .complete(PartMetadata {
                part_index: 0,
                size_bytes: 64,
                created_at: 1,
                shards: Vec::new(),
                checksum: Some("sha256:test".into()),
            })
            .unwrap();

        assert!(matches!(
            claim_standard_part(session, 0),
            Ok(StandardPartClaimResult::AlreadyComplete)
        ));
    }

    #[test]
    fn standard_upload_reuse_requires_the_exact_upload_shape() {
        let session = standard_test_session();
        let exact = InitiateUploadParams {
            total_size_bytes: 128,
            expected_parts: 2,
            part_size_bytes: Some(64),
            content_type: Some("application/octet-stream".into()),
            packed_ext_u64: 0,
        };
        assert!(standard_request_matches(
            &session,
            "videos/video.bin",
            &exact
        ));

        let changed_size = InitiateUploadParams {
            total_size_bytes: 129,
            ..exact.clone()
        };
        assert!(!standard_request_matches(
            &session,
            "videos/video.bin",
            &changed_size
        ));
        assert!(!standard_request_matches(
            &session,
            "videos/other.bin",
            &exact
        ));
    }

    #[test]
    fn finalized_standard_session_is_retained_for_ncnx_retry() {
        let session = standard_test_session();
        session
            .lifecycle
            .store(STANDARD_SESSION_FINALIZED, Ordering::Release);
        session.last_activity_at.store(0, Ordering::Release);
        assert!(!standard_session_is_expired(&session, i64::MAX, 1));
    }

    #[test]
    fn multipart_expected_part_size_uses_manifest_part_size() {
        let m = manifest(1024 * 1024 * 1024, 16, 64 * 1024 * 1024);

        assert_eq!(expected_part_size(&m, 0), 64 * 1024 * 1024);
        assert_eq!(expected_part_size(&m, 14), 64 * 1024 * 1024);
        assert_eq!(expected_part_size(&m, 15), 64 * 1024 * 1024);
    }

    #[test]
    fn multipart_expected_part_size_handles_short_final_part() {
        let m = manifest(130 * 1024 * 1024, 3, 64 * 1024 * 1024);

        assert_eq!(expected_part_size(&m, 0), 64 * 1024 * 1024);
        assert_eq!(expected_part_size(&m, 1), 64 * 1024 * 1024);
        assert_eq!(expected_part_size(&m, 2), 2 * 1024 * 1024);
    }

    #[test]
    fn cluster_one_shot_rejects_large_objects() {
        assert!(validate_cluster_one_shot_size(CLUSTER_ONE_SHOT_MAX_BYTES).is_ok());
        assert!(validate_cluster_one_shot_size(CLUSTER_ONE_SHOT_MAX_BYTES + 1).is_err());
    }

    fn fixed_runtime() -> ClusterRuntime {
        ClusterRuntime {
            local_node_id: "a".into(),
            local_instance_id: "a".into(),
            local_is_reserve: false,
            local_pool_id: "pool-a".into(),
            local_storage_roots: vec![],
            membership_path: PathBuf::from("membership.json"),
            layout_policy: ClusterLayoutPolicy {
                data_shards: 2,
                parity_shards: 1,
            },
            membership: ClusterMembership {
                format_version: N2_MEMBERSHIP_FORMAT_VERSION,
                cluster_id: "test".into(),
                epoch: 7,
                binding_epoch: 1,
                pools: vec![ServerPool {
                    id: "pool-a".into(),
                    generation: 7,
                    policy: ClusterPolicy {
                        object_data_shards: 2,
                        object_parity_shards: 1,
                    },
                    state: PoolState::Writable,
                    node_ids: vec!["a".into(), "b".into(), "c".into()],
                }],
                nodes: ["a", "b", "c"]
                    .into_iter()
                    .map(|id| ClusterNode {
                        id: id.into(),
                        instance_id: id.into(),
                        binding_epoch: 1,
                        endpoint: format!("https://{id}:50001"),
                        native_endpoint: Some(format!("n2://{id}:50002")),
                        state: Some(NodeState::Online),
                        weight: 1,
                        storage_roots: vec![StorageRoot {
                            id: format!("root-{id}"),
                            physical_id: None,
                            path: format!("/disk-{id}"),
                        }],
                        joined_epoch: 1,
                        capacity_bytes: None,
                        used_bytes: None,
                        last_seen_unix: None,
                    })
                    .collect(),
                reserve_nodes: Vec::new(),
                retired_instances: Vec::new(),
            },
            auto_rebalance: false,
        }
    }

    #[test]
    fn fixed_cluster_admits_full_and_k_threshold_survival_writes() {
        let runtime = fixed_runtime();
        let layout = resolve_survival_write_layout(3, 3, &runtime.layout_policy).unwrap();

        assert_eq!(layout.data_shards, 2);
        assert_eq!(layout.parity_shards, 1);
        assert!(validate_write_admission(&runtime, 3, 3, layout).is_ok());
        assert!(validate_write_admission(&runtime, 2, 2, layout).is_ok());
    }

    #[test]
    fn offline_member_remains_part_of_the_static_layout_but_not_live_admission() {
        let mut runtime = fixed_runtime();
        runtime.membership.nodes[2].state = Some(NodeState::Offline);

        let layout = configured_layout(&runtime).unwrap();
        assert_eq!(layout.data_shards, 2);
        assert_eq!(layout.parity_shards, 1);
        assert_eq!(
            runtime.membership.active_newest_pool_nodes().len(),
            layout.data_shards
        );
        assert!(validate_write_admission(&runtime, 2, 2, layout).is_ok());
    }

    #[test]
    fn fixed_cluster_rejects_a_blob_plane_below_k() {
        let runtime = fixed_runtime();
        assert!(resolve_survival_write_layout(2, 3, &runtime.layout_policy).is_ok());
        assert!(resolve_survival_write_layout(1, 3, &runtime.layout_policy).is_err());
    }

    #[test]
    fn survival_admission_rejects_namespace_reachability_below_k() {
        let runtime = fixed_runtime();
        let layout = resolve_survival_write_layout(3, 3, &runtime.layout_policy).unwrap();
        assert!(validate_write_admission(&runtime, 1, 3, layout).is_err());
    }

    #[test]
    fn pinned_one_plus_one_session_survives_node_return() {
        let layout = ClusterEcLayout {
            data_shards: 1,
            parity_shards: 1,
        };

        assert!(validate_pinned_upload_layout(layout, 2).is_ok());
        assert!(validate_pinned_upload_layout(layout, 3).is_ok());
    }

    #[test]
    fn pinned_layout_validates_immutable_pool_width_not_live_reachability() {
        let layout = ClusterEcLayout {
            data_shards: 2,
            parity_shards: 1,
        };

        assert!(validate_pinned_upload_layout(layout, 2).is_err());
        assert!(validate_pinned_upload_layout(layout, 3).is_ok());
    }
}
