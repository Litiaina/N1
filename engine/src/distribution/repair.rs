use crate::core::disk_manager::{ObjectMetadata, PartMetadata, ShardInfo};
use crate::core::repair_manager::{ObjectRepairStatus, RepairCheckMode};
use crate::distribution::download::{
    ShardProbeResult, VerifiedShardCache, reconstruct_cluster_shard_into, shard_probe,
};
use crate::distribution::membership::ClusterRuntime;
use crate::distribution::peer::{PeerShardTarget, RemoteShardWriter, parse_peer_shard_locator};
use crate::distribution::status::inspect_cluster_object_status;
use crate::enums::app_error_enum::AppError;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Duration, sleep};
use tracing::warn;

const POST_REPAIR_VERIFY_ATTEMPTS: usize = 3;
const POST_REPAIR_VERIFY_DELAY: Duration = Duration::from_millis(25);

// Admission never blocks GET and does not create an unbounded repair backlog.
const MAX_CLUSTER_READ_REPAIR_PARTS: usize = 8;
// Read-triggered, explicit and autonomous repair share admission. Otherwise
// each entry point's independent task limit can multiply the working set.
const MAX_CLUSTER_REPAIR_WORKERS: usize = 8;
static CLUSTER_REPAIR_WORKERS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(MAX_CLUSTER_REPAIR_WORKERS)));
static READ_REPAIR_ADMITTED: AtomicU64 = AtomicU64::new(0);
static READ_REPAIR_MERGED: AtomicU64 = AtomicU64::new(0);
static READ_REPAIR_DEFERRED: AtomicU64 = AtomicU64::new(0);
static READ_REPAIR_COMPLETED: AtomicU64 = AtomicU64::new(0);
static READ_REPAIR_FAILED: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Serialize)]
pub struct ReadRepairSnapshot {
    pub admitted_parts: u64,
    pub merged_hints: u64,
    pub capacity_deferred: u64,
    pub completed_attempts: u64,
    pub failed_attempts: u64,
    pub active_parts: usize,
    pub active_streaming_workers: usize,
    pub streaming_worker_limit: usize,
}

pub fn read_repair_snapshot() -> ReadRepairSnapshot {
    ReadRepairSnapshot {
        admitted_parts: READ_REPAIR_ADMITTED.load(Ordering::Relaxed),
        merged_hints: READ_REPAIR_MERGED.load(Ordering::Relaxed),
        capacity_deferred: READ_REPAIR_DEFERRED.load(Ordering::Relaxed),
        completed_attempts: READ_REPAIR_COMPLETED.load(Ordering::Relaxed),
        failed_attempts: READ_REPAIR_FAILED.load(Ordering::Relaxed),
        active_parts: CLUSTER_READ_REPAIR_INFLIGHT
            .lock()
            .map(|parts| parts.len())
            .unwrap_or(0),
        active_streaming_workers: MAX_CLUSTER_REPAIR_WORKERS
            .saturating_sub(CLUSTER_REPAIR_WORKERS.available_permits()),
        streaming_worker_limit: MAX_CLUSTER_REPAIR_WORKERS,
    }
}

fn try_repair_worker(gate: &Arc<Semaphore>) -> Result<OwnedSemaphorePermit, AppError> {
    gate.clone().try_acquire_owned().map_err(|_| {
        AppError::ServiceUnavailable("cluster streaming repair is at capacity; retry later".into())
    })
}

#[derive(Default)]
struct PendingReadRepair {
    owner: Arc<()>,
    targets: HashSet<usize>,
}

static CLUSTER_READ_REPAIR_INFLIGHT: LazyLock<Mutex<HashMap<String, PendingReadRepair>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

struct ClusterReadRepairGuard {
    key: String,
    owner: Arc<()>,
}

impl Drop for ClusterReadRepairGuard {
    fn drop(&mut self) {
        if let Ok(mut inflight) = CLUSTER_READ_REPAIR_INFLIGHT.lock() {
            // A drained worker can finish after a new job for the same part
            // was admitted. Never remove that new worker's pending targets.
            remove_owned_read_repair(&mut inflight, &self.key, &self.owner);
        }
    }
}

fn remove_owned_read_repair(
    inflight: &mut HashMap<String, PendingReadRepair>,
    key: &str,
    owner: &Arc<()>,
) {
    if inflight
        .get(key)
        .is_some_and(|pending| Arc::ptr_eq(&pending.owner, owner))
    {
        inflight.remove(key);
    }
}

fn enqueue_read_repair_targets(
    inflight: &mut HashMap<String, PendingReadRepair>,
    key: &str,
    targets: HashSet<usize>,
) -> Option<Arc<()>> {
    if let Some(pending) = inflight.get_mut(key) {
        pending.targets.extend(targets);
        READ_REPAIR_MERGED.fetch_add(1, Ordering::Relaxed);
        return None;
    }
    if inflight.len() >= MAX_CLUSTER_READ_REPAIR_PARTS {
        READ_REPAIR_DEFERRED.fetch_add(1, Ordering::Relaxed);
        warn!("Cluster read repair is at capacity; a later verified read can retry this part");
        return None;
    }
    let owner = Arc::new(());
    inflight.insert(
        key.to_string(),
        PendingReadRepair {
            owner: owner.clone(),
            targets,
        },
    );
    READ_REPAIR_ADMITTED.fetch_add(1, Ordering::Relaxed);
    Some(owner)
}

fn take_read_repair_targets(
    inflight: &mut HashMap<String, PendingReadRepair>,
    key: &str,
    owner: &Arc<()>,
) -> Option<HashSet<usize>> {
    let pending = inflight.get_mut(key)?;
    if !Arc::ptr_eq(&pending.owner, owner) {
        return None;
    }
    if pending.targets.is_empty() {
        inflight.remove(key);
        return None;
    }
    Some(std::mem::take(&mut pending.targets))
}

fn enqueue_cluster_read_repair(
    part: &crate::core::disk_manager::PartMetadata,
    failed_shards: impl IntoIterator<Item = usize>,
) -> Option<ClusterReadRepairGuard> {
    let first = part.shards.first()?;
    let key = format!(
        "{}\0{}\0{}",
        part.part_index, first.absolute_path, first.checksum
    );
    let targets = failed_shards
        .into_iter()
        .filter(|index| part.shards.iter().any(|shard| shard.shard_index == *index))
        .collect::<HashSet<_>>();
    if targets.is_empty() {
        return None;
    }
    let mut inflight = CLUSTER_READ_REPAIR_INFLIGHT.lock().ok()?;
    let owner = enqueue_read_repair_targets(&mut inflight, &key, targets)?;
    Some(ClusterReadRepairGuard { key, owner })
}

impl ClusterReadRepairGuard {
    fn take_targets(&self) -> Option<HashSet<usize>> {
        let mut inflight = CLUSTER_READ_REPAIR_INFLIGHT.lock().ok()?;
        take_read_repair_targets(&mut inflight, &self.key, &self.owner)
    }
}

pub(crate) fn schedule_cluster_part_read_repair(
    part: crate::core::disk_manager::PartMetadata,
    failed_shards: impl IntoIterator<Item = usize>,
) {
    let Some(guard) = enqueue_cluster_read_repair(&part, failed_shards) else {
        return;
    };
    tokio::spawn(async move {
        while let Some(targets) = guard.take_targets() {
            match repair_cluster_part_after_read(&part, &targets).await {
                Ok(()) => {
                    READ_REPAIR_COMPLETED.fetch_add(1, Ordering::Relaxed);
                }
                Err(err) => {
                    READ_REPAIR_FAILED.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        "Cluster read repair failed for part {}: {}",
                        part.part_index, err
                    );
                }
            }
        }
    });
}

async fn repair_cluster_part_after_read(
    part: &crate::core::disk_manager::PartMetadata,
    failed_shards: &HashSet<usize>,
) -> Result<(), AppError> {
    let _worker = try_repair_worker(&CLUSTER_REPAIR_WORKERS).inspect_err(|_| {
        READ_REPAIR_DEFERRED.fetch_add(1, Ordering::Relaxed);
    })?;
    let block_size = part
        .shards
        .iter()
        .map(|shard| shard.checksum_block_size)
        .find(|size| *size > 0)
        .unwrap_or(256 * 1024);
    let mut cache = VerifiedShardCache::for_repair();
    let mut first_error = None;
    for shard in part
        .shards
        .iter()
        .filter(|shard| failed_shards.contains(&shard.shard_index))
    {
        if let Err(error) = repair_cluster_read_target(part, shard, block_size, &mut cache).await {
            warn!(
                "Cluster read repair failed for shard {}: {}",
                shard.shard_index, error
            );
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

async fn repair_cluster_read_target(
    part: &crate::core::disk_manager::PartMetadata,
    shard: &crate::core::disk_manager::ShardInfo,
    block_size: usize,
    cache: &mut VerifiedShardCache,
) -> Result<(), AppError> {
    // A readable byte is not evidence that the shard is healthy. Recheck
    // the exact failed holder's SHA, and never replace an unavailable
    // holder merely because GET observed a transient transport error.
    match shard_probe(shard, true).await {
        ShardProbeResult::Healthy | ShardProbeResult::Unavailable => return Ok(()),
        ShardProbeResult::Missing | ShardProbeResult::ChecksumMismatch => {}
    }
    stream_repaired_peer_shard(part, shard, block_size, cache).await
}

async fn stream_repaired_peer_shard(
    part: &PartMetadata,
    shard: &ShardInfo,
    block_size: usize,
    cache: &mut VerifiedShardCache,
) -> Result<(), AppError> {
    let locator = parse_peer_shard_locator(&shard.absolute_path).ok_or_else(|| {
        AppError::ServiceUnavailable("cluster repair requires a recorded peer shard locator".into())
    })?;
    if locator.part_index != part.part_index
        || locator.shard_index != shard.shard_index
        || locator.is_parity != shard.is_parity
    {
        return Err(AppError::InternalServerError(
            "read repair target differs from recorded shard layout".into(),
        ));
    }
    let target = PeerShardTarget {
        pool_id: locator.pool_id,
        pool_generation: locator.pool_generation,
        node_id: locator.node_id,
        instance_id: locator.instance_id,
        binding_epoch: locator.binding_epoch,
        root_id: locator.root_id,
        endpoint: locator.endpoint,
        fragment: locator.fragment,
        object_id: locator.object_id,
        version_id: locator.version_id,
        part_index: locator.part_index,
        shard_index: locator.shard_index,
        is_parity: locator.is_parity,
        expected_len: shard.size_bytes,
        deferred_durability: false,
        repair: true,
        expected_checksum: Some(shard.checksum.clone()),
    };
    let mut writer = RemoteShardWriter::spawn(target)?;
    reconstruct_cluster_shard_into(part, shard.shard_index, block_size, cache, &mut writer).await?;
    let repaired = writer.finish().await?;
    if !repaired.checksum.eq_ignore_ascii_case(&shard.checksum) {
        return Err(AppError::InternalServerError(format!(
            "read-repaired shard {} checksum differed: got {}, expected {}",
            shard.shard_index, repaired.checksum, shard.checksum
        )));
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterRepairPolicy {
    pub object_repair_scope: String,
    pub fragment_repair_scope: String,
    pub byte_payload_consensus: bool,
    pub strict_ncnx_replica_repair: bool,
}

pub fn repair_policy(_runtime: &ClusterRuntime) -> ClusterRepairPolicy {
    ClusterRepairPolicy {
        object_repair_scope: "object placement record and missing shard/parity holders".to_string(),
        fragment_repair_scope:
            "flat NCNX pointers, immutable object records, and server-pool storage health"
                .to_string(),
        byte_payload_consensus: false,
        strict_ncnx_replica_repair: true,
    }
}

pub async fn repair_cluster_object_shards(
    meta: &ObjectMetadata,
    check_mode: RepairCheckMode,
) -> Result<ObjectRepairStatus, AppError> {
    let before = inspect_cluster_object_status(meta, check_mode).await?;
    if before.status == "healthy" || before.status == "incomplete" || !before.repairable {
        return Ok(before);
    }

    let _worker = try_repair_worker(&CLUSTER_REPAIR_WORKERS)?;

    let mut repaired_shards = 0u64;
    let mut failed_repairs = 0u64;

    for part in &meta.parts {
        let mut cache = VerifiedShardCache::for_repair();
        for shard in &part.shards {
            match shard_probe(shard, check_mode.verifies_full_integrity()).await {
                ShardProbeResult::Healthy | ShardProbeResult::Unavailable => continue,
                ShardProbeResult::Missing | ShardProbeResult::ChecksumMismatch => {}
            }

            match stream_repaired_peer_shard(part, shard, meta.stripe_size_bytes, &mut cache).await
            {
                Ok(()) => {
                    repaired_shards = repaired_shards.saturating_add(1);
                }
                Err(err) => {
                    failed_repairs = failed_repairs.saturating_add(1);
                    warn!(
                        "Cluster object repair failed writing {}/{} shard {}: {}",
                        meta.fragment, meta.object_key, shard.shard_index, err
                    );
                }
            }
        }
    }

    let mut after = inspect_cluster_object_status(meta, check_mode).await?;
    if repaired_shards > 0 && failed_repairs == 0 {
        for _ in 1..POST_REPAIR_VERIFY_ATTEMPTS {
            if after.status == "healthy" {
                break;
            }
            sleep(POST_REPAIR_VERIFY_DELAY).await;
            after = inspect_cluster_object_status(meta, check_mode).await?;
        }
    }
    after.repaired_shards = repaired_shards;
    after.failed_repairs = failed_repairs;
    Ok(after)
}

#[cfg(test)]
#[path = "repair/tests.rs"]
mod tests;
