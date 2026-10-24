use crate::core::disk_manager::ObjectMetadata;
use crate::core::repair_manager::{ObjectRepairStatus, RepairCheckMode};
use crate::distribution::download::{
    ShardProbeResult, VerifiedShardCache, reconstruct_cluster_shard_cached, shard_probe,
};
use crate::distribution::membership::ClusterRuntime;
use crate::distribution::peer::{PeerShardTarget, parse_peer_shard_locator, put_peer_shard_bytes};
use crate::distribution::status::inspect_cluster_object_status;
use crate::enums::app_error_enum::AppError;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex};
use tokio::time::{Duration, sleep};
use tracing::warn;

const POST_REPAIR_VERIFY_ATTEMPTS: usize = 3;
const POST_REPAIR_VERIFY_DELAY: Duration = Duration::from_millis(25);

// Admission never blocks GET and does not create an unbounded repair backlog.
const MAX_CLUSTER_READ_REPAIR_PARTS: usize = 8;

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
        return None;
    }
    if inflight.len() >= MAX_CLUSTER_READ_REPAIR_PARTS {
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
            if let Err(err) = repair_cluster_part_after_read(&part, &targets).await {
                warn!(
                    "Cluster read repair failed for part {}: {}",
                    part.part_index, err
                );
            }
        }
    });
}

async fn repair_cluster_part_after_read(
    part: &crate::core::disk_manager::PartMetadata,
    failed_shards: &HashSet<usize>,
) -> Result<(), AppError> {
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
    let Some(locator) = parse_peer_shard_locator(&shard.absolute_path) else {
        return Ok(());
    };
    if locator.part_index != part.part_index
        || locator.shard_index != shard.shard_index
        || locator.is_parity != shard.is_parity
    {
        return Err(AppError::InternalServerError(
            "read repair target differs from recorded shard layout".into(),
        ));
    }
    let reconstructed =
        reconstruct_cluster_shard_cached(part, shard.shard_index, block_size, cache).await?;
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
    let repaired = put_peer_shard_bytes(target, &reconstructed).await?;
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

    let mut repaired_shards = 0u64;
    let mut failed_repairs = 0u64;

    for part in &meta.parts {
        let mut cache = VerifiedShardCache::for_repair();
        for shard in &part.shards {
            match shard_probe(shard, check_mode.verifies_full_integrity()).await {
                ShardProbeResult::Healthy | ShardProbeResult::Unavailable => continue,
                ShardProbeResult::Missing | ShardProbeResult::ChecksumMismatch => {}
            }

            let Some(locator) = parse_peer_shard_locator(&shard.absolute_path) else {
                failed_repairs = failed_repairs.saturating_add(1);
                warn!(
                    "Cluster object repair cannot target non-peer shard locator for {}/{} shard {}",
                    meta.fragment, meta.object_key, shard.shard_index
                );
                continue;
            };

            let reconstructed = match reconstruct_cluster_shard_cached(
                part,
                shard.shard_index,
                meta.stripe_size_bytes,
                &mut cache,
            )
            .await
            {
                Ok(bytes) => bytes,
                Err(err) => {
                    failed_repairs = failed_repairs.saturating_add(1);
                    warn!(
                        "Cluster object repair failed reconstructing {}/{} shard {}: {}",
                        meta.fragment, meta.object_key, shard.shard_index, err
                    );
                    continue;
                }
            };

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

            match put_peer_shard_bytes(target, &reconstructed).await {
                Ok(info) if info.checksum.eq_ignore_ascii_case(&shard.checksum) => {
                    repaired_shards = repaired_shards.saturating_add(1);
                }
                Ok(info) => {
                    failed_repairs = failed_repairs.saturating_add(1);
                    warn!(
                        "Cluster object repair wrote {}/{} shard {} but checksum differed: got {}, expected {}",
                        meta.fragment,
                        meta.object_key,
                        shard.shard_index,
                        info.checksum,
                        shard.checksum
                    );
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
