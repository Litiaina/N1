use crate::core::disk_manager::ObjectMetadata;
use crate::core::repair_manager::{ObjectRepairStatus, RepairCheckMode};
use crate::distribution::download::{
    VerifiedShardCache, reconstruct_cluster_shard_cached, shard_probe_readable,
    shard_probe_verified,
};
use crate::distribution::membership::ClusterRuntime;
use crate::distribution::peer::{PeerShardTarget, parse_peer_shard_locator, put_peer_shard_bytes};
use crate::distribution::status::inspect_cluster_object_status;
use crate::enums::app_error_enum::AppError;
use serde::{Deserialize, Serialize};
use tracing::warn;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterRepairPolicy {
    pub object_repair_scope: String,
    pub fragment_repair_scope: String,
    pub byte_payload_consensus: bool,
    pub namespace_consensus_required: bool,
}

pub fn repair_policy(_runtime: &ClusterRuntime) -> ClusterRepairPolicy {
    ClusterRepairPolicy {
        object_repair_scope: "object placement record and missing shard/parity holders".to_string(),
        fragment_repair_scope: "namespace ledger, object records, and local node storage health"
            .to_string(),
        byte_payload_consensus: false,
        namespace_consensus_required: true,
    }
}

pub async fn repair_cluster_object_shards(
    meta: &ObjectMetadata,
    check_mode: RepairCheckMode,
) -> Result<ObjectRepairStatus, AppError> {
    let before = inspect_cluster_object_status(meta, check_mode).await?;
    if before.status == "healthy" || !before.repairable {
        return Ok(before);
    }

    let mut repaired_shards = 0u64;
    let mut failed_repairs = 0u64;

    for part in &meta.parts {
        let mut cache = VerifiedShardCache::default();
        for shard in &part.shards {
            let healthy = if check_mode.verifies_full_integrity() {
                shard_probe_verified(shard).await
            } else {
                shard_probe_readable(shard).await
            };
            if healthy {
                continue;
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
                node_id: locator.node_id,
                endpoint: locator.endpoint,
                fragment: locator.fragment,
                object_id: locator.object_id,
                version_id: locator.version_id,
                part_index: locator.part_index,
                shard_index: locator.shard_index,
                is_parity: locator.is_parity,
                expected_len: shard.size_bytes,
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
    after.repaired_shards = repaired_shards;
    after.failed_repairs = failed_repairs;
    Ok(after)
}
