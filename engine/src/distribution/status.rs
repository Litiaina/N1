use crate::core::disk_manager::ObjectMetadata;
use crate::core::repair_manager::{ObjectRepairStatus, RepairCheckMode, inspect_object_status};
use crate::distribution::download::{ShardProbeResult, part_has_remote_shards, shard_probe};
use crate::distribution::membership::active_cluster_runtime;
use crate::distribution::rebalance::object_layout_status;
use crate::enums::app_error_enum::AppError;
use futures_util::stream::{FuturesUnordered, StreamExt};

fn attach_layout_status(
    mut report: ObjectRepairStatus,
    meta: &ObjectMetadata,
) -> Result<ObjectRepairStatus, AppError> {
    let runtime = active_cluster_runtime()?;
    report.layout = Some(object_layout_status(meta, &runtime)?);
    Ok(report)
}

pub async fn inspect_cluster_object_status(
    meta: &ObjectMetadata,
    check_mode: RepairCheckMode,
) -> Result<ObjectRepairStatus, AppError> {
    if meta.parts.is_empty() {
        return attach_layout_status(
            ObjectRepairStatus {
                object_key: meta.object_key.clone(),
                version_id: meta.version_id.clone(),
                status: if meta.total_size_bytes == 0 {
                    "healthy".to_string()
                } else {
                    "lost".to_string()
                },
                total_parts: meta.parts.len() as u64,
                total_shards: 0,
                healthy_shards: 0,
                missing_shards: 0,
                corrupt_shards: 0,
                unavailable_shards: 0,
                repairable: meta.total_size_bytes == 0,
                repaired_shards: 0,
                failed_repairs: 0,
                layout: None,
            },
            meta,
        );
    }

    if !meta.parts.iter().any(part_has_remote_shards) {
        let report = inspect_object_status(meta, check_mode).await?;
        return attach_layout_status(report, meta);
    }

    let mut report = ObjectRepairStatus {
        object_key: meta.object_key.clone(),
        version_id: meta.version_id.clone(),
        status: "healthy".to_string(),
        total_parts: meta.parts.len() as u64,
        total_shards: 0,
        healthy_shards: 0,
        missing_shards: 0,
        corrupt_shards: 0,
        unavailable_shards: 0,
        repairable: true,
        repaired_shards: 0,
        failed_repairs: 0,
        layout: None,
    };

    let mut any_degraded = false;
    let mut any_critical = false;
    let mut any_lost = false;
    let mut any_unavailable = false;

    for part in &meta.parts {
        let data_shards = part
            .shards
            .iter()
            .filter(|shard| !shard.is_parity)
            .count()
            .max(1);
        let mut healthy_in_part = 0usize;
        let mut unavailable_in_part = 0usize;

        let mut probes = FuturesUnordered::new();
        for shard in &part.shards {
            probes.push(
                async move { shard_probe(shard, check_mode.verifies_full_integrity()).await },
            );
        }

        while let Some(probe) = probes.next().await {
            report.total_shards += 1;
            match probe {
                ShardProbeResult::Healthy => {
                    report.healthy_shards += 1;
                    healthy_in_part += 1;
                }
                ShardProbeResult::Missing => report.missing_shards += 1,
                ShardProbeResult::ChecksumMismatch => report.corrupt_shards += 1,
                ShardProbeResult::Unavailable => {
                    report.unavailable_shards += 1;
                    unavailable_in_part += 1;
                    any_unavailable = true;
                }
            }
        }

        if healthy_in_part + unavailable_in_part < part.shards.len() {
            any_degraded = true;
        }
        if unavailable_in_part == 0 && healthy_in_part == 0 {
            any_lost = true;
        } else if unavailable_in_part == 0 && healthy_in_part < data_shards {
            any_critical = true;
        }
        if unavailable_in_part == 0 && healthy_in_part < data_shards {
            report.repairable = false;
        }
    }

    if any_unavailable {
        report.repairable = false;
    }
    report.status = if any_unavailable {
        "incomplete".to_string()
    } else if any_lost {
        "lost".to_string()
    } else if any_critical {
        "critical".to_string()
    } else if any_degraded {
        "degraded".to_string()
    } else {
        "healthy".to_string()
    };

    attach_layout_status(report, meta)
}
