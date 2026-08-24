use super::*;

pub(super) async fn inspect_object_parts(
    meta: &ObjectMetadata,
    check_mode: RepairCheckMode,
) -> Result<(ObjectRepairStatus, Vec<PartMetadata>, Vec<Vec<usize>>), AppError> {
    let path_manager = ObjectPathManager::new();
    let blob_roots = reachable_dir_roots(path_manager.get_storage_roots()).await;
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

    let mut normalized_parts = Vec::with_capacity(meta.parts.len());
    let mut repair_targets = Vec::with_capacity(meta.parts.len());
    let mut any_degraded = false;
    let mut any_critical = false;
    let mut any_lost = false;
    let mut any_unavailable = false;

    for part in &meta.parts {
        let normalized = normalize_part_paths_for_repair(
            &path_manager,
            &blob_roots,
            &meta.fragment,
            &meta.object_key,
            &meta.version_id,
            part.clone(),
        )
        .await;
        let data_shards = normalized
            .shards
            .iter()
            .filter(|shard| !shard.is_parity)
            .count()
            .max(1);
        let mut healthy_in_part = 0usize;
        let mut unavailable_in_part = 0usize;
        let mut bad_indexes = Vec::new();

        for shard in &normalized.shards {
            report.total_shards += 1;
            if shard_is_remote(shard) {
                match shard_probe(shard, check_mode.verifies_full_integrity()).await {
                    ShardProbeResult::Healthy => {
                        report.healthy_shards += 1;
                        healthy_in_part += 1;
                    }
                    ShardProbeResult::Missing => {
                        report.missing_shards += 1;
                        bad_indexes.push(shard.shard_index);
                    }
                    ShardProbeResult::ChecksumMismatch => {
                        report.corrupt_shards += 1;
                        bad_indexes.push(shard.shard_index);
                    }
                    ShardProbeResult::Unavailable => {
                        report.unavailable_shards += 1;
                        unavailable_in_part += 1;
                        any_unavailable = true;
                    }
                }
                continue;
            }
            let shard_path = PathBuf::from(strip_tmp_attempt_suffix(&shard.absolute_path));
            match fs::metadata(&shard_path).await {
                Ok(metadata) => {
                    if metadata.len() != shard.size_bytes {
                        report.corrupt_shards += 1;
                        bad_indexes.push(shard.shard_index);
                    } else if check_mode.verifies_full_integrity() {
                        match File::open(&shard_path).await {
                            Ok(mut file) => {
                                if verify_shard_integrity(&mut file, &shard.checksum).await {
                                    report.healthy_shards += 1;
                                    healthy_in_part += 1;
                                } else {
                                    report.corrupt_shards += 1;
                                    bad_indexes.push(shard.shard_index);
                                }
                            }
                            Err(error) => {
                                if error.kind() == std::io::ErrorKind::NotFound {
                                    report.missing_shards += 1;
                                    bad_indexes.push(shard.shard_index);
                                } else {
                                    report.unavailable_shards += 1;
                                    unavailable_in_part += 1;
                                    any_unavailable = true;
                                }
                            }
                        }
                    } else {
                        report.healthy_shards += 1;
                        healthy_in_part += 1;
                    }
                }
                Err(error) => {
                    if error.kind() == std::io::ErrorKind::NotFound {
                        report.missing_shards += 1;
                        bad_indexes.push(shard.shard_index);
                    } else {
                        report.unavailable_shards += 1;
                        unavailable_in_part += 1;
                        any_unavailable = true;
                    }
                }
            }
        }

        if !bad_indexes.is_empty() {
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

        normalized_parts.push(normalized);
        repair_targets.push(bad_indexes);
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

    Ok((report, normalized_parts, repair_targets))
}

pub async fn inspect_object_status(
    meta: &ObjectMetadata,
    check_mode: RepairCheckMode,
) -> Result<ObjectRepairStatus, AppError> {
    let _permit = acquire_object_maintenance_permit(
        meta,
        ObjectMaintenanceKind::Status,
        matches!(check_mode, RepairCheckMode::Scrub),
    )
    .await?;
    let _io = try_acquire_disk_io(DiskIoClass::ForegroundRead, Some(&meta.fragment))?;
    inspect_object_parts(meta, check_mode)
        .await
        .map(|(report, _, _)| report)
}

pub async fn repair_object(
    meta: &ObjectMetadata,
    check_mode: RepairCheckMode,
) -> Result<ObjectRepairStatus, AppError> {
    let _permit =
        acquire_object_maintenance_permit(meta, ObjectMaintenanceKind::Repair, true).await?;
    let _io = try_acquire_disk_io(DiskIoClass::BackgroundRepair, Some(&meta.fragment))?;
    let (mut report, normalized_parts, repair_targets) =
        inspect_object_parts(meta, check_mode).await?;
    if report.status == "healthy" || report.status == "incomplete" || !report.repairable {
        return Ok(report);
    }

    for (part, bad_indexes) in normalized_parts.into_iter().zip(repair_targets.into_iter()) {
        let part = Arc::new(part);
        for missing_idx in bad_indexes {
            match repair_entire_shard(part.clone(), missing_idx, meta.stripe_size_bytes).await {
                Ok(()) => report.repaired_shards += 1,
                Err(err) => {
                    report.failed_repairs += 1;
                    warn!(
                        "Object-level repair failed for {}/{} shard {}: {}",
                        meta.fragment, meta.object_key, missing_idx, err
                    );
                }
            }
        }
    }

    let (mut after, _, _) = inspect_object_parts(meta, check_mode).await?;
    after.repaired_shards = report.repaired_shards;
    after.failed_repairs = report.failed_repairs;
    Ok(after)
}

/// Repairs a distributed object in-place against the immutable placement
/// recorded in its metadata. This deliberately does not select the current
/// cluster layout or publish a new object version; that is redistribution.
pub async fn repair_cluster_object_recorded_layout(
    meta: &ObjectMetadata,
    check_mode: RepairCheckMode,
) -> Result<ObjectRepairStatus, AppError> {
    let _permit =
        acquire_object_maintenance_permit(meta, ObjectMaintenanceKind::Repair, true).await?;
    let _io = try_acquire_disk_io(DiskIoClass::BackgroundRepair, Some(&meta.fragment))?;
    repair_cluster_object_shards(meta, check_mode).await
}
