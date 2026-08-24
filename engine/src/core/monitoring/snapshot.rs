use super::*;

#[derive(Clone, Copy)]
pub(super) struct DiskCapacitySnapshot {
    pub(super) total_bytes: u64,
    pub(super) used_bytes: u64,
    pub(super) available_bytes: u64,
    pub(super) usage_percent: f64,
}

#[cfg(unix)]
pub(super) fn disk_capacity_snapshot(path: &std::path::Path) -> Option<DiskCapacitySnapshot> {
    let raw_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let rc = unsafe { libc::statvfs(raw_path.as_ptr(), stats.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }

    let stats = unsafe { stats.assume_init() };
    let block_size = u128::from(if stats.f_frsize > 0 {
        stats.f_frsize
    } else {
        stats.f_bsize
    });
    if block_size == 0 {
        return None;
    }

    let total_bytes = u128::from(stats.f_blocks).saturating_mul(block_size);
    let free_bytes = u128::from(stats.f_bfree).saturating_mul(block_size);
    let available_bytes = u128::from(stats.f_bavail).saturating_mul(block_size);
    let used_bytes = total_bytes.saturating_sub(free_bytes);
    let usage_percent = if total_bytes == 0 {
        0.0
    } else {
        (used_bytes as f64 / total_bytes as f64) * 100.0
    };

    Some(DiskCapacitySnapshot {
        total_bytes: total_bytes.min(u128::from(u64::MAX)) as u64,
        used_bytes: used_bytes.min(u128::from(u64::MAX)) as u64,
        available_bytes: available_bytes.min(u128::from(u64::MAX)) as u64,
        usage_percent,
    })
}

#[cfg(not(unix))]
pub(super) fn disk_capacity_snapshot(_path: &std::path::Path) -> Option<DiskCapacitySnapshot> {
    None
}

async fn collect_system_health(
    state: &Arc<AppState>,
    params: &HashMap<String, String>,
) -> Result<SystemHealthResponse, AppError> {
    let target_fragment = match params
        .get("fragment")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        Some(fragment) => Some(resolve_stable_fragment_id(&fragment).await?),
        None => None,
    };
    let system_metrics = get_system_metrics().await;
    let redist_status = get_redist_status().await;
    let repair_status = get_repair_status().await;
    let gc_status = get_gc_status().await;

    let storage_disks_config = STORAGE_DISKS.get().ok_or(AppError::InternalServerError(
        "Storage config missing".into(),
    ))?;

    let mut check_futures = Vec::new();

    for disk in storage_disks_config {
        check_futures.push(check_disk_accessibility(
            disk.clone(),
            "storage".to_string(),
        ));
    }

    let disk_results = join_all(check_futures).await;

    let mut online_storage_count = 0;
    let mut active_storage_roots = Vec::new();
    let mut disk_statuses = Vec::new();

    for stat in disk_results {
        if stat.status == "online" || stat.status == "slow" {
            online_storage_count += 1;
            active_storage_roots.push(PathBuf::from(&stat.path));
        }
        disk_statuses.push(stat);
    }

    let deep_scan = params
        .get("deep_scan")
        .map(|v| v == "true")
        .unwrap_or(false);
    let force_refresh = params.get("refresh").map(|v| v == "true").unwrap_or(false);
    let mut object_record_counts = Vec::new();
    let mut object_record_map = HashMap::new();
    let mut scan_performed = false;
    let mut scan_busy = false;

    if deep_scan && !active_storage_roots.is_empty() {
        if let Ok(_permit) = DEEP_SCAN_SEMAPHORE.try_acquire() {
            scan_performed = true;
            let roots_clone = active_storage_roots.clone();
            let target_fragment_for_scan = target_fragment.clone();
            let (tx, rx) = oneshot::channel();

            std::thread::spawn(move || {
                if let Some(core_ids) = core_affinity::get_core_ids() {
                    if let Some(last_core) = core_ids.last() {
                        let _ = core_affinity::set_for_current(*last_core);
                    }
                }

                let mut counts = Vec::new();
                for root in roots_clone {
                    let mut current_disk_count = 0u64;
                    let scan_root = target_fragment_for_scan
                        .as_ref()
                        .map(|fragment| fragment_root_on(&root, fragment))
                        .unwrap_or_else(|| root.clone());
                    let walker = WalkDir::new(&scan_root).into_iter();

                    for entry in walker.filter_map(|e| e.ok()) {
                        let path = entry.path();
                        if entry.file_type().is_file() && should_count_index_record(path) {
                            current_disk_count += 1;
                        }

                        if current_disk_count % 1000 == 0 {
                            std::thread::yield_now();
                        }
                    }

                    counts.push(current_disk_count);
                }

                let _ = tx.send(counts);
            });

            object_record_counts = rx.await.map_err(|e| {
                AppError::InternalServerError(format!("Consistency check thread failed: {}", e))
            })?;

            for (i, root) in active_storage_roots.iter().enumerate() {
                if let Some(count) = object_record_counts.get(i) {
                    object_record_map.insert(root.to_string_lossy().to_string(), *count);
                }
            }
        } else {
            scan_busy = true;
        }
    }

    let total_object_records = object_record_counts.iter().copied().sum::<u64>();

    let object_record_health = if scan_busy {
        "skipped (scan in progress)".to_string()
    } else if !scan_performed {
        "skipped (use ?deep_scan=true)".to_string()
    } else if object_record_counts.is_empty() {
        "empty".to_string()
    } else {
        "records-scanned".to_string()
    };

    let total_storage = storage_disks_config.len();
    let uploads =
        collect_upload_runtime_stats(&active_storage_roots, target_fragment.as_deref()).await?;
    let namespace_runtime = global_namespace_runtime().runtime_stats();
    let redistribution_debt = load_redistribution_intent_debt(
        &active_storage_roots,
        force_refresh || deep_scan,
        target_fragment.clone(),
    )
    .await;
    let fragment_redundancy_debt = detect_fragment_redundancy_debt(
        &active_storage_roots,
        &active_storage_roots,
        deep_scan,
        target_fragment.as_deref(),
    )
    .await?;
    let namespace_replica_debt = detect_namespace_replica_debt(
        &active_storage_roots,
        &active_storage_roots,
        deep_scan,
        target_fragment.as_deref(),
    )
    .await?;
    let storage = collect_storage_plane_stats(
        &disk_statuses,
        total_storage as u64,
        online_storage_count as u64,
    );
    let placement_limiter_low = storage
        .placement_limiter_available_bytes
        .map(|bytes| bytes <= 8 * 1024 * 1024 * 1024)
        .unwrap_or(false);
    let placement_limiter_summary = storage
        .placement_limiter_path
        .as_ref()
        .zip(storage.placement_limiter_available_bytes)
        .map(|(path, available)| {
            format!(
                "Placement limited by {} ({} free).",
                path,
                human_bytes_for_display(available)
            )
        });

    let (status, mut summary) = if online_storage_count == 0 {
        (
            "critical",
            "All configured storage devices are offline. System cannot function.".to_string(),
        )
    } else if online_storage_count < total_storage {
        (
            "degraded",
            format!(
                "Running on {}/{} storage devices. Redundancy and capacity are reduced.",
                online_storage_count, total_storage
            ),
        )
    } else if !fragment_redundancy_debt
        .sampled_degraded_fragments
        .is_empty()
    {
        (
            "degraded",
            format!(
                "Fragments have missing or out-of-layout shard copies and need repair/redistribution: {}.",
                fragment_redundancy_debt
                    .sampled_degraded_fragments
                    .join(", ")
            ),
        )
    } else if namespace_replica_debt.fragments_critical > 0 {
        (
            "critical",
            format!(
                "N2 index has no valid replica for {} fragment(s): {}.",
                namespace_replica_debt.fragments_critical,
                namespace_replica_debt.degraded_fragments.join(", ")
            ),
        )
    } else if namespace_replica_debt.fragments_degraded > 0 {
        (
            "degraded",
            format!(
                "N2 NCNX replicas need repair for {} fragment(s): {}.",
                namespace_replica_debt.fragments_degraded,
                namespace_replica_debt.degraded_fragments.join(", ")
            ),
        )
    } else if placement_limiter_low {
        (
            "degraded",
            placement_limiter_summary.clone().unwrap_or_else(|| {
                "Storage placement is capacity-limited; uploads may fail before aggregate capacity is exhausted.".to_string()
            }),
        )
    } else {
        ("healthy", "All systems operational.".to_string())
    };

    if status == "healthy" {
        if !fragment_redundancy_debt
            .pending_gc_blob_fragments
            .is_empty()
        {
            summary.push_str(&format!(
                " Headless blob payload is pending GC for fragment(s): {}.",
                fragment_redundancy_debt
                    .pending_gc_blob_fragments
                    .join(", ")
            ));
        }
        if storage.heterogeneous_storage_capacity {
            if let Some(ratio) = storage.storage_capacity_imbalance_ratio {
                let note = if let Some(limit) = placement_limiter_summary.as_ref() {
                    format!(" Storage devices are size-imbalanced ({ratio:.2}x). {limit}")
                } else {
                    format!(" Storage devices are size-imbalanced ({ratio:.2}x).")
                };
                summary.push_str(&note);
            }
        } else if let Some(limit) = placement_limiter_summary.as_ref() {
            summary.push(' ');
            summary.push_str(limit);
        }
    }

    let monitoring_update = StorageEventPayload::MonitoringUpdate {
        system: serde_json::to_value(&system_metrics).unwrap_or(serde_json::Value::Null),
        uploads: serde_json::to_value(&uploads).ok(),
        namespace_runtime: serde_json::to_value(&namespace_runtime).ok(),
        maintenance: serde_json::to_value(MaintenanceRuntimeStats {
            access_mode: maintenance_access_mode_label(current_maintenance_access_mode()).into(),
            repair_running: repair_status.is_running,
            repair_phase: repair_status.phase.clone(),
            redistribution_running: redist_status.is_running,
            redistribution_phase: redist_status.phase.clone(),
            gc_running: gc_status.is_running,
            gc_phase: gc_status.phase.clone(),
            pending_redistribution_intents: redistribution_debt.pending_intents,
            failed_redistribution_intents: redistribution_debt.failed_intents,
            fragments_with_pending_redistribution: redistribution_debt
                .fragments_with_pending_intents,
            oldest_pending_redistribution_intent_age_seconds: redistribution_debt
                .oldest_pending_intent_age_seconds,
        })
        .ok(),
        repair_job: serde_json::to_value(&repair_status).ok(),
        redist_job: serde_json::to_value(&redist_status).ok(),
        object_record_status: object_record_health.clone(),
        namespace_healthy: online_storage_count == total_storage,
        sampled_at: Some(Utc::now().timestamp()),
        summary: Some(summary.clone()),
    };
    state.ws_manager.broadcast_system_wide(monitoring_update);

    Ok(SystemHealthResponse {
        sampled_at: Utc::now().timestamp(),
        status: status.to_string(),
        disks: disk_statuses,
        consistency: ConsistencyStats {
            total_object_records_scanned: total_object_records,
            object_record_health,
            namespace_replicas: namespace_replica_debt,
            object_record_counts: if !object_record_map.is_empty() {
                Some(object_record_map)
            } else {
                None
            },
        },
        system: system_metrics,
        uploads,
        io_scheduler: disk_io_scheduler_snapshot(),
        io_control: io_control_snapshot(),
        object_record_runtime: ObjectRecordRuntimeStats {
            scanned_object_records: total_object_records,
            shard_roots_online: online_storage_count as u64,
        },
        namespace_runtime,
        storage,
        maintenance: MaintenanceRuntimeStats {
            access_mode: maintenance_access_mode_label(current_maintenance_access_mode()).into(),
            repair_running: repair_status.is_running,
            repair_phase: repair_status.phase,
            redistribution_running: redist_status.is_running,
            redistribution_phase: redist_status.phase,
            gc_running: gc_status.is_running,
            gc_phase: gc_status.phase,
            pending_redistribution_intents: redistribution_debt.pending_intents,
            failed_redistribution_intents: redistribution_debt.failed_intents,
            fragments_with_pending_redistribution: redistribution_debt
                .fragments_with_pending_intents,
            oldest_pending_redistribution_intent_age_seconds: redistribution_debt
                .oldest_pending_intent_age_seconds,
        },
        summary,
    })
}

async fn load_redistribution_intent_debt(
    active_index_roots: &[PathBuf],
    force_refresh: bool,
    target_fragment: Option<String>,
) -> RedistributionIntentDebtSnapshot {
    if active_index_roots.is_empty() {
        return RedistributionIntentDebtSnapshot::default();
    }

    if target_fragment.is_none() && !force_refresh {
        let cached = REDIST_INTENT_DEBT_CACHE.read().await.clone();
        if let Some(cached) = cached {
            if cached.collected_at.elapsed() < REDIST_INTENT_DEBT_REFRESH_INTERVAL {
                return cached.snapshot;
            }
        }
    }

    let snapshot =
        collect_redistribution_intent_debt(active_index_roots, target_fragment.clone()).await;
    if target_fragment.is_none() {
        *REDIST_INTENT_DEBT_CACHE.write().await = Some(CachedRedistributionIntentDebt {
            collected_at: Instant::now(),
            snapshot: snapshot.clone(),
        });
    }
    snapshot
}

async fn collect_upload_runtime_stats(
    active_index_roots: &[PathBuf],
    target_fragment: Option<&str>,
) -> Result<UploadRuntimeStats, AppError> {
    if active_index_roots.is_empty() {
        return Ok(UploadRuntimeStats::default());
    }

    let roots = active_index_roots.to_vec();
    let target_fragment_owned = target_fragment.map(str::to_string);
    let fragments = tokio::task::spawn_blocking(move || -> Vec<String> {
        let mut seen = HashSet::new();
        if let Some(fragment) = target_fragment_owned {
            if roots
                .iter()
                .any(|root| fragment_root_on(root, &fragment).is_dir())
            {
                seen.insert(fragment);
            }
            return seen.into_iter().collect();
        }
        for root in roots {
            if !root.exists() {
                continue;
            }
            for name in list_fragment_aliases(&root) {
                if is_fragment_hash(&name) {
                    seen.insert(stable_fragment_id_for_root(&root, &name).unwrap_or(name));
                }
            }
        }
        let mut out: Vec<String> = seen.into_iter().collect();
        out.sort();
        out
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("upload stats join error: {}", e)))?;

    let now = Utc::now().timestamp();
    let stale_after = CONFIG
        .garbage_collector
        .file_ttl_seconds
        .min(i64::MAX as u64) as i64;
    let mut stats = UploadRuntimeStats::default();
    let mut oldest_created_at: Option<i64> = None;
    let mut oldest_stale_created_at: Option<i64> = None;

    for fragment in fragments {
        let sessions = index::list_active_sessions(&fragment).await?;
        let mut fragment_has_active = false;

        for session in sessions
            .into_iter()
            .filter(|state| state.finalized_at.is_none() && state.aborted_at.is_none())
        {
            let session_created_at = session.manifest.created_at;
            fragment_has_active = true;
            stats.active_sessions = stats.active_sessions.saturating_add(1);
            let reserved_bytes = session
                .reservation
                .map(|receipt| receipt.amount)
                .unwrap_or(0);
            stats.reserved_bytes = stats.reserved_bytes.saturating_add(reserved_bytes);

            if !session.part_receipts.is_empty() {
                stats.committed_parts = stats
                    .committed_parts
                    .saturating_add(session.part_receipts.len() as u64);
                stats.committed_bytes = stats.committed_bytes.saturating_add(
                    session
                        .part_receipts
                        .values()
                        .map(|receipt| receipt.size_bytes)
                        .sum::<u64>(),
                );
            } else {
                stats.committed_parts = stats
                    .committed_parts
                    .saturating_add(session.parts.len() as u64);
                stats.committed_bytes = stats.committed_bytes.saturating_add(
                    session
                        .parts
                        .values()
                        .map(|part| part.size_bytes)
                        .sum::<u64>(),
                );
            }

            if stale_after > 0 && now.saturating_sub(session.last_activity_at) >= stale_after {
                stats.stale_sessions = stats.stale_sessions.saturating_add(1);
                stats.stale_reserved_bytes =
                    stats.stale_reserved_bytes.saturating_add(reserved_bytes);
                oldest_stale_created_at = Some(match oldest_stale_created_at {
                    Some(current) => current.min(session_created_at),
                    None => session_created_at,
                });
            }

            oldest_created_at = Some(match oldest_created_at {
                Some(current) => current.min(session_created_at),
                None => session_created_at,
            });
        }

        if fragment_has_active {
            stats.active_fragments = stats.active_fragments.saturating_add(1);
        }
    }

    stats.oldest_session_age_seconds = oldest_created_at.map(|created_at| {
        if now > created_at {
            (now - created_at) as u64
        } else {
            0
        }
    });
    stats.oldest_stale_session_age_seconds = oldest_stale_created_at.map(|created_at| {
        if now > created_at {
            (now - created_at) as u64
        } else {
            0
        }
    });

    Ok(stats)
}

fn maintenance_access_mode_label(mode: MaintenanceAccessMode) -> &'static str {
    match mode {
        MaintenanceAccessMode::Availability => "availability",
        MaintenanceAccessMode::ReadOnly => "read_only",
        MaintenanceAccessMode::Quiesced => "quiesced",
    }
}

fn collect_storage_plane_stats(
    disk_statuses: &[DiskStatus],
    storage_devices_total: u64,
    storage_devices_online: u64,
) -> StoragePlaneStats {
    let mut raw_total_bytes = 0u64;
    let mut raw_used_bytes = 0u64;
    let mut raw_available_bytes = 0u64;

    for disk in disk_statuses.iter().filter(|disk| disk.role == "storage") {
        raw_total_bytes = raw_total_bytes.saturating_add(disk.total_bytes.unwrap_or(0));
        raw_used_bytes = raw_used_bytes.saturating_add(disk.used_bytes.unwrap_or(0));
        raw_available_bytes = raw_available_bytes.saturating_add(disk.available_bytes.unwrap_or(0));
    }

    let blob_risk = analyze_storage_placement_risk(disk_statuses);

    StoragePlaneStats {
        storage_devices_total,
        storage_devices_online,
        raw_total_bytes,
        raw_used_bytes,
        raw_available_bytes,
        heterogeneous_storage_capacity: blob_risk.heterogeneous_storage_capacity,
        storage_capacity_imbalance_ratio: blob_risk.storage_capacity_imbalance_ratio,
        placement_limiter_path: blob_risk.placement_limiter_path,
        placement_limiter_available_bytes: blob_risk.placement_limiter_available_bytes,
        placement_limiter_usage_percent: blob_risk.placement_limiter_usage_percent,
    }
}

fn analyze_storage_placement_risk(disk_statuses: &[DiskStatus]) -> StoragePlacementRisk {
    let storage_disks: Vec<&DiskStatus> = disk_statuses
        .iter()
        .filter(|disk| disk.role == "storage" && disk.total_bytes.unwrap_or(0) > 0)
        .collect();

    if storage_disks.is_empty() {
        return StoragePlacementRisk {
            heterogeneous_storage_capacity: false,
            storage_capacity_imbalance_ratio: None,
            placement_limiter_path: None,
            placement_limiter_available_bytes: None,
            placement_limiter_usage_percent: None,
        };
    }

    let smallest_total = storage_disks
        .iter()
        .filter_map(|disk| disk.total_bytes)
        .min()
        .unwrap_or(0);
    let largest_total = storage_disks
        .iter()
        .filter_map(|disk| disk.total_bytes)
        .max()
        .unwrap_or(0);
    let storage_capacity_imbalance_ratio = if smallest_total > 0 && largest_total >= smallest_total
    {
        Some(largest_total as f64 / smallest_total as f64)
    } else {
        None
    };
    let heterogeneous_storage_capacity = storage_capacity_imbalance_ratio
        .map(|ratio| ratio >= 1.5)
        .unwrap_or(false);

    let limiter = storage_disks
        .iter()
        .filter_map(|disk| {
            disk.available_bytes
                .map(|available| (available, disk.path.clone(), disk.usage_percent))
        })
        .min_by_key(|(available, _, _)| *available);

    StoragePlacementRisk {
        heterogeneous_storage_capacity,
        storage_capacity_imbalance_ratio,
        placement_limiter_path: limiter.as_ref().map(|(_, path, _)| path.clone()),
        placement_limiter_available_bytes: limiter.as_ref().map(|(available, _, _)| *available),
        placement_limiter_usage_percent: limiter.and_then(|(_, _, usage)| usage),
    }
}

pub async fn run_system_monitor(state: Arc<AppState>) {
    let mut ticker = tokio::time::interval(monitor_interval());
    ticker.tick().await;

    loop {
        ticker.tick().await;

        let do_deep_scan = {
            let last = LAST_DEEP_SCAN_AT.read().await;
            match *last {
                Some(t) => t.elapsed() >= deep_scan_interval(),
                None => true,
            }
        };

        let mut params = HashMap::new();
        if do_deep_scan {
            params.insert("deep_scan".to_string(), "true".to_string());
        }

        match collect_system_health(&state, &params).await {
            Ok(snapshot) => {
                *HEALTH_CACHE.write().await = Some(snapshot);
                if do_deep_scan {
                    *LAST_DEEP_SCAN_AT.write().await = Some(Instant::now());
                }
            }
            Err(_) => {
                // Keep serving the last good snapshot if probe fails.
            }
        }
    }
}

pub async fn get_system_health(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    Ok(Json(system_health_snapshot(state, claims, params).await?))
}

pub async fn system_health_snapshot(
    state: Arc<AppState>,
    claims: Claims,
    params: HashMap<String, String>,
) -> Result<SystemHealthResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }

    let deep_scan = params
        .get("deep_scan")
        .map(|v| v == "true")
        .unwrap_or(false);
    let refresh = params.get("refresh").map(|v| v == "true").unwrap_or(false);

    if deep_scan || refresh {
        let snapshot = collect_system_health(&state, &params).await?;
        *HEALTH_CACHE.write().await = Some(snapshot.clone());
        if deep_scan {
            *LAST_DEEP_SCAN_AT.write().await = Some(Instant::now());
        }
        return Ok(snapshot);
    }

    if let Some(cached) = HEALTH_CACHE.read().await.clone() {
        return Ok(cached);
    }

    let snapshot = collect_system_health(&state, &params).await?;
    *HEALTH_CACHE.write().await = Some(snapshot.clone());
    Ok(snapshot)
}
