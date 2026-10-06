//! Repair-job traversal, batching, pacing, and status orchestration.

use super::object_repair::{
    fragment_is_present, list_all_fragments, process_object_repair, reachable_dir_roots,
    repair_fragment_index_db, repair_fragment_metadata_replicas,
};
use super::*;

#[cfg(test)]
pub async fn run_repair_job(
    cfg: RepairRunConfig,
    ws_manager: Arc<WsConnectionManager>,
) -> Result<(), AppError> {
    let run_opts = RepairRunOptions {
        fast_mode: false,
        access_mode: cfg.check_mode.default_maintenance_access_mode(),
        replay_pending_redistribution: true,
    };
    run_repair_job_with_options(cfg, run_opts, ws_manager).await
}

pub async fn run_repair_job_with_options(
    cfg: RepairRunConfig,
    opts: RepairRunOptions,
    ws_manager: Arc<WsConnectionManager>,
) -> Result<(), AppError> {
    let running_guard = acquire_repair_running()?;
    run_repair_job_reserved(cfg, opts, ws_manager, running_guard).await
}

pub(super) async fn run_repair_job_reserved(
    cfg: RepairRunConfig,
    opts: RepairRunOptions,
    ws_manager: Arc<WsConnectionManager>,
    _running_guard: RepairRunningGuard,
) -> Result<(), AppError> {
    let result = run_repair_job_inner(cfg, opts, ws_manager.clone()).await;
    if let Err(error) = &result {
        let mut status = REPAIR_STATUS.write().await;
        status.is_running = false;
        status.phase = "Failed".to_string();
        status.last_error = Some(error.to_string());
        ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
    }
    result
}

async fn run_repair_job_inner(
    cfg: RepairRunConfig,
    opts: RepairRunOptions,
    ws_manager: Arc<WsConnectionManager>,
) -> Result<(), AppError> {
    STOP_SIGNAL.store(false, Ordering::Relaxed);
    let start_time = Instant::now();
    let effective_access_mode = opts
        .access_mode
        .max(cfg.check_mode.minimum_maintenance_access_mode());
    if effective_access_mode != opts.access_mode {
        info!(
            "Repair mode upgraded maintenance gate from {:?} to {:?} based on check_mode={:?}",
            opts.access_mode, effective_access_mode, cfg.check_mode
        );
    }
    let _write_gate = acquire_maintenance_mode(effective_access_mode);
    let io_guard = acquire_disk_io(
        DiskIoClass::BackgroundRepair,
        cfg.target_fragment.as_deref(),
        "fragment repair job",
    )
    .await?;
    let batch_pacing = availability_pacing_delay(cfg.check_mode, opts.fast_mode);

    {
        let mut status = REPAIR_STATUS.write().await;
        status.is_running = true;
        status.fast_mode = opts.fast_mode;
        status.phase = format!(
            "Initializing (access_mode={:?}, mode={:?}, meta_conc={}, repair_conc={}, batch={}, dry_run={}, fast_mode={})",
            effective_access_mode,
            cfg.check_mode,
            cfg.meta_concurrency,
            cfg.shard_repair_concurrency,
            cfg.meta_batch_size,
            cfg.dry_run,
            opts.fast_mode
        );
        status.total_scanned = 0;
        status.total_repaired = 0;
        status.corrupt_found = 0;
        status.failed_repairs = 0;
        status.missing_meta_repaired = 0;
        status.divergent_meta_repaired = 0;
        status.last_disagreement = None;
        status.current_fragment.clear();
        status.last_repaired = None;
        status.last_error = None;
        status.start_timestamp = Utc::now().timestamp();
        status.duration_seconds = 0;
        status.scan_rate_per_sec = 0.0;

        ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
    }

    let path_manager = ObjectPathManager::new();
    let _index_roots_config = INDEX_STORE_DISKS
        .get()
        .ok_or(AppError::InternalServerError(
            "Index Store Config not initialized".into(),
        ))?;

    let index_roots: Vec<PathBuf> = reachable_dir_roots(path_manager.get_all_index_roots())
        .await
        .into_iter()
        .filter(|root| configured_root_identity_is_current(root))
        .collect();
    let blob_roots: Arc<Vec<PathBuf>> = Arc::new(
        reachable_dir_roots(path_manager.get_blob_roots())
            .await
            .into_iter()
            .filter(|root| configured_root_identity_is_current(root))
            .collect(),
    );
    let num_disks = blob_roots.len();
    if index_roots.is_empty() {
        return Err(AppError::InternalServerError(
            "No reachable index roots".into(),
        ));
    }
    if num_disks == 0 {
        return Err(AppError::InternalServerError(
            "No reachable blob disks".into(),
        ));
    }
    let cfg = normalize_repair_cfg_for_layout(cfg, num_disks, index_roots.len());
    info!(
        "Repair runtime caps after layout normalization: mode={:?} meta_conc={} repair_conc={} batch={} blob_disks={} index_roots={}",
        cfg.check_mode,
        cfg.meta_concurrency,
        cfg.shard_repair_concurrency,
        cfg.meta_batch_size,
        num_disks,
        index_roots.len()
    );

    let shard_repair_sem = Arc::new(Semaphore::new(cfg.shard_repair_concurrency));
    let alias_roots = index_roots.clone();
    let alias_dry_run = cfg.dry_run;
    let alias_report = tokio::task::spawn_blocking(move || {
        index_store::reconcile_fragment_alias_replicas(&alias_roots, alias_dry_run)
    })
    .await
    .map_err(|error| {
        AppError::InternalServerError(format!("fragment alias reconciliation join error: {error}"))
    })??;
    if alias_report.repaired_aliases > 0 || alias_report.missing_aliases > 0 {
        info!(
            "N1 fragment alias reconciliation: discovered={} missing={} repaired={} ambiguous={}",
            alias_report.discovered_fragments,
            alias_report.missing_aliases,
            alias_report.repaired_aliases,
            alias_report.ambiguous_fragments
        );
        let mut status = REPAIR_STATUS.write().await;
        status.missing_meta_repaired = status
            .missing_meta_repaired
            .saturating_add(alias_report.repaired_aliases);
        status.total_repaired = status
            .total_repaired
            .saturating_add(alias_report.repaired_aliases);
        status.last_repaired = (alias_report.repaired_aliases > 0).then(|| {
            format!(
                "{} fragment alias replica(s)",
                alias_report.repaired_aliases
            )
        });
        ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
    }
    let fragments = match cfg.target_fragment.as_ref() {
        Some(fragment) => vec![fragment.clone()],
        None => list_all_fragments(&index_roots).await,
    };

    info!(
        "Starting Storage Repair Job (scope={}, fragments={}, index_roots={}). mode={:?}, dry_run={}",
        if cfg.target_fragment.is_some() {
            "single-fragment"
        } else {
            "all-fragments"
        },
        fragments.len(),
        index_roots.len(),
        cfg.check_mode,
        cfg.dry_run
    );

    let mut unresolved_repairs = 0u64;
    let mut last_unresolved_failure: Option<String> = None;

    for fragment_hash in fragments {
        if should_stop() {
            break;
        }
        io_guard.heartbeat();

        let _lifecycle_guard =
            crate::core::fragment_lifecycle::acquire_shared(&fragment_hash).await?;
        // A fragment can be deleted after enumeration but before this turn.
        // Do not rebuild a deleted fragment from a stale scan snapshot.
        if !fragment_is_present(&index_roots, &fragment_hash).await? {
            continue;
        }

        {
            let mut status = REPAIR_STATUS.write().await;
            status.current_fragment = fragment_hash.clone();
            status.phase = format!("Repairing Fragment {}", fragment_hash);
            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }

        {
            let mut status = REPAIR_STATUS.write().await;
            status.phase = format!("Repairing fragment metadata {}", fragment_hash);
            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }

        let (fmiss, fdiv, fcorrupt) =
            repair_fragment_metadata_replicas(&index_roots, &fragment_hash, cfg.dry_run).await?;

        {
            let mut status = REPAIR_STATUS.write().await;
            status.missing_meta_repaired += fmiss;
            status.divergent_meta_repaired += fdiv;
            status.corrupt_found += fcorrupt;
            status.total_repaired += fmiss + fdiv;
            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }

        {
            let mut status = REPAIR_STATUS.write().await;
            status.phase = format!("Repairing N1 index replicas {}", fragment_hash);
            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }

        let (pmiss, pdiv, pcorrupt) =
            repair_fragment_index_db(&index_roots, &fragment_hash, cfg.dry_run).await?;

        {
            let mut status = REPAIR_STATUS.write().await;
            status.missing_meta_repaired += pmiss;
            status.divergent_meta_repaired += pdiv;
            status.corrupt_found += pcorrupt;
            status.total_repaired += pmiss + pdiv;
            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }

        if !cfg.dry_run && (pmiss > 0 || pdiv > 0 || pcorrupt > 0) {
            global_namespace_runtime().evict_fragment(&fragment_hash);
        }

        {
            let mut status = REPAIR_STATUS.write().await;
            status.phase = format!("Repairing N1 index replicas {}", fragment_hash);
            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }

        let namespace_report =
            repair_namespace_replicas(&fragment_hash, &index_roots, cfg.dry_run).await?;

        {
            let mut status = REPAIR_STATUS.write().await;
            status.missing_meta_repaired += namespace_report.missing_repaired;
            status.divergent_meta_repaired += namespace_report.divergent_repaired;
            status.corrupt_found += namespace_report.corrupt_found;
            status.total_repaired +=
                namespace_report.missing_repaired + namespace_report.divergent_repaired;
            if namespace_report.missing_repaired > 0
                || namespace_report.divergent_repaired > 0
                || namespace_report.corrupt_found > 0
            {
                status.last_repaired = Some(format!("index:{}", fragment_hash));
                status.last_disagreement = namespace_report.selected_source.clone();
            }
            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
        }

        if !cfg.dry_run
            && (namespace_report.missing_repaired > 0
                || namespace_report.divergent_repaired > 0
                || namespace_report.corrupt_found > 0)
        {
            global_namespace_runtime().evict_fragment(&fragment_hash);
        }

        if !cfg.dry_run && opts.replay_pending_redistribution {
            if let Err(err) = replay_pending_intents_for_fragment_now(&fragment_hash, true).await {
                warn!(
                    "Failed replaying pending redistribution intents for fragment {} during repair: {}",
                    fragment_hash, err
                );
            }
        }

        for is_trash in [false, true] {
            if should_stop() {
                break;
            }

            let mut continuation_token = None;
            loop {
                let page = if is_trash {
                    index::scan_fragment_trash_metadata_page(
                        &fragment_hash,
                        None,
                        None,
                        cfg.meta_batch_size,
                        continuation_token.clone(),
                        true,
                    )
                    .await?
                } else {
                    index::scan_fragment_live_metadata_page(
                        &fragment_hash,
                        None,
                        None,
                        cfg.meta_batch_size,
                        continuation_token.clone(),
                        true,
                    )
                    .await?
                };

                if page.objects.is_empty() && page.continuation_token.is_none() {
                    break;
                }

                let d = apply_meta_batch(
                    &page.objects,
                    blob_roots.clone(),
                    cfg.clone(),
                    shard_repair_sem.clone(),
                    batch_pacing,
                    ws_manager.clone(),
                    start_time,
                    if is_trash {
                        MetadataCatalog::Trash
                    } else {
                        MetadataCatalog::Live
                    },
                )
                .await?;
                unresolved_repairs += d.failed_repairs;
                if d.last_failure.is_some() {
                    last_unresolved_failure = d.last_failure.clone();
                }
                io_guard.heartbeat();

                let _fragment_touched = d.repaired_shards > 0
                    || d.missing_meta_repaired > 0
                    || d.divergent_meta_repaired > 0;

                match page.continuation_token {
                    Some(token) => continuation_token = Some(token),
                    None => break,
                }
            }
        }
    }

    ensure_no_unresolved_repairs(unresolved_repairs, last_unresolved_failure.as_deref())?;

    io_guard.complete();
    drop(_write_gate);

    {
        let mut status = REPAIR_STATUS.write().await;
        status.is_running = false;
        status.fast_mode = opts.fast_mode;
        status.phase = if should_stop() {
            "Stopped by User".to_string()
        } else {
            "Completed".to_string()
        };
        status.duration_seconds = start_time.elapsed().as_secs();
        ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));

        info!(
            "Repair Job Ended. Scanned: {}, Repaired: {}, Corrupt: {}, Failed: {}, MissingMeta: {}, DivergentMeta: {}",
            status.total_scanned,
            status.total_repaired,
            status.corrupt_found,
            status.failed_repairs,
            status.missing_meta_repaired,
            status.divergent_meta_repaired
        );
    }

    Ok(())
}

async fn apply_meta_batch(
    batch: &[ObjectMetadata],
    blob_roots: Arc<Vec<PathBuf>>,
    cfg: RepairRunConfig,
    shard_repair_sem: Arc<Semaphore>,
    batch_pacing: Option<Duration>,
    ws_manager: Arc<WsConnectionManager>,
    start_time: Instant,
    catalog: MetadataCatalog,
) -> Result<BatchDelta, AppError> {
    if batch.is_empty() {
        return Ok(BatchDelta::default());
    }

    let mut seen_object_versions: HashSet<String> = HashSet::with_capacity(batch.len());
    let mut deduped_batch: Vec<ObjectMetadata> = Vec::with_capacity(batch.len());
    for meta in batch {
        let dedupe_key = format!(
            "{}\u{001f}{}\u{001f}{}",
            meta.fragment, meta.object_key, meta.version_id
        );
        if !seen_object_versions.insert(dedupe_key) {
            continue;
        }
        deduped_batch.push(meta.clone());
    }

    let mut stream =
        stream::iter(deduped_batch.into_iter())
            .map(|meta| {
                let blob_roots = blob_roots.clone();
                let shard_repair_sem = shard_repair_sem.clone();
                let cfg = cfg.clone();

                async move {
                    process_one_metadata(meta, blob_roots, cfg, shard_repair_sem, catalog).await
                }
            })
            .buffer_unordered(cfg.meta_concurrency);

    let mut batch_agg = BatchDelta::default();
    let mut pending_update = BatchDelta::default();
    let mut last_update = Instant::now();
    let mut processed_since_pause: usize = 0;

    while let Some(res) = stream.next().await {
        let delta = match res {
            Ok(d) => d,
            Err(e) => {
                let message = format!("Object maintenance failed: {e}");
                warn!("{message}");
                batch_agg.failed_repairs += 1;
                batch_agg.last_failure = Some(message.clone());
                pending_update.failed_repairs += 1;
                pending_update.last_failure = Some(message);
                continue;
            }
        };

        batch_agg.scanned += delta.scanned;
        batch_agg.repaired_shards += delta.repaired_shards;
        batch_agg.corrupt_found += delta.corrupt_found;
        batch_agg.missing_meta_repaired += delta.missing_meta_repaired;
        batch_agg.divergent_meta_repaired += delta.divergent_meta_repaired;
        batch_agg.failed_repairs += delta.failed_repairs;
        if delta.last_repaired.is_some() {
            batch_agg.last_repaired = delta.last_repaired.clone();
        }
        if delta.last_disagreement.is_some() {
            batch_agg.last_disagreement = delta.last_disagreement.clone();
        }
        if delta.last_failure.is_some() {
            batch_agg.last_failure = delta.last_failure.clone();
        }

        pending_update.scanned += delta.scanned;
        pending_update.repaired_shards += delta.repaired_shards;
        pending_update.corrupt_found += delta.corrupt_found;
        pending_update.missing_meta_repaired += delta.missing_meta_repaired;
        pending_update.divergent_meta_repaired += delta.divergent_meta_repaired;
        pending_update.failed_repairs += delta.failed_repairs;
        if delta.last_repaired.is_some() {
            pending_update.last_repaired = delta.last_repaired.clone();
        }
        if delta.last_disagreement.is_some() {
            pending_update.last_disagreement = delta.last_disagreement.clone();
        }
        if delta.last_failure.is_some() {
            pending_update.last_failure = delta.last_failure.clone();
        }

        if let Some(delay) = batch_pacing {
            processed_since_pause += 1;
            if processed_since_pause >= 32 {
                processed_since_pause = 0;
                tokio::time::sleep(delay).await;
            }
        }

        let significant_change = delta.repaired_shards > 0
            || delta.missing_meta_repaired > 0
            || delta.divergent_meta_repaired > 0
            || delta.corrupt_found > 0
            || delta.failed_repairs > 0;

        if significant_change
            || pending_update.scanned >= 10
            || last_update.elapsed().as_millis() > 200
        {
            let mut status = REPAIR_STATUS.write().await;
            status.total_scanned += pending_update.scanned;
            status.total_repaired += pending_update.repaired_shards
                + pending_update.missing_meta_repaired
                + pending_update.divergent_meta_repaired;
            status.corrupt_found += pending_update.corrupt_found;
            status.missing_meta_repaired += pending_update.missing_meta_repaired;
            status.divergent_meta_repaired += pending_update.divergent_meta_repaired;
            status.failed_repairs += pending_update.failed_repairs;

            if let Some(s) = &pending_update.last_repaired {
                status.last_repaired = Some(s.clone());
            }
            if let Some(s) = &pending_update.last_disagreement {
                status.last_disagreement = Some(s.clone());
            }
            if let Some(s) = &pending_update.last_failure {
                status.last_error = Some(s.clone());
            }

            let elapsed = start_time.elapsed().as_secs_f64();
            status.duration_seconds = elapsed as u64;
            if elapsed > 0.0 {
                status.scan_rate_per_sec = status.total_scanned as f64 / elapsed;
            }

            ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));

            pending_update = BatchDelta::default();
            last_update = Instant::now();
        }
    }

    if pending_update.scanned > 0 || pending_update.failed_repairs > 0 {
        let mut status = REPAIR_STATUS.write().await;
        status.total_scanned += pending_update.scanned;
        status.total_repaired += pending_update.repaired_shards
            + pending_update.missing_meta_repaired
            + pending_update.divergent_meta_repaired;
        status.corrupt_found += pending_update.corrupt_found;
        status.missing_meta_repaired += pending_update.missing_meta_repaired;
        status.divergent_meta_repaired += pending_update.divergent_meta_repaired;
        status.failed_repairs += pending_update.failed_repairs;

        if let Some(s) = &pending_update.last_repaired {
            status.last_repaired = Some(s.clone());
        }
        if let Some(s) = &pending_update.last_disagreement {
            status.last_disagreement = Some(s.clone());
        }
        if let Some(s) = &pending_update.last_failure {
            status.last_error = Some(s.clone());
        }

        let elapsed = start_time.elapsed().as_secs_f64();
        status.duration_seconds = elapsed as u64;
        if elapsed > 0.0 {
            status.scan_rate_per_sec = status.total_scanned as f64 / elapsed;
        }

        ws_manager.broadcast_system_wide(StorageEventPayload::RepairUpdate(status.clone()));
    }

    Ok(batch_agg)
}

async fn process_one_metadata(
    meta: ObjectMetadata,
    blob_roots: Arc<Vec<PathBuf>>,
    cfg: RepairRunConfig,
    shard_repair_sem: Arc<Semaphore>,
    catalog: MetadataCatalog,
) -> Result<BatchDelta, AppError> {
    let mut delta = BatchDelta::default();
    if should_stop() {
        return Ok(delta);
    }

    delta.scanned = 1;

    if meta.is_delete_marker {
        return Ok(delta);
    }

    let (repaired, corrupt, metadata_repaired, failed_repairs, last_rep, last_failure) =
        process_object_repair(meta, &blob_roots, cfg, shard_repair_sem, catalog).await?;

    delta.repaired_shards += repaired as u64;
    delta.corrupt_found += corrupt as u64;
    delta.missing_meta_repaired += metadata_repaired as u64;
    delta.failed_repairs += failed_repairs as u64;
    delta.last_repaired = last_rep;
    delta.last_failure = last_failure;

    Ok(delta)
}
