//! Object-level redistribution planning, execution, publication, and cleanup.

use super::intent::{
    dedupe_id_from_metadata, fragment_has_blob_payload, fragment_has_indexed_payload,
    get_all_intent_paths_custom, get_shard_dir_custom, list_all_fragments, reachable_roots,
    remove_intent_paths_best_effort, replay_pending_intents_for_fragment,
    stage_old_shard_gc_with_symlink, write_intent_state,
};
use super::*;

struct LiveLayout {
    blob_cfg: Vec<PathBuf>,
    index_cfg: Vec<PathBuf>,
    live_blob: Arc<Vec<PathBuf>>,
    live_index: Arc<Vec<PathBuf>>,
    last_refresh: Instant,
}

impl LiveLayout {
    async fn new(blob_cfg: Vec<PathBuf>, index_cfg: Vec<PathBuf>) -> Self {
        let live_blob = Arc::new(reachable_roots(&blob_cfg, true).await);
        let live_index = Arc::new(reachable_roots(&index_cfg, true).await);
        Self {
            blob_cfg,
            index_cfg,
            live_blob,
            live_index,
            last_refresh: Instant::now(),
        }
    }

    async fn refresh_if_needed(&mut self) {
        if self.last_refresh.elapsed() < Duration::from_secs(2) {
            return;
        }
        self.live_blob = Arc::new(reachable_roots(&self.blob_cfg, true).await);
        self.live_index = Arc::new(reachable_roots(&self.index_cfg, true).await);
        self.last_refresh = Instant::now();
    }
}

fn rename_exchange(a: &Path, b: &Path) -> std::io::Result<()> {
    linux_io::rename_exchange_blocking(a, b)
}

async fn publish_tmp_overwrite(
    tmp: &Path,
    final_p: &Path,
    parent: &Path,
    file_name: &str,
    sync_parent_immediately: bool,
) -> Result<(), AppError> {
    let final_exists = tokio_fs::try_exists(final_p).await.unwrap_or(false);

    if !final_exists {
        linux_io::rename(tmp, final_p).await?;
        if sync_parent_immediately {
            let _ = sync_dir(parent).await;
        }
        return Ok(());
    }

    {
        let a = tmp.to_path_buf();
        let b = final_p.to_path_buf();

        let ex = tokio::task::spawn_blocking(move || rename_exchange(&a, &b))
            .await
            .map_err(|e| AppError::InternalServerError(format!("rename_exchange join: {}", e)))?;

        if ex.is_ok() {
            let gc_name = format!(
                "{}.preexisting.{}{}",
                file_name,
                Ulid::new().to_string(),
                GC_SUFFIX
            );
            let gc_path = parent.join(gc_name);
            let _ = linux_io::rename(tmp, &gc_path).await;
            if sync_parent_immediately {
                let _ = sync_dir(parent).await;
            }
            return Ok(());
        }
    }

    let gc_name = format!(
        "{}.preexisting.{}{}",
        file_name,
        Ulid::new().to_string(),
        GC_SUFFIX
    );
    let gc_path = parent.join(gc_name);

    linux_io::rename(final_p, &gc_path).await?;

    linux_io::rename(tmp, final_p).await?;

    if sync_parent_immediately {
        let _ = sync_dir(parent).await;
    }
    Ok(())
}

async fn redistribute_target_object(
    fragment: &str,
    object_id: &str,
    version_id: Option<&str>,
    dry_run: bool,
    target_data_shards: usize,
    target_parity_shards: usize,
    target_stripe_size: usize,
    live_blob: &[PathBuf],
    live_index: &[PathBuf],
    strict_durability: bool,
) -> Result<bool, AppError> {
    let mut meta = match version_id {
        Some(version_id) => index::read_object_version(fragment, object_id, version_id).await?,
        None => index::read_latest_object(fragment, object_id).await?,
    };
    let needs = check_needs_redistribution(
        &meta,
        RedistributionScope::Object,
        target_data_shards,
        target_parity_shards,
        target_stripe_size,
        live_blob.len(),
        live_blob,
        live_index,
        false,
    )
    .await;
    if !needs.object() || dry_run {
        return Ok(needs.object());
    }

    let intent_paths =
        get_all_intent_paths_custom(live_index, fragment, object_id, &meta.version_id, false);
    let now = Utc::now().timestamp_millis();
    let mut intent = RedistributionIntent {
        tx_id: Ulid::new().to_string(),
        fragment: fragment.to_string(),
        object_key: object_id.to_string(),
        version_id: meta.version_id.clone(),
        is_trash: false,
        scope: RedistributionScope::Object.as_str().to_string(),
        phase: "running".to_string(),
        created_at_unix_ms: now,
        updated_at_unix_ms: now,
    };
    write_intent_state(&intent_paths, &intent, strict_durability).await?;

    let result = if needs.payload {
        redistribute_object_streaming(
            &mut meta,
            target_data_shards,
            target_parity_shards,
            target_stripe_size,
            live_blob,
            false,
            strict_durability,
        )
        .await
    } else {
        restore_colocated_metadata(&meta, false).await
    };

    match result {
        Ok(()) => {
            remove_intent_paths_best_effort(&intent_paths).await;
            Ok(true)
        }
        Err(err) => {
            intent.phase = "failed".to_string();
            intent.updated_at_unix_ms = Utc::now().timestamp_millis();
            let _ = write_intent_state(&intent_paths, &intent, strict_durability).await;
            Err(err)
        }
    }
}

#[cfg(test)]
pub async fn run_redistribution_job(
    dry_run: bool,
    ws_manager: Arc<WsConnectionManager>,
) -> Result<(), AppError> {
    run_redistribution_job_with_options(dry_run, RedistributionRunOptions::default(), ws_manager)
        .await
}

pub async fn run_redistribution_job_with_options(
    dry_run: bool,
    opts: RedistributionRunOptions,
    ws_manager: Arc<WsConnectionManager>,
) -> Result<(), AppError> {
    let running_guard = acquire_redistribution_running()?;
    run_redistribution_job_reserved(dry_run, opts, ws_manager, running_guard).await
}

pub(super) async fn run_redistribution_job_reserved(
    dry_run: bool,
    opts: RedistributionRunOptions,
    ws_manager: Arc<WsConnectionManager>,
    _running_guard: RedistributionRunningGuard,
) -> Result<(), AppError> {
    STOP_SIGNAL.store(false, Ordering::Relaxed);
    let start_time = Instant::now();
    let _write_gate = acquire_maintenance_mode(opts.execution_mode.maintenance_access_mode());
    let io_guard = acquire_disk_io(
        DiskIoClass::BackgroundRedistribution,
        opts.target_fragment.as_deref(),
        "redistribution job",
    )
    .await?;
    let strict_durability = opts.strict_durability();
    let effective_durability = opts.effective_durability_mode();
    let pacing_delay = redistribution_pacing_delay(opts.execution_mode, opts.fast_mode);

    {
        let mut st = REDIST_STATUS.write().await;
        *st = RedistributionJobStatus {
            is_running: true,
            phase: format!(
                "Initializing (mode={}, scope={}, fast_mode={}, durability={}=>{}, object_concurrency={}, status_batch={}, dry_run={})",
                opts.execution_mode.as_str(),
                opts.scope.as_str(),
                opts.fast_mode,
                opts.durability_mode.as_str(),
                effective_durability,
                opts.object_concurrency,
                opts.status_batch,
                dry_run
            ),
            fast_mode: opts.fast_mode,
            durability_mode: effective_durability.to_string(),
            scope: opts.scope.as_str().to_string(),
            start_timestamp: Utc::now().timestamp(),
            ..Default::default()
        };
        ws_manager.broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));
    }

    let target_data_shards = DATA_SHARDS_ATOMIC
        .get()
        .ok_or(AppError::InternalServerError("DATA_SHARDS missing".into()))?
        .load(Ordering::Relaxed) as usize;

    let target_parity_shards = PARITY_SHARDS_ATOMIC
        .get()
        .ok_or(AppError::InternalServerError(
            "PARITY_SHARDS missing".into(),
        ))?
        .load(Ordering::Relaxed) as usize;

    let target_stripe_size = STRIPE_SIZE_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or_else(default_stripe_size);

    let pm = ObjectPathManager::new();
    let blob_roots_cfg = pm.get_blob_roots();

    let index_roots_cfg: Vec<PathBuf> = INDEX_STORE_DISKS
        .get()
        .ok_or(AppError::InternalServerError(
            "Index Store Config not initialized".into(),
        ))?
        .iter()
        .map(PathBuf::from)
        .collect();

    let mut layout = LiveLayout::new(blob_roots_cfg.clone(), index_roots_cfg.clone()).await;

    if opts.scope != RedistributionScope::Namespace && layout.live_blob.is_empty() {
        return Err(AppError::InternalServerError(
            "No reachable blob disks".into(),
        ));
    }
    if layout.live_index.is_empty() {
        return Err(AppError::InternalServerError(
            "No reachable index roots".into(),
        ));
    }
    let opts = normalize_redistribution_opts_for_layout(
        opts,
        layout.live_blob.len(),
        layout.live_index.len(),
    );

    info!(
        "Starting Redistribution Job (Active & Trash). mode={} scope={} target={}D+{}P stripe={} bytes blob_cfg={} index_cfg={} dry_run={} fast_mode={} durability={}=>{} object_concurrency={} status_batch={}",
        opts.execution_mode.as_str(),
        opts.scope.as_str(),
        target_data_shards,
        target_parity_shards,
        target_stripe_size,
        blob_roots_cfg.len(),
        index_roots_cfg.len(),
        dry_run,
        opts.fast_mode,
        opts.durability_mode.as_str(),
        effective_durability,
        opts.object_concurrency,
        opts.status_batch
    );
    info!(
        "Redistribution runtime caps after layout normalization: mode={} object_concurrency={} live_blob_disks={} live_index_roots={}",
        opts.execution_mode.as_str(),
        opts.object_concurrency,
        layout.live_blob.len(),
        layout.live_index.len()
    );

    let mut touched_fragments: HashSet<String> = HashSet::new();
    let mut scanned_fragments: HashSet<String> = HashSet::new();
    let mut total_task_errors: u64 = 0;
    let mut last_task_error: Option<String> = None;
    let fragments = match opts.target_fragment.as_ref() {
        Some(fragment) => vec![fragment.clone()],
        None => list_all_fragments(layout.live_index.as_ref()).await,
    };

    {
        let mut st = REDIST_STATUS.write().await;
        st.phase = format!(
            "Preparing (fragments={}, scope={})",
            fragments.len(),
            opts.scope.as_str()
        );
        ws_manager.broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));
    }

    let object_concurrency = opts.object_concurrency.max(1);
    let status_batch = opts.status_batch.max(1);

    for fragment_hash in fragments {
        if should_stop() {
            break;
        }
        io_guard.heartbeat();

        layout.refresh_if_needed().await;
        let live_blob = layout.live_blob.clone();
        let live_index = layout.live_index.clone();

        if live_index.is_empty()
            || (opts.scope != RedistributionScope::Namespace && live_blob.is_empty())
        {
            continue;
        }

        if !dry_run {
            if let Err(err) = replay_pending_intents_for_fragment(
                &fragment_hash,
                live_index.as_ref(),
                live_blob.as_ref(),
                target_data_shards,
                target_parity_shards,
                target_stripe_size,
                strict_durability,
            )
            .await
            {
                warn!(
                    "Failed replaying pending redistribution intents for fragment {} before scan: {}",
                    fragment_hash, err
                );
            }
        }

        {
            let mut st = REDIST_STATUS.write().await;
            st.current_fragment = fragment_hash.clone();
            st.total_fragments_scanned += 1;
            st.phase = format!("Scanning Fragment {}", fragment_hash);
            ws_manager.broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));
        }
        scanned_fragments.insert(fragment_hash.clone());

        if matches!(
            opts.scope,
            RedistributionScope::All | RedistributionScope::Namespace
        ) {
            let inspection = index_store::inspect_fragment_partition_replicas(
                &fragment_hash,
                live_index.as_ref(),
            );
            if !inspection.healthy {
                {
                    let mut st = REDIST_STATUS.write().await;
                    st.total_objects_need_redistribution =
                        st.total_objects_need_redistribution.saturating_add(1);
                    st.last_object = Some(format!("{fragment_hash}:namespace"));
                }
                if !dry_run {
                    index::sync_fragment_partition_replicas(&fragment_hash).await?;
                    touched_fragments.insert(fragment_hash.clone());
                    let mut st = REDIST_STATUS.write().await;
                    st.total_objects_migrated = st.total_objects_migrated.saturating_add(1);
                }
            }
        }
        if opts.scope == RedistributionScope::Namespace {
            continue;
        }

        if let Some(object_id) = opts.target_object.as_deref() {
            {
                let mut st = REDIST_STATUS.write().await;
                st.total_objects_scanned = st.total_objects_scanned.saturating_add(1);
                st.last_object = Some(match opts.target_version.as_deref() {
                    Some(version) => format!("{fragment_hash}/{object_id} (v:{version})"),
                    None => format!("{fragment_hash}/{object_id} (head)"),
                });
            }
            let changed = redistribute_target_object(
                &fragment_hash,
                object_id,
                opts.target_version.as_deref(),
                dry_run,
                target_data_shards,
                target_parity_shards,
                target_stripe_size,
                live_blob.as_ref(),
                live_index.as_ref(),
                strict_durability,
            )
            .await?;
            if changed {
                let mut st = REDIST_STATUS.write().await;
                st.total_objects_need_redistribution =
                    st.total_objects_need_redistribution.saturating_add(1);
                if !dry_run {
                    st.total_objects_migrated = st.total_objects_migrated.saturating_add(1);
                    touched_fragments.insert(fragment_hash.clone());
                }
            }
            continue;
        }

        let has_blob_payload = fragment_has_blob_payload(&fragment_hash, live_blob.as_ref()).await;
        let has_indexed_payload =
            fragment_has_indexed_payload(&fragment_hash, live_index.as_ref()).await?;
        if has_blob_payload && !has_indexed_payload {
            info!(
                "Fragment {} has headless blob payload with no live/trash index; leaving it for GC",
                fragment_hash
            );
            continue;
        }

        let mut seen_meta_ids: HashSet<u64> = HashSet::new();

        for is_trash in [false, true] {
            if should_stop() {
                break;
            }
            io_guard.heartbeat();

            let mut delta = StatusDelta {
                scanned: 0,
                needs: 0,
                migrated: 0,
                errors: 0,
                last_object: None,
                last_error: None,
            };
            let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();
            let mut continuation_token = None;
            loop {
                if should_stop() {
                    break;
                }
                io_guard.heartbeat();

                let page = if is_trash {
                    index::scan_fragment_trash_metadata_page(
                        &fragment_hash,
                        None,
                        None,
                        usize::try_from(status_batch.max(1)).unwrap_or(usize::MAX),
                        continuation_token.clone(),
                        true,
                    )
                    .await?
                } else {
                    index::scan_fragment_live_metadata_page(
                        &fragment_hash,
                        None,
                        None,
                        usize::try_from(status_batch.max(1)).unwrap_or(usize::MAX),
                        continuation_token.clone(),
                        true,
                    )
                    .await?
                };

                for meta in page.objects {
                    io_guard.heartbeat();
                    let is_trash_flag = is_trash;
                    let id = dedupe_id_from_metadata(&meta, is_trash_flag);
                    if !seen_meta_ids.insert(id) {
                        continue;
                    }

                    if meta.is_delete_marker {
                        continue;
                    }

                    delta.scanned += 1;
                    if delta.scanned >= status_batch {
                        layout.refresh_if_needed().await;
                        flush_status_delta(&ws_manager, start_time, &mut delta).await;
                    }

                    let needs = check_needs_redistribution(
                        &meta,
                        opts.scope,
                        target_data_shards,
                        target_parity_shards,
                        target_stripe_size,
                        live_blob.len(),
                        &live_blob,
                        &live_index,
                        is_trash_flag,
                    )
                    .await;

                    if !needs.object() {
                        continue;
                    }

                    let object_label = format!(
                        "{}/{} (v:{}) [Trash:{}]",
                        meta.fragment, meta.object_key, meta.version_id, is_trash_flag
                    );
                    delta.needs += 1;
                    delta.last_object = Some(object_label.clone());

                    if dry_run {
                        delta.migrated += 1;
                        if delta.needs + delta.migrated + delta.errors >= status_batch {
                            flush_status_delta(&ws_manager, start_time, &mut delta).await;
                        }
                        continue;
                    }

                    let live_blob_for_task = live_blob.clone();
                    let live_index_for_task = live_index.clone();
                    let task_strict_durability = strict_durability;
                    let task_scope = opts.scope;
                    let task_needs = needs;
                    in_flight.push(async move {
                        let mut meta = meta;
                        let fragment = meta.fragment.clone();
                        let key = meta.object_key.clone();
                        let version = meta.version_id.clone();
                        let intent_paths = get_all_intent_paths_custom(
                            &live_index_for_task,
                            &fragment,
                            &key,
                            &version,
                            is_trash_flag,
                        );
                        let tx_id = Ulid::new().to_string();
                        let created_ms = Utc::now().timestamp_millis();
                        let mut intent = RedistributionIntent {
                            tx_id,
                            fragment: fragment.clone(),
                            object_key: key.clone(),
                            version_id: version.clone(),
                            is_trash: is_trash_flag,
                            scope: task_scope.as_str().to_string(),
                            phase: "prepare".to_string(),
                            created_at_unix_ms: created_ms,
                            updated_at_unix_ms: created_ms,
                        };

                        if let Err(e) =
                            write_intent_state(&intent_paths, &intent, task_strict_durability).await
                        {
                            return Err((
                                fragment,
                                key,
                                version,
                                is_trash_flag,
                                format!("failed to persist redistribution intent: {}", e),
                            ));
                        }
                        intent.phase = "running".to_string();
                        intent.updated_at_unix_ms = Utc::now().timestamp_millis();
                        if let Err(e) =
                            write_intent_state(&intent_paths, &intent, task_strict_durability).await
                        {
                            return Err((
                                fragment,
                                key,
                                version,
                                is_trash_flag,
                                format!("failed to update redistribution intent: {}", e),
                            ));
                        }

                        let result = if task_needs.payload {
                            redistribute_object_streaming(
                                &mut meta,
                                target_data_shards,
                                target_parity_shards,
                                target_stripe_size,
                                &live_blob_for_task,
                                is_trash_flag,
                                task_strict_durability,
                            )
                            .await
                        } else {
                            restore_colocated_metadata(&meta, is_trash_flag).await
                        };

                        match result {
                            Ok(_) => {
                                remove_intent_paths_best_effort(&intent_paths).await;
                                Ok(Some((fragment, key, version, is_trash_flag)))
                            }
                            Err(e) if is_deferrable_owner_unavailability(&e) => {
                                intent.phase = "deferred".to_string();
                                intent.updated_at_unix_ms = Utc::now().timestamp_millis();
                                let _ = write_intent_state(
                                    &intent_paths,
                                    &intent,
                                    task_strict_durability,
                                )
                                .await;
                                Ok(None)
                            }
                            Err(e) => {
                                let err_text = e.to_string();
                                if !(should_stop() || is_stopped_error(&err_text)) {
                                    intent.phase = "failed".to_string();
                                    intent.updated_at_unix_ms = Utc::now().timestamp_millis();
                                    let _ = write_intent_state(
                                        &intent_paths,
                                        &intent,
                                        task_strict_durability,
                                    )
                                    .await;
                                }
                                Err((fragment, key, version, is_trash_flag, err_text))
                            }
                        }
                    });

                    while in_flight.len() >= object_concurrency {
                        if let Some(res) = in_flight.next().await {
                            io_guard.heartbeat();
                            match res {
                                Ok(Some((fragment, key, version, task_is_trash))) => {
                                    touched_fragments.insert(fragment.clone());
                                    delta.migrated += 1;
                                    delta.last_object = Some(format!(
                                        "{}/{} (v:{}) [Trash:{}]",
                                        fragment, key, version, task_is_trash
                                    ));
                                }
                                Ok(None) => {}
                                Err((fragment, key, version, task_is_trash, err)) => {
                                    if should_stop() || is_stopped_error(&err) {
                                        continue;
                                    }
                                    warn!(
                                        "Redistribute failed for {}/{} (v:{}) [Trash:{}]: {}",
                                        fragment, key, version, task_is_trash, err
                                    );
                                    total_task_errors = total_task_errors.saturating_add(1);
                                    delta.errors += 1;
                                    last_task_error = Some(err.clone());
                                    delta.last_error = Some(err);
                                }
                            }
                            if let Some(delay) = pacing_delay {
                                tokio::time::sleep(delay).await;
                            }
                        }
                        if delta.needs + delta.migrated + delta.errors >= status_batch {
                            flush_status_delta(&ws_manager, start_time, &mut delta).await;
                        }
                    }
                }

                match page.continuation_token {
                    Some(token) => continuation_token = Some(token),
                    None => break,
                }
            }

            while let Some(res) = in_flight.next().await {
                io_guard.heartbeat();
                match res {
                    Ok(Some((fragment, key, version, task_is_trash))) => {
                        touched_fragments.insert(fragment.clone());
                        delta.migrated += 1;
                        delta.last_object = Some(format!(
                            "{}/{} (v:{}) [Trash:{}]",
                            fragment, key, version, task_is_trash
                        ));
                    }
                    Ok(None) => {}
                    Err((fragment, key, version, task_is_trash, err)) => {
                        if should_stop() || is_stopped_error(&err) {
                            continue;
                        }
                        warn!(
                            "Redistribute failed for {}/{} (v:{}) [Trash:{}]: {}",
                            fragment, key, version, task_is_trash, err
                        );
                        total_task_errors = total_task_errors.saturating_add(1);
                        delta.errors += 1;
                        last_task_error = Some(err.clone());
                        delta.last_error = Some(err);
                    }
                }
                if let Some(delay) = pacing_delay {
                    tokio::time::sleep(delay).await;
                }
                if delta.needs + delta.migrated + delta.errors >= status_batch {
                    flush_status_delta(&ws_manager, start_time, &mut delta).await;
                }
            }

            flush_status_delta(&ws_manager, start_time, &mut delta).await;
        }
    }

    if !dry_run {
        for fragment in &touched_fragments {
            if let Err(err) = index::sync_fragment_partition_replicas(fragment).await {
                warn!(
                    "Failed to resynchronize N1 index replicas for fragment {}: {}",
                    fragment, err
                );
            }
        }
        for fragment in &scanned_fragments {
            if let Err(err) = index_store::cleanup_stale_index_plane_on_blob_only_roots(fragment) {
                warn!(
                    "Failed to clean stale N1 index plane from blob-only roots for fragment {}: {}",
                    fragment, err
                );
            }
        }
    }

    io_guard.complete();
    drop(_write_gate);

    {
        let mut st = REDIST_STATUS.write().await;
        st.is_running = false;
        st.fast_mode = opts.fast_mode;
        st.durability_mode = effective_durability.to_string();
        st.phase = if should_stop() {
            "Stopped by User".to_string()
        } else if total_task_errors > 0 {
            "Completed with Errors".to_string()
        } else {
            "Completed".to_string()
        };
        if total_task_errors > 0 {
            st.total_errors = st.total_errors.max(total_task_errors);
            st.last_error = last_task_error.clone();
        }
        st.duration_seconds = start_time.elapsed().as_secs();
        ws_manager.broadcast_system_wide(StorageEventPayload::RedistributionUpdate(st.clone()));
    }

    info!("Redistribution Job Ended.");
    if !should_stop() && total_task_errors > 0 {
        let message = match last_task_error {
            Some(last) => format!(
                "Redistribution finished with {} error(s); last error: {}",
                total_task_errors, last
            ),
            None => format!(
                "Redistribution finished with {} error(s)",
                total_task_errors
            ),
        };
        return Err(AppError::InternalServerError(message));
    }

    Ok(())
}

pub(crate) async fn check_needs_redistribution(
    meta: &ObjectMetadata,
    scope: RedistributionScope,
    target_data: usize,
    target_parity: usize,
    target_stripe_size: usize,
    target_num_disks: usize,
    target_blob_roots: &[PathBuf],
    _target_index_roots: &[PathBuf],
    _is_trash: bool,
) -> RedistributionNeeds {
    let mut needs = RedistributionNeeds::default();
    if scope == RedistributionScope::Namespace {
        return needs;
    }

    let target_stripe_size = target_stripe_size_for_metadata(meta, target_stripe_size);
    let total_shards = target_data.saturating_add(target_parity);
    if target_data == 0
        || total_shards == 0
        || target_num_disks < total_shards
        || target_blob_roots.len() != target_num_disks
        || meta.parts.is_empty()
        || usize::from(meta.data_shards) != target_data
        || usize::from(meta.parity_shards) != target_parity
        || meta.stripe_size_bytes != target_stripe_size
    {
        needs.payload = true;
    }

    if !needs.payload {
        let path_manager = ObjectPathManager::new();
        for part in &meta.parts {
            let data_count = part.shards.iter().filter(|shard| !shard.is_parity).count();
            let parity_count = part.shards.iter().filter(|shard| shard.is_parity).count();
            if data_count != target_data
                || parity_count != target_parity
                || part.shards.len() != total_shards
            {
                needs.payload = true;
                break;
            }

            for shard in &part.shards {
                if shard.shard_index >= total_shards
                    || shard.is_parity != (shard.shard_index >= target_data)
                {
                    needs.payload = true;
                    break;
                }
                let root_index = (part.part_index as usize + shard.shard_index) % target_num_disks;
                let expected_path = path_manager
                    .get_shard_dir_on_disk(
                        &target_blob_roots[root_index],
                        &meta.fragment,
                        &meta.object_key,
                        &meta.version_id,
                    )
                    .join(&shard.file_name);
                if Path::new(&shard.absolute_path) != expected_path
                    || !tokio_fs::try_exists(&expected_path).await.unwrap_or(false)
                {
                    needs.payload = true;
                    break;
                }
            }
            if needs.payload {
                break;
            }
        }
    }

    needs.metadata = index_store::object_metadata_placements_healthy(meta)
        .map(|healthy| !healthy)
        .unwrap_or(true);

    needs
}

async fn cleanup_tmp_files(files: &[PathBuf]) {
    for p in files {
        let _ = tokio_fs::remove_file(p).await;
    }
}

async fn write_redistributed_metadata(
    meta: &ObjectMetadata,
    is_trash: bool,
) -> Result<(), AppError> {
    if is_trash {
        index::publish_trashed_object_version_for_maintenance(meta).await
    } else {
        index::upsert_object_version(meta).await
    }
}

pub(super) async fn restore_colocated_metadata(
    meta: &ObjectMetadata,
    is_trash: bool,
) -> Result<(), AppError> {
    let _object_lock =
        index::acquire_object_operation_lock(&meta.fragment, &meta.object_key).await?;
    if should_stop() {
        return Err(AppError::InternalServerError("Stopped".into()));
    }
    let still_present = if is_trash {
        index::read_trashed_object_version(&meta.fragment, &meta.object_key, &meta.version_id)
            .await
            .is_ok()
    } else {
        index::read_object_version(&meta.fragment, &meta.object_key, &meta.version_id)
            .await
            .is_ok()
    };
    if still_present {
        write_redistributed_metadata(meta, is_trash).await?;
    }
    Ok(())
}

async fn crc32_file_hex(path: &Path) -> Result<String, AppError> {
    let mut f = tokio_fs::File::open(path)
        .await
        .map_err(|e| AppError::Io(e, format!("open for crc32 {}", path.display())))?;

    let mut hasher = Crc32Hasher::new();
    let mut buf = vec![0u8; 256 * 1024];

    loop {
        let n = f
            .read(&mut buf)
            .await
            .map_err(|e| AppError::Io(e, format!("read crc32 {}", path.display())))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    Ok(format!("{:08x}", hasher.finalize()))
}

async fn verify_tmp_shard(
    tmp_path: &Path,
    expected_len: u64,
    expected_crc32: Option<&str>,
) -> Result<(), AppError> {
    let len_ok = tokio_fs::metadata(tmp_path)
        .await
        .map(|m| m.len() == expected_len)
        .unwrap_or(false);

    if !len_ok {
        return Err(AppError::InternalServerError(format!(
            "Shard length mismatch for {}",
            tmp_path.display()
        )));
    }

    if let Some(exp) = expected_crc32.filter(|s| !s.is_empty()) {
        let got = crc32_file_hex(tmp_path).await?;
        if !got.eq_ignore_ascii_case(exp) {
            return Err(AppError::InternalServerError(format!(
                "Shard checksum mismatch for {} (expected {}, got {})",
                tmp_path.display(),
                exp,
                got
            )));
        }
    }

    Ok(())
}

async fn ensure_authoritative_partition_owner_writable(
    fragment: &str,
    object_key: &str,
) -> Result<(), AppError> {
    index::ensure_object_owner_partition_available(fragment, object_key).await
}

pub(super) async fn redistribute_object_streaming(
    meta: &mut ObjectMetadata,
    target_data_shards: usize,
    target_parity_shards: usize,
    target_stripe_size: usize,
    target_blob_roots: &[PathBuf],
    is_trash: bool,
    strict_durability: bool,
) -> Result<(), AppError> {
    let _object_lock =
        index::acquire_object_operation_lock(&meta.fragment, &meta.object_key).await?;
    if should_stop() {
        return Err(AppError::InternalServerError("Stopped".into()));
    }
    ensure_authoritative_partition_owner_writable(&meta.fragment, &meta.object_key).await?;

    let still_present = if is_trash {
        index::read_trashed_object_version(&meta.fragment, &meta.object_key, &meta.version_id)
            .await
            .is_ok()
    } else {
        index::read_object_version(&meta.fragment, &meta.object_key, &meta.version_id)
            .await
            .is_ok()
    };
    if !still_present {
        return Ok(());
    }

    let start_time = Instant::now();
    let num_disks = target_blob_roots.len();
    let total_shards = target_data_shards + target_parity_shards;

    if num_disks < total_shards {
        return Err(AppError::InsufficientStorage);
    }

    let old_block_size = meta.stripe_size_bytes;
    let new_block_size = target_stripe_size_for_metadata(meta, target_stripe_size);
    let verify_tmp_crc = strict_durability;

    let mut file_cache: HashMap<String, File> = HashMap::new();

    let old_parts = std::mem::take(&mut meta.parts);
    let mut new_parts: Vec<PartMetadata> = Vec::with_capacity(old_parts.len());
    let mut old_shards_for_gc: Vec<String> = Vec::new();

    for old_part in old_parts {
        if should_stop() {
            return Err(AppError::InternalServerError("Stopped".into()));
        }

        let old_part_arc = Arc::new(old_part);

        let old_data_count = old_part_arc.shards.iter().filter(|s| !s.is_parity).count() as u64;
        if old_data_count == 0 {
            return Err(AppError::InternalServerError("invalid old EC".into()));
        }

        let old_stripe_width = old_block_size as u64 * old_data_count;

        let new_data_width = (new_block_size * target_data_shards) as u64;
        if new_data_width == 0 {
            return Err(AppError::InternalServerError(
                "invalid target stripe".into(),
            ));
        }

        let new_stripe_count = (old_part_arc.size_bytes + new_data_width - 1) / new_data_width;
        let final_shard_len = new_stripe_count * new_block_size as u64;

        let mut writers: Vec<Option<DirectWriter>> = Vec::with_capacity(total_shards);
        let mut commit_list: Vec<(PathBuf, PathBuf, PathBuf, String)> = Vec::new();
        let mut new_shards: Vec<ShardInfo> = Vec::with_capacity(total_shards);
        let mut created_tmps: Vec<PathBuf> = Vec::new();

        for shard_idx in 0..total_shards {
            if should_stop() {
                cleanup_tmp_files(&created_tmps).await;
                return Err(AppError::InternalServerError("Stopped".into()));
            }

            let is_parity = shard_idx >= target_data_shards;
            let ext = if is_parity {
                PARITY_SUFFIX
            } else {
                SHARD_SUFFIX
            };

            let file_name = format!(
                "part_{}_shard_{}{}",
                old_part_arc.part_index, shard_idx, ext
            );

            let disk_idx = (old_part_arc.part_index as usize + shard_idx) % num_disks;
            let disk_root = &target_blob_roots[disk_idx];

            if !tokio_fs::try_exists(disk_root).await.unwrap_or(false) {
                cleanup_tmp_files(&created_tmps).await;
                return Err(AppError::InternalServerError(
                    "blob disk unreachable".into(),
                ));
            }

            let shard_dir = get_shard_dir_custom(
                disk_root,
                &meta.fragment,
                &meta.object_key,
                &meta.version_id,
                is_trash,
            );
            let _ = tokio_fs::create_dir_all(&shard_dir).await;

            let final_path = shard_dir.join(&file_name);

            let tmp_path = shard_dir.join(format!(
                "{}{}{}",
                file_name,
                REDIST_TMP_SUFFIX,
                Ulid::new().to_string()
            ));
            created_tmps.push(tmp_path.clone());

            let std_file = create_direct_file(&tmp_path).await.map_err(|e| {
                AppError::InternalServerError(format!(
                    "create shard tmp {}: {}",
                    tmp_path.display(),
                    e
                ))
            })?;

            let file_clone = std_file
                .try_clone()
                .map_err(|e| AppError::Io(e, "clone direct file".into()))?;

            let len = final_shard_len;
            tokio::task::spawn_blocking(move || file_clone.set_len(len))
                .await
                .map_err(|e| AppError::InternalServerError(format!("set_len join: {}", e)))?
                .map_err(|e| AppError::Io(e, "set_len shard".into()))?;

            writers.push(Some(DirectWriter::new(std_file)));
            commit_list.push((
                tmp_path,
                final_path.clone(),
                shard_dir.clone(),
                file_name.clone(),
            ));

            new_shards.push(ShardInfo {
                shard_index: shard_idx,
                file_name: file_name.clone(),
                absolute_path: final_path.to_string_lossy().to_string(),
                size_bytes: final_shard_len,
                checksum: String::new(),
                is_parity,
            });
        }

        let mut stream_writer = ErasureStreamWriter::new(
            writers,
            target_data_shards,
            target_parity_shards,
            old_part_arc.size_bytes,
            false,
            new_block_size,
        )?;

        let mut pos = 0u64;
        let mut batch: Vec<u8> = Vec::with_capacity(new_block_size);

        while pos < old_part_arc.size_bytes {
            if should_stop() {
                cleanup_tmp_files(&created_tmps).await;
                return Err(AppError::InternalServerError("Stopped".into()));
            }

            let stripe_index = pos / old_stripe_width;
            let offset_in_stripe = pos % old_stripe_width;

            let shard_index = (offset_in_stripe / old_block_size as u64) as usize;
            let offset_in_block = offset_in_stripe % old_block_size as u64;
            let file_offset = (stripe_index * old_block_size as u64) + offset_in_block;

            let to_read = ((old_block_size as u64 - offset_in_block)
                .min(old_part_arc.size_bytes - pos)) as usize;
            if to_read == 0 {
                break;
            }

            let chunk = match read_or_recover_chunk(
                &mut file_cache,
                Path::new(""),
                old_part_arc.clone(),
                shard_index,
                file_offset,
                to_read,
                old_block_size,
            )
            .await
            {
                Ok(chunk) => chunk,
                Err(err) => {
                    let still_present = if is_trash {
                        index::read_trashed_object_version(
                            &meta.fragment,
                            &meta.object_key,
                            &meta.version_id,
                        )
                        .await
                        .is_ok()
                    } else {
                        index::read_object_version(
                            &meta.fragment,
                            &meta.object_key,
                            &meta.version_id,
                        )
                        .await
                        .is_ok()
                    };
                    if !still_present {
                        cleanup_tmp_files(&created_tmps).await;
                        return Ok(());
                    }
                    return Err(err);
                }
            };

            batch.extend_from_slice(&chunk);

            if batch.len() >= new_block_size || (pos + to_read as u64) >= old_part_arc.size_bytes {
                stream_writer.write(&batch).await?;
                batch.clear();
            }

            pos += to_read as u64;
        }

        stream_writer.finalize().await?;

        let sums = stream_writer.get_shard_checksums();
        let mut sum_map: HashMap<usize, String> = HashMap::new();
        for (i, s) in sums.into_iter().enumerate() {
            sum_map.insert(i, s);
        }
        let writers = stream_writer.into_writers();

        if strict_durability {
            for w in writers.into_iter().flatten() {
                if let Err(e) = w.sync_data().await {
                    cleanup_tmp_files(&created_tmps).await;
                    return Err(AppError::Io(e, "fsync redistribution shard".into()));
                }
            }
        }

        for s in new_shards.iter_mut() {
            if let Some(v) = sum_map.get(&s.shard_index) {
                s.checksum = v.clone();
            }
        }
        let checksum_by_name: HashMap<&str, &str> = new_shards
            .iter()
            .map(|s| (s.file_name.as_str(), s.checksum.as_str()))
            .collect();
        let mut batched_sync_dirs: HashSet<PathBuf> = HashSet::new();

        for (tmp, final_p, parent, fname) in &commit_list {
            if should_stop() {
                cleanup_tmp_files(&created_tmps).await;
                return Err(AppError::InternalServerError("Stopped".into()));
            }

            let expected_crc = if verify_tmp_crc {
                checksum_by_name.get(fname.as_str()).copied()
            } else {
                None
            };

            verify_tmp_shard(tmp.as_path(), final_shard_len, expected_crc).await?;

            publish_tmp_overwrite(
                tmp.as_path(),
                final_p.as_path(),
                parent.as_path(),
                fname,
                strict_durability,
            )
            .await?;

            if !strict_durability {
                batched_sync_dirs.insert(parent.clone());
            }
        }

        if !strict_durability {
            for d in batched_sync_dirs {
                let _ = sync_dir(&d).await;
            }
        }

        let new_part = PartMetadata {
            part_index: old_part_arc.part_index,
            size_bytes: old_part_arc.size_bytes,
            created_at: old_part_arc.created_at,
            shards: new_shards.clone(),
            checksum: old_part_arc.checksum.clone(),
        };
        new_parts.push(new_part);

        let new_paths: HashSet<String> = new_parts
            .last()
            .unwrap()
            .shards
            .iter()
            .map(|s| s.absolute_path.clone())
            .collect();

        for old_shard in &old_part_arc.shards {
            if !new_paths.contains(&old_shard.absolute_path) {
                old_shards_for_gc.push(old_shard.absolute_path.clone());
            }
        }

        file_cache.clear();
    }

    meta.parts = new_parts;
    meta.data_shards = u16::try_from(target_data_shards).map_err(|_| {
        AppError::InternalServerError("target data shard count exceeds metadata format".into())
    })?;
    meta.parity_shards = u16::try_from(target_parity_shards).map_err(|_| {
        AppError::InternalServerError("target parity shard count exceeds metadata format".into())
    })?;
    meta.stripe_size_bytes = new_block_size;

    let still_present = if is_trash {
        index::read_trashed_object_version(&meta.fragment, &meta.object_key, &meta.version_id)
            .await
            .is_ok()
    } else {
        index::read_object_version(&meta.fragment, &meta.object_key, &meta.version_id)
            .await
            .is_ok()
    };

    if !still_present {
        for part in &meta.parts {
            for shard in &part.shards {
                let p = PathBuf::from(&shard.absolute_path);
                let _ = tokio_fs::remove_file(&p).await;
            }
        }
        return Ok(());
    }

    meta.placement_generation = meta.placement_generation.checked_add(1).ok_or_else(|| {
        AppError::InternalServerError(
            "object placement generation exhausted during redistribution".into(),
        )
    })?;
    write_redistributed_metadata(meta, is_trash).await?;

    for old in old_shards_for_gc {
        if should_stop() {
            return Err(AppError::InternalServerError("Stopped".into()));
        }
        let _ = stage_old_shard_gc_with_symlink(&old).await;
    }

    info!(
        "Redistributed {}/{} (v:{}) [Trash:{}] in {:.2?}.",
        meta.fragment,
        meta.object_key,
        meta.version_id,
        is_trash,
        start_time.elapsed()
    );

    Ok(())
}
