//! Fragment metadata, namespace replica, and object-layout repair mechanics.

use super::*;

pub(super) async fn list_all_fragments(index_roots: &[PathBuf]) -> Vec<String> {
    let roots = index_roots.to_vec();
    tokio::task::spawn_blocking(move || {
        let mut out = HashSet::new();
        for root in roots {
            if !root.exists() {
                continue;
            }
            for name in list_fragment_aliases(&root) {
                if fragment_root_on(&root, &name)
                    .join(FRAGMENT_METADATA_FILE)
                    .is_file()
                {
                    out.insert(stable_fragment_id_for_root(&root, &name).unwrap_or(name));
                }
            }
        }
        let mut v: Vec<String> = out.into_iter().collect();
        v.sort();
        v
    })
    .await
    .unwrap_or_default()
}

pub(super) async fn reachable_dir_roots(roots: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for root in roots {
        if let Ok(meta) = fs::metadata(&root).await {
            if meta.is_dir() {
                out.push(root);
            }
        }
    }
    out
}

fn sha256_bytes(b: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b);
    let out = h.finalize();
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&out[..]);
    arr
}

#[derive(Clone)]
struct RawReplica {
    bytes: Option<Vec<u8>>,
    mtime: SystemTime,
    len: u64,
    sha256: Option<[u8; 32]>,
    valid: bool,
}

async fn read_raw_replica<F>(full: &Path, valid_fn: &F) -> RawReplica
where
    F: Fn(&[u8]) -> bool + Send + Sync,
{
    match fs::read(full).await {
        Ok(bytes) => {
            let meta = fs::metadata(full).await.ok();
            let mtime = meta
                .as_ref()
                .and_then(|m| m.modified().ok())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            let len = meta.as_ref().map(|m| m.len()).unwrap_or(bytes.len() as u64);
            let sha = Some(sha256_bytes(&bytes));
            let valid = valid_fn(&bytes);

            RawReplica {
                bytes: Some(bytes),
                mtime,
                len,
                sha256: sha,
                valid,
            }
        }
        Err(_) => RawReplica {
            bytes: None,
            mtime: SystemTime::UNIX_EPOCH,
            len: 0,
            sha256: None,
            valid: false,
        },
    }
}

fn choose_best_raw_replica(replicas: &[RawReplica]) -> Option<usize> {
    let mut best: Option<usize> = None;
    for (i, r) in replicas.iter().enumerate() {
        if !r.valid {
            continue;
        }
        let take = match best {
            None => true,
            Some(cur) => {
                let c = &replicas[cur];
                (r.mtime > c.mtime)
                    || (r.mtime == c.mtime && r.len > c.len)
                    || (r.mtime == c.mtime
                        && r.len == c.len
                        && r.sha256.unwrap_or([0u8; 32]) > c.sha256.unwrap_or([0u8; 32]))
            }
        };
        if take {
            best = Some(i);
        }
    }
    best
}

async fn replicate_bytes_strict(
    paths: &[PathBuf],
    canonical: &[u8],
    dry_run: bool,
) -> Result<(u64, u64), AppError> {
    let canon_sha = sha256_bytes(canonical);
    let mut missing = 0u64;
    let mut divergent = 0u64;

    for p in paths {
        match fs::read(p).await {
            Ok(b) => {
                if sha256_bytes(&b) != canon_sha {
                    divergent += 1;
                    if !dry_run {
                        linux_io::atomic_write(p, canonical.to_vec()).await?;
                    }
                }
            }
            Err(_) => {
                missing += 1;
                if !dry_run {
                    linux_io::atomic_write(p, canonical.to_vec()).await?;
                }
            }
        }
    }

    Ok((missing, divergent))
}

pub(super) async fn fragment_is_present(
    roots: &[PathBuf],
    fragment: &str,
) -> Result<bool, AppError> {
    for root in roots {
        let path = fragment_root_on(root, fragment);
        match fs::metadata(&path).await {
            Ok(metadata) if metadata.is_dir() => return Ok(true),
            Ok(_) => {
                return Err(AppError::Conflict(format!(
                    "fragment root is not a directory: {}",
                    path.display()
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(AppError::Io(
                    error,
                    format!("inspect fragment lifecycle {}", path.display()),
                ));
            }
        }
    }
    Ok(false)
}

pub(super) async fn repair_fragment_metadata_replicas(
    index_roots: &[PathBuf],
    fragment: &str,
    dry_run: bool,
) -> Result<(u64, u64, u64), AppError> {
    let paths: Vec<PathBuf> = index_roots
        .iter()
        .map(|r| fragment_root_on(r, fragment).join(FRAGMENT_METADATA_FILE))
        .collect();

    let valid = |b: &[u8]| -> bool { rmp_serde::from_slice::<serde_json::Value>(b).is_ok() };

    let mut reps = Vec::with_capacity(paths.len());
    for p in &paths {
        reps.push(read_raw_replica(p, &valid).await);
    }

    let Some(best) = choose_best_raw_replica(&reps) else {
        let any_exists = reps.iter().any(|r| r.bytes.is_some());
        let corrupt = if any_exists { 1 } else { 0 };
        return Ok((0, 0, corrupt));
    };

    let canon = reps[best].bytes.clone().unwrap();
    let (m, d) = replicate_bytes_strict(&paths, &canon, dry_run).await?;
    Ok((m, d, 0))
}

pub(super) async fn repair_fragment_index_db(
    _index_roots: &[PathBuf],
    fragment: &str,
    dry_run: bool,
) -> Result<(u64, u64, u64), AppError> {
    let fragment = fragment.to_string();
    tokio::task::spawn_blocking(move || {
        index_store::repair_fragment_all_partition_replicas(&fragment, dry_run)
    })
    .await
    .map_err(|err| AppError::InternalServerError(format!("N1 repair worker: {err}")))?
}

pub(super) async fn process_object_repair(
    meta: ObjectMetadata,
    writable_blob_roots: &[PathBuf],
    cfg: RepairRunConfig,
    shard_repair_sem: Arc<Semaphore>,
    catalog: MetadataCatalog,
) -> Result<(usize, usize, usize, usize, Option<String>, Option<String>), AppError> {
    if meta.parts.is_empty() || writable_blob_roots.is_empty() {
        return Ok((0, 0, 0, 0, None, None));
    }

    let path_manager = ObjectPathManager::new();
    let _object_guard =
        index::acquire_object_operation_lock(&meta.fragment, &meta.object_key).await?;
    let mut metadata = match catalog {
        MetadataCatalog::Live => {
            match index::read_object_version(&meta.fragment, &meta.object_key, &meta.version_id)
                .await
            {
                Ok(current) => current,
                Err(AppError::NotFound) => return Ok((0, 0, 0, 0, None, None)),
                Err(error) => return Err(error),
            }
        }
        MetadataCatalog::Trash => {
            match index::read_trashed_object_version(
                &meta.fragment,
                &meta.object_key,
                &meta.version_id,
            )
            .await
            {
                Ok(current) => current,
                Err(AppError::NotFound) => return Ok((0, 0, 0, 0, None, None)),
                Err(error) => return Err(error),
            }
        }
    };
    let configured_blob_roots = path_manager.get_blob_roots();

    let mut repaired_total = 0usize;
    let mut corrupt_found = 0usize;
    let mut last_repaired: Option<String> = None;
    let mut failed_repairs = 0usize;
    let mut last_failure: Option<String> = None;
    let mut placement_changed = false;

    for part_index in 0..metadata.parts.len() {
        if should_stop() {
            break;
        }

        let mut part_fixed =
            normalize_part_paths_for_repair(metadata.parts[part_index].clone()).await;
        let mut missing_shards: Vec<usize> = Vec::new();

        for shard in &part_fixed.shards {
            let shard_path = PathBuf::from(strip_tmp_attempt_suffix(&shard.absolute_path));
            let mut healthy = false;

            if !configured_blob_roots
                .iter()
                .any(|root| shard_path.starts_with(root))
            {
                missing_shards.push(shard.shard_index);
                continue;
            }

            match fs::metadata(&shard_path).await {
                Ok(m) => {
                    if !cfg.check_mode.verifies_full_integrity() {
                        healthy = m.len() == shard.size_bytes;
                        if !healthy {
                            corrupt_found += 1;
                            warn!("Size mismatch shard (availability): {:?}", shard_path);
                        }
                    } else {
                        if let Ok(f) = linux_io::open_read(&shard_path).await {
                            healthy = verify_shard_integrity(&f, &shard.checksum).await;
                            if !healthy {
                                corrupt_found += 1;
                                warn!("Corrupt shard found: {:?}", shard_path);
                            }
                        }
                    }
                }
                Err(_) => {
                    healthy = false;
                }
            }

            if !healthy {
                missing_shards.push(shard.shard_index);
            }
        }

        if missing_shards.is_empty() || cfg.dry_run {
            continue;
        }

        for missing_idx in missing_shards {
            let Some(shard_position) = part_fixed
                .shards
                .iter()
                .position(|shard| shard.shard_index == missing_idx)
            else {
                continue;
            };
            let original_path = PathBuf::from(&part_fixed.shards[shard_position].absolute_path);
            let original_root = configured_blob_roots
                .iter()
                .filter(|root| original_path.starts_with(root))
                .max_by_key(|root| root.components().count());
            let original_root_is_writable = original_root
                .is_some_and(|root| writable_blob_roots.iter().any(|active| active == root));

            if !original_root_is_writable {
                let occupied_roots = part_fixed
                    .shards
                    .iter()
                    .filter(|shard| shard.shard_index != missing_idx)
                    .filter_map(|shard| {
                        let path = Path::new(&shard.absolute_path);
                        configured_blob_roots
                            .iter()
                            .filter(|root| path.starts_with(root))
                            .max_by_key(|root| root.components().count())
                            .cloned()
                    })
                    .collect::<HashSet<_>>();
                #[cfg(unix)]
                let occupied_devices = {
                    use std::os::unix::fs::MetadataExt;
                    occupied_roots
                        .iter()
                        .filter_map(|root| std::fs::metadata(root).ok().map(|meta| meta.dev()))
                        .collect::<HashSet<_>>()
                };
                let ranked_roots = if crate::core::storage_pool::configured().is_empty() {
                    path_manager.rank_blob_roots_for_object(
                        &metadata.fragment,
                        &metadata.object_key,
                        &metadata.version_id,
                    )
                } else {
                    crate::core::storage_pool::pool_for_metadata(&metadata)
                        .ok_or_else(|| {
                            AppError::Conflict(
                                "recorded placement does not belong to one immutable storage pool"
                                    .into(),
                            )
                        })?
                        .roots
                        .clone()
                };
                let replacement = ranked_roots.into_iter().find(|candidate| {
                    let eligible = writable_blob_roots.iter().any(|active| active == candidate)
                        && !occupied_roots.contains(candidate);
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::MetadataExt;
                        eligible
                            && std::fs::metadata(candidate)
                                .ok()
                                .is_some_and(|meta| !occupied_devices.contains(&meta.dev()))
                    }
                    #[cfg(not(unix))]
                    {
                        eligible
                    }
                });
                let Some(replacement) = replacement else {
                    let message = format!(
                        "Deferred repair for {}/{} shard {}: no distinct writable replacement root",
                        metadata.fragment, metadata.object_key, missing_idx
                    );
                    warn!("{message}");
                    failed_repairs += 1;
                    last_failure = Some(message);
                    continue;
                };
                let replacement_path = path_manager
                    .get_shard_dir_on_disk(
                        &replacement,
                        &metadata.fragment,
                        &metadata.object_key,
                        &metadata.version_id,
                    )
                    .join(&part_fixed.shards[shard_position].file_name);
                part_fixed.shards[shard_position].absolute_path =
                    replacement_path.to_string_lossy().to_string();
            }

            let _permit = shard_repair_sem
                .acquire()
                .await
                .map_err(|_| AppError::InternalServerError("Repair semaphore closed".into()))?;
            match repair_entire_shard(
                Arc::new(part_fixed.clone()),
                missing_idx,
                metadata.stripe_size_bytes,
            )
            .await
            {
                Ok(_) => {
                    repaired_total += 1;
                    placement_changed |=
                        Path::new(&part_fixed.shards[shard_position].absolute_path)
                            != original_path;
                    metadata.parts[part_index] = part_fixed.clone();
                    last_repaired = Some(format!("{}/{}", metadata.fragment, metadata.object_key));
                }
                Err(e) => {
                    part_fixed.shards[shard_position].absolute_path =
                        original_path.to_string_lossy().to_string();
                    let message = format!(
                        "Shard repair failed for {}/{} shard {}: {}",
                        metadata.fragment, metadata.object_key, missing_idx, e
                    );
                    warn!("{message}");
                    failed_repairs += 1;
                    last_failure = Some(message);
                }
            }
        }
    }

    let metadata_needs_repair = if placement_changed {
        true
    } else {
        let metadata_for_check = metadata.clone();
        !tokio::task::spawn_blocking(move || {
            index_store::object_metadata_placements_healthy(&metadata_for_check)
        })
        .await
        .map_err(|error| {
            AppError::InternalServerError(format!("object metadata health worker failed: {error}"))
        })??
    };

    if placement_changed {
        metadata.placement_generation =
            metadata
                .placement_generation
                .checked_add(1)
                .ok_or_else(|| {
                    AppError::InternalServerError(
                        "object placement generation exhausted during repair".into(),
                    )
                })?;
    }
    let metadata_repaired = if metadata_needs_repair {
        match catalog {
            MetadataCatalog::Live => index::upsert_object_version(&metadata).await?,
            MetadataCatalog::Trash => {
                index::publish_trashed_object_version_for_maintenance(&metadata).await?
            }
        }
        1
    } else {
        0
    };

    Ok((
        repaired_total,
        corrupt_found,
        metadata_repaired,
        failed_repairs,
        last_repaired,
        last_failure,
    ))
}
