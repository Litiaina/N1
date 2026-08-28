//! Index-plane replica inspection, selection, synchronization, and repair.

use super::*;

pub fn fragment_has_object_index_records(fragment: &str, roots: &[PathBuf]) -> bool {
    for root in roots {
        let Ok(object_dirs) = local_object_dirs_under_root(root, fragment) else {
            continue;
        };
        for (object_dir, _) in object_dirs {
            let Ok(records) = fs::read_dir(object_dir) else {
                continue;
            };
            for record in records.filter_map(|entry| entry.ok()) {
                let Some(name) = record.file_name().to_str().map(str::to_string) else {
                    continue;
                };
                if parse_ndx_file_name(&name).is_some() {
                    return true;
                }
            }
        }
    }
    false
}

pub(super) fn count_object_index_records(root: &Path, fragment: &str) -> u64 {
    let Ok(object_dirs) = local_object_dirs_under_root(root, fragment) else {
        return 0;
    };
    let mut count = 0u64;
    for (object_dir, _) in object_dirs {
        let Ok(records) = fs::read_dir(object_dir) else {
            continue;
        };
        for record in records.filter_map(|entry| entry.ok()) {
            if record
                .file_name()
                .to_str()
                .is_some_and(|name| parse_ndx_file_name(name).is_some())
            {
                count += 1;
            }
        }
    }
    count
}

fn count_upload_records(root: &Path, fragment: &str) -> u64 {
    let uploads_root = fragment_root_on(root, fragment).join(UPLOADS_DIR);
    let mut count = 0u64;
    let mut stack = vec![uploads_root];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.filter_map(|entry| entry.ok()) {
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                stack.push(entry.path());
            } else if file_type.is_file() {
                count += 1;
            }
        }
    }
    count
}

fn count_namespace_links(root: &Path, fragment: &str) -> u64 {
    collect_symlink_object_ids_under(root, fragment, &fragment_root_on(root, fragment), true)
        .map(|ids| ids.len() as u64)
        .unwrap_or(0)
}

fn replica_score(root: &Path, fragment: &str) -> ReplicaScore {
    ReplicaScore {
        meta_present: u64::from(
            fragment_root_on(root, fragment)
                .join(FRAGMENT_META_FILE)
                .is_file(),
        ),
        object_records: 0,
        namespace_links: count_namespace_links(root, fragment),
        upload_records: count_upload_records(root, fragment),
    }
}

fn add_digest_mod_256(accumulator: &mut [u8; 32], digest: &[u8; 32]) {
    let mut carry = 0u16;
    for index in (0..32).rev() {
        let sum = u16::from(accumulator[index]) + u16::from(digest[index]) + carry;
        accumulator[index] = sum as u8;
        carry = sum >> 8;
    }
}

fn hash_replica_tree(fragment_root: &Path, directory: &Path) -> Result<[u8; 32], AppError> {
    let mut sum = [0u8; 32];
    let mut xor = [0u8; 32];
    let mut entry_count = 0u64;
    visit_dir_names(directory, |name| {
        if directory == fragment_root
            && matches!(
                name.as_str(),
                OBJECTS_DIR | TMP_DIR | LOCKS_DIR | REPAIR_DIR
            )
        {
            return Ok(true);
        }
        let path = directory.join(&name);
        let relative = path.strip_prefix(fragment_root).map_err(|_| {
            AppError::InternalServerError(format!(
                "N1 replica manifest path {} escapes {}",
                path.display(),
                fragment_root.display()
            ))
        })?;
        let relative = relative.to_string_lossy();
        let metadata = fs::symlink_metadata(&path)
            .map_err(|e| io_err(e, "inspect N1 replica manifest entry"))?;
        let mut entry_hasher = Sha256::new();
        if metadata.file_type().is_symlink() {
            let target =
                fs::read_link(&path).map_err(|e| io_err(e, "read N1 replica manifest symlink"))?;
            entry_hasher.update(b"L\0");
            entry_hasher.update(relative.as_bytes());
            entry_hasher.update(b"\0");
            entry_hasher.update(target.as_os_str().as_encoded_bytes());
        } else if metadata.is_dir() {
            entry_hasher.update(b"D\0");
            entry_hasher.update(relative.as_bytes());
            entry_hasher.update(b"\0");
            entry_hasher.update(hash_replica_tree(fragment_root, &path)?);
        } else if metadata.is_file() {
            entry_hasher.update(b"F\0");
            entry_hasher.update(relative.as_bytes());
            entry_hasher.update(b"\0");
            let mut file =
                File::open(&path).map_err(|e| io_err(e, "open N1 replica manifest file"))?;
            let mut buffer = [0u8; 64 * 1024];
            loop {
                let read = file
                    .read(&mut buffer)
                    .map_err(|e| io_err(e, "read N1 replica manifest file"))?;
                if read == 0 {
                    break;
                }
                entry_hasher.update(&buffer[..read]);
            }
        } else {
            return Err(AppError::InternalServerError(format!(
                "N1 replica manifest contains unsupported entry {}",
                path.display()
            )));
        }
        let digest: [u8; 32] = entry_hasher.finalize().into();
        add_digest_mod_256(&mut sum, &digest);
        for (target, value) in xor.iter_mut().zip(digest) {
            *target ^= value;
        }
        entry_count = entry_count.saturating_add(1);
        Ok(true)
    })?;

    let mut tree_hasher = Sha256::new();
    tree_hasher.update(b"n1-replica-tree-unordered-v1\0");
    tree_hasher.update(entry_count.to_be_bytes());
    tree_hasher.update(sum);
    tree_hasher.update(xor);
    Ok(tree_hasher.finalize().into())
}

fn replica_state(root: &Path, fragment: &str) -> Result<ReplicaState, AppError> {
    let fragment_root = fragment_root_on(root, fragment);
    let content_digest = if fragment_root.is_dir() {
        hash_replica_tree(&fragment_root, &fragment_root)?
    } else {
        Sha256::digest(b"n1-replica-tree-missing-v1").into()
    };
    Ok(ReplicaState {
        score: replica_score(root, fragment),
        content_digest,
    })
}

fn replica_index_records(score: &ReplicaScore) -> u64 {
    score
        .object_records
        .saturating_add(score.namespace_links)
        .saturating_add(score.upload_records)
}

pub(super) fn mtime_nanos(path: &Path) -> u128 {
    fs::symlink_metadata(path)
        .and_then(|meta| meta.modified())
        .ok()
        .and_then(|mtime| mtime.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}

pub(super) fn latest_index_plane_mtime_nanos(fragment_root: &Path) -> u128 {
    let mut latest = mtime_nanos(fragment_root);
    let Ok(entries) = fs::read_dir(fragment_root) else {
        return latest;
    };
    for entry in entries.filter_map(|entry| entry.ok()) {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if matches!(
            name.as_ref(),
            OBJECTS_DIR | FRAGMENT_META_FILE | TMP_DIR | LOCKS_DIR | GC_DIR | REPAIR_DIR
        ) {
            continue;
        }
        let path = entry.path();
        latest = latest.max(mtime_nanos(&path));
        if fs::symlink_metadata(&path)
            .map(|meta| meta.is_dir())
            .unwrap_or(false)
        {
            latest = latest.max(latest_index_plane_mtime_nanos(&path));
        }
    }
    latest
}

pub(super) fn choose_replica_source(fragment: &str) -> Option<(PathBuf, ReplicaState)> {
    let mut best: Option<(PathBuf, ReplicaState, u128)> = None;
    for root in index_roots() {
        let fragment_root = fragment_root_on(&root, fragment);
        if !fragment_root.exists() {
            continue;
        }
        let Ok(state) = replica_state(&root, fragment) else {
            continue;
        };
        let modified = latest_index_plane_mtime_nanos(&fragment_root);
        let take = match best.as_ref() {
            None => true,
            Some((_, current_state, current_modified)) => {
                let records = replica_index_records(&state.score);
                let current_records = replica_index_records(&current_state.score);
                if records > current_records {
                    true
                } else if records < current_records {
                    false
                } else {
                    // Namespace replicas are filesystem state. For equal-size replicas,
                    // the latest filesystem mutation is authoritative and maintenance
                    // reconciles the other roots outside the request path.
                    modified > *current_modified
                        || (modified == *current_modified && state.score > current_state.score)
                }
            }
        };
        if take {
            best = Some((root, state, modified));
        }
    }
    best.map(|(root, score, _)| (fragment_root_on(&root, fragment), score))
}

pub fn inspect_fragment_partition_replicas(fragment: &str, roots: &[PathBuf]) -> ReplicaInspection {
    let expected_replicas = roots.len() as u64;
    let mut present_replicas = 0u64;
    let mut missing_replicas = 0u64;
    let mut divergent_replicas = 0u64;
    let mut corrupt_replicas = 0u64;
    let Some((source_fragment_root, canonical_state)) = choose_replica_source(fragment) else {
        return ReplicaInspection {
            expected_replicas,
            present_replicas: 0,
            valid_replicas: 0,
            missing_replicas: expected_replicas,
            divergent_replicas: 0,
            corrupt_replicas: 0,
            healthy: expected_replicas == 0,
            canonical_path: None,
        };
    };

    let mut valid_replicas = 0u64;
    for root in roots {
        let fragment_root = fragment_root_on(&root, fragment);
        if !fragment_root.exists() {
            missing_replicas += 1;
            continue;
        }
        present_replicas += 1;
        let state = match replica_state(root, fragment) {
            Ok(state) => state,
            Err(_) => {
                corrupt_replicas += 1;
                continue;
            }
        };
        if state == canonical_state {
            valid_replicas += 1;
            continue;
        }
        if state.score < canonical_state.score {
            missing_replicas += 1;
        } else {
            divergent_replicas += 1;
        }
    }

    ReplicaInspection {
        expected_replicas,
        present_replicas,
        valid_replicas,
        missing_replicas,
        divergent_replicas,
        corrupt_replicas,
        healthy: missing_replicas == 0 && divergent_replicas == 0 && corrupt_replicas == 0,
        canonical_path: Some(source_fragment_root.display().to_string()),
    }
}

fn read_fragment_meta_from(root: &Path, fragment: &str) -> Result<Option<FragmentMeta>, AppError> {
    let path = fragment_root_on(root, fragment).join(FRAGMENT_META_FILE);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path).map_err(|e| io_err(e, "read N1 fragment metadata"))?;
    decode_fragment_meta(&bytes).map(Some)
}

pub fn fragment_meta(fragment: &str) -> Result<Option<FragmentMeta>, AppError> {
    for root in index_roots() {
        if let Some(meta) = read_fragment_meta_from(&root, fragment)? {
            return Ok(Some(meta));
        }
    }
    Ok(None)
}

pub fn sync_fragment_partition_replicas(fragment: &str) -> Result<(), AppError> {
    let _guard = N1_FS_LOCK
        .lock()
        .map_err(|_| AppError::InternalServerError("N1 index sync lock poisoned".to_string()))?;
    let Some((source, source_state)) = choose_replica_source(fragment) else {
        return Ok(());
    };
    ensure_replica_source_unambiguous(fragment, &source, &source_state)?;
    let source_root = source.parent().unwrap_or(&source).to_path_buf();
    let source_state = replica_state(&source_root, fragment)?;
    for dst in fragment_roots(fragment) {
        if dst == source {
            continue;
        }
        let dst_root = dst.parent().unwrap_or(&dst).to_path_buf();
        if dst.exists() && replica_state(&dst_root, fragment)? == source_state {
            continue;
        }
        clear_index_plane(&dst)?;
        copy_dir_contents(&source, &dst)?;
    }
    let _ = cleanup_stale_index_plane_on_blob_only_roots(fragment)?;
    Ok(())
}

pub fn repair_fragment_partition_replicas(
    fragment: &str,
    dry_run: bool,
) -> Result<(u64, u64, u64), AppError> {
    repair_fragment_all_partition_replicas(fragment, dry_run)
}

pub fn repair_fragment_all_partition_replicas(
    fragment: &str,
    dry_run: bool,
) -> Result<(u64, u64, u64), AppError> {
    let _guard = N1_FS_LOCK
        .lock()
        .map_err(|_| AppError::InternalServerError("N1 index repair lock poisoned".to_string()))?;
    let Some((source, mut source_state)) = choose_replica_source(fragment) else {
        return Ok((0, 0, 0));
    };
    if !dry_run {
        ensure_replica_source_unambiguous(fragment, &source, &source_state)?;
        let source_root = source.parent().unwrap_or(&source).to_path_buf();
        source_state = replica_state(&source_root, fragment)?;
    }
    let mut missing = 0;
    let mut divergent = 0;
    let mut corrupt = 0;
    for dst in fragment_roots(fragment) {
        if dst == source {
            continue;
        }
        if !dst.exists() {
            missing += 1;
        } else {
            let dst_root = dst.parent().unwrap_or(&dst).to_path_buf();
            let dst_state = match replica_state(&dst_root, fragment) {
                Ok(state) => state,
                Err(_) => {
                    corrupt += 1;
                    if !dry_run {
                        clear_index_plane(&dst)?;
                        copy_dir_contents(&source, &dst)?;
                    }
                    continue;
                }
            };
            if dst_state == source_state {
                continue;
            }
            if dst_state.score < source_state.score {
                missing += 1;
            } else {
                divergent += 1;
            }
        }
        if !dry_run {
            clear_index_plane(&dst)?;
            copy_dir_contents(&source, &dst)?;
        }
    }
    if !dry_run {
        let _ = cleanup_stale_index_plane_on_blob_only_roots(fragment)?;
    }
    Ok((missing, divergent, corrupt))
}

fn ensure_replica_source_unambiguous(
    fragment: &str,
    source: &Path,
    source_state: &ReplicaState,
) -> Result<(), AppError> {
    let source_root = source.parent().unwrap_or(source);
    let source_objects = collect_symlink_object_ids_under(source_root, fragment, source, true)?;
    for root in index_roots() {
        let fragment_root = fragment_root_on(&root, fragment);
        if !fragment_root.exists() || fragment_root == source {
            continue;
        }
        let Ok(state) = replica_state(&root, fragment) else {
            continue;
        };
        if state.score != source_state.score || state.content_digest == source_state.content_digest
        {
            continue;
        }
        let objects = collect_symlink_object_ids_under(&root, fragment, &fragment_root, true)?;
        if objects != source_objects {
            return Err(AppError::Conflict(format!(
                "N1 namespace replicas for fragment '{fragment}' reference different objects; refusing destructive automatic repair"
            )));
        }
    }
    Ok(())
}

pub(super) fn same_root(left: &Path, right: &Path) -> bool {
    let left = fs::canonicalize(left).unwrap_or_else(|_| left.to_path_buf());
    let right = fs::canonicalize(right).unwrap_or_else(|_| right.to_path_buf());
    left == right
}

pub fn cleanup_stale_index_plane_on_blob_only_roots(fragment: &str) -> Result<usize, AppError> {
    let active_index_roots = index_roots();
    let Some((_source, source_score)) = choose_replica_source(fragment) else {
        return Ok(0);
    };
    if source_score.score.meta_present == 0 && replica_index_records(&source_score.score) == 0 {
        return Ok(0);
    }

    let blob_roots = BLOB_STORE_DISKS
        .get()
        .map(|roots| roots.iter().map(PathBuf::from).collect::<Vec<_>>())
        .unwrap_or_default();
    let mut cleaned = 0usize;
    for blob_root in blob_roots {
        if active_index_roots
            .iter()
            .any(|index_root| same_root(index_root, &blob_root))
        {
            continue;
        }
        let fragment_root = fragment_root_on(&blob_root, fragment);
        if !fragment_root.exists() {
            continue;
        }
        let score = replica_score(&blob_root, fragment);
        if score.meta_present == 0 && replica_index_records(&score) == 0 {
            continue;
        }
        clear_index_plane(&fragment_root)?;
        cleaned += 1;
    }
    Ok(cleaned)
}

fn clear_index_plane(fragment_root: &Path) -> Result<(), AppError> {
    fs::create_dir_all(fragment_root).map_err(|e| io_err(e, "create N1 replica root"))?;
    for entry in fs::read_dir(fragment_root).map_err(|e| io_err(e, "scan N1 replica root"))? {
        let entry = entry.map_err(|e| io_err(e, "scan N1 replica root entry"))?;
        let name = entry.file_name();
        if matches!(name.to_str(), Some(OBJECTS_DIR)) {
            continue;
        }
        let path = entry.path();
        let meta =
            fs::symlink_metadata(&path).map_err(|e| io_err(e, "inspect N1 replica target"))?;
        if meta.file_type().is_symlink() || meta.is_file() {
            fs::remove_file(&path).map_err(|e| io_err(e, "clear N1 replica file"))?;
        } else if meta.is_dir() {
            fs::remove_dir_all(&path).map_err(|e| io_err(e, "clear N1 replica dir"))?;
        }
    }
    Ok(())
}

fn copy_dir_contents(src: &Path, dst: &Path) -> Result<(), AppError> {
    fs::create_dir_all(dst).map_err(|e| io_err(e, "create N1 replica dir"))?;
    for entry in fs::read_dir(src).map_err(|e| io_err(e, "scan N1 replica source"))? {
        let entry = entry.map_err(|e| io_err(e, "scan N1 replica entry"))?;
        if matches!(entry.file_name().to_str(), Some(OBJECTS_DIR)) {
            continue;
        }
        let source = entry.path();
        let target = dst.join(entry.file_name());
        let meta =
            fs::symlink_metadata(&source).map_err(|e| io_err(e, "inspect N1 replica source"))?;
        if meta.file_type().is_symlink() {
            if fs::symlink_metadata(&target).is_ok() {
                let _ = fs::remove_file(&target);
            }
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).map_err(|e| io_err(e, "create N1 replica parent"))?;
            }
            let link_target =
                fs::read_link(&source).map_err(|e| io_err(e, "read N1 replica symlink"))?;
            unix_fs::symlink(link_target, &target)
                .map_err(|e| io_err(e, "copy N1 replica symlink"))?;
        } else if meta.is_dir() {
            copy_dir_contents(&source, &target)?;
        } else {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).map_err(|e| io_err(e, "create N1 replica parent"))?;
            }
            fs::copy(&source, &target).map_err(|e| io_err(e, "copy N1 replica file"))?;
        }
    }
    Ok(())
}
