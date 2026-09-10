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
            } else if file_type.is_file()
                && !is_replica_staging_name(&entry.file_name().to_string_lossy())
                && !entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(REPLICA_TMP_PREFIX)
            {
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

fn is_replica_staging_name(name: &str) -> bool {
    name.starts_with('.')
        && name
            .rsplit_once(".tmp.")
            .is_some_and(|(_, nonce)| nonce.len() == 26 && Ulid::from_string(nonce).is_ok())
}

fn replica_local_entry(path: &Path, name: &str, at_fragment_root: bool) -> bool {
    // User objects are symlinks, and user directories may legitimately have a
    // name resembling an atomic-write sibling. Only regular-file staging and
    // our reserved repair prefix belong to the writer, not the namespace.
    name.starts_with(REPLICA_TMP_PREFIX)
        || (is_replica_staging_name(name)
            && match fs::symlink_metadata(path) {
                Ok(metadata) => metadata.is_file(),
                // The owning writer may have completed its rename since readdir.
                Err(error) => error.kind() == std::io::ErrorKind::NotFound,
            })
        || (at_fragment_root
            && matches!(
                name,
                OBJECTS_DIR | TMP_DIR | LOCKS_DIR | GC_DIR | REPAIR_DIR
            ))
}

fn hash_replica_tree(fragment_root: &Path, directory: &Path) -> Result<[u8; 32], AppError> {
    let mut sum = [0u8; 32];
    let mut xor = [0u8; 32];
    let mut entry_count = 0u64;
    visit_dir_names(directory, |name| {
        if replica_local_entry(&directory.join(&name), &name, directory == fragment_root) {
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
        if replica_local_entry(&entry.path(), &name, false) {
            continue;
        }
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
        if replica_local_entry(&entry.path(), &name.to_string_lossy(), true) {
            continue;
        }
        remove_stale_replica_entry(&entry.path())?;
    }
    linux_io::sync_dir_blocking(fragment_root).map_err(|e| io_err(e, "sync cleared N1 replica"))?;
    Ok(())
}

// Never recursively unlink a parent: an atomic writer can still own a temporary
// sibling underneath it. This also preserves unrecognised non-file entries.
fn remove_stale_replica_entry(path: &Path) -> Result<(), AppError> {
    let meta = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(io_err(err, "inspect stale N1 replica")),
    };
    if replica_local_entry(
        path,
        &path.file_name().unwrap_or_default().to_string_lossy(),
        false,
    ) {
        return Ok(());
    }
    if meta.is_dir() {
        for entry in fs::read_dir(path).map_err(|e| io_err(e, "scan stale N1 replica dir"))? {
            remove_stale_replica_entry(
                &entry
                    .map_err(|e| io_err(e, "scan stale N1 replica"))?
                    .path(),
            )?;
        }
        match fs::remove_dir(path) {
            Ok(()) => (),
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::DirectoryNotEmpty | std::io::ErrorKind::NotFound
                ) =>
            {
                ()
            }
            Err(err) => return Err(io_err(err, "remove empty N1 replica dir")),
        }
    } else if meta.is_file() || meta.file_type().is_symlink() {
        match fs::remove_file(path) {
            Ok(()) => (),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => (),
            Err(err) => return Err(io_err(err, "remove stale N1 replica file")),
        }
    }
    Ok(())
}

fn copy_dir_contents(src: &Path, dst: &Path) -> Result<(), AppError> {
    retry_changed_replica_snapshot(src, dst, || reconcile_replica_directory(src, dst, true))
}

// A durable upload can finish and remove its session directory after readdir
// but before repair opens/copies that entry. Retry from the same source, never
// promote the partially copied destination to authority. Missing roots and
// non-ENOENT I/O failures remain real failures, not concurrency conflicts.
fn retry_changed_replica_snapshot(
    src: &Path,
    dst: &Path,
    mut reconcile: impl FnMut() -> Result<(), AppError>,
) -> Result<(), AppError> {
    for attempt in 0..3 {
        match reconcile() {
            Err(AppError::Io(error, _))
                if error.kind() == std::io::ErrorKind::NotFound
                    && fs::symlink_metadata(src).is_ok_and(|metadata| metadata.is_dir())
                    && fs::symlink_metadata(dst).is_ok_and(|metadata| metadata.is_dir()) =>
            {
                if attempt == 2 {
                    return Err(AppError::Conflict(
                        "N1 replica contents changed during online repair; retry the scan".into(),
                    ));
                }
            }
            result => return result,
        }
    }
    unreachable!("the final changed-snapshot attempt returns a conflict")
}

fn reconcile_replica_directory(
    src: &Path,
    dst: &Path,
    at_fragment_root: bool,
) -> Result<(), AppError> {
    if let Ok(meta) = fs::symlink_metadata(dst) {
        if !meta.is_dir() {
            return Err(AppError::Conflict(format!(
                "N1 replica directory conflicts with {}",
                dst.display()
            )));
        }
    }
    fs::create_dir_all(dst).map_err(|e| io_err(e, "create N1 replica dir"))?;
    for entry in fs::read_dir(src).map_err(|e| io_err(e, "scan N1 replica source"))? {
        let entry = entry.map_err(|e| io_err(e, "scan N1 replica entry"))?;
        if replica_local_entry(
            &entry.path(),
            &entry.file_name().to_string_lossy(),
            at_fragment_root,
        ) {
            continue;
        }
        let source = entry.path();
        let target = dst.join(entry.file_name());
        let meta =
            fs::symlink_metadata(&source).map_err(|e| io_err(e, "inspect N1 replica source"))?;
        if meta.is_dir() {
            reconcile_replica_directory(&source, &target, false)?;
        } else if meta.file_type().is_symlink() || meta.is_file() {
            // Publish a complete sibling without truncating the destination or
            // introducing a missing-file window. Payload memory stays bounded.
            let temporary = target.with_file_name(format!("{REPLICA_TMP_PREFIX}{}", Ulid::new()));
            let result = (|| -> std::io::Result<()> {
                if meta.file_type().is_symlink() {
                    unix_fs::symlink(fs::read_link(&source)?, &temporary)?;
                } else {
                    let mut input = File::open(&source)?;
                    let mut output = fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&temporary)?;
                    std::io::copy(&mut input, &mut output)?;
                    output.sync_all()?;
                }
                fs::rename(&temporary, &target)?;
                linux_io::sync_dir_blocking(dst)
            })();
            if let Err(err) = result {
                let _ = fs::remove_file(&temporary);
                return Err(io_err(
                    err,
                    &format!(
                        "publish N1 replica entry {} -> {}",
                        source.display(),
                        target.display()
                    ),
                ));
            }
        } else {
            return Err(AppError::Conflict(format!(
                "unsupported N1 replica entry {}",
                source.display()
            )));
        }
    }
    for entry in fs::read_dir(dst).map_err(|e| io_err(e, "scan N1 replica destination"))? {
        let entry = entry.map_err(|e| io_err(e, "scan N1 replica destination entry"))?;
        let name = entry.file_name();
        if replica_local_entry(&entry.path(), &name.to_string_lossy(), at_fragment_root) {
            continue;
        }
        match fs::symlink_metadata(src.join(&name)) {
            Ok(_) => (),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                remove_stale_replica_entry(&entry.path())?;
            }
            Err(err) => return Err(io_err(err, "inspect N1 replica source before pruning")),
        }
    }
    linux_io::sync_dir_blocking(dst).map_err(|e| io_err(e, "sync N1 replica directory"))?;
    Ok(())
}

#[cfg(test)]
mod snapshot_retry_tests {
    use super::*;

    #[test]
    fn changed_children_retry_but_lost_roots_and_hard_io_errors_do_not() {
        let base = std::env::temp_dir().join(format!("n1-replica-retry-{}", Ulid::new()));
        let source = base.join("source");
        let target = base.join("target");
        fs::create_dir_all(&source).unwrap();
        fs::create_dir_all(&target).unwrap();
        let missing = || {
            AppError::Io(
                std::io::Error::from(std::io::ErrorKind::NotFound),
                "test child disappeared".into(),
            )
        };
        let mut calls = 0;
        retry_changed_replica_snapshot(&source, &target, || {
            calls += 1;
            if calls == 1 { Err(missing()) } else { Ok(()) }
        })
        .unwrap();
        assert_eq!(calls, 2);
        calls = 0;
        assert!(matches!(
            retry_changed_replica_snapshot(&source, &target, || {
                calls += 1;
                Err(missing())
            }),
            Err(AppError::Conflict(_))
        ));
        assert_eq!(calls, 3, "online retry work must stay bounded");
        calls = 0;
        assert!(matches!(
            retry_changed_replica_snapshot(&source, &target, || {
                calls += 1;
                Err(AppError::Io(
                    std::io::Error::from_raw_os_error(5),
                    "test EIO".into(),
                ))
            }),
            Err(AppError::Io(_, _))
        ));
        assert_eq!(calls, 1);
        fs::remove_dir(&target).unwrap();
        calls = 0;
        assert!(matches!(
            retry_changed_replica_snapshot(&source, &target, || {
                calls += 1;
                Err(missing())
            }),
            Err(AppError::Io(_, _))
        ));
        assert_eq!(calls, 1, "root loss must retain the safety fault");
        fs::remove_dir_all(base).unwrap();
    }
}
