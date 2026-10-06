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

#[cfg(test)]
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

#[cfg(test)]
fn count_namespace_links(root: &Path, fragment: &str) -> u64 {
    if !crate::core::storage_pool::configured().is_empty() {
        return 0;
    }
    collect_symlink_object_ids_under(root, fragment, &fragment_root_on(root, fragment), true)
        .map(|ids| ids.len() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
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

#[cfg(test)]
fn add_digest_mod_256(accumulator: &mut [u8; 32], digest: &[u8; 32]) {
    let mut carry = 0u16;
    for index in (0..32).rev() {
        let sum = u16::from(accumulator[index]) + u16::from(digest[index]) + carry;
        accumulator[index] = sum as u8;
        carry = sum >> 8;
    }
}

#[cfg(test)]
fn is_replica_staging_name(name: &str) -> bool {
    name.starts_with('.')
        && name
            .rsplit_once(".tmp.")
            .is_some_and(|(_, nonce)| nonce.len() == 26 && Ulid::from_string(nonce).is_ok())
}

#[cfg(test)]
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
                OBJECTS_DIR | TMP_DIR | LOCKS_DIR | GC_DIR | REPAIR_DIR | ".namespace" | ".trash"
            ))
}

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
fn replica_index_records(score: &ReplicaScore) -> u64 {
    score
        .object_records
        .saturating_add(score.namespace_links)
        .saturating_add(score.upload_records)
}

#[cfg(test)]
pub(super) fn mtime_nanos(path: &Path) -> u128 {
    fs::symlink_metadata(path)
        .and_then(|meta| meta.modified())
        .ok()
        .and_then(|mtime| mtime.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}

#[cfg(test)]
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

#[cfg(test)]
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
    let (missing, divergent, corrupt) = match crate::namespace::flat::repair(fragment, true) {
        Ok(counts) => counts,
        Err(_) => (0, 0, 1),
    };
    ReplicaInspection {
        expected_replicas: roots.len() as u64,
        present_replicas: roots
            .iter()
            .filter(|r| fragment_root_on(r, fragment).is_dir())
            .count() as u64,
        valid_replicas: (roots.len() as u64).saturating_sub(missing + divergent + corrupt),
        missing_replicas: missing,
        divergent_replicas: divergent,
        corrupt_replicas: corrupt,
        healthy: missing == 0 && divergent == 0 && corrupt == 0,
        canonical_path: None,
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
    repair_fragment_all_partition_replicas(fragment, false)?;
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
    // No whole-fragment copying: object versions, multipart sessions and local
    // operation workspaces are not global namespace replicas.
    crate::namespace::flat::repair(fragment, dry_run)
}

#[cfg(test)]
pub(super) fn same_root(left: &Path, right: &Path) -> bool {
    let left = fs::canonicalize(left).unwrap_or_else(|_| left.to_path_buf());
    let right = fs::canonicalize(right).unwrap_or_else(|_| right.to_path_buf());
    left == right
}

#[cfg(test)]
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

#[cfg(test)]
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
#[cfg(test)]
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

// A durable upload can finish and remove its session directory after readdir
// but before repair opens/copies that entry. Retry from the same source, never
// promote the partially copied destination to authority. Missing roots and
// non-ENOENT I/O failures remain real failures, not concurrency conflicts.
#[cfg(test)]
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
