//! TTL trash catalog, recovery, and physical reclaim operations.

use super::*;

pub fn publish_trash_version_for_maintenance(metadata: &ObjectMetadata) -> Result<(), AppError> {
    write_metadata_to_roots(metadata)
}

pub fn read_trash_version(
    fragment: &str,
    object_id: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let mut paths = Vec::new();
    for dir in object_dir_candidates(fragment, object_id)? {
        if let Some(path) = ndx_path_for_version(&dir, version_id)? {
            paths.push(path);
        }
    }
    read_authoritative_object_metadata(paths, object_id)
}

pub fn read_latest_trash(
    fragment: &str,
    object_key: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let versions = list_trash_versions(fragment, object_key)?;
    Ok(versions
        .into_iter()
        .max_by(|a, b| a.version_id.cmp(&b.version_id)))
}

pub fn list_trash_versions(
    fragment: &str,
    object_id: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for dir in object_dir_candidates(fragment, object_id)? {
        if !dir.is_dir() {
            continue;
        }
        for name in read_dir_names(&dir)? {
            if parse_ndx_file_name(&name).is_some() {
                let meta = decode_metadata_file(&dir.join(name))?;
                if seen.insert(meta.version_id.clone()) {
                    out.push(meta);
                }
            }
        }
    }
    out.sort_by(|a, b| a.version_id.cmp(&b.version_id));
    Ok(out)
}

pub(crate) fn remove_trash_payload(fragment: &str, record: &TrashRecord) -> Result<bool, AppError> {
    let mut removed_primary = false;
    for root in index_roots() {
        let payload = fragment_root_on(&root, fragment)
            .join(TRASH_DIR)
            .join(&record.source_relative_path);
        let meta = match fs::symlink_metadata(&payload) {
            Ok(meta) => meta,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound && !removed_primary => {
                return Ok(false);
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(io_err(err, "inspect N1 expired trash payload")),
        };
        if meta.file_type().is_symlink() || meta.is_file() {
            match fs::remove_file(&payload) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound && !removed_primary => {
                    return Ok(false);
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(io_err(err, "remove N1 expired trash file")),
            }
        } else if meta.is_dir() {
            match fs::remove_dir_all(&payload) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound && !removed_primary => {
                    return Ok(false);
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(io_err(err, "remove N1 expired trash directory")),
            }
        }
        let trash_root = fragment_root_on(&root, fragment).join(TRASH_DIR);
        let mut parent = payload.parent();
        while let Some(directory) = parent {
            if directory == trash_root {
                break;
            }
            match fs::remove_dir(directory) {
                Ok(()) => parent = directory.parent(),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    parent = directory.parent();
                }
                Err(err) if err.kind() == std::io::ErrorKind::DirectoryNotEmpty => break,
                Err(err) => return Err(io_err(err, "prune N1 trash generation")),
            }
        }
        removed_primary = true;
    }
    Ok(removed_primary)
}

fn trash_generation_anchor(
    generation_root: &Path,
    path_depth: usize,
) -> Result<(PathBuf, String), AppError> {
    let mut current = generation_root.to_path_buf();
    let mut components = Vec::with_capacity(path_depth);
    for position in 0..path_depth {
        let names = read_dir_names(&current)?;
        if names.is_empty() {
            // Trash publication builds the parent chain before moving its
            // anchor; recovery removes the anchor before pruning that chain.
            // An empty intermediate directory is not a committed trash item.
            return Err(AppError::NotFound);
        }
        if names.len() != 1 {
            return Err(AppError::InternalServerError(format!(
                "N1 trash generation '{}' has {} entries at logical path depth {}; expected exactly one",
                generation_root.display(),
                names.len(),
                position
            )));
        }
        let name = names.into_iter().next().expect("one trash path component");
        components.push(name.clone());
        current = current.join(name);
        if position + 1 < path_depth {
            match fs::symlink_metadata(&current) {
                Ok(metadata) if metadata.is_dir() => (),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(AppError::NotFound);
                }
                Err(error) => return Err(io_err(error, "inspect N1 trash parent")),
                Ok(_) => {
                    return Err(AppError::InternalServerError(format!(
                        "N1 trash generation '{}' ends before its declared logical path depth {}",
                        generation_root.display(),
                        path_depth
                    )));
                }
            }
        }
    }
    Ok((current, components.join("/")))
}

fn collect_trash_object_refs(
    index_root: &Path,
    fragment: &str,
    anchor: &Path,
) -> Result<(Vec<String>, Vec<String>), AppError> {
    let mut object_keys = BTreeSet::new();
    let mut version_ids = BTreeSet::new();
    let mut stack = vec![anchor.to_path_buf()];
    while let Some(directory) = stack.pop() {
        for name in read_dir_names(&directory)? {
            let child = directory.join(name);
            if let Some(object_id) = read_namespace_symlink_object_id(index_root, fragment, &child)?
            {
                if object_keys.insert(object_id.clone()) {
                    for metadata in list_live_versions(fragment, &object_id)? {
                        version_ids.insert(metadata.version_id);
                    }
                }
            } else if child.is_dir() {
                stack.push(child);
            }
        }
    }
    Ok((
        object_keys.into_iter().collect(),
        version_ids.into_iter().collect(),
    ))
}

pub(crate) fn visit_trash_records(
    fragment: &str,
    mut visit: impl FnMut(TrashRecord) -> Result<bool, AppError>,
) -> Result<(), AppError> {
    for fragment_root in existing_fragment_roots(fragment) {
        let index_root = fragment_root
            .parent()
            .unwrap_or(&fragment_root)
            .to_path_buf();
        let trash_root = fragment_root.join(TRASH_DIR);
        if !trash_root.is_dir() {
            continue;
        }
        'generation: for generation in read_dir_names(&trash_root)? {
            let Some((unix_nanos, path_depth)) = parse_trash_generation_name(&generation) else {
                return Err(AppError::InternalServerError(format!(
                    "N1 trash entry '{}' uses the pre-3.3 layout; migrate it before starting N1 3.3",
                    trash_root.join(&generation).display()
                )));
            };
            let generation_root = trash_root.join(&generation);
            match fs::symlink_metadata(&generation_root) {
                Ok(metadata) if metadata.is_dir() => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Ok(_) => {
                    return Err(AppError::InternalServerError(format!(
                        "N1 trash generation '{}' is not a directory",
                        generation_root.display()
                    )));
                }
                Err(error) => {
                    return Err(io_err(error, "inspect N1 trash generation"));
                }
            }
            let (anchor, original_path) =
                match trash_generation_anchor(&generation_root, path_depth) {
                    Ok(anchor) => anchor,
                    Err(AppError::NotFound) => continue,
                    Err(error) => {
                        match fs::symlink_metadata(&generation_root) {
                            Err(missing) if missing.kind() == std::io::ErrorKind::NotFound => {
                                continue;
                            }
                            Ok(metadata)
                                if metadata.is_dir()
                                    && read_dir_names(&generation_root)
                                        .is_ok_and(|names| names.is_empty()) =>
                            {
                                // A concurrent recover/TTL removal has already moved the
                                // only anchor and is pruning this now-empty generation.
                                continue;
                            }
                            _ => return Err(error),
                        }
                    }
                };
            let metadata = match fs::symlink_metadata(&anchor) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(io_err(error, "inspect N1 trash generation anchor")),
            };
            let (kind, object_keys, version_ids) = if let Some(object_id) =
                read_namespace_symlink_object_id(&index_root, fragment, &anchor)?
            {
                let Some(versions) = while_trash_anchor_present(&anchor, || {
                    list_live_versions(fragment, &object_id)
                })?
                else {
                    continue 'generation;
                };
                let mut versions = versions
                    .into_iter()
                    .map(|metadata| metadata.version_id)
                    .collect::<Vec<_>>();
                versions.sort();
                versions.dedup();
                (TrashKind::File, vec![object_id], versions)
            } else if metadata.is_dir() {
                let Some((objects, versions)) = while_trash_anchor_present(&anchor, || {
                    collect_trash_object_refs(&index_root, fragment, &anchor)
                })?
                else {
                    continue 'generation;
                };
                (TrashKind::Directory, objects, versions)
            } else if metadata.is_file() {
                (TrashKind::File, Vec::new(), Vec::new())
            } else {
                if matches!(
                    fs::symlink_metadata(&anchor),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound
                ) {
                    // The anchor was a namespace symlink when it was first
                    // inspected, but a concurrent recover/TTL sweep removed
                    // it before its target could be read. Treat that as the
                    // generation disappearing, not as malformed metadata.
                    continue 'generation;
                }
                return Err(AppError::InternalServerError(format!(
                    "N1 trash generation anchor '{}' has an unsupported file type",
                    anchor.display()
                )));
            };
            let source_relative_path = format!("{generation}/{original_path}");
            let record = TrashRecord {
                trash_entry_id: source_relative_path.clone(),
                original_path,
                source_relative_path,
                kind,
                object_keys,
                version_ids,
                deleted_at_unix: unix_nanos / 1_000_000_000,
            };
            if !visit(record)? {
                return Ok(());
            }
        }
        break;
    }
    Ok(())
}

fn while_trash_anchor_present<T>(
    anchor: &Path,
    read: impl FnOnce() -> Result<T, AppError>,
) -> Result<Option<T>, AppError> {
    match read() {
        Ok(value) => Ok(Some(value)),
        Err(error) => {
            let missing = matches!(&error, AppError::NotFound)
                || matches!(&error, AppError::Io(io, _) if io.kind() == std::io::ErrorKind::NotFound);
            // TTL can unlink the anchor and reclaim NDX between enumeration
            // and decoding. Only that confirmed disappearance is benign;
            // missing metadata for a retained anchor, EIO and corruption fail.
            if missing
                && matches!(fs::symlink_metadata(anchor), Err(io) if io.kind() == std::io::ErrorKind::NotFound)
            {
                Ok(None)
            } else {
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod race_tests {
    use super::*;

    #[test]
    fn metadata_disappearing_after_ttl_unlinks_anchor_is_not_a_broken_record() {
        let root = std::env::temp_dir().join(format!("trash-read-race-{}", ulid::Ulid::new()));
        fs::create_dir_all(&root).unwrap();
        let anchor = root.join("anchor");
        let metadata = root.join("object.ndx");
        fs::write(&anchor, b"anchor").unwrap();
        fs::write(&metadata, b"fixture").unwrap();
        let result = while_trash_anchor_present(&anchor, || {
            fs::remove_file(&anchor).unwrap();
            fs::remove_file(&metadata).unwrap();
            fs::read(&metadata).map_err(|io| io_err(io, "read N1 object metadata"))
        });
        assert!(matches!(result, Ok(None)));
        fs::write(&anchor, b"still committed").unwrap();
        assert!(
            while_trash_anchor_present(&anchor, || {
                fs::read(&metadata).map_err(|io| io_err(io, "read N1 object metadata"))
            })
            .is_err()
        );
        fs::remove_file(&anchor).unwrap();
        assert!(
            while_trash_anchor_present::<()>(&anchor, || {
                Err(io_err(
                    std::io::Error::from_raw_os_error(libc::EIO),
                    "injected hard I/O failure",
                ))
            })
            .is_err()
        );
        fs::remove_dir_all(root).unwrap();
    }
}

pub(crate) fn scan_trash_records(fragment: &str) -> Result<Vec<TrashRecord>, AppError> {
    let mut out = Vec::new();
    visit_trash_records(fragment, |record| {
        out.push(record);
        Ok(true)
    })?;
    out.sort_by(|a, b| {
        a.original_path
            .cmp(&b.original_path)
            .then_with(|| a.deleted_at_unix.cmp(&b.deleted_at_unix))
            .then_with(|| a.trash_entry_id.cmp(&b.trash_entry_id))
    });
    Ok(out)
}

pub fn list_trash_heads(fragment: &str) -> Result<Vec<ObjectMetadata>, AppError> {
    let mut out = Vec::new();
    for record in scan_trash_records(fragment)? {
        for object_key in &record.object_keys {
            if let Some(mut meta) = read_latest_trash(fragment, object_key)? {
                meta.object_key = record.original_path.clone();
                out.push(meta);
            }
        }
    }
    Ok(out)
}

pub fn list_trash_catalog(fragment: &str) -> Result<Vec<TrashCatalogEntry>, AppError> {
    Ok(scan_trash_records(fragment)?
        .into_iter()
        .flat_map(|record| {
            record
                .object_keys
                .into_iter()
                .map(move |object_id| TrashCatalogEntry {
                    object_key: object_id,
                    version_ids: record.version_ids.clone(),
                    deleted_at: record.deleted_at_unix,
                })
        })
        .collect())
}

fn scan_trash_page(
    fragment: &str,
    prefix: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
) -> Result<ListObjectsResult, AppError> {
    let prefix = prefix
        .map(|prefix| normalize_object_key(&prefix))
        .transpose()?
        .unwrap_or_default();
    let limit = limit.max(1);
    let mut by_record = BTreeMap::<String, (IndexListEntry, Option<ObjectMetadata>)>::new();

    for record in scan_trash_records(fragment)? {
        let relative = if prefix.is_empty() {
            record.original_path.as_str()
        } else if record.original_path == prefix {
            if record.kind == TrashKind::Directory {
                continue;
            }
            record.original_path.as_str()
        } else if let Some(rest) = record
            .original_path
            .strip_prefix(prefix.as_str())
            .and_then(|rest| rest.strip_prefix('/'))
        {
            rest
        } else {
            continue;
        };

        if relative.is_empty() {
            continue;
        }

        let (name, is_nested) = relative
            .split_once('/')
            .map(|(head, _)| (head.to_string(), true))
            .unwrap_or_else(|| (relative.to_string(), false));

        if continuation_token
            .as_ref()
            .is_some_and(|after| name.as_str() <= after.as_str())
        {
            continue;
        }

        if is_nested || record.kind == TrashKind::Directory {
            by_record
                .entry(name.clone())
                .or_insert((directory_entry(name), None));
            if by_record.len() > limit.saturating_add(1) {
                by_record.pop_last();
            }
            continue;
        }

        let version_id = record.version_ids.last().cloned().unwrap_or_default();
        let metadata = ObjectMetadata {
            object_key: record.original_path.clone(),
            fragment: fragment.to_string(),
            version_id,
            upload_session_id: String::new(),
            owner_uid: fragment.to_string(),
            content_type: "application/octet-stream".to_string(),
            total_size_bytes: 0,
            created_at: record.deleted_at_unix,
            data_shards: 0,
            parity_shards: 0,
            placement_generation: 0,
            parts: Vec::new(),
            is_delete_marker: false,
            stripe_size_bytes: crate::core::disk_manager::default_stripe_size(),
        };
        let entry = IndexListEntry {
            record: name.clone(),
            kind: "file".to_string(),
            size_bytes: metadata.total_size_bytes,
            created_at_unix_nanos: metadata.created_at.saturating_mul(1_000_000_000),
            packed_ext_u64: Some(packed_extension_key(&metadata.object_key)),
            version_ulid: Some(metadata.version_id.clone()),
        };
        by_record.entry(name).or_insert((entry, Some(metadata)));
        if by_record.len() > limit.saturating_add(1) {
            by_record.pop_last();
        }
    }

    let has_more = by_record.len() > limit;
    if has_more {
        by_record.pop_last();
    }
    let page = by_record.into_iter().collect::<Vec<_>>();
    let continuation = has_more
        .then(|| page.last().map(|(record, _)| record.clone()))
        .flatten();
    let entries = page
        .iter()
        .map(|(_, (entry, _))| entry.clone())
        .collect::<Vec<_>>();
    let mut objects = page
        .into_iter()
        .filter_map(|(_, (_, meta))| meta)
        .collect::<Vec<_>>();
    objects.sort_by(|a, b| a.object_key.cmp(&b.object_key));
    Ok(ListObjectsResult {
        prefix: Some(prefix),
        limit: Some(limit),
        entries,
        objects,
        common_prefixes: Vec::new(),
        continuation_token: continuation.clone(),
        next_continuation_token: continuation,
    })
}

fn scan_trash_maintenance_page(
    fragment: &str,
    limit: usize,
    continuation_token: Option<String>,
) -> Result<ListObjectsResult, AppError> {
    let limit = limit.max(1);
    let after = continuation_token
        .as_deref()
        .map(|token| {
            token.split_once(':').ok_or_else(|| {
                AppError::BadRequest("invalid N1 trash maintenance continuation token".to_string())
            })
        })
        .transpose()?;
    let mut candidates = Vec::<(String, ObjectMetadata)>::with_capacity(limit.saturating_add(1));

    'records: for record in scan_trash_records(fragment)? {
        if after.is_some_and(|(record_id, _)| record.trash_entry_id.as_str() < record_id) {
            continue;
        }
        for object_id in &record.object_keys {
            if after.is_some_and(|(record_id, after_object_id)| {
                record.trash_entry_id == record_id && object_id.as_str() <= after_object_id
            }) {
                continue;
            }
            let Some(metadata) = read_latest_trash(fragment, object_id)? else {
                continue;
            };
            let token = format!("{}:{object_id}", record.trash_entry_id);
            candidates.push((token, metadata));
            if candidates.len() > limit {
                break 'records;
            }
        }
    }

    let continuation = if candidates.len() > limit {
        candidates.truncate(limit);
        candidates.last().map(|(token, _)| token.clone())
    } else {
        None
    };
    let objects = candidates
        .into_iter()
        .map(|(_, metadata)| metadata)
        .collect();
    Ok(ListObjectsResult {
        prefix: None,
        limit: Some(limit),
        entries: Vec::new(),
        objects,
        common_prefixes: Vec::new(),
        continuation_token: continuation.clone(),
        next_continuation_token: continuation,
    })
}

pub fn scan_trash_heads_page(
    fragment: &str,
    prefix: Option<String>,
    _delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    _include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    scan_trash_page(fragment, prefix, limit, continuation_token)
}

pub fn scan_trash_full_metadata_page(
    fragment: &str,
    _prefix: Option<String>,
    _delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    _include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    scan_trash_maintenance_page(fragment, limit, continuation_token)
}

pub fn list_object_keys_with_trash_versions(fragment: &str) -> Result<Vec<String>, AppError> {
    Ok(list_trash_catalog(fragment)?
        .into_iter()
        .map(|entry| entry.object_key)
        .collect())
}

pub(super) fn collect_symlink_object_ids_under(
    root: &Path,
    fragment: &str,
    base: &Path,
    include_trash_root: bool,
) -> Result<BTreeSet<String>, AppError> {
    let mut out = BTreeSet::new();
    let mut stack = vec![base.to_path_buf()];
    while let Some(dir) = stack.pop() {
        if !dir.is_dir() {
            continue;
        }
        for name in read_dir_names(&dir)? {
            if internal_namespace_entry_name(&name) {
                if !(include_trash_root && name == TRASH_DIR) {
                    continue;
                }
            }
            let child = dir.join(&name);
            if let Some(object_id) = read_namespace_symlink_object_id(root, fragment, &child)? {
                out.insert(object_id);
            } else if child.is_dir() {
                stack.push(child);
            }
        }
    }
    Ok(out)
}

pub(crate) fn referenced_object_ids(fragment: &str) -> Result<BTreeSet<String>, AppError> {
    let mut out = BTreeSet::new();
    for fragment_root in existing_fragment_roots(fragment) {
        let index_root = fragment_root
            .parent()
            .unwrap_or(&fragment_root)
            .to_path_buf();
        out.extend(collect_symlink_object_ids_under(
            &index_root,
            fragment,
            &fragment_root,
            true,
        )?);
    }
    Ok(out)
}

pub(crate) fn delete_unreferenced_object_index(
    fragment: &str,
    object_id: &str,
) -> Result<bool, AppError> {
    Ok(!delete_unreferenced_object_indexes_batch(fragment, &[object_id.to_owned()])?.is_empty())
}

pub(crate) fn delete_unreferenced_object_indexes_batch(
    fragment: &str,
    candidates: &[String],
) -> Result<Vec<String>, AppError> {
    // Start tracking before either snapshot. The last trash anchor can expire
    // while a previously initialized upload still owns this physical object.
    let upload_scan = crate::core::upload::UploadGcScan::begin(fragment)?;
    let mut pending = crate::core::upload::standard_upload_object_ids(fragment)?;
    for session in list_active_sessions(fragment)? {
        pending.insert(normalize_object_id(&session.manifest.object_key)?);
    }
    let referenced = referenced_object_ids(fragment)?;
    let mut removed = Vec::new();
    for object_id in candidates.iter().collect::<BTreeSet<_>>() {
        let object_id = normalize_object_id(object_id)?;
        if referenced.contains(&object_id) || pending.contains(&object_id) {
            continue;
        }
        let Some(_reclaim_guard) = upload_scan.try_reclaim(&object_id)? else {
            continue;
        };
        let mut object_removed = false;
        for dir in object_dir_candidates(fragment, &object_id)? {
            if !dir.exists() {
                continue;
            }
            fs::remove_dir_all(&dir).map_err(|e| io_err(e, "reclaim N1 expired trash object"))?;
            object_removed = true;
            if let Some(parent) = dir.parent() {
                sync_dir(parent);
            }
        }
        if object_removed {
            removed.push(object_id);
        }
    }
    Ok(removed)
}

pub fn list_active_trash_partition_ids(_fragment: &str) -> Result<BTreeSet<u16>, AppError> {
    Ok(BTreeSet::from([0]))
}

pub fn list_partition_trash_entries(
    fragment: &str,
    _partition_id: u16,
) -> Result<HashMap<String, Vec<String>>, AppError> {
    Ok(list_trash_catalog(fragment)?
        .into_iter()
        .map(|entry| (entry.object_key, entry.version_ids))
        .collect())
}

pub fn list_trashed_object_version_ids_batch(
    fragment: &str,
    object_keys: &[String],
) -> Result<HashMap<String, Vec<String>>, AppError> {
    let records = scan_trash_records(fragment)?;
    let mut out = HashMap::new();
    for key in object_keys {
        let versions = records
            .iter()
            .find(|record| {
                record.original_path == *key || record.object_keys.iter().any(|id| id == key)
            })
            .map(|record| record.version_ids.clone())
            .unwrap_or_default();
        out.insert(key.clone(), versions);
    }
    Ok(out)
}

pub fn move_object_version_to_trash_with_metadata(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    read_live_version(fragment, object_key, version_id)
}

pub fn restore_object_version_from_trash_with_metadata(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    read_trash_version(fragment, object_key, version_id)
}

pub fn move_object_to_trash_fast(
    fragment: &str,
    object_key: &str,
) -> Result<Option<Vec<String>>, AppError> {
    let versions = list_live_versions(fragment, object_key)?;
    if versions.is_empty() {
        return Ok(None);
    }
    let ids = versions
        .iter()
        .map(|meta| meta.version_id.clone())
        .collect::<Vec<_>>();
    Ok(Some(ids))
}

pub fn move_objects_to_trash_batch(
    fragment: &str,
    object_keys: &[String],
) -> Result<HashMap<String, Vec<String>>, AppError> {
    let mut out = HashMap::new();
    for key in object_keys {
        if let Some(ids) = move_object_to_trash_fast(fragment, key)? {
            out.insert(key.clone(), ids);
        }
    }
    Ok(out)
}

pub fn restore_object_from_trash_fast(
    fragment: &str,
    object_key: &str,
) -> Result<Option<Vec<String>>, AppError> {
    let versions = list_trash_versions(fragment, object_key)?;
    if versions.is_empty() {
        return Ok(None);
    }
    let ids = versions
        .iter()
        .map(|meta| meta.version_id.clone())
        .collect::<Vec<_>>();
    Ok(Some(ids))
}

pub fn restore_objects_from_trash_batch(
    fragment: &str,
    object_keys: &[String],
) -> Result<HashMap<String, Vec<String>>, AppError> {
    let mut out = HashMap::new();
    for key in object_keys {
        if let Some(ids) = restore_object_from_trash_fast(fragment, key)? {
            out.insert(key.clone(), ids);
        }
    }
    Ok(out)
}

pub fn restore_objects_from_trash_manifest_batch(
    fragment: &str,
    object_versions: &HashMap<String, Vec<String>>,
) -> Result<HashMap<String, Vec<String>>, AppError> {
    let mut out = HashMap::new();
    let records = scan_trash_records(fragment)?;
    for (key, versions) in object_versions {
        let object_ids = records
            .iter()
            .find(|record| {
                record.original_path == *key || record.object_keys.iter().any(|id| id == key)
            })
            .map(|record| record.object_keys.clone())
            .unwrap_or_else(|| vec![key.clone()]);
        let mut restored = Vec::new();
        for version_id in versions {
            for object_id in &object_ids {
                if restore_object_version_from_trash_with_metadata(fragment, object_id, version_id)?
                    .is_some()
                {
                    restored.push(version_id.clone());
                    break;
                }
            }
        }
        if !restored.is_empty() {
            out.insert(key.clone(), restored);
        }
    }
    Ok(out)
}

pub fn delete_object_fast(
    fragment: &str,
    object_id: &str,
) -> Result<Option<Vec<String>>, AppError> {
    let mut ids = Vec::new();
    let live_versions = list_live_versions(fragment, object_id)?;
    for meta in live_versions {
        if !ids.contains(&meta.version_id) {
            ids.push(meta.version_id);
        }
    }
    let trash_versions = list_trash_versions(fragment, object_id)?;
    for meta in trash_versions {
        if !ids.contains(&meta.version_id) {
            ids.push(meta.version_id);
        }
    }
    delete_unreferenced_object_index(fragment, object_id)?;
    Ok((!ids.is_empty()).then_some(ids))
}

pub fn object_partition_id(_fragment: &str, object_key: &str) -> Result<u16, AppError> {
    let hash = object_key_hash(object_key)?;
    Ok(u16::from_str_radix(&hash[..4], 16).unwrap_or(0))
}

pub fn has_object_index_record(fragment: &str, object_key: &str) -> Result<bool, AppError> {
    Ok(object_dir_candidates(fragment, object_key)?
        .into_iter()
        .any(|path| path.is_dir()))
}

pub fn ensure_object_owner_partition_available(
    fragment: &str,
    _object_key: &str,
) -> Result<(), AppError> {
    ensure_fragment_dirs(fragment)
}

pub fn ensure_fragment_owner_partition_available(fragment: &str) -> Result<(), AppError> {
    ensure_fragment_dirs(fragment)
}

pub fn ensure_fragment_owner_partition_available_strict(fragment: &str) -> Result<(), AppError> {
    ensure_fragment_dirs(fragment)
}

pub fn has_any_fragment_index(fragment: &str) -> Result<bool, AppError> {
    Ok(existing_fragment_roots(fragment).into_iter().any(|root| {
        root.join(FRAGMENT_META_FILE).exists()
            || root.join(UPLOADS_DIR).exists()
            || object_dirs_for_all_live(fragment)
                .map(|v| !v.is_empty())
                .unwrap_or(false)
    }) || blob_roots()
        .iter()
        .any(|root| count_object_index_records(root, fragment) > 0))
}
