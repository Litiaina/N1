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
    read_current_live_head(fragment, object_key)
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

pub(crate) fn visit_trash_records(
    fragment: &str,
    mut visit: impl FnMut(TrashRecord) -> Result<bool, AppError>,
) -> Result<(), AppError> {
    let mut scan = crate::namespace::flat::Scan::new(fragment, true)?;
    while let Some(entry) = scan.next_record()? {
        let Some(mut record) = entry.trash else {
            continue;
        };
        // NCX owns the logical name/object identity, not immutable version
        // metadata. Derive the maintenance catalog from authoritative NDX,
        // deduplicating physical replicas rather than reporting an empty
        // version list for every flat trash anchor.
        let mut versions = BTreeSet::new();
        for object_id in &record.object_keys {
            versions.extend(
                list_live_versions(fragment, object_id)?
                    .into_iter()
                    .map(|metadata| metadata.version_id),
            );
        }
        record.version_ids = versions.into_iter().collect();
        if !visit(record)? {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
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

#[cfg(test)]
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
    crate::namespace::flat::referenced_objects(fragment)
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
    // Destructive absence checks must include enrolled members that foreground
    // placement filters out because their identity is missing or stale.
    let roots = crate::core::disk_manager::BLOB_STORE_DISKS
        .get()
        .into_iter()
        .flatten()
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    let mut removed = Vec::new();
    for object_id in candidates.iter().collect::<BTreeSet<_>>() {
        let object_id = normalize_object_id(object_id)?;
        if referenced.contains(&object_id) || pending.contains(&object_id) {
            continue;
        }
        let Some(_reclaim_guard) = upload_scan.try_reclaim(&object_id)? else {
            continue;
        };
        if delete_unreferenced_object_from_roots(fragment, &object_id, &roots)? {
            removed.push(object_id);
        }
    }
    Ok(removed)
}

fn delete_unreferenced_object_from_roots(
    fragment: &str,
    object_id: &str,
    roots: &[PathBuf],
) -> Result<bool, AppError> {
    // One object's reclamation participates in online capture, not the entire
    // potentially large trash batch. The caller already fences this object
    // against uploads and has established the absence of namespace references.
    let _snapshot_boundary = crate::core::backup::barrier::mutation();
    if roots.is_empty()
        || roots
            .iter()
            .any(|root| !crate::core::storage_pool::root_available(root))
    {
        return Err(AppError::ServiceUnavailable(
            "cannot prove every enrolled member before expired-object reclamation".into(),
        ));
    }
    let mut directories = Vec::with_capacity(roots.len());
    for root in roots {
        let directory = object_index_dir(root, fragment, object_id)?;
        // Check ALL member paths before unlinking ANY. A symlinked fanout
        // ancestor redirects remove_dir_all even when its final object is a
        // real directory; the shared path proof rejects that static layout.
        if crate::core::garbage_collector::object_directory_present(root, &directory)? {
            directories.push(directory);
        }
    }
    let mut removed = false;
    for directory in directories {
        fs::remove_dir_all(&directory)
            .map_err(|error| io_err(error, "reclaim N1 expired trash object"))?;
        removed = true;
        if let Some(parent) = directory.parent() {
            sync_dir(parent);
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

#[cfg(test)]
mod expired_trash_path_safety_tests {
    use super::*;

    #[test]
    fn fanout_redirection_preserves_foreign_and_healthy_member_objects() {
        let base = std::env::temp_dir().join(format!("n1-trash-fanout-{}", Ulid::new()));
        let roots = vec![base.join("healthy"), base.join("redirected")];
        let fragment = Ulid::new().to_string();
        let object = Ulid::new().to_string();
        let healthy = object_index_dir(&roots[0], &fragment, &object).unwrap();
        fs::create_dir_all(&healthy).unwrap();
        fs::write(healthy.join("retained"), b"healthy member bytes").unwrap();

        let redirected = object_index_dir(&roots[1], &fragment, &object).unwrap();
        let second_fanout = redirected.parent().unwrap();
        let first_fanout = second_fanout.parent().unwrap();
        fs::create_dir_all(first_fanout.parent().unwrap()).unwrap();
        let foreign = base.join("foreign");
        let foreign_object = foreign
            .join(second_fanout.file_name().unwrap())
            .join(redirected.file_name().unwrap());
        fs::create_dir_all(&foreign_object).unwrap();
        fs::write(foreign_object.join("retained"), b"foreign bytes").unwrap();
        unix_fs::symlink(&foreign, first_fanout).unwrap();
        for root in &roots {
            assert!(crate::core::storage_pool::root_available(root));
        }

        assert!(matches!(
            delete_unreferenced_object_from_roots(&fragment, &object, &roots),
            Err(AppError::Conflict(_))
        ));
        assert_eq!(
            fs::read(healthy.join("retained")).unwrap(),
            b"healthy member bytes"
        );
        assert_eq!(
            fs::read(foreign_object.join("retained")).unwrap(),
            b"foreign bytes"
        );
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn absent_member_defers_reclamation_but_absent_object_on_available_member_is_safe() {
        let base = std::env::temp_dir().join(format!("n1-trash-member-{}", Ulid::new()));
        let roots = vec![base.join("healthy"), base.join("missing")];
        let fragment = Ulid::new().to_string();
        let object = Ulid::new().to_string();
        let healthy = object_index_dir(&roots[0], &fragment, &object).unwrap();
        fs::create_dir_all(&healthy).unwrap();
        fs::write(
            healthy.join("retained"),
            b"protected until all members present",
        )
        .unwrap();
        assert!(matches!(
            delete_unreferenced_object_from_roots(&fragment, &object, &roots),
            Err(AppError::ServiceUnavailable(_))
        ));
        assert!(healthy.join("retained").is_file());
        fs::create_dir(&roots[1]).unwrap();
        assert!(delete_unreferenced_object_from_roots(&fragment, &object, &roots).unwrap());
        assert!(!healthy.exists());
        assert!(!delete_unreferenced_object_from_roots(&fragment, &object, &roots).unwrap());
        assert!(delete_unreferenced_object_from_roots(&fragment, &object, &[]).is_err());
        fs::remove_dir_all(base).unwrap();
    }
}
