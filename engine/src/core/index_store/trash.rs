use super::*;

pub fn publish_trash_version_for_maintenance(metadata: &ObjectMetadata) -> Result<(), AppError> {
    write_metadata_to_roots(metadata)
}

pub fn read_trash_version(
    fragment: &str,
    object_id: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    for dir in object_dir_candidates(fragment, object_id)? {
        if let Some(path) = ncdx_path_for_version(&dir, version_id)? {
            return decode_metadata_file_if_present(&path);
        }
    }
    Ok(None)
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
        let names = match read_dir_names(&dir) {
            Ok(names) => names,
            Err(AppError::NotFound) => continue,
            Err(error) => return Err(error),
        };
        for name in names {
            if parse_ncdx_file_name(&name).is_some() {
                let Some(meta) = decode_metadata_file_if_present(&dir.join(name))? else {
                    continue;
                };
                if seen.insert(meta.version_id.clone()) {
                    out.push(meta);
                }
            }
        }
    }
    out.sort_by(|a, b| a.version_id.cmp(&b.version_id));
    Ok(out)
}

pub(crate) fn scan_trash_records(fragment: &str) -> Result<Vec<TrashRecord>, AppError> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for fragment_root in existing_fragment_roots(fragment) {
        let index_root = fragment_root
            .parent()
            .unwrap_or(&fragment_root)
            .to_path_buf();
        let trash_root = fragment_root.join(TRASH_DIR);
        let mut stack = vec![(trash_root.clone(), String::new())];
        while let Some((dir, rel)) = stack.pop() {
            if !dir.is_dir() {
                continue;
            }
            for name in read_dir_names(&dir)? {
                if internal_namespace_entry_name(&name) && name != OBJECTS_DIR {
                    continue;
                }
                let child = dir.join(&name);
                let child_rel = if rel.is_empty() {
                    name.clone()
                } else {
                    format!("{rel}/{name}")
                };
                if let Some(object_id) =
                    read_namespace_symlink_object_id(&index_root, fragment, &child)?
                {
                    let mut object_keys = BTreeSet::new();
                    let mut version_ids = BTreeSet::new();
                    let mut deleted_at = Utc::now().timestamp();
                    let Some(object_dir) = object_dir_candidates(fragment, &object_id)?
                        .into_iter()
                        .find(|dir| dir.is_dir())
                    else {
                        continue;
                    };
                    for ncdx_name in read_dir_names(&object_dir)? {
                        if parse_ncdx_file_name(&ncdx_name).is_none() {
                            continue;
                        }
                        let meta = decode_metadata_file(&object_dir.join(&ncdx_name))?;
                        deleted_at = meta.created_at;
                        object_keys.insert(object_id.clone());
                        version_ids.insert(meta.version_id);
                    }
                    if seen.insert(child_rel.clone()) {
                        out.push(TrashRecord {
                            trash_entry_id: child_rel.clone(),
                            original_path: child_rel.clone(),
                            source_relative_path: child_rel,
                            kind: TrashKind::File,
                            object_keys: object_keys.into_iter().collect(),
                            version_ids: version_ids.into_iter().collect(),
                            deleted_at_unix: deleted_at,
                        });
                    }
                } else if child.is_dir() {
                    stack.push((child, child_rel));
                }
            }
        }
    }
    out.sort_by(|a, b| a.source_relative_path.cmp(&b.source_relative_path));
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
    full_metadata: bool,
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
            continue;
        }

        let version_id = record.version_ids.last().cloned().unwrap_or_default();
        let mut metadata = if full_metadata {
            let Some(object_id) = record.object_keys.first() else {
                continue;
            };
            let Some(mut meta) = read_latest_trash(fragment, object_id)? else {
                continue;
            };
            meta.object_key = record.original_path.clone();
            meta
        } else {
            ObjectMetadata {
                object_key: record.original_path.clone(),
                packed_ext_u64: packed_extension_key(&record.original_path),
                fragment: fragment.to_string(),
                version_id,
                upload_session_id: String::new(),
                owner_uid: fragment.to_string(),
                content_type: "application/octet-stream".to_string(),
                total_size_bytes: 0,
                created_at: record.deleted_at_unix,
                // Trash list summaries do not claim an object layout.
                pool_id: String::new(),
                pool_generation: 0,
                membership_epoch: 0,
                data_shards: 0,
                parity_shards: 0,
                parts: Vec::new(),
                is_delete_marker: false,
                stripe_size_bytes: crate::core::disk_manager::default_stripe_size(),
            }
        };
        metadata.object_key = record.original_path.clone();
        let entry = IndexListEntry {
            record: name.clone(),
            kind: "file".to_string(),
            size_bytes: metadata.total_size_bytes,
            created_at_unix_nanos: metadata.created_at.saturating_mul(1_000_000_000),
            packed_ext_u64: Some(packed_extension_key(&metadata.object_key)),
            version_ulid: Some(metadata.version_id.clone()),
        };
        by_record.entry(name).or_insert((entry, Some(metadata)));
    }

    let mut page = by_record.into_iter().collect::<Vec<_>>();
    page.sort_by(|a, b| a.0.cmp(&b.0));
    let continuation = if page.len() > limit {
        page.truncate(limit);
        page.last().map(|(record, _)| record.clone())
    } else {
        None
    };
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

pub fn scan_trash_heads_page(
    fragment: &str,
    prefix: Option<String>,
    _delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    _include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    scan_trash_page(fragment, prefix, limit, continuation_token, false)
}

pub fn scan_trash_full_metadata_page(
    fragment: &str,
    prefix: Option<String>,
    _delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    _include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    scan_trash_page(fragment, prefix, limit, continuation_token, true)
}

pub fn list_object_keys_with_trash_versions(fragment: &str) -> Result<Vec<String>, AppError> {
    Ok(list_trash_catalog(fragment)?
        .into_iter()
        .map(|entry| entry.object_key)
        .collect())
}

fn collect_symlink_object_ids_under(
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

pub(crate) fn delete_unreferenced_object_record(
    fragment: &str,
    object_id: &str,
) -> Result<bool, AppError> {
    let object_id = normalize_object_id(object_id)?;
    if referenced_object_ids(fragment)?.contains(&object_id) {
        return Ok(false);
    }
    let mut removed = false;
    for dir in object_dir_candidates(fragment, &object_id)? {
        if dir.exists() {
            match fs::remove_dir_all(&dir) {
                Ok(()) => {
                    removed = true;
                    if let Some(parent) = dir.parent() {
                        sync_dir(parent);
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    // Another coordinator reclaimed the same immutable
                    // object directory after the existence check.
                }
                Err(error) => return Err(io_err(error, "delete N2 object record dir")),
            }
        }
    }
    Ok(removed)
}

pub(crate) fn cleanup_unreferenced_object_records(fragment: &str) -> Result<usize, AppError> {
    let referenced = referenced_object_ids(fragment)?;
    let mut removed = 0usize;
    let mut seen = HashSet::new();
    for fragment_root in existing_fragment_roots(fragment) {
        let root = fragment_root.parent().unwrap_or(&fragment_root);
        for (_, object_id) in object_record_dirs_under_root(root, fragment)? {
            if referenced.contains(&object_id) || !seen.insert(object_id.clone()) {
                continue;
            }
            if delete_unreferenced_object_record(fragment, &object_id)? {
                removed += 1;
            }
        }
    }
    Ok(removed)
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

pub fn delete_all_trash_versions_for_key(
    fragment: &str,
    object_key: &str,
) -> Result<Option<Vec<String>>, AppError> {
    let records = scan_trash_records(fragment)?;
    let object_ids = records
        .iter()
        .find(|record| {
            record.original_path == object_key
                || record.object_keys.iter().any(|id| id == object_key)
        })
        .map(|record| record.object_keys.clone())
        .unwrap_or_else(|| vec![object_key.to_string()]);
    let mut ids = Vec::new();
    for object_id in &object_ids {
        for meta in list_trash_versions(fragment, object_id)? {
            if !ids.contains(&meta.version_id) {
                ids.push(meta.version_id);
            }
        }
    }
    if ids.is_empty() {
        return Ok(None);
    }
    for object_id in object_ids {
        delete_unreferenced_object_record(fragment, &object_id)?;
    }
    Ok(Some(ids))
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
    delete_unreferenced_object_record(fragment, object_id)?;
    Ok((!ids.is_empty()).then_some(ids))
}

pub fn delete_live_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<bool, AppError> {
    let mut removed = false;
    for dir in object_dir_candidates(fragment, object_key)? {
        if let Some(path) = ncdx_path_for_version(&dir, version_id)? {
            removed |= remove_file_if_exists(&path)?;
        }
        let payload = dir.join(version_id);
        if payload.is_dir() {
            fs::remove_dir_all(&payload)
                .map_err(|e| io_err(e, "delete N2 object version payload"))?;
            removed = true;
        }
        if dir.is_dir() && read_dir_names(&dir)?.is_empty() {
            fs::remove_dir(&dir).map_err(|e| io_err(e, "remove empty N2 object directory"))?;
        }
    }
    Ok(removed)
}

pub fn delete_trash_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<bool, AppError> {
    delete_live_version(fragment, object_key, version_id)
}

pub fn clear_fragment_trash(fragment: &str, object_key: Option<&str>) -> Result<(), AppError> {
    if let Some(key) = object_key {
        let _ = delete_unreferenced_object_record(fragment, key)?;
        return Ok(());
    }
    for root in index_roots() {
        let path = fragment_root_on(&root, fragment).join(TRASH_DIR);
        if path.exists() {
            fs::remove_dir_all(&path).map_err(|e| io_err(e, "clear N2 trash object dir"))?;
        }
        fs::create_dir_all(&path).map_err(|e| io_err(e, "recreate N2 trash object dir"))?;
    }
    let _ = cleanup_unreferenced_object_records(fragment)?;
    Ok(())
}

pub fn drop_fragment_trash_namespace(fragment: &str) -> Result<(), AppError> {
    for root in index_roots() {
        let fragment_root = fragment_root_on(&root, fragment);
        let path = fragment_root.join(TRASH_DIR);
        if path.exists() {
            fs::remove_dir_all(&path).map_err(|e| io_err(e, "drop N2 trash namespace"))?;
        }
        fs::create_dir_all(&path).map_err(|e| io_err(e, "recreate N2 trash namespace"))?;
        sync_dir(&fragment_root);
    }
    Ok(())
}
