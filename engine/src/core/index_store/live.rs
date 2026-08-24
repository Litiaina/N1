use super::*;

pub fn upsert_live_version(metadata: &ObjectMetadata) -> Result<(), AppError> {
    write_metadata_to_roots(metadata)
}

pub fn read_live_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let object_key = normalize_object_key(object_key)?;
    for dir in object_dir_candidates(fragment, &object_key)? {
        if let Some(path) = ncdx_path_for_version(&dir, version_id)? {
            return decode_metadata_file_as_if_present(&path, &object_key);
        }
    }
    Ok(None)
}

pub fn find_live_version_by_id(
    fragment: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let version_id = version_id.trim();
    if version_id.is_empty() {
        return Ok(None);
    }

    for root in blob_roots() {
        for (dir, object_id) in object_record_dirs_under_root(&root, fragment)? {
            if let Some(path) = ncdx_path_for_version(&dir, version_id)? {
                return decode_metadata_file_as_if_present(&path, &object_id);
            }
        }
    }
    Ok(None)
}

pub fn read_current_live_head(
    fragment: &str,
    object_key: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let object_key = normalize_object_key(object_key)?;
    let mut latest = None::<(PathBuf, NcdxSummary)>;
    for dir in object_dir_candidates(fragment, &object_key)? {
        let Some(summary) = latest_summary_in_object_dir(&dir)? else {
            continue;
        };
        if latest
            .as_ref()
            .is_none_or(|(_, current)| summary.version_id > current.version_id)
        {
            latest = Some((dir, summary));
        }
    }
    let Some((dir, summary)) = latest else {
        return Ok(None);
    };
    decode_metadata_file_as_if_present(&dir.join(summary.file_name), &object_key)
}

pub fn list_live_versions(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let object_key = normalize_object_key(object_key)?;
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for path in all_live_version_paths(fragment, &object_key)? {
        let Some(meta) = decode_metadata_file_as_if_present(&path, &object_key)? else {
            continue;
        };
        if seen.insert(meta.version_id.clone()) {
            out.push(meta);
        }
    }
    out.sort_by(|a, b| a.version_id.cmp(&b.version_id));
    Ok(out)
}

pub fn list_live_heads(fragment: &str) -> Result<Vec<ObjectMetadata>, AppError> {
    let mut out = Vec::new();
    for (dir, rel, _object_id) in object_dirs_for_all_live(fragment)? {
        if let Some(summary) = latest_summary_in_object_dir(&dir)? {
            out.push(summary_metadata(fragment, rel, summary));
        }
    }
    out.sort_by(|a, b| a.object_key.cmp(&b.object_key));
    Ok(out)
}

fn scan_live_heads_direct_page(
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
    for fragment_root in existing_fragment_roots(fragment) {
        let index_root = fragment_root
            .parent()
            .unwrap_or(&fragment_root)
            .to_path_buf();
        let dir = fragment_root.join(&prefix);
        if !dir.is_dir() {
            continue;
        }
        for name in read_dir_names(&dir)? {
            if internal_namespace_entry_name(&name) {
                continue;
            }
            let child = dir.join(&name);
            if let Some(object_id) =
                read_namespace_symlink_object_id(&index_root, fragment, &child)?
            {
                if continuation_token
                    .as_ref()
                    .is_some_and(|after| name.as_str() <= after.as_str())
                {
                    continue;
                }
                let Some(object_dir) = object_dir_candidates(fragment, &object_id)?
                    .into_iter()
                    .find(|dir| dir.is_dir())
                else {
                    continue;
                };
                let Some(summary) = latest_summary_in_object_dir(&object_dir)? else {
                    continue;
                };
                let record = record_path(&prefix, &name);
                let entry = summary_entry(name.clone(), &summary);
                let metadata = summary_metadata(fragment, record, summary);
                by_record.entry(name).or_insert((entry, Some(metadata)));
            } else if child.is_dir() {
                if continuation_token
                    .as_ref()
                    .is_some_and(|after| name.as_str() <= after.as_str())
                {
                    continue;
                }
                by_record
                    .entry(name.clone())
                    .or_insert((directory_entry(name), None));
            }
        }
    }

    let mut entries = Vec::new();
    let mut objects = Vec::new();
    let mut common_prefixes = Vec::new();
    let mut next = None;
    for (idx, (_record, (entry, object))) in by_record.into_iter().enumerate() {
        if idx >= limit {
            next = entries
                .last()
                .map(|entry: &IndexListEntry| entry.record.clone());
            break;
        }
        if entry.kind == "directory" {
            common_prefixes.push(entry.record.clone());
        }
        if let Some(object) = object {
            objects.push(object);
        }
        entries.push(entry);
    }
    Ok(ListObjectsResult {
        prefix: Some(prefix),
        limit: Some(limit),
        entries,
        objects,
        common_prefixes,
        continuation_token: next.clone(),
        next_continuation_token: next,
    })
}

pub fn scan_live_heads_page(
    fragment: &str,
    prefix: Option<String>,
    delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    _include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    let _ = delimiter;
    scan_live_heads_direct_page(fragment, prefix, limit, continuation_token)
}

pub fn scan_live_full_metadata_page(
    fragment: &str,
    prefix: Option<String>,
    delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    _include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    let prefix = prefix.unwrap_or_default();
    let delimiter = delimiter.unwrap_or_default();
    let limit = limit.max(1);
    let mut candidates = Vec::new();
    let mut objects = Vec::new();
    let mut common_prefixes = BTreeSet::new();
    for (dir, rel, object_id) in object_dirs_for_all_live(fragment)? {
        if !prefix.is_empty() && !rel.starts_with(&prefix) {
            continue;
        }
        if continuation_token
            .as_ref()
            .is_some_and(|after| rel <= *after)
        {
            continue;
        }
        if delimiter == "/" {
            let rest = if prefix.is_empty() {
                rel.as_str()
            } else {
                rel.strip_prefix(&prefix).unwrap_or("")
            };
            let rest = rest.trim_start_matches('/');
            if let Some((first, _)) = rest.split_once('/') {
                let cp = if prefix.is_empty() {
                    format!("{first}/")
                } else {
                    format!("{}/{}", prefix.trim_end_matches('/'), first)
                };
                common_prefixes.insert(cp);
                continue;
            }
        }
        candidates.push((rel, object_id, dir));
    }

    let mut continuation = None;
    if candidates.len() > limit {
        candidates.truncate(limit);
        continuation = candidates.last().map(|(rel, _, _)| rel.clone());
    }

    for (_rel, object_id, dir) in candidates {
        let Some(summary) = latest_summary_in_object_dir(&dir)? else {
            continue;
        };
        if let Some(metadata) =
            decode_metadata_file_as_if_present(&dir.join(summary.file_name), &object_id)?
        {
            objects.push(metadata);
        }
    }
    Ok(ListObjectsResult {
        prefix: Some(prefix),
        limit: Some(limit),
        entries: Vec::new(),
        objects,
        common_prefixes: common_prefixes.into_iter().collect(),
        continuation_token: continuation.clone(),
        next_continuation_token: continuation,
    })
}
