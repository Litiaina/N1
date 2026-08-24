use super::*;

#[derive(Debug)]
pub(super) enum LexicalEntry {
    File {
        sort_key: String,
        item: NcnxListItem,
    },
    Directory {
        sort_key: String,
        physical_path: PathBuf,
        logical_prefix: String,
    },
}

impl LexicalEntry {
    fn sort_key(&self) -> &str {
        match self {
            Self::File { sort_key, .. } | Self::Directory { sort_key, .. } => sort_key,
        }
    }
}

#[derive(Debug)]
pub(super) struct LexicalFrame {
    pub(super) entries: Vec<LexicalEntry>,
}

pub(super) fn prefixes_overlap(branch: &str, requested: &str) -> bool {
    requested.is_empty() || requested.starts_with(branch) || branch.starts_with(requested)
}

pub(super) fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut successor = prefix.to_vec();
    for index in (0..successor.len()).rev() {
        if successor[index] != u8::MAX {
            successor[index] += 1;
            successor.truncate(index + 1);
            return Some(successor);
        }
    }
    None
}

pub(super) fn branch_can_exceed(branch: &str, after: Option<&str>) -> bool {
    let Some(after) = after else {
        return true;
    };
    prefix_successor(branch.as_bytes()).map_or(true, |upper_bound| {
        after.as_bytes() < upper_bound.as_slice()
    })
}

pub(super) fn directory_frame(
    physical_directory: &Path,
    logical_parent: &str,
    requested_prefix: &str,
    after: Option<&str>,
    include_after: bool,
) -> Result<LexicalFrame, AppError> {
    let mut entries = Vec::new();
    let directory = match fs::read_dir(physical_directory) {
        Ok(directory) => directory,
        // Namespace listing is deliberately lockless. Another coordinator
        // may remove the last immutable NCNX and prune this branch after its
        // parent was enumerated. That disappearance is an empty branch, not
        // an I/O failure.
        Err(err) if err.kind() == ErrorKind::NotFound => {
            return Ok(LexicalFrame { entries });
        }
        Err(err) => {
            return Err(AppError::Io(
                err,
                format!("read NCNX directory {}", physical_directory.display()),
            ));
        }
    };
    for entry in directory {
        let entry = match entry {
            Ok(entry) => entry,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(AppError::Io(
                    err,
                    format!(
                        "read NCNX directory entry in {}",
                        physical_directory.display()
                    ),
                ));
            }
        };
        let file_name = entry.file_name().into_string().map_err(|name| {
            AppError::Conflict(format!(
                "NCNX path component is not UTF-8: {}",
                PathBuf::from(name).display()
            ))
        })?;
        if file_name.starts_with(".n2-") {
            continue;
        }
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(AppError::Io(
                    err,
                    format!("inspect NCNX path {}", entry.path().display()),
                ));
            }
        };
        if file_type.is_dir() {
            let logical_prefix = format!("{logical_parent}{file_name}/");
            if prefixes_overlap(&logical_prefix, requested_prefix)
                && branch_can_exceed(&logical_prefix, after)
            {
                entries.push(LexicalEntry::Directory {
                    sort_key: logical_prefix.clone(),
                    physical_path: entry.path(),
                    logical_prefix,
                });
            }
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        let relative = format!("{logical_parent}{file_name}");
        let Some(logical_key) = relative.strip_suffix(NCNX_SUFFIX) else {
            continue;
        };
        if !logical_key.starts_with(requested_prefix) {
            continue;
        }
        if let Some(after) = after {
            if logical_key < after || (!include_after && logical_key == after) {
                continue;
            }
        }
        entries.push(LexicalEntry::File {
            sort_key: logical_key.to_string(),
            item: NcnxListItem {
                logical_key: logical_key.to_string(),
                physical_path: entry.path(),
                trash: None,
            },
        });
    }
    // Reverse order lets the streaming walk pop the next lexical entry in O(1).
    entries.sort_unstable_by(|left, right| right.sort_key().cmp(left.sort_key()));
    Ok(LexicalFrame { entries })
}

pub(super) fn list_names(
    root: &Path,
    prefix: &str,
    after: Option<&str>,
    include_after: bool,
    limit: usize,
) -> Result<Vec<NcnxListItem>, AppError> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let limit = limit.clamp(1, 10_000);
    let mut out = Vec::with_capacity(limit);
    let mut stack = vec![directory_frame(root, "", &prefix, after, include_after)?];
    while let Some(frame) = stack.last_mut() {
        let Some(entry) = frame.entries.pop() else {
            stack.pop();
            continue;
        };
        match entry {
            LexicalEntry::File { item, .. } => {
                out.push(item);
                if out.len() == limit {
                    break;
                }
            }
            LexicalEntry::Directory {
                physical_path,
                logical_prefix,
                ..
            } => stack.push(directory_frame(
                &physical_path,
                &logical_prefix,
                &prefix,
                after,
                include_after,
            )?),
        }
    }
    Ok(out)
}

pub fn parse_trash_relative_path(relative: &str) -> Option<TrashLocator> {
    let (generation, encoded_key) = relative.split_once('/')?;
    let (deleted_at_unix_nanos, delete_mutation_id) = parse_trash_generation_name(generation)?;
    let key = encoded_key.strip_suffix(NCNX_SUFFIX)?;
    let logical_key = validate_logical_key(key).ok()?;
    Some(TrashLocator {
        logical_key,
        deleted_at_unix_nanos,
        delete_mutation_id,
    })
}

pub(super) fn parse_trash_cursor(after: &str) -> Result<(&str, i64, &str), AppError> {
    let mut fields = after.split('\0');
    let logical_key = fields.next().unwrap_or_default();
    let deleted_at = fields.next();
    let mutation_id = fields.next();
    if fields.next().is_some()
        || logical_key.is_empty()
        || deleted_at.is_none()
        || mutation_id.is_none()
    {
        return Err(AppError::BadRequest(
            "invalid NCNX trash continuation token".into(),
        ));
    }
    let deleted_at_unix_nanos = deleted_at
        .unwrap()
        .parse::<i64>()
        .map_err(|_| AppError::BadRequest("invalid NCNX trash continuation token".into()))?;
    let mutation_id = mutation_id.unwrap();
    validate_logical_key(logical_key)?;
    if deleted_at_unix_nanos <= 0 || Ulid::from_string(mutation_id).is_err() {
        return Err(AppError::BadRequest(
            "invalid NCNX trash continuation token".into(),
        ));
    }
    Ok((logical_key, deleted_at_unix_nanos, mutation_id))
}

pub(super) fn list_generation_trash_names(
    trash_root: &Path,
    prefix: &str,
    after: Option<&str>,
    limit: usize,
) -> Result<Vec<NcnxListItem>, AppError> {
    if !trash_root.exists() {
        return Ok(Vec::new());
    }
    let limit = limit.clamp(1, 10_000);
    let after = after.map(parse_trash_cursor).transpose()?;
    let after_cursor =
        after.map(|(key, nanos, mutation)| format!("{key}\0{nanos:020}\0{mutation}"));
    let mut selected = BTreeMap::<String, NcnxListItem>::new();
    let generations = match fs::read_dir(trash_root) {
        Ok(generations) => generations,
        // The final generation may be reclaimed between the existence check
        // above and opening the directory.
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(AppError::Io(
                err,
                format!("read NCNX trash root {}", trash_root.display()),
            ));
        }
    };
    for entry in generations {
        let entry = match entry {
            Ok(entry) => entry,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(AppError::Io(
                    err,
                    format!("read NCNX trash entry in {}", trash_root.display()),
                ));
            }
        };
        let generation_name = entry.file_name().into_string().map_err(|name| {
            AppError::Conflict(format!(
                "NCNX trash generation is not UTF-8: {}",
                PathBuf::from(name).display()
            ))
        })?;
        if generation_name.starts_with(".n2-") {
            continue;
        }
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(AppError::Io(
                    err,
                    format!("inspect NCNX trash generation {}", entry.path().display()),
                ));
            }
        };
        let Some((deleted_at_unix_nanos, delete_mutation_id)) =
            parse_trash_generation_name(&generation_name)
        else {
            return Err(AppError::Conflict(format!(
                "invalid N2 3.4 trash generation '{}' in {}",
                generation_name,
                trash_root.display()
            )));
        };
        if !file_type.is_dir() {
            return Err(AppError::Conflict(format!(
                "N2 3.4 trash generation is not a directory: {}",
                entry.path().display()
            )));
        }

        let (logical_after, include_after) = match after {
            Some((key, after_nanos, after_mutation)) => {
                let generation_is_after = (deleted_at_unix_nanos, delete_mutation_id.as_str())
                    > (after_nanos, after_mutation);
                (Some(key), generation_is_after)
            }
            None => (None, false),
        };
        for mut item in list_names(&entry.path(), prefix, logical_after, include_after, limit)? {
            let locator = TrashLocator {
                logical_key: item.logical_key.clone(),
                deleted_at_unix_nanos,
                delete_mutation_id: delete_mutation_id.clone(),
            };
            let cursor = format!(
                "{}\0{:020}\0{}",
                locator.logical_key, locator.deleted_at_unix_nanos, locator.delete_mutation_id
            );
            if after_cursor
                .as_ref()
                .is_some_and(|after| cursor.as_str() <= after.as_str())
            {
                continue;
            }
            item.trash = Some(locator);
            selected.insert(cursor, item);
            if selected.len() > limit {
                selected.pop_last();
            }
        }
    }
    Ok(selected.into_values().collect())
}

pub(super) fn prune_empty_trash_parents(path: &Path, trash_root: &Path) -> Result<(), AppError> {
    let mut current = path.parent().map(Path::to_path_buf);
    while let Some(directory) = current {
        if directory == trash_root {
            break;
        }
        let parent = directory.parent().map(Path::to_path_buf);
        match fs::remove_dir(&directory) {
            Ok(()) => sync_parent(&directory)?,
            Err(err)
                if matches!(
                    err.kind(),
                    ErrorKind::NotFound | ErrorKind::DirectoryNotEmpty
                ) =>
            {
                break;
            }
            Err(err) => {
                return Err(AppError::Io(
                    err,
                    format!("prune empty NCNX trash directory {}", directory.display()),
                ));
            }
        }
        current = parent;
    }
    Ok(())
}
