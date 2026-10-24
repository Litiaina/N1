use super::*;
use rustix::fs::{CWD, Dir, FileType, Mode, OFlags, SeekFrom, fstat, openat, seek};

struct DirectoryFrame {
    physical_directory: PathBuf,
    logical_parent: String,
    next_cookie: u64,
    identity: Option<(u64, u64)>,
}

impl DirectoryFrame {
    fn new(physical_directory: PathBuf, logical_parent: String) -> Self {
        Self {
            physical_directory,
            logical_parent,
            next_cookie: 0,
            identity: None,
        }
    }
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

fn open_directory(frame: &mut DirectoryFrame) -> Result<Option<Dir>, AppError> {
    let fd = match openat(
        CWD,
        &frame.physical_directory,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    ) {
        Ok(fd) => fd,
        // Namespace listing is deliberately lockless. Another coordinator
        // may remove the last immutable NCNX and prune this branch after its
        // parent was enumerated. That disappearance is an empty branch, not
        // an I/O failure.
        Err(rustix::io::Errno::NOENT) => {
            return Ok(None);
        }
        Err(err) => {
            return Err(AppError::Io(
                err.into(),
                format!("open NCNX directory {}", frame.physical_directory.display()),
            ));
        }
    };
    let stat = fstat(&fd).map_err(|err| {
        AppError::Io(
            err.into(),
            format!(
                "inspect NCNX directory {}",
                frame.physical_directory.display()
            ),
        )
    })?;
    let identity = (stat.st_dev as u64, stat.st_ino as u64);
    if frame.identity.is_some_and(|expected| expected != identity) {
        return Err(AppError::ServiceUnavailable(format!(
            "NCNX directory changed while listing {}; retry listing",
            frame.physical_directory.display()
        )));
    }
    frame.identity = Some(identity);
    if frame.next_cookie != 0 {
        seek(&fd, SeekFrom::Start(frame.next_cookie)).map_err(|err| {
            AppError::Io(
                err.into(),
                format!(
                    "resume NCNX directory {}",
                    frame.physical_directory.display()
                ),
            )
        })?;
    }
    Dir::new(fd).map(Some).map_err(|err| {
        AppError::Io(
            err.into(),
            format!("read NCNX directory {}", frame.physical_directory.display()),
        )
    })
}

pub(super) fn list_names(
    root: &Path,
    prefix: &str,
    after: Option<&str>,
    include_after: bool,
    limit: usize,
) -> Result<Vec<NcnxListItem>, AppError> {
    list_names_observed(root, prefix, after, include_after, limit, |_, _| {})
}

fn list_names_observed(
    root: &Path,
    prefix: &str,
    after: Option<&str>,
    include_after: bool,
    limit: usize,
    mut observe: impl FnMut(usize, usize),
) -> Result<Vec<NcnxListItem>, AppError> {
    let limit = limit.clamp(1, 10_000);
    let mut selected = BTreeMap::<String, NcnxListItem>::new();
    let mut stack = vec![DirectoryFrame::new(root.to_path_buf(), String::new())];
    let mut active = None;
    observe(selected.len(), stack.len());

    // Filesystem enumeration is unordered. Keep only the lowest `limit`
    // matching logical keys, rather than materializing every directory entry
    // and sorting it. Suspended parent frames keep only a path, inode identity
    // and an opaque kernel directory cookie. Close the parent before descent
    // and resume at that cookie, rather than retaining one FD per depth or
    // restarting a wide directory from its beginning. At most one directory
    // descriptor is open here, independent of depth and object count.
    // This bounds memory, not traversal time: without a separate index a page
    // can still inspect every relevant filename. NCNX bodies are never read.
    while let Some(frame) = stack.last_mut() {
        if active.is_none() {
            active = open_directory(frame)?;
            if active.is_none() {
                stack.pop();
                continue;
            }
        }
        let Some(entry) = active.as_mut().expect("active NCNX directory").read() else {
            active = None;
            stack.pop();
            continue;
        };
        let entry = match entry {
            Ok(entry) => entry,
            Err(rustix::io::Errno::NOENT) => continue,
            Err(err) => {
                return Err(AppError::Io(
                    err.into(),
                    format!(
                        "read NCNX directory entry in {}",
                        frame.physical_directory.display()
                    ),
                ));
            }
        };
        frame.next_cookie = entry.offset().try_into().map_err(|_| {
            AppError::Conflict(format!(
                "NCNX directory has an invalid resume cookie: {}",
                frame.physical_directory.display()
            ))
        })?;
        let file_name = entry.file_name().to_str().map_err(|_| {
            AppError::Conflict(format!(
                "NCNX path component is not UTF-8: {}",
                entry.file_name().to_string_lossy()
            ))
        })?;
        if matches!(file_name, "." | "..") || file_name.starts_with(".n2-") {
            continue;
        }
        let physical_path = frame.physical_directory.join(file_name);
        let (is_directory, is_file) = match entry.file_type() {
            FileType::Directory => (true, false),
            FileType::RegularFile => (false, true),
            FileType::Unknown => match fs::symlink_metadata(&physical_path) {
                Ok(metadata) => (metadata.is_dir(), metadata.is_file()),
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(AppError::Io(
                        err,
                        format!("inspect NCNX path {}", physical_path.display()),
                    ));
                }
            },
            _ => (false, false),
        };
        if is_directory {
            let logical_prefix = format!("{}{file_name}/", frame.logical_parent);
            if !prefixes_overlap(&logical_prefix, prefix)
                || !branch_can_exceed(&logical_prefix, after)
            {
                continue;
            }
            // Every descendant sorts after its directory prefix. Once this
            // lower bound cannot beat the current page maximum, opening the
            // branch cannot contribute a result.
            if selected.len() == limit
                && selected
                    .last_key_value()
                    .is_some_and(|(last, _)| logical_prefix.as_str() >= last.as_str())
            {
                continue;
            }
            if frame.next_cookie == 0 {
                return Err(AppError::Conflict(format!(
                    "NCNX directory does not provide resumable entry cookies: {}",
                    frame.physical_directory.display()
                )));
            }
            active = None;
            stack.push(DirectoryFrame::new(physical_path, logical_prefix));
            observe(selected.len(), stack.len());
            continue;
        }
        if !is_file {
            continue;
        }
        let relative = format!("{}{file_name}", frame.logical_parent);
        let Some(logical_key) = relative.strip_suffix(NCNX_SUFFIX) else {
            continue;
        };
        if !logical_key.starts_with(prefix) {
            continue;
        }
        if let Some(after) = after {
            if logical_key < after || (!include_after && logical_key == after) {
                continue;
            }
        }
        if selected.len() == limit {
            if selected
                .last_key_value()
                .is_some_and(|(last, _)| logical_key >= last.as_str())
            {
                continue;
            }
            // Pop BEFORE inserting, so retained candidates never exceed the
            // page bound, even temporarily.
            selected.pop_last();
        }
        selected.insert(
            logical_key.to_string(),
            NcnxListItem {
                logical_key: logical_key.to_string(),
                physical_path,
                trash: None,
            },
        );
        observe(selected.len(), stack.len());
    }
    Ok(selected.into_values().collect())
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

#[cfg(test)]
#[path = "listing/tests.rs"]
mod tests;
