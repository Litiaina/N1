use super::*;

pub(super) fn trash_generation_name(
    deleted_at_unix_nanos: i64,
    delete_mutation_id: &str,
) -> Result<String, AppError> {
    if deleted_at_unix_nanos <= 0 || Ulid::from_string(delete_mutation_id).is_err() {
        return Err(AppError::BadRequest(
            "trash requires a positive deletion time and mutation ULID".into(),
        ));
    }
    Ok(format!("{deleted_at_unix_nanos}.{delete_mutation_id}"))
}

pub(super) fn parse_trash_generation_name(name: &str) -> Option<(i64, String)> {
    let (deleted_at, mutation_id) = name.split_once('.')?;
    if mutation_id.contains('.') {
        return None;
    }
    let deleted_at_unix_nanos = deleted_at.parse::<i64>().ok()?;
    if deleted_at_unix_nanos <= 0 || Ulid::from_string(mutation_id).is_err() {
        return None;
    }
    Some((deleted_at_unix_nanos, mutation_id.to_string()))
}

pub fn validate_logical_key(key: &str) -> Result<String, AppError> {
    let key = normalize_path(key).map_err(|err| AppError::BadRequest(err.to_string()))?;
    if key.is_empty() {
        return Err(AppError::BadRequest(
            "N2 3.4 object key cannot be empty".into(),
        ));
    }
    if key.len() > MAX_KEY_BYTES {
        return Err(AppError::BadRequest(format!(
            "object key exceeds {MAX_KEY_BYTES} bytes"
        )));
    }
    for component in key.split('/') {
        if component.as_bytes().len() > MAX_COMPONENT_BYTES {
            return Err(AppError::BadRequest(format!(
                "object key component exceeds {MAX_COMPONENT_BYTES} bytes"
            )));
        }
        if component.as_bytes().contains(&0)
            || component.starts_with(".n2-")
            || matches!(
                component,
                ".namespace" | ".trash" | ".uploads" | ".objects" | ".fragment"
            )
        {
            return Err(AppError::BadRequest(format!(
                "object key component '{component}' is reserved"
            )));
        }
    }
    Ok(key)
}

pub(super) fn validate_prefix(prefix: &str) -> Result<String, AppError> {
    if prefix.trim().is_empty() || prefix.trim() == "/" {
        return Ok(String::new());
    }
    let trailing_slash = prefix.trim().ends_with('/');
    let normalized = validate_logical_key(prefix)?;
    Ok(if trailing_slash {
        format!("{normalized}/")
    } else {
        normalized
    })
}

pub(super) fn read_record_if_present(
    path: &Path,
) -> Result<Option<(NcnxRecord, Vec<u8>)>, AppError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(AppError::Io(err, format!("read NCNX {}", path.display()))),
    };
    let record = NcnxRecord::decode(&bytes)?;
    Ok(Some((record, bytes)))
}

pub(super) struct StagedCreate {
    path: PathBuf,
    temporary: PathBuf,
    file: File,
    bytes: Vec<u8>,
}

pub(super) fn cleanup_staged_creates(staged: &[StagedCreate]) {
    for entry in staged {
        let _ = fs::remove_file(&entry.temporary);
    }
}

pub(super) fn create_immutable_batch(entries: Vec<(PathBuf, Vec<u8>)>) -> Result<usize, AppError> {
    if entries.is_empty() {
        return Ok(0);
    }

    let mut prepared = Vec::with_capacity(entries.len());
    let mut parents = BTreeSet::new();
    for (path, bytes) in entries {
        if let Some((_, existing_bytes)) = read_record_if_present(&path)? {
            if existing_bytes == bytes {
                continue;
            }
            return Err(AppError::Conflict(format!(
                "NCNX key already exists with a different immutable pointer at {}",
                path.display()
            )));
        }
        let parent = path.parent().ok_or_else(|| {
            AppError::InternalServerError("NCNX publication path has no parent".into())
        })?;
        parents.insert(parent.to_path_buf());
        prepared.push((path, bytes));
    }
    if prepared.is_empty() {
        return Ok(0);
    }

    for parent in &parents {
        fs::create_dir_all(parent).map_err(|err| {
            AppError::Io(err, format!("create NCNX directory {}", parent.display()))
        })?;
    }

    let mut staged = Vec::with_capacity(prepared.len());
    for (path, bytes) in prepared {
        let parent = path.parent().expect("validated NCNX parent");
        let temporary = parent.join(format!(".n2-publish-{}", Ulid::new()));
        let mut file = match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
        {
            Ok(file) => file,
            Err(err) => {
                cleanup_staged_creates(&staged);
                return Err(AppError::Io(
                    err,
                    format!("create NCNX staging file {}", temporary.display()),
                ));
            }
        };
        if let Err(err) = file.write_all(&bytes) {
            let _ = fs::remove_file(&temporary);
            cleanup_staged_creates(&staged);
            return Err(AppError::Io(
                err,
                format!("write NCNX staging file {}", temporary.display()),
            ));
        }
        staged.push(StagedCreate {
            path,
            temporary,
            file,
            bytes,
        });
    }

    if let Some((path, err)) = staged.par_iter().find_map_any(|entry| {
        entry
            .file
            .sync_data()
            .err()
            .map(|error| (entry.temporary.clone(), error))
    }) {
        cleanup_staged_creates(&staged);
        return Err(AppError::Io(
            err,
            format!("sync NCNX staging file {}", path.display()),
        ));
    }

    let mut changed_parents = BTreeSet::new();
    let mut changed = 0usize;
    for entry in &staged {
        match fs::hard_link(&entry.temporary, &entry.path) {
            Ok(()) => {
                changed = changed.saturating_add(1);
                if let Some(parent) = entry.path.parent() {
                    changed_parents.insert(parent.to_path_buf());
                }
            }
            Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                let existing = read_record_if_present(&entry.path)?.ok_or_else(|| {
                    AppError::Conflict(format!(
                        "NCNX destination disappeared during batch publication at {}",
                        entry.path.display()
                    ))
                })?;
                if existing.1 != entry.bytes {
                    cleanup_staged_creates(&staged);
                    sync_directories(&changed_parents)?;
                    return Err(AppError::Conflict(format!(
                        "NCNX key already exists with a different immutable pointer at {}",
                        entry.path.display()
                    )));
                }
            }
            Err(err) => {
                cleanup_staged_creates(&staged);
                sync_directories(&changed_parents)?;
                return Err(AppError::Io(
                    err,
                    format!("publish immutable NCNX {}", entry.path.display()),
                ));
            }
        }
    }
    cleanup_staged_creates(&staged);
    sync_directories(&changed_parents)?;
    Ok(changed)
}

pub(super) fn sync_directories(directories: &BTreeSet<PathBuf>) -> Result<(), AppError> {
    if let Some((path, err)) = directories.par_iter().find_map_any(|path| {
        File::open(path)
            .and_then(|directory| directory.sync_all())
            .err()
            .map(|error| (path.clone(), error))
    }) {
        return Err(AppError::Io(
            err,
            format!("sync NCNX directory {}", path.display()),
        ));
    }
    Ok(())
}

pub(super) fn create_immutable(path: &Path, bytes: &[u8]) -> Result<bool, AppError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            AppError::Io(err, format!("create NCNX directory {}", parent.display()))
        })?;
    }
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError("NCNX publication path has no parent".into())
    })?;
    let temporary = parent.join(format!(".n2-publish-{}", Ulid::new()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|err| {
            AppError::Io(
                err,
                format!("create NCNX staging file {}", temporary.display()),
            )
        })?;
    // NCNX publication durably syncs the file data first, then the parent
    // directory after creating the immutable hard link.
    if let Err(err) = file.write_all(bytes).and_then(|_| file.sync_data()) {
        let _ = fs::remove_file(&temporary);
        return Err(AppError::Io(
            err,
            format!("write NCNX staging file {}", temporary.display()),
        ));
    }
    drop(file);
    match fs::hard_link(&temporary, path) {
        Ok(()) => {
            let _ = fs::remove_file(&temporary);
            sync_parent(path)?;
            Ok(true)
        }
        Err(err) if err.kind() == ErrorKind::AlreadyExists => {
            let _ = fs::remove_file(&temporary);
            let existing = fs::read(path).map_err(|err| {
                AppError::Io(err, format!("read existing NCNX {}", path.display()))
            })?;
            if existing == bytes {
                Ok(false)
            } else {
                Err(AppError::Conflict(format!(
                    "NCNX key already exists with a different immutable pointer at {}",
                    path.display()
                )))
            }
        }
        Err(err) => {
            let _ = fs::remove_file(&temporary);
            Err(AppError::Io(
                err,
                format!("publish immutable NCNX {}", path.display()),
            ))
        }
    }
}

pub(super) fn remove_if_exact(path: &Path, expected_bytes: &[u8]) -> Result<bool, AppError> {
    let (removed, parent) = remove_if_exact_deferred(path, expected_bytes)?;
    if let Some(parent) = parent {
        sync_directories(&BTreeSet::from([parent]))?;
    }
    Ok(removed)
}

pub(super) fn remove_if_exact_deferred(
    path: &Path,
    expected_bytes: &[u8],
) -> Result<(bool, Option<PathBuf>), AppError> {
    let mut opened = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok((false, None)),
        Err(err) => return Err(AppError::Io(err, format!("read NCNX {}", path.display()))),
    };
    let opened_metadata = opened
        .metadata()
        .map_err(|err| AppError::Io(err, format!("stat NCNX {}", path.display())))?;
    let mut existing = Vec::new();
    opened
        .read_to_end(&mut existing)
        .map_err(|err| AppError::Io(err, format!("read NCNX {}", path.display())))?;
    if existing != expected_bytes {
        return Err(AppError::Conflict(format!(
            "refused to remove changed NCNX at {}",
            path.display()
        )));
    }
    let parent = path
        .parent()
        .ok_or_else(|| AppError::InternalServerError("NCNX removal path has no parent".into()))?;
    let removed = parent.join(format!(".n2-remove-{}", Ulid::new()));
    match fs::rename(path, &removed) {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok((false, None)),
        Err(err) => {
            return Err(AppError::Io(
                err,
                format!("isolate NCNX {}", path.display()),
            ));
        }
    }
    let removed_metadata = fs::metadata(&removed)
        .map_err(|err| AppError::Io(err, format!("stat isolated NCNX {}", removed.display())))?;
    #[cfg(unix)]
    let same_file = opened_metadata.dev() == removed_metadata.dev()
        && opened_metadata.ino() == removed_metadata.ino();
    #[cfg(not(unix))]
    let same_file = fs::read(&removed).is_ok_and(|bytes| bytes == expected_bytes);
    if !same_file {
        match fs::hard_link(&removed, path) {
            Ok(()) => {
                fs::remove_file(&removed).map_err(|err| {
                    AppError::Io(
                        err,
                        format!("remove restored NCNX link {}", removed.display()),
                    )
                })?;
                sync_parent(path)?;
            }
            Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                return Err(AppError::Conflict(format!(
                    "NCNX changed during exact removal; preserved displaced record at {}",
                    removed.display()
                )));
            }
            Err(err) => {
                return Err(AppError::Io(
                    err,
                    format!("restore changed NCNX {}", path.display()),
                ));
            }
        }
        return Err(AppError::Conflict(format!(
            "NCNX changed during exact removal at {}",
            path.display()
        )));
    }
    fs::remove_file(&removed)
        .map_err(|err| AppError::Io(err, format!("remove isolated NCNX {}", removed.display())))?;
    Ok((true, Some(parent.to_path_buf())))
}

pub(super) fn replace_durable(path: &Path, bytes: &[u8]) -> Result<bool, AppError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            AppError::Io(
                err,
                format!("create NCNX repair directory {}", parent.display()),
            )
        })?;
    }
    if fs::read(path).is_ok_and(|existing| existing == bytes) {
        return Ok(false);
    }
    let parent = path
        .parent()
        .ok_or_else(|| AppError::InternalServerError("NCNX repair path has no parent".into()))?;
    let temporary = parent.join(format!(".n2-repair-{}", Ulid::new()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|err| AppError::Io(err, format!("create NCNX repair {}", temporary.display())))?;
    if let Err(err) = file.write_all(bytes).and_then(|_| file.sync_data()) {
        let _ = fs::remove_file(&temporary);
        return Err(AppError::Io(
            err,
            format!("write NCNX repair {}", temporary.display()),
        ));
    }
    fs::rename(&temporary, path)
        .map_err(|err| AppError::Io(err, format!("publish NCNX repair {}", path.display())))?;
    sync_parent(path)?;
    Ok(true)
}

pub(super) fn sync_parent(path: &Path) -> Result<(), AppError> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|err| AppError::Io(err, format!("sync NCNX directory {}", parent.display())))
}
