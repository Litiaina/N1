//! Durable multipart session manifests, bindings, and receipt persistence.

use super::*;

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SessionFsBoundary {
    BindingRead,
    ManifestRead,
    SessionEnumerate,
    SessionRemove,
    UploadsEnumerate,
    PartRead,
    ReceiptWrite,
}

#[cfg(test)]
type SessionFsHook = std::rc::Rc<dyn Fn(SessionFsBoundary, &Path)>;

#[cfg(test)]
thread_local! {
    static SESSION_FS_HOOK: std::cell::RefCell<Option<SessionFsHook>> = const {
        std::cell::RefCell::new(None)
    };
}

#[cfg(test)]
pub(super) struct SessionFsHookGuard {
    previous: Option<SessionFsHook>,
}

#[cfg(test)]
impl Drop for SessionFsHookGuard {
    fn drop(&mut self) {
        SESSION_FS_HOOK.with(|slot| *slot.borrow_mut() = self.previous.take());
    }
}

#[cfg(test)]
pub(super) fn test_session_fs_hook(
    hook: impl Fn(SessionFsBoundary, &Path) + 'static,
) -> SessionFsHookGuard {
    let previous = SESSION_FS_HOOK.with(|slot| slot.replace(Some(std::rc::Rc::new(hook))));
    SessionFsHookGuard { previous }
}

#[cfg(test)]
fn session_fs_boundary(boundary: SessionFsBoundary, path: &Path) {
    // Invoke outside the RefCell borrow so a test may exercise another session
    // operation at the boundary without introducing a test-only lock.
    let hook = SESSION_FS_HOOK.with(|slot| slot.borrow().clone());
    if let Some(hook) = hook {
        hook(boundary, path);
    }
}

pub fn open_session(
    manifest: &UploadSessionManifest,
    reservation: Option<ReservationReceipt>,
) -> Result<(), AppError> {
    ensure_fragment_dirs(&manifest.fragment)?;
    let now = Utc::now().timestamp();
    let row = SessionRow {
        manifest: manifest.clone(),
        reservation,
        last_activity_at: now,
        finalized_at: None,
        aborted_at: None,
    };
    let bytes = encode(&row)?;
    let placements = if crate::core::storage_pool::configured().is_empty()
        || manifest.placement_roots.is_empty()
    {
        index_roots()
    } else {
        manifest
            .placement_roots
            .iter()
            .map(|root| crate::core::storage_pool::resolve_address(root))
            .collect::<Result<Vec<_>, _>>()?
    };
    for root in placements {
        write_atomic(
            &fragment_root_on(&root, &manifest.fragment)
                .join(UPLOADS_DIR)
                .join(&manifest.version_id)
                .join(MANIFEST_FILE),
            &bytes,
        )?;
    }
    Ok(())
}

fn session_dir(root: &Path, fragment: &str, version_id: &str) -> PathBuf {
    fragment_root_on(root, fragment)
        .join(UPLOADS_DIR)
        .join(version_id)
}

fn session_dirs(fragment: &str, version_id: &str) -> Vec<PathBuf> {
    index_roots()
        .into_iter()
        .map(|root| session_dir(&root, fragment, version_id))
        .collect()
}

fn session_roots(fragment: &str, version_id: &str) -> Vec<PathBuf> {
    session_dirs(fragment, version_id)
        .into_iter()
        .filter(|dir| dir.is_dir())
        .collect()
}

fn load_session_row(fragment: &str, version_id: &str) -> Result<Option<SessionRow>, AppError> {
    let mut selected: Option<SessionRow> = None;
    let mut corrupt = None;
    for dir in session_dirs(fragment, version_id) {
        let path = dir.join(MANIFEST_FILE);
        #[cfg(test)]
        session_fs_boundary(SessionFsBoundary::ManifestRead, &path);
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            // Finalization/cancellation may remove a listed session at any
            // point. Its absence is not a corrupt manifest or an I/O fault.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                corrupt = Some(io_err(error, "read upload manifest"));
                continue;
            }
        };
        let row: SessionRow = match decode(&bytes) {
            Ok(row) => row,
            Err(error) => {
                corrupt = Some(error);
                continue;
            }
        };
        if let Some(current) = &selected {
            if encode(&current.manifest)? != encode(&row.manifest)? {
                return Err(AppError::Conflict(
                    "conflicting multipart placement manifests".into(),
                ));
            }
            if current.last_activity_at > row.last_activity_at {
                continue;
            }
        }
        selected = Some(row);
    }
    if selected.is_none() {
        if let Some(error) = corrupt {
            return Err(error);
        }
    }
    Ok(selected)
}

pub fn commit_session_part(
    fragment: &str,
    version_id: &str,
    part: &PartMetadata,
) -> Result<(), AppError> {
    let bytes = encode(part)?;
    for dir in session_roots(fragment, version_id) {
        write_atomic(
            &dir.join(format!("{}{:020}.ndx", PART_META_PREFIX, part.part_index)),
            &bytes,
        )?;
    }
    Ok(())
}

pub fn commit_session_part_receipt(
    fragment: &str,
    version_id: &str,
    receipt: &SessionPartReceipt,
) -> Result<(), AppError> {
    for dir in session_roots(fragment, version_id) {
        #[cfg(test)]
        session_fs_boundary(SessionFsBoundary::ReceiptWrite, &dir);
        // These directories were created by open_session. A late part must not
        // recreate a session retired by finalization or cancellation.
        remove_receipts_for_part(&dir, receipt.part_index)?;
        if let Some(checksum) = receipt.checksum {
            create_empty_marker(&dir.join(part_receipt_name(receipt.part_index, checksum)))?;
        }
        for (shard_index, checksum) in receipt.shard_checksums.iter().copied().enumerate() {
            create_empty_marker(&dir.join(shard_receipt_name(
                receipt.part_index,
                shard_index,
                checksum,
            )))?;
        }
        sync_dir(&dir);
    }
    Ok(())
}

pub fn load_session(
    fragment: &str,
    version_id: &str,
) -> Result<Option<IndexSessionState>, AppError> {
    let Some(row) = load_session_row(fragment, version_id)? else {
        return Ok(None);
    };
    let mut state = row.into_state();
    let mut shard_receipts = BTreeMap::<u64, BTreeMap<usize, u32>>::new();
    let mut part_checksums = BTreeMap::<u64, u32>::new();
    let mut corrupt_part = None;
    let mut enumerated_any = false;
    for dir in session_dirs(fragment, version_id) {
        let directory_metadata = match fs::metadata(&dir) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(io_err(error, "inspect upload session directory")),
        };
        if !directory_metadata.is_dir() {
            continue;
        }
        // Receipt creation/atomic replacement already advances this directory's
        // mtime. Reuse the existing directory metadata check: no per-part stat,
        // manifest rewrite, or new durable heartbeat is needed for idle expiry.
        let activity = directory_metadata
            .modified()
            .map_err(|error| io_err(error, "read upload session activity"))?
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        state.last_activity_at = state
            .last_activity_at
            .max(i64::try_from(activity).unwrap_or(i64::MAX));
        #[cfg(test)]
        session_fs_boundary(SessionFsBoundary::SessionEnumerate, &dir);
        let names = match read_dir_names(&dir) {
            Ok(names) => names,
            Err(AppError::NotFound) => continue,
            Err(error) => return Err(error),
        };
        enumerated_any = true;
        for name in names {
            let path = dir.join(&name);
            if name.starts_with(PART_META_PREFIX) && name.ends_with(".ndx") {
                #[cfg(test)]
                session_fs_boundary(SessionFsBoundary::PartRead, &path);
                let bytes = match fs::read(path) {
                    Ok(bytes) => bytes,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => {
                        corrupt_part = Some(io_err(error, "read upload part metadata"));
                        continue;
                    }
                };
                let part: PartMetadata = match decode(&bytes) {
                    Ok(part) => part,
                    Err(error) => {
                        corrupt_part = Some(error);
                        continue;
                    }
                };
                if let Some(existing) = state.parts.get(&part.part_index) {
                    if encode(existing)? != encode(&part)? {
                        return Err(AppError::Conflict(
                            "conflicting multipart part metadata".into(),
                        ));
                    }
                }
                state.parts.insert(part.part_index, part);
            } else if let Some((part_index, shard_index, checksum)) =
                parse_shard_receipt_name(&name)
            {
                let previous = shard_receipts
                    .entry(part_index)
                    .or_default()
                    .insert(shard_index, checksum);
                if previous.is_some_and(|old| old != checksum) {
                    return Err(AppError::Conflict(
                        "conflicting multipart shard receipts".into(),
                    ));
                }
            } else if let Some((part_index, checksum)) = parse_part_receipt_name(&name) {
                if part_checksums
                    .insert(part_index, checksum)
                    .is_some_and(|old| old != checksum)
                {
                    return Err(AppError::Conflict(
                        "conflicting multipart part receipts".into(),
                    ));
                }
            }
        }
    }
    // A previously read manifest is not resume authority after every session
    // directory has disappeared during finalization or cancellation.
    if !enumerated_any {
        return Ok(None);
    }
    push_receipts_from_filename(&mut state, shard_receipts, part_checksums);
    // A corrupt copy must not hide valid progress elsewhere. If no complete
    // part/receipt survived at all, retain the original failure, not an empty
    // resume state that could incorrectly authorize reclamation.
    if state.parts.is_empty() && state.part_receipts.is_empty() {
        if let Some(error) = corrupt_part {
            return Err(error);
        }
    }
    Ok(Some(state))
}

pub fn read_session_part(
    fragment: &str,
    version_id: &str,
    part_index: u64,
) -> Result<Option<PartMetadata>, AppError> {
    Ok(load_session(fragment, version_id)?.and_then(|state| state.parts.get(&part_index).cloned()))
}

pub fn load_session_part_receipts(
    fragment: &str,
    version_id: &str,
    manifest: &UploadSessionManifest,
) -> Result<Option<Vec<Option<SessionPartReceipt>>>, AppError> {
    let Some(state) = load_session(fragment, version_id)? else {
        return Ok(None);
    };
    let mut out = vec![None; manifest.expected_parts as usize];
    for (idx, receipt) in state.part_receipts {
        if let Some(slot) = out.get_mut(idx as usize) {
            *slot = Some(receipt);
        }
    }
    Ok(Some(out))
}

pub fn remove_session(fragment: &str, version_id: &str) -> Result<bool, AppError> {
    let mut removed = false;
    for dir in session_dirs(fragment, version_id) {
        #[cfg(test)]
        session_fs_boundary(SessionFsBoundary::SessionRemove, &dir);
        match fs::remove_dir_all(&dir) {
            Ok(()) => removed = true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_err(error, "remove upload session")),
        }
    }
    Ok(removed)
}

pub fn list_active_sessions(fragment: &str) -> Result<Vec<IndexSessionState>, AppError> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for root in existing_fragment_roots(fragment) {
        let uploads = root.join(UPLOADS_DIR);
        #[cfg(test)]
        session_fs_boundary(SessionFsBoundary::UploadsEnumerate, &uploads);
        let version_ids = match read_dir_names(&uploads) {
            Ok(names) => names,
            Err(AppError::NotFound) => continue,
            Err(error) => return Err(error),
        };
        for version_id in version_ids {
            if !seen.insert(version_id.clone()) {
                continue;
            }
            if let Some(state) = load_session(fragment, &version_id)? {
                if state.finalized_at.is_none() && state.aborted_at.is_none() {
                    out.push(state);
                }
            }
        }
    }
    Ok(out)
}

pub(crate) fn write_pending_binding(binding: &PendingUploadBinding) -> Result<(), AppError> {
    let bytes = encode(binding)?;
    for dir in session_roots(&binding.fragment, &binding.version_id) {
        write_atomic(&dir.join(BINDING_FILE), &bytes)?;
    }
    Ok(())
}

pub(crate) fn read_pending_binding(
    fragment: &str,
    version_id: &str,
) -> Result<Option<PendingUploadBinding>, AppError> {
    for dir in session_dirs(fragment, version_id) {
        let path = dir.join(BINDING_FILE);
        #[cfg(test)]
        session_fs_boundary(SessionFsBoundary::BindingRead, &path);
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(io_err(error, "read pending upload binding")),
        };
        return decode(&bytes).map(Some);
    }
    Ok(None)
}

pub(crate) fn list_pending_bindings(fragment: &str) -> Result<Vec<PendingUploadBinding>, AppError> {
    let mut out = Vec::new();
    for session in list_active_sessions(fragment)? {
        if let Some(binding) = read_pending_binding(fragment, &session.manifest.version_id)? {
            out.push(binding);
        }
    }
    Ok(out)
}

pub(crate) fn clear_pending_binding(fragment: &str, version_id: &str) -> Result<(), AppError> {
    for dir in session_roots(fragment, version_id) {
        let _ = remove_file_if_exists(&dir.join(BINDING_FILE))?;
    }
    Ok(())
}
