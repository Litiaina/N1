use super::*;

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
    for dir in session_roots(&manifest.fragment, &manifest.version_id) {
        write_atomic(&dir.join(MANIFEST_FILE), &bytes)?;
    }
    Ok(())
}

fn session_dir(root: &Path, fragment: &str, version_id: &str) -> PathBuf {
    fragment_root_on(root, fragment)
        .join(UPLOADS_DIR)
        .join(version_id)
}

fn session_roots(fragment: &str, version_id: &str) -> Vec<PathBuf> {
    ObjectPathManager::new()
        .get_session_root(fragment, version_id)
        .into_iter()
        .map(|root| session_dir(&root, fragment, version_id))
        .collect()
}

fn load_session_row(fragment: &str, version_id: &str) -> Result<Option<SessionRow>, AppError> {
    for dir in session_roots(fragment, version_id) {
        let path = dir.join(MANIFEST_FILE);
        if path.exists() {
            return decode(&fs::read(path).map_err(|e| io_err(e, "read upload manifest"))?)
                .map(Some);
        }
    }
    Ok(None)
}

pub fn commit_session_part(
    fragment: &str,
    version_id: &str,
    part: &PartMetadata,
) -> Result<(), AppError> {
    let bytes = encode(part)?;
    for dir in session_roots(fragment, version_id) {
        write_atomic(
            &dir.join(format!("{}{:020}.ncdx", PART_META_PREFIX, part.part_index)),
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
        fs::create_dir_all(&dir).map_err(|e| io_err(e, "create upload receipt dir"))?;
        remove_receipts_for_part(&dir, receipt.part_index)?;
        if let Some(checksum) = receipt.checksum {
            create_empty_marker(&dir.join(part_receipt_name(receipt.part_index, &checksum)))?;
        }
        for (shard_index, checksum) in receipt.shard_checksums.iter().copied().enumerate() {
            create_empty_marker(&dir.join(shard_receipt_name(
                receipt.part_index,
                shard_index,
                &checksum,
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
    for dir in session_roots(fragment, version_id) {
        if !dir.is_dir() {
            continue;
        }
        let mut shard_receipts = BTreeMap::<u64, BTreeMap<usize, Sha256Digest>>::new();
        let mut part_checksums = BTreeMap::<u64, Sha256Digest>::new();
        for name in read_dir_names(&dir)? {
            let path = dir.join(&name);
            if name.starts_with(PART_META_PREFIX) && name.ends_with(".ncdx") {
                let part: PartMetadata =
                    decode(&fs::read(path).map_err(|e| io_err(e, "read upload part metadata"))?)?;
                state.parts.insert(part.part_index, part);
            } else if let Some((part_index, shard_index, checksum)) =
                parse_shard_receipt_name(&name)
            {
                shard_receipts
                    .entry(part_index)
                    .or_default()
                    .insert(shard_index, checksum);
            } else if let Some((part_index, checksum)) = parse_part_receipt_name(&name) {
                part_checksums.insert(part_index, checksum);
            }
        }
        push_receipts_from_filename(&mut state, shard_receipts, part_checksums);
        break;
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
    for dir in session_roots(fragment, version_id) {
        if dir.exists() {
            fs::remove_dir_all(&dir).map_err(|e| io_err(e, "remove upload session"))?;
            removed = true;
        }
    }
    Ok(removed)
}

pub fn list_active_sessions(fragment: &str) -> Result<Vec<IndexSessionState>, AppError> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for root in existing_fragment_roots(fragment) {
        let uploads = root.join(UPLOADS_DIR);
        if !uploads.is_dir() {
            continue;
        }
        for version_id in read_dir_names(&uploads)? {
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
    for dir in session_roots(fragment, version_id) {
        let path = dir.join(BINDING_FILE);
        if path.exists() {
            return decode(&fs::read(path).map_err(|e| io_err(e, "read pending upload binding"))?)
                .map(Some);
        }
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
