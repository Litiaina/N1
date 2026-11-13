//! A single metadata snapshot selects immutable versions into a bounded HDD
//! manifest. Renames and subsequent head updates cannot change a captured job.
use super::{DownloadArchiveRequest, length::ManifestSummary, models::ExportManifestEntry};
use crate::{Error, MetadataEngine, Result, namespace::Namespace};
use std::{collections::BTreeSet, io::Write, path::Path};
pub(super) fn validate_archive_path(path: &str) -> Result<()> {
    if path.is_empty()
        || path.len() > 4096
        || path.starts_with('/')
        || path.contains('\\')
        || path.chars().any(char::is_control)
        || path
            .split('/')
            .any(|p| p.is_empty() || p == "." || p == "..")
    {
        return Err(Error::Invalid(
            "object path cannot be represented safely in an archive".into(),
        ));
    }
    Ok(())
}
pub(super) fn capture_with_cancel(
    db: &MetadataEngine,
    fragment: &str,
    request: &DownloadArchiveRequest,
    path: &Path,
    cancelled: Option<&std::sync::atomic::AtomicBool>,
) -> Result<ManifestSummary> {
    let id = n1_link::PoolId(
        ulid::Ulid::from_string(fragment)
            .map_err(|_| Error::Invalid("invalid export fragment".into()))?
            .to_bytes(),
    );
    let view = db.snapshot();
    let temp = path.with_extension("partial");
    let mut options = std::fs::OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = std::io::BufWriter::new(options.open(&temp)?);
    let mut summary = ManifestSummary::default();
    let normalize = |s: &str| -> Result<String> {
        let s = crate::namespace::paths::normalize_path(s)
            .map_err(|e| Error::Invalid(e.to_string()))?;
        if !s.is_empty() {
            validate_archive_path(&s)?;
        }
        Ok(s)
    };
    let mut prefixes = if request.export_all {
        vec![String::new()]
    } else {
        request
            .prefix
            .iter()
            .chain(request.prefixes.iter())
            .map(|s| normalize(s))
            .collect::<Result<Vec<_>>>()?
    };
    prefixes.sort();
    prefixes.dedup();
    let mut roots: Vec<String> = Vec::new();
    for p in prefixes {
        if !roots
            .iter()
            .any(|r| r.is_empty() || p == *r || p.starts_with(&format!("{r}/")))
        {
            roots.push(p);
        }
    }
    let roots: Vec<String> = roots
        .into_iter()
        .map(|p| if p.is_empty() { p } else { format!("{p}/") })
        .collect();
    let mut names = BTreeSet::new();
    let mut emit = |entry: ExportManifestEntry| -> Result<()> {
        if cancelled.is_some_and(|cancel| cancel.load(std::sync::atomic::Ordering::Acquire)) {
            return Err(Error::Closed);
        }
        validate_archive_path(&entry.archive_path)?;
        let job = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| Error::Invalid("manifest has no job identity".into()))?;
        Namespace::new(db.clone()).pin(
            &crate::namespace::store::lifecycle::Target {
                fragment: id,
                object: entry.object_id.clone(),
                version: entry.version_id.clone(),
            },
            job,
        )?;
        let line = serde_json::to_vec(&entry)
            .map_err(|_| Error::Invalid("cannot encode export manifest".into()))?;
        file.write_all(&line)?;
        file.write_all(b"\n")?;
        summary.record(&entry)?;
        Ok(())
    };
    for key in &request.keys {
        let key = normalize(key)?;
        if roots.iter().any(|p| key.starts_with(p)) {
            continue;
        }
        if !names.insert(key.clone()) {
            continue;
        }
        let v = Namespace::resolve_from_snapshot(&view, id, &key, None)?;
        emit(ExportManifestEntry {
            archive_path: key,
            object_id: v.metadata.object_id,
            version_id: v.metadata.version_id,
            source_offset_bytes: 0,
            size_bytes: v.metadata.total_size_bytes,
        })?;
    }
    for entry in &request.entries {
        validate_archive_path(&entry.archive_path)?;
        if !names.insert(entry.archive_path.clone()) {
            return Err(Error::NameConflict("duplicate archive path".into()));
        }
        let v = Namespace::resolve_from_snapshot(
            &view,
            id,
            &entry.object_key,
            entry.version_id.as_deref(),
        )?;
        let offset = entry.offset_bytes.unwrap_or(0);
        let length = entry
            .length_bytes
            .unwrap_or(v.metadata.total_size_bytes.saturating_sub(offset));
        if offset
            .checked_add(length)
            .is_none_or(|end| end > v.metadata.total_size_bytes)
        {
            return Err(Error::Invalid(
                "export entry range exceeds immutable source".into(),
            ));
        }
        emit(ExportManifestEntry {
            archive_path: entry.archive_path.clone(),
            object_id: v.metadata.object_id,
            version_id: v.metadata.version_id,
            source_offset_bytes: offset,
            size_bytes: length,
        })?;
    }
    for prefix in roots {
        let mut after = None;
        loop {
            let (rows, cursor) =
                Namespace::files_from_snapshot(&view, id, &prefix, after.as_ref())?;
            if cursor.is_none() {
                break;
            }
            after = cursor;
            for (key, v) in rows {
                let path = key.identifier().to_owned();
                if names.contains(&path) {
                    return Err(Error::NameConflict("duplicate archive path".into()));
                }
                emit(ExportManifestEntry {
                    archive_path: path,
                    object_id: v.object_id,
                    version_id: v.version_id,
                    source_offset_bytes: 0,
                    size_bytes: v.total_size_bytes,
                })?;
            }
        }
    }
    file.flush()?;
    file.get_ref().sync_all()?;
    drop(file);
    std::fs::rename(temp, path)?;
    std::fs::File::open(
        path.parent()
            .ok_or_else(|| Error::Invalid("manifest has no parent".into()))?,
    )?
    .sync_all()?;
    Ok(summary)
}

/// The NOA manifest is newline-delimited keys, optionally VERSION<TAB>KEY.
/// Read one bounded line at a time, against one metadata snapshot. No Vec or
/// deduplication set grows with the caller's inventory.
pub(super) fn capture_lines(
    db: &MetadataEngine,
    fragment: &str,
    input: &Path,
    path: &Path,
    cancelled: &std::sync::atomic::AtomicBool,
) -> Result<ManifestSummary> {
    use std::io::{BufRead, Read};
    let id = n1_link::PoolId(
        ulid::Ulid::from_string(fragment)
            .map_err(|_| Error::Invalid("invalid archive fragment".into()))?
            .to_bytes(),
    );
    let view = db.snapshot();
    let job = path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| Error::Invalid("invalid archive identity".into()))?;
    let temporary = path.with_extension("partial");
    let mut options = std::fs::OpenOptions::new();
    use std::os::unix::fs::OpenOptionsExt;
    options
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW);
    let mut output = std::io::BufWriter::new(options.open(&temporary)?);
    let mut input = std::io::BufReader::new(std::fs::File::open(input)?);
    let mut line = Vec::with_capacity(8192);
    let mut summary = ManifestSummary::default();
    loop {
        if cancelled.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::Closed);
        }
        line.clear();
        let count = input.by_ref().take(8193).read_until(b'\n', &mut line)?;
        if count == 0 {
            break;
        }
        if line.len() > 8192 {
            return Err(Error::Invalid(
                "archive manifest line exceeds 8192 bytes".into(),
            ));
        }
        let text = std::str::from_utf8(&line)
            .map_err(|_| Error::Invalid("archive manifest must be UTF-8".into()))?
            .trim();
        if text.is_empty() {
            continue;
        }
        let (version, key) = text
            .split_once('\t')
            .map_or((None, text), |(version, key)| (Some(version), key));
        let key = crate::namespace::paths::normalize_path(key)
            .map_err(|e| Error::Invalid(e.to_string()))?;
        validate_archive_path(&key)?;
        let stored = Namespace::resolve_from_snapshot(&view, id, &key, version)?;
        let entry = ExportManifestEntry {
            archive_path: key,
            object_id: stored.metadata.object_id,
            version_id: stored.metadata.version_id,
            source_offset_bytes: 0,
            size_bytes: stored.metadata.total_size_bytes,
        };
        Namespace::new(db.clone()).pin(
            &crate::namespace::store::lifecycle::Target {
                fragment: id,
                object: entry.object_id.clone(),
                version: entry.version_id.clone(),
            },
            job,
        )?;
        serde_json::to_writer(&mut output, &entry)
            .map_err(|_| Error::Invalid("cannot encode archive manifest".into()))?;
        output.write_all(b"\n")?;
        summary.record(&entry)?;
    }
    output.flush()?;
    output.get_ref().sync_all()?;
    drop(output);
    std::fs::rename(temporary, path)?;
    std::fs::File::open(
        path.parent()
            .ok_or_else(|| Error::Invalid("manifest has no parent".into()))?,
    )?
    .sync_all()?;
    Ok(summary)
}
