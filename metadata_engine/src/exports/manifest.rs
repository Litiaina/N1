//! A single metadata snapshot selects immutable versions into a bounded HDD
//! manifest. Renames and subsequent head updates cannot change a captured job.
use super::{DownloadArchiveRequest, models::ExportManifestEntry};
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
pub(super) fn capture(
    db: &MetadataEngine,
    fragment: &str,
    request: &DownloadArchiveRequest,
    path: &Path,
) -> Result<(u64, u64)> {
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
    let (mut count, mut bytes) = (0u64, 0u64);
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
        count += 1;
        bytes = bytes
            .checked_add(entry.size_bytes)
            .ok_or_else(|| Error::Invalid("export source size overflow".into()))?;
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
    Ok((count, bytes))
}
