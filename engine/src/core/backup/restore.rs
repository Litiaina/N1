use super::format::{Kind, MAX_METADATA_BYTES, Record, Root, Snapshot};
use super::fs::{read_regular, restore_dir, safe_relative};
use super::repository::Repository;
use super::{Result, invalid};
use crate::core::instance_lease::InstanceLease;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{OpenOptionsExt, symlink};
use std::path::{Path, PathBuf};
use std::time::{Duration, UNIX_EPOCH};

pub(crate) fn validate_link(path: &str, target: &str) -> Result<()> {
    let identity = if path.starts_with(".fragment/") {
        target.strip_prefix("../@f_")
    } else {
        target.strip_prefix("@o_")
    };
    if identity.is_none_or(|id| {
        id.is_empty()
            || id.len() > 128
            || !id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
    }) {
        return Err(invalid("backup refuses non-N1 symlink targets"));
    }
    Ok(())
}

fn validate(record: &Record, snapshot: &Snapshot) -> Result<()> {
    validate_record(record, snapshot.roots.len())
}

pub(crate) fn validate_record(record: &Record, roots: usize) -> Result<()> {
    safe_relative(&record.path)?;
    if record.root >= roots || record.modified_nanos >= 1_000_000_000 || record.modified_seconds < 0
    {
        return Err(invalid("invalid backup record root/timestamp"));
    }
    if super::restore_guard::reserved(&record.path) {
        return Err(invalid(
            "snapshot attempts to replace an engine safety marker",
        ));
    }
    if let Kind::Link { target } = &record.kind {
        validate_link(&record.path, target)?;
    }
    Ok(())
}

pub fn verify(repo: &Repository, snapshot: &Snapshot) -> Result<()> {
    super::capture::validate_roots(&snapshot.roots)?;
    let mut total = 0u64;
    repo.records(snapshot, |record| {
        validate(&record, snapshot)?;
        if let Kind::File { size, blocks } = record.kind {
            if super::metadata::requires_remap(&record.path) && size > MAX_METADATA_BYTES as u64 {
                return Err(invalid(
                    "internal N1 metadata exceeds the portable backup limit (64 MiB)",
                ));
            }
            let mut actual = 0u64;
            for hash in blocks {
                actual = actual
                    .checked_add(repo.get(&hash)?.len() as u64)
                    .ok_or_else(|| invalid("size overflow"))?;
            }
            if actual != size {
                return Err(invalid("backup file length mismatch"));
            }
            total = total
                .checked_add(size)
                .ok_or_else(|| invalid("snapshot size overflow"))?;
        }
        Ok(())
    })?;
    if total != snapshot.source_bytes {
        return Err(invalid("snapshot byte count mismatch"));
    }
    Ok(())
}

fn ensure_parents(root: &Path, relative: &Path) -> Result<PathBuf> {
    let mut parent = root.to_path_buf();
    if let Some(parts) = relative.parent() {
        for part in parts {
            parent.push(part);
            restore_dir(&parent)?;
        }
    }
    Ok(root.join(relative))
}

fn same_file(a: &Path, b: &Path) -> Result<bool> {
    let mut a = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(a)?;
    let mut b = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(b)?;
    if !b.metadata()?.is_file() || a.metadata()?.len() != b.metadata()?.len() {
        return Ok(false);
    }
    let mut x = vec![0; 256 * 1024];
    let mut y = vec![0; x.len()];
    loop {
        let n = a.read(&mut x)?;
        if n == 0 {
            break;
        }
        b.read_exact(&mut y[..n])?;
        if x[..n] != y[..n] {
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn restore(
    repo: &Repository,
    snapshot: &Snapshot,
    target: Vec<Root>,
    resume: bool,
) -> Result<()> {
    super::capture::validate_roots(&target)?;
    if snapshot.roots.len() != target.len()
        || snapshot
            .roots
            .iter()
            .zip(&target)
            .any(|(a, b)| a.index != b.index || a.blob != b.blob)
    {
        return Err(invalid(
            "restore requires the same ordered root roles/count; EC recoding is a separate operation",
        ));
    }
    let paths = target
        .iter()
        .map(|r| PathBuf::from(&r.path))
        .collect::<Vec<_>>();
    for root in &paths {
        if repo.path.starts_with(root) || root.starts_with(&repo.path) {
            return Err(invalid("restore roots overlap backup repository"));
        }
    }
    // Authenticate everything before creating an engine-visible restore marker.
    verify(repo, snapshot)?;
    let marker = serde_json::to_vec(&(snapshot.id.as_str(), repo.key.id.as_str(), &target))?;
    let _lease = InstanceLease::acquire(&paths, false)?;
    super::restore_guard::prepare(&paths, &marker, resume)?;
    super::restore_guard::inventory(repo, snapshot, &paths)?;
    super::restore_guard::audit(&paths)?;
    repo.records(snapshot, |record| {
        validate(&record, snapshot)?;
        let path = ensure_parents(&paths[record.root], safe_relative(&record.path)?)?;
        match &record.kind {
            Kind::Directory => restore_dir(&path)?,
            Kind::Link { target } => match fs::symlink_metadata(&path) {
                Ok(meta)
                    if meta.file_type().is_symlink()
                        && fs::read_link(&path)? == Path::new(target) => {}
                Ok(_) => return Err(invalid("restore found a conflicting namespace entry")),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    symlink(target, &path)?;
                }
                Err(e) => return Err(e.into()),
            },
            Kind::File { size, blocks } => {
                let tmp = paths[record.root]
                    .join(super::restore_guard::WORK)
                    .join(format!("temporary-{}", ulid::Ulid::new()));
                let outcome = (|| -> Result<()> {
                    let mut file = OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .mode(0o600)
                        .open(&tmp)?;
                    let mut count = 0;
                    for hash in blocks {
                        let bytes = repo.get(hash)?;
                        file.write_all(&bytes)?;
                        count += bytes.len() as u64;
                    }
                    if count != *size {
                        return Err(invalid("restored file length mismatch"));
                    }
                    if super::metadata::requires_remap(&record.path) {
                        file.flush()?;
                        let mapped = super::metadata::remap(
                            &record.path,
                            read_regular(&tmp, MAX_METADATA_BYTES)?,
                            &snapshot.roots,
                            &target,
                        )?;
                        file.set_len(0)?;
                        use std::io::{Seek, SeekFrom};
                        file.seek(SeekFrom::Start(0))?;
                        file.write_all(&mapped)?;
                    }
                    let times = fs::FileTimes::new().set_modified(
                        UNIX_EPOCH
                            + Duration::new(record.modified_seconds as u64, record.modified_nanos),
                    );
                    file.set_times(times)?;
                    file.sync_all()?;
                    match fs::symlink_metadata(&path) {
                        Ok(_) if resume && same_file(&tmp, &path)? => {
                            let existing = OpenOptions::new()
                                .read(true)
                                .custom_flags(libc::O_NOFOLLOW)
                                .open(&path)?;
                            existing.set_times(times)?;
                            existing.sync_all()?;
                        }
                        Ok(_) => return Err(invalid("restore would overwrite an existing file")),
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            fs::hard_link(&tmp, &path)?
                        }
                        Err(e) => return Err(e.into()),
                    }
                    Ok(())
                })();
                let _ = fs::remove_file(&tmp);
                outcome?;
            }
        }
        Ok(())
    })?;
    // Files have been individually synchronized. Now make ALL directory entries
    // durable, after their children exist, before committing the restore. A crash
    // before this boundary may lose unpublished work; resume reconstructs it.
    // Per-entry parent fsyncs are unnecessary behind persistent start fences.
    repo.records(snapshot, |record| {
        if matches!(record.kind, Kind::Directory) {
            let path = paths[record.root].join(safe_relative(&record.path)?);
            let file = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW)
                .open(path)?;
            file.set_times(fs::FileTimes::new().set_modified(
                UNIX_EPOCH + Duration::new(record.modified_seconds as u64, record.modified_nanos),
            ))?;
            file.sync_all()?;
        }
        Ok(())
    })?;
    for root in &paths {
        File::open(root)?.sync_all()?;
    }
    super::restore_guard::audit(&paths)?;
    super::restore_guard::complete(&paths, &marker)?;
    Ok(())
}
