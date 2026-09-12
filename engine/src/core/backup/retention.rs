//! Explicit retention only. Hard-link pins keep garbage collection bounded in
//! memory and prevent removal of blocks used by another completed snapshot.
use super::repository::Repository;
use super::{Result, invalid};
use std::fs;
use std::os::unix::fs::MetadataExt;

pub fn forget(repo: &Repository, id: &str) -> Result<()> {
    repo.ensure_writable()?;
    repo.snapshot(id)?;
    if repo.list()?.len() <= 1 {
        return Err(invalid("refusing to remove the last recovery point"));
    }
    let source = repo.path.join("snapshots").join(id);
    let pending = repo
        .path
        .join("pending")
        .join(ulid::Ulid::new().to_string());
    fs::rename(&source, &pending)?;
    super::fs::sync_parent(&source)?;
    super::fs::sync_parent(&pending)?;
    fs::remove_dir_all(&pending)?;
    super::fs::sync_parent(&pending)?;
    Ok(())
}

pub fn gc(repo: &Repository, discard_incomplete: bool) -> Result<u64> {
    repo.ensure_writable()?;
    // Verify all completed snapshots and their pins before deleting anything.
    // An externally removed pin must not silently turn live data into garbage.
    for snapshot in repo.list()? {
        super::restore::verify(repo, &snapshot)?;
        let dir = repo.path.join("snapshots").join(&snapshot.id);
        for hash in &snapshot.manifest {
            repo.pin(hash, &dir)?;
        }
        repo.records(&snapshot, |record| {
            if let super::format::Kind::File { blocks, .. } = record.kind {
                for hash in blocks {
                    repo.pin(&hash, &dir)?;
                }
            }
            Ok(())
        })?;
    }
    if discard_incomplete {
        for entry in fs::read_dir(repo.path.join("pending"))? {
            let entry = entry?;
            super::fs::safe_id(
                entry
                    .file_name()
                    .to_str()
                    .ok_or_else(|| invalid("invalid pending job"))?,
            )?;
            if !entry.file_type()?.is_dir() {
                return Err(invalid("invalid pending job directory"));
            }
            fs::remove_dir_all(entry.path())?;
            super::fs::sync_parent(&entry.path())?;
        }
    }
    let mut removed = 0;
    for entry in walkdir::WalkDir::new(repo.path.join("blocks"))
        .max_open(16)
        .follow_links(false)
    {
        let entry = entry?;
        if entry.file_type().is_dir() {
            continue;
        }
        let name = entry
            .file_name()
            .to_str()
            .ok_or_else(|| invalid("invalid block filename"))?;
        if let Some(id) = name.strip_prefix(".pending-") {
            super::fs::safe_id(id)?;
        } else {
            super::fs::safe_hash(name)?;
        }
        let meta = fs::symlink_metadata(entry.path())?;
        if !meta.is_file() {
            return Err(invalid("nonregular repository block"));
        }
        if meta.nlink() == 1 {
            removed += meta.len();
            fs::remove_file(entry.path())?;
            super::fs::sync_parent(entry.path())?;
        }
    }
    Ok(removed)
}
