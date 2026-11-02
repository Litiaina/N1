//! Explicit bounded retention. Completed points and hard-link pins authenticate
//! before reclamation; pending transfers remain protected unless discarded.
use super::{Result, invalid, repository::Repository};
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashSet},
    fs,
    os::unix::fs::MetadataExt,
};
pub fn prune(repo: &Repository, keep_last: usize, protect: &str) -> Result<()> {
    repo.ensure_writable()?;
    if !(1..=10000).contains(&keep_last) {
        return Err(invalid("keep-last must be 1..10000"));
    }
    let mut found = false;
    let mut newest = BinaryHeap::new();
    repo.walk_snapshots(|point| {
        repo.verify(&point)?;
        if point.id == protect {
            found = true;
        } else if keep_last > 1 {
            newest.push(Reverse((point.created_at, point.id)));
            if newest.len() > keep_last - 1 {
                newest.pop();
            }
        }
        Ok(())
    })?;
    if !found {
        return Err(invalid("protected recovery point does not exist"));
    }
    let mut retained = newest
        .into_iter()
        .map(|Reverse((_, id))| id)
        .collect::<HashSet<_>>();
    retained.insert(protect.to_owned());
    super::retention_guard::check(repo, |id| retained.contains(id))?;
    repo.walk_snapshots(|point| {
        if !retained.contains(&point.id) {
            forget_verified(repo, &point.id)?;
        }
        Ok(())
    })?;
    gc(repo, false)?;
    Ok(())
}
pub fn forget(repo: &Repository, id: &str) -> Result<()> {
    repo.ensure_writable()?;
    repo.snapshot(id)?;
    let mut count = 0;
    repo.walk_snapshots(|point| {
        repo.verify(&point)?;
        count += 1;
        Ok(())
    })?;
    if count < 2 {
        return Err(invalid("refusing to remove the last recovery point"));
    }
    super::retention_guard::check(repo, |other| other != id)?;
    forget_verified(repo, id)
}
fn forget_verified(repo: &Repository, id: &str) -> Result<()> {
    let source = repo.path.join("snapshots").join(id);
    let pending = repo
        .path
        .join("pending")
        .join(ulid::Ulid::new().to_string());
    fs::rename(&source, &pending)?;
    super::fs::sync_parent(&source)?;
    super::fs::sync_parent(&pending)?;
    fs::remove_dir_all(&pending)?;
    super::fs::sync_parent(&pending)
}
pub fn gc(repo: &Repository, discard_incomplete: bool) -> Result<u64> {
    repo.ensure_writable()?;
    repo.walk_snapshots(|point| {
        repo.verify(&point)?;
        let directory = repo.path.join("snapshots").join(&point.id);
        repo.manifest_hashes(&point, |hash| repo.pin(hash, &directory))?;
        repo.records(&point, |record| repo.pin(&record.hash, &directory))
    })?;
    if discard_incomplete {
        for entry in fs::read_dir(repo.path.join("pending"))? {
            let entry = entry?;
            let name = entry.file_name();
            super::fs::safe_id(
                name.to_str()
                    .ok_or_else(|| invalid("invalid pending recovery ID"))?,
            )?;
            if !entry.file_type()?.is_dir() {
                return Err(invalid("invalid pending recovery directory"));
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
        repo.check_cancel()?;
        let entry = entry?;
        if entry.file_type().is_dir() {
            continue;
        }
        let name = entry
            .file_name()
            .to_str()
            .ok_or_else(|| invalid("invalid recovery block filename"))?;
        if let Some(id) = name.strip_prefix(".pending-") {
            super::fs::safe_id(id)?;
        } else {
            super::fs::safe_hash(name)?;
        }
        let metadata = fs::symlink_metadata(entry.path())?;
        if !metadata.is_file() {
            return Err(invalid("nonregular recovery block"));
        }
        if metadata.nlink() == 1 {
            removed += metadata.len();
            fs::remove_file(entry.path())?;
            super::fs::sync_parent(entry.path())?;
        }
    }
    Ok(removed)
}
