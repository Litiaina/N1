//! Disk-backed inventory makes interrupted restore validation bounded in RAM.
use super::format::{Kind, MAX_RECORD_BYTES, Record, Snapshot};
use super::fs::{atomic_new, read_regular, restore_dir, sync_parent};
use super::repository::Repository;
use super::{Result, invalid};
use crate::core::instance_lease::{LEASE_FILE, RESTORE_PENDING};
use std::fs;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

pub const WORK: &str = ".n1-restore-work";
pub const COMPLETE: &str = ".n1-restore-complete";

pub fn reserved(path: &str) -> bool {
    path.split('/').next().is_some_and(|name| {
        matches!(
            name,
            WORK | COMPLETE
                | RESTORE_PENDING
                | LEASE_FILE
                | ".n1-root-id"
                | super::online::VIEW_DIR
        )
    })
}

pub fn prepare(paths: &[PathBuf], marker: &[u8], resume: bool) -> Result<()> {
    let mut pending = false;
    for root in paths {
        let path = root.join(RESTORE_PENDING);
        if path.try_exists()? {
            if !resume || read_regular(&path, MAX_RECORD_BYTES)? != marker {
                return Err(invalid("restore marker does not match this job"));
            }
            pending = true;
        } else {
            let completed = root.join(COMPLETE);
            if resume
                && completed.try_exists()?
                && read_regular(&completed, MAX_RECORD_BYTES)? == marker
            {
                continue;
            }
            for entry in fs::read_dir(root)? {
                if entry?.file_name() != LEASE_FILE {
                    return Err(invalid(
                        "restore requires empty roots or a matching interrupted job",
                    ));
                }
            }
        }
    }
    if resume && !pending {
        return Err(invalid("there is no interrupted restore to resume"));
    }
    for root in paths {
        let path = root.join(RESTORE_PENDING);
        if !path.try_exists()? {
            atomic_new(&path, marker)?;
        }
        let work = root.join(WORK);
        if work.try_exists()? {
            if !fs::symlink_metadata(&work)?.is_dir() {
                return Err(invalid("invalid restore work directory"));
            }
            fs::remove_dir_all(&work)?;
            sync_parent(&work)?;
        }
        restore_dir(&work)?;
    }
    Ok(())
}

fn inventory_path(root: &Path, path: &str) -> PathBuf {
    let hash = super::crypto::digest(path.as_bytes());
    root.join(WORK).join(&hash[..2]).join(hash)
}
fn descriptor(record: &Record) -> Vec<u8> {
    format!(
        "{}\n{}",
        match record.kind {
            Kind::File { .. } => "file",
            Kind::Directory => "directory",
            Kind::Link { .. } => "link",
        },
        record.path
    )
    .into_bytes()
}

pub fn inventory(repo: &Repository, snapshot: &Snapshot, paths: &[PathBuf]) -> Result<()> {
    repo.records(snapshot, |record| {
        if record.root >= paths.len() || reserved(&record.path) {
            return Err(invalid("invalid restore inventory entry"));
        }
        super::fs::safe_relative(&record.path)?;
        let path = inventory_path(&paths[record.root], &record.path);
        restore_dir(path.parent().unwrap())?;
        // Duplicate manifest paths fail, even when their bytes happen to match.
        // The inventory is a rebuildable validation index, never recovery
        // truth. The durable start fences already exist on every root, and
        // resume discards/rebuilds this directory. Avoid an fsync transaction
        // per namespace entry solely for this disposable index.
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)?
            .write_all(&descriptor(&record))?;
        Ok(())
    })
}

pub fn audit(paths: &[PathBuf]) -> Result<()> {
    for root in paths {
        for entry in walkdir::WalkDir::new(root)
            .min_depth(1)
            .max_open(16)
            .follow_links(false)
            .into_iter()
            .filter_entry(|e| !(e.depth() == 1 && e.file_name() == WORK))
        {
            let entry = entry?;
            let path = entry
                .path()
                .strip_prefix(root)?
                .to_str()
                .ok_or_else(|| invalid("non-UTF8 restore path"))?;
            if entry.depth() == 1 && matches!(path, LEASE_FILE | RESTORE_PENDING | COMPLETE) {
                continue;
            }
            let kind = if entry.file_type().is_dir() {
                "directory"
            } else if entry.file_type().is_symlink() {
                "link"
            } else if entry.file_type().is_file() {
                "file"
            } else {
                return Err(invalid("invalid restored file type"));
            };
            if read_regular(&inventory_path(root, path), 8192)?
                != format!("{kind}\n{path}").as_bytes()
            {
                return Err(invalid("unexpected entry in restore target"));
            }
        }
    }
    Ok(())
}

pub fn complete(paths: &[PathBuf], marker: &[u8]) -> Result<()> {
    for root in paths {
        let completed = root.join(COMPLETE);
        if completed.try_exists()? {
            if read_regular(&completed, MAX_RECORD_BYTES)? != marker {
                return Err(invalid("restore completion marker mismatch"));
            }
        } else {
            atomic_new(&completed, marker)?;
        }
    }
    for root in paths {
        fs::remove_dir_all(root.join(WORK))?;
        fs::File::open(root)?.sync_all()?;
    }
    // Every root has a durable completion record before any start fence drops.
    for root in paths {
        fs::remove_file(root.join(RESTORE_PENDING))?;
        fs::File::open(root)?.sync_all()?;
    }
    Ok(())
}
