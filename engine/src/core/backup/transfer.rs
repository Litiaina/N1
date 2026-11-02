//! Resumable recovery-point transfer. Destination blocks are authenticated
//! before reuse, then pinned to the pending point before publication.
use super::{
    Result,
    format::{FORMAT, MAX_RECORD_BYTES, Snapshot},
    invalid,
    repository::Repository,
};
use std::{fs::File, io::ErrorKind};
pub fn replicate(source: &Repository, destination: &Repository, id: &str) -> Result<Snapshot> {
    destination.ensure_writable()?;
    let snapshot = source.snapshot(id)?;
    source.verify(&snapshot)?;
    match destination.snapshot(id) {
        Ok(existing) => {
            if serde_json::to_value(&existing)? != serde_json::to_value(&snapshot)? {
                return Err(invalid("destination recovery point ID conflicts"));
            }
            destination.verify(&existing)?;
            destination.sync_publication(id)?;
            return Ok(existing);
        }
        Err(e)
            if e.downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == ErrorKind::NotFound) => {}
        Err(e) => return Err(e),
    }
    let pending = match destination.pending(id) {
        Ok(pending) => pending,
        Err(e)
            if e.downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == ErrorKind::AlreadyExists) =>
        {
            let path = destination.path.join("pending").join(id);
            if !std::fs::symlink_metadata(&path)?.is_dir() {
                return Err(invalid("invalid pending recovery directory"));
            }
            super::fs::private_dir(&path.join("blocks"))?;
            path
        }
        Err(e) => return Err(e),
    };
    let proposal = pending.join("proposal");
    let bytes = serde_json::to_vec(&snapshot)?;
    let label = format!("{FORMAT}:proposal:{id}");
    match super::fs::read_regular(&proposal, MAX_RECORD_BYTES + 28) {
        Ok(existing) => {
            if destination.key.open(&label, existing)? != bytes {
                return Err(invalid("pending recovery proposal conflicts"));
            }
        }
        Err(e)
            if e.downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == ErrorKind::NotFound) =>
        {
            super::fs::atomic_new(&proposal, &destination.key.seal(&label, &bytes)?)?
        }
        Err(e) => return Err(e),
    }
    source.manifest_hashes(&snapshot, |hash| {
        if destination.put(&source.get(hash)?, &pending)? != hash {
            return Err(invalid("manifest transfer hash mismatch"));
        }
        Ok(())
    })?;
    source.records(&snapshot, |record| {
        if destination.put(&source.get(&record.hash)?, &pending)? != record.hash {
            return Err(invalid("payload transfer hash mismatch"));
        }
        Ok(())
    })?;
    destination.verify(&snapshot)?;
    File::open(&pending)?.sync_all()?;
    destination.publish(&snapshot, &pending)?;
    Ok(snapshot)
}
