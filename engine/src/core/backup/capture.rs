use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
    mpsc,
};

use super::format::{BLOCK_BYTES, ENGINE_FORMAT, FORMAT, Kind, Member, Record, Root, Snapshot};
use super::repository::{ManifestWriter, Repository};
use super::{Result, invalid};
use crate::core::instance_lease::{InstanceLease, LEASE_FILE, RESTORE_PENDING};

pub fn roots_from_config(config: &Path) -> Result<Vec<Root>> {
    let config = fs::canonicalize(config)?;
    let ini = ini::Ini::load_from_file(&config)?;
    let mut roots = BTreeMap::<String, Root>::new();
    let control = Path::new(
        ini.section(Some("disk_config"))
            .and_then(|s| s.get("fs_storage_path"))
            .unwrap_or("fs-storage-path"),
    );
    let pools = crate::core::storage_pool::bind_locations(
        crate::core::storage_pool::parse(&ini).map_err(invalid)?,
        control,
    )
    .map_err(invalid)?;
    for pool in pools {
        for (slot, path) in pool.roots.iter().enumerate() {
            let path = fs::canonicalize(path)?;
            let value = path
                .to_str()
                .ok_or_else(|| invalid("storage roots must be UTF-8"))?
                .to_owned();
            roots.insert(
                value.clone(),
                Root {
                    path: value,
                    legacy_path: Some(pool.address_roots()[slot].to_string_lossy().into_owned()),
                    index: true,
                    blob: true,
                    member: Some(Member {
                        pool_id: pool.id.clone(),
                        generation: pool.generation,
                        data_shards: pool.data_shards,
                        parity_shards: pool.parity_shards,
                        slot,
                    }),
                },
            );
        }
    }
    let mut roots: Vec<_> = roots.into_values().collect();
    roots.sort_by(|a, b| a.member.cmp(&b.member));
    validate_roots(&roots)?;
    Ok(roots)
}

pub(crate) fn validate_roots(roots: &[Root]) -> Result<()> {
    if roots.is_empty() || roots.len() > 4096 {
        return Err(invalid("invalid snapshot root count"));
    }
    for (i, a) in roots.iter().enumerate() {
        if !Path::new(&a.path).is_absolute() || (!a.index && !a.blob) {
            return Err(invalid("invalid snapshot root descriptor"));
        }
        if let Some(member) = &a.member {
            if member.pool_id.is_empty()
                || member.generation == 0
                || member.data_shards == 0
                || member.slot
                    >= usize::from(member.data_shards) + usize::from(member.parity_shards)
                || roots
                    .iter()
                    .skip(i + 1)
                    .any(|b| b.member.as_ref() == Some(member))
            {
                return Err(invalid("invalid or duplicate logical backup member"));
            }
        }
        for b in roots.iter().skip(i + 1) {
            if Path::new(&a.path).starts_with(&b.path) || Path::new(&b.path).starts_with(&a.path) {
                return Err(invalid(
                    "storage roots cannot overlap; colocated roles must share one descriptor",
                ));
            }
        }
    }
    if roots.iter().any(|r| r.member.is_some()) {
        for root in roots {
            let member = root
                .member
                .as_ref()
                .ok_or_else(|| invalid("mixed anonymous/enrolled snapshot membership"))?;
            let pool: Vec<_> = roots
                .iter()
                .filter_map(|r| r.member.as_ref())
                .filter(|m| m.pool_id == member.pool_id)
                .collect();
            let width = usize::from(member.data_shards) + usize::from(member.parity_shards);
            if pool.len() != width
                || pool.iter().any(|m| {
                    m.generation != member.generation
                        || m.data_shards != member.data_shards
                        || m.parity_shards != member.parity_shards
                })
                || (0..width).any(|slot| pool.iter().filter(|m| m.slot == slot).count() != 1)
            {
                return Err(invalid("incomplete or conflicting logical snapshot pool"));
            }
        }
    }
    Ok(())
}

pub fn create(repo: &Repository, roots: Vec<Root>, workers: usize) -> Result<Snapshot> {
    repo.ensure_writable()?;
    validate_roots(&roots)?;
    let paths = roots
        .iter()
        .map(|root| PathBuf::from(&root.path))
        .collect::<Vec<_>>();
    for path in &paths {
        if repo.path.starts_with(path) || path.starts_with(&repo.path) {
            return Err(invalid(
                "backup repository must be outside all engine roots",
            ));
        }
        if fs::symlink_metadata(path.join(RESTORE_PENDING)).is_ok() {
            return Err(invalid("cannot back up an incomplete restore"));
        }
    }
    // Kept until every read and the durable snapshot publication complete.
    // A crashed capture releases this OS lease but has no committed snapshot.
    let _source = InstanceLease::acquire(&paths, true)?;
    crate::core::storage_admin::verify_backup_roots(&roots)?;
    let id = ulid::Ulid::new().to_string();
    let pending = repo.pending(&id)?;
    let writer = Mutex::new(ManifestWriter::new(repo, pending.clone()));
    let source_bytes = AtomicU64::new(0);
    let workers = workers.clamp(1, 32);
    let (send, receive) = mpsc::sync_channel::<(usize, PathBuf)>(workers * 2);
    let receive = Mutex::new(receive);
    let failed = AtomicBool::new(false);
    std::thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for _ in 0..workers {
            handles.push(scope.spawn(|| {
                let result = (|| -> Result<()> {
                    loop {
                        let item = receive
                            .lock()
                            .map_err(|_| invalid("capture queue poisoned"))?
                            .recv();
                        let Ok((root, path)) = item else {
                            break;
                        };
                        let record = capture_entry(repo, &pending, root, &paths[root], &path)?;
                        super::restore::validate_record(&record, paths.len())?;
                        if let Kind::File { size, .. } = &record.kind {
                            source_bytes
                                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |total| {
                                    total.checked_add(*size)
                                })
                                .map_err(|_| {
                                    invalid("snapshot physical byte count exceeds format limit")
                                })?;
                        }
                        writer
                            .lock()
                            .map_err(|_| invalid("capture manifest poisoned"))?
                            .record(&record)?;
                    }
                    Ok(())
                })();
                if result.is_err() {
                    failed.store(true, Ordering::Release);
                }
                result
            }));
        }
        let walk_result = (|| -> Result<()> {
            for (root, path) in paths.iter().enumerate() {
                for entry in walkdir::WalkDir::new(path)
                    .min_depth(1)
                    .max_open(32)
                    .follow_links(false)
                    .into_iter()
                    .filter_entry(|entry| {
                        !(entry.depth() == 1
                            && matches!(
                                entry.file_name().to_str(),
                                Some(LEASE_FILE | ".n1-root-id" | super::restore_guard::COMPLETE)
                            ))
                    })
                {
                    let entry = entry?;
                    if entry
                        .path()
                        .components()
                        .any(|c| c.as_os_str() == ".intents")
                        && entry.file_type().is_file()
                    {
                        return Err(invalid(
                            "finish pending redistribution intents before snapshot capture",
                        ));
                    }
                    let mut item = (root, entry.into_path());
                    loop {
                        if failed.load(Ordering::Acquire) {
                            return Err(invalid("backup capture worker failed"));
                        }
                        match send.try_send(item) {
                            Ok(()) => break,
                            Err(mpsc::TrySendError::Full(value)) => {
                                item = value;
                                std::thread::sleep(std::time::Duration::from_millis(1));
                            }
                            Err(mpsc::TrySendError::Disconnected(_)) => {
                                return Err(invalid("backup capture workers disconnected"));
                            }
                        }
                    }
                }
            }
            Ok(())
        })();
        drop(send);
        for handle in handles {
            handle
                .join()
                .map_err(|_| invalid("backup capture worker panicked"))??;
        }
        walk_result
    })?;
    let mut writer = writer
        .into_inner()
        .map_err(|_| invalid("capture manifest poisoned"))?;
    writer.finish()?;
    let snapshot = Snapshot {
        format: FORMAT.to_owned(),
        engine_format: ENGINE_FORMAT,
        id,
        created_at: chrono::Utc::now().timestamp(),
        roots,
        records: writer.count,
        source_bytes: source_bytes.load(Ordering::Relaxed),
        manifest: writer.blocks,
    };
    repo.publish(&snapshot, &pending)?;
    Ok(snapshot)
}

fn capture_entry(
    repo: &Repository,
    pending: &Path,
    root: usize,
    base: &Path,
    path: &Path,
) -> Result<Record> {
    let relative = path
        .strip_prefix(base)?
        .to_str()
        .ok_or_else(|| invalid("non-UTF8 namespace entry"))?
        .to_owned();
    super::fs::safe_relative(&relative)?;
    let meta = fs::symlink_metadata(path)?;
    let kind = if meta.is_dir() {
        Kind::Directory
    } else if meta.file_type().is_symlink() {
        let target = fs::read_link(path)?
            .to_str()
            .ok_or_else(|| invalid("non-UTF8 namespace link"))?
            .to_owned();
        super::restore::validate_link(&relative, &target)?;
        Kind::Link { target }
    } else if meta.is_file() {
        if super::metadata::requires_remap(&relative)
            && meta.len() > super::format::MAX_METADATA_BYTES as u64
        {
            return Err(invalid(
                "internal N1 metadata exceeds the portable backup limit (64 MiB)",
            ));
        }
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?;
        let mut blocks = Vec::new();
        let mut size = 0;
        let mut buffer = vec![0; BLOCK_BYTES];
        loop {
            let mut n = 0;
            while n < buffer.len() {
                let read = file.read(&mut buffer[n..])?;
                if read == 0 {
                    break;
                }
                n += read;
            }
            if n == 0 {
                break;
            }
            blocks.push(repo.put(&buffer[..n], pending)?);
            size += n as u64;
            if blocks.len() > 100_000 {
                return Err(invalid("one physical file exceeds backup record limit"));
            }
        }
        if size != meta.len() || file.metadata()?.modified()? != meta.modified()? {
            return Err(invalid("source changed during offline snapshot"));
        }
        Kind::File { size, blocks }
    } else {
        return Err(invalid(
            "backup refuses devices, sockets and named pipes in storage roots",
        ));
    };
    Ok(Record {
        root,
        path: relative,
        modified_seconds: meta.mtime(),
        modified_nanos: meta.mtime_nsec() as u32,
        kind,
    })
}
