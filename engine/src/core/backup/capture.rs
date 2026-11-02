use super::{
    Result,
    format::{self, BLOCK_BYTES, PoolDescriptor, Record, Root, Snapshot},
    invalid,
    repository::{ManifestWriter, Repository},
};
use crate::core::{Store, storage_pool::Pool};
use std::{
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
};

pub fn descriptors(pools: &[std::sync::Arc<Pool>], mode: &str) -> Result<Snapshot> {
    let mut roots = Vec::new();
    let mut descriptors = Vec::new();
    for (i, pool) in pools.iter().enumerate() {
        pool.writable()?;
        let first = pool
            .members
            .iter()
            .flatten()
            .next()
            .ok_or_else(|| invalid("no enrolled member"))?;
        descriptors.push(PoolDescriptor {
            layout: pool.layout.clone(),
            members: first.marker.members.clone(),
        });
        for slot in 0..pool.layout.width() {
            roots.push(Root { pool: i, slot });
        }
    }
    Ok(Snapshot {
        service_control: None,
        format: format::FORMAT.into(),
        engine_format: format::ENGINE_FORMAT.into(),
        id: ulid::Ulid::new().to_string(),
        created_at: chrono::Utc::now().timestamp(),
        capture_mode: mode.into(),
        pools: descriptors,
        roots,
        records: 0,
        files: 0,
        source_bytes: 0,
        manifest_depth: 0,
        manifest: Vec::new(),
    })
}

/// Configuration-only preflight for the retained job supervisor. It never
/// acquires a running engine's member leases or initializes missing roots.
pub fn roots_from_config(path: &std::path::Path) -> Result<Vec<ConfiguredRoot>> {
    let config = crate::settings::config::Config::load(path)?;
    config
        .pools
        .into_iter()
        .flat_map(|pool| pool.roots)
        .map(|path| {
            let path = path.canonicalize()?;
            if !path.is_dir() {
                return Err(invalid("backup source is not a directory"));
            }
            Ok(ConfiguredRoot {
                path: path
                    .to_str()
                    .ok_or_else(|| invalid("backup source must be UTF-8"))?
                    .into(),
            })
        })
        .collect()
}
pub struct ConfiguredRoot {
    pub path: String,
}
/// Offline capture opens configured members under their exclusive engine leases.
/// No running engine can mutate durable multipart progress while it is copied.
pub fn offline(repo: &Repository, store: &Store) -> Result<Snapshot> {
    let snapshot = descriptors(&store.pools, "offline_all")?;
    let sources = store
        .pools
        .iter()
        .flat_map(|p| p.members.iter().flatten().map(|m| m.path.clone()))
        .collect::<Vec<_>>();
    capture(repo, snapshot, &sources, store.recovery_control.get())
}
pub(crate) fn capture(
    repo: &Repository,
    mut snapshot: Snapshot,
    sources: &[PathBuf],
    control: Option<&super::control::ServiceControl>,
) -> Result<Snapshot> {
    repo.ensure_writable()?;
    format::validate_snapshot(&snapshot)?;
    if sources.len() != snapshot.roots.len() {
        return Err(invalid("capture source count mismatch"));
    }
    let pending = repo.pending(&snapshot.id)?;
    if let Some(control) = control {
        let bytes = serde_json::to_vec(control)?;
        if bytes.len() > BLOCK_BYTES {
            return Err(invalid(
                "service recovery configuration exceeds the 4 MiB block bound",
            ));
        }
        snapshot.service_control = Some(repo.put(&bytes, &pending)?);
    }
    let mut manifest = ManifestWriter::new(repo, pending.clone());
    let mut buffer = vec![0; BLOCK_BYTES];
    for source in sources {
        let source = source.canonicalize()?;
        if repo.path.starts_with(&source) || source.starts_with(&repo.path) {
            return Err(invalid(
                "recovery repository must be outside all member roots",
            ));
        }
    }
    for (root, source) in sources.iter().enumerate() {
        let marker = snapshot.marker(root)?;
        for entry in walkdir::WalkDir::new(source)
            .max_depth(3)
            .max_open(16)
            .follow_links(false)
        {
            let entry = entry?;
            if entry.file_type().is_dir() {
                continue;
            }
            let relative = entry
                .path()
                .strip_prefix(source)?
                .to_str()
                .ok_or_else(|| invalid("non-UTF8 member filename"))?;
            if super::restore::allowed_path(
                relative,
                &marker,
                snapshot.capture_mode == "offline_all",
            )?
            .is_none()
            {
                continue;
            }
            if !entry.file_type().is_file() {
                return Err(invalid("nonregular file at a calculated backup path"));
            }
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                .open(entry.path())?;
            let length = file.metadata()?.len();
            if !file.metadata()?.is_file() {
                return Err(invalid("capture file changed type"));
            }
            let mut offset = 0;
            loop {
                let take = (length - offset).min(BLOCK_BYTES as u64) as usize;
                file.read_exact(&mut buffer[..take])?;
                let hash = repo.put(&buffer[..take], &pending)?;
                let last = offset + take as u64 == length;
                manifest.record(&Record {
                    root,
                    path: relative.into(),
                    offset,
                    total_bytes: length,
                    bytes: take as u32,
                    hash,
                    last,
                })?;
                offset += take as u64;
                if last {
                    break;
                }
            }
            if file.metadata()?.len() != length {
                return Err(invalid("capture source changed size"));
            }
            // The final path grammar and inline bytes are validated before a
            // snapshot is published, not trusted just because copying succeeded.
            file.seek(SeekFrom::Start(0))?;
            super::restore::validate_member_file(
                relative,
                &marker,
                &mut file,
                length,
                snapshot.capture_mode == "offline_all",
            )?;
            snapshot.files += 1;
            snapshot.source_bytes = snapshot
                .source_bytes
                .checked_add(length)
                .ok_or_else(|| invalid("snapshot byte count overflow"))?;
        }
    }
    manifest.finish()?;
    snapshot.records = manifest.count;
    snapshot.manifest_depth = manifest.depth;
    snapshot.manifest = std::mem::take(&mut manifest.blocks);
    drop(manifest);
    repo.publish(&snapshot, &pending)?;
    Ok(snapshot)
}
use std::os::unix::fs::OpenOptionsExt;
