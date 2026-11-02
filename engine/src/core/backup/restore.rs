use super::{Result, format::Snapshot, invalid, repository::Repository};
use crate::core::storage_pool::MemberMarker;
pub use crate::core::storage_pool::RESTORE_FENCE;
use n1_link::{
    Fingerprint,
    retirement::Retirement,
    shard::{self, ShardHeader},
};
use rustix::fs::{self as rfs, AtFlags, Mode, OFlags};
use sha2::Digest;
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    os::{fd::AsRawFd, unix::fs::MetadataExt},
    path::{Path, PathBuf},
    sync::Arc,
};
#[derive(Clone, Copy)]
pub(crate) enum FileKind {
    Marker,
    Final(Fingerprint, u16),
    Staged(Fingerprint, u16),
    Retirement(Fingerprint, u16),
}
pub(crate) fn allowed_path(
    path: &str,
    marker: &MemberMarker,
    staging: bool,
) -> Result<Option<FileKind>> {
    super::fs::safe_relative(path)?;
    if path == ".n1-arc-pool.json" {
        return Ok(Some(FileKind::Marker));
    }
    let parts = path.split('/').collect::<Vec<_>>();
    if parts.len() != 3 {
        return Ok(None);
    }
    if parts[0] == ".uploads" && staging {
        let Ok(fp) = parts[1].parse::<Fingerprint>() else {
            return Ok(None);
        };
        let shard = marker.layout.shard_at_slot(fp, marker.slot)?;
        if parts[2] == "retire.tmp" {
            return Ok(Some(FileKind::Retirement(fp, shard)));
        }
        if parts[2] == "shard.tmp" || parts[2] == format!("repair.s{shard}.tmp") {
            return Ok(Some(FileKind::Staged(fp, shard)));
        }
        return Ok(None);
    }
    let Some((raw, tail)) = parts[2].split_once(".s") else {
        return Ok(None);
    };
    let (Some(shard), Ok(fp)) = (
        tail.strip_suffix(".shard")
            .and_then(|s| s.parse::<u16>().ok()),
        raw.parse::<Fingerprint>(),
    ) else {
        return Ok(None);
    };
    if fp.shard_path(shard) != path || marker.layout.member_slot(fp, shard)? != marker.slot {
        return Err(invalid("misplaced LINK shard in recovery point"));
    }
    Ok(Some(FileKind::Final(fp, shard)))
}
pub(crate) fn validate_member_file(
    path: &str,
    marker: &MemberMarker,
    reader: &mut dyn Read,
    length: u64,
    staging: bool,
) -> Result<()> {
    let kind = allowed_path(path, marker, staging)?
        .ok_or_else(|| invalid("unsupported recovery file path"))?;
    let (fp, index, finalized) = match kind {
        FileKind::Marker => {
            if length > 65536 {
                return Err(invalid("oversized pool marker"));
            }
            let mut bytes = vec![0; length as usize];
            reader.read_exact(&mut bytes)?;
            if serde_json::from_slice::<MemberMarker>(&bytes)? != *marker {
                return Err(invalid("recovery pool marker differs from descriptor"));
            }
            return Ok(());
        }
        FileKind::Final(fp, index) => (fp, index, true),
        FileKind::Staged(fp, index) => (fp, index, false),
        FileKind::Retirement(fp, index) => {
            if length != 96 {
                return Err(invalid("invalid private retirement length"));
            }
            let mut bytes = [0; 96];
            reader.read_exact(&mut bytes)?;
            Retirement::decode(&bytes, fp, index)?;
            return Ok(());
        }
    };
    if finalized && length == 96 {
        let mut bytes = [0; 96];
        reader.read_exact(&mut bytes)?;
        Retirement::decode(&bytes, fp, index)?;
        return Ok(());
    }
    let mut bytes = [0; 128];
    reader.read_exact(&mut bytes)?;
    let header = ShardHeader::decode(&bytes, &marker.layout, fp, index)?;
    if !finalized {
        if length < 128 || length > header.file_bytes()? {
            return Err(invalid("invalid staged recovery length"));
        }
        // Sparse/missing/torn private frames may be legitimate interrupted parts.
        // Their exact bytes are authenticated by the repository; resume/status
        // validates completion after restore. They are never declared finalized.
        let copied = std::io::copy(&mut reader.take(length - 128), &mut std::io::sink())?;
        if copied != length - 128 {
            return Err(invalid("truncated staged recovery file"));
        }
        return Ok(());
    }
    if length != header.file_bytes()? {
        return Err(invalid("incorrect finalized recovery length"));
    }
    let mut record = vec![0; header.record_bytes() as usize];
    let mut object = shard::object_hasher(header.format, header.total_bytes, header.part_bytes);
    for part in 0..header.parts() {
        let mut root = shard::part_hasher(header.format, part);
        for stripe in header.part_stripes(part)? {
            reader.read_exact(&mut record)?;
            let frame = shard::decode_frame(&header, fp, stripe, &record)?;
            root.update(frame.source_hash);
        }
        object.update(root.finalize());
    }
    let mut seal = [0; 96];
    reader.read_exact(&mut seal)?;
    if shard::decode_seal(&header, fp, &seal)? != <[u8; 32]>::from(object.finalize()) {
        return Err(invalid("recovery shard integrity root mismatch"));
    }
    Ok(())
}

pub(crate) struct Target {
    path: PathBuf,
    root: Arc<File>,
    device: u64,
    inode: u64,
}
impl Drop for Target {
    fn drop(&mut self) {
        // This helper does only synchronous target I/O. Its lease ends when
        // that work returns, even if a concurrently forked child inherited a
        // duplicate of this open file description before exec closed it.
        // Active engine members deliberately keep their separate lease policy.
        if unsafe { libc::flock(self.root.as_raw_fd(), libc::LOCK_UN) } != 0 {
            tracing::warn!(
                "cannot unlock completed restore target {}: {}",
                self.path.display(),
                std::io::Error::last_os_error()
            );
        }
    }
}
impl Target {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        if !path.is_absolute() {
            return Err(invalid("restore targets must be absolute"));
        }
        let root = File::from(rfs::open(
            path,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?);
        if unsafe { libc::flock(root.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(invalid("restore member is leased by a running engine"));
        }
        let metadata = root.metadata()?;
        Ok(Self {
            path: path.into(),
            root: Arc::new(root),
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
    pub(crate) fn check(&self) -> Result<()> {
        let current = std::fs::symlink_metadata(&self.path)?;
        if !current.is_dir() || current.dev() != self.device || current.ino() != self.inode {
            return Err(invalid("restore target binding changed"));
        }
        Ok(())
    }
    pub(crate) fn parent(&self, path: &Path, create: bool) -> Result<Arc<File>> {
        self.check()?;
        let mut dir = self.root.clone();
        for component in path
            .parent()
            .ok_or_else(|| invalid("restore file has no parent"))?
            .components()
        {
            let std::path::Component::Normal(name) = component else {
                return Err(invalid("unsafe restore directory"));
            };
            if create {
                match rfs::mkdirat(&*dir, name, Mode::RWXU) {
                    Ok(()) => dir.sync_all()?,
                    Err(rustix::io::Errno::EXIST) => {}
                    Err(e) => return Err(e.into()),
                }
            }
            dir = Arc::new(File::from(rfs::openat(
                &*dir,
                name,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )?));
        }
        Ok(dir)
    }
}

/// Restore only into explicit, empty, independent members. A durable fence on
/// every target prevents engine startup until the entire recovery point passes.
pub fn restore(repo: &Repository, snapshot: &Snapshot, paths: &[PathBuf]) -> Result<()> {
    restore_inner(repo, snapshot, paths, true, false)
}
fn restore_inner(
    repo: &Repository,
    snapshot: &Snapshot,
    paths: &[PathBuf],
    independent: bool,
    resume: bool,
) -> Result<()> {
    super::format::validate_snapshot(snapshot)?;
    if paths.len() != snapshot.roots.len() {
        return Err(invalid("restore target count mismatch"));
    }
    let mut fenced = Vec::new();
    let mut empty = Vec::new();
    let mut targets = Vec::new();
    let mut devices = std::collections::HashSet::new();
    for path in paths {
        let target = Target::open(path)?;
        if independent && !devices.insert(target.device) {
            return Err(invalid("restore members must use independent filesystems"));
        }
        let ordinal = targets.len();
        let is_empty = std::fs::read_dir(path)?.next().is_none();
        let expected = restore_owner(repo, snapshot, ordinal)?;
        let fence = match rfs::openat(
            &*target.root,
            RESTORE_FENCE,
            OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        ) {
            Ok(fd) => {
                if !resume {
                    return Err(invalid("restore target is pending; use explicit resume"));
                }
                let mut file = File::from(fd);
                if !file.metadata()?.is_file() || file.metadata()?.len() != expected.len() as u64 {
                    return Err(invalid("invalid restore fence"));
                }
                let mut bytes = vec![0; expected.len()];
                file.read_exact(&mut bytes)?;
                if bytes != expected {
                    return Err(invalid(
                        "restore fence belongs to another recovery point, root or key",
                    ));
                }
                true
            }
            Err(rustix::io::Errno::NOENT) => false,
            Err(e) => return Err(e.into()),
        };
        if !resume && !is_empty {
            return Err(invalid("restore requires empty targets"));
        }
        fenced.push(fence);
        empty.push(is_empty);
        targets.push(target);
    }
    if resume && !fenced.iter().any(|f| *f) {
        return Err(invalid(
            "resume requires an owned incomplete recovery point",
        ));
    }
    let writable = fenced
        .iter()
        .zip(&empty)
        .map(|(f, e)| *f || *e)
        .collect::<Vec<_>>();
    for (ordinal, target) in targets.iter().enumerate() {
        if fenced[ordinal] || !empty[ordinal] {
            continue;
        }
        let owner = restore_owner(repo, snapshot, ordinal)?;
        let mut file = File::from(rfs::openat(
            &*target.root,
            RESTORE_FENCE,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )?);
        file.write_all(&owner)?;
        file.sync_all()?;
        target.root.sync_all()?;
    }
    repo.files(snapshot, |ordinal, path, length, reader| {
        let target = targets
            .get(ordinal)
            .ok_or_else(|| invalid("restore root out of range"))?;
        let relative = super::fs::safe_relative(path)?;
        let parent = target.parent(relative, writable[ordinal])?;
        let name = relative
            .file_name()
            .ok_or_else(|| invalid("restore file has no name"))?;
        if let Some(FileKind::Final(fp, shard)) =
            allowed_path(path, &snapshot.marker(ordinal)?, false)?
            && let Some(record) = read_terminal(target, fp, shard)?
        {
            validate_member_file(
                path,
                &snapshot.marker(ordinal)?,
                reader,
                length,
                snapshot.capture_mode == "offline_all",
            )?;
            let _ = record;
            return Ok(());
        }
        let mut file = if writable[ordinal] {
            let flags = OFlags::RDWR | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC;
            let file = match rfs::openat(
                &*parent,
                name,
                flags | OFlags::CREATE | OFlags::EXCL,
                Mode::RUSR | Mode::WUSR,
            ) {
                Ok(fd) => File::from(fd),
                Err(rustix::io::Errno::EXIST) if resume => {
                    File::from(rfs::openat(&*parent, name, flags, Mode::empty())?)
                }
                Err(e) => return Err(e.into()),
            };
            if !file.metadata()?.is_file() || file.metadata()?.nlink() != 1 {
                return Err(invalid("restore file is not private and regular"));
            }
            file.set_len(0)?;
            let mut file = file;
            if std::io::copy(reader, &mut file)? != length {
                return Err(invalid("restore file length differs from manifest"));
            }
            file
        } else {
            // A fence may have been cleared just before a crash during the
            // final cleanup. Compare that already-published member, never
            // truncate or replace it while resuming the remaining members.
            let mut file = File::from(rfs::openat(
                &*parent,
                name,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            )?);
            if !file.metadata()?.is_file() || file.metadata()?.len() != length {
                return Err(invalid("unfenced restored file differs"));
            }
            let mut expected = vec![0; 64 * 1024];
            let mut actual = vec![0; 64 * 1024];
            let mut left = length;
            while left > 0 {
                let n = left.min(expected.len() as u64) as usize;
                reader.read_exact(&mut expected[..n])?;
                file.read_exact(&mut actual[..n])?;
                if expected[..n] != actual[..n] {
                    return Err(invalid(
                        "unfenced restored bytes differ; refusing overwrite",
                    ));
                }
                left -= n as u64;
            }
            file
        };
        file.seek(SeekFrom::Start(0))?;
        validate_member_file(
            path,
            &snapshot.marker(ordinal)?,
            &mut file,
            length,
            snapshot.capture_mode == "offline_all",
        )?;
        file.sync_all()?;
        parent.sync_all()?;
        Ok(())
    })?;
    preserve_retirements(repo, snapshot, &targets)?;
    // An empty .uploads organizer is required even for online committed-only
    // captures; create it before clearing any startup fence.
    for target in &targets {
        let _ = target.parent(Path::new(".uploads/placeholder"), true)?;
        target.root.sync_all()?;
        target.check()?;
    }
    for target in &targets {
        match rfs::unlinkat(&*target.root, RESTORE_FENCE, AtFlags::empty()) {
            Ok(()) | Err(rustix::io::Errno::NOENT) => {}
            Err(e) => return Err(e.into()),
        }
        target.root.sync_all()?;
    }
    Ok(())
}
#[cfg(test)]
pub(crate) fn restore_fixture(
    repo: &Repository,
    snapshot: &Snapshot,
    paths: &[PathBuf],
) -> Result<()> {
    restore_inner(repo, snapshot, paths, false, false)
}

fn restore_owner(repo: &Repository, snapshot: &Snapshot, ordinal: usize) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(
        &serde_json::json!({"format":"N1-LINK-restore-v1","snapshot":snapshot.id,"root":ordinal,"key_id":repo.key.id}),
    )?)
}
pub fn resume(repo: &Repository, snapshot: &Snapshot, paths: &[PathBuf]) -> Result<()> {
    restore_inner(repo, snapshot, paths, true, true)
}
#[cfg(test)]
pub(crate) fn resume_fixture(
    repo: &Repository,
    snapshot: &Snapshot,
    paths: &[PathBuf],
) -> Result<()> {
    restore_inner(repo, snapshot, paths, false, true)
}

fn read_terminal(target: &Target, fp: Fingerprint, shard: u16) -> Result<Option<Retirement>> {
    let path = fp.shard_path(shard);
    let path = Path::new(&path);
    let parent = match target.parent(path, false) {
        Ok(parent) => parent,
        Err(e)
            if e.downcast_ref::<rustix::io::Errno>()
                .is_some_and(|e| *e == rustix::io::Errno::NOENT) =>
        {
            return Ok(None);
        }
        Err(e) => return Err(e),
    };
    let file = match rfs::openat(
        &*parent,
        path.file_name().unwrap(),
        OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(fd) => File::from(fd),
        Err(rustix::io::Errno::NOENT) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    if !file.metadata()?.is_file() {
        return Err(invalid("nonregular restore retirement path"));
    }
    if file.metadata()?.len() != 96 {
        return Ok(None);
    }
    let mut bytes = [0; 96];
    crate::core::linux_io::pread_exact_at(&file, &mut bytes, 0)?;
    Ok(Some(Retirement::decode(&bytes, fp, shard)?))
}
fn write_terminal(target: &Target, fp: Fingerprint, record: Retirement) -> Result<()> {
    let temporary = format!(".uploads/{fp}/retire.tmp");
    let parent = target.parent(Path::new(&temporary), true)?;
    match rfs::unlinkat(&*parent, "retire.tmp", AtFlags::empty()) {
        Ok(()) | Err(rustix::io::Errno::NOENT) => {}
        Err(e) => return Err(e.into()),
    }
    let mut file = File::from(rfs::openat(
        &*parent,
        "retire.tmp",
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )?);
    file.write_all(&record.encode(fp))?;
    file.sync_all()?;
    let path = fp.shard_path(record.shard);
    let path = Path::new(&path);
    let destination = target.parent(path, true)?;
    rfs::renameat(
        &*parent,
        "retire.tmp",
        &*destination,
        path.file_name().unwrap(),
    )?;
    destination.sync_all()?;
    parent.sync_all()?;
    target.root.sync_all()?;
    Ok(())
}
fn retire_recovered(
    snapshot: &Snapshot,
    targets: &[Target],
    pool: usize,
    fp: Fingerprint,
    source: Retirement,
) -> Result<()> {
    use n1_link::retirement::Phase;
    let layout = &snapshot.pools[pool].layout;
    let mut operation = sha2::Sha256::new();
    operation.update(b"Litiaina/LINK/restore-retirement/v1\0");
    operation.update(snapshot.id.as_bytes());
    operation.update(fp.0);
    let hash = operation.finalize();
    let nonce: [u8; 16] = hash[..16].try_into().unwrap();
    let root_for = |shard: u16| -> Result<usize> {
        let slot = layout.member_slot(fp, shard)?;
        snapshot
            .roots
            .iter()
            .position(|r| r.pool == pool && r.slot == slot)
            .ok_or_else(|| invalid("retirement restore target is unavailable"))
    };
    // This operation's completed certificate proves every target was made
    // terminal first. It also deduplicates repeated authority/scan records
    // without keeping a retirement catalogue in RAM or another on-disk index.
    if read_terminal(&targets[root_for(0)?], fp, 0)?
        .is_some_and(|r| r.phase == Phase::Retired && r.nonce == nonce)
    {
        let mut terminal = true;
        for shard in 0..layout.width() {
            terminal &=
                read_terminal(&targets[root_for(shard as u16)?], fp, shard as u16)?.is_some();
        }
        if terminal {
            for shard in 0..layout.width() {
                cleanup_retired_stage(&targets[root_for(shard as u16)?], fp, shard as u16)?;
            }
            return Ok(());
        }
    }
    for phase in [Phase::Retiring, Phase::Retired] {
        for shard in 0..layout.width() {
            let record = Retirement {
                phase,
                shard: shard as u16,
                nonce,
                timestamp: source.timestamp,
            };
            write_terminal(&targets[root_for(shard as u16)?], fp, record)?;
        }
    }
    for shard in 0..layout.width() {
        cleanup_retired_stage(&targets[root_for(shard as u16)?], fp, shard as u16)?;
    }
    Ok(())
}
fn preserve_retirements(repo: &Repository, snapshot: &Snapshot, targets: &[Target]) -> Result<()> {
    // Preserve any terminal state left by an earlier interrupted restore before
    // consulting repository points. This cannot turn a known retired key live.
    for (ordinal, target) in targets.iter().enumerate() {
        let marker = snapshot.marker(ordinal)?;
        for entry in walkdir::WalkDir::new(&target.path)
            .min_depth(3)
            .max_depth(3)
            .follow_links(false)
            .max_open(16)
        {
            let entry = entry?;
            if !entry.file_type().is_file() {
                continue;
            }
            let path = entry
                .path()
                .strip_prefix(&target.path)?
                .to_str()
                .ok_or_else(|| invalid("non-UTF8 retirement scan path"))?;
            if let Some(FileKind::Final(fp, shard)) = allowed_path(path, &marker, false)?
                && let Some(record) = read_terminal(target, fp, shard)?
            {
                retire_recovered(snapshot, targets, snapshot.roots[ordinal].pool, fp, record)?;
            }
        }
    }
    // All available recovery points contribute known retirement state. Clock
    // rollback cannot make a later deletion disappear from this union.
    repo.walk_snapshots(|point| {
        for known in &point.pools {
            if let Some(expected) = snapshot.pools.iter().find(|p| {
                p.layout.id == known.layout.id && p.layout.generation == known.layout.generation
            }) && (expected.layout != known.layout || expected.members != known.members)
            {
                return Err(invalid(
                    "recovery points disagree about immutable pool membership",
                ));
            }
        }
        repo.files(&point, |root, path, length, reader| {
            let marker = point.marker(root)?;
            if length == 96
                && let Some(FileKind::Final(fp, shard)) = allowed_path(path, &marker, false)?
            {
                let mut bytes = [0; 96];
                reader.read_exact(&mut bytes)?;
                let record = Retirement::decode(&bytes, fp, shard)?;
                if let Some(pool) = snapshot
                    .pools
                    .iter()
                    .position(|p| p.layout == marker.layout)
                {
                    retire_recovered(snapshot, targets, pool, fp, record)?;
                }
                return Ok(());
            }
            validate_member_file(
                path,
                &marker,
                reader,
                length,
                point.capture_mode == "offline_all",
            )
        })?;
        Ok(())
    })
}

fn cleanup_retired_stage(target: &Target, fp: Fingerprint, shard: u16) -> Result<()> {
    let stage = match target.parent(Path::new(&format!(".uploads/{fp}/placeholder")), false) {
        Ok(stage) => stage,
        Err(e)
            if e.downcast_ref::<rustix::io::Errno>()
                .is_some_and(|e| *e == rustix::io::Errno::NOENT) =>
        {
            return Ok(());
        }
        Err(e) => return Err(e),
    };
    for name in [
        "shard.tmp".to_owned(),
        "retire.tmp".to_owned(),
        format!("repair.s{shard}.tmp"),
    ] {
        match rfs::unlinkat(&*stage, &name, AtFlags::empty()) {
            Ok(()) | Err(rustix::io::Errno::NOENT) => {}
            Err(e) => return Err(e.into()),
        }
    }
    stage.sync_all()?;
    let uploads = target.parent(Path::new(".uploads/placeholder"), false)?;
    match rfs::unlinkat(&*uploads, fp.to_string(), AtFlags::REMOVEDIR) {
        Ok(()) | Err(rustix::io::Errno::NOENT | rustix::io::Errno::NOTEMPTY) => {}
        Err(e) => return Err(e.into()),
    }
    uploads.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod lease_tests {
    use super::*;
    #[test]
    fn synchronous_restore_lease_ends_before_inherited_descriptors_are_closed() {
        let path = std::env::temp_dir().join(format!("n1-restore-lease-{}", ulid::Ulid::new()));
        std::fs::create_dir(&path).unwrap();
        let owner = Target::open(&path).unwrap();
        // dup and fork retain the same Linux flock open-file-description owner.
        let inherited = owner.root.try_clone().unwrap();
        assert!(Target::open(&path).is_err());
        drop(owner);
        let next = Target::open(&path).unwrap();
        drop(inherited);
        assert!(Target::open(&path).is_err());
        drop(next);
        drop(Target::open(&path).unwrap());
        std::fs::remove_dir(path).unwrap();
    }
}
