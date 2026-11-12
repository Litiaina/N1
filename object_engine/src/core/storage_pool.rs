//! Leased member roots and descriptor-relative paths. Every descendant is
//! opened with NOFOLLOW; generated names never become arbitrary host paths.
use super::{Error, Result};
use n1_link::{Fingerprint, PoolId, PoolLayout};
use rand::{RngCore, rngs::OsRng};
use rustix::fs::{self as rfs, AtFlags, Mode, OFlags, RenameFlags};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{self, Read, Write},
    os::{fd::AsRawFd, unix::fs::MetadataExt},
    path::{Component, Path, PathBuf},
    sync::Arc,
};

const MARKER: &str = ".n1-arc-pool.json";
pub const RESTORE_FENCE: &str = ".n1-restore-pending";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberMarker {
    pub format: String,
    pub layout: PoolLayout,
    pub members: Vec<PoolId>,
    pub slot: usize,
}

pub struct Member {
    pub path: PathBuf,
    pub marker: MemberMarker,
    root: Arc<File>,
    device: u64,
    inode: u64,
}

impl Drop for Member {
    fn drop(&mut self) {
        // A fork before exec can inherit the open-file-description lock even
        // with CLOEXEC. End the lease when the final admitted member owner
        // ends, rather than waiting for an unrelated duplicate to close.
        // Arc<Member> keeps this from running while any worker owns the member.
        if unsafe { libc::flock(self.root.as_raw_fd(), libc::LOCK_UN) } != 0 {
            tracing::warn!(path=%self.path.display(),error=%io::Error::last_os_error(),"cannot release completed member lease");
        }
    }
}

pub struct Pool {
    pub layout: PoolLayout,
    pub members: Vec<Option<Arc<Member>>>,
}

impl Member {
    /// Monitoring uses the same pinned device and binding fence as object I/O.
    pub fn filesystem_metrics(&self) -> Result<cli_core::system::FilesystemMetrics> {
        self.check_binding()?;
        Ok(cli_core::system::filesystem_metrics(&self.root)?)
    }
}

impl Pool {
    /// Resume only a durably planned, unpublished enrollment. The caller holds
    /// the node's control lease and supplies identities from its trusted journal.
    /// No payload, unknown entry or differing marker can be adopted here.
    pub(crate) fn resume_enrollment(
        layout: PoolLayout,
        paths: &[PathBuf],
        ids: Vec<PoolId>,
    ) -> Result<Self> {
        layout.validate()?;
        if paths.len() != layout.width()
            || ids.len() != layout.width()
            || ids.iter().collect::<std::collections::HashSet<_>>().len() != ids.len()
        {
            return Err(Error::Invalid("invalid planned pool enrollment".into()));
        }
        let mut roots = Vec::new();
        let mut devices = std::collections::HashSet::new();
        let mut records = Vec::new();
        for (slot, path) in paths.iter().enumerate() {
            let root = open_root(path)?;
            if !devices.insert(root.metadata()?.dev()) {
                return Err(Error::Invalid(
                    "pool members must use independent filesystems".into(),
                ));
            }
            let marker = MemberMarker {
                format: "LINK-pool-v1".into(),
                layout: layout.clone(),
                members: ids.clone(),
                slot,
            };
            let mut expected =
                serde_json::to_vec(&marker).map_err(|error| Error::Invalid(error.to_string()))?;
            expected.push(b'\n');
            let mut existing = None;
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                if entry.file_name() == MARKER {
                    let file = File::from(rfs::openat(
                        &root,
                        MARKER,
                        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                        Mode::empty(),
                    )?);
                    if !file.metadata()?.is_file() || file.metadata()?.len() > expected.len() as u64
                    {
                        return Err(Error::Conflict(
                            "planned enrollment marker differs from its durable identity".into(),
                        ));
                    }
                    let mut bytes = Vec::new();
                    file.take(expected.len() as u64 + 1)
                        .read_to_end(&mut bytes)?;
                    if !expected.starts_with(&bytes) {
                        return Err(Error::Conflict(
                            "planned enrollment marker differs from its durable identity".into(),
                        ));
                    }
                    existing = Some(bytes);
                } else if entry.file_name() == ".uploads" {
                    let _directory = rfs::openat(
                        &root,
                        ".uploads",
                        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                        Mode::empty(),
                    )?;
                    if std::fs::read_dir(entry.path())?.next().is_some() {
                        return Err(Error::Conflict(
                            "planned enrollment already contains upload data".into(),
                        ));
                    }
                } else {
                    return Err(Error::Conflict(
                        "enrollment requires empty member roots; refusing existing data".into(),
                    ));
                }
            }
            roots.push(root);
            records.push((marker, expected, existing));
        }
        let mut members = Vec::new();
        for ((path, root), (marker, bytes, existing)) in paths.iter().zip(roots).zip(records) {
            if existing.as_deref() != Some(bytes.as_slice()) {
                let mode = if existing.is_some() {
                    OFlags::TRUNC
                } else {
                    OFlags::CREATE | OFlags::EXCL
                };
                let mut file = File::from(rfs::openat(
                    &root,
                    MARKER,
                    OFlags::WRONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | mode,
                    Mode::RUSR | Mode::WUSR,
                )?);
                file.write_all(&bytes)?;
                file.sync_all()?;
            }
            match rfs::mkdirat(&root, ".uploads", Mode::RWXU) {
                Ok(()) => {}
                Err(error) if error == rustix::io::Errno::EXIST => {}
                Err(error) => return Err(error.into()),
            }
            root.sync_all()?;
            let metadata = root.metadata()?;
            members.push(Some(Arc::new(Member {
                path: path.clone(),
                marker,
                root: Arc::new(root),
                device: metadata.dev(),
                inode: metadata.ino(),
            })));
        }
        Ok(Self { layout, members })
    }
    /// Explicit offline enrollment only. Refuse nonempty or previously enrolled
    /// roots; no automatic formatting, fallback enrollment or marker replacement.
    pub fn enroll(layout: PoolLayout, paths: &[PathBuf]) -> Result<Self> {
        Self::enroll_inner(layout, paths, true, None)
    }

    pub fn enroll_with_members(
        layout: PoolLayout,
        paths: &[PathBuf],
        members: Vec<PoolId>,
    ) -> Result<Self> {
        if members.len() != layout.width()
            || members
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != layout.width()
        {
            return Err(Error::Invalid(
                "invalid immutable enrollment member identities".into(),
            ));
        }
        Self::enroll_inner(layout, paths, true, Some(members))
    }

    fn enroll_inner(
        layout: PoolLayout,
        paths: &[PathBuf],
        independent: bool,
        members: Option<Vec<PoolId>>,
    ) -> Result<Self> {
        layout.validate()?;
        if paths.len() != layout.width() {
            return Err(Error::Invalid("pool root count differs from K+P".into()));
        }
        let mut roots = Vec::new();
        let mut devices = Vec::new();
        for path in paths {
            let root = open_root(path)?;
            let meta = root.metadata()?;
            if devices.contains(&meta.dev()) && independent {
                return Err(Error::Invalid(
                    "pool members must use independent filesystems".into(),
                ));
            }
            devices.push(meta.dev());
            if std::fs::read_dir(path)?.next().is_some() {
                return Err(Error::Conflict(
                    "enrollment requires empty member roots".into(),
                ));
            }
            roots.push(root);
        }
        let ids = members.unwrap_or_else(|| {
            paths
                .iter()
                .map(|_| {
                    let mut id = [0; 16];
                    OsRng.fill_bytes(&mut id);
                    PoolId(id)
                })
                .collect::<Vec<_>>()
        });
        let mut members = Vec::new();
        for (slot, (path, root)) in paths.iter().zip(roots).enumerate() {
            let marker = MemberMarker {
                format: "LINK-pool-v1".into(),
                layout: layout.clone(),
                members: ids.clone(),
                slot,
            };
            let mut file = File::from(rfs::openat(
                &root,
                MARKER,
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::RUSR | Mode::WUSR,
            )?);
            serde_json::to_writer(&mut file, &marker).map_err(|e| Error::Invalid(e.to_string()))?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            rfs::mkdirat(&root, ".uploads", Mode::RWXU)?;
            root.sync_all()?;
            let meta = root.metadata()?;
            members.push(Some(Arc::new(Member {
                path: path.clone(),
                marker,
                root: Arc::new(root),
                device: meta.dev(),
                inode: meta.ino(),
            })));
        }
        Ok(Self { layout, members })
    }

    pub fn open(layout: PoolLayout, paths: &[PathBuf]) -> Result<Self> {
        Self::open_inner(layout, paths, true)
    }

    fn open_inner(layout: PoolLayout, paths: &[PathBuf], independent: bool) -> Result<Self> {
        Self::open_selected(layout, paths, independent, None)
    }

    /// Explicit offline replacement omits the authorized logical slot. Other
    /// roots without a marker remain unavailable, never enrolled or written.
    /// Ordinary startup always validates every configured path it can open.
    pub(crate) fn open_survivors(
        layout: PoolLayout,
        paths: &[PathBuf],
        replacing: usize,
    ) -> Result<Self> {
        Self::open_selected(layout, paths, true, Some(replacing))
    }

    fn open_selected(
        layout: PoolLayout,
        paths: &[PathBuf],
        independent: bool,
        replacing: Option<usize>,
    ) -> Result<Self> {
        layout.validate()?;
        if paths.len() != layout.width() || replacing.is_some_and(|slot| slot >= paths.len()) {
            return Err(Error::Invalid("pool root count differs from K+P".into()));
        }
        let mut members = Vec::new();
        let mut known: Option<Vec<PoolId>> = None;
        let mut devices = Vec::new();
        for (slot, path) in paths.iter().enumerate() {
            if replacing == Some(slot) {
                members.push(None);
                continue;
            }
            let root = match open_root(path) {
                Ok(f) => f,
                Err(Error::Io(e)) if e.kind() == io::ErrorKind::NotFound => {
                    members.push(None);
                    continue;
                }
                Err(e) => return Err(e),
            };
            match rfs::openat(
                &root,
                RESTORE_FENCE,
                OFlags::PATH | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            ) {
                Ok(_) => {
                    return Err(Error::Conflict(
                        "restore is incomplete; refusing pool activation".into(),
                    ));
                }
                Err(rustix::io::Errno::NOENT) => {}
                Err(e) => return Err(e.into()),
            }
            let mut marker_file = match rfs::openat(
                &root,
                MARKER,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
                Mode::empty(),
            ) {
                Ok(file) => File::from(file),
                // Several lost members may already have blank replacement
                // disks connected. Only the selected target may be enrolled;
                // require K validated peers below and leave other roots alone.
                Err(rustix::io::Errno::NOENT) if replacing.is_some() => {
                    members.push(None);
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            if !marker_file.metadata()?.is_file() || marker_file.metadata()?.len() > 65536 {
                return Err(Error::Invalid("invalid pool marker file".into()));
            }
            let mut bytes = Vec::new();
            Read::by_ref(&mut marker_file)
                .take(65537)
                .read_to_end(&mut bytes)?;
            let marker: MemberMarker = serde_json::from_slice(&bytes)
                .map_err(|e| Error::Invalid(format!("invalid pool marker: {e}")))?;
            if marker.format != "LINK-pool-v1"
                || marker.layout != layout
                || marker.slot != slot
                || marker.members.len() != layout.width()
                || known.as_ref().is_some_and(|ids| *ids != marker.members)
            {
                return Err(Error::Invalid("conflicting pool/member identity".into()));
            }
            let unique = marker
                .members
                .iter()
                .collect::<std::collections::HashSet<_>>();
            if unique.len() != layout.width() {
                return Err(Error::Invalid("duplicate member identities".into()));
            }
            known = Some(marker.members.clone());
            let meta = root.metadata()?;
            if devices.contains(&meta.dev()) && independent {
                return Err(Error::Invalid(
                    "pool members must use independent filesystems".into(),
                ));
            }
            devices.push(meta.dev());
            members.push(Some(Arc::new(Member {
                path: path.clone(),
                marker,
                root: Arc::new(root),
                device: meta.dev(),
                inode: meta.ino(),
            })));
        }
        if members.iter().filter(|m| m.is_some()).count() < usize::from(layout.data_shards) {
            return Err(Error::Unavailable(
                "too few enrolled members to read this pool".into(),
            ));
        }
        Ok(Self { layout, members })
    }

    pub fn writable(&self) -> Result<()> {
        for member in &self.members {
            member
                .as_ref()
                .ok_or_else(|| Error::Unavailable("pool has unavailable members".into()))?
                .check_binding()?;
        }
        Ok(())
    }

    pub fn holder(&self, fp: Fingerprint, shard: u16) -> Result<Arc<Member>> {
        self.members[self.layout.member_slot(fp, shard)?]
            .clone()
            .ok_or_else(|| Error::Unavailable("calculated member is unavailable".into()))
    }

    #[cfg(test)]
    pub(crate) fn fixture(layout: PoolLayout, paths: &[PathBuf]) -> Result<Self> {
        Self::enroll_inner(layout, paths, false, None)
    }
    #[cfg(test)]
    pub(crate) fn reopen_fixture(layout: PoolLayout, paths: &[PathBuf]) -> Result<Self> {
        Self::open_inner(layout, paths, false)
    }

    #[cfg(test)]
    pub(crate) fn survivors_fixture(
        layout: PoolLayout,
        paths: &[PathBuf],
        slot: usize,
    ) -> Result<Self> {
        Self::open_selected(layout, paths, false, Some(slot))
    }
}

impl Member {
    pub(crate) fn check_capacity(&self, required: u64) -> Result<()> {
        self.check_binding()?;
        super::capacity_manager::ensure_member_capacity(&self.root, required)?;
        Ok(())
    }
    pub fn check_binding(&self) -> Result<()> {
        let meta = std::fs::symlink_metadata(&self.path)?;
        if !meta.is_dir() || meta.dev() != self.device || meta.ino() != self.inode {
            return Err(Error::Unavailable(
                "member mount or directory changed while engine was running".into(),
            ));
        }
        Ok(())
    }

    /// Open every path component relative to already-open directory handles.
    pub fn directory(&self, path: &Path, create: bool, strict: bool) -> Result<Arc<File>> {
        self.check_binding()?;
        let mut dir = self.root.clone();
        for component in path.components() {
            let Component::Normal(name) = component else {
                return Err(Error::Invalid("invalid member-relative directory".into()));
            };
            if create {
                match rfs::mkdirat(&*dir, name, Mode::RWXU) {
                    Ok(()) => {
                        if strict {
                            dir.sync_all()?
                        }
                    }
                    Err(rustix::io::Errno::EXIST) => {}
                    Err(e) => return Err(e.into()),
                }
            }
            let next = File::from(rfs::openat(
                &*dir,
                name,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )?);
            dir = Arc::new(next);
        }
        Ok(dir)
    }

    pub fn open_read(&self, path: &Path) -> Result<Arc<File>> {
        let parent = self.directory(
            path.parent()
                .ok_or_else(|| Error::Invalid("file has no parent".into()))?,
            false,
            false,
        )?;
        let name = path
            .file_name()
            .ok_or_else(|| Error::Invalid("file has no name".into()))?;
        let file = File::from(rfs::openat(
            &*parent,
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )?);
        if !file.metadata()?.is_file() {
            return Err(Error::Invalid("shard is not a regular file".into()));
        }
        Ok(Arc::new(file))
    }

    pub fn open_stage(
        &self,
        fp: Fingerprint,
        strict: bool,
        create: bool,
    ) -> Result<(Arc<File>, bool)> {
        let relative = fp.staging_path();
        let path = Path::new(&relative);
        let dir = self.directory(
            path.parent().expect("generated staging parent"),
            create,
            strict,
        )?;
        #[cfg(test)]
        if create
            && std::env::var_os("N1_TEST_FAIL_STAGE_ALLOCATION")
                .is_some_and(|path| std::path::Path::new(&path) == self.path)
        {
            // Exercise mkdir-success/file-allocation-failure without filling
            // the operator's filesystem. Release binaries omit this hook.
            return Err(std::io::Error::from_raw_os_error(libc::ENOSPC).into());
        }
        let flags = OFlags::RDWR | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC;
        if !create {
            let file = File::from(rfs::openat(&*dir, "shard.tmp", flags, Mode::empty())?);
            if !file.metadata()?.is_file() || file.metadata()?.nlink() != 1 {
                return Err(Error::Invalid(
                    "staged shard is not a private regular file".into(),
                ));
            }
            return Ok((Arc::new(file), false));
        }
        let (file, new) = match rfs::openat(
            &*dir,
            "shard.tmp",
            flags | OFlags::CREATE | OFlags::EXCL,
            Mode::RUSR | Mode::WUSR,
        ) {
            Ok(fd) => (File::from(fd), true),
            Err(rustix::io::Errno::EXIST) => (
                File::from(rfs::openat(&*dir, "shard.tmp", flags, Mode::empty())?),
                false,
            ),
            Err(e) => return Err(e.into()),
        };
        if !file.metadata()?.is_file() || file.metadata()?.nlink() != 1 {
            return Err(Error::Invalid(
                "staged shard is not a private regular file".into(),
            ));
        }
        Ok((Arc::new(file), new))
    }

    /// Reopen an existing, private staging inode for aligned payload writes.
    /// Never create/truncate here: continuation cannot allocate an identity.
    pub(crate) fn open_stage_direct(&self, fp: Fingerprint, expected: &File) -> Result<Arc<File>> {
        let relative = fp.staging_path();
        let path = Path::new(&relative);
        let dir = self.directory(
            path.parent().expect("generated staging parent"),
            false,
            false,
        )?;
        let file = File::from(rfs::openat(
            &*dir,
            "shard.tmp",
            OFlags::WRONLY | OFlags::DIRECT | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )?);
        let actual = file.metadata()?;
        let expected = expected.metadata()?;
        if !actual.is_file()
            || actual.nlink() != 1
            || actual.dev() != expected.dev()
            || actual.ino() != expected.ino()
        {
            return Err(Error::Conflict(
                "staged shard changed before direct I/O".into(),
            ));
        }
        Ok(Arc::new(file))
    }

    pub fn persist_stage_parent(&self, fp: Fingerprint) -> Result<()> {
        self.directory(Path::new(&format!(".uploads/{fp}")), false, false)?
            .sync_all()?;
        self.directory(Path::new(".uploads"), false, false)?
            .sync_all()?;
        self.root.sync_all()?;
        Ok(())
    }

    pub fn promote(&self, fp: Fingerprint, shard: u16, strict: bool) -> Result<()> {
        let source = self.directory(Path::new(&format!(".uploads/{fp}")), false, false)?;
        let final_path = fp.shard_path(shard);
        let path = Path::new(&final_path);
        // Persist the complete ancestry after publication. This covers a
        // directory created by another admitted writer as well as this one,
        // without separately flushing each mkdir and the destination twice.
        let dest = self.directory(path.parent().expect("generated fanout"), true, false)?;
        rfs::renameat_with(
            &*source,
            "shard.tmp",
            &*dest,
            path.file_name().expect("generated filename"),
            RenameFlags::NOREPLACE,
        )?;
        if strict {
            dest.sync_all()?;
            source.sync_all()?;
            let (a, _) = fp.fanout();
            self.directory(Path::new(&a), false, false)?.sync_all()?;
            self.root.sync_all()?;
        }
        Ok(())
    }

    pub fn persist_final_parent(&self, fp: Fingerprint) -> Result<()> {
        let (a, b) = fp.fanout();
        self.directory(Path::new(&format!("{a}/{b}")), false, false)?
            .sync_all()?;
        self.directory(Path::new(&a), false, false)?.sync_all()?;
        self.root.sync_all()?;
        Ok(())
    }

    pub fn retire_empty_stage(&self, fp: Fingerprint, strict: bool) -> Result<()> {
        let uploads = self.directory(Path::new(".uploads"), false, false)?;
        match rfs::unlinkat(&*uploads, fp.to_string(), AtFlags::REMOVEDIR) {
            Ok(()) => {
                if strict {
                    uploads.sync_all()?
                }
            }
            Err(rustix::io::Errno::NOENT | rustix::io::Errno::NOTEMPTY) => {}
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }

    pub fn unlink(&self, path: &Path, strict: bool) -> Result<bool> {
        let parent = match self.directory(
            path.parent()
                .ok_or_else(|| Error::Invalid("file has no parent".into()))?,
            false,
            false,
        ) {
            Ok(p) => p,
            Err(Error::Io(e)) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e),
        };
        match rfs::unlinkat(
            &*parent,
            path.file_name()
                .ok_or_else(|| Error::Invalid("file has no name".into()))?,
            AtFlags::empty(),
        ) {
            Ok(()) => {
                if strict {
                    parent.sync_all()?;
                }
                Ok(true)
            }
            Err(rustix::io::Errno::NOENT) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    pub fn create_repair(&self, fp: Fingerprint, shard: u16) -> Result<Arc<File>> {
        let dir = self.directory(Path::new(&format!(".uploads/{fp}")), true, true)?;
        let name = format!("repair.s{shard}.tmp");
        match rfs::openat(
            &*dir,
            &name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        ) {
            Ok(fd) => {
                let old = File::from(fd);
                if !old.metadata()?.is_file() || old.metadata()?.nlink() != 1 {
                    return Err(Error::Invalid("unsafe abandoned repair file".into()));
                }
                rfs::unlinkat(&*dir, &name, AtFlags::empty())?;
            }
            Err(rustix::io::Errno::NOENT) => {}
            Err(e) => return Err(e.into()),
        }
        Ok(Arc::new(File::from(rfs::openat(
            &*dir,
            &name,
            OFlags::RDWR | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )?)))
    }

    pub fn publish_repair(&self, fp: Fingerprint, shard: u16) -> Result<()> {
        let source = self.directory(Path::new(&format!(".uploads/{fp}")), false, false)?;
        let name = format!("repair.s{shard}.tmp");
        let final_path = fp.shard_path(shard);
        let path = Path::new(&final_path);
        let dest = self.directory(path.parent().expect("generated fanout"), true, true)?;
        rfs::renameat(
            &*source,
            &name,
            &*dest,
            path.file_name().expect("generated filename"),
        )?;
        dest.sync_all()?;
        source.sync_all()?;
        self.persist_final_parent(fp)?;
        self.retire_empty_stage(fp, true)
    }
}

fn open_root(path: &Path) -> Result<File> {
    if !path.is_absolute() {
        return Err(Error::Invalid("member roots must be absolute".into()));
    }
    let root = File::from(rfs::open(
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?);
    // The directory handle is the lifetime lease. A second engine cannot own it.
    if unsafe { libc::flock(root.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(io::Error::last_os_error().into());
    }
    Ok(root)
}

impl From<rustix::io::Errno> for Error {
    fn from(e: rustix::io::Errno) -> Self {
        Self::Io(e.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;
    fn roots(count: usize) -> (PathBuf, Vec<PathBuf>, PoolLayout) {
        let base = PathBuf::from(
            std::env::var_os("N1_LINK_TEST_ROOT").unwrap_or_else(|| "/tmp/n1-link-tests".into()),
        )
        .join(format!(
            "pool-{}-{:016x}",
            std::process::id(),
            OsRng.next_u64()
        ));
        let roots = (0..count)
            .map(|i| {
                let p = base.join(i.to_string());
                std::fs::create_dir_all(&p).unwrap();
                p
            })
            .collect();
        let layout = PoolLayout {
            id: PoolId([8; 16]),
            generation: 1,
            data_shards: count as u16,
            parity_shards: 0,
            block_bytes: 4096,
            placement_version: 1,
            shard_format: n1_link::ShardFormat::Crc32cSha256V2,
        };
        (base, roots, layout)
    }
    #[test]
    fn member_lease_follows_last_member_owner_and_not_an_inherited_descriptor() {
        let (base, paths, layout) = roots(2);
        let pool = Pool::fixture(layout.clone(), &paths).unwrap();
        let active = pool.members[0].as_ref().unwrap().clone();
        let inherited = active.root.try_clone().unwrap();
        drop(pool);
        assert!(Pool::reopen_fixture(layout.clone(), &paths).is_err());
        drop(active);
        let next = Pool::reopen_fixture(layout.clone(), &paths).unwrap();
        drop(inherited);
        assert!(Pool::reopen_fixture(layout.clone(), &paths).is_err());
        drop(next);
        drop(Pool::reopen_fixture(layout, &paths).unwrap());
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn physical_membership_guard_precedes_any_enrollment_write() {
        let (base, paths, layout) = roots(2);
        assert!(Pool::enroll(layout, &paths).is_err());
        assert!(paths.iter().all(|p| !p.join(MARKER).exists()));
        std::fs::remove_dir_all(base).unwrap();
    }
    #[test]
    fn exclusive_leases_and_restart_identity_are_enforced() {
        let (base, paths, layout) = roots(2);
        let pool = Pool::fixture(layout.clone(), &paths).unwrap();
        assert!(Pool::reopen_fixture(layout.clone(), &paths).is_err());
        let markers = pool
            .members
            .iter()
            .map(|m| m.as_ref().unwrap().marker.clone())
            .collect::<Vec<_>>();
        drop(pool);
        let reopened = Pool::reopen_fixture(layout.clone(), &paths).unwrap();
        for (member, marker) in reopened.members.iter().zip(markers) {
            assert_eq!(member.as_ref().unwrap().marker, marker);
        }
        drop(reopened);
        let wrong = PoolLayout {
            generation: 2,
            ..layout
        };
        assert!(Pool::reopen_fixture(wrong, &paths).is_err());
        std::fs::remove_dir_all(base).unwrap();
    }
    #[test]
    fn redirected_descendants_and_changed_root_binding_are_rejected() {
        let (base, paths, layout) = roots(1);
        let pool = Pool::fixture(layout, &paths).unwrap();
        let member = pool.members[0].as_ref().unwrap();
        let outside = base.join("outside");
        std::fs::create_dir(&outside).unwrap();
        symlink(&outside, paths[0].join("4a")).unwrap();
        assert!(member.directory(Path::new("4a/7c"), true, true).is_err());
        assert!(!outside.join("7c").exists());
        assert!(
            member
                .directory(Path::new("../outside"), true, true)
                .is_err()
        );
        std::fs::rename(&paths[0], base.join("old-root")).unwrap();
        symlink(&outside, &paths[0]).unwrap();
        assert!(member.check_binding().is_err());
        drop(pool);
        std::fs::remove_dir_all(base).unwrap();
    }
    #[test]
    fn promotion_never_replaces_existing_final_bytes() {
        let (base, paths, layout) = roots(1);
        let pool = Pool::fixture(layout, &paths).unwrap();
        let member = pool.members[0].as_ref().unwrap();
        let fp = Fingerprint([4; 32]);
        let (file, new) = member.open_stage(fp, true, true).unwrap();
        assert!(new);
        crate::core::linux_io::pwrite_all_at(&*file, b"first", 0).unwrap();
        file.sync_all().unwrap();
        drop(file);
        member.promote(fp, 0, true).unwrap();
        let (file, new) = member.open_stage(fp, true, true).unwrap();
        assert!(new);
        crate::core::linux_io::pwrite_all_at(&*file, b"second", 0).unwrap();
        drop(file);
        assert!(member.promote(fp, 0, true).is_err());
        assert_eq!(
            std::fs::read(paths[0].join(fp.shard_path(0))).unwrap(),
            b"first"
        );
        assert_eq!(
            std::fs::read(paths[0].join(fp.staging_path())).unwrap(),
            b"second"
        );
        drop(pool);
        std::fs::remove_dir_all(base).unwrap();
    }
}
