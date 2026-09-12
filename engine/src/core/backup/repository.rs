use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use super::crypto::{Key, digest};
use super::format::{BLOCK_BYTES, FORMAT, MAX_RECORD_BYTES, Record, Snapshot};
use super::fs::{atomic_new, private_dir, read_regular, safe_hash, safe_id, sync_parent};
use super::{Result, invalid};

pub struct Repository {
    pub path: PathBuf,
    pub(crate) key: Key,
    _lease: File,
    writable: bool,
}

impl Repository {
    pub fn initialize(path: &Path, key: &Path) -> Result<()> {
        private_dir(path)?;
        if fs::read_dir(path)?.next().is_some() {
            return Err(invalid(
                "repository initialization requires an empty directory",
            ));
        }
        let key = Key::load(key)?;
        atomic_new(
            &path.join("repository"),
            format!("{FORMAT}\n{}\n", key.id).as_bytes(),
        )?;
        for dir in ["blocks", "snapshots", "pending"] {
            private_dir(&path.join(dir))?;
        }
        Ok(())
    }
    pub fn open(path: &Path, key: &Path, exclusive: bool) -> Result<Self> {
        if !fs::symlink_metadata(path)?.is_dir()
            || fs::metadata(path)?.permissions().mode() & 0o077 != 0
        {
            return Err(invalid(
                "backup repository must be an owner-only real directory",
            ));
        }
        let path = fs::canonicalize(path)?;
        let key = Key::load(key)?;
        let expected = format!("{FORMAT}\n{}\n", key.id);
        if read_regular(&path.join("repository"), 256)? != expected.as_bytes() {
            return Err(invalid("backup repository format/key mismatch"));
        }
        for dir in ["blocks", "snapshots", "pending"] {
            if !fs::symlink_metadata(path.join(dir))?.is_dir() {
                return Err(invalid("invalid repository directory"));
            }
        }
        let lease = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path.join(".lock"))?;
        let mode = if exclusive {
            libc::LOCK_EX
        } else {
            libc::LOCK_SH
        };
        // SAFETY: File owns the descriptor until repository drop.
        if unsafe { libc::flock(lease.as_raw_fd(), mode | libc::LOCK_NB) } != 0 {
            return Err(invalid("backup repository is busy; retry later"));
        }
        Ok(Self {
            path,
            key,
            _lease: lease,
            writable: exclusive,
        })
    }
    pub(crate) fn ensure_writable(&self) -> Result<()> {
        if !self.writable {
            return Err(invalid("operation requires an exclusive repository lease"));
        }
        Ok(())
    }
    pub(crate) fn block_path(&self, hash: &str) -> Result<PathBuf> {
        safe_hash(hash)?;
        let parent = self.path.join("blocks").join(&hash[..2]);
        if parent.exists() && !fs::symlink_metadata(&parent)?.is_dir() {
            return Err(invalid("invalid block directory"));
        }
        Ok(parent.join(hash))
    }
    pub(crate) fn put(&self, bytes: &[u8], pending: &Path) -> Result<String> {
        self.ensure_writable()?;
        if bytes.len() > BLOCK_BYTES {
            return Err(invalid("backup block exceeds format limit"));
        }
        let hash = digest(bytes);
        let path = self.block_path(&hash)?;
        private_dir(path.parent().unwrap())?;
        if fs::symlink_metadata(&path).is_ok() {
            if self.get(&hash)? != bytes {
                return Err(invalid("existing backup block differs"));
            }
        } else {
            let encrypted = self.key.seal(&format!("{FORMAT}:block:{hash}"), bytes)?;
            if let Err(error) = atomic_new(&path, &encrypted) {
                if !error
                    .downcast_ref::<io::Error>()
                    .is_some_and(|e| e.kind() == io::ErrorKind::AlreadyExists)
                {
                    return Err(error);
                }
                if self.get(&hash)? != bytes {
                    return Err(error);
                }
            }
        }
        self.pin(&hash, pending)?;
        Ok(hash)
    }
    pub(crate) fn pin(&self, hash: &str, pending: &Path) -> Result<()> {
        safe_hash(hash)?;
        let parent = pending.join("blocks").join(&hash[..2]);
        private_dir(&parent)?;
        match fs::hard_link(self.block_path(hash)?, parent.join(hash)) {
            Ok(()) => sync_parent(&parent.join(hash))?,
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                let a = fs::symlink_metadata(self.block_path(hash)?)?;
                let b = fs::symlink_metadata(parent.join(hash))?;
                if !b.is_file() || a.dev() != b.dev() || a.ino() != b.ino() {
                    return Err(invalid("invalid backup block pin"));
                }
                sync_parent(&parent.join(hash))?;
            }
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }
    pub(crate) fn get(&self, hash: &str) -> Result<Vec<u8>> {
        let encrypted = read_regular(&self.block_path(hash)?, BLOCK_BYTES + 28)?;
        let plain = self
            .key
            .open(&format!("{FORMAT}:block:{hash}"), encrypted)?;
        if digest(&plain) != hash {
            return Err(invalid("backup block checksum mismatch"));
        }
        Ok(plain)
    }
    pub(crate) fn pending(&self, id: &str) -> Result<PathBuf> {
        self.ensure_writable()?;
        safe_id(id)?;
        let dir = self.path.join("pending").join(id);
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new().mode(0o700).create(&dir)?;
        private_dir(&dir.join("blocks"))?;
        Ok(dir)
    }
    pub(crate) fn publish(&self, snapshot: &Snapshot, pending: &Path) -> Result<()> {
        self.ensure_writable()?;
        safe_id(&snapshot.id)?;
        let bytes = serde_json::to_vec(snapshot)?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(invalid("snapshot manifest index exceeds format limit"));
        }
        let encrypted = self
            .key
            .seal(&format!("{FORMAT}:snapshot:{}", snapshot.id), &bytes)?;
        atomic_new(&pending.join("manifest"), &encrypted)?;
        File::open(pending)?.sync_all()?;
        let target = self.path.join("snapshots").join(&snapshot.id);
        if fs::symlink_metadata(&target).is_ok() {
            return Err(invalid("snapshot already exists"));
        }
        fs::rename(pending, &target)?;
        sync_parent(&target)?;
        File::open(self.path.join("pending"))?.sync_all()?;
        Ok(())
    }
    pub fn snapshot(&self, id: &str) -> Result<Snapshot> {
        safe_id(id)?;
        let dir = self.path.join("snapshots").join(id);
        if !fs::symlink_metadata(&dir)?.is_dir() {
            return Err(invalid("invalid snapshot directory"));
        }
        let raw = read_regular(&dir.join("manifest"), MAX_RECORD_BYTES + 28)?;
        let snapshot: Snapshot =
            serde_json::from_slice(&self.key.open(&format!("{FORMAT}:snapshot:{id}"), raw)?)?;
        if snapshot.id != id
            || snapshot.format != FORMAT
            || snapshot.engine_format != 3
            || snapshot.roots.is_empty()
        {
            return Err(invalid("invalid or unsupported N1 snapshot"));
        }
        Ok(snapshot)
    }

    pub(crate) fn sync_publication(&self, id: &str) -> Result<()> {
        self.ensure_writable()?;
        safe_id(id)?;
        let directory = self.path.join("snapshots").join(id);
        File::open(&directory)?.sync_all()?;
        sync_parent(&directory)?;
        File::open(self.path.join("pending"))?.sync_all()?;
        Ok(())
    }
    pub fn list(&self) -> Result<Vec<Snapshot>> {
        let mut snapshots = Vec::new();
        for entry in fs::read_dir(self.path.join("snapshots"))? {
            let entry = entry?;
            snapshots.push(
                self.snapshot(
                    entry
                        .file_name()
                        .to_str()
                        .ok_or_else(|| invalid("invalid snapshot name"))?,
                )?,
            );
        }
        snapshots.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(snapshots)
    }
    pub(crate) fn records(
        &self,
        snapshot: &Snapshot,
        mut visit: impl FnMut(Record) -> Result<()>,
    ) -> Result<()> {
        let reader = ManifestReader {
            repo: self,
            hashes: snapshot.manifest.iter(),
            block: io::Cursor::new(Vec::new()),
        };
        let mut reader = io::BufReader::new(reader);
        let mut count = 0u64;
        loop {
            let mut line = Vec::new();
            io::BufRead::read_until(
                &mut (&mut reader).take(MAX_RECORD_BYTES as u64 + 1),
                b'\n',
                &mut line,
            )?;
            if line.is_empty() {
                break;
            }
            if line.len() > MAX_RECORD_BYTES || line.last() != Some(&b'\n') {
                return Err(invalid("truncated/oversized backup manifest record"));
            }
            visit(serde_json::from_slice(&line)?)?;
            count += 1;
        }
        if count != snapshot.records {
            return Err(invalid("snapshot record count mismatch"));
        }
        Ok(())
    }
}

pub(crate) struct ManifestWriter<'a> {
    repo: &'a Repository,
    pending: PathBuf,
    buffer: Vec<u8>,
    pub blocks: Vec<String>,
    pub count: u64,
}
impl<'a> ManifestWriter<'a> {
    pub fn new(repo: &'a Repository, pending: PathBuf) -> Self {
        Self {
            repo,
            pending,
            buffer: Vec::new(),
            blocks: Vec::new(),
            count: 0,
        }
    }
    pub fn record(&mut self, record: &Record) -> Result<()> {
        let mut bytes = serde_json::to_vec(record)?;
        bytes.push(b'\n');
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(invalid("backup record exceeds format limit"));
        }
        self.write_all(&bytes)?;
        self.count += 1;
        Ok(())
    }
    pub fn finish(&mut self) -> Result<()> {
        if !self.buffer.is_empty() {
            if self.blocks.len() >= 100_000 {
                return Err(invalid(
                    "snapshot manifest index exceeds bounded format limit",
                ));
            }
            self.blocks
                .push(self.repo.put(&self.buffer, &self.pending)?);
            self.buffer.clear();
        }
        Ok(())
    }
}
impl Write for ManifestWriter<'_> {
    fn write(&mut self, mut bytes: &[u8]) -> io::Result<usize> {
        let total = bytes.len();
        while !bytes.is_empty() {
            let n = bytes.len().min(BLOCK_BYTES - self.buffer.len());
            self.buffer.extend_from_slice(&bytes[..n]);
            bytes = &bytes[n..];
            if self.buffer.len() == BLOCK_BYTES {
                self.finish().map_err(io::Error::other)?;
            }
        }
        Ok(total)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.finish().map_err(io::Error::other)
    }
}
struct ManifestReader<'a> {
    repo: &'a Repository,
    hashes: std::slice::Iter<'a, String>,
    block: io::Cursor<Vec<u8>>,
}
impl Read for ManifestReader<'_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        loop {
            let n = self.block.read(out)?;
            if n > 0 {
                return Ok(n);
            }
            let Some(hash) = self.hashes.next() else {
                return Ok(0);
            };
            self.block = io::Cursor::new(self.repo.get(hash).map_err(io::Error::other)?);
        }
    }
}
