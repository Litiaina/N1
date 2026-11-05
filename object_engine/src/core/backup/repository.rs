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
    cancel: tokio_util::sync::CancellationToken,
}

impl Drop for Repository {
    fn drop(&mut self) {
        // A concurrent process spawn can briefly inherit this close-on-exec
        // descriptor between fork and exec. Closing only the parent's fd would
        // leave its flock alive in that unrelated child. Release the owner's
        // lease explicitly before closing; no child may use a Repository.
        unsafe { libc::flock(self._lease.as_raw_fd(), libc::LOCK_UN) };
    }
}

#[cfg(test)]
mod lease_tests {
    use super::*;
    #[test]
    fn dropping_repository_releases_lease_despite_a_fork_inherited_descriptor() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("repository");
        let key = directory.path().join("key");
        crate::core::backup::create_key(&key).unwrap();
        Repository::initialize(&path, &key).unwrap();
        let owner = Repository::open(&path, &key, true).unwrap();
        // dup and fork preserve the same open-file description. Keeping this
        // descriptor alive models the child-before-exec window deterministically.
        let inherited = owner._lease.try_clone().unwrap();
        assert!(Repository::open(&path, &key, true).is_err());
        drop(owner);
        let next = Repository::open(&path, &key, true).unwrap();
        assert!(Repository::open(&path, &key, false).is_err());
        drop(inherited);
        assert!(Repository::open(&path, &key, false).is_err());
        drop(next);
        Repository::open(&path, &key, false).unwrap();
    }
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
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "backup repository is busy; retry later",
            )
            .into());
        }
        Ok(Self {
            path,
            key,
            _lease: lease,
            writable: exclusive,
            cancel: tokio_util::sync::CancellationToken::new(),
        })
    }
    pub(crate) fn ensure_writable(&self) -> Result<()> {
        self.check_cancel()?;
        if !self.writable {
            return Err(invalid("operation requires an exclusive repository lease"));
        }
        Ok(())
    }
    pub(crate) fn with_cancel(mut self, cancel: tokio_util::sync::CancellationToken) -> Self {
        self.cancel = cancel;
        self
    }
    pub(crate) fn check_cancel(&self) -> Result<()> {
        if self.cancel.is_cancelled() {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "recovery job stopped").into());
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
                if error
                    .downcast_ref::<io::Error>()
                    .is_none_or(|e| e.kind() != io::ErrorKind::AlreadyExists)
                {
                    return Err(error);
                }
                if self.get(&hash)? != bytes {
                    return Err(error);
                }
            }
        }
        sync_parent(&path)?;
        self.pin(&hash, pending)?;
        Ok(hash)
    }
    pub(crate) fn pin(&self, hash: &str, pending: &Path) -> Result<()> {
        self.check_cancel()?;
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
        self.check_cancel()?;
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
        let manifest = pending.join("manifest");
        match read_regular(&manifest, MAX_RECORD_BYTES + 28) {
            Ok(existing) => {
                if self
                    .key
                    .open(&format!("{FORMAT}:snapshot:{}", snapshot.id), existing)?
                    != bytes
                {
                    return Err(invalid("pending manifest conflicts with recovery point"));
                }
                File::open(&manifest)?.sync_all()?;
                sync_parent(&manifest)?;
            }
            Err(e)
                if e.downcast_ref::<io::Error>()
                    .is_some_and(|e| e.kind() == io::ErrorKind::NotFound) =>
            {
                atomic_new(&manifest, &encrypted)?
            }
            Err(e) => return Err(e),
        }
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
        self.check_cancel()?;
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
            || snapshot.engine_format != super::format::ENGINE_FORMAT
            || snapshot.roots.is_empty()
        {
            return Err(invalid("invalid or unsupported N1 snapshot"));
        }
        super::format::validate_snapshot(&snapshot)?;
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
    pub fn walk_snapshots(&self, mut visit: impl FnMut(Snapshot) -> Result<()>) -> Result<()> {
        for entry in fs::read_dir(self.path.join("snapshots"))? {
            self.check_cancel()?;
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                return Err(invalid("invalid snapshot entry"));
            }
            let id = entry.file_name();
            let id = id.to_str().ok_or_else(|| invalid("non-UTF8 snapshot ID"))?;
            visit(self.snapshot(id)?)?;
        }
        Ok(())
    }
    pub fn list_page(&self, after: &str, limit: usize) -> Result<Vec<super::format::Summary>> {
        if limit == 0 || limit > 256 {
            return Err(invalid("recovery page size must be 1..=256"));
        }
        if !after.is_empty() {
            safe_id(after)?;
        }
        let mut selected = std::collections::BinaryHeap::new();
        for entry in fs::read_dir(self.path.join("snapshots"))? {
            self.check_cancel()?;
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                return Err(invalid("invalid snapshot entry"));
            }
            let id = entry
                .file_name()
                .into_string()
                .map_err(|_| invalid("non-UTF8 snapshot ID"))?;
            safe_id(&id)?;
            if id.as_str() <= after {
                continue;
            }
            selected.push(id);
            if selected.len() > limit {
                selected.pop();
            }
        }
        selected
            .into_sorted_vec()
            .into_iter()
            .map(|id| {
                self.snapshot(&id)
                    .map(|point| super::format::Summary::from(&point))
            })
            .collect()
    }
    #[cfg(test)]
    pub fn list(&self) -> Result<Vec<Snapshot>> {
        let mut points = Vec::new();
        self.walk_snapshots(|point| {
            points.push(point);
            Ok(())
        })?;
        Ok(points)
    }
    pub(crate) fn records(
        &self,
        snapshot: &Snapshot,
        mut visit: impl FnMut(Record) -> Result<()>,
    ) -> Result<()> {
        let reader = ManifestReader::new(self, snapshot);
        let mut reader = io::BufReader::new(reader);
        let mut count = 0u64;
        loop {
            self.check_cancel()?;
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

// A bounded Merkle tree keeps the snapshot header independent of inventory
// size. Tests use smaller leaves/fanout to exercise multiple levels cheaply.
const MANIFEST_LEAF_BYTES: usize = if cfg!(test) { 512 } else { BLOCK_BYTES };
const WRITER_FANOUT: usize = if cfg!(test) { 4 } else { 4096 };
const MAX_FANOUT: usize = 4096;
const MAX_DEPTH: u8 = 8;
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestNode {
    format: String,
    children: Vec<String>,
}
fn node_children(repo: &Repository, hash: &str) -> Result<Vec<String>> {
    let bytes = repo.get(hash)?;
    if bytes.len() > 512 * 1024 {
        return Err(invalid("manifest node exceeds limit"));
    }
    let node: ManifestNode = serde_json::from_slice(&bytes)?;
    if node.format != "N1-LINK-manifest-v1"
        || node.children.is_empty()
        || node.children.len() > MAX_FANOUT
    {
        return Err(invalid("invalid manifest node"));
    }
    for hash in &node.children {
        safe_hash(hash)?;
    }
    Ok(node.children)
}
pub(crate) struct ManifestWriter<'a> {
    repo: &'a Repository,
    pending: PathBuf,
    buffer: Vec<u8>,
    levels: Vec<Vec<String>>,
    pub blocks: Vec<String>,
    pub depth: u8,
    pub count: u64,
    finished: bool,
}
impl<'a> ManifestWriter<'a> {
    pub fn new(repo: &'a Repository, pending: PathBuf) -> Self {
        Self {
            repo,
            pending,
            buffer: Vec::new(),
            levels: vec![Vec::new()],
            blocks: Vec::new(),
            depth: 0,
            count: 0,
            finished: false,
        }
    }
    pub fn record(&mut self, record: &Record) -> Result<()> {
        let mut bytes = serde_json::to_vec(record)?;
        bytes.push(b'\n');
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(invalid("backup record exceeds limit"));
        }
        self.write_all(&bytes)?;
        self.count = self
            .count
            .checked_add(1)
            .ok_or_else(|| invalid("manifest record count overflow"))?;
        Ok(())
    }
    fn push_hash(&mut self, level: usize, hash: String) -> Result<()> {
        if level > MAX_DEPTH as usize {
            return Err(invalid("manifest depth exceeds format limit"));
        }
        while self.levels.len() <= level {
            self.levels.push(Vec::new());
        }
        self.levels[level].push(hash);
        if self.levels[level].len() >= WRITER_FANOUT {
            self.flush_level(level)?;
        }
        Ok(())
    }
    fn flush_level(&mut self, level: usize) -> Result<()> {
        let children = std::mem::take(&mut self.levels[level]);
        if children.is_empty() {
            return Ok(());
        }
        let bytes = serde_json::to_vec(&ManifestNode {
            format: "N1-LINK-manifest-v1".into(),
            children,
        })?;
        let hash = self.repo.put(&bytes, &self.pending)?;
        self.push_hash(level + 1, hash)
    }
    fn flush_leaf(&mut self) -> Result<()> {
        if !self.buffer.is_empty() {
            let hash = self.repo.put(&self.buffer, &self.pending)?;
            self.buffer.clear();
            self.push_hash(0, hash)?;
        }
        Ok(())
    }
    pub fn finish(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }
        self.flush_leaf()?;
        let mut level = 0;
        loop {
            let highest = self
                .levels
                .iter()
                .rposition(|children| !children.is_empty())
                .unwrap_or(0);
            if level >= highest {
                self.blocks = std::mem::take(&mut self.levels[highest]);
                self.depth = highest as u8;
                break;
            }
            self.flush_level(level)?;
            level += 1;
        }
        self.finished = true;
        Ok(())
    }
}
impl Write for ManifestWriter<'_> {
    fn write(&mut self, mut bytes: &[u8]) -> io::Result<usize> {
        if self.finished {
            return Err(io::Error::other("manifest is already finished"));
        }
        let total = bytes.len();
        while !bytes.is_empty() {
            let n = bytes.len().min(MANIFEST_LEAF_BYTES - self.buffer.len());
            self.buffer.extend_from_slice(&bytes[..n]);
            bytes = &bytes[n..];
            if self.buffer.len() == MANIFEST_LEAF_BYTES {
                self.flush_leaf().map_err(repository_io_error)?;
            }
        }
        Ok(total)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.flush_leaf().map_err(repository_io_error)
    }
}
struct ManifestReader<'a> {
    repo: &'a Repository,
    stack: Vec<(std::vec::IntoIter<String>, u8)>,
    block: io::Cursor<Vec<u8>>,
}
impl<'a> ManifestReader<'a> {
    fn new(repo: &'a Repository, snapshot: &Snapshot) -> Self {
        Self {
            repo,
            stack: vec![(
                snapshot.manifest.clone().into_iter(),
                snapshot.manifest_depth,
            )],
            block: io::Cursor::new(Vec::new()),
        }
    }
    fn leaf(&mut self) -> Result<Option<String>> {
        loop {
            let Some((hashes, depth)) = self.stack.last_mut() else {
                return Ok(None);
            };
            let Some(hash) = hashes.next() else {
                self.stack.pop();
                continue;
            };
            let depth = *depth;
            if depth == 0 {
                return Ok(Some(hash));
            }
            let children = node_children(self.repo, &hash)?;
            self.stack.push((children.into_iter(), depth - 1));
        }
    }
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
            let Some(hash) = self.leaf().map_err(repository_io_error)? else {
                return Ok(0);
            };
            self.block = io::Cursor::new(self.repo.get(&hash).map_err(repository_io_error)?);
        }
    }
}
impl Repository {
    pub(crate) fn manifest_hashes(
        &self,
        snapshot: &Snapshot,
        mut visit: impl FnMut(&str) -> Result<()>,
    ) -> Result<()> {
        if let Some(hash) = &snapshot.service_control {
            visit(hash)?;
        }
        let mut stack = vec![(
            snapshot.manifest.clone().into_iter(),
            snapshot.manifest_depth,
        )];
        while let Some((hashes, depth)) = stack.last_mut() {
            let Some(hash) = hashes.next() else {
                stack.pop();
                continue;
            };
            let depth = *depth;
            visit(&hash)?;
            if depth > 0 {
                let children = node_children(self, &hash)?;
                stack.push((children.into_iter(), depth - 1));
            }
        }
        Ok(())
    }
}

impl Repository {
    pub fn verify(&self, snapshot: &Snapshot) -> Result<()> {
        super::format::validate_snapshot(snapshot)?;
        if let Some(hash) = &snapshot.service_control {
            super::control::decode(&self.get(hash)?, snapshot)?;
        }
        self.files(snapshot, |root, path, length, reader| {
            let marker = snapshot.marker(root)?;
            super::restore::validate_member_file(
                path,
                &marker,
                reader,
                length,
                snapshot.capture_mode == "offline_all",
            )
        })
    }
    /// Reassemble one bounded chunk at a time. Neither verification nor restore
    /// allocates a complete shard or its complete list of block identifiers.
    pub(crate) fn files(
        &self,
        snapshot: &Snapshot,
        mut visit: impl FnMut(usize, &str, u64, &mut dyn Read) -> Result<()>,
    ) -> Result<()> {
        let stream = ManifestReader::new(self, snapshot);
        let mut records = Records {
            reader: io::BufReader::new(stream),
            seen: 0,
            expected: snapshot.records,
            ended: false,
        };
        let mut files = 0u64;
        let mut bytes = 0u64;
        let mut markers = vec![false; snapshot.roots.len()];
        while let Some(record) = records.next() {
            let record = record?;
            if record.root >= snapshot.roots.len() || record.offset != 0 {
                return Err(invalid("file manifest begins at an invalid root/offset"));
            }
            super::fs::safe_relative(&record.path)?;
            if super::restore::allowed_path(
                &record.path,
                &snapshot.marker(record.root)?,
                snapshot.capture_mode == "offline_all",
            )?
            .is_none()
            {
                return Err(invalid("unsupported recovery file path"));
            }
            if record.path == ".n1-arc-pool.json" {
                if markers[record.root] {
                    return Err(invalid("duplicate recovery pool marker"));
                }
                markers[record.root] = true;
            }
            let root = record.root;
            let path = record.path.clone();
            let total = record.total_bytes;
            let mut chunks = FileChunks {
                repo: self,
                records: &mut records,
                first: Some(record),
                buffer: io::Cursor::new(Vec::new()),
                root,
                path: path.clone(),
                total,
                offset: 0,
                last: false,
            };
            visit(root, &path, total, &mut chunks)?;
            let mut extra = [0; 1];
            if chunks.read(&mut extra)? != 0 || chunks.offset != total {
                return Err(invalid(
                    "recovery file visitor did not consume the complete file",
                ));
            }
            files += 1;
            bytes = bytes
                .checked_add(total)
                .ok_or_else(|| invalid("recovery byte count overflow"))?;
        }
        if markers.iter().any(|seen| !seen)
            || files != snapshot.files
            || bytes != snapshot.source_bytes
        {
            return Err(invalid("snapshot file/byte count mismatch"));
        }
        Ok(())
    }
}
struct Records<'a> {
    reader: io::BufReader<ManifestReader<'a>>,
    seen: u64,
    expected: u64,
    ended: bool,
}
impl Iterator for Records<'_> {
    type Item = Result<Record>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.ended {
            return None;
        }
        let result = (|| {
            let mut line = Vec::new();
            io::BufRead::read_until(
                &mut (&mut self.reader).take(MAX_RECORD_BYTES as u64 + 1),
                b'\n',
                &mut line,
            )?;
            if line.is_empty() {
                self.ended = true;
                if self.seen != self.expected {
                    return Err(invalid("snapshot record count mismatch"));
                }
                return Ok(None);
            }
            if line.len() > MAX_RECORD_BYTES || line.last() != Some(&b'\n') {
                return Err(invalid("truncated or oversized recovery record"));
            }
            self.seen = self
                .seen
                .checked_add(1)
                .ok_or_else(|| invalid("record count overflow"))?;
            if self.seen > self.expected {
                return Err(invalid("too many recovery records"));
            }
            serde_json::from_slice(&line).map(Some).map_err(Into::into)
        })();
        match result {
            Ok(record) => record.map(Ok),
            Err(error) => {
                self.ended = true;
                Some(Err(error))
            }
        }
    }
}
struct FileChunks<'a, 'b> {
    repo: &'a Repository,
    records: &'b mut Records<'a>,
    first: Option<Record>,
    buffer: io::Cursor<Vec<u8>>,
    root: usize,
    path: String,
    total: u64,
    offset: u64,
    last: bool,
}
impl Read for FileChunks<'_, '_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        loop {
            let n = self.buffer.read(out)?;
            if n > 0 {
                return Ok(n);
            }
            if self.last {
                return Ok(0);
            }
            let record = match self.first.take() {
                Some(record) => record,
                None => self
                    .records
                    .next()
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::UnexpectedEof, "unfinished recovery file")
                    })?
                    .map_err(repository_io_error)?,
            };
            if record.root != self.root
                || record.path != self.path
                || record.total_bytes != self.total
                || record.offset != self.offset
                || record.bytes as usize > BLOCK_BYTES
                || (!record.last && record.bytes == 0)
            {
                return Err(io::Error::other("inconsistent recovery file chunks"));
            }
            let bytes = self.repo.get(&record.hash).map_err(repository_io_error)?;
            self.offset = self
                .offset
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| io::Error::other("recovery offset overflow"))?;
            if bytes.len() != record.bytes as usize
                || self.offset > self.total
                || record.last != (self.offset == self.total)
            {
                return Err(io::Error::other(
                    "recovery chunk length/termination mismatch",
                ));
            }
            self.last = record.last;
            self.buffer = io::Cursor::new(bytes);
        }
    }
}

impl Repository {
    pub(crate) fn raw_block(&self, hash: &str) -> Result<Vec<u8>> {
        self.check_cancel()?;
        let raw = read_regular(&self.block_path(hash)?, BLOCK_BYTES + 28)?;
        let plain = self
            .key
            .open(&format!("{FORMAT}:block:{hash}"), raw.clone())?;
        if digest(&plain) != hash {
            return Err(invalid("replication source block checksum mismatch"));
        }
        Ok(raw)
    }
    pub(crate) fn import_raw(&self, hash: &str, raw: &[u8], pending: &Path) -> Result<()> {
        self.ensure_writable()?;
        safe_hash(hash)?;
        if raw.len() > BLOCK_BYTES + 28 {
            return Err(invalid("encrypted recovery block exceeds limit"));
        }
        let plain = self
            .key
            .open(&format!("{FORMAT}:block:{hash}"), raw.to_vec())?;
        if digest(&plain) != hash {
            return Err(invalid("received recovery block checksum mismatch"));
        }
        let path = self.block_path(hash)?;
        private_dir(path.parent().unwrap())?;
        match self.get(hash) {
            Ok(existing) => {
                if existing != plain {
                    return Err(invalid("existing recovery block conflicts"));
                }
            }
            Err(e)
                if e.downcast_ref::<io::Error>()
                    .is_some_and(|e| e.kind() == io::ErrorKind::NotFound) =>
            {
                atomic_new(&path, raw)?
            }
            Err(e) => return Err(e),
        }
        sync_parent(&path)?;
        self.pin(hash, pending)
    }
}

fn repository_io_error(error: super::Error) -> io::Error {
    match error.downcast::<io::Error>() {
        Ok(error) => *error,
        Err(error) => io::Error::other(error),
    }
}
