//! Canonical big-endian records, durable root publication and crash recovery.
use crate::{Error, FORMAT_VERSION, MAX_KEY_BYTES, MAX_VALUE_BYTES, Result, State, tree};
use std::{
    fs::{File, OpenOptions},
    io,
    os::unix::fs::{DirBuilderExt, FileExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

// Durable groups never share a 4 KiB write unit. Appending an unacknowledged
// group must not rewrite the sector containing a previously acknowledged one.
pub(crate) const DATA_START: u64 = 4096;
const RECORD_HEADER: usize = 28;
const MAX_RECORD_BYTES: usize = 32 * 1024 * 1024;
pub(crate) const VALUE: u8 = 1;
pub(crate) const NODE: u8 = 2;
pub(crate) const COMMIT: u8 = 3;

pub(crate) fn private_open(path: &Path, create: bool) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    if create {
        options.create_new(true);
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_file() || file.metadata()?.permissions().mode() & 0o077 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "M1 files must be private regular files",
        ));
    }
    Ok(file)
}

fn checksum(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
}

pub(crate) fn generation_path(path: &Path, id: u64) -> PathBuf {
    path.join(format!("store-{id:020}.m1"))
}

fn generation_id(name: &str) -> Option<u64> {
    let digits = name.strip_prefix("store-")?.strip_suffix(".m1")?;
    if digits.len() != 20 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

pub(crate) fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

pub(crate) struct Generation {
    pub file: File,
    pub id: u64,
    pub path: PathBuf,
    pub cache: Arc<tree::NodeCache>,
    pub poisoned: Arc<AtomicBool>,
    pub remove_on_drop: AtomicBool,
}

impl Generation {
    pub fn new(
        file: File,
        id: u64,
        path: PathBuf,
        cache: Arc<tree::NodeCache>,
        poisoned: Arc<AtomicBool>,
        remove: bool,
    ) -> Self {
        Self {
            file,
            id,
            path,
            cache,
            poisoned,
            remove_on_drop: AtomicBool::new(remove),
        }
    }

    pub fn record(&self, offset: u64) -> Result<Record> {
        // Tree/value references name complete immutable records. Exact reads
        // already detect truncation; a preceding fstat adds a syscall without
        // making a concurrent truncate safe. Recovery separately supplies the
        // actual EOF so it can distinguish an incomplete uncommitted suffix.
        let result = read_record(&self.file, offset, u64::MAX)
            .and_then(|record| {
                record.ok_or_else(|| Error::Corrupt("M1 record is truncated".into()))
            })
            .map_err(|error| match error {
                Error::Io(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                    Error::Corrupt("M1 record is truncated".into())
                }
                other => other,
            });
        if result.is_err() {
            self.poisoned.store(true, Ordering::Release);
        }
        result
    }

    pub fn value(&self, entry: &tree::Entry) -> Result<Vec<u8>> {
        let result = (|| {
            let mut record = self.record(entry.offset)?;
            let mut input = Input::new(&record.bytes);
            let sequence = input.u64()?;
            let key = input.key()?;
            let value = input.remaining();
            if record.kind != VALUE
                || sequence != entry.sequence
                || key != entry.key
                || value.len() != entry.length as usize
            {
                self.poisoned.store(true, Ordering::Release);
                return Err(Error::Corrupt(
                    "M1 value identity differs from its index".into(),
                ));
            }
            // Return the owned read buffer after validation instead of
            // allocating and copying a second value buffer on every lookup.
            let length = value.len();
            let begin = record.bytes.len() - length;
            record.bytes.copy_within(begin.., 0);
            record.bytes.truncate(length);
            Ok(record.bytes)
        })();
        if result.is_err() {
            self.poisoned.store(true, Ordering::Release);
        }
        result
    }
}

impl Drop for Generation {
    fn drop(&mut self) {
        // An abandoned checkpoint may later reuse this generation number.
        // Its cached nodes must not be confused with a different file's bytes.
        self.cache.remove_generation(self.id);
        if self.remove_on_drop.load(Ordering::Acquire) {
            let _ = std::fs::remove_file(&self.path);
            if let Some(parent) = self.path.parent() {
                let _ = sync_directory(parent);
            }
        }
    }
}

mod descriptor;
mod journal;
mod recovery;
pub(crate) use descriptor::{Manifest, cleanup, create_generation, open, publish_manifest};
pub(crate) use journal::{Appender, Input, decode_commit, encode_key};
use journal::{Record, read_record};
pub(crate) use recovery::recover;
