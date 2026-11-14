//! M1's service-facing storage boundary. Domain records and NOA semantics stay
//! here; the independent m1_storage crate owns pages, transactions and recovery.
use crate::{Error, RecordKey, Result, Table};
use n1_link::PoolId;
use std::{ops::Bound, path::Path, sync::Arc};
mod backup;

const MAX_VALUE_BYTES: usize = 1024 * 1024;
const MAX_TRANSACTION_BYTES: usize = 16 * 1024 * 1024;
const MAX_TRANSACTION_WRITES: usize = 4096;
const MAX_PAGE_SIZE: usize = 1000;
const MAX_PAGE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct Options {
    pub cache_bytes: u64,
    /// Retained operator setting: bounds batching memory, not a process RSS cap.
    pub memtable_bytes: u64,
    /// Minimum changed-data threshold for background checkpoint admission.
    pub journal_bytes: u64,
    pub background_workers: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            cache_bytes: 64 * 1024 * 1024,
            memtable_bytes: 8 * 1024 * 1024,
            journal_bytes: 256 * 1024 * 1024,
            background_workers: 2,
        }
    }
}

struct Inner {
    db: m1_storage::Database,
}

/// Process/file ownership survives through every transaction and snapshot.
#[derive(Clone)]
pub struct MetadataEngine(Arc<Inner>);

impl MetadataEngine {
    pub fn open(path: impl AsRef<Path>, options: Options) -> Result<Self> {
        if !(1..=64).contains(&options.background_workers)
            || options.cache_bytes < 1024 * 1024
            || options.memtable_bytes < 1024 * 1024
            || options.journal_bytes < 64 * 1024 * 1024
        {
            return Err(Error::Invalid("invalid metadata resource limits".into()));
        }
        private_database_directory(path.as_ref())?;
        let db = m1_storage::Database::open(
            path,
            m1_storage::Options {
                cache_bytes: options.cache_bytes,
                write_buffer_bytes: options.memtable_bytes,
                journal_bytes: options.journal_bytes,
                background_workers: options.background_workers,
            },
        )?;
        Ok(Self(Arc::new(Inner { db })))
    }

    pub fn snapshot(&self) -> MetadataSnapshot {
        MetadataSnapshot {
            view: self.0.db.snapshot(),
        }
    }

    pub fn write(&self) -> Result<MetadataTransaction> {
        Ok(MetadataTransaction {
            tx: self.0.db.write()?,
            writes: 0,
            bytes: 0,
            failed: false,
        })
    }

    /// Retry only serialization conflicts. Closures must not perform external
    /// side effects or object I/O, because a conflicted attempt is discarded.
    pub fn transact<T>(
        &self,
        attempts: usize,
        mut apply: impl FnMut(&mut MetadataTransaction) -> Result<T>,
    ) -> Result<T> {
        if !(1..=1024).contains(&attempts) {
            return Err(Error::Invalid("transaction attempts must be 1–1024".into()));
        }
        for _ in 0..attempts {
            let mut tx = self.write()?;
            let result = apply(&mut tx)?;
            #[cfg(feature = "upload-profile")]
            let start = std::time::Instant::now();
            let committed = tx.commit();
            #[cfg(feature = "upload-profile")]
            if let Some(trace) = crate::upload_profile::Trace::current() {
                let name = if matches!(committed, Err(Error::Conflict)) {
                    trace
                        .conflicts
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    "metadata.commit_conflict"
                } else {
                    "metadata.commit"
                };
                trace.record(name, start, std::time::Instant::now());
            }
            match committed {
                Ok(()) => return Ok(result),
                Err(Error::Conflict) => std::thread::yield_now(),
                Err(error) => return Err(error),
            }
        }
        Err(Error::Conflict)
    }

    pub fn disk_bytes(&self) -> Result<u64> {
        Ok(self.0.db.disk_bytes()?)
    }

    pub fn verify(&self) -> Result<u64> {
        Ok(self.0.db.verify()?.records)
    }
}

// A metadata database contains private LINK access credentials.
fn private_database_directory(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.is_dir() || metadata.file_type().is_symlink() => {
            return Err(Error::Invalid(
                "metadata path must be a real directory".into(),
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut builder = std::fs::DirBuilder::new();
            builder.recursive(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            builder.create(path)?;
        }
        Err(error) => return Err(error.into()),
    }
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(Error::Invalid(
            "metadata path must be a real directory".into(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(Error::Invalid("metadata directory holds private credentials; use owner-only permissions (chmod 700)".into()));
        }
    }
    Ok(())
}

/// A stable cross-table view, retained only for a request/page or backup.
pub struct MetadataSnapshot {
    view: m1_storage::Snapshot,
}

impl MetadataSnapshot {
    /// Startup index construction; routine maintenance uses indexed prefixes.
    pub(crate) fn scan_all(
        &self,
        table: Table,
        after: Option<&RecordKey>,
        limit: usize,
    ) -> Result<Vec<(RecordKey, Vec<u8>)>> {
        validate_limit(limit)?;
        let start = after.map_or_else(
            || Bound::Included(vec![table.index() as u8]),
            |key| Bound::Excluded(storage_key(table, key)),
        );
        collect_page(
            self.view
                .range(start, Bound::Excluded(vec![table.index() as u8 + 1]))?,
            table,
            limit,
        )
    }

    pub(crate) fn scan_from(
        &self,
        table: Table,
        fragment: PoolId,
        prefix: &str,
        from: &RecordKey,
        limit: usize,
    ) -> Result<Vec<(RecordKey, Vec<u8>)>> {
        validate_limit(limit)?;
        let (start, end) = scan_bounds(table, fragment, prefix, Some((from, true)))?;
        collect_page(self.view.range(start, end)?, table, limit)
    }

    pub fn get(&self, table: Table, key: &RecordKey) -> Result<Option<Vec<u8>>> {
        Ok(self.view.get(&storage_key(table, key))?)
    }

    /// Later pages using a new snapshot have a new view, not one global listing.
    pub fn scan(
        &self,
        table: Table,
        fragment: PoolId,
        prefix: &str,
        after: Option<&RecordKey>,
        limit: usize,
    ) -> Result<Vec<(RecordKey, Vec<u8>)>> {
        validate_limit(limit)?;
        let (start, end) = scan_bounds(table, fragment, prefix, after.map(|key| (key, false)))?;
        collect_page(self.view.range(start, end)?, table, limit)
    }
}

/// Concurrent optimistic transaction; successful commits are synchronized.
pub struct MetadataTransaction {
    tx: m1_storage::Transaction,
    writes: usize,
    bytes: usize,
    failed: bool,
}

impl MetadataTransaction {
    pub fn get(&self, table: Table, key: &RecordKey) -> Result<Option<Vec<u8>>> {
        Ok(self.tx.get(&storage_key(table, key))?)
    }

    pub fn scan(
        &self,
        table: Table,
        fragment: PoolId,
        prefix: &str,
        after: Option<&RecordKey>,
        limit: usize,
    ) -> Result<Vec<(RecordKey, Vec<u8>)>> {
        validate_limit(limit)?;
        let (start, end) = scan_bounds(table, fragment, prefix, after.map(|key| (key, false)))?;
        self.tx
            .scan(start, end, limit, MAX_PAGE_BYTES)?
            .into_iter()
            .map(|(key, value)| Ok((decode_key(table, &key)?, value)))
            .collect()
    }

    pub fn put(&mut self, table: Table, key: &RecordKey, value: &[u8]) -> Result<()> {
        self.reserve(key, value.len())?;
        self.tx.put(&storage_key(table, key), value)?;
        Ok(())
    }

    pub fn remove(&mut self, table: Table, key: &RecordKey) -> Result<()> {
        self.reserve(key, 0)?;
        self.tx.remove(&storage_key(table, key))?;
        Ok(())
    }

    pub fn compare_exchange(
        &mut self,
        table: Table,
        key: &RecordKey,
        expected: Option<&[u8]>,
        replacement: Option<&[u8]>,
    ) -> Result<bool> {
        if self.get(table, key)?.as_deref() != expected {
            return Ok(false);
        }
        match replacement {
            Some(value) => self.put(table, key, value)?,
            None => self.remove(table, key)?,
        }
        Ok(true)
    }

    pub fn commit(self) -> Result<()> {
        if self.failed {
            return Err(Error::Invalid(
                "metadata transaction exceeded its limits and must be discarded".into(),
            ));
        }
        Ok(self.tx.commit()?)
    }

    fn reserve(&mut self, key: &RecordKey, value_len: usize) -> Result<()> {
        let bytes = key.identifier().len() + 16 + value_len;
        if value_len > MAX_VALUE_BYTES
            || self.writes >= MAX_TRANSACTION_WRITES
            || self.bytes + bytes > MAX_TRANSACTION_BYTES
        {
            self.failed = true;
            return Err(Error::Invalid(
                "metadata transaction size limit exceeded".into(),
            ));
        }
        self.bytes += bytes;
        self.writes += 1;
        Ok(())
    }
}

fn storage_key(table: Table, key: &RecordKey) -> Vec<u8> {
    let mut bytes = vec![table.index() as u8];
    bytes.extend_from_slice(&key.encode());
    bytes
}

fn decode_key(table: Table, key: &[u8]) -> Result<RecordKey> {
    if key.first() != Some(&(table.index() as u8)) {
        return Err(Error::Incompatible(
            "metadata cursor crossed its table boundary".into(),
        ));
    }
    RecordKey::decode(&key[1..])
}

fn validate_limit(limit: usize) -> Result<()> {
    if !(1..=MAX_PAGE_SIZE).contains(&limit) {
        return Err(Error::Invalid("metadata page size must be 1–1000".into()));
    }
    Ok(())
}

fn collect_page(
    rows: m1_storage::Scan,
    table: Table,
    limit: usize,
) -> Result<Vec<(RecordKey, Vec<u8>)>> {
    let mut page = Vec::with_capacity(limit);
    let mut bytes = 0;
    for row in rows.take(limit) {
        let (key, value) = row?;
        let size = key.len() + value.len();
        if bytes + size > MAX_PAGE_BYTES {
            break;
        }
        bytes += size;
        page.push((decode_key(table, &key)?, value));
    }
    Ok(page)
}

type ScanBounds = (Bound<Vec<u8>>, Bound<Vec<u8>>);
fn scan_bounds(
    table: Table,
    fragment: PoolId,
    prefix: &str,
    after: Option<(&RecordKey, bool)>,
) -> Result<ScanBounds> {
    let mut encoded = vec![table.index() as u8];
    encoded.extend_from_slice(&RecordKey::prefix(fragment, prefix)?);
    let mut upper = encoded.clone();
    while upper.last() == Some(&255) {
        upper.pop();
    }
    let end = match upper.last_mut() {
        Some(last) => {
            *last += 1;
            Bound::Excluded(upper)
        }
        None => Bound::Unbounded,
    };
    let start = match after {
        Some((key, inclusive)) => {
            if key.fragment() != fragment || !key.identifier().starts_with(prefix) {
                return Err(Error::Invalid(
                    "metadata cursor is outside the requested prefix".into(),
                ));
            }
            if inclusive {
                Bound::Included(storage_key(table, key))
            } else {
                Bound::Excluded(storage_key(table, key))
            }
        }
        None => Bound::Included(encoded),
    };
    Ok((start, end))
}
