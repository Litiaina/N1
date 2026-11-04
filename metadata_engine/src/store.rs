use crate::{Error, RecordKey, Result, Table};
use n1_link::PoolId;
use procyon_engine::{
    KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, OptimisticWriteTx,
    PersistMode, Readable, Snapshot,
};
use serde::{Deserialize, Serialize};
use std::{ops::Bound, path::Path, sync::Arc};
mod backup;

const FORMAT_KEY: &[u8] = b"format";
const SYSTEM: &str = "n1_system";
const SCHEMA_VERSION: u32 = 1;
const MAX_VALUE_BYTES: usize = 1024 * 1024;
const MAX_TRANSACTION_BYTES: usize = 16 * 1024 * 1024;
const MAX_TRANSACTION_WRITES: usize = 4096;
const MAX_PAGE_SIZE: usize = 1000;
const MAX_PAGE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct Options {
    pub cache_bytes: u64,
    /// Active memtable rotation threshold for each keyspace, not a process RSS cap.
    pub memtable_bytes: u64,
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

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Format {
    product: String,
    schema: u32,
    key_encoding: String,
}

impl Default for Format {
    fn default() -> Self {
        Self {
            product: "N1-METADATA".into(),
            schema: SCHEMA_VERSION,
            key_encoding: "fragment16-utf8-v1".into(),
        }
    }
}

struct Inner {
    db: OptimisticTxDatabase,
    tables: [OptimisticTxKeyspace; 4],
}

/// Owns the database lock and background maintenance through cloned handles.
/// Only one process can open a metadata directory at a time.
#[derive(Clone)]
pub struct MetadataEngine(Arc<Inner>);

impl MetadataEngine {
    pub fn open(path: impl AsRef<Path>, options: Options) -> Result<Self> {
        if options.background_workers == 0
            || options.background_workers > 64
            || options.cache_bytes < 1024 * 1024
            || options.memtable_bytes < 1024 * 1024
            || options.journal_bytes < 64 * 1024 * 1024
        {
            return Err(Error::Invalid("invalid metadata resource limits".into()));
        }
        let path = path.as_ref();
        private_database_directory(path)?;
        // Never adopt a directory containing application data or another engine.
        // Procyon's own VERSION marker is necessary but not sufficient; the
        // keyspace and format checks below verify this is an N1 database.
        if path.exists()
            && !path.join("version").exists()
            && std::fs::read_dir(path)?.next().is_some()
        {
            return Err(Error::Incompatible(
                "metadata directory is not an N1 database or an empty directory".into(),
            ));
        }
        let db = OptimisticTxDatabase::builder(path)
            .cache_size(options.cache_bytes)
            .max_journaling_size(options.journal_bytes)
            .worker_threads(options.background_workers)
            .open()?;
        for name in db.list_keyspace_names() {
            if name.as_ref() != SYSTEM
                && !Table::ALL.iter().any(|table| name.as_ref() == table.name())
            {
                return Err(Error::Incompatible(
                    "foreign metadata keyspace; refusing adoption".into(),
                ));
            }
        }
        let system = db.keyspace(SYSTEM, KeyspaceCreateOptions::default)?;
        if let Some(bytes) = system.get(FORMAT_KEY)? {
            let format: Format = serde_json::from_slice(&bytes).map_err(|error| {
                Error::Incompatible(format!("invalid metadata format: {error}"))
            })?;
            if format != Format::default() {
                return Err(Error::Incompatible(
                    "unsupported metadata storage format".into(),
                ));
            }
            if Table::ALL
                .iter()
                .any(|table| !db.keyspace_exists(table.name()))
            {
                return Err(Error::Incompatible(
                    "a metadata keyspace is missing from an initialized database".into(),
                ));
            }
        }
        let table_options =
            || KeyspaceCreateOptions::default().max_memtable_size(options.memtable_bytes);
        let tables = [
            db.keyspace(Table::Entries.name(), table_options)?,
            db.keyspace(Table::Versions.name(), table_options)?,
            db.keyspace(Table::Uploads.name(), table_options)?,
            db.keyspace(Table::Reclamation.name(), table_options)?,
        ];
        let mut tx = db.write_tx()?.durability(Some(PersistMode::SyncAll));
        if let Some(bytes) = tx.get(&system, FORMAT_KEY)? {
            let format: Format = serde_json::from_slice(&bytes).map_err(|error| {
                Error::Incompatible(format!("invalid metadata format: {error}"))
            })?;
            if format != Format::default() {
                return Err(Error::Incompatible(
                    "unsupported metadata storage format".into(),
                ));
            }
        } else {
            // A crash during initial creation may leave empty keyspaces. Finish
            // initialization only if there is no data to reinterpret.
            if tx.first_key_value(&system).is_some()
                || tables
                    .iter()
                    .any(|table| tx.first_key_value(table).is_some())
            {
                return Err(Error::Incompatible(
                    "metadata format record is missing from a populated database".into(),
                ));
            }
            let bytes = serde_json::to_vec(&Format::default())
                .map_err(|error| Error::Invalid(error.to_string()))?;
            tx.insert(&system, FORMAT_KEY, bytes);
        }
        tx.commit()?.map_err(|_| Error::Conflict)?;
        Ok(Self(Arc::new(Inner { db, tables })))
    }

    pub fn snapshot(&self) -> MetadataSnapshot {
        MetadataSnapshot {
            engine: self.clone(),
            snapshot: self.0.db.read_tx(),
        }
    }

    pub fn write(&self) -> Result<MetadataTransaction> {
        Ok(MetadataTransaction {
            engine: self.clone(),
            tx: self.0.db.write_tx()?.durability(Some(PersistMode::SyncAll)),
            writes: 0,
            bytes: 0,
            failed: false,
        })
    }

    /// Retries the complete read/modify/write operation after serialization
    /// conflicts. The closure must have no external side effects or object I/O.
    /// Storage failures are returned immediately, never retried as conflicts.
    pub fn transact<T>(
        &self,
        attempts: usize,
        mut apply: impl FnMut(&mut MetadataTransaction) -> Result<T>,
    ) -> Result<T> {
        if attempts == 0 || attempts > 1024 {
            return Err(Error::Invalid("transaction attempts must be 1–1024".into()));
        }
        for _ in 0..attempts {
            let mut tx = self.write()?;
            let result = apply(&mut tx)?;
            match tx.commit() {
                Ok(()) => return Ok(result),
                Err(Error::Conflict) => std::thread::yield_now(),
                Err(error) => return Err(error),
            }
        }
        Err(Error::Conflict)
    }

    pub fn disk_bytes(&self) -> Result<u64> {
        Ok(self.0.db.disk_space()?)
    }

    fn table(&self, table: Table) -> &OptimisticTxKeyspace {
        &self.0.tables[table.index()]
    }
}

/// Metadata now retains LINK access credentials. Protect the database boundary
/// before Procyon creates journals or background files, regardless of umask.
/// Existing directories are never silently chmod'd or adopted through symlinks.
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

/// A stable cross-table view. Hold one snapshot for both index and entry reads.
/// Drop it after a request/page to allow old versions to be reclaimed.
pub struct MetadataSnapshot {
    engine: MetadataEngine,
    snapshot: Snapshot,
}

impl MetadataSnapshot {
    pub(crate) fn scan_from(
        &self,
        table: Table,
        fragment: PoolId,
        prefix: &str,
        from: &RecordKey,
        limit: usize,
    ) -> Result<Vec<(RecordKey, Vec<u8>)>> {
        scan(
            &self.snapshot,
            &self.engine,
            table,
            fragment,
            prefix,
            Some(ScanStart::At(from)),
            limit,
        )
    }
    pub fn get(&self, table: Table, key: &RecordKey) -> Result<Option<Vec<u8>>> {
        read(&self.snapshot, &self.engine, table, key)
    }

    /// Keyset pagination seeks directly after the last key, within this snapshot.
    /// A new snapshot for a later page is a new view, not a global listing snapshot.
    pub fn scan(
        &self,
        table: Table,
        fragment: PoolId,
        prefix: &str,
        after: Option<&RecordKey>,
        limit: usize,
    ) -> Result<Vec<(RecordKey, Vec<u8>)>> {
        scan(
            &self.snapshot,
            &self.engine,
            table,
            fragment,
            prefix,
            after.map(ScanStart::After),
            limit,
        )
    }
}

/// Concurrent optimistic transaction. Reads, including ranges, participate in
/// conflict validation. A successful write commit waits for SyncAll.
pub struct MetadataTransaction {
    engine: MetadataEngine,
    tx: OptimisticWriteTx,
    writes: usize,
    bytes: usize,
    failed: bool,
}

impl MetadataTransaction {
    pub fn get(&self, table: Table, key: &RecordKey) -> Result<Option<Vec<u8>>> {
        read(&self.tx, &self.engine, table, key)
    }

    pub fn scan(
        &self,
        table: Table,
        fragment: PoolId,
        prefix: &str,
        after: Option<&RecordKey>,
        limit: usize,
    ) -> Result<Vec<(RecordKey, Vec<u8>)>> {
        scan(
            &self.tx,
            &self.engine,
            table,
            fragment,
            prefix,
            after.map(ScanStart::After),
            limit,
        )
    }

    pub fn put(&mut self, table: Table, key: &RecordKey, value: &[u8]) -> Result<()> {
        self.reserve(key, value.len())?;
        self.tx
            .insert(self.engine.table(table), key.encode(), value);
        Ok(())
    }

    pub fn remove(&mut self, table: Table, key: &RecordKey) -> Result<()> {
        self.reserve(key, 0)?;
        self.tx.remove(self.engine.table(table), key.encode());
        Ok(())
    }

    /// A conditional update and its predicate belong to the same transaction.
    /// False leaves this key unchanged. Other changes in the transaction remain.
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
        self.tx.commit()?.map_err(|_| Error::Conflict)
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

fn read(
    view: &impl Readable,
    engine: &MetadataEngine,
    table: Table,
    key: &RecordKey,
) -> Result<Option<Vec<u8>>> {
    Ok(view
        .get(engine.table(table), key.encode())?
        .map(|value| value.to_vec()))
}

enum ScanStart<'a> {
    After(&'a RecordKey),
    At(&'a RecordKey),
}
fn scan(
    view: &impl Readable,
    engine: &MetadataEngine,
    table: Table,
    fragment: PoolId,
    prefix: &str,
    after: Option<ScanStart<'_>>,
    limit: usize,
) -> Result<Vec<(RecordKey, Vec<u8>)>> {
    if limit == 0 || limit > MAX_PAGE_SIZE {
        return Err(Error::Invalid("metadata page size must be 1–1000".into()));
    }
    let encoded = RecordKey::prefix(fragment, prefix)?;
    let (start, end) = procyon_engine::util::prefix_to_range(&encoded);
    let start = if let Some(after) = after {
        let (after, inclusive) = match after {
            ScanStart::After(key) => (key, false),
            ScanStart::At(key) => (key, true),
        };
        if after.fragment() != fragment || !after.identifier().starts_with(prefix) {
            return Err(Error::Invalid(
                "metadata cursor is outside the requested prefix".into(),
            ));
        }
        let key = procyon_engine::Slice::from(after.encode());
        if inclusive {
            Bound::Included(key)
        } else {
            Bound::Excluded(key)
        }
    } else {
        start
    };
    let mut page = Vec::with_capacity(limit);
    let mut page_bytes = 0;
    for item in view.range(engine.table(table), (start, end)).take(limit) {
        let (key, value) = item.into_inner()?;
        let item_bytes = key.len() + value.len();
        if page_bytes + item_bytes > MAX_PAGE_BYTES {
            break;
        }
        page_bytes += item_bytes;
        page.push((RecordKey::decode(&key)?, value.to_vec()));
    }
    Ok(page)
}
