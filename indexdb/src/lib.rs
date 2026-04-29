#![deny(clippy::all, clippy::pedantic, clippy::disallowed_methods)]
#![allow(
    clippy::default_trait_access,
    clippy::if_not_else,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions,
    clippy::needless_pass_by_value,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::unnecessary_wraps,
    clippy::unreadable_literal
)]

//! Internal embedded index database for Litiaina N2.
//!
//! `indexdb` owns N1's `.ndxdb` metadata files. Its public API remains a typed
//! embedded table interface over the N1 metadata authority used by each
//! fragment.

pub use error::{
    CommitError, CompactionError, DatabaseError, Error, StorageError, TableError, TransactionError,
};
pub use simple::{
    AccessGuard, AccessGuardMut, AccessGuardMutInPlace, Builder, CacheStats, Database,
    DatabaseStats, Range, ReadOnlyDatabase, ReadOnlyTable, ReadOnlyUntypedTable, ReadableDatabase,
    ReadableTable, ReadableTableMetadata, RepairSession, StorageBackend, Table, TableDefinition,
    TableHandle, TableStats, UntypedTableHandle, WriteTransaction,
};
pub use types::{Key, MutInPlaceValue, TypeName, Value};

pub type Result<T = (), E = StorageError> = std::result::Result<T, E>;

mod error;
mod ffi;
pub mod n1;
mod simple;
mod types;

#[cfg(test)]
#[allow(dead_code)]
struct TempDbFile {
    path: std::path::PathBuf,
}

#[cfg(test)]
#[allow(dead_code)]
impl TempDbFile {
    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

#[cfg(test)]
impl Drop for TempDbFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_file(self.path.with_extension("ndxdb-wal"));
        let _ = std::fs::remove_file(self.path.with_extension("ndxdb-shm"));
    }
}

#[cfg(test)]
#[allow(dead_code)]
fn create_temp_db_file() -> TempDbFile {
    let mut path = std::env::temp_dir();
    let unique = format!(
        "n1-indexdb-test-{}-{}.ndxdb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    path.push(unique);
    TempDbFile { path }
}
