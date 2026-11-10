//! M1's independently implemented, single-node metadata database.
//!
//! Ordered records live in a copy-on-write tree on disk. Readers retain an
//! immutable root; concurrent optimistic transactions validate point and range
//! reads before an ordered, group-synchronized durable publication. Checkpoints
//! rebuild live records in the background and retain old files for old readers.
mod checkpoint;
mod database;
mod error;
#[cfg(test)]
mod fault;
mod format;
mod options;
mod transaction;
mod tree;
mod worker;

pub use database::Database;
pub(crate) use database::{Core, Owner, State};
pub use error::{Error, Result};
pub use options::{
    FORMAT_VERSION, MAX_KEY_BYTES, MAX_TRANSACTION_BYTES, MAX_TRANSACTION_WRITES, MAX_VALUE_BYTES,
    Options,
};
pub use transaction::{Scan, Snapshot, Transaction};
pub use tree::Integrity;
