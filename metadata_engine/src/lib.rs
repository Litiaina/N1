//! M1, Litiaina's metadata store, with its own transactional storage core.
//!
//! M1 owns fragments, names, versions, permissions and named-object lifecycle.
//! It accesses the independent N1 object store through the public native NOA API.
//! N1 does not depend on M1. Storage transactions live in `store`;
//! public NOA request semantics belong to the service adapters in `server`.
mod error;
mod executor;
pub mod exports;
pub mod fragments;
mod key;
pub mod maintenance;
pub mod middleware;
pub mod namespace;
pub mod object_store;
pub mod posix;
pub mod server;
pub mod service;
mod store;
pub mod uploads;

pub use error::{Error, Result};
pub use executor::MetadataExecutor;
pub use key::{RecordKey, Table};
pub use store::{MetadataEngine, MetadataSnapshot, MetadataTransaction, Options};
