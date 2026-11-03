//! N1's optional metadata component, using the ProcyonDB transactional core.
//!
//! The object engine does not depend on this crate. These are internal storage
//! transactions; public NOA request semantics belong to the service adapters.
mod error;
mod executor;
pub mod exports;
pub mod fragments;
mod key;
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
