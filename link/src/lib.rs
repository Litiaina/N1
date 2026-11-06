//! LINK (Litiaina Immutable Native Key), N1's key algorithm and storage architecture.
//!
//! Versioned canonical identities, authenticated object keys, deterministic shard
//! placement and immutable upload geometry are shared by N1 and its clients.
//!
//! This crate has no filesystem, HTTP, engine configuration, clock, or object
//! catalogue dependency. Native and HTTP use these exact same calculations.
mod identity;
mod key;
pub mod noa;
mod placement;
pub mod shard;

pub use identity::{Fingerprint, IDENTITY_BYTES, Identity, PoolId};
pub use key::{Durability, KeySigner, ObjectKey, UploadPlan};
pub use placement::PoolLayout;
pub use shard::ShardFormat;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error(pub &'static str);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for Error {}
