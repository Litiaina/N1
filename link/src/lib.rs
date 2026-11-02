//! LINK v1: canonical identities and stateless, fragment-scoped upload tickets.
//!
//! This crate has no filesystem, HTTP, engine configuration, clock, or object
//! catalogue dependency. Native and HTTP use these exact same calculations.
mod identity;
pub mod noa;
mod placement;
pub mod retirement;
pub mod shard;
mod ticket;

pub use identity::{Fingerprint, IDENTITY_BYTES, Identity, PoolId};
pub use placement::PoolLayout;
pub use shard::ShardFormat;
pub use ticket::{Durability, KeySigner, ObjectKey, UploadPlan, UploadTicket};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error(pub &'static str);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for Error {}
