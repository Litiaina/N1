//! LINK recovery points. Repository records live outside member object storage;
//! they preserve exact shard bytes and terminal state, not client key catalogs.
pub mod automation;
pub mod capture;
pub mod cli;
pub mod control;
mod crypto;
pub mod format;
pub(crate) mod fs;
pub mod jobs;
pub mod network;
pub mod online;
pub mod repository;
pub mod restore;
pub mod retention;
mod retention_guard;
pub mod transfer;
pub use crypto::create_key;
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
pub(crate) fn invalid(message: impl Into<String>) -> Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into()).into()
}

#[cfg(test)]
mod tests;
