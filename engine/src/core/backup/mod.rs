//! Encrypted incremental snapshots and one-way snapshot replication.
//! Online capture pins a committed view inside the engine; offline capture also
//! preserves durable-upload progress. Neither introduces mutable version heads.
pub mod automation;
pub(crate) mod barrier;
pub mod capture;
pub mod cli;
mod crypto;
pub(crate) mod format;
mod fs;
mod metadata;
pub mod network;
pub mod online;
pub mod repository;
pub mod restore;
mod restore_guard;
pub mod retention;
pub mod transfer;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
pub(crate) fn invalid(message: impl Into<String>) -> Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into()).into()
}

#[cfg(test)]
mod tests;
