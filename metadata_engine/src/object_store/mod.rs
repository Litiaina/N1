//! The metadata service's native NOA client boundary.
//!
//! This module never opens member roots, calculates shard paths, edits pool
//! markers, or asks the object engine to understand a logical name. LINK remains
//! independently usable through precisely the same public object API.
mod native;

use n1_native_client::NativeClientError;
use serde::{Deserialize, Serialize};
use std::fmt;

pub use native::{NativeObjectStore, ObjectStoreConfig};

/// Private upload state owned by the metadata service. These credentials must
/// not be included in public namespace or version responses, logs, or Debug.
#[derive(Clone, Serialize, Deserialize)]
pub struct ObjectUpload {
    pub upload_key: String,
    pub object_key: String,
    pub total_bytes: u64,
    pub part_bytes: u64,
    pub parts: u64,
    pub expires_at: u64,
    pub durable: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ObjectStat {
    pub total_bytes: u64,
    pub data_shards: u16,
    pub parity_shards: u16,
    pub finalized_shards: usize,
    pub integrity_algorithm: String,
    pub integrity_root: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ObjectUploadStatus {
    pub finalized: bool,
    pub finalized_shards: usize,
    pub parts: u64,
    pub checked_start: u64,
    pub checked_end: u64,
    pub missing_parts: Vec<u64>,
    pub unavailable_members: Vec<usize>,
}

#[derive(Clone, Deserialize)]
pub struct ObjectFinalization {
    pub object_key: String,
    pub already_finalized: bool,
    pub total_bytes: u64,
}

pub enum ObjectStoreError {
    Invalid(&'static str),
    Contract(&'static str),
    Closed,
    Native(NativeClientError),
    Response(serde_json::Error),
}

impl fmt::Debug for ObjectStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectStoreError")
            .field("status", &self.status())
            .field("message", &self.to_string())
            .finish()
    }
}

impl ObjectStoreError {
    /// Preserve the object's specific status. A failed operation is not a
    /// declaration that the engine or an entire storage pool has failed.
    pub fn status(&self) -> u16 {
        match self {
            Self::Invalid(_) => 400,
            Self::Contract(_) => 502,
            Self::Closed => 503,
            Self::Native(NativeClientError::Native { code, .. }) => code
                .parse()
                .ok()
                .filter(|code| (400..=599).contains(code))
                .unwrap_or(502),
            Self::Native(_) | Self::Response(_) => 502,
        }
    }
}

impl fmt::Display for ObjectStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Backend errors can contain self-describing access keys. The public
        // adapter returns a safe description; the source remains inspectable.
        match self {
            Self::Invalid(message) | Self::Contract(message) => f.write_str(message),
            Self::Closed => f.write_str("object client is shutting down"),
            Self::Native(_) => write!(f, "object operation returned status {}", self.status()),
            Self::Response(_) => f.write_str("invalid response from object service"),
        }
    }
}

impl std::error::Error for ObjectStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Native(error) => Some(error),
            Self::Response(error) => Some(error),
            _ => None,
        }
    }
}

impl From<NativeClientError> for ObjectStoreError {
    fn from(error: NativeClientError) -> Self {
        Self::Native(error)
    }
}

pub type ObjectStoreResult<T> = std::result::Result<T, ObjectStoreError>;
