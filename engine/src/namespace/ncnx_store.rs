use crate::core::checksum::{Sha256Digest, format_sha256, sha256_digest};
use crate::core::fragment_identity::physical_fragment_root_on;
use crate::enums::app_error_enum::AppError;
use crate::namespace::paths::{base_name, normalize_path, parent_path};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use ulid::Ulid;

mod listing;
mod store;
mod support;

pub use listing::*;
pub use support::validate_logical_key;
use support::*;

pub const NCNX_FORMAT_VERSION: u32 = 4;
pub const NAMESPACE_DIR: &str = ".namespace";
pub const NAMESPACE_TRASH_DIR: &str = ".trash";
pub const NCNX_SUFFIX: &str = ".ncnx";
const MAX_KEY_BYTES: usize = 4_096;
const MAX_COMPONENT_BYTES: usize = 250;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NcnxRecord {
    pub format_version: u32,
    pub fragment_id: String,
    pub object_id: String,
    pub mutation_id: String,
    pub created_at_unix_nanos: i64,
    pub payload_sha256: Sha256Digest,
}

impl NcnxRecord {
    pub fn new(
        fragment_id: impl Into<String>,
        object_id: impl Into<String>,
        mutation_id: impl Into<String>,
        created_at_unix_nanos: i64,
    ) -> Result<Self, AppError> {
        let mut record = Self {
            format_version: NCNX_FORMAT_VERSION,
            fragment_id: fragment_id.into(),
            object_id: object_id.into(),
            mutation_id: mutation_id.into(),
            created_at_unix_nanos,
            payload_sha256: [0; 32],
        };
        record.validate_fields()?;
        record.payload_sha256 = record.expected_sha256()?;
        Ok(record)
    }

    fn validate_fields(&self) -> Result<(), AppError> {
        if self.format_version != NCNX_FORMAT_VERSION {
            return Err(AppError::Conflict(format!(
                "NCNX format {} is not N2 3.4 format {}",
                self.format_version, NCNX_FORMAT_VERSION
            )));
        }
        if self.fragment_id.trim().is_empty()
            || self.object_id.trim().is_empty()
            || self.mutation_id.trim().is_empty()
            || self.created_at_unix_nanos <= 0
        {
            return Err(AppError::Conflict(
                "NCNX requires fragment, object, mutation, and creation identities".into(),
            ));
        }
        Ok(())
    }

    fn expected_sha256(&self) -> Result<Sha256Digest, AppError> {
        let mut unsigned = self.clone();
        unsigned.payload_sha256 = [0; 32];
        let payload = rmp_serde::to_vec_named(&unsigned).map_err(|err| {
            AppError::InternalServerError(format!("encode NCNX integrity payload: {err}"))
        })?;
        Ok(sha256_digest(&payload))
    }

    pub fn validate(&self) -> Result<(), AppError> {
        self.validate_fields()?;
        let expected = self.expected_sha256()?;
        if expected != self.payload_sha256 {
            return Err(AppError::Conflict(format!(
                "NCNX integrity mismatch: expected {}, recorded {}",
                format_sha256(&expected),
                format_sha256(&self.payload_sha256)
            )));
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, AppError> {
        self.validate()?;
        rmp_serde::to_vec_named(self)
            .map_err(|err| AppError::InternalServerError(format!("encode NCNX: {err}")))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, AppError> {
        let record: Self = rmp_serde::from_slice(bytes)
            .map_err(|err| AppError::Conflict(format!("decode NCNX: {err}")))?;
        record.validate()?;
        Ok(record)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TrashLocator {
    pub logical_key: String,
    pub deleted_at_unix_nanos: i64,
    pub delete_mutation_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NcnxListItem {
    pub logical_key: String,
    pub physical_path: PathBuf,
    pub trash: Option<TrashLocator>,
}

#[derive(Clone, Debug)]
pub struct NcnxStore {
    root_id: String,
    storage_root: PathBuf,
}

#[cfg(test)]
#[path = "ncnx_store/tests.rs"]
mod tests;
