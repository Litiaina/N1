//! LINK object engine. No namespace, NCX, NDX, export or legacy address resolver.
pub mod adaptive_concurrency;
pub mod auto_healing;
pub mod backup;
pub mod capacity_manager;
pub mod delete;
pub mod download;
pub mod ec;
pub mod header_parser;
pub mod instance_lease;
pub mod io_scheduler;
pub mod linux_io;
pub mod maintenance;
pub mod monitor;
pub mod object_parser;
pub mod operation_gate;
pub mod recovery;
pub mod repair_manager;
pub mod root_identity;
pub mod storage_admin;
pub mod storage_pool;
mod store;
pub mod system;
pub mod upload;
pub use download::{DownloadCompletion, DownloadStream, ObjectHealth, ObjectInfo};
pub use maintenance::{MaintenanceReport, ScrubOptions};
pub use repair_manager::RepairResult;
pub use store::{FinalizedObject, InitializedUpload, Store, UploadStatus};

#[cfg(test)]
pub(crate) mod tests;

use std::{fmt, io};
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Unauthorized,
    Forbidden,
    RateLimited,
    Retiring,
    Retired,
    Invalid(String),
    Conflict(String),
    NotFound,
    Unavailable(String),
    Corrupt(String),
    RetirementCorrupt,
    Io(io::Error),
}

impl Error {
    pub fn status(&self) -> u16 {
        match self {
            Self::Unauthorized => 401,
            Self::Forbidden => 403,
            Self::RateLimited => 429,
            Self::Retiring => 503,
            Self::Retired => 410,
            Self::Invalid(_) => 400,
            Self::Conflict(_) => 409,
            Self::NotFound => 404,
            Self::Unavailable(_) => 503,
            Self::Corrupt(_) | Self::RetirementCorrupt => 422,
            Self::Io(e) if matches!(e.raw_os_error(), Some(libc::ENOSPC | libc::EDQUOT)) => 507,
            Self::Io(_) => 500,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unauthorized => f.write_str("authentication required or invalid"),
            Self::Forbidden => f.write_str("permission denied"),
            Self::RateLimited => f.write_str("request rate limit exceeded"),
            Self::Retiring => f.write_str("object retirement is incomplete; retry deletion"),
            Self::Retired => f.write_str("object key is permanently retired"),
            Self::RetirementCorrupt => f.write_str("retirement state is corrupt; retry authenticated deletion before accessing this key"),
            Self::Invalid(s) | Self::Conflict(s) | Self::Unavailable(s) | Self::Corrupt(s) => {
                f.write_str(s)
            }
            Self::NotFound => f.write_str("object or upload not found"),
            Self::Io(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for Error {}
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<n1_link::Error> for Error {
    fn from(e: n1_link::Error) -> Self {
        Self::Invalid(e.to_string())
    }
}

impl From<crate::enums::app_error_enum::AppError> for Error {
    fn from(error: crate::enums::app_error_enum::AppError) -> Self {
        use crate::enums::app_error_enum::AppError;
        match error {
            AppError::Io(error, _) => Self::Io(error),
            AppError::ServiceUnavailable(message) | AppError::Timeout(message) => {
                Self::Unavailable(message)
            }
            AppError::Unauthorized => Self::Unauthorized,
            AppError::Forbidden(_) => Self::Forbidden,
            AppError::NotFound => Self::NotFound,
            AppError::Conflict(message) => Self::Conflict(message),
            AppError::InsufficientStorage => Self::Io(io::Error::from_raw_os_error(libc::ENOSPC)),
            other => Self::Invalid(other.to_string()),
        }
    }
}

pub(crate) fn corrupt(message: impl Into<String>) -> Error {
    Error::Corrupt(message.into())
}
