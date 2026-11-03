use std::fmt;

#[derive(Debug)]
pub enum Error {
    Invalid(String),
    Incompatible(String),
    Storage(procyon_engine::Error),
    Io(std::io::Error),
    Conflict,
    NotFound,
    NameConflict(String),
    Busy,
    Closed,
    Worker(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) | Self::Incompatible(message) | Self::Worker(message) => {
                f.write_str(message)
            }
            Self::Storage(error) => write!(f, "metadata storage: {error}"),
            Self::Io(error) => write!(f, "metadata I/O: {error}"),
            Self::Conflict => f.write_str("metadata transaction conflicted; retry the transaction"),
            Self::NotFound => f.write_str("metadata record was not found"),
            Self::NameConflict(message) => f.write_str(message),
            Self::Busy => f.write_str("metadata worker capacity is exhausted"),
            Self::Closed => f.write_str("metadata executor is shutting down"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<procyon_engine::Error> for Error {
    fn from(error: procyon_engine::Error) -> Self {
        Self::Storage(error)
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
