use std::{fmt, io};

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Locked,
    Invalid(String),
    Incompatible(String),
    Corrupt(String),
    Conflict,
    Closed,
    Poisoned,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::Locked => f.write_str("M1 database is already open"),
            Self::Invalid(message) | Self::Incompatible(message) | Self::Corrupt(message) => {
                f.write_str(message)
            }
            Self::Conflict => f.write_str("metadata transaction conflict"),
            Self::Closed => f.write_str("M1 storage coordinator is closed"),
            Self::Poisoned => f.write_str("M1 storage requires reopen after a storage failure"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
