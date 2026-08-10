use std::fmt;
use std::io;

#[derive(Debug)]
pub enum NativeClientError {
    Io(io::Error),
    Encode(rmp_serde::encode::Error),
    Decode(rmp_serde::decode::Error),
    Json(serde_json::Error),
    Protocol(String),
    Native {
        status: Option<u16>,
        code: String,
        message: String,
    },
}

pub type NativeClientResult<T> = Result<T, NativeClientError>;

impl NativeClientError {
    pub fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol(message.into())
    }
}

impl fmt::Display for NativeClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::Encode(err) => write!(f, "{err}"),
            Self::Decode(err) => write!(f, "{err}"),
            Self::Json(err) => write!(f, "{err}"),
            Self::Protocol(message) => write!(f, "{message}"),
            Self::Native {
                status: _,
                code,
                message,
            } => write!(f, "{code}: {message}"),
        }
    }
}

impl std::error::Error for NativeClientError {}

impl From<io::Error> for NativeClientError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<rmp_serde::encode::Error> for NativeClientError {
    fn from(value: rmp_serde::encode::Error) -> Self {
        Self::Encode(value)
    }
}

impl From<rmp_serde::decode::Error> for NativeClientError {
    fn from(value: rmp_serde::decode::Error) -> Self {
        Self::Decode(value)
    }
}

impl From<serde_json::Error> for NativeClientError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}
