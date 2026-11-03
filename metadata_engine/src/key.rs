use crate::{Error, Result};
use n1_link::PoolId;
use serde::{Deserialize, Serialize};

/// Internal tables share one journal and one transaction/snapshot boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Table {
    Entries,
    Versions,
    Uploads,
    Reclamation,
}

impl Table {
    pub(crate) const ALL: [Self; 4] = [
        Self::Entries,
        Self::Versions,
        Self::Uploads,
        Self::Reclamation,
    ];

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Entries => "n1_entries",
            Self::Versions => "n1_versions",
            Self::Uploads => "n1_uploads",
            Self::Reclamation => "n1_reclamation",
        }
    }

    pub(crate) fn index(self) -> usize {
        match self {
            Self::Entries => 0,
            Self::Versions => 1,
            Self::Uploads => 2,
            Self::Reclamation => 3,
        }
    }
}

/// A stable fragment ID followed by an exact UTF-8 identifier. No trimming,
/// case folding, slash rewriting or platform path normalization is performed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "WireKey", into = "WireKey")]
pub struct RecordKey {
    fragment: PoolId,
    identifier: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireKey {
    fragment: PoolId,
    identifier: String,
}

impl TryFrom<WireKey> for RecordKey {
    type Error = Error;

    fn try_from(key: WireKey) -> Result<Self> {
        Self::new(key.fragment, key.identifier)
    }
}

impl From<RecordKey> for WireKey {
    fn from(key: RecordKey) -> Self {
        Self {
            fragment: key.fragment,
            identifier: key.identifier,
        }
    }
}

impl RecordKey {
    pub fn new(fragment: PoolId, identifier: impl Into<String>) -> Result<Self> {
        let identifier = identifier.into();
        Self::validate_identifier(&identifier, false)?;
        Ok(Self {
            fragment,
            identifier,
        })
    }

    pub fn fragment(&self) -> PoolId {
        self.fragment
    }

    pub fn identifier(&self) -> &str {
        &self.identifier
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        Self::prefix(self.fragment, &self.identifier).expect("validated record key")
    }

    pub(crate) fn prefix(fragment: PoolId, prefix: &str) -> Result<Vec<u8>> {
        Self::validate_identifier(prefix, true)?;
        let mut key = Vec::with_capacity(16 + prefix.len());
        key.extend_from_slice(&fragment.0);
        key.extend_from_slice(prefix.as_bytes());
        Ok(key)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() <= 16 {
            return Err(Error::Incompatible("invalid stored metadata key".into()));
        }
        let fragment = PoolId(bytes[..16].try_into().expect("checked length"));
        let identifier = std::str::from_utf8(&bytes[16..])
            .map_err(|_| Error::Incompatible("invalid UTF-8 metadata key".into()))?;
        Self::new(fragment, identifier)
    }

    fn validate_identifier(identifier: &str, allow_empty: bool) -> Result<()> {
        if (!allow_empty && identifier.is_empty())
            || identifier.len() > 4096
            || identifier.contains('\0')
        {
            return Err(Error::Invalid(
                "metadata identifiers must contain 1–4096 UTF-8 bytes without NUL".into(),
            ));
        }
        Ok(())
    }
}
