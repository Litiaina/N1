use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fmt, str::FromStr};

pub const IDENTITY_BYTES: usize = 57;
const DOMAIN: &[u8] = b"Litiaina/LINK/identity/v1\0";

/// Pool IDs are opaque 16-byte values, never display names or mount paths.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct PoolId(pub [u8; 16]);

impl fmt::Display for PoolId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl FromStr for PoolId {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self> {
        if value.len() != 32
            || value
                .bytes()
                .any(|b| !b.is_ascii_digit() && !(b'a'..=b'f').contains(&b))
        {
            return Err(Error("pool ID must be 32 lowercase hexadecimal characters"));
        }
        let mut bytes = [0; 16];
        hex::decode_to_slice(value, &mut bytes).map_err(|_| Error("invalid pool ID"))?;
        Ok(Self(bytes))
    }
}

impl TryFrom<String> for PoolId {
    type Error = Error;
    fn try_from(value: String) -> Result<Self> {
        value.parse()
    }
}

impl From<PoolId> for String {
    fn from(value: PoolId) -> String {
        value.to_string()
    }
}

/// The complete immutable object identity. Tickets and final keys contain
/// exactly these same bytes. Authentication and upload parameters are excluded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Identity {
    pub pool: PoolId,
    pub generation: u64,
    pub token: [u8; 32],
}

impl Identity {
    pub fn encode(self) -> [u8; IDENTITY_BYTES] {
        let mut bytes = [0; IDENTITY_BYTES];
        bytes[0] = 1;
        bytes[1..17].copy_from_slice(&self.pool.0);
        bytes[17..25].copy_from_slice(&self.generation.to_be_bytes());
        bytes[25..57].copy_from_slice(&self.token);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != IDENTITY_BYTES || bytes[0] != 1 {
            return Err(Error("unsupported or malformed LINK identity"));
        }
        Ok(Self {
            pool: PoolId(bytes[1..17].try_into().expect("fixed pool ID")),
            generation: u64::from_be_bytes(bytes[17..25].try_into().expect("fixed generation")),
            token: bytes[25..57].try_into().expect("fixed random token"),
        })
    }

    pub fn fingerprint(self) -> Fingerprint {
        let mut hash = Sha256::new();
        hash.update(DOMAIN);
        hash.update(self.encode());
        Fingerprint(hash.finalize().into())
    }
}

/// A derived physical filename, not a saved LINK key or reversible key index.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Fingerprint(pub [u8; 32]);

impl Fingerprint {
    pub fn fanout(self) -> (String, String) {
        (format!("{:02x}", self.0[0]), format!("{:02x}", self.0[1]))
    }

    pub fn shard_path(self, shard: u16) -> String {
        let (a, b) = self.fanout();
        format!("{a}/{b}/{self}.s{shard}.shard")
    }

    pub fn staging_path(self) -> String {
        format!(".uploads/{self}/shard.tmp")
    }
}

impl fmt::Display for Fingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl FromStr for Fingerprint {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self> {
        if value.len() != 64
            || value
                .bytes()
                .any(|b| !b.is_ascii_digit() && !(b'a'..=b'f').contains(&b))
        {
            return Err(Error(
                "fingerprint must be 64 lowercase hexadecimal characters",
            ));
        }
        let mut bytes = [0; 32];
        hex::decode_to_slice(value, &mut bytes).map_err(|_| Error("invalid fingerprint"))?;
        Ok(Self(bytes))
    }
}
