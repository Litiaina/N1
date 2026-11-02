//! Permanent terminal state at the calculated shard filename. Normal uploads
//! write no deletion record or extra index. GC never expires these records.
use crate::{Error, Fingerprint, Result};
use sha2::{Digest, Sha256};
pub const BYTES: usize = 96;
pub const MAGIC: &[u8; 8] = b"N1LDEL01";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Retiring,
    Retired,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Retirement {
    pub phase: Phase,
    pub shard: u16,
    pub nonce: [u8; 16],
    pub timestamp: u64,
}
impl Retirement {
    pub fn encode(self, fp: Fingerprint) -> [u8; BYTES] {
        let mut bytes = [0; BYTES];
        bytes[..8].copy_from_slice(MAGIC);
        bytes[8] = 1;
        bytes[9] = match self.phase {
            Phase::Retiring => 1,
            Phase::Retired => 2,
        };
        bytes[10..12].copy_from_slice(&self.shard.to_be_bytes());
        bytes[16..32].copy_from_slice(&self.nonce);
        bytes[32..40].copy_from_slice(&self.timestamp.to_be_bytes());
        let hash = digest(fp, &bytes[..64]);
        bytes[64..96].copy_from_slice(&hash);
        bytes
    }
    pub fn decode(bytes: &[u8], fp: Fingerprint, shard: u16) -> Result<Self> {
        if bytes.len() != BYTES
            || &bytes[..8] != MAGIC
            || bytes[8] != 1
            || !matches!(bytes[9], 1 | 2)
            || u16::from_be_bytes(bytes[10..12].try_into().expect("fixed shard")) != shard
            || bytes[12..16] != [0; 4]
            || bytes[40..64] != [0; 24]
            || digest(fp, &bytes[..64]) != bytes[64..96]
        {
            return Err(Error("invalid retirement record"));
        }
        Ok(Self {
            phase: if bytes[9] == 1 {
                Phase::Retiring
            } else {
                Phase::Retired
            },
            shard,
            nonce: bytes[16..32].try_into().expect("fixed nonce"),
            timestamp: u64::from_be_bytes(bytes[32..40].try_into().expect("fixed timestamp")),
        })
    }
}
fn digest(fp: Fingerprint, bytes: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"Litiaina/LINK/retirement/v1\0");
    h.update(fp.0);
    h.update(bytes);
    h.finalize().into()
}
