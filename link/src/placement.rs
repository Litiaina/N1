use crate::{Error, Fingerprint, Identity, PoolId, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// All fields are frozen when the pool is enrolled. Exactly K+P member slots.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolLayout {
    pub id: PoolId,
    pub generation: u64,
    pub data_shards: u16,
    pub parity_shards: u16,
    pub block_bytes: u32,
    pub placement_version: u16,
    /// Absent only in existing SHA-256 pools; fixed when a pool is enrolled.
    #[serde(default)]
    pub shard_format: crate::shard::ShardFormat,
}

impl PoolLayout {
    pub fn validate(&self) -> Result<()> {
        if self.data_shards == 0
            || u32::from(self.data_shards) + u32::from(self.parity_shards) > 256
        {
            return Err(Error(
                "LINK v1 requires 1..=256 total shards and at least one data shard",
            ));
        }
        if !(4096..=1024 * 1024).contains(&self.block_bytes)
            || !self.block_bytes.is_multiple_of(4096)
        {
            return Err(Error(
                "block size must be a multiple of 4096 in 4096..=1048576",
            ));
        }
        if self.placement_version != 1 {
            return Err(Error("unsupported LINK placement version"));
        }
        Ok(())
    }

    pub fn width(&self) -> usize {
        usize::from(self.data_shards) + usize::from(self.parity_shards)
    }
    pub fn stripe_bytes(&self) -> u64 {
        u64::from(self.data_shards) * u64::from(self.block_bytes)
    }

    pub fn accepts(&self, identity: Identity) -> bool {
        self.id == identity.pool && self.generation == identity.generation
    }

    /// Full SHA-256 interpreted as an unsigned big-endian integer modulo N.
    /// Folding bytes specifies the calculation identically on every CPU width.
    pub fn rotation(&self, fingerprint: Fingerprint) -> Result<usize> {
        self.validate()?;
        let mut hash = Sha256::new();
        hash.update(b"Litiaina/LINK/placement/v1\0");
        hash.update(fingerprint.0);
        Ok(hash.finalize().iter().fold(0usize, |rem, byte| {
            (rem * 256 + usize::from(*byte)) % self.width()
        }))
    }

    pub fn member_slot(&self, fingerprint: Fingerprint, shard: u16) -> Result<usize> {
        if usize::from(shard) >= self.width() {
            return Err(Error("shard index outside pool geometry"));
        }
        Ok((self.rotation(fingerprint)? + usize::from(shard)) % self.width())
    }

    pub fn shard_at_slot(&self, fingerprint: Fingerprint, slot: usize) -> Result<u16> {
        if slot >= self.width() {
            return Err(Error("member slot outside pool geometry"));
        }
        Ok(((slot + self.width() - self.rotation(fingerprint)?) % self.width()) as u16)
    }
}
