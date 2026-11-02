//! Self-contained shard bytes. No object key, path list, sidecar receipt, or
//! external publication record is stored. Every checksum binds its filename's
//! fingerprint, so copying a valid frame to a different object is detectable.
use crate::{Durability, Error, Fingerprint, PoolLayout, Result, UploadPlan};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const HEADER_BYTES: usize = 128;
pub const FRAME_HEADER_BYTES: usize = 96;
pub const SEAL_BYTES: usize = 96;
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardFormat {
    /// The missing field in an existing pool means exactly its original format.
    #[default]
    #[serde(rename = "sha256-v1")]
    Sha256V1,
    #[serde(rename = "crc32c-sha256-v2")]
    Crc32cSha256V2,
}

impl ShardFormat {
    pub const fn version(self) -> u16 {
        match self {
            Self::Sha256V1 => 1,
            Self::Crc32cSha256V2 => 2,
        }
    }

    pub const fn integrity_algorithm(self) -> &'static str {
        match self {
            Self::Sha256V1 => "LINK-SHA256-tree-v1",
            Self::Crc32cSha256V2 => "LINK-SHA256-tree-v2",
        }
    }

    fn choose<T>(self, v1: T, v2: T) -> T {
        match self {
            Self::Sha256V1 => v1,
            Self::Crc32cSha256V2 => v2,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardHeader {
    pub format: ShardFormat,
    pub shard: u16,
    pub data_shards: u16,
    pub parity_shards: u16,
    pub block_bytes: u32,
    pub total_bytes: u64,
    pub part_bytes: u64,
    pub expires_at: u64,
    pub durability: Durability,
}

impl ShardHeader {
    pub fn new(pool: &PoolLayout, plan: UploadPlan, shard: u16) -> Result<Self> {
        plan.validate(pool)?;
        if usize::from(shard) >= pool.width() {
            return Err(Error("shard outside pool geometry"));
        }
        let header = Self {
            format: pool.shard_format,
            shard,
            data_shards: pool.data_shards,
            parity_shards: pool.parity_shards,
            block_bytes: pool.block_bytes,
            total_bytes: plan.total_bytes,
            part_bytes: plan.part_bytes,
            expires_at: plan.expires_at,
            durability: plan.durability,
        };
        header.file_bytes()?;
        Ok(header)
    }

    pub fn stripe_bytes(&self) -> u64 {
        u64::from(self.data_shards) * u64::from(self.block_bytes)
    }
    pub fn stripes(&self) -> u64 {
        self.total_bytes.div_ceil(self.stripe_bytes())
    }
    pub fn parts(&self) -> u64 {
        self.total_bytes.div_ceil(self.part_bytes)
    }
    pub fn record_bytes(&self) -> u64 {
        FRAME_HEADER_BYTES as u64 + u64::from(self.block_bytes)
    }

    pub fn frame_offset(&self, stripe: u64) -> Result<u64> {
        if stripe >= self.stripes() {
            return Err(Error("stripe outside object"));
        }
        Ok(HEADER_BYTES as u64 + stripe * self.record_bytes())
    }

    pub fn seal_offset(&self) -> u64 {
        HEADER_BYTES as u64 + self.stripes() * self.record_bytes()
    }

    pub fn file_bytes(&self) -> Result<u64> {
        self.stripes()
            .checked_mul(self.record_bytes())
            .and_then(|n| n.checked_add((HEADER_BYTES + SEAL_BYTES) as u64))
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or(Error("encoded shard exceeds file offset limits"))
    }

    pub fn source_bytes_in_stripe(&self, stripe: u64) -> Result<u32> {
        if stripe >= self.stripes() {
            return Err(Error("stripe outside object"));
        }
        Ok((self.total_bytes - stripe * self.stripe_bytes()).min(self.stripe_bytes()) as u32)
    }

    pub fn part_stripes(&self, part: u64) -> Result<std::ops::Range<u64>> {
        if part >= self.parts() {
            return Err(Error("part outside object"));
        }
        let per_part = self.part_bytes / self.stripe_bytes();
        Ok(part * per_part..((part + 1) * per_part).min(self.stripes()))
    }

    pub fn matches_plan(&self, plan: UploadPlan) -> bool {
        self.total_bytes == plan.total_bytes
            && self.part_bytes == plan.part_bytes
            && self.expires_at == plan.expires_at
            && self.durability == plan.durability
    }

    /// V2 binds logical bytes to their immutable identity and stripe position.
    /// All data/parity fragments of a stripe carry the same source digest.
    pub fn source_hash(&self, fp: Fingerprint, stripe: u64, source: &[u8]) -> Result<[u8; 32]> {
        let length = self.source_bytes_in_stripe(stripe)?;
        if source.len() != length as usize {
            return Err(Error("incorrect logical source stripe length"));
        }
        let mut hash = Sha256::new();
        if self.format == ShardFormat::Crc32cSha256V2 {
            hash.update(b"Litiaina/LINK/source/v2\0");
            hash.update(fp.0);
            hash.update(stripe.to_be_bytes());
            hash.update(length.to_be_bytes());
        }
        hash.update(source);
        Ok(hash.finalize().into())
    }

    pub fn encode(&self, fp: Fingerprint) -> [u8; HEADER_BYTES] {
        let mut out = [0; HEADER_BYTES];
        out[..8].copy_from_slice(self.format.choose(b"N1LSHD01", b"N1LSHD02"));
        out[8..10].copy_from_slice(&self.format.version().to_be_bytes());
        out[10..12].copy_from_slice(&self.shard.to_be_bytes());
        out[12..14].copy_from_slice(&self.data_shards.to_be_bytes());
        out[14..16].copy_from_slice(&self.parity_shards.to_be_bytes());
        out[16..20].copy_from_slice(&self.block_bytes.to_be_bytes());
        out[20..24]
            .copy_from_slice(&u32::from(self.durability == Durability::Durable).to_be_bytes());
        out[24..32].copy_from_slice(&self.total_bytes.to_be_bytes());
        out[32..40].copy_from_slice(&self.part_bytes.to_be_bytes());
        out[40..48].copy_from_slice(&self.expires_at.to_be_bytes());
        let sum = checksum(
            self.format.choose(
                b"Litiaina/LINK/shard-header/v1\0",
                b"Litiaina/LINK/shard-header/v2\0",
            ),
            fp,
            &[&out[..96]],
        );
        out[96..128].copy_from_slice(&sum);
        out
    }

    pub fn decode(bytes: &[u8], pool: &PoolLayout, fp: Fingerprint, shard: u16) -> Result<Self> {
        pool.validate()?;
        if bytes.len() != HEADER_BYTES
            || &bytes[..8] != pool.shard_format.choose(b"N1LSHD01", b"N1LSHD02")
            || read16(bytes, 8) != pool.shard_format.version()
            || bytes[48..96].iter().any(|b| *b != 0)
            || checksum(
                pool.shard_format.choose(
                    b"Litiaina/LINK/shard-header/v1\0",
                    b"Litiaina/LINK/shard-header/v2\0",
                ),
                fp,
                &[&bytes[..96]],
            ) != bytes[96..128]
        {
            return Err(Error("invalid shard header"));
        }
        let flags = read32(bytes, 20);
        let header = Self {
            format: pool.shard_format,
            shard: read16(bytes, 10),
            data_shards: read16(bytes, 12),
            parity_shards: read16(bytes, 14),
            block_bytes: read32(bytes, 16),
            total_bytes: read64(bytes, 24),
            part_bytes: read64(bytes, 32),
            expires_at: read64(bytes, 40),
            durability: if flags == 0 {
                Durability::Standard
            } else {
                Durability::Durable
            },
        };
        if header.shard != shard
            || usize::from(shard) >= pool.width()
            || flags > 1
            || header.data_shards != pool.data_shards
            || header.parity_shards != pool.parity_shards
            || header.block_bytes != pool.block_bytes
            || header.part_bytes == 0
            || !header.part_bytes.is_multiple_of(pool.stripe_bytes())
            || header.total_bytes > i64::MAX as u64
            || header.parts() > 16_777_216
        {
            return Err(Error("shard header does not match immutable layout"));
        }
        header.file_bytes()?;
        Ok(header)
    }
}

pub struct Frame<'a> {
    pub source_hash: [u8; 32],
    pub payload: &'a [u8],
}

/// One fixed-size record per EC stripe. A sparse hole is not a valid frame.
pub fn encode_frame(
    header: &ShardHeader,
    fp: Fingerprint,
    stripe: u64,
    source_hash: [u8; 32],
    payload: &[u8],
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    encode_frame_into(header, fp, stripe, source_hash, payload, &mut out)?;
    Ok(out)
}

/// Reuse an owned record allocation after its previous I/O has completed.
pub fn encode_frame_into(
    header: &ShardHeader,
    fp: Fingerprint,
    stripe: u64,
    source_hash: [u8; 32],
    payload: &[u8],
    out: &mut Vec<u8>,
) -> Result<()> {
    out.clear();
    append_frame(header, fp, stripe, source_hash, payload, out)
}

/// Append a canonical frame to a bounded contiguous I/O buffer.
pub fn append_frame(
    header: &ShardHeader,
    fp: Fingerprint,
    stripe: u64,
    source_hash: [u8; 32],
    payload: &[u8],
    buffer: &mut Vec<u8>,
) -> Result<()> {
    if payload.len() != header.block_bytes as usize {
        return Err(Error("incorrect EC fragment size"));
    }
    let source_bytes = header.source_bytes_in_stripe(stripe)?;
    let start = buffer.len();
    buffer.resize(start + FRAME_HEADER_BYTES + payload.len(), 0);
    let out = &mut buffer[start..];
    out[..FRAME_HEADER_BYTES].fill(0);
    out[..8].copy_from_slice(header.format.choose(b"N1LFRM01", b"N1LFRM02"));
    out[8..16].copy_from_slice(&stripe.to_be_bytes());
    out[16..20].copy_from_slice(&source_bytes.to_be_bytes());
    out[20..22].copy_from_slice(&header.shard.to_be_bytes());
    out[24..56].copy_from_slice(&source_hash);
    out[FRAME_HEADER_BYTES..].copy_from_slice(payload);
    let sum = frame_checksum(header.format, fp, out);
    out[56..88].copy_from_slice(&sum);
    Ok(())
}

pub fn decode_frame<'a>(
    header: &ShardHeader,
    fp: Fingerprint,
    stripe: u64,
    bytes: &'a [u8],
) -> Result<Frame<'a>> {
    if bytes.len() != FRAME_HEADER_BYTES + header.block_bytes as usize
        || &bytes[..8] != header.format.choose(b"N1LFRM01", b"N1LFRM02")
        || read64(bytes, 8) != stripe
        || read32(bytes, 16) != header.source_bytes_in_stripe(stripe)?
        || read16(bytes, 20) != header.shard
        || bytes[22..24] != [0; 2]
        || bytes[88..96] != [0; 8]
        || frame_checksum(header.format, fp, bytes) != bytes[56..88]
    {
        return Err(Error("missing, torn, or corrupt EC frame"));
    }
    Ok(Frame {
        source_hash: bytes[24..56].try_into().expect("fixed digest"),
        payload: &bytes[FRAME_HEADER_BYTES..],
    })
}

/// Written inside the shard only after all parts have been validated. The root
/// is an ordered part/stripe integrity tree, not SHA256(concatenated user bytes).
pub fn encode_seal(header: &ShardHeader, fp: Fingerprint, root: [u8; 32]) -> [u8; SEAL_BYTES] {
    let mut out = [0; SEAL_BYTES];
    out[..8].copy_from_slice(header.format.choose(b"N1LEND01", b"N1LEND02"));
    out[8..16].copy_from_slice(&header.total_bytes.to_be_bytes());
    out[16..24].copy_from_slice(&header.stripes().to_be_bytes());
    out[32..64].copy_from_slice(&root);
    let h = header.encode(fp);
    let sum = checksum(
        header.format.choose(
            b"Litiaina/LINK/shard-seal/v1\0",
            b"Litiaina/LINK/shard-seal/v2\0",
        ),
        fp,
        &[&h, &out[..64]],
    );
    out[64..96].copy_from_slice(&sum);
    out
}

pub fn decode_seal(header: &ShardHeader, fp: Fingerprint, bytes: &[u8]) -> Result<[u8; 32]> {
    if bytes.len() != SEAL_BYTES
        || &bytes[..8] != header.format.choose(b"N1LEND01", b"N1LEND02")
        || read64(bytes, 8) != header.total_bytes
        || read64(bytes, 16) != header.stripes()
        || bytes[24..32] != [0; 8]
        || checksum(
            header.format.choose(
                b"Litiaina/LINK/shard-seal/v1\0",
                b"Litiaina/LINK/shard-seal/v2\0",
            ),
            fp,
            &[&header.encode(fp), &bytes[..64]],
        ) != bytes[64..96]
    {
        return Err(Error("shard has no valid final seal"));
    }
    Ok(bytes[32..64].try_into().expect("fixed root"))
}

pub fn part_hasher(format: ShardFormat, part: u64) -> Sha256 {
    let mut hash = Sha256::new();
    hash.update(format.choose(
        b"Litiaina/LINK/part-root/v1\0",
        b"Litiaina/LINK/part-root/v2\0",
    ));
    hash.update(part.to_be_bytes());
    hash
}

pub fn object_hasher(format: ShardFormat, total_bytes: u64, part_bytes: u64) -> Sha256 {
    let mut hash = Sha256::new();
    hash.update(format.choose(
        b"Litiaina/LINK/object-root/v1\0",
        b"Litiaina/LINK/object-root/v2\0",
    ));
    hash.update(total_bytes.to_be_bytes());
    hash.update(part_bytes.to_be_bytes());
    hash
}

fn frame_checksum(format: ShardFormat, fp: Fingerprint, bytes: &[u8]) -> [u8; 32] {
    match format {
        ShardFormat::Sha256V1 => checksum(
            b"Litiaina/LINK/frame/v1\0",
            fp,
            &[&bytes[..56], &bytes[88..]],
        ),
        ShardFormat::Crc32cSha256V2 => {
            let mut crc = crc32c::crc32c(b"Litiaina/LINK/frame/v2\0");
            for part in [&fp.0[..], &bytes[..56], &bytes[60..]] {
                crc = crc32c::crc32c_append(crc, part);
            }
            let mut out = [0; 32];
            out[..4].copy_from_slice(&crc.to_be_bytes());
            out
        }
    }
}

fn checksum(domain: &[u8], fp: Fingerprint, parts: &[&[u8]]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(fp.0);
    for part in parts {
        hash.update(part);
    }
    hash.finalize().into()
}
fn read16(b: &[u8], p: usize) -> u16 {
    u16::from_be_bytes(b[p..p + 2].try_into().expect("fixed field"))
}
fn read32(b: &[u8], p: usize) -> u32 {
    u32::from_be_bytes(b[p..p + 4].try_into().expect("fixed field"))
}
fn read64(b: &[u8], p: usize) -> u64 {
    u64::from_be_bytes(b[p..p + 8].try_into().expect("fixed field"))
}
