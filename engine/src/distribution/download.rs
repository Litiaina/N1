use crate::core::checksum::{Sha256Digest, Sha256Stream, parse_sha256_hex, sha256_matches};
use crate::core::disk_manager::{PartMetadata, ShardInfo};
use crate::distribution::peer::{fetch_peer_shard_chunk, parse_peer_shard_locator};
use crate::enums::app_error_enum::AppError;
use crate::io_control::record_read_recover;
use ecs_core::{ReedSolomonDecoder, ReedSolomonEncoder};
use futures_util::future::join_all;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::SeekFrom;
use std::sync::{Arc, LazyLock};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Semaphore;

/// Repair/inspection payload buffers are independent of immutable shard length.
pub(crate) const MAX_REPAIR_CHUNK_BYTES: usize = 1024 * 1024;
pub(crate) const REPAIR_CACHE_BYTES: usize = 4 * MAX_REPAIR_CHUNK_BYTES;
const DEFAULT_VERIFIED_BLOCK_CACHE_BYTES: usize = 16 * MAX_REPAIR_CHUNK_BYTES;
const MAX_CHECKSUM_PROBES: usize = 32;
static CHECKSUM_PROBE_GATE: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(MAX_CHECKSUM_PROBES)));

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShardProbeResult {
    Healthy,
    Missing,
    ChecksumMismatch,
    Unavailable,
}

pub struct VerifiedShardCache {
    verified: HashMap<String, Vec<u8>>,
    rejected: HashSet<String>,
    verified_blocks: HashMap<String, Vec<u8>>,
    block_order: VecDeque<String>,
    max_verified_blocks: usize,
    verified_block_bytes: usize,
    max_verified_block_bytes: usize,
    schedule_read_repair: bool,
    streaming_repair: bool,
    verified_repair_sources: HashSet<String>,
}

impl Default for VerifiedShardCache {
    fn default() -> Self {
        Self {
            verified: HashMap::new(),
            rejected: HashSet::new(),
            verified_blocks: HashMap::new(),
            block_order: VecDeque::new(),
            max_verified_blocks: 16,
            verified_block_bytes: 0,
            max_verified_block_bytes: DEFAULT_VERIFIED_BLOCK_CACHE_BYTES,
            schedule_read_repair: true,
            streaming_repair: false,
            verified_repair_sources: HashSet::new(),
        }
    }
}

impl VerifiedShardCache {
    pub(crate) fn for_repair() -> Self {
        Self {
            schedule_read_repair: false,
            streaming_repair: true,
            max_verified_block_bytes: REPAIR_CACHE_BYTES,
            ..Self::default()
        }
    }

    pub(crate) fn for_sequential_export() -> Self {
        Self {
            max_verified_blocks: 1,
            ..Self::default()
        }
    }

    pub fn clear(&mut self) {
        self.verified.clear();
        self.rejected.clear();
        self.verified_blocks.clear();
        self.block_order.clear();
        self.verified_block_bytes = 0;
        self.verified_repair_sources.clear();
    }

    fn insert_verified_block(&mut self, key: String, bytes: Vec<u8>) {
        if self.verified_blocks.contains_key(&key) {
            return;
        }
        while self.verified_blocks.len() >= self.max_verified_blocks
            || self.verified_block_bytes.saturating_add(bytes.len()) > self.max_verified_block_bytes
        {
            let Some(oldest) = self.block_order.pop_front() else {
                break;
            };
            if let Some(old) = self.verified_blocks.remove(&oldest) {
                self.verified_block_bytes = self.verified_block_bytes.saturating_sub(old.len());
            }
        }
        // Oversized recorded checksum blocks use incremental verification instead.
        // Never let one metadata-controlled block defeat the cache byte bound.
        if bytes.len() > self.max_verified_block_bytes {
            return;
        }
        self.verified_block_bytes = self.verified_block_bytes.saturating_add(bytes.len());
        self.block_order.push_back(key.clone());
        self.verified_blocks.insert(key, bytes);
    }
}

pub fn shard_is_remote(shard: &ShardInfo) -> bool {
    parse_peer_shard_locator(&shard.absolute_path).is_some()
}

pub fn part_has_remote_shards(part: &PartMetadata) -> bool {
    part.shards.iter().any(shard_is_remote)
}

fn data_shards_count(part: &PartMetadata) -> usize {
    part.shards.iter().filter(|s| !s.is_parity).count()
}

fn nth_data_shard(part: &PartMetadata, nth: usize) -> Option<&ShardInfo> {
    let mut data_idx = 0usize;
    for shard in &part.shards {
        if shard.is_parity {
            continue;
        }
        if data_idx == nth {
            return Some(shard);
        }
        data_idx += 1;
    }
    None
}

async fn read_local_shard_chunk(
    shard: &ShardInfo,
    offset: u64,
    len: usize,
) -> Result<Vec<u8>, AppError> {
    let mut file = File::open(&shard.absolute_path)
        .await
        .map_err(|e| AppError::Io(e, format!("open cluster shard {}", shard.absolute_path)))?;
    file.seek(SeekFrom::Start(offset))
        .await
        .map_err(|e| AppError::Io(e, format!("seek cluster shard {}", shard.absolute_path)))?;
    let mut buf = vec![0u8; len];
    file.read_exact(&mut buf)
        .await
        .map_err(|e| AppError::Io(e, format!("read cluster shard {}", shard.absolute_path)))?;
    Ok(buf)
}

async fn read_shard_chunk(shard: &ShardInfo, offset: u64, len: usize) -> Result<Vec<u8>, AppError> {
    if let Some(locator) = parse_peer_shard_locator(&shard.absolute_path) {
        let bytes = fetch_peer_shard_chunk(&locator, offset, len).await?;
        if bytes.len() != len {
            return Err(AppError::ServiceUnavailable(format!(
                "peer shard {} returned {} bytes, expected {}",
                locator.node_id,
                bytes.len(),
                len
            )));
        }
        return Ok(bytes);
    }
    read_local_shard_chunk(shard, offset, len).await
}

fn expected_sha256(checksum: &str) -> Option<Sha256Digest> {
    parse_sha256_hex(checksum)
}

pub fn part_has_block_checksums(part: &PartMetadata, block_size: usize) -> bool {
    block_size > 0
        && part.shards.iter().all(|shard| {
            shard.checksum_block_size == block_size
                && shard.block_checksums.len()
                    >= usize::try_from(shard.size_bytes.div_ceil(block_size as u64))
                        .unwrap_or(usize::MAX)
        })
}

async fn read_verified_shard_block(
    shard: &ShardInfo,
    offset: u64,
    block_size: usize,
) -> Result<Vec<u8>, AppError> {
    if shard.checksum_block_size != block_size || block_size == 0 || offset % block_size as u64 != 0
    {
        return Err(AppError::InternalServerError(format!(
            "cluster shard {} has invalid block checksum geometry",
            shard.file_name
        )));
    }
    let block_index = usize::try_from(offset / block_size as u64)
        .map_err(|_| AppError::BadRequest("shard block index exceeds usize".into()))?;
    let expected = shard
        .block_checksums
        .get(block_index)
        .copied()
        .ok_or_else(|| {
            AppError::ServiceUnavailable(format!(
                "cluster shard {} is missing block checksum {}",
                shard.file_name, block_index
            ))
        })?;
    let remaining = shard.size_bytes.saturating_sub(offset);
    let length = remaining.min(block_size as u64) as usize;
    if length == 0 {
        return Err(AppError::ServiceUnavailable(format!(
            "cluster shard {} is shorter than block {}",
            shard.file_name, block_index
        )));
    }
    let bytes = read_shard_chunk(shard, offset, length).await?;
    if !sha256_matches(&bytes, &expected) {
        return Err(AppError::ServiceUnavailable(format!(
            "cluster shard {} block {} failed checksum verification",
            shard.file_name, block_index
        )));
    }
    Ok(bytes)
}

async fn read_verified_shard_range_cached(
    shard: &ShardInfo,
    offset: u64,
    len: usize,
    cache: &mut VerifiedShardCache,
) -> Result<Vec<u8>, AppError> {
    let block_size = shard.checksum_block_size;
    if block_size == 0 || shard.block_checksums.is_empty() || len == 0 {
        return Err(AppError::InternalServerError(format!(
            "cluster shard {} has no range-verification geometry",
            shard.file_name
        )));
    }
    let request_end = offset
        .checked_add(len as u64)
        .ok_or_else(|| AppError::BadRequest("shard read range overflows u64".into()))?;
    if request_end > shard.size_bytes {
        return Err(AppError::ServiceUnavailable(format!(
            "cluster shard {} is shorter than requested range",
            shard.file_name
        )));
    }
    let block_size_u64 = block_size as u64;
    let first_block = offset / block_size_u64;
    let last_block = request_end.saturating_sub(1) / block_size_u64;
    let mut output = Vec::with_capacity(len);
    for block_index in first_block..=last_block {
        let block_start = block_index * block_size_u64;
        if block_size > cache.max_verified_block_bytes
            || (cache.streaming_repair && block_size > MAX_REPAIR_CHUNK_BYTES)
        {
            let slice_start = offset.max(block_start);
            let slice_end = request_end.min(
                block_start
                    .saturating_add(block_size_u64)
                    .min(shard.size_bytes),
            );
            let bytes =
                read_verified_large_block_slice(shard, block_start, slice_start, slice_end).await?;
            output.extend_from_slice(&bytes);
            continue;
        }
        let cache_key = format!("{}\0{}", verified_cache_key(shard), block_index);
        if !cache.verified_blocks.contains_key(&cache_key) {
            let bytes = read_verified_shard_block(shard, block_start, block_size).await?;
            cache.insert_verified_block(cache_key.clone(), bytes);
        }
        let bytes = cache.verified_blocks.get(&cache_key).ok_or_else(|| {
            AppError::InternalServerError("verified shard block cache lost an entry".into())
        })?;
        let slice_start = offset.max(block_start).saturating_sub(block_start);
        let block_end = block_start.saturating_add(bytes.len() as u64);
        let slice_end = request_end.min(block_end).saturating_sub(block_start);
        let slice_start = usize::try_from(slice_start)
            .map_err(|_| AppError::BadRequest("verified block slice exceeds usize".into()))?;
        let slice_end = usize::try_from(slice_end)
            .map_err(|_| AppError::BadRequest("verified block slice exceeds usize".into()))?;
        if slice_start > slice_end || slice_end > bytes.len() {
            return Err(AppError::ServiceUnavailable(format!(
                "cluster shard {} block {} is shorter than requested range",
                shard.file_name, block_index
            )));
        }
        output.extend_from_slice(&bytes[slice_start..slice_end]);
    }
    if output.len() != len {
        return Err(AppError::ServiceUnavailable(format!(
            "cluster shard {} returned {} verified bytes, expected {}",
            shard.file_name,
            output.len(),
            len
        )));
    }
    Ok(output)
}

/// Verify a recorded block without allocating its metadata-controlled length.
/// Only the requested slice is retained. This fallback is deliberately allowed
/// to reread unusually large historical blocks rather than weaken their SHA.
async fn read_verified_large_block_slice(
    shard: &ShardInfo,
    block_start: u64,
    slice_start: u64,
    slice_end: u64,
) -> Result<Vec<u8>, AppError> {
    let block_size = shard.checksum_block_size as u64;
    if block_size == 0 || block_start % block_size != 0 || slice_start > slice_end {
        return Err(AppError::InternalServerError(
            "cluster shard has invalid large checksum block geometry".into(),
        ));
    }
    let expected = shard
        .block_checksums
        .get(
            usize::try_from(block_start / block_size)
                .map_err(|_| AppError::BadRequest("checksum block index exceeds usize".into()))?,
        )
        .ok_or_else(|| AppError::ServiceUnavailable("recorded checksum block is missing".into()))?;
    let block_end = block_start.saturating_add(block_size).min(shard.size_bytes);
    if slice_start < block_start || slice_end > block_end {
        return Err(AppError::ServiceUnavailable(
            "requested slice exceeds recorded checksum block".into(),
        ));
    }
    let slice_len = usize::try_from(slice_end - slice_start)
        .map_err(|_| AppError::BadRequest("checksum block slice exceeds usize".into()))?;
    let mut output = Vec::with_capacity(slice_len);
    let mut hasher = Sha256Stream::new();
    let mut offset = block_start;
    while offset < block_end {
        let len = (block_end - offset).min(MAX_REPAIR_CHUNK_BYTES as u64) as usize;
        let bytes = read_shard_chunk(shard, offset, len).await?;
        hasher.update(&bytes);
        let start = slice_start.max(offset);
        let end = slice_end.min(offset + bytes.len() as u64);
        if start < end {
            output.extend_from_slice(&bytes[(start - offset) as usize..(end - offset) as usize]);
        }
        offset += bytes.len() as u64;
    }
    if hasher.finalize() != *expected || output.len() != slice_len {
        return Err(AppError::ServiceUnavailable(format!(
            "cluster shard {} block failed checksum verification",
            shard.file_name
        )));
    }
    Ok(output)
}

pub async fn read_verified_cluster_data_stripe(
    part: &PartMetadata,
    offset: u64,
    block_size: usize,
) -> Result<Vec<u8>, AppError> {
    if !part_has_block_checksums(part, block_size) {
        return Err(AppError::InternalServerError(
            "cluster part does not contain complete block checksums".into(),
        ));
    }
    let data_count = data_shards_count(part);
    let parity_count = part.shards.len().saturating_sub(data_count);
    if data_count == 0 {
        return Err(AppError::InternalServerError(
            "cluster part has no data shards".into(),
        ));
    }

    let reads = part.shards.iter().cloned().map(|shard| async move {
        let result = read_verified_shard_block(&shard, offset, block_size).await;
        (shard, result)
    });
    let results = join_all(reads).await;
    let failed_shards = results
        .iter()
        .filter(|(_, result)| result.is_err())
        .map(|(shard, _)| shard.shard_index)
        .collect::<Vec<_>>();
    if !failed_shards.is_empty() {
        crate::distribution::repair::schedule_cluster_part_read_repair(part.clone(), failed_shards);
    }
    let mut data = vec![None; data_count];
    let mut readable = 0usize;
    for (shard, result) in &results {
        if !shard.is_parity && shard.shard_index < data_count {
            if let Ok(bytes) = result {
                data[shard.shard_index] = Some(bytes.clone());
                readable += 1;
            }
        }
    }
    if readable == data_count {
        return Ok(data.into_iter().flatten().flatten().collect());
    }
    if parity_count == 0 {
        return Err(AppError::ServiceUnavailable(
            "cluster data block unavailable and no parity is available".into(),
        ));
    }

    let mut decoder = ReedSolomonDecoder::new(data_count, parity_count, block_size)
        .map_err(|e| AppError::InternalServerError(e.to_string()))?;
    let mut added = 0usize;
    for (shard, result) in results {
        let Ok(bytes) = result else {
            continue;
        };
        let added_result = if shard.is_parity {
            decoder.add_recovery_shard(shard.shard_index.saturating_sub(data_count), bytes)
        } else {
            decoder.add_original_shard(shard.shard_index, bytes)
        };
        if added_result.is_ok() {
            added += 1;
        }
    }
    if added < data_count {
        return Err(AppError::ServiceUnavailable(format!(
            "insufficient verified cluster blocks; need {}, found {}",
            data_count, added
        )));
    }
    let decoded = decoder
        .decode()
        .map_err(|e| AppError::InternalServerError(e.to_string()))?;
    for (index, bytes) in decoded.restored_original_iter() {
        if index < data.len() {
            data[index] = Some(bytes.to_vec());
        }
    }
    if data.iter().any(Option::is_none) {
        return Err(AppError::InternalServerError(
            "cluster block reconstruction did not restore every data block".into(),
        ));
    }
    let restored = data.into_iter().flatten().flatten().collect::<Vec<_>>();
    record_read_recover(
        restored.len() as u64,
        part.shards.len().saturating_sub(added) as u64,
    );
    Ok(restored)
}

fn verified_cache_key(shard: &ShardInfo) -> String {
    format!(
        "{}\0{}\0{}",
        shard.absolute_path, shard.checksum, shard.size_bytes
    )
}

async fn read_full_shard(shard: &ShardInfo) -> Result<Vec<u8>, AppError> {
    let len = usize::try_from(shard.size_bytes).map_err(|_| {
        AppError::InternalServerError("shard is too large to verify in memory".into())
    })?;
    read_shard_chunk(shard, 0, len).await
}

fn classify_probe_error(error: AppError) -> ShardProbeResult {
    match error {
        AppError::NotFound => ShardProbeResult::Missing,
        AppError::Io(error, _) if error.kind() == std::io::ErrorKind::NotFound => {
            ShardProbeResult::Missing
        }
        _ => ShardProbeResult::Unavailable,
    }
}

async fn read_probe_bytes(shard: &ShardInfo, length: usize) -> Result<Vec<u8>, ShardProbeResult> {
    if let Some(locator) = parse_peer_shard_locator(&shard.absolute_path) {
        return match fetch_peer_shard_chunk(&locator, 0, length).await {
            Ok(bytes) if bytes.len() == length => Ok(bytes),
            Ok(_) => Err(ShardProbeResult::ChecksumMismatch),
            Err(error) => Err(classify_probe_error(error)),
        };
    }

    let mut file = match File::open(&shard.absolute_path).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(ShardProbeResult::Missing);
        }
        Err(_) => return Err(ShardProbeResult::Unavailable),
    };
    let mut bytes = vec![0u8; length];
    match file.read_exact(&mut bytes).await {
        Ok(_) => Ok(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
            Err(ShardProbeResult::ChecksumMismatch)
        }
        Err(_) => Err(ShardProbeResult::Unavailable),
    }
}

pub async fn shard_probe(shard: &ShardInfo, verify_integrity: bool) -> ShardProbeResult {
    // Status scans may inspect many objects and EC roles concurrently. Acquire
    // before any payload allocation, independently of repair worker admission.
    let Ok(_permit) = CHECKSUM_PROBE_GATE.clone().acquire_owned().await else {
        return ShardProbeResult::Unavailable;
    };
    if !verify_integrity {
        let probe_length = usize::from(shard.size_bytes != 0);
        return match read_probe_bytes(shard, probe_length).await {
            Ok(bytes) if bytes.len() == probe_length => ShardProbeResult::Healthy,
            Ok(_) => ShardProbeResult::ChecksumMismatch,
            Err(result) => result,
        };
    }
    let Some(expected) = expected_sha256(&shard.checksum) else {
        return ShardProbeResult::ChecksumMismatch;
    };
    match checksum_recorded_shard(shard).await {
        Ok(actual) if actual == expected => ShardProbeResult::Healthy,
        Ok(_) => ShardProbeResult::ChecksumMismatch,
        Err(result) => result,
    }
}

async fn checksum_recorded_shard(shard: &ShardInfo) -> Result<Sha256Digest, ShardProbeResult> {
    let mut hasher = Sha256Stream::new();
    if let Some(locator) = parse_peer_shard_locator(&shard.absolute_path) {
        // Even an empty shard must exist on the exact bound holder.
        if shard.size_bytes == 0 {
            let bytes = fetch_peer_shard_chunk(&locator, 0, 0)
                .await
                .map_err(classify_probe_error)?;
            if !bytes.is_empty() {
                return Err(ShardProbeResult::ChecksumMismatch);
            }
        }
        let mut offset = 0u64;
        while offset < shard.size_bytes {
            let len = (shard.size_bytes - offset).min(MAX_REPAIR_CHUNK_BYTES as u64) as usize;
            let bytes = fetch_peer_shard_chunk(&locator, offset, len)
                .await
                .map_err(classify_probe_error)?;
            if bytes.len() != len {
                return Err(ShardProbeResult::ChecksumMismatch);
            }
            hasher.update(&bytes);
            offset += bytes.len() as u64;
        }
    } else {
        let mut file = File::open(&shard.absolute_path).await.map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                ShardProbeResult::Missing
            } else {
                ShardProbeResult::Unavailable
            }
        })?;
        let info = file
            .metadata()
            .await
            .map_err(|_| ShardProbeResult::Unavailable)?;
        if !info.is_file() {
            return Err(ShardProbeResult::Unavailable);
        }
        if info.len() != shard.size_bytes {
            return Err(ShardProbeResult::ChecksumMismatch);
        }
        let mut bytes = vec![0u8; shard.size_bytes.min(MAX_REPAIR_CHUNK_BYTES as u64) as usize];
        let mut offset = 0u64;
        while offset < shard.size_bytes {
            let len = (shard.size_bytes - offset).min(bytes.len() as u64) as usize;
            file.read_exact(&mut bytes[..len]).await.map_err(|error| {
                if error.kind() == std::io::ErrorKind::UnexpectedEof {
                    ShardProbeResult::ChecksumMismatch
                } else {
                    ShardProbeResult::Unavailable
                }
            })?;
            hasher.update(&bytes[..len]);
            offset += len as u64;
        }
    }
    Ok(hasher.finalize())
}

pub async fn prefetch_verified_data_shards(part: &PartMetadata, cache: &mut VerifiedShardCache) {
    let pending = part
        .shards
        .iter()
        .filter(|shard| !shard.is_parity)
        .filter_map(|shard| {
            let expected = expected_sha256(&shard.checksum)?;
            let key = verified_cache_key(shard);
            if cache.verified.contains_key(&key) || cache.rejected.contains(&key) {
                return None;
            }
            let shard = shard.clone();
            Some(async move {
                let bytes = read_full_shard(&shard).await;
                (key, shard, expected, bytes)
            })
        });

    for (key, shard, expected, result) in join_all(pending).await {
        match result {
            Ok(bytes)
                if bytes.len() as u64 == shard.size_bytes && sha256_matches(&bytes, &expected) =>
            {
                cache.verified.insert(key, bytes);
            }
            Ok(_) | Err(_) => {
                cache.rejected.insert(key);
            }
        }
    }
}

pub(crate) async fn read_verified_shard_slice_cached(
    shard: &ShardInfo,
    offset: u64,
    len: usize,
    cache: &mut VerifiedShardCache,
) -> Result<Vec<u8>, AppError> {
    if shard.checksum_block_size > 0 && !shard.block_checksums.is_empty() {
        return read_verified_shard_range_cached(shard, offset, len, cache).await;
    }
    let Some(expected) = expected_sha256(&shard.checksum) else {
        return Err(AppError::ServiceUnavailable(format!(
            "cluster shard {} has no valid SHA-256 checksum",
            shard.file_name
        )));
    };
    let key = verified_cache_key(shard);
    if cache.rejected.contains(&key) {
        return Err(AppError::ServiceUnavailable(format!(
            "cluster shard {} failed checksum verification",
            shard.file_name
        )));
    }
    if cache.streaming_repair {
        // Historical records may have only one SHA for the complete donor.
        // Verify it once using bounded reads, then stream requested slices.
        // The target's final immutable SHA is independently checked before the
        // repair can succeed/publicize it, including a donor changing between
        // this verification pass and reconstruction. No source payload is kept.
        if !cache.verified_repair_sources.contains(&key) {
            match shard_probe(shard, true).await {
                ShardProbeResult::Healthy => {
                    cache.verified_repair_sources.insert(key.clone());
                }
                ShardProbeResult::Missing | ShardProbeResult::ChecksumMismatch => {
                    cache.rejected.insert(key);
                    return Err(AppError::ServiceUnavailable(format!(
                        "cluster shard {} failed checksum verification",
                        shard.file_name
                    )));
                }
                ShardProbeResult::Unavailable => {
                    return Err(AppError::ServiceUnavailable(format!(
                        "cluster shard {} is unavailable for checksum verification",
                        shard.file_name
                    )));
                }
            }
        }
        if len > MAX_REPAIR_CHUNK_BYTES {
            return Err(AppError::BadRequest(
                "repair source read exceeds its bounded chunk size".into(),
            ));
        }
        return read_shard_chunk(shard, offset, len).await;
    }
    if !cache.verified.contains_key(&key) {
        let bytes = read_full_shard(shard).await?;
        if bytes.len() as u64 != shard.size_bytes || !sha256_matches(&bytes, &expected) {
            cache.rejected.insert(key.clone());
            return Err(AppError::ServiceUnavailable(format!(
                "cluster shard {} failed checksum verification",
                shard.file_name
            )));
        }
        cache.verified.insert(key.clone(), bytes);
    }
    let bytes = cache.verified.get(&key).ok_or_else(|| {
        AppError::InternalServerError("verified shard cache lost an inserted shard".into())
    })?;
    let start = usize::try_from(offset)
        .map_err(|_| AppError::BadRequest("shard read offset exceeds usize".into()))?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| AppError::BadRequest("shard read range overflows usize".into()))?;
    if end > bytes.len() {
        return Err(AppError::ServiceUnavailable(format!(
            "cluster shard {} is shorter than requested range",
            shard.file_name
        )));
    }
    Ok(bytes[start..end].to_vec())
}

pub async fn read_cluster_data_chunk(
    part: &PartMetadata,
    shard_idx: usize,
    offset: u64,
    len: usize,
) -> Result<Vec<u8>, AppError> {
    let mut cache = VerifiedShardCache::default();
    read_cluster_data_chunk_cached(part, shard_idx, offset, len, &mut cache).await
}

pub async fn read_cluster_data_chunk_cached(
    part: &PartMetadata,
    shard_idx: usize,
    offset: u64,
    len: usize,
    cache: &mut VerifiedShardCache,
) -> Result<Vec<u8>, AppError> {
    let target = nth_data_shard(part, shard_idx).ok_or_else(|| {
        AppError::InternalServerError("cluster data shard index out of bounds".into())
    })?;

    if let Ok(bytes) = read_verified_shard_slice_cached(target, offset, len, cache).await {
        return Ok(bytes);
    }

    let data_count = data_shards_count(part);
    let parity_count = part.shards.len().saturating_sub(data_count);
    if data_count == 0 || parity_count == 0 {
        return Err(AppError::ServiceUnavailable(
            "cluster shard unavailable and no parity is available".into(),
        ));
    }

    let recovery_len = if len % 2 == 0 { len } else { len + 1 };
    let mut decoder = ReedSolomonDecoder::new(data_count, parity_count, recovery_len)
        .map_err(|e| AppError::InternalServerError(e.to_string()))?;
    let mut added = 0usize;
    let mut failed_shards = vec![target.shard_index];
    for shard in &part.shards {
        let shard = shard.clone();
        let read_len = if shard.shard_index == target.shard_index {
            len
        } else {
            recovery_len
        };
        let Ok(chunk) = read_verified_shard_slice_cached(&shard, offset, read_len, cache).await
        else {
            failed_shards.push(shard.shard_index);
            continue;
        };
        if shard.shard_index == target.shard_index {
            return Ok(chunk);
        }
        let added_result = if shard.is_parity {
            decoder.add_recovery_shard(shard.shard_index.saturating_sub(data_count), chunk)
        } else {
            decoder.add_original_shard(shard.shard_index, chunk)
        };
        if added_result.is_ok() {
            added += 1;
            if added >= data_count {
                break;
            }
        }
    }

    if added < data_count {
        return Err(AppError::ServiceUnavailable(format!(
            "insufficient cluster shards to reconstruct data shard {}; need {}, found {}",
            target.shard_index, data_count, added
        )));
    }

    let missing_observed = part.shards.len().saturating_sub(added).max(1);
    let restored = decoder
        .decode()
        .map_err(|e| AppError::InternalServerError(e.to_string()))?
        .restored_original_iter()
        .find(|(idx, _)| *idx == target.shard_index)
        .ok_or_else(|| AppError::InternalServerError("cluster reconstruction failed".into()))?
        .1
        .to_vec();
    let mut restored = restored;
    restored.truncate(len);
    record_read_recover(restored.len() as u64, missing_observed as u64);
    if cache.schedule_read_repair {
        crate::distribution::repair::schedule_cluster_part_read_repair(part.clone(), failed_shards);
    }
    Ok(restored)
}

/// Reconstruct a shard through a bounded writer. The caller MUST keep its
/// destination unpublished until this function and destination SHA/length
/// validation succeed. Production peer writers already stage privately and
/// verify the immutable expected checksum before atomic publication.
pub(crate) async fn reconstruct_cluster_shard_into<W: AsyncWrite + Unpin>(
    part: &PartMetadata,
    target_shard_idx: usize,
    block_size: usize,
    cache: &mut VerifiedShardCache,
    writer: &mut W,
) -> Result<u64, AppError> {
    if block_size == 0 {
        return Err(AppError::InternalServerError(
            "cluster repair block size is zero".into(),
        ));
    }
    let target = part
        .shards
        .iter()
        .find(|shard| shard.shard_index == target_shard_idx)
        .ok_or_else(|| {
            AppError::InternalServerError("cluster repair target shard missing".into())
        })?;
    let total_len = target.size_bytes;
    let expected = expected_sha256(&target.checksum).ok_or_else(|| {
        AppError::InternalServerError(format!(
            "reconstructed shard {} has no valid SHA-256 checksum",
            target.file_name
        ))
    })?;
    if total_len == 0 {
        return if sha256_matches(&[], &expected) {
            Ok(0)
        } else {
            Err(AppError::InternalServerError(
                "empty repaired shard checksum is invalid".into(),
            ))
        };
    }

    let data_count = data_shards_count(part);
    let parity_count = part.shards.len().saturating_sub(data_count);
    if data_count == 0 || parity_count == 0 {
        return Err(AppError::ServiceUnavailable(
            "cluster shard repair requires data and parity shards".into(),
        ));
    }
    if part.shards.iter().enumerate().any(|(index, shard)| {
        shard.shard_index != index
            || shard.is_parity != (index >= data_count)
            || shard.size_bytes != total_len
    }) {
        return Err(AppError::InternalServerError(
            "cluster repair requires unique ordered roles with equal recorded shard lengths".into(),
        ));
    }
    if !cache.streaming_repair {
        cache.clear();
        cache.streaming_repair = true;
        cache.schedule_read_repair = false;
        cache.max_verified_block_bytes = REPAIR_CACHE_BYTES;
    }
    let chunk_size = block_size.min(MAX_REPAIR_CHUNK_BYTES);
    let mut hasher = Sha256Stream::new();
    let mut offset = 0u64;
    let data_nth = part
        .shards
        .iter()
        .filter(|shard| !shard.is_parity)
        .position(|shard| shard.shard_index == target_shard_idx);

    while offset < total_len {
        let cur = (total_len - offset).min(chunk_size as u64) as usize;

        if let Some(data_idx) = data_nth {
            let bytes = read_cluster_data_chunk_cached(part, data_idx, offset, cur, cache).await?;
            if bytes.len() != cur {
                return Err(AppError::ServiceUnavailable(
                    "reconstructed data chunk has an incorrect length".into(),
                ));
            }
            hasher.update(&bytes);
            writer
                .write_all(&bytes)
                .await
                .map_err(|error| AppError::Io(error, "stream reconstructed data shard".into()))?;
            offset += cur as u64;
            continue;
        }

        let recovery_len = if cur % 2 == 0 { cur } else { cur + 1 };
        let mut encoder = ReedSolomonEncoder::new(data_count, parity_count, recovery_len)
            .map_err(|e| AppError::InternalServerError(format!("RS Enc Init: {e}")))?;
        for data_idx in 0..data_count {
            let mut data =
                read_cluster_data_chunk_cached(part, data_idx, offset, cur, cache).await?;
            if data.len() < recovery_len {
                data.resize(recovery_len, 0);
            }
            encoder
                .add_original_shard(&data)
                .map_err(|e| AppError::InternalServerError(format!("RS Add: {e}")))?;
        }
        let encoded = encoder
            .encode()
            .map_err(|e| AppError::InternalServerError(format!("RS Encode: {e}")))?;
        let recovery_idx = target_shard_idx.saturating_sub(data_count);
        let parity = encoded
            .recovery(recovery_idx)
            .ok_or_else(|| AppError::InternalServerError("missing reconstructed parity".into()))?;
        hasher.update(&parity[..cur]);
        writer
            .write_all(&parity[..cur])
            .await
            .map_err(|error| AppError::Io(error, "stream reconstructed parity shard".into()))?;
        offset += cur as u64;
    }

    if hasher.finalize() != expected {
        return Err(AppError::InternalServerError(format!(
            "reconstructed shard {} failed checksum validation",
            target.file_name
        )));
    }
    Ok(offset)
}

/// Small fixtures may collect output; production must use the streaming API.
#[cfg(test)]
async fn reconstruct_cluster_shard_cached(
    part: &PartMetadata,
    target_shard_idx: usize,
    block_size: usize,
    cache: &mut VerifiedShardCache,
) -> Result<Vec<u8>, AppError> {
    let mut out = Vec::new();
    reconstruct_cluster_shard_into(part, target_shard_idx, block_size, cache, &mut out).await?;
    Ok(out)
}

#[cfg(test)]
#[path = "download/tests.rs"]
mod tests;
