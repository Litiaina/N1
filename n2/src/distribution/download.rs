use crate::core::disk_manager::{PartMetadata, ShardInfo};
use crate::distribution::peer::{fetch_peer_shard_chunk, parse_peer_shard_locator};
use crate::enums::app_error_enum::AppError;
use crate::io_control::record_read_recover;
use crc32fast::Hasher;
use simd_erasure_core::{ReedSolomonDecoder, ReedSolomonEncoder};
use std::collections::{HashMap, HashSet};
use std::io::SeekFrom;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

#[derive(Default)]
pub struct VerifiedShardCache {
    verified: HashMap<String, Vec<u8>>,
    rejected: HashSet<String>,
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

pub async fn shard_probe_readable(shard: &ShardInfo) -> bool {
    if shard.size_bytes == 0 {
        return true;
    }
    read_shard_chunk(shard, 0, 1).await.is_ok()
}

fn expected_crc32(checksum: &str) -> Option<u32> {
    let checksum = checksum.trim();
    if checksum.len() != 8 {
        return None;
    }
    u32::from_str_radix(checksum, 16).ok()
}

fn crc32_matches(bytes: &[u8], expected: u32) -> bool {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize() == expected
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

pub async fn shard_probe_verified(shard: &ShardInfo) -> bool {
    let Some(expected) = expected_crc32(&shard.checksum) else {
        return shard_probe_readable(shard).await;
    };
    let Ok(bytes) = read_full_shard(shard).await else {
        return false;
    };
    bytes.len() as u64 == shard.size_bytes && crc32_matches(&bytes, expected)
}

pub(crate) async fn read_verified_shard_slice_cached(
    shard: &ShardInfo,
    offset: u64,
    len: usize,
    cache: &mut VerifiedShardCache,
) -> Result<Vec<u8>, AppError> {
    let Some(expected) = expected_crc32(&shard.checksum) else {
        return read_shard_chunk(shard, offset, len).await;
    };
    let key = verified_cache_key(shard);
    if cache.rejected.contains(&key) {
        return Err(AppError::ServiceUnavailable(format!(
            "cluster shard {} failed checksum verification",
            shard.file_name
        )));
    }
    if !cache.verified.contains_key(&key) {
        let bytes = read_full_shard(shard).await?;
        if bytes.len() as u64 != shard.size_bytes || !crc32_matches(&bytes, expected) {
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
    for shard in &part.shards {
        let shard = shard.clone();
        let read_len = if shard.shard_index == target.shard_index {
            len
        } else {
            recovery_len
        };
        let Ok(chunk) = read_verified_shard_slice_cached(&shard, offset, read_len, cache).await
        else {
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
    Ok(restored)
}

pub(crate) async fn reconstruct_cluster_shard_cached(
    part: &PartMetadata,
    target_shard_idx: usize,
    block_size: usize,
    cache: &mut VerifiedShardCache,
) -> Result<Vec<u8>, AppError> {
    let target = part
        .shards
        .iter()
        .find(|shard| shard.shard_index == target_shard_idx)
        .ok_or_else(|| {
            AppError::InternalServerError("cluster repair target shard missing".into())
        })?;
    let total_len = usize::try_from(target.size_bytes).map_err(|_| {
        AppError::InternalServerError("cluster repair target shard is too large".into())
    })?;
    if total_len == 0 {
        return Ok(Vec::new());
    }

    let data_count = data_shards_count(part);
    let parity_count = part.shards.len().saturating_sub(data_count);
    if data_count == 0 || parity_count == 0 {
        return Err(AppError::ServiceUnavailable(
            "cluster shard repair requires data and parity shards".into(),
        ));
    }

    let mut out = Vec::with_capacity(total_len);
    let mut offset = 0u64;
    let data_nth = part
        .shards
        .iter()
        .filter(|shard| !shard.is_parity)
        .position(|shard| shard.shard_index == target_shard_idx);

    while (offset as usize) < total_len {
        let remaining = total_len - offset as usize;
        let cur = remaining.min(block_size);

        if let Some(data_idx) = data_nth {
            let bytes = read_cluster_data_chunk_cached(part, data_idx, offset, cur, cache).await?;
            out.extend_from_slice(&bytes);
            offset = offset.saturating_add(cur as u64);
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
        out.extend_from_slice(&parity[..cur]);
        offset = offset.saturating_add(cur as u64);
    }

    out.truncate(total_len);
    if let Some(expected) = expected_crc32(&target.checksum) {
        if !crc32_matches(&out, expected) {
            return Err(AppError::InternalServerError(format!(
                "reconstructed shard {} failed checksum validation",
                target.file_name
            )));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::disk_manager::ShardInfo;
    use simd_erasure_core::ReedSolomonEncoder;
    use ulid::Ulid;

    #[tokio::test]
    async fn cluster_read_reconstructs_missing_data_from_parity() {
        let root = std::env::temp_dir().join(format!("n2-cluster-read-{}", Ulid::new()));
        tokio::fs::create_dir_all(&root).await.expect("create root");
        let data = b"abcd".to_vec();
        let mut encoder = ReedSolomonEncoder::new(1, 1, data.len()).expect("encoder");
        encoder.add_original_shard(&data).expect("add data");
        let encoded = encoder.encode().expect("encode");
        let parity = encoded.recovery(0).expect("parity").to_vec();
        drop(encoded);
        let parity_path = root.join("part_0_shard_1.parity");
        tokio::fs::write(&parity_path, parity)
            .await
            .expect("write parity");
        let part = PartMetadata {
            part_index: 0,
            size_bytes: data.len() as u64,
            created_at: 0,
            checksum: None,
            shards: vec![
                ShardInfo {
                    shard_index: 0,
                    file_name: "part_0_shard_0.shard".into(),
                    absolute_path: root
                        .join("missing-data.shard")
                        .to_string_lossy()
                        .to_string(),
                    size_bytes: data.len() as u64,
                    checksum: String::new(),
                    is_parity: false,
                },
                ShardInfo {
                    shard_index: 1,
                    file_name: "part_0_shard_1.parity".into(),
                    absolute_path: parity_path.to_string_lossy().to_string(),
                    size_bytes: data.len() as u64,
                    checksum: String::new(),
                    is_parity: true,
                },
            ],
        };

        let restored = read_cluster_data_chunk(&part, 0, 0, data.len())
            .await
            .expect("reconstruct");
        assert_eq!(restored, data);
        let _ = tokio::fs::remove_dir_all(root).await;
    }
}
