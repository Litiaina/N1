use crate::core::disk_manager::{PartMetadata, ShardInfo};
use crate::distribution::peer::{fetch_peer_shard_chunk, parse_peer_shard_locator};
use crate::enums::app_error_enum::AppError;
use crate::io_control::record_read_recover;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use simd_erasure_core::ReedSolomonDecoder;
use std::io::SeekFrom;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

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

pub async fn read_cluster_data_chunk(
    part: &PartMetadata,
    shard_idx: usize,
    offset: u64,
    len: usize,
) -> Result<Vec<u8>, AppError> {
    let target = nth_data_shard(part, shard_idx).ok_or_else(|| {
        AppError::InternalServerError("cluster data shard index out of bounds".into())
    })?;

    if !shard_is_remote(target)
        && let Ok(bytes) = read_shard_chunk(target, offset, len).await
    {
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
    let mut reads = FuturesUnordered::new();

    for shard in &part.shards {
        let shard = shard.clone();
        let read_len = if shard.shard_index == target.shard_index {
            len
        } else {
            recovery_len
        };
        reads.push(async move {
            let result = read_shard_chunk(&shard, offset, read_len).await;
            (shard, result)
        });
    }

    while let Some((shard, result)) = reads.next().await {
        let Ok(chunk) = result else {
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
