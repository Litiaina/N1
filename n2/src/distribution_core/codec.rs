use crate::enums::app_error_enum::AppError;
#[cfg(test)]
use simd_erasure_core::ReedSolomonDecoder;
use simd_erasure_core::ReedSolomonEncoder;
#[cfg(test)]
use std::collections::HashMap;

pub(crate) fn normalize_n2_stripe_block_size(v: usize) -> usize {
    let clamped = v.clamp(4 * 1024, 4 * 1024 * 1024);
    ((clamped + 4095) / 4096) * 4096
}

pub(crate) fn encode_cluster_stripe_blocks(
    stripe: &[u8],
    kc: usize,
    mc: usize,
    block_size: usize,
) -> Result<Vec<Vec<u8>>, AppError> {
    if stripe.len() != kc.saturating_mul(block_size) {
        return Err(AppError::BadRequest(format!(
            "invalid stripe length {} for kc={} block_size={}",
            stripe.len(),
            kc,
            block_size
        )));
    }

    let mut out = Vec::with_capacity(kc.saturating_add(mc));
    for i in 0..kc {
        let start = i * block_size;
        let end = start + block_size;
        out.push(stripe[start..end].to_vec());
    }

    if mc > 0 {
        let mut encoder = ReedSolomonEncoder::new(kc, mc, block_size)
            .map_err(|e| AppError::InternalServerError(format!("RS init: {}", e)))?;
        for original in &out {
            encoder
                .add_original_shard(original.as_slice())
                .map_err(|e| AppError::InternalServerError(format!("RS add shard: {}", e)))?;
        }
        let encoded = encoder
            .encode()
            .map_err(|e| AppError::InternalServerError(format!("RS encode: {}", e)))?;
        for i in 0..mc {
            let parity = encoded.recovery(i).ok_or_else(|| {
                AppError::InternalServerError("missing RS parity shard".to_string())
            })?;
            out.push(parity.to_vec());
        }
    }

    Ok(out)
}

#[cfg(test)]
pub(crate) fn reconstruct_cluster_shard_from_available(
    kc: usize,
    mc: usize,
    stripe_block_size: usize,
    target_cluster_shard_index: usize,
    available: &HashMap<usize, Vec<u8>>,
) -> Result<Vec<u8>, AppError> {
    if kc == 0 {
        return Err(AppError::InternalServerError("invalid kc=0".to_string()));
    }
    if available.len() < kc {
        return Err(AppError::InternalServerError(format!(
            "insufficient cluster shards for reconstruction (need {}, have {})",
            kc,
            available.len()
        )));
    }

    let max_shard_size = available.values().map(Vec::len).max().unwrap_or(0);
    if max_shard_size == 0 {
        return Ok(Vec::new());
    }
    if mc == 0 {
        if target_cluster_shard_index >= kc {
            return Err(AppError::InternalServerError(format!(
                "cannot reconstruct parity shard {} when mc=0",
                target_cluster_shard_index
            )));
        }
        let shard = available.get(&target_cluster_shard_index).ok_or_else(|| {
            AppError::InternalServerError(format!(
                "cannot reconstruct missing data shard {} without parity",
                target_cluster_shard_index
            ))
        })?;
        let mut out = shard.clone();
        out.resize(max_shard_size, 0);
        return Ok(out);
    }

    let stripe_count = max_shard_size.div_ceil(stripe_block_size);
    let mut out = Vec::with_capacity(max_shard_size);

    for stripe_idx in 0..stripe_count {
        let mut decoder = ReedSolomonDecoder::new(kc, mc, stripe_block_size).map_err(|e| {
            AppError::InternalServerError(format!("reconstruct shard decoder init failed: {}", e))
        })?;
        let mut originals: Vec<Option<Vec<u8>>> = vec![None; kc];
        let mut added = 0usize;

        for (cluster_idx, shard_bytes) in available {
            let start = stripe_idx * stripe_block_size;
            let mut block = vec![0u8; stripe_block_size];
            if start < shard_bytes.len() {
                let available_len = (shard_bytes.len() - start).min(stripe_block_size);
                block[..available_len].copy_from_slice(&shard_bytes[start..start + available_len]);
            }

            let add_res = if *cluster_idx < kc {
                originals[*cluster_idx] = Some(block.clone());
                decoder.add_original_shard(*cluster_idx, &block)
            } else {
                decoder.add_recovery_shard(*cluster_idx - kc, &block)
            };
            if add_res.is_ok() {
                added = added.saturating_add(1);
            }
        }

        if added < kc {
            return Err(AppError::InternalServerError(format!(
                "insufficient cluster blocks for stripe {} (need {}, have {})",
                stripe_idx, kc, added
            )));
        }

        let decoded = decoder.decode().map_err(|e| {
            AppError::InternalServerError(format!("reconstruct shard decode failed: {}", e))
        })?;
        for (idx, data) in decoded.restored_original_iter() {
            if idx < kc && originals[idx].is_none() {
                originals[idx] = Some(data.to_vec());
            }
        }

        let target_block = if target_cluster_shard_index < kc {
            originals[target_cluster_shard_index]
                .clone()
                .ok_or_else(|| {
                    AppError::InternalServerError(
                        "missing reconstructed original data block".to_string(),
                    )
                })?
        } else {
            let mut encoder = ReedSolomonEncoder::new(kc, mc, stripe_block_size).map_err(|e| {
                AppError::InternalServerError(format!("reconstruct parity init failed: {}", e))
            })?;
            for original in &originals {
                let block = original.as_ref().ok_or_else(|| {
                    AppError::InternalServerError(
                        "missing reconstructed original block for parity rebuild".to_string(),
                    )
                })?;
                encoder.add_original_shard(block).map_err(|e| {
                    AppError::InternalServerError(format!("reconstruct parity add failed: {}", e))
                })?;
            }
            let encoded = encoder.encode().map_err(|e| {
                AppError::InternalServerError(format!("reconstruct parity encode failed: {}", e))
            })?;
            encoded
                .recovery(target_cluster_shard_index - kc)
                .ok_or_else(|| {
                    AppError::InternalServerError(
                        "reconstructed parity shard index out of bounds".to_string(),
                    )
                })?
                .to_vec()
        };

        out.extend_from_slice(&target_block);
    }

    out.truncate(max_shard_size);
    Ok(out)
}

#[cfg(test)]
pub(crate) fn reconstruct_object_from_cluster_shards(
    kc: usize,
    mc: usize,
    stripe_block_size: usize,
    total_size_bytes: u64,
    mut available: Vec<(usize, Vec<u8>)>,
) -> Result<Vec<u8>, AppError> {
    if kc == 0 {
        return Err(AppError::InternalServerError("invalid kc=0".to_string()));
    }
    if available.len() < kc {
        return Err(AppError::InternalServerError(format!(
            "insufficient cluster shards for reconstruction (need {}, have {})",
            kc,
            available.len()
        )));
    }
    available.sort_by_key(|(idx, _)| *idx);

    let max_shard_size = available.iter().map(|(_, b)| b.len()).max().unwrap_or(0);
    if max_shard_size == 0 {
        return Ok(Vec::new());
    }
    let stripe_count = max_shard_size.div_ceil(stripe_block_size);
    let mut out = Vec::with_capacity((stripe_count * kc * stripe_block_size).max(1024));

    if mc == 0 {
        let mut data_shards = HashMap::<usize, Vec<u8>>::new();
        for (idx, bytes) in available {
            if idx < kc {
                data_shards.entry(idx).or_insert(bytes);
            }
        }
        if data_shards.len() < kc {
            return Err(AppError::InternalServerError(format!(
                "insufficient data shards without parity (need {}, have {})",
                kc,
                data_shards.len()
            )));
        }
        for stripe_idx in 0..stripe_count {
            let start = stripe_idx * stripe_block_size;
            for data_idx in 0..kc {
                let shard_bytes = data_shards.get(&data_idx).ok_or_else(|| {
                    AppError::InternalServerError(format!(
                        "missing data shard {} for no-parity reconstruction",
                        data_idx
                    ))
                })?;
                let mut block = vec![0u8; stripe_block_size];
                if start < shard_bytes.len() {
                    let copy_len = (shard_bytes.len() - start).min(stripe_block_size);
                    block[..copy_len].copy_from_slice(&shard_bytes[start..start + copy_len]);
                }
                out.extend_from_slice(&block);
            }
        }
        out.truncate(total_size_bytes as usize);
        return Ok(out);
    }

    for stripe_idx in 0..stripe_count {
        let mut decoder = ReedSolomonDecoder::new(kc, mc, stripe_block_size).map_err(|e| {
            AppError::InternalServerError(format!("reconstruct decoder init failed: {}", e))
        })?;
        let mut originals: Vec<Option<Vec<u8>>> = vec![None; kc];
        let mut added = 0usize;

        for (cluster_idx, shard_bytes) in &available {
            let start = stripe_idx * stripe_block_size;
            let end = start + stripe_block_size;
            if end > shard_bytes.len() {
                continue;
            }
            let block = &shard_bytes[start..end];

            let add_res = if *cluster_idx < kc {
                originals[*cluster_idx] = Some(block.to_vec());
                decoder.add_original_shard(*cluster_idx, block)
            } else {
                decoder.add_recovery_shard(*cluster_idx - kc, block)
            };
            if add_res.is_ok() {
                added = added.saturating_add(1);
            }
        }

        if added < kc {
            return Err(AppError::InternalServerError(format!(
                "insufficient cluster blocks for stripe {} (need {}, have {})",
                stripe_idx, kc, added
            )));
        }

        let decoded = decoder.decode().map_err(|e| {
            AppError::InternalServerError(format!("reconstruct decode failed: {}", e))
        })?;
        for (idx, data) in decoded.restored_original_iter() {
            if idx < kc {
                originals[idx] = Some(data.to_vec());
            }
        }

        for maybe in originals {
            let block = maybe.ok_or_else(|| {
                AppError::InternalServerError(
                    "missing reconstructed original data block".to_string(),
                )
            })?;
            out.extend_from_slice(&block);
        }
    }

    out.truncate(total_size_bytes as usize);
    Ok(out)
}
