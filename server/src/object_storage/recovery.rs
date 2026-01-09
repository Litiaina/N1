use crate::enums::app_error_enum::AppError;
use crate::object_storage::ec::STRIPE_BLOCK_SIZE;
use crate::structs::object_storage_structs::{PartMetadata, ShardInfo};
use crc32fast::Hasher;
use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::{error, info};

pub async fn verify_shard_integrity(file: &mut File, expected_checksum: &str) -> bool {
    if expected_checksum.is_empty() {
        return true;
    }

    if expected_checksum.len() != 8 {
        return false;
    }

    let original_pos = match file.seek(SeekFrom::Current(0)).await {
        Ok(pos) => pos,
        Err(_) => return false,
    };

    if file.seek(SeekFrom::Start(0)).await.is_err() {
        let _ = file.seek(SeekFrom::Start(original_pos)).await;
        return false;
    }

    let mut hasher = Hasher::new();
    let mut buffer = vec![0u8; 256 * 1024];

    loop {
        match file.read(&mut buffer).await {
            Ok(0) => break,
            Ok(n) => hasher.update(&buffer[..n]),
            Err(_) => {
                let _ = file.seek(SeekFrom::Start(original_pos)).await;
                return false;
            }
        }
    }

    let calculated = hasher.finalize();

    let expected = match u32::from_str_radix(expected_checksum, 16) {
        Ok(v) => v,
        Err(_) => {
            let _ = file.seek(SeekFrom::Start(original_pos)).await;
            return false;
        }
    };

    let _ = file.seek(SeekFrom::Start(original_pos)).await;

    calculated == expected
}

pub async fn read_or_recover_chunk(
    file_cache: &mut HashMap<String, File>,
    version_dir: &Path,
    part: Arc<PartMetadata>,
    shard_idx: usize,
    offset: u64,
    len: usize,
) -> Result<Vec<u8>, AppError> {
    let mut buf = vec![0u8; len];

    let target_shard = part
        .shards
        .get(shard_idx)
        .ok_or(AppError::InternalServerError("Bad shard index".into()))?;

    let file_name = target_shard.file_name.as_str();

    if let Some(file) = file_cache.get_mut(file_name) {
        if file.seek(SeekFrom::Start(offset)).await.is_ok()
            && file.read_exact(&mut buf).await.is_ok()
        {
            return Ok(buf);
        }
    }

    if !file_cache.contains_key(file_name) {
        let path = version_dir.join(file_name);
        if let Ok(mut f) = File::open(&path).await {
            if verify_shard_integrity(&mut f, &target_shard.checksum).await {
                file_cache.insert(file_name.to_owned(), f);
            }
        }
    }

    if let Some(file) = file_cache.get_mut(file_name) {
        if file.seek(SeekFrom::Start(offset)).await.is_ok()
            && file.read_exact(&mut buf).await.is_ok()
        {
            return Ok(buf);
        }
    }

    let data_count = part.shards.iter().filter(|s| !s.is_parity).count();
    let parity_count = part.shards.len() - data_count;
    let mut decoder = ReedSolomonDecoder::new(data_count, parity_count, len)
        .map_err(|e| AppError::InternalServerError(e.to_string()))?;

    let mut shards_added = 0;

    for s in &part.shards {
        if s.shard_index == shard_idx {
            continue;
        }

        let fname = s.file_name.as_str();

        let file = if let Some(f) = file_cache.get_mut(fname) {
            f
        } else {
            let path = version_dir.join(fname);
            let mut f = match File::open(&path).await {
                Ok(f) => f,
                Err(_) => continue,
            };
            if !verify_shard_integrity(&mut f, &s.checksum).await {
                continue;
            }
            file_cache.insert(fname.to_owned(), f);
            file_cache.get_mut(fname).unwrap()
        };

        if file.seek(SeekFrom::Start(offset)).await.is_err() {
            continue;
        }

        let mut chunk = vec![0u8; len];
        if file.read_exact(&mut chunk).await.is_err() {
            continue;
        }

        let res = if s.is_parity {
            decoder.add_recovery_shard(s.shard_index - data_count, chunk)
        } else {
            decoder.add_original_shard(s.shard_index, chunk)
        };

        if res.is_ok() {
            shards_added += 1;
            if shards_added >= data_count {
                break;
            }
        }
    }

    if shards_added < data_count {
        return Err(AppError::InternalServerError("Insufficient shards".into()));
    }

    let restored = decoder
        .decode()
        .map_err(|e| AppError::InternalServerError(e.to_string()))?
        .restored_original_iter()
        .find(|(idx, _)| *idx == shard_idx)
        .ok_or(AppError::InternalServerError("Missing shard".into()))?
        .1
        .to_vec();

    let version_dir_clone = version_dir.to_path_buf();
    let part_clone = Arc::clone(&part);
    let shard_index = target_shard.shard_index;

    tokio::spawn(async move {
        if let Err(e) = repair_entire_shard(&version_dir_clone, part_clone, shard_index).await {
            error!("Background repair failed for shard {}: {}", shard_index, e);
        }
    });

    Ok(restored)
}

pub async fn repair_entire_shard(
    version_dir: &Path,
    part: Arc<PartMetadata>,
    target_shard_idx: usize,
) -> Result<(), AppError> {
    let target_shard = part
        .shards
        .get(target_shard_idx)
        .ok_or(AppError::InternalServerError(
            "Target shard not found".into(),
        ))?;

    let final_path = version_dir.join(&target_shard.file_name);

    let unique_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();

    let temp_path = version_dir.join(format!("{}.{}.repair", target_shard.file_name, unique_id));

    if final_path.exists() {
        if let Ok(mut f) = File::open(&final_path).await {
            if verify_shard_integrity(&mut f, &target_shard.checksum).await {
                return Ok(());
            }
        }
    }

    let mut temp_file = File::create(&temp_path)
        .await
        .map_err(|e| AppError::Io(e, "create repair file".into()))?;

    let data_shards: Vec<&ShardInfo> = part.shards.iter().filter(|s| !s.is_parity).collect();
    let data_count = data_shards.len();
    let parity_count = part.shards.len() - data_count;
    let block_size = STRIPE_BLOCK_SIZE;

    let total_shard_size = part.shards.first().map(|s| s.size_bytes).unwrap_or(0);

    let mut peer_files: Vec<Option<File>> = Vec::with_capacity(part.shards.len());
    for s in &part.shards {
        if s.shard_index == target_shard_idx {
            peer_files.push(None);
        } else {
            let s_path = version_dir.join(&s.file_name);
            let f = File::open(&s_path).await.ok();
            peer_files.push(f);
        }
    }

    let mut offset = 0u64;
    while offset < total_shard_size {
        let mut decoder = ReedSolomonDecoder::new(data_count, parity_count, block_size)
            .map_err(|e| AppError::InternalServerError(format!("RS Init: {}", e)))?;

        let mut stripe_data: Vec<Option<Vec<u8>>> = vec![None; data_count];
        let mut shards_found = 0;

        for (idx, file_opt) in peer_files.iter_mut().enumerate() {
            let s_info = &part.shards[idx];

            if let Some(f) = file_opt {
                let mut buf = vec![0u8; block_size];
                if f.read_exact(&mut buf).await.is_ok() {
                    if !s_info.is_parity && s_info.shard_index < data_count {
                        stripe_data[s_info.shard_index] = Some(buf.clone());
                    }

                    let res = if s_info.is_parity {
                        decoder.add_recovery_shard(s_info.shard_index - data_count, buf)
                    } else {
                        decoder.add_original_shard(s_info.shard_index, buf)
                    };

                    if res.is_ok() {
                        shards_found += 1;
                    }
                }
            }
        }

        if shards_found < data_count {
            let _ = fs::remove_file(&temp_path).await;
            return Err(AppError::InternalServerError(
                "Not enough shards for repair".into(),
            ));
        }

        let decoded = decoder
            .decode()
            .map_err(|e| AppError::InternalServerError(format!("RS Decode: {}", e)))?;

        for (idx, data) in decoded.restored_original_iter() {
            if idx < data_count {
                stripe_data[idx] = Some(data.to_vec());
            }
        }

        if target_shard_idx < data_count {
            if let Some(data) = &stripe_data[target_shard_idx] {
                temp_file
                    .write_all(data)
                    .await
                    .map_err(|e| AppError::Io(e, "write repair chunk".into()))?;
            }
        } else {
            let mut encoder = ReedSolomonEncoder::new(data_count, parity_count, block_size)
                .map_err(|e| AppError::InternalServerError(format!("RS Enc Init: {}", e)))?;

            for data in stripe_data.iter().flatten() {
                encoder
                    .add_original_shard(data)
                    .map_err(|e| AppError::InternalServerError(format!("RS Add: {}", e)))?;
            }

            let enc_result = encoder
                .encode()
                .map_err(|e| AppError::InternalServerError(format!("RS Encode: {}", e)))?;
            let parity_rel_idx = target_shard_idx - data_count;
            if let Some(data) = enc_result.recovery_iter().nth(parity_rel_idx) {
                temp_file
                    .write_all(data)
                    .await
                    .map_err(|e| AppError::Io(e, "write repair chunk".into()))?;
            }
        }

        offset += block_size as u64;
    }

    temp_file
        .flush()
        .await
        .map_err(|e| AppError::Io(e, "flush repair".into()))?;

    drop(temp_file);
    drop(peer_files);

    if let Err(e) = fs::rename(&temp_path, &final_path).await {
        if final_path.exists() {
            let _ = fs::remove_file(&temp_path).await;
            info!(
                "Shard {} was repaired by another process, discarding temp file.",
                target_shard_idx
            );
            return Ok(());
        }
        return Err(AppError::Io(e, "finalize repair".into()));
    }

    Ok(())
}
