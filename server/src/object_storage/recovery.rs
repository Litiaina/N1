use crate::enums::app_error_enum::AppError;
use crate::object_storage::disk_manager::{PartMetadata, ShardInfo};
use crate::object_storage::upload::DirectWriter;

use crc32fast::Hasher;
use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};
use std::collections::HashMap;
use std::fs::OpenOptions as StdOpenOptions;
use std::io::SeekFrom;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Semaphore;
use tracing::{info, warn};
use ulid::Ulid;

static RECOVERY_SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(4));

static RECOVERY_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    let total_cores = core_ids.len();
    let recovery_worker_count = (total_cores / 4).max(1);

    let pinned_cores = core_ids
        .into_iter()
        .rev()
        .take(recovery_worker_count)
        .collect::<Vec<_>>();
    let pinned_cores_arc = Arc::new(pinned_cores);
    let next_core_idx = Arc::new(AtomicUsize::new(0));

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(recovery_worker_count)
        .thread_name("n1-recovery")
        .on_thread_start(move || {
            if !pinned_cores_arc.is_empty() {
                let idx = next_core_idx.fetch_add(1, Ordering::Relaxed);
                let core = pinned_cores_arc[idx % pinned_cores_arc.len()];
                let _ = core_affinity::set_for_current(core);
            }
        })
        .enable_all()
        .build()
        .expect("Failed to build dedicated recovery runtime")
});

#[inline]
fn strip_tmp_attempt_suffix(s: &str) -> &str {
    if let Some(pos) = s.find(".tmp.") {
        &s[..pos]
    } else {
        s
    }
}

fn stable_abs_candidates(abs: &str) -> Vec<String> {
    let mut out = Vec::new();

    if abs.is_empty() {
        return out;
    }

    let stable = strip_tmp_attempt_suffix(abs).to_string();
    if stable != abs {
        out.push(stable);
    }

    out.push(abs.to_string());

    if let Some(stripped) = abs.strip_suffix(".recovering") {
        out.push(stripped.to_string());
        out.push(strip_tmp_attempt_suffix(stripped).to_string());
    }

    let mut seen = std::collections::HashSet::new();
    out.retain(|p| seen.insert(p.clone()));
    out
}

async fn create_direct_file_for_repair(path: &PathBuf) -> Result<std::fs::File, AppError> {
    let path_clone = path.clone();

    tokio::task::spawn_blocking(move || {
        if let Some(parent) = path_clone.parent() {
            if !parent.exists() {
                let _ = std::fs::create_dir_all(parent);
            }
        }

        let mut options = StdOpenOptions::new();
        options.write(true).create(true).truncate(true);

        #[cfg(target_os = "linux")]
        {
            options.custom_flags(libc::O_DIRECT);
        }
        #[cfg(windows)]
        {
            const FILE_FLAG_NO_BUFFERING: u32 = 0x20000000;
            options.custom_flags(FILE_FLAG_NO_BUFFERING);
        }

        options
            .open(&path_clone)
            .map_err(|e| AppError::Io(e, format!("create repair file {}", path_clone.display())))
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("Spawn blocking failed: {}", e)))?
}

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
        return false;
    }

    let mut hasher = Hasher::new();
    let mut buffer = vec![0u8; 64 * 1024];

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
        Err(_) => return false,
    };

    let _ = file.seek(SeekFrom::Start(original_pos)).await;

    calculated == expected
}

async fn try_open_verified(path: &str, checksum: &str) -> Option<File> {
    if path.is_empty() {
        return None;
    }
    if let Ok(mut f) = File::open(path).await {
        if verify_shard_integrity(&mut f, checksum).await {
            return Some(f);
        }
    }
    None
}

async fn get_or_open_shard(
    file_cache: &mut HashMap<String, File>,
    shard_info: &ShardInfo,
) -> Option<File> {
    if let Some(file) = file_cache.get(&shard_info.file_name) {
        return file.try_clone().await.ok();
    }

    for cand in stable_abs_candidates(&shard_info.absolute_path) {
        if let Some(f) = try_open_verified(&cand, &shard_info.checksum).await {
            if let Ok(cloned) = f.try_clone().await {
                file_cache.insert(shard_info.file_name.clone(), cloned);
                return Some(f);
            }
        }
    }

    warn!(
        "Failed to open/verify shard for read: {}",
        shard_info.absolute_path
    );
    None
}

pub async fn read_or_recover_chunk(
    file_cache: &mut HashMap<String, File>,
    _version_dir: &Path,
    part: Arc<PartMetadata>,
    shard_idx: usize,
    offset: u64,
    len: usize,
    block_size: usize,
) -> Result<Vec<u8>, AppError> {
    let mut buf = vec![0u8; len];
    let target_shard = part
        .shards
        .get(shard_idx)
        .ok_or(AppError::InternalServerError("Bad shard index".into()))?;

    if let Some(mut file) = get_or_open_shard(file_cache, target_shard).await {
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

        let mut file = match get_or_open_shard(file_cache, s).await {
            Some(f) => f,
            None => continue,
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
        return Err(AppError::InternalServerError(format!(
            "Insufficient shards to recover data. Need {}, found {}.",
            data_count, shards_added
        )));
    }

    let restored = decoder
        .decode()
        .map_err(|e| AppError::InternalServerError(e.to_string()))?
        .restored_original_iter()
        .find(|(idx, _)| *idx == shard_idx)
        .ok_or(AppError::InternalServerError(
            "Reconstruction failed for target shard".into(),
        ))?
        .1
        .to_vec();

    let part_clone = Arc::clone(&part);
    let target_idx = target_shard.shard_index;

    RECOVERY_RUNTIME.spawn(async move {
        if let Err(e) = repair_entire_shard(part_clone, target_idx, block_size).await {
            warn!("Background repair failed for shard {}: {}", target_idx, e);
        }
    });

    Ok(restored)
}

pub async fn repair_entire_shard(
    part: Arc<PartMetadata>,
    target_shard_idx: usize,
    block_size: usize,
) -> Result<(), AppError> {
    let _permit = RECOVERY_SEMAPHORE
        .acquire()
        .await
        .map_err(|_| AppError::InternalServerError("Semaphore error".into()))?;

    let target_shard = part
        .shards
        .get(target_shard_idx)
        .ok_or(AppError::InternalServerError(
            "Target shard not found in metadata".into(),
        ))?;

    let final_path_str = strip_tmp_attempt_suffix(&target_shard.absolute_path).to_string();
    if final_path_str.is_empty() {
        return Err(AppError::InternalServerError(
            "Target shard has empty absolute_path".into(),
        ));
    }
    let final_path = PathBuf::from(final_path_str);

    let parent_dir = final_path
        .parent()
        .ok_or(AppError::InternalServerError("Invalid shard path".into()))?
        .to_path_buf();

    if !fs::try_exists(&parent_dir).await.unwrap_or(false) {
        let _ = fs::create_dir_all(&parent_dir).await;
    }

    let temp_path = parent_dir.join(format!(
        "{}.tmp.repair.{}",
        strip_tmp_attempt_suffix(&target_shard.file_name),
        Ulid::new().to_string()
    ));

    if fs::try_exists(&final_path).await.unwrap_or(false) {
        if let Ok(mut f) = File::open(&final_path).await {
            if verify_shard_integrity(&mut f, &target_shard.checksum).await {
                return Ok(());
            }
        }
    }

    let std_file = match create_direct_file_for_repair(&temp_path).await {
        Ok(f) => f,
        Err(e) => {
            warn!("Failed to create repair file: {}", e);
            return Err(e);
        }
    };

    let total_shard_size = target_shard.size_bytes;

    let file_clone = std_file
        .try_clone()
        .map_err(|e| AppError::Io(e, "clone handle".into()))?;

    if let Err(e) = tokio::task::spawn_blocking(move || file_clone.set_len(total_shard_size)).await
    {
        let _ = fs::remove_file(&temp_path).await;
        return Err(AppError::InternalServerError(format!(
            "Failed to preallocate repair file: {}",
            e
        )));
    }

    let mut temp_file = DirectWriter::new(std_file);

    let data_count = part.shards.iter().filter(|s| !s.is_parity).count();
    let parity_count = part.shards.len() - data_count;

    let mut peer_files: Vec<Option<File>> = Vec::with_capacity(part.shards.len());
    for s in &part.shards {
        if s.shard_index == target_shard_idx {
            peer_files.push(None);
        } else {
            let mut opened: Option<File> = None;
            for cand in stable_abs_candidates(&s.absolute_path) {
                if let Ok(f) = File::open(&cand).await {
                    opened = Some(f);
                    break;
                }
            }
            peer_files.push(opened);
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
                if f.seek(SeekFrom::Start(offset)).await.is_err() {
                    continue;
                }

                let mut buf = vec![0u8; block_size];
                if f.read_exact(&mut buf).await.is_ok() {
                    let res = if s_info.is_parity {
                        decoder.add_recovery_shard(s_info.shard_index - data_count, buf.clone())
                    } else {
                        if s_info.shard_index < data_count {
                            stripe_data[s_info.shard_index] = Some(buf.clone());
                        }
                        decoder.add_original_shard(s_info.shard_index, buf)
                    };

                    if res.is_ok() {
                        shards_found += 1;
                    }
                }
            }
        }

        if shards_found < data_count {
            drop(temp_file);
            let _ = fs::remove_file(&temp_path).await;
            return Err(AppError::InternalServerError(
                "Not enough healthy shards to repair on disk".into(),
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
                    .map_err(|e| AppError::Io(e, "write repair data".into()))?;
            }
        } else {
            let mut encoder = ReedSolomonEncoder::new(data_count, parity_count, block_size)
                .map_err(|e| AppError::InternalServerError(format!("RS Enc Init: {}", e)))?;

            for data in stripe_data.iter().flatten() {
                encoder
                    .add_original_shard(data)
                    .map_err(|e| AppError::InternalServerError(format!("RS Add: {}", e)))?;
            }

            let enc = encoder
                .encode()
                .map_err(|e| AppError::InternalServerError(format!("RS Encode: {}", e)))?;

            if let Some(data) = enc.recovery_iter().nth(target_shard_idx - data_count) {
                temp_file
                    .write_all(data)
                    .await
                    .map_err(|e| AppError::Io(e, "write repair parity".into()))?;
            }
        }

        offset += block_size as u64;
    }

    temp_file
        .flush()
        .await
        .map_err(|e| AppError::Io(e, "flush repair".into()))?;

    match fs::rename(&temp_path, &final_path).await {
        Ok(_) => {}
        Err(e) => {
            warn!("Rename failed (will retry after mkdir): {}", e);
            let _ = fs::create_dir_all(&parent_dir).await;
            fs::rename(&temp_path, &final_path)
                .await
                .map_err(|e2| AppError::Io(e2, "rename repaired file".into()))?;
        }
    }

    info!(
        "Successfully repaired shard {} -> {}",
        target_shard.file_name,
        final_path.display()
    );
    Ok(())
}
