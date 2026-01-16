use crate::enums::app_error_enum::AppError;
use crate::object_storage::upload::DirectWriter;
use crate::structs::object_storage_structs::{PartMetadata, ShardInfo};
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

    info!(
        "Initializing Recovery Runtime on {} dedicated cores.",
        recovery_worker_count
    );

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(recovery_worker_count)
        .thread_name("n1-recovery")
        .on_thread_start(move || {
            if !pinned_cores_arc.is_empty() {
                let idx = next_core_idx.fetch_add(1, Ordering::Relaxed);
                let core = pinned_cores_arc[idx % pinned_cores_arc.len()];
                if core_affinity::set_for_current(core) {
                    info!("Recovery worker thread pinned to CPU core: {:?}", core);
                } else {
                    warn!(
                        "Failed to pin recovery worker thread to CPU core: {:?}",
                        core
                    );
                }
            }
        })
        .enable_all()
        .build()
        .expect("Failed to build dedicated recovery runtime")
});

async fn create_direct_file_for_repair(path: &PathBuf) -> Result<std::fs::File, AppError> {
    let path_clone = path.clone();
    tokio::task::spawn_blocking(move || {
        let mut options = StdOpenOptions::new();
        options.write(true).create_new(true);

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
        let _ = file.seek(SeekFrom::Start(original_pos)).await;
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
        Err(_) => {
            let _ = file.seek(SeekFrom::Start(original_pos)).await;
            return false;
        }
    };

    let _ = file.seek(SeekFrom::Start(original_pos)).await;

    calculated == expected
}

async fn get_or_open_shard(
    file_cache: &mut HashMap<String, File>,
    shard_info: &ShardInfo,
) -> Option<File> {
    if let Some(file) = file_cache.get(&shard_info.file_name) {
        return file.try_clone().await.ok();
    }

    if let Ok(mut f) = File::open(&shard_info.absolute_path).await {
        if verify_shard_integrity(&mut f, &shard_info.checksum).await {
            file_cache.insert(shard_info.file_name.clone(), f.try_clone().await.ok()?);
            return Some(f);
        }
    }

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

    let part_clone = Arc::clone(&part);
    let shard_index = target_shard.shard_index;

    RECOVERY_RUNTIME.spawn(async move {
        if let Err(e) = repair_entire_shard(part_clone, shard_index, block_size).await {
            warn!(
                "Background repair skipped or failed for shard {}: {}",
                shard_index, e
            );
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
        .map_err(|_| AppError::InternalServerError("Recovery semaphore error".into()))?;

    let target_shard = part
        .shards
        .get(target_shard_idx)
        .ok_or(AppError::InternalServerError(
            "Target shard not found".into(),
        ))?;

    let final_path = PathBuf::from(&target_shard.absolute_path);

    let parent_dir = final_path
        .parent()
        .ok_or(AppError::InternalServerError("Invalid shard path".into()))?;

    if !parent_dir.exists() {
        if let Err(e) = fs::create_dir_all(parent_dir).await {
            warn!(
                "Cannot create repair directory at {:?}: {}. Disk may be unmounted or read-only. Skipping repair.",
                parent_dir, e
            );
            return Ok(());
        }
    }

    let temp_path = parent_dir.join(format!("{}.recovering", target_shard.file_name));

    if final_path.exists() {
        if let Ok(mut f) = File::open(&final_path).await {
            if verify_shard_integrity(&mut f, &target_shard.checksum).await {
                return Ok(());
            }
        }
    }

    let std_file = match create_direct_file_for_repair(&temp_path).await {
        Ok(f) => f,
        Err(e) => {
            if let AppError::Io(ref io_err, _) = e {
                if io_err.kind() == std::io::ErrorKind::AlreadyExists {
                    return Ok(());
                }
            }

            if let AppError::Io(ref io_err, _) = e {
                if io_err.kind() == std::io::ErrorKind::NotFound
                    || io_err.kind() == std::io::ErrorKind::PermissionDenied
                {
                    warn!(
                        "Disk became unavailable during repair init: {}. Skipping.",
                        io_err
                    );
                    return Ok(());
                }
            }
            return Err(e);
        }
    };

    let total_shard_size = part.shards.first().map(|s| s.size_bytes).unwrap_or(0);
    let file_clone = std_file
        .try_clone()
        .map_err(|e| AppError::Io(e, "clone handle".into()))?;

    tokio::task::spawn_blocking(move || file_clone.set_len(total_shard_size))
        .await
        .map_err(|e| AppError::InternalServerError(format!("prealloc failed: {}", e)))?
        .map_err(|e| AppError::Io(e, "prealloc set_len".into()))?;

    let mut temp_file = DirectWriter::new(std_file);

    let data_shards: Vec<&ShardInfo> = part.shards.iter().filter(|s| !s.is_parity).collect();
    let data_count = data_shards.len();
    let parity_count = part.shards.len() - data_count;

    let mut peer_files: Vec<Option<File>> = Vec::with_capacity(part.shards.len());
    for s in &part.shards {
        if s.shard_index == target_shard_idx {
            peer_files.push(None);
        } else {
            let f = File::open(&s.absolute_path).await.ok();
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

    info!(
        "Successfully repaired shard {} via background task",
        target_shard.file_name
    );
    Ok(())
}
