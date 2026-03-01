use crate::enums::app_error_enum::AppError;
use crate::n1_core::blob_store::{read_shard_range, verify_shard_checksum, write_shard_range};
use crate::n1_core::constants::GC_SUFFIX;
use crate::n1_core::disk_manager::{ObjectPathManager, PartMetadata, ShardInfo};
use crate::n1_core::upload::{DirectWriter, sync_dir};

use crc32fast::Hasher;
use simd_erasure_core::{ReedSolomonDecoder, ReedSolomonEncoder};
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions as StdOpenOptions;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Semaphore;
use tracing::{info, warn};
use ulid::Ulid;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

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

static BLOB_ROOTS: LazyLock<Vec<PathBuf>> =
    LazyLock::new(|| ObjectPathManager::new().get_blob_roots());

pub enum CachedShardHandle {
    File(File),
    RawVerified,
}

pub type ShardFileCache = HashMap<String, CachedShardHandle>;

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
        out.push(stable.clone());
    }

    out.push(abs.to_string());

    if let Some(stripped) = abs.strip_suffix(".recovering") {
        out.push(stripped.to_string());
        out.push(strip_tmp_attempt_suffix(stripped).to_string());
    }

    if !stable.is_empty() {
        out.push(format!("{}{}", stable, GC_SUFFIX));
    }

    let mut seen = HashSet::<String>::new();
    out.retain(|p| seen.insert(p.clone()));
    out
}

fn extract_fragment_version_from_abs(abs: &str) -> Option<(String, String, bool)> {
    if abs.is_empty() {
        return None;
    }
    let norm = abs.replace('\\', "/");
    let parts: Vec<&str> = norm.split('/').filter(|s| !s.is_empty()).collect();
    let blob_pos = parts.iter().position(|p| *p == "blob")?;
    if blob_pos == 0 || blob_pos + 1 >= parts.len() {
        return None;
    }

    let is_trash = blob_pos >= 2 && parts[blob_pos - 1] == ".trash";
    let fragment = if is_trash {
        if blob_pos < 2 {
            return None;
        }
        parts[blob_pos - 2]
    } else {
        parts[blob_pos - 1]
    };

    let version_id = parts[blob_pos + 1];
    Some((fragment.to_string(), version_id.to_string(), is_trash))
}

fn parse_version_dir_hint(version_dir: &Path) -> Option<(String, String, bool)> {
    let version_id = version_dir.file_name()?.to_string_lossy().to_string();
    let blob_dir = version_dir.parent()?;
    if blob_dir.file_name()?.to_string_lossy() != "blob" {
        return None;
    }

    let up = blob_dir.parent()?;
    let name = up.file_name()?.to_string_lossy();

    if name == ".trash" {
        let frag = up.parent()?.file_name()?.to_string_lossy().to_string();
        return Some((frag, version_id, true));
    }

    Some((name.to_string(), version_id, false))
}

fn shard_dir_on_root(
    disk_root: &Path,
    fragment: &str,
    version_id: &str,
    is_trash: bool,
) -> PathBuf {
    if is_trash {
        disk_root
            .join(fragment)
            .join(".trash")
            .join("blob")
            .join(version_id)
    } else {
        disk_root.join(fragment).join("blob").join(version_id)
    }
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
        } else {
            warn!("Shard failed integrity check: {}", path);
        }
    }
    None
}

async fn get_or_open_shard_ha(
    file_cache: &mut ShardFileCache,
    shard_info: &ShardInfo,
    version_dir_hint: Option<&Path>,
) -> Option<File> {
    if shard_info.raw_blob_address.is_some() {
        return None;
    }

    if let Some(CachedShardHandle::File(file)) = file_cache.get(&shard_info.file_name) {
        return file.try_clone().await.ok();
    }

    let mut candidates: Vec<String> = stable_abs_candidates(&shard_info.absolute_path);

    if let Some(vdir) = version_dir_hint {
        candidates.push(
            vdir.join(&shard_info.file_name)
                .to_string_lossy()
                .to_string(),
        );

        if let Some((fragment, version_id, is_trash)) = parse_version_dir_hint(vdir) {
            for root in BLOB_ROOTS.iter() {
                let dir = shard_dir_on_root(root.as_path(), &fragment, &version_id, is_trash);
                candidates.push(
                    dir.join(&shard_info.file_name)
                        .to_string_lossy()
                        .to_string(),
                );
            }
        }
    }

    if let Some((fragment, version_id, is_trash)) =
        extract_fragment_version_from_abs(&shard_info.absolute_path)
    {
        for root in BLOB_ROOTS.iter() {
            let dir = shard_dir_on_root(root.as_path(), &fragment, &version_id, is_trash);
            candidates.push(
                dir.join(&shard_info.file_name)
                    .to_string_lossy()
                    .to_string(),
            );
        }
    }

    let mut tried = HashSet::<String>::new();
    for cand in candidates {
        if cand.is_empty() || !tried.insert(cand.clone()) {
            continue;
        }

        if let Some(f) = try_open_verified(&cand, &shard_info.checksum).await {
            if let Ok(cloned) = f.try_clone().await {
                file_cache.insert(
                    shard_info.file_name.clone(),
                    CachedShardHandle::File(cloned),
                );
                return Some(f);
            }
        }
    }

    None
}

async fn read_shard_chunk_cached(
    file_cache: &mut ShardFileCache,
    shard_info: &ShardInfo,
    version_dir_hint: Option<&Path>,
    offset: u64,
    len: usize,
) -> Option<Vec<u8>> {
    if shard_info.raw_blob_address.is_some() {
        let verified = matches!(
            file_cache.get(&shard_info.file_name),
            Some(CachedShardHandle::RawVerified)
        );
        if !verified {
            if !verify_shard_checksum(shard_info).await.ok()? {
                return None;
            }
            file_cache.insert(shard_info.file_name.clone(), CachedShardHandle::RawVerified);
        }
        return read_shard_range(shard_info, offset, len).await.ok();
    }

    let mut file = get_or_open_shard_ha(file_cache, shard_info, version_dir_hint).await?;
    if file.seek(SeekFrom::Start(offset)).await.is_err() {
        return None;
    }

    let mut chunk = vec![0u8; len];
    if file.read_exact(&mut chunk).await.is_err() {
        return None;
    }
    Some(chunk)
}

fn data_shards_count(part: &PartMetadata) -> usize {
    part.shards.iter().filter(|s| !s.is_parity).count()
}

fn nth_data_shard<'a>(part: &'a PartMetadata, nth: usize) -> Option<&'a ShardInfo> {
    let mut i = 0usize;
    for s in &part.shards {
        if s.is_parity {
            continue;
        }
        if i == nth {
            return Some(s);
        }
        i += 1;
    }
    None
}

pub async fn read_or_recover_chunk(
    file_cache: &mut ShardFileCache,
    version_dir: &Path,
    part: Arc<PartMetadata>,
    shard_idx: usize,
    offset: u64,
    len: usize,
    block_size: usize,
) -> Result<Vec<u8>, AppError> {
    let target_shard = nth_data_shard(&part, shard_idx)
        .ok_or(AppError::InternalServerError("Bad shard index".into()))?;

    let vdir_hint = if version_dir.as_os_str().is_empty() {
        None
    } else {
        Some(version_dir)
    };

    if let Some(chunk) =
        read_shard_chunk_cached(file_cache, target_shard, vdir_hint, offset, len).await
    {
        return Ok(chunk);
    }
    warn!("Read failed for validated shard, attempting recovery.");

    let data_count = data_shards_count(&part);
    let parity_count = part.shards.len().saturating_sub(data_count);

    if data_count == 0 {
        return Err(AppError::InternalServerError("No data shards".into()));
    }

    let mut decoder = ReedSolomonDecoder::new(data_count, parity_count, len)
        .map_err(|e| AppError::InternalServerError(e.to_string()))?;

    let mut shards_added = 0usize;

    for s in &part.shards {
        if s.shard_index == target_shard.shard_index {
            continue;
        }

        let Some(chunk) = read_shard_chunk_cached(file_cache, s, vdir_hint, offset, len).await
        else {
            continue;
        };

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
        .find(|(idx, _)| *idx == target_shard.shard_index)
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
        .iter()
        .find(|s| s.shard_index == target_shard_idx)
        .or_else(|| part.shards.get(target_shard_idx))
        .ok_or(AppError::InternalServerError(
            "Target shard not found in metadata".into(),
        ))?
        .clone();

    let total_shard_size = target_shard.size_bytes;
    let raw_target = target_shard.raw_blob_address.is_some();

    let (final_path, parent_dir, temp_path, mut temp_file) = if raw_target {
        if verify_shard_checksum(&target_shard).await? {
            return Ok(());
        }
        (PathBuf::new(), PathBuf::new(), PathBuf::new(), None)
    } else {
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

        let file_clone = std_file
            .try_clone()
            .map_err(|e| AppError::Io(e, "clone handle".into()))?;

        tokio::task::spawn_blocking(move || file_clone.set_len(total_shard_size))
            .await
            .map_err(|e| {
                AppError::InternalServerError(format!("Failed to preallocate repair file: {}", e))
            })?
            .map_err(|e| {
                let _ = std::fs::remove_file(&temp_path);
                AppError::Io(e, format!("set_len repair file {}", temp_path.display()))
            })?;

        let temp_file = Some(DirectWriter::new(std_file));
        (final_path, parent_dir, temp_path, temp_file)
    };

    let data_count = data_shards_count(&part);
    let parity_count = part.shards.len().saturating_sub(data_count);

    if data_count == 0 {
        drop(temp_file);
        let _ = fs::remove_file(&temp_path).await;
        return Err(AppError::InternalServerError("No data shards".into()));
    }

    let mut peer_files: Vec<Option<File>> = Vec::with_capacity(part.shards.len());
    let mut peer_cache: ShardFileCache = HashMap::new();

    for s in &part.shards {
        if s.shard_index == target_shard.shard_index {
            peer_files.push(None);
        } else {
            peer_files.push(get_or_open_shard_ha(&mut peer_cache, s, None).await);
        }
    }

    let mut offset = 0u64;
    let target_is_parity = target_shard.shard_index >= data_count;
    while offset < total_shard_size {
        let remaining = (total_shard_size - offset) as usize;
        let cur = remaining.min(block_size);

        let mut decoder = ReedSolomonDecoder::new(data_count, parity_count, cur)
            .map_err(|e| AppError::InternalServerError(format!("RS Init: {}", e)))?;

        let mut stripe_data: Vec<Option<Vec<u8>>> = vec![None; data_count];
        let mut shards_found = 0usize;

        for (idx, file_opt) in peer_files.iter_mut().enumerate() {
            let s_info = &part.shards[idx];

            if s_info.shard_index == target_shard.shard_index {
                continue;
            }

            if s_info.raw_blob_address.is_some() {
                if let Ok(buf) = read_shard_range(s_info, offset, cur).await {
                    let res = if s_info.is_parity {
                        decoder.add_recovery_shard(s_info.shard_index - data_count, &buf)
                    } else {
                        let add = decoder.add_original_shard(s_info.shard_index, &buf);
                        if add.is_ok() && target_is_parity && s_info.shard_index < data_count {
                            stripe_data[s_info.shard_index] = Some(buf);
                        }
                        add
                    };

                    if res.is_ok() {
                        shards_found += 1;
                    }
                }
                continue;
            }

            let mut need_reopen = false;

            if let Some(f) = file_opt {
                if f.seek(SeekFrom::Start(offset)).await.is_err() {
                    need_reopen = true;
                } else {
                    let mut buf = vec![0u8; cur];
                    if f.read_exact(&mut buf).await.is_ok() {
                        let res = if s_info.is_parity {
                            decoder.add_recovery_shard(s_info.shard_index - data_count, &buf)
                        } else {
                            let add = decoder.add_original_shard(s_info.shard_index, &buf);
                            if add.is_ok() && target_is_parity && s_info.shard_index < data_count {
                                stripe_data[s_info.shard_index] = Some(buf);
                            }
                            add
                        };

                        if res.is_ok() {
                            shards_found += 1;
                        }
                    } else {
                        need_reopen = true;
                    }
                }
            } else {
                need_reopen = true;
            }

            if need_reopen {
                *file_opt = None;
            }
        }

        if shards_found < data_count {
            drop(temp_file.take());
            if !raw_target {
                let _ = fs::remove_file(&temp_path).await;
            }
            return Err(AppError::InternalServerError(
                "Not enough healthy shards to repair on disk".into(),
            ));
        }

        let decoded = decoder
            .decode()
            .map_err(|e| AppError::InternalServerError(format!("RS Decode: {}", e)))?;

        if target_shard.shard_index < data_count {
            for (idx, data) in decoded.restored_original_iter() {
                if idx < data_count {
                    stripe_data[idx] = Some(data.to_vec());
                }
            }
            let data = stripe_data[target_shard.shard_index]
                .as_ref()
                .ok_or_else(|| {
                    AppError::InternalServerError("Missing restored data shard".into())
                })?;
            if raw_target {
                write_shard_range(&target_shard, offset, data).await?;
            } else if let Some(writer) = temp_file.as_mut() {
                writer
                    .write_all(data)
                    .await
                    .map_err(|e| AppError::Io(e, "write repair data".into()))?;
            }
        } else {
            for (idx, data) in decoded.restored_original_iter() {
                if idx < data_count {
                    stripe_data[idx] = Some(data.to_vec());
                }
            }

            if stripe_data.iter().any(|d| d.is_none()) {
                drop(temp_file.take());
                if !raw_target {
                    let _ = fs::remove_file(&temp_path).await;
                }
                return Err(AppError::InternalServerError(
                    "Missing data shards required to recompute parity".into(),
                ));
            }

            let mut encoder = ReedSolomonEncoder::new(data_count, parity_count, cur)
                .map_err(|e| AppError::InternalServerError(format!("RS Enc Init: {}", e)))?;

            for data in stripe_data.iter().flatten() {
                encoder
                    .add_original_shard(data)
                    .map_err(|e| AppError::InternalServerError(format!("RS Add: {}", e)))?;
            }

            let enc = encoder
                .encode()
                .map_err(|e| AppError::InternalServerError(format!("RS Encode: {}", e)))?;

            let ridx = target_shard.shard_index - data_count;
            let data = enc
                .recovery(ridx)
                .ok_or_else(|| AppError::InternalServerError("Missing parity output".into()))?;

            if raw_target {
                write_shard_range(&target_shard, offset, data).await?;
            } else if let Some(writer) = temp_file.as_mut() {
                writer
                    .write_all(data)
                    .await
                    .map_err(|e| AppError::Io(e, "write repair parity".into()))?;
            }
        }

        offset += cur as u64;
    }

    if raw_target {
        if !verify_shard_checksum(&target_shard).await? {
            return Err(AppError::InternalServerError(
                "Repaired raw shard failed checksum verification".into(),
            ));
        }
        info!("Successfully repaired raw shard {}", target_shard.file_name);
    } else {
        let mut writer = temp_file
            .take()
            .ok_or_else(|| AppError::InternalServerError("repair temp writer missing".into()))?;
        writer
            .flush()
            .await
            .map_err(|e| AppError::Io(e, "flush repair".into()))?;
        writer
            .sync_data()
            .await
            .map_err(|e| AppError::Io(e, "fsync repair".into()))?;

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
        let _ = sync_dir(&parent_dir).await;

        info!(
            "Successfully repaired shard {} -> {}",
            target_shard.file_name,
            final_path.display()
        );
    }

    Ok(())
}
