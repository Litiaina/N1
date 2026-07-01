use crate::core::constants::{GC_SUFFIX, N2_BLOB_DIR};
use crate::core::disk_manager::{ObjectPathManager, PartMetadata, ShardInfo};
use crate::core::upload::{DirectWriter, sync_dir};
use crate::enums::app_error_enum::AppError;

use crc32fast::Hasher;
use simd_erasure_core::{ReedSolomonDecoder, ReedSolomonEncoder};
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions as StdOpenOptions;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Semaphore;
use tokio::time::{Duration, sleep};
use tracing::{info, warn};
use ulid::Ulid;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

static RECOVERY_SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(4));
static REPAIR_INFLIGHT: LazyLock<Mutex<HashSet<PathBuf>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

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
        .thread_name("n2-recovery")
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
    LazyLock::new(|| ObjectPathManager::new().get_storage_roots());

struct RepairInflightGuard {
    path: PathBuf,
}

impl Drop for RepairInflightGuard {
    fn drop(&mut self) {
        if let Ok(mut inflight) = REPAIR_INFLIGHT.lock() {
            inflight.remove(&self.path);
        }
    }
}

fn acquire_repair_inflight(path: &Path) -> Option<RepairInflightGuard> {
    let mut inflight = REPAIR_INFLIGHT.lock().ok()?;
    let path = path.to_path_buf();
    if !inflight.insert(path.clone()) {
        return None;
    }
    Some(RepairInflightGuard { path })
}

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

#[derive(Clone, Debug, Eq, PartialEq)]
struct ShardPathHint {
    fragment: String,
    object_id: String,
    version_id: String,
}

fn extract_shard_path_hint(abs: &str) -> Option<ShardPathHint> {
    if abs.is_empty() {
        return None;
    }
    let norm = abs.replace('\\', "/");
    let parts: Vec<&str> = norm.split('/').filter(|s| !s.is_empty()).collect();
    let blob_pos = parts.iter().position(|p| *p == N2_BLOB_DIR)?;
    if blob_pos == 0 || blob_pos + 6 >= parts.len() {
        return None;
    }
    let object_dir = parts[blob_pos + 5];
    let object_id = object_dir.strip_prefix("@o_")?;
    let version_id = parts[blob_pos + 6];
    Some(ShardPathHint {
        fragment: parts[blob_pos - 1].to_string(),
        object_id: object_id.to_string(),
        version_id: version_id.to_string(),
    })
}

fn parse_version_dir_hint(version_dir: &Path) -> Option<ShardPathHint> {
    let version_id = version_dir.file_name()?.to_string_lossy().to_string();
    let object_dir = version_dir.parent()?;
    let object_id = object_dir
        .file_name()?
        .to_string_lossy()
        .strip_prefix("@o_")?
        .to_string();
    let mut cursor = object_dir.parent()?;
    for _ in 0..4 {
        cursor = cursor.parent()?;
    }
    if cursor.file_name()?.to_string_lossy() != N2_BLOB_DIR {
        return None;
    }
    Some(ShardPathHint {
        fragment: cursor.parent()?.file_name()?.to_string_lossy().to_string(),
        object_id,
        version_id,
    })
}

fn shard_dir_on_root(disk_root: &Path, hint: &ShardPathHint) -> PathBuf {
    crate::core::disk_manager::ObjectPathManager::new().get_shard_dir_on_disk(
        &disk_root.to_path_buf(),
        &hint.fragment,
        &hint.object_id,
        &hint.version_id,
    )
}

fn owning_blob_root_for_path(path: &Path) -> Option<&'static PathBuf> {
    BLOB_ROOTS
        .iter()
        .filter(|root| path.starts_with(root.as_path()))
        .max_by_key(|root| root.as_os_str().len())
}

async fn should_schedule_background_repair(shard: &ShardInfo) -> bool {
    let stable = strip_tmp_attempt_suffix(&shard.absolute_path);
    let path = Path::new(stable);
    if let Some(parent) = path.parent() {
        if fs::try_exists(parent).await.unwrap_or(false) {
            return true;
        }
    }
    if let Some(root) = owning_blob_root_for_path(path) {
        return fs::try_exists(root).await.unwrap_or(false);
    }

    false
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

async fn try_open_unverified(path: &str) -> Option<File> {
    if path.is_empty() {
        return None;
    }

    File::open(path).await.ok()
}

async fn preexisting_gc_candidates(path: &str) -> Vec<String> {
    let path = Path::new(path);
    let Some(parent) = path.parent() else {
        return Vec::new();
    };
    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
        return Vec::new();
    };
    let prefix = format!("{file_name}.preexisting.");
    let mut out = Vec::new();
    let Ok(mut entries) = fs::read_dir(parent).await else {
        return out;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if name.starts_with(&prefix) && name.ends_with(GC_SUFFIX) {
            out.push(entry.path().to_string_lossy().to_string());
        }
    }
    out.sort();
    out
}

async fn get_or_open_shard_ha(
    file_cache: &mut HashMap<String, File>,
    shard_info: &ShardInfo,
    version_dir_hint: Option<&Path>,
) -> Option<File> {
    if let Some(file) = file_cache.get(&shard_info.file_name) {
        return file.try_clone().await.ok();
    }

    let mut candidates: Vec<String> = stable_abs_candidates(&shard_info.absolute_path);

    if let Some(vdir) = version_dir_hint {
        candidates.push(
            vdir.join(&shard_info.file_name)
                .to_string_lossy()
                .to_string(),
        );

        if let Some(hint) = parse_version_dir_hint(vdir) {
            for root in BLOB_ROOTS.iter() {
                let dir = shard_dir_on_root(root.as_path(), &hint);
                candidates.push(
                    dir.join(&shard_info.file_name)
                        .to_string_lossy()
                        .to_string(),
                );
            }
        }
    }

    if let Some(hint) = extract_shard_path_hint(&shard_info.absolute_path) {
        for root in BLOB_ROOTS.iter() {
            let dir = shard_dir_on_root(root.as_path(), &hint);
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
                file_cache.insert(shard_info.file_name.clone(), cloned);
                return Some(f);
            }
        }

        for preexisting in preexisting_gc_candidates(&cand).await {
            if !tried.insert(preexisting.clone()) {
                continue;
            }
            if let Some(f) = try_open_verified(&preexisting, &shard_info.checksum).await {
                if let Ok(cloned) = f.try_clone().await {
                    file_cache.insert(shard_info.file_name.clone(), cloned);
                    return Some(f);
                }
            }
        }
    }

    None
}

async fn get_or_open_shard_fast(
    file_cache: &mut HashMap<String, File>,
    shard_info: &ShardInfo,
    version_dir_hint: Option<&Path>,
) -> Option<File> {
    if let Some(file) = file_cache.get(&shard_info.file_name) {
        return file.try_clone().await.ok();
    }

    let mut candidates: Vec<String> = stable_abs_candidates(&shard_info.absolute_path);

    if let Some(vdir) = version_dir_hint {
        candidates.push(
            vdir.join(&shard_info.file_name)
                .to_string_lossy()
                .to_string(),
        );

        if let Some(hint) = parse_version_dir_hint(vdir) {
            for root in BLOB_ROOTS.iter() {
                let dir = shard_dir_on_root(root.as_path(), &hint);
                candidates.push(
                    dir.join(&shard_info.file_name)
                        .to_string_lossy()
                        .to_string(),
                );
            }
        }
    }

    if let Some(hint) = extract_shard_path_hint(&shard_info.absolute_path) {
        for root in BLOB_ROOTS.iter() {
            let dir = shard_dir_on_root(root.as_path(), &hint);
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

        if let Some(f) = try_open_unverified(&cand).await {
            if let Ok(cloned) = f.try_clone().await {
                file_cache.insert(shard_info.file_name.clone(), cloned);
                return Some(f);
            }
        }

        for preexisting in preexisting_gc_candidates(&cand).await {
            if !tried.insert(preexisting.clone()) {
                continue;
            }
            if let Some(f) = try_open_unverified(&preexisting).await {
                if let Ok(cloned) = f.try_clone().await {
                    file_cache.insert(shard_info.file_name.clone(), cloned);
                    return Some(f);
                }
            }
        }
    }

    None
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
    file_cache: &mut HashMap<String, File>,
    version_dir: &Path,
    part: Arc<PartMetadata>,
    shard_idx: usize,
    offset: u64,
    len: usize,
    block_size: usize,
) -> Result<Vec<u8>, AppError> {
    let mut buf = vec![0u8; len];

    let target_shard = nth_data_shard(&part, shard_idx)
        .ok_or(AppError::InternalServerError("Bad shard index".into()))?;

    let vdir_hint = if version_dir.as_os_str().is_empty() {
        None
    } else {
        Some(version_dir)
    };

    if let Some(mut file) = get_or_open_shard_ha(file_cache, target_shard, vdir_hint).await {
        if file.seek(SeekFrom::Start(offset)).await.is_ok() {
            if file.read_exact(&mut buf).await.is_ok() {
                return Ok(buf);
            }
        }
        warn!("Read failed for validated shard, attempting recovery.");
    }

    let data_count = data_shards_count(&part);
    let parity_count = part.shards.len().saturating_sub(data_count);

    if data_count == 0 {
        return Err(AppError::InternalServerError("No data shards".into()));
    }

    let recovery_len = if len % 2 == 0 { len } else { len + 1 };

    let mut decoder = ReedSolomonDecoder::new(data_count, parity_count, recovery_len)
        .map_err(|e| AppError::InternalServerError(e.to_string()))?;

    let mut shards_added = 0usize;

    for s in &part.shards {
        if s.shard_index == target_shard.shard_index {
            continue;
        }

        let mut file = match get_or_open_shard_ha(file_cache, s, vdir_hint).await {
            Some(f) => f,
            None => continue,
        };

        if file.seek(SeekFrom::Start(offset)).await.is_err() {
            continue;
        }

        let mut chunk = vec![0u8; recovery_len];
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
        .find(|(idx, _)| *idx == target_shard.shard_index)
        .ok_or(AppError::InternalServerError(
            "Reconstruction failed for target shard".into(),
        ))?
        .1
        .to_vec();

    if should_schedule_background_repair(target_shard).await {
        let part_clone = Arc::clone(&part);
        let target_idx = target_shard.shard_index;

        RECOVERY_RUNTIME.spawn(async move {
            if let Err(e) = repair_entire_shard(part_clone, target_idx, block_size).await {
                warn!("Background repair failed for shard {}: {}", target_idx, e);
            }
        });
    }

    let mut restored = restored;
    restored.truncate(len);
    Ok(restored)
}

pub async fn read_or_recover_chunk_streaming(
    file_cache: &mut HashMap<String, File>,
    recovery_file_cache: &mut HashMap<String, File>,
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

    if let Some(mut file) = get_or_open_shard_fast(file_cache, target_shard, vdir_hint).await {
        let mut buf = vec![0u8; len];
        if file.seek(SeekFrom::Start(offset)).await.is_ok()
            && file.read_exact(&mut buf).await.is_ok()
        {
            return Ok(buf);
        }
        file_cache.remove(&target_shard.file_name);
        warn!(
            "Fast stream read failed for shard {}; attempting verified recovery.",
            target_shard.file_name
        );
    }

    read_or_recover_chunk(
        recovery_file_cache,
        version_dir,
        part,
        shard_idx,
        offset,
        len,
        block_size,
    )
    .await
}

pub async fn repair_entire_shard(
    part: Arc<PartMetadata>,
    target_shard_idx: usize,
    block_size: usize,
) -> Result<(), AppError> {
    let target_shard = part
        .shards
        .iter()
        .find(|s| s.shard_index == target_shard_idx)
        .or_else(|| part.shards.get(target_shard_idx))
        .ok_or(AppError::InternalServerError(
            "Target shard not found in metadata".into(),
        ))?
        .clone();

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

    if let Some(root) = owning_blob_root_for_path(&final_path) {
        if !fs::try_exists(root).await.unwrap_or(false) {
            info!(
                "Skipping background repair for shard {} because storage device {} is unavailable",
                target_shard_idx,
                root.display()
            );
            return Ok(());
        }
    }

    if !fs::try_exists(&parent_dir).await.unwrap_or(false) {
        let _ = fs::create_dir_all(&parent_dir).await;
    }

    if fs::try_exists(&final_path).await.unwrap_or(false) {
        if let Ok(mut f) = File::open(&final_path).await {
            if verify_shard_integrity(&mut f, &target_shard.checksum).await {
                return Ok(());
            }
        }
    }

    let _repair_guard = loop {
        if let Some(guard) = acquire_repair_inflight(&final_path) {
            break guard;
        }
        if fs::try_exists(&final_path).await.unwrap_or(false) {
            if let Ok(mut f) = File::open(&final_path).await {
                if verify_shard_integrity(&mut f, &target_shard.checksum).await {
                    return Ok(());
                }
            }
        }
        sleep(Duration::from_millis(50)).await;
    };

    if fs::try_exists(&final_path).await.unwrap_or(false) {
        if let Ok(mut f) = File::open(&final_path).await {
            if verify_shard_integrity(&mut f, &target_shard.checksum).await {
                return Ok(());
            }
        }
    }

    let temp_path = parent_dir.join(format!(
        "{}.tmp.repair.{}",
        strip_tmp_attempt_suffix(&target_shard.file_name),
        Ulid::new().to_string()
    ));

    let _permit = RECOVERY_SEMAPHORE
        .acquire()
        .await
        .map_err(|_| AppError::InternalServerError("Semaphore error".into()))?;

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

    tokio::task::spawn_blocking(move || file_clone.set_len(total_shard_size))
        .await
        .map_err(|e| {
            AppError::InternalServerError(format!("Failed to preallocate repair file: {}", e))
        })?
        .map_err(|e| {
            let _ = std::fs::remove_file(&temp_path);
            AppError::Io(e, format!("set_len repair file {}", temp_path.display()))
        })?;

    let mut temp_file = DirectWriter::new(std_file);

    let data_count = data_shards_count(&part);
    let parity_count = part.shards.len().saturating_sub(data_count);

    if data_count == 0 {
        drop(temp_file);
        let _ = fs::remove_file(&temp_path).await;
        return Err(AppError::InternalServerError("No data shards".into()));
    }

    let mut peer_files: Vec<Option<File>> = Vec::with_capacity(part.shards.len());
    let mut peer_cache: HashMap<String, File> = HashMap::new();

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
            drop(temp_file);
            let _ = fs::remove_file(&temp_path).await;
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
            temp_file
                .write_all(data)
                .await
                .map_err(|e| AppError::Io(e, "write repair data".into()))?;
        } else {
            for (idx, data) in decoded.restored_original_iter() {
                if idx < data_count {
                    stripe_data[idx] = Some(data.to_vec());
                }
            }

            if stripe_data.iter().any(|d| d.is_none()) {
                drop(temp_file);
                let _ = fs::remove_file(&temp_path).await;
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

            temp_file
                .write_all(data)
                .await
                .map_err(|e| AppError::Io(e, "write repair parity".into()))?;
        }

        offset += cur as u64;
    }

    temp_file
        .flush()
        .await
        .map_err(|e| AppError::Io(e, "flush repair".into()))?;
    temp_file
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

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::disk_manager::{FS_STORAGE_ROOT, PartMetadata, STORAGE_DISKS, ShardInfo};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn crc32_hex(bytes: &[u8]) -> String {
        let mut hasher = Hasher::new();
        hasher.update(bytes);
        format!("{:08x}", hasher.finalize())
    }

    #[tokio::test]
    async fn read_or_recover_chunk_restores_single_data_shard_for_nine_byte_object() {
        let root = std::env::temp_dir().join(format!("n2-recovery-nine-byte-{}", Ulid::new()));
        let matrix_base = std::env::temp_dir().join(format!("n2-matrix-{}", std::process::id()));
        let index_roots = vec![
            matrix_base.join("index_1").to_string_lossy().to_string(),
            matrix_base.join("index_2").to_string_lossy().to_string(),
        ];
        let blob_roots = vec![
            matrix_base.join("blob_1").to_string_lossy().to_string(),
            matrix_base.join("blob_2").to_string_lossy().to_string(),
            matrix_base.join("blob_3").to_string_lossy().to_string(),
        ];
        for root in index_roots.iter().chain(blob_roots.iter()) {
            fs::create_dir_all(root).await.expect("create suite root");
        }
        if STORAGE_DISKS.get().is_none() {
            STORAGE_DISKS
                .set(index_roots)
                .expect("set suite index roots");
        }
        if STORAGE_DISKS.get().is_none() {
            STORAGE_DISKS.set(blob_roots).expect("set suite blob roots");
        }
        if FS_STORAGE_ROOT.get().is_none() {
            FS_STORAGE_ROOT
                .set(matrix_base.to_string_lossy().to_string())
                .expect("set suite fs root");
        }
        let object_id = "01KRECOVERYOBJECT0000000000";
        let version_id = "01KRECOVERYVERSION000000000";
        let parity_dir = crate::core::disk_manager::ObjectPathManager::new()
            .get_shard_dir_on_disk(&root, "frag", object_id, version_id);
        let missing_dir = crate::core::disk_manager::ObjectPathManager::new()
            .get_shard_dir_on_disk(&root.join("missing"), "frag", object_id, version_id);
        fs::create_dir_all(&parity_dir)
            .await
            .expect("create parity dir");

        let mut shard_bytes = vec![0u8; 262_144];
        shard_bytes[..9].copy_from_slice(b"trash-me\n");
        let checksum = crc32_hex(&shard_bytes);

        let parity_path = parity_dir.join("part_0_shard_1.parity");
        fs::write(&parity_path, &shard_bytes)
            .await
            .expect("write parity shard");

        let part = Arc::new(PartMetadata {
            part_index: 0,
            size_bytes: 9,
            created_at: 0,
            checksum: None,
            shards: vec![
                ShardInfo {
                    shard_index: 0,
                    file_name: "part_0_shard_0.shard".to_string(),
                    absolute_path: missing_dir
                        .join("part_0_shard_0.shard")
                        .to_string_lossy()
                        .to_string(),
                    size_bytes: 262_144,
                    checksum: checksum.clone(),
                    block_checksums: Vec::new(),
                    checksum_block_size: 0,
                    is_parity: false,
                },
                ShardInfo {
                    shard_index: 1,
                    file_name: "part_0_shard_1.parity".to_string(),
                    absolute_path: parity_path.to_string_lossy().to_string(),
                    size_bytes: 262_144,
                    checksum,
                    block_checksums: Vec::new(),
                    checksum_block_size: 0,
                    is_parity: true,
                },
            ],
        });

        let bytes =
            read_or_recover_chunk(&mut HashMap::new(), &missing_dir, part, 0, 0, 9, 262_144)
                .await
                .expect("recover missing shard");

        assert_eq!(bytes, b"trash-me\n");

        let bytes = read_or_recover_chunk_streaming(
            &mut HashMap::new(),
            &mut HashMap::new(),
            &missing_dir,
            Arc::new(PartMetadata {
                part_index: 0,
                size_bytes: 9,
                created_at: 0,
                checksum: None,
                shards: vec![
                    ShardInfo {
                        shard_index: 0,
                        file_name: "part_0_shard_0.shard".to_string(),
                        absolute_path: missing_dir
                            .join("part_0_shard_0.shard")
                            .to_string_lossy()
                            .to_string(),
                        size_bytes: 262_144,
                        checksum: crc32_hex(&shard_bytes),
                        block_checksums: Vec::new(),
                        checksum_block_size: 0,
                        is_parity: false,
                    },
                    ShardInfo {
                        shard_index: 1,
                        file_name: "part_0_shard_1.parity".to_string(),
                        absolute_path: parity_path.to_string_lossy().to_string(),
                        size_bytes: 262_144,
                        checksum: crc32_hex(&shard_bytes),
                        block_checksums: Vec::new(),
                        checksum_block_size: 0,
                        is_parity: true,
                    },
                ],
            }),
            0,
            0,
            9,
            262_144,
        )
        .await
        .expect("streaming fallback recovers missing shard");

        assert_eq!(bytes, b"trash-me\n");

        let _ = fs::remove_dir_all(&root).await;
    }
}
