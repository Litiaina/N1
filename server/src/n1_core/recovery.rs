use crate::enums::app_error_enum::AppError;
use crate::n1_core::blob_store::{
    commit_staged_raw_shard, configured_blob_disk_count, free_raw_address, open_raw_direct_io,
    raw_blob_debug_path, raw_stage_path_for_shard, verify_shard_checksum, write_shard_range,
};
use crate::n1_core::constants::GC_SUFFIX;
use crate::n1_core::disk_manager::{
    ObjectMetadata, ObjectPathManager, PartMetadata, RawBlobAddress, ShardInfo,
};
use crate::n1_core::index;
use crate::n1_core::raw_io_ring::{RawReadCompletion, RawReadRing, RawReadSubmitError};
use crate::n1_core::upload::{DirectWriter, sync_dir};

use crc32fast::Hasher;
use futures_util::future::{BoxFuture, FutureExt};
use futures_util::stream::{FuturesUnordered, StreamExt};
use once_cell::sync::OnceCell;
use simd_erasure_core::{ReedSolomonDecoder, ReedSolomonEncoder};
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions as StdOpenOptions;
use std::io;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex as StdMutex};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};
use ulid::Ulid;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

static RECOVERY_SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(4));

static BLOB_ROOTS: LazyLock<Vec<PathBuf>> =
    LazyLock::new(|| ObjectPathManager::new().get_blob_roots());
static INFLIGHT_REPAIRS: LazyLock<StdMutex<HashSet<String>>> =
    LazyLock::new(|| StdMutex::new(HashSet::new()));
static RAW_RELOCATIONS: LazyLock<StdMutex<HashMap<String, RawBlobAddress>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));
pub static RAW_VERIFY_ON_READ_ATOMIC: OnceCell<AtomicBool> = OnceCell::new();

const MAX_RAW_RELOCATIONS: usize = 8192;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RepairLogMode {
    Verbose,
    Summary,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RepairShardOutcome {
    pub relocated_raw_shard: bool,
    pub stale_mapping_relocated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShardRepairContext {
    pub fragment: String,
    pub object_key: String,
    pub version_id: String,
    pub is_trash: bool,
}

impl ShardRepairContext {
    pub fn new(fragment: String, object_key: String, version_id: String, is_trash: bool) -> Self {
        Self {
            fragment,
            object_key,
            version_id,
            is_trash,
        }
    }
}

enum RepairTarget {
    RawInPlace,
    TempFile {
        final_path: PathBuf,
        parent_dir: PathBuf,
        temp_path: PathBuf,
        writer: DirectWriter,
        raw_commit_disk_idx: Option<usize>,
    },
}

enum RelocatedRawMetadataOutcome {
    Updated,
    AlreadyRelocated(RawBlobAddress),
}

pub enum CachedShardHandle {
    File(File),
    Raw {
        ring: Arc<RawReadRing>,
        raw_blob_address: RawBlobAddress,
        verified: bool,
    },
}

pub type ShardFileCache = HashMap<String, CachedShardHandle>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RawReadValidationMode {
    Fast,
    Verified,
}

struct DecoderBufferPool {
    peer_buffers: Vec<Vec<u8>>,
    stripe_data: Vec<Vec<u8>>,
    stripe_present: Vec<bool>,
    restored_target: Vec<u8>,
}

impl DecoderBufferPool {
    fn new(data_count: usize) -> Self {
        Self {
            peer_buffers: Vec::new(),
            stripe_data: (0..data_count).map(|_| Vec::new()).collect(),
            stripe_present: vec![false; data_count],
            restored_target: Vec::new(),
        }
    }

    fn take_peer_buffer(&mut self, len: usize) -> Vec<u8> {
        let mut buffer = self.peer_buffers.pop().unwrap_or_default();
        buffer.clear();
        buffer.resize(len, 0);
        buffer
    }

    fn recycle_peer_buffer(&mut self, mut buffer: Vec<u8>) {
        buffer.clear();
        self.peer_buffers.push(buffer);
    }

    fn reset_for_stripe(&mut self, target_is_parity: bool) {
        self.restored_target.clear();
        if target_is_parity {
            self.stripe_present.fill(false);
            for shard in &mut self.stripe_data {
                shard.clear();
            }
        }
    }

    fn store_data_shard(&mut self, idx: usize, data: &[u8]) {
        let slot = &mut self.stripe_data[idx];
        slot.clear();
        slot.extend_from_slice(data);
        self.stripe_present[idx] = true;
    }
}

#[derive(Clone)]
struct RawPeerReadTask {
    ring: Arc<RawReadRing>,
    shard_index: usize,
    is_parity: bool,
    file_name: String,
    absolute_path: String,
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

fn repair_job_key(
    part: &PartMetadata,
    target_shard: &ShardInfo,
    repair_context: Option<&ShardRepairContext>,
) -> String {
    if let Some(ctx) = repair_context {
        return format!(
            "{}:{}:{}:{}:{}:{}",
            ctx.fragment,
            ctx.object_key,
            ctx.version_id,
            ctx.is_trash,
            part.part_index,
            target_shard.shard_index
        );
    }

    format!(
        "{}:{}:{}",
        part.part_index, target_shard.shard_index, target_shard.absolute_path
    )
}

fn raw_address_key(raw: &RawBlobAddress) -> String {
    format!(
        "{}:{}:{}:{}",
        raw.device_id, raw.allocation_id, raw.offset_bytes, raw.size_bytes
    )
}

fn remember_raw_relocation(old_raw: &RawBlobAddress, new_raw: &RawBlobAddress) {
    let Ok(mut relocations) = RAW_RELOCATIONS.lock() else {
        return;
    };
    if relocations.len() >= MAX_RAW_RELOCATIONS {
        relocations.clear();
    }
    relocations.insert(raw_address_key(old_raw), new_raw.clone());
}

fn resolve_raw_relocation(raw: &RawBlobAddress) -> Option<RawBlobAddress> {
    let Ok(relocations) = RAW_RELOCATIONS.lock() else {
        return None;
    };

    let mut current = raw.clone();
    let mut changed = false;
    for _ in 0..8 {
        let Some(next) = relocations.get(&raw_address_key(&current)).cloned() else {
            break;
        };
        if next == current {
            break;
        }
        current = next;
        changed = true;
    }

    changed.then_some(current)
}

fn remap_raw_shard_info(shard_info: &ShardInfo) -> Option<ShardInfo> {
    let raw = shard_info.raw_blob_address.as_ref()?;
    let remapped = resolve_raw_relocation(raw)?;
    let mut relocated = shard_info.clone();
    relocated.raw_blob_address = Some(remapped.clone());
    relocated.absolute_path = raw_blob_debug_path(&remapped);
    Some(relocated)
}

fn register_inflight_repair(key: &str) -> bool {
    let Ok(mut inflight) = INFLIGHT_REPAIRS.lock() else {
        return true;
    };
    inflight.insert(key.to_string())
}

fn raw_verify_on_read_enabled() -> bool {
    RAW_VERIFY_ON_READ_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(false)
}

fn unregister_inflight_repair(key: &str) {
    if let Ok(mut inflight) = INFLIGHT_REPAIRS.lock() {
        inflight.remove(key);
    }
}

async fn load_version_metadata_for_repair(
    ctx: &ShardRepairContext,
) -> Result<ObjectMetadata, AppError> {
    if ctx.is_trash {
        index::read_trashed_object_version(&ctx.fragment, &ctx.object_key, &ctx.version_id).await
    } else {
        index::read_object_version(&ctx.fragment, &ctx.object_key, &ctx.version_id).await
    }
}

async fn persist_version_metadata_for_repair(
    ctx: &ShardRepairContext,
    metadata: &ObjectMetadata,
) -> Result<(), AppError> {
    if ctx.is_trash {
        index::upsert_trashed_object_version(metadata).await
    } else {
        index::upsert_object_version(metadata).await
    }
}

async fn persist_relocated_raw_shard_metadata(
    ctx: &ShardRepairContext,
    part_index: u64,
    shard_index: usize,
    old_raw: Option<&RawBlobAddress>,
    new_raw: &RawBlobAddress,
) -> Result<RelocatedRawMetadataOutcome, AppError> {
    let mut metadata = load_version_metadata_for_repair(ctx).await?;
    let part = metadata
        .parts
        .iter_mut()
        .find(|part| part.part_index == part_index)
        .ok_or_else(|| {
            AppError::InternalServerError(format!(
                "Failed to locate part {} in repaired metadata",
                part_index
            ))
        })?;
    let shard = part
        .shards
        .iter_mut()
        .find(|shard| shard.shard_index == shard_index)
        .ok_or_else(|| {
            AppError::InternalServerError(format!(
                "Failed to locate shard {} in repaired metadata",
                shard_index
            ))
        })?;

    if let Some(old) = old_raw {
        match shard.raw_blob_address.as_ref() {
            Some(current) if current == old => {}
            Some(current) => {
                return Ok(RelocatedRawMetadataOutcome::AlreadyRelocated(
                    current.clone(),
                ));
            }
            None => {}
        }
    } else if shard.raw_blob_address.as_ref() == Some(new_raw) {
        return Ok(RelocatedRawMetadataOutcome::AlreadyRelocated(
            new_raw.clone(),
        ));
    }

    shard.raw_blob_address = Some(new_raw.clone());
    shard.absolute_path = raw_blob_debug_path(new_raw);
    persist_version_metadata_for_repair(ctx, &metadata).await?;
    Ok(RelocatedRawMetadataOutcome::Updated)
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

fn cached_raw_handle(
    file_cache: &ShardFileCache,
    shard_info: &ShardInfo,
) -> Option<(Arc<RawReadRing>, bool)> {
    let expected_raw = shard_info.raw_blob_address.as_ref()?;
    let CachedShardHandle::Raw {
        ring,
        raw_blob_address,
        verified,
    } = file_cache.get(&shard_info.file_name)?
    else {
        return None;
    };
    if raw_blob_address != expected_raw {
        return None;
    }
    Some((Arc::clone(ring), *verified))
}

async fn get_or_open_raw_shard(
    file_cache: &mut ShardFileCache,
    shard_info: &ShardInfo,
    validation_mode: RawReadValidationMode,
) -> Option<Arc<RawReadRing>> {
    if let Some((ring, verified)) = cached_raw_handle(file_cache, shard_info) {
        if validation_mode == RawReadValidationMode::Verified && !verified {
            if !verify_shard_checksum(shard_info).await.ok()? {
                return None;
            }
            file_cache.insert(
                shard_info.file_name.clone(),
                CachedShardHandle::Raw {
                    ring: Arc::clone(&ring),
                    raw_blob_address: shard_info.raw_blob_address.clone()?,
                    verified: true,
                },
            );
        }
        return Some(ring);
    }

    if validation_mode == RawReadValidationMode::Verified
        && !verify_shard_checksum(shard_info).await.ok()?
    {
        return None;
    }

    let io = open_raw_direct_io(shard_info).ok()?;
    let ring = Arc::new(RawReadRing::new(io, 4));
    file_cache.insert(
        shard_info.file_name.clone(),
        CachedShardHandle::Raw {
            ring: Arc::clone(&ring),
            raw_blob_address: shard_info.raw_blob_address.clone()?,
            verified: validation_mode == RawReadValidationMode::Verified,
        },
    );
    Some(ring)
}

async fn submit_raw_read_via_ring(
    ring: &Arc<RawReadRing>,
    offset: u64,
    buffer: Vec<u8>,
) -> Result<u64, (io::Error, Vec<u8>)> {
    match ring.submit_read(offset, buffer) {
        Ok(submission_id) => Ok(submission_id),
        Err(RawReadSubmitError::Full(returned)) => Err((
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "raw read ring submission queue is full",
            ),
            returned,
        )),
        Err(RawReadSubmitError::Closed(returned)) => Err((
            io::Error::new(io::ErrorKind::BrokenPipe, "raw read ring is closed"),
            returned,
        )),
    }
}

async fn await_raw_peer_task(
    task: RawPeerReadTask,
    submission_id: u64,
) -> (RawPeerReadTask, Result<Vec<u8>, (io::Error, Vec<u8>)>) {
    let result = match task.ring.wait_for_completion(submission_id).await {
        RawReadCompletion::ReadOk { buffer, .. } => Ok(buffer),
        RawReadCompletion::ReadErr { buffer, err, .. } => Err((err, buffer.unwrap_or_default())),
    };
    (task, result)
}

fn apply_decoder_shard(
    decoder: &mut ReedSolomonDecoder,
    data_count: usize,
    shard_index: usize,
    is_parity: bool,
    buffer: &[u8],
) -> bool {
    let result = if is_parity {
        decoder.add_recovery_shard(shard_index - data_count, buffer)
    } else {
        decoder.add_original_shard(shard_index, buffer)
    };
    result.is_ok()
}

async fn prime_raw_peer_reads(
    pending_raw: &mut FuturesUnordered<
        BoxFuture<'static, (RawPeerReadTask, Result<Vec<u8>, (io::Error, Vec<u8>)>)>,
    >,
    remaining_raw: &mut std::vec::IntoIter<RawPeerReadTask>,
    buffer_pool: &mut DecoderBufferPool,
    offset: u64,
    len: usize,
    inflight_limit: usize,
) {
    while pending_raw.len() < inflight_limit {
        let Some(task) = remaining_raw.next() else {
            break;
        };
        let buffer = buffer_pool.take_peer_buffer(len);
        match submit_raw_read_via_ring(&task.ring, offset, buffer).await {
            Ok(submission_id) => pending_raw.push(await_raw_peer_task(task, submission_id).boxed()),
            Err((_err, buffer)) => buffer_pool.recycle_peer_buffer(buffer),
        }
    }
}

async fn read_raw_chunk_via_ring(
    ring: &Arc<RawReadRing>,
    offset: u64,
    len: usize,
    out: &mut Vec<u8>,
) -> Result<(), (io::Error, Vec<u8>)> {
    let mut buffer = std::mem::take(out);
    buffer.clear();
    buffer.resize(len, 0);
    let submission_id = submit_raw_read_via_ring(ring, offset, buffer).await?;
    match ring.wait_for_completion(submission_id).await {
        RawReadCompletion::ReadOk { buffer: buf, .. } => {
            *out = buf;
            Ok(())
        }
        RawReadCompletion::ReadErr {
            buffer: buf, err, ..
        } => Err((err, buf.unwrap_or_default())),
    }
}

async fn read_shard_chunk_cached_into(
    file_cache: &mut ShardFileCache,
    shard_info: &ShardInfo,
    version_dir_hint: Option<&Path>,
    offset: u64,
    len: usize,
    validation_mode: RawReadValidationMode,
    out: &mut Vec<u8>,
) -> Option<()> {
    let remapped_raw_shard;
    let effective_shard = if let Some(remapped) = remap_raw_shard_info(shard_info) {
        remapped_raw_shard = Some(remapped);
        remapped_raw_shard.as_ref().expect("remapped raw shard")
    } else {
        shard_info
    };

    if effective_shard.raw_blob_address.is_some() {
        let ring = get_or_open_raw_shard(file_cache, effective_shard, validation_mode).await?;
        return match read_raw_chunk_via_ring(&ring, offset, len, out).await {
            Ok(()) => Some(()),
            Err((err, returned)) => {
                *out = returned;
                warn!(
                    "Raw shard read failed file={} path={} offset={} len={} err={}",
                    effective_shard.file_name, effective_shard.absolute_path, offset, len, err
                );
                None
            }
        };
    }

    let mut file = get_or_open_shard_ha(file_cache, shard_info, version_dir_hint).await?;
    if file.seek(SeekFrom::Start(offset)).await.is_err() {
        return None;
    }

    out.clear();
    out.resize(len, 0);
    if file.read_exact(out).await.is_err() {
        return None;
    }
    Some(())
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

pub async fn read_or_recover_chunk_into(
    file_cache: &mut ShardFileCache,
    version_dir: &Path,
    part: Arc<PartMetadata>,
    shard_idx: usize,
    offset: u64,
    len: usize,
    block_size: usize,
    repair_context: Option<ShardRepairContext>,
    out: &mut Vec<u8>,
) -> Result<(), AppError> {
    let target_shard = nth_data_shard(&part, shard_idx)
        .ok_or(AppError::InternalServerError("Bad shard index".into()))?;

    let vdir_hint = if version_dir.as_os_str().is_empty() {
        None
    } else {
        Some(version_dir)
    };
    let target_validation_mode = if raw_verify_on_read_enabled() {
        RawReadValidationMode::Verified
    } else {
        RawReadValidationMode::Fast
    };

    if read_shard_chunk_cached_into(
        file_cache,
        target_shard,
        vdir_hint,
        offset,
        len,
        target_validation_mode,
        out,
    )
    .await
    .is_some()
    {
        return Ok(());
    }
    let data_count = data_shards_count(&part);
    let parity_count = part.shards.len().saturating_sub(data_count);
    let recovery_len = if len % 2 == 0 { len } else { len + 1 };

    if data_count == 0 {
        return Err(AppError::InternalServerError("No data shards".into()));
    }

    if parity_count == 0 {
        return Err(AppError::InternalServerError(format!(
            "Read failed for validated shard {} and no parity shards are available",
            target_shard.file_name
        )));
    }

    warn!("Read failed for validated shard, attempting recovery.");

    let mut decoder = ReedSolomonDecoder::new(data_count, parity_count, recovery_len)
        .map_err(|e| AppError::InternalServerError(e.to_string()))?;

    let mut shards_added = 0usize;
    let mut buffer_pool = DecoderBufferPool::new(data_count);
    let mut file_peers = Vec::new();
    let mut raw_peers = Vec::new();

    for s in &part.shards {
        if s.shard_index == target_shard.shard_index {
            continue;
        }

        if s.raw_blob_address.is_some() {
            let Some(ring) =
                get_or_open_raw_shard(file_cache, s, RawReadValidationMode::Verified).await
            else {
                continue;
            };
            raw_peers.push(RawPeerReadTask {
                ring,
                shard_index: s.shard_index,
                is_parity: s.is_parity,
                file_name: s.file_name.clone(),
                absolute_path: s.absolute_path.clone(),
            });
        } else {
            file_peers.push(s.clone());
        }
    }

    let mut pending_raw: FuturesUnordered<
        BoxFuture<'static, (RawPeerReadTask, Result<Vec<u8>, (io::Error, Vec<u8>)>)>,
    > = FuturesUnordered::new();
    let mut remaining_raw = raw_peers.into_iter();
    let raw_inflight_limit = data_count.clamp(1, 4);

    prime_raw_peer_reads(
        &mut pending_raw,
        &mut remaining_raw,
        &mut buffer_pool,
        offset,
        recovery_len,
        raw_inflight_limit,
    )
    .await;

    for s in &file_peers {
        if shards_added >= data_count {
            break;
        }

        let mut peer_scratch = buffer_pool.take_peer_buffer(recovery_len);
        let read_ok = read_shard_chunk_cached_into(
            file_cache,
            s,
            vdir_hint,
            offset,
            recovery_len,
            RawReadValidationMode::Verified,
            &mut peer_scratch,
        )
        .await
        .is_some();

        if read_ok {
            if apply_decoder_shard(
                &mut decoder,
                data_count,
                s.shard_index,
                s.is_parity,
                &peer_scratch,
            ) {
                shards_added += 1;
            }
        }

        buffer_pool.recycle_peer_buffer(peer_scratch);
    }

    while shards_added < data_count {
        let Some((task, result)) = pending_raw.next().await else {
            break;
        };

        match result {
            Ok(buffer) => {
                if apply_decoder_shard(
                    &mut decoder,
                    data_count,
                    task.shard_index,
                    task.is_parity,
                    &buffer,
                ) {
                    shards_added += 1;
                }
                buffer_pool.recycle_peer_buffer(buffer);
            }
            Err((err, buffer)) => {
                warn!(
                    "Raw shard read failed file={} path={} offset={} len={} err={}",
                    task.file_name, task.absolute_path, offset, recovery_len, err
                );
                buffer_pool.recycle_peer_buffer(buffer);
            }
        }

        prime_raw_peer_reads(
            &mut pending_raw,
            &mut remaining_raw,
            &mut buffer_pool,
            offset,
            recovery_len,
            raw_inflight_limit,
        )
        .await;
    }

    if shards_added < data_count {
        return Err(AppError::InternalServerError(format!(
            "Insufficient shards to recover data. Need {}, found {}.",
            data_count, shards_added
        )));
    }

    let decoded = decoder
        .decode()
        .map_err(|e| AppError::InternalServerError(e.to_string()))?;
    let restored = decoded
        .restored_original_iter()
        .find(|(idx, _)| *idx == target_shard.shard_index)
        .ok_or(AppError::InternalServerError(
            "Reconstruction failed for target shard".into(),
        ))?
        .1;

    out.clear();
    out.extend_from_slice(&restored[..len]);

    let part_clone = Arc::clone(&part);
    let target_idx = target_shard.shard_index;
    let repair_key = repair_job_key(&part, target_shard, repair_context.as_ref());

    if register_inflight_repair(&repair_key) {
        if let Err(e) = repair_entire_shard(
            part_clone,
            target_idx,
            block_size,
            repair_context.clone(),
            RepairLogMode::Verbose,
        )
        .await
        {
            warn!("Read-triggered repair failed for shard {}: {}", target_idx, e);
        }
        unregister_inflight_repair(&repair_key);
    }

    Ok(())
}

pub async fn repair_entire_shard(
    part: Arc<PartMetadata>,
    target_shard_idx: usize,
    block_size: usize,
    repair_context: Option<ShardRepairContext>,
    log_mode: RepairLogMode,
) -> Result<RepairShardOutcome, AppError> {
    let _permit = RECOVERY_SEMAPHORE
        .acquire()
        .await
        .map_err(|_| AppError::InternalServerError("Semaphore error".into()))?;

    let mut target_shard = part
        .shards
        .iter()
        .find(|s| s.shard_index == target_shard_idx)
        .or_else(|| part.shards.get(target_shard_idx))
        .ok_or(AppError::InternalServerError(
            "Target shard not found in metadata".into(),
        ))?
        .clone();

    if let Some(remapped) = remap_raw_shard_info(&target_shard) {
        target_shard = remapped;
    }

    let total_shard_size = target_shard.size_bytes;
    let raw_target = target_shard.raw_blob_address.is_some();
    let mut outcome = RepairShardOutcome::default();

    let mut repair_target = if raw_target {
        match verify_shard_checksum(&target_shard).await {
            Ok(true) => return Ok(outcome),
            Ok(false) => RepairTarget::RawInPlace,
            Err(err) => {
                let Some(ctx) = repair_context.as_ref() else {
                    return Err(err);
                };
                let disk_count = configured_blob_disk_count()?;
                if disk_count == 0 {
                    return Err(AppError::InternalServerError(
                        "No blob storage disks configured".into(),
                    ));
                }
                let disk_idx = (part.part_index as usize + target_shard.shard_index) % disk_count;
                let attempt_id = Ulid::new().to_string();
                let temp_path = raw_stage_path_for_shard(
                    disk_idx,
                    ctx.fragment.as_str(),
                    ctx.version_id.as_str(),
                    attempt_id.as_str(),
                    strip_tmp_attempt_suffix(&target_shard.file_name),
                )
                .await?;
                let parent_dir = temp_path.parent().map(Path::to_path_buf).ok_or(
                    AppError::InternalServerError("Invalid staged raw repair path".into()),
                )?;
                let std_file = match create_direct_file_for_repair(&temp_path).await {
                    Ok(f) => f,
                    Err(e) => {
                        warn!("Failed to create staged raw repair file: {}", e);
                        return Err(e);
                    }
                };
                let file_clone = std_file
                    .try_clone()
                    .map_err(|e| AppError::Io(e, "clone staged repair handle".into()))?;
                let temp_path_for_set_len = temp_path.clone();
                tokio::task::spawn_blocking(move || file_clone.set_len(total_shard_size))
                    .await
                    .map_err(|e| {
                        AppError::InternalServerError(format!(
                            "Failed to preallocate staged raw repair file: {}",
                            e
                        ))
                    })?
                    .map_err(|e| {
                        let _ = std::fs::remove_file(&temp_path_for_set_len);
                        AppError::Io(
                            e,
                            format!(
                                "set_len staged raw repair file {}",
                                temp_path_for_set_len.display()
                            ),
                        )
                    })?;
                match log_mode {
                    RepairLogMode::Verbose => warn!(
                        "Relocating raw shard {} after stale or missing raw device mapping: {}",
                        target_shard.file_name, err
                    ),
                    RepairLogMode::Summary => debug!(
                        "Relocating raw shard {} after stale or missing raw device mapping: {}",
                        target_shard.file_name, err
                    ),
                }
                RepairTarget::TempFile {
                    final_path: PathBuf::new(),
                    parent_dir,
                    temp_path,
                    writer: DirectWriter::new(std_file),
                    raw_commit_disk_idx: Some(disk_idx),
                }
            }
        }
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
                    return Ok(outcome);
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
        let temp_path_for_set_len = temp_path.clone();

        tokio::task::spawn_blocking(move || file_clone.set_len(total_shard_size))
            .await
            .map_err(|e| {
                AppError::InternalServerError(format!("Failed to preallocate repair file: {}", e))
            })?
            .map_err(|e| {
                let _ = std::fs::remove_file(&temp_path_for_set_len);
                AppError::Io(
                    e,
                    format!("set_len repair file {}", temp_path_for_set_len.display()),
                )
            })?;

        RepairTarget::TempFile {
            final_path,
            parent_dir,
            temp_path,
            writer: DirectWriter::new(std_file),
            raw_commit_disk_idx: None,
        }
    };

    let data_count = data_shards_count(&part);
    let parity_count = part.shards.len().saturating_sub(data_count);

    if data_count == 0 {
        if let RepairTarget::TempFile { temp_path, .. } = &repair_target {
            let _ = fs::remove_file(temp_path).await;
        }
            return Err(AppError::InternalServerError("No data shards".into()));
        }

    let mut peer_cache: ShardFileCache = HashMap::new();
    let mut file_peers = Vec::new();
    let mut raw_peer_templates = Vec::new();

    for s_info in &part.shards {
        if s_info.shard_index == target_shard.shard_index {
            continue;
        }

        if s_info.raw_blob_address.is_some() {
            let Some(ring) =
                get_or_open_raw_shard(&mut peer_cache, s_info, RawReadValidationMode::Verified)
                    .await
            else {
                continue;
            };
            raw_peer_templates.push(RawPeerReadTask {
                ring,
                shard_index: s_info.shard_index,
                is_parity: s_info.is_parity,
                file_name: s_info.file_name.clone(),
                absolute_path: s_info.absolute_path.clone(),
            });
        } else {
            file_peers.push(s_info.clone());
        }
    }

    let mut offset = 0u64;
    let target_is_parity = target_shard.shard_index >= data_count;
    let raw_inflight_limit = data_count.clamp(1, 4);
    let mut buffer_pool = DecoderBufferPool::new(data_count);
    let mut decoder_state: Option<ReedSolomonDecoder> = None;
    let mut encoder_state: Option<ReedSolomonEncoder> = None;
    while offset < total_shard_size {
        let remaining = (total_shard_size - offset) as usize;
        let cur = remaining.min(block_size);

        if let Some(decoder) = decoder_state.as_mut() {
            decoder
                .reset(data_count, parity_count, cur)
                .map_err(|e| AppError::InternalServerError(format!("RS Reset: {}", e)))?;
        } else {
            decoder_state = Some(
                ReedSolomonDecoder::new(data_count, parity_count, cur)
                    .map_err(|e| AppError::InternalServerError(format!("RS Init: {}", e)))?,
            );
        }
        let decoder = decoder_state
            .as_mut()
            .expect("decoder state initialized for repair stripe");

        buffer_pool.reset_for_stripe(target_is_parity);
        let mut shards_found = 0usize;

        let mut pending_raw: FuturesUnordered<
            BoxFuture<'static, (RawPeerReadTask, Result<Vec<u8>, (io::Error, Vec<u8>)>)>,
        > = FuturesUnordered::new();
        let mut remaining_raw = raw_peer_templates.clone().into_iter();

        prime_raw_peer_reads(
            &mut pending_raw,
            &mut remaining_raw,
            &mut buffer_pool,
            offset,
            cur,
            raw_inflight_limit,
        )
        .await;

        for s_info in &file_peers {
            if shards_found >= data_count {
                break;
            }
            let mut peer_scratch = buffer_pool.take_peer_buffer(cur);
            let Some(()) = read_shard_chunk_cached_into(
                &mut peer_cache,
                s_info,
                None,
                offset,
                cur,
                RawReadValidationMode::Verified,
                &mut peer_scratch,
            )
            .await
            else {
                buffer_pool.recycle_peer_buffer(peer_scratch);
                continue;
            };

            let added = if s_info.is_parity {
                decoder.add_recovery_shard(s_info.shard_index - data_count, &peer_scratch)
            } else {
                let add = decoder.add_original_shard(s_info.shard_index, &peer_scratch);
                if add.is_ok() && target_is_parity {
                    buffer_pool.store_data_shard(s_info.shard_index, &peer_scratch);
                }
                add
            }
            .is_ok();

            buffer_pool.recycle_peer_buffer(peer_scratch);
            if added {
                shards_found += 1;
            }
        }

        while shards_found < data_count {
            let Some((task, result)) = pending_raw.next().await else {
                break;
            };

            match result {
                Ok(buffer) => {
                    let added = if task.is_parity {
                        decoder.add_recovery_shard(task.shard_index - data_count, &buffer)
                    } else {
                        let add = decoder.add_original_shard(task.shard_index, &buffer);
                        if add.is_ok() && target_is_parity {
                            buffer_pool.store_data_shard(task.shard_index, &buffer);
                        }
                        add
                    }
                    .is_ok();

                    if added {
                        shards_found += 1;
                    }
                    buffer_pool.recycle_peer_buffer(buffer);
                }
                Err((err, buffer)) => {
                    warn!(
                        "Raw shard read failed file={} path={} offset={} len={} err={}",
                        task.file_name, task.absolute_path, offset, cur, err
                    );
                    buffer_pool.recycle_peer_buffer(buffer);
                }
            }

            prime_raw_peer_reads(
                &mut pending_raw,
                &mut remaining_raw,
                &mut buffer_pool,
                offset,
                cur,
                raw_inflight_limit,
            )
            .await;
        }

        if shards_found < data_count {
            if let RepairTarget::TempFile { temp_path, .. } = &repair_target {
                let _ = fs::remove_file(temp_path).await;
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
                if idx == target_shard.shard_index {
                    buffer_pool.restored_target.extend_from_slice(data);
                    break;
                }
            }
            if buffer_pool.restored_target.is_empty() {
                if let RepairTarget::TempFile { temp_path, .. } = &repair_target {
                    let _ = fs::remove_file(temp_path).await;
                }
                return Err(AppError::InternalServerError(
                    "Missing restored data shard".into(),
                ));
            }
            match &mut repair_target {
                RepairTarget::RawInPlace => {
                    write_shard_range(&target_shard, offset, &buffer_pool.restored_target).await?;
                }
                RepairTarget::TempFile { writer, .. } => {
                    writer
                        .write_all(&buffer_pool.restored_target)
                        .await
                        .map_err(|e| AppError::Io(e, "write repair data".into()))?;
                }
            }
        } else {
            for (idx, data) in decoded.restored_original_iter() {
                if idx < data_count {
                    buffer_pool.store_data_shard(idx, data);
                }
            }

            if buffer_pool.stripe_present.iter().any(|present| !*present) {
                if let RepairTarget::TempFile { temp_path, .. } = &repair_target {
                    let _ = fs::remove_file(temp_path).await;
                }
                return Err(AppError::InternalServerError(
                    "Missing data shards required to recompute parity".into(),
                ));
            }

            if let Some(encoder) = encoder_state.as_mut() {
                encoder
                    .reset(data_count, parity_count, cur)
                    .map_err(|e| AppError::InternalServerError(format!("RS Enc Reset: {}", e)))?;
            } else {
                encoder_state = Some(
                    ReedSolomonEncoder::new(data_count, parity_count, cur).map_err(|e| {
                        AppError::InternalServerError(format!("RS Enc Init: {}", e))
                    })?,
                );
            }
            let encoder = encoder_state
                .as_mut()
                .expect("encoder state initialized for repair stripe");

            for data in &buffer_pool.stripe_data {
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

            match &mut repair_target {
                RepairTarget::RawInPlace => {
                    write_shard_range(&target_shard, offset, data).await?;
                }
                RepairTarget::TempFile { writer, .. } => {
                    writer
                        .write_all(data)
                        .await
                        .map_err(|e| AppError::Io(e, "write repair parity".into()))?;
                }
            }
        }

        offset += cur as u64;
    }

    match repair_target {
        RepairTarget::RawInPlace => {
            if !verify_shard_checksum(&target_shard).await? {
                return Err(AppError::InternalServerError(
                    "Repaired raw shard failed checksum verification".into(),
                ));
            }
            match log_mode {
                RepairLogMode::Verbose => {
                    info!("Successfully repaired raw shard {}", target_shard.file_name);
                }
                RepairLogMode::Summary => {
                    debug!("Successfully repaired raw shard {}", target_shard.file_name);
                }
            }
        }
        RepairTarget::TempFile {
            final_path,
            parent_dir,
            temp_path,
            mut writer,
            raw_commit_disk_idx,
        } => {
            writer
                .flush()
                .await
                .map_err(|e| AppError::Io(e, "flush repair".into()))?;
            writer
                .sync_data()
                .await
                .map_err(|e| AppError::Io(e, "fsync repair".into()))?;

            if let Some(disk_idx) = raw_commit_disk_idx {
                let new_raw =
                    match commit_staged_raw_shard(disk_idx, &temp_path, total_shard_size).await {
                        Ok(raw) => raw,
                        Err(err) => {
                            let _ = fs::remove_file(&temp_path).await;
                            return Err(err);
                        }
                    };

                let mut repaired_shard = target_shard.clone();
                repaired_shard.raw_blob_address = Some(new_raw.clone());
                repaired_shard.absolute_path = raw_blob_debug_path(&new_raw);

                if !verify_shard_checksum(&repaired_shard).await? {
                    let _ = free_raw_address(&new_raw).await;
                    return Err(AppError::InternalServerError(
                        "Relocated raw shard failed checksum verification".into(),
                    ));
                }

                let Some(ctx) = repair_context.as_ref() else {
                    let _ = free_raw_address(&new_raw).await;
                    return Err(AppError::InternalServerError(
                        "Missing repair context for relocated raw shard".into(),
                    ));
                };

                match persist_relocated_raw_shard_metadata(
                    ctx,
                    part.part_index,
                    target_shard.shard_index,
                    target_shard.raw_blob_address.as_ref(),
                    &new_raw,
                )
                .await
                {
                    Ok(RelocatedRawMetadataOutcome::Updated) => {
                        if let Some(old_raw) = target_shard.raw_blob_address.as_ref() {
                            remember_raw_relocation(old_raw, &new_raw);
                            let _ = free_raw_address(old_raw).await;
                        }

                        outcome.relocated_raw_shard = true;
                        outcome.stale_mapping_relocated = true;
                        match log_mode {
                            RepairLogMode::Verbose => info!(
                                "Successfully relocated raw shard {} -> {}",
                                target_shard.file_name,
                                raw_blob_debug_path(&new_raw)
                            ),
                            RepairLogMode::Summary => debug!(
                                "Successfully relocated raw shard {} -> {}",
                                target_shard.file_name,
                                raw_blob_debug_path(&new_raw)
                            ),
                        }
                    }
                    Ok(RelocatedRawMetadataOutcome::AlreadyRelocated(existing_raw)) => {
                        let _ = free_raw_address(&new_raw).await;
                        if let Some(old_raw) = target_shard.raw_blob_address.as_ref() {
                            remember_raw_relocation(old_raw, &existing_raw);
                        }
                        outcome.relocated_raw_shard = true;
                        outcome.stale_mapping_relocated = true;
                        match log_mode {
                            RepairLogMode::Verbose => info!(
                                "Raw shard {} already relocated to {}",
                                target_shard.file_name,
                                raw_blob_debug_path(&existing_raw)
                            ),
                            RepairLogMode::Summary => debug!(
                                "Raw shard {} already relocated to {}",
                                target_shard.file_name,
                                raw_blob_debug_path(&existing_raw)
                            ),
                        }
                    }
                    Err(err) => {
                        let _ = free_raw_address(&new_raw).await;
                        return Err(err);
                    }
                }
            } else {
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

                match log_mode {
                    RepairLogMode::Verbose => info!(
                        "Successfully repaired shard {} -> {}",
                        target_shard.file_name,
                        final_path.display()
                    ),
                    RepairLogMode::Summary => debug!(
                        "Successfully repaired shard {} -> {}",
                        target_shard.file_name,
                        final_path.display()
                    ),
                }
            }
        }
    }

    Ok(outcome)
}
