use crate::codec::{decode_record, encode_header, encode_record, read_header};
use crate::constants::{JOURNAL_RECORD_SIZE, SECONDARY_SUPERBLOCK_BLOCK, SUPERBLOCK_MAGIC};
use crate::device_io::{
    block_offset, div_ceil, open_direct_file, read_exact_at, read_exact_at_direct,
    validate_block_size, write_all_at, write_all_at_direct, zero_region,
};
use crate::error::BlockManagerError;
use crate::types::{
    AllocationGroupHealth, AllocationHandle, AllocatorHealthSnapshot, DeviceHeader, DirectExtentIo,
    Extent, FormatNonceChallenge, FormatOptions, FormatTargetPreflight, FormatTargetSignature,
    JournalOp, JournalRecord, RawExtentAddress,
};
use crc32fast::Hasher as Crc32Hasher;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[cfg(unix)]
use std::os::unix::fs::FileTypeExt;

const MIN_AG_COUNT: usize = 1;
const MAX_AG_COUNT: usize = 16;
const TARGET_AG_BYTES: u64 = 32 * 1024 * 1024 * 1024;
const SEGMENT_POOL_MIN_DEVICE_BYTES: u64 = 1 * 1024 * 1024 * 1024;
const DEFAULT_SEGMENT_SIZE_BYTES: u64 = 256 * 1024 * 1024;
const MIN_METADATA_POOL_BYTES: u64 = 256 * 1024 * 1024;
const MAX_METADATA_POOL_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const SEGMENT_RECORD_BYTES: usize = 32;
const SEGMENT_RECORD_SLOT_BYTES: u64 = 64;
const SEGMENT_RECORD_MAGIC: u32 = 0x4C42_4D53;
const SEGMENT_STATE_FREE: u8 = 0;
const SEGMENT_STATE_OPEN: u8 = 1;
const SEGMENT_STATE_SEALED: u8 = 2;
const SEGMENT_ALLOCATION_ID_TAG: u64 = 1u64 << 63;
const SEGMENT_ALLOCATION_OFFSET_BITS: u32 = 16;
const SEGMENT_ALLOCATION_GENERATION_BITS: u32 = 23;
const SEGMENT_ALLOCATION_SEGMENT_BITS: u32 = 24;
const SEGMENT_ALLOCATION_OFFSET_MASK: u64 = (1u64 << SEGMENT_ALLOCATION_OFFSET_BITS) - 1;
const SEGMENT_ALLOCATION_GENERATION_MASK: u64 = (1u64 << SEGMENT_ALLOCATION_GENERATION_BITS) - 1;
const SEGMENT_ALLOCATION_SEGMENT_MASK: u64 = (1u64 << SEGMENT_ALLOCATION_SEGMENT_BITS) - 1;
const FORMAT_NONCE_TTL_SECS: u64 = 5 * 60;
const FORMAT_NONCE_FILE_PREFIX: &str = "n1-bm-format-nonce-";
const FORMAT_NONCE_FILE_VERSION: &str = "v1";

#[cfg(test)]
std::thread_local! {
    static IO_FAILPOINTS: std::cell::RefCell<HashMap<&'static str, usize>> =
        std::cell::RefCell::new(HashMap::new());
}

#[cfg(test)]
fn maybe_failpoint(name: &'static str) -> Result<(), BlockManagerError> {
    let mut should_fail = false;
    IO_FAILPOINTS.with(|map| {
        let mut map = map.borrow_mut();
        if let Some(remaining_successes) = map.get_mut(name) {
            if *remaining_successes == 0 {
                should_fail = true;
            } else {
                *remaining_successes = remaining_successes.saturating_sub(1);
            }
        }
    });
    if should_fail {
        return Err(BlockManagerError::Io(std::io::Error::other(format!(
            "injected failpoint: {}",
            name
        ))));
    }
    Ok(())
}

#[cfg(not(test))]
fn maybe_failpoint(_name: &'static str) -> Result<(), BlockManagerError> {
    Ok(())
}

fn write_all_at_with_failpoint(
    file: &File,
    buf: &[u8],
    offset: u64,
    failpoint: &'static str,
) -> Result<(), BlockManagerError> {
    maybe_failpoint(failpoint)?;
    write_all_at(file, buf, offset)
}

fn sync_all_with_failpoint(file: &File, failpoint: &'static str) -> Result<(), BlockManagerError> {
    maybe_failpoint(failpoint)?;
    file.sync_all()?;
    Ok(())
}

fn zero_region_with_failpoint(
    file: &File,
    block_size: u32,
    offset: u64,
    len: u64,
    failpoint: &'static str,
) -> Result<(), BlockManagerError> {
    maybe_failpoint(failpoint)?;
    zero_region(file, block_size, offset, len)
}

fn detect_format_target_signature(
    file: &File,
    total_bytes: u64,
) -> Result<FormatTargetSignature, BlockManagerError> {
    if total_bytes == 0 {
        return Ok(FormatTargetSignature::Empty);
    }

    let mut magic = [0u8; 16];
    if total_bytes >= 16 {
        read_exact_at(file, &mut magic, 0)?;
    } else {
        let mut partial = vec![0u8; total_bytes as usize];
        read_exact_at(file, &mut partial, 0)?;
        magic[..partial.len()].copy_from_slice(&partial);
    }

    if magic == SUPERBLOCK_MAGIC {
        return Ok(FormatTargetSignature::BlockManager);
    }
    if magic.iter().all(|b| *b == 0) {
        return Ok(FormatTargetSignature::Empty);
    }
    Ok(FormatTargetSignature::Foreign)
}

fn format_signature_tag(signature: FormatTargetSignature) -> u8 {
    match signature {
        FormatTargetSignature::Empty => 0,
        FormatTargetSignature::BlockManager => 1,
        FormatTargetSignature::Foreign => 2,
    }
}

fn format_target_fingerprint(preflight: &FormatTargetPreflight) -> u64 {
    let mut hasher = Crc32Hasher::new();
    hasher.update(preflight.canonical_path.to_string_lossy().as_bytes());
    hasher.update(&preflight.total_bytes.to_le_bytes());
    hasher.update(&[
        preflight.is_block_device as u8,
        preflight.is_mounted as u8,
        format_signature_tag(preflight.signature),
    ]);
    u64::from(hasher.finalize())
}

fn current_unix_secs() -> Result<u64, BlockManagerError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| BlockManagerError::InvalidArgument(format!("invalid system time: {}", e)))?;
    Ok(now.as_secs())
}

fn nonce_state_file_path(canonical_path: &Path) -> PathBuf {
    let mut hasher = Crc32Hasher::new();
    hasher.update(canonical_path.to_string_lossy().as_bytes());
    let digest = hasher.finalize();
    std::env::temp_dir().join(format!("{FORMAT_NONCE_FILE_PREFIX}{digest:08x}.state"))
}

#[derive(Debug)]
struct FormatNonceState {
    nonce: String,
    fingerprint: u64,
    issued_unix_secs: u64,
    expires_unix_secs: u64,
}

fn persist_nonce_state(
    state_path: &Path,
    state: &FormatNonceState,
) -> Result<(), BlockManagerError> {
    let payload = format!(
        "{}|{}|{}|{}|{}\n",
        FORMAT_NONCE_FILE_VERSION,
        state.nonce,
        state.fingerprint,
        state.issued_unix_secs,
        state.expires_unix_secs
    );
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(state_path)?;
    write_all_at(&file, payload.as_bytes(), 0)?;
    file.sync_all()?;
    Ok(())
}

fn read_nonce_state(state_path: &Path) -> Result<FormatNonceState, BlockManagerError> {
    let payload = std::fs::read_to_string(state_path).map_err(|e| {
        BlockManagerError::InvalidArgument(format!(
            "failed to read format nonce state {}: {}",
            state_path.display(),
            e
        ))
    })?;
    let fields = payload.trim().split('|').collect::<Vec<_>>();
    if fields.len() != 5 || fields[0] != FORMAT_NONCE_FILE_VERSION {
        return Err(BlockManagerError::InvalidArgument(format!(
            "invalid format nonce state payload at {}",
            state_path.display()
        )));
    }
    let fingerprint = fields[2].parse::<u64>().map_err(|e| {
        BlockManagerError::InvalidArgument(format!(
            "invalid format nonce fingerprint at {}: {}",
            state_path.display(),
            e
        ))
    })?;
    let issued_unix_secs = fields[3].parse::<u64>().map_err(|e| {
        BlockManagerError::InvalidArgument(format!(
            "invalid format nonce issue time at {}: {}",
            state_path.display(),
            e
        ))
    })?;
    let expires_unix_secs = fields[4].parse::<u64>().map_err(|e| {
        BlockManagerError::InvalidArgument(format!(
            "invalid format nonce expiry time at {}: {}",
            state_path.display(),
            e
        ))
    })?;
    Ok(FormatNonceState {
        nonce: fields[1].to_string(),
        fingerprint,
        issued_unix_secs,
        expires_unix_secs,
    })
}

#[cfg(unix)]
fn is_mounted_block_device_path(path: &Path) -> bool {
    let Ok(mountinfo) = std::fs::read_to_string("/proc/self/mountinfo") else {
        return false;
    };
    for line in mountinfo.lines() {
        let Some((_left, right)) = line.split_once(" - ") else {
            continue;
        };
        let mut right_fields = right.split_whitespace();
        let _fs_type = right_fields.next();
        let Some(source) = right_fields.next() else {
            continue;
        };
        let source_path = Path::new(source);
        if source_path == path {
            return true;
        }
        if let Ok(source_canonical) = std::fs::canonicalize(source_path) {
            if source_canonical == path {
                return true;
            }
        }
    }
    false
}

#[cfg(not(unix))]
fn is_mounted_block_device_path(_path: &Path) -> bool {
    false
}

#[cfg(test)]
fn set_failpoint(name: &'static str, passes_before_fail: usize) {
    IO_FAILPOINTS.with(|map| {
        map.borrow_mut().insert(name, passes_before_fail);
    });
}

#[cfg(test)]
fn clear_failpoints() {
    IO_FAILPOINTS.with(|map| {
        map.borrow_mut().clear();
    });
}

#[derive(Clone, Debug)]
struct AllocationGroup {
    ag_id: usize,
    start_block: u64,
    block_count: u64,
    free_blocks: u64,
    by_addr: BTreeMap<u64, Extent>,
    by_size: BTreeMap<u64, BTreeSet<u64>>,
}

impl AllocationGroup {
    fn new(ag_id: usize, start_block: u64, block_count: u64) -> Self {
        let mut group = Self {
            ag_id,
            start_block,
            block_count,
            free_blocks: 0,
            by_addr: BTreeMap::new(),
            by_size: BTreeMap::new(),
        };
        if block_count > 0 {
            group.insert_extent(Extent {
                start_block,
                block_count,
            });
        }
        group
    }

    fn contains_block(&self, block: u64) -> bool {
        block >= self.start_block
            && block < self.start_block.saturating_add(self.block_count)
            && self.block_count > 0
    }

    fn best_fit_len(&self, requested_blocks: u64) -> Option<u64> {
        self.by_size
            .range(requested_blocks..)
            .next()
            .map(|(len, _)| *len)
    }

    fn find_best_fit_extent(&self, requested_blocks: u64) -> Option<Extent> {
        let (_len, starts) = self.by_size.range(requested_blocks..).next()?;
        let start = starts.iter().next().copied()?;
        self.by_addr.get(&start).copied().map(|free| Extent {
            start_block: free.start_block,
            block_count: requested_blocks,
        })
    }

    fn remove_known_extent(&mut self, target: Extent) -> Result<(), BlockManagerError> {
        let Some((&container_start, container_extent)) =
            self.by_addr.range(..=target.start_block).next_back()
        else {
            return Err(BlockManagerError::Corrupt(format!(
                "AG {} allocated extent {:?} not present in free-space index",
                self.ag_id, target
            )));
        };

        let target_end = target.start_block.saturating_add(target.block_count);
        let container_end = container_extent
            .start_block
            .saturating_add(container_extent.block_count);
        if target.start_block < container_extent.start_block || target_end > container_end {
            return Err(BlockManagerError::Corrupt(format!(
                "AG {} allocated extent {:?} escapes container extent {:?}",
                self.ag_id, target, container_extent
            )));
        }

        let removed = self.remove_exact_extent(container_start)?;
        let prefix_len = target.start_block.saturating_sub(removed.start_block);
        let suffix_start = target_end;
        let suffix_len = removed
            .start_block
            .saturating_add(removed.block_count)
            .saturating_sub(suffix_start);

        if prefix_len > 0 {
            self.insert_extent(Extent {
                start_block: removed.start_block,
                block_count: prefix_len,
            });
        }
        if suffix_len > 0 {
            self.insert_extent(Extent {
                start_block: suffix_start,
                block_count: suffix_len,
            });
        }
        Ok(())
    }

    fn free_extent(&mut self, extent: Extent) {
        self.insert_extent(extent);
    }

    fn insert_extent(&mut self, extent: Extent) {
        if extent.block_count == 0 {
            return;
        }

        let mut merged_start = extent.start_block;
        let mut merged_end = extent.start_block.saturating_add(extent.block_count);

        if let Some((&prev_start, prev_extent)) = self.by_addr.range(..=merged_start).next_back() {
            let prev_end = prev_extent
                .start_block
                .saturating_add(prev_extent.block_count);
            if prev_end >= merged_start {
                if let Ok(prev) = self.remove_exact_extent(prev_start) {
                    merged_start = merged_start.min(prev.start_block);
                    merged_end = merged_end.max(prev.start_block.saturating_add(prev.block_count));
                }
            }
        }

        loop {
            let next = self
                .by_addr
                .range(merged_start..)
                .next()
                .map(|(&start, extent)| (start, *extent));
            let Some((next_start, next_extent)) = next else {
                break;
            };
            if merged_end < next_start {
                break;
            }
            if let Ok(removed) = self.remove_exact_extent(next_start) {
                merged_start = merged_start.min(removed.start_block);
                merged_end =
                    merged_end.max(removed.start_block.saturating_add(removed.block_count));
            } else {
                break;
            }
            if next_extent.block_count == 0 {
                break;
            }
        }

        self.add_exact_extent(Extent {
            start_block: merged_start,
            block_count: merged_end.saturating_sub(merged_start),
        });
    }

    fn add_exact_extent(&mut self, extent: Extent) {
        if extent.block_count == 0 {
            return;
        }
        self.by_addr.insert(extent.start_block, extent);
        self.by_size
            .entry(extent.block_count)
            .or_default()
            .insert(extent.start_block);
        self.free_blocks = self.free_blocks.saturating_add(extent.block_count);
    }

    fn remove_exact_extent(&mut self, start_block: u64) -> Result<Extent, BlockManagerError> {
        let extent = self.by_addr.remove(&start_block).ok_or_else(|| {
            BlockManagerError::Corrupt(format!(
                "AG {} free extent starting at block {} not found",
                self.ag_id, start_block
            ))
        })?;
        let remove_size_bucket = if let Some(starts) = self.by_size.get_mut(&extent.block_count) {
            starts.remove(&start_block);
            starts.is_empty()
        } else {
            false
        };
        if remove_size_bucket {
            self.by_size.remove(&extent.block_count);
        }
        self.free_blocks = self.free_blocks.saturating_sub(extent.block_count);
        Ok(extent)
    }
}

#[derive(Clone, Debug)]
struct AllocationState {
    ags: Vec<AllocationGroup>,
    ag_start_blocks: Vec<u64>,
    next_ag_hint: usize,
    ag_max_free_len: Vec<u64>,
    ags_by_max_free_len: BTreeMap<u64, BTreeSet<usize>>,
}

impl AllocationState {
    fn new(data_start_block: u64, data_blocks: u64, ag_count: usize) -> Self {
        let mut ags = Vec::new();
        for (ag_id, (start_block, block_count)) in
            split_data_blocks_into_ags(data_start_block, data_blocks, ag_count)
                .into_iter()
                .enumerate()
        {
            ags.push(AllocationGroup::new(ag_id, start_block, block_count));
        }
        let mut state = Self {
            ags,
            ag_start_blocks: Vec::new(),
            next_ag_hint: 0,
            ag_max_free_len: Vec::new(),
            ags_by_max_free_len: BTreeMap::new(),
        };
        state.ag_start_blocks = state.ags.iter().map(|ag| ag.start_block).collect();
        state.rebuild_ag_free_index();
        state
    }

    fn available_blocks(&self) -> u64 {
        self.ags.iter().map(|ag| ag.free_blocks).sum()
    }

    fn ag_max_len(ag: &AllocationGroup) -> u64 {
        ag.by_size.keys().next_back().copied().unwrap_or(0)
    }

    fn rebuild_ag_free_index(&mut self) {
        self.ag_max_free_len.clear();
        self.ags_by_max_free_len.clear();
        self.ag_max_free_len.reserve(self.ags.len());
        for (idx, ag) in self.ags.iter().enumerate() {
            let max_len = Self::ag_max_len(ag);
            self.ag_max_free_len.push(max_len);
            if max_len == 0 {
                continue;
            }
            self.ags_by_max_free_len
                .entry(max_len)
                .or_default()
                .insert(idx);
        }
    }

    fn refresh_ag_free_index_for(&mut self, ag_idx: usize) {
        if ag_idx >= self.ags.len() {
            return;
        }
        let old_max_len = self.ag_max_free_len.get(ag_idx).copied().unwrap_or(0);
        if old_max_len > 0 {
            let remove_bucket = if let Some(set) = self.ags_by_max_free_len.get_mut(&old_max_len) {
                set.remove(&ag_idx);
                set.is_empty()
            } else {
                false
            };
            if remove_bucket {
                self.ags_by_max_free_len.remove(&old_max_len);
            }
        }

        let new_max_len = Self::ag_max_len(&self.ags[ag_idx]);
        if ag_idx >= self.ag_max_free_len.len() {
            self.ag_max_free_len.resize(ag_idx + 1, 0);
        }
        self.ag_max_free_len[ag_idx] = new_max_len;
        if new_max_len > 0 {
            self.ags_by_max_free_len
                .entry(new_max_len)
                .or_default()
                .insert(ag_idx);
        }
    }

    fn find_best_fit_extent(&self, requested_blocks: u64) -> Option<Extent> {
        if requested_blocks == 0 || self.ags.is_empty() {
            return None;
        }

        let start = self.next_ag_hint % self.ags.len();
        let mut best: Option<(usize, usize, u64)> = None;
        for (_max_len, ag_set) in self.ags_by_max_free_len.range(requested_blocks..) {
            for idx in ag_set {
                let Some(candidate_len) = self.ags[*idx].best_fit_len(requested_blocks) else {
                    continue;
                };
                let offset = (*idx + self.ags.len() - start) % self.ags.len();
                match best {
                    None => best = Some((*idx, offset, candidate_len)),
                    Some((_best_idx, best_offset, best_len)) => {
                        if candidate_len < best_len
                            || (candidate_len == best_len && offset < best_offset)
                        {
                            best = Some((*idx, offset, candidate_len));
                        }
                    }
                }
            }
        }

        let (idx, _, _) = best?;
        self.ags[idx].find_best_fit_extent(requested_blocks)
    }

    fn remove_known_extent(&mut self, extent: Extent) -> Result<(), BlockManagerError> {
        let idx = self.ag_index_for_block(extent.start_block).ok_or_else(|| {
            BlockManagerError::Corrupt(format!(
                "extent {:?} is outside known allocation-group ranges",
                extent
            ))
        })?;
        self.ags[idx].remove_known_extent(extent)?;
        self.refresh_ag_free_index_for(idx);
        if !self.ags.is_empty() {
            self.next_ag_hint = (idx + 1) % self.ags.len();
        }
        Ok(())
    }

    fn free_extent(&mut self, extent: Extent) -> Result<(), BlockManagerError> {
        let idx = self.ag_index_for_block(extent.start_block).ok_or_else(|| {
            BlockManagerError::Corrupt(format!(
                "extent {:?} is outside known allocation-group ranges",
                extent
            ))
        })?;
        self.ags[idx].free_extent(extent);
        self.refresh_ag_free_index_for(idx);
        Ok(())
    }

    fn ag_index_for_block(&self, block: u64) -> Option<usize> {
        if self.ags.is_empty() {
            return None;
        }
        let idx = match self.ag_start_blocks.binary_search(&block) {
            Ok(i) => i,
            Err(0) => return None,
            Err(i) => i.saturating_sub(1),
        };
        self.ags.get(idx).and_then(|ag| {
            if ag.contains_block(block) {
                Some(idx)
            } else {
                None
            }
        })
    }
}

fn derive_ag_count(data_blocks: u64, block_size: u32) -> usize {
    if data_blocks == 0 {
        return 1;
    }
    let data_bytes = data_blocks.saturating_mul(u64::from(block_size));
    let mut ag_count = data_bytes.div_ceil(TARGET_AG_BYTES) as usize;
    ag_count = ag_count.clamp(MIN_AG_COUNT, MAX_AG_COUNT);
    if data_blocks < ag_count as u64 {
        ag_count = data_blocks as usize;
    }
    ag_count.max(1)
}

fn split_data_blocks_into_ags(
    data_start_block: u64,
    data_blocks: u64,
    ag_count: usize,
) -> Vec<(u64, u64)> {
    if data_blocks == 0 {
        return vec![];
    }
    let effective_count = ag_count.max(1).min(data_blocks as usize);
    let base = data_blocks / effective_count as u64;
    let rem = data_blocks % effective_count as u64;
    let mut cursor = data_start_block;
    let mut out = Vec::with_capacity(effective_count);
    for idx in 0..effective_count {
        let block_count = base + u64::from((idx as u64) < rem);
        if block_count == 0 {
            continue;
        }
        out.push((cursor, block_count));
        cursor = cursor.saturating_add(block_count);
    }
    out
}

fn fragmentation_ppm(free_blocks: u64, largest_free_extent_blocks: u64) -> u32 {
    if free_blocks == 0 {
        return 0;
    }
    let fragmented_blocks = free_blocks.saturating_sub(largest_free_extent_blocks);
    let ppm = fragmented_blocks
        .saturating_mul(1_000_000)
        .checked_div(free_blocks)
        .unwrap_or(0);
    ppm.min(1_000_000) as u32
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SegmentCatalogEntry {
    sequence: u64,
    generation: u32,
    write_pointer_bytes: u32,
    dead_bytes: u32,
    state: u8,
}

impl Default for SegmentCatalogEntry {
    fn default() -> Self {
        Self {
            sequence: 0,
            generation: 1,
            write_pointer_bytes: 0,
            dead_bytes: 0,
            state: SEGMENT_STATE_FREE,
        }
    }
}

#[derive(Clone, Debug)]
struct SegmentPoolState {
    segment_start_block: u64,
    segment_block_count: u64,
    segment_size_bytes: u64,
    entries: Vec<SegmentCatalogEntry>,
    active_segment_id: Option<u32>,
}

fn recommended_metadata_pool_bytes(total_bytes: u64) -> u64 {
    (total_bytes / 1024).clamp(MIN_METADATA_POOL_BYTES, MAX_METADATA_POOL_BYTES)
}

fn encode_segment_record(entry: SegmentCatalogEntry) -> [u8; SEGMENT_RECORD_BYTES] {
    let mut out = [0u8; SEGMENT_RECORD_BYTES];
    if entry.sequence == 0
        && entry.write_pointer_bytes == 0
        && entry.dead_bytes == 0
        && entry.state == SEGMENT_STATE_FREE
        && entry.generation <= 1
    {
        return out;
    }
    out[0..4].copy_from_slice(&SEGMENT_RECORD_MAGIC.to_le_bytes());
    out[4..12].copy_from_slice(&entry.sequence.to_le_bytes());
    out[12..16].copy_from_slice(&entry.generation.to_le_bytes());
    out[16..20].copy_from_slice(&entry.write_pointer_bytes.to_le_bytes());
    out[20..24].copy_from_slice(&entry.dead_bytes.to_le_bytes());
    out[24] = entry.state;
    let mut hasher = Crc32Hasher::new();
    hasher.update(&out[..28]);
    let crc = hasher.finalize();
    out[28..32].copy_from_slice(&crc.to_le_bytes());
    out
}

fn decode_segment_record(buf: &[u8; SEGMENT_RECORD_BYTES]) -> Result<SegmentCatalogEntry, BlockManagerError> {
    if buf.iter().all(|byte| *byte == 0) {
        return Ok(SegmentCatalogEntry::default());
    }
    let magic = u32::from_le_bytes(buf[0..4].try_into().expect("segment record magic"));
    if magic != SEGMENT_RECORD_MAGIC {
        return Err(BlockManagerError::Corrupt(
            "segment catalog record magic mismatch".to_string(),
        ));
    }
    let expected_crc = u32::from_le_bytes(buf[28..32].try_into().expect("segment record crc"));
    let mut hasher = Crc32Hasher::new();
    hasher.update(&buf[..28]);
    let actual_crc = hasher.finalize();
    if expected_crc != actual_crc {
        return Err(BlockManagerError::Corrupt(
            "segment catalog record checksum mismatch".to_string(),
        ));
    }
    Ok(SegmentCatalogEntry {
        sequence: u64::from_le_bytes(buf[4..12].try_into().expect("segment sequence")),
        generation: u32::from_le_bytes(buf[12..16].try_into().expect("segment generation")).max(1),
        write_pointer_bytes: u32::from_le_bytes(
            buf[16..20].try_into().expect("segment write pointer bytes"),
        ),
        dead_bytes: u32::from_le_bytes(buf[20..24].try_into().expect("segment dead bytes")),
        state: buf[24],
    })
}

fn encode_segment_allocation_id(
    segment_id: u32,
    generation: u32,
    offset_blocks: u32,
) -> Result<u64, BlockManagerError> {
    if u64::from(segment_id) > SEGMENT_ALLOCATION_SEGMENT_MASK {
        return Err(BlockManagerError::InvalidArgument(format!(
            "segment id {} exceeds encoded range",
            segment_id
        )));
    }
    if generation == 0 || u64::from(generation) > SEGMENT_ALLOCATION_GENERATION_MASK {
        return Err(BlockManagerError::InvalidArgument(format!(
            "segment generation {} exceeds encoded range",
            generation
        )));
    }
    if u64::from(offset_blocks) > SEGMENT_ALLOCATION_OFFSET_MASK {
        return Err(BlockManagerError::InvalidArgument(format!(
            "segment offset blocks {} exceeds encoded range",
            offset_blocks
        )));
    }
    Ok(
        SEGMENT_ALLOCATION_ID_TAG
            | (u64::from(segment_id)
                << (SEGMENT_ALLOCATION_GENERATION_BITS + SEGMENT_ALLOCATION_OFFSET_BITS))
            | (u64::from(generation) << SEGMENT_ALLOCATION_OFFSET_BITS)
            | u64::from(offset_blocks),
    )
}

fn decode_segment_allocation_id(
    allocation_id: u64,
) -> Result<(u32, u32, u32), BlockManagerError> {
    if allocation_id & SEGMENT_ALLOCATION_ID_TAG == 0 {
        return Err(BlockManagerError::InvalidArgument(format!(
            "allocation id {} is not segment-backed",
            allocation_id
        )));
    }
    let raw = allocation_id & !SEGMENT_ALLOCATION_ID_TAG;
    let offset_blocks = (raw & SEGMENT_ALLOCATION_OFFSET_MASK) as u32;
    let generation =
        ((raw >> SEGMENT_ALLOCATION_OFFSET_BITS) & SEGMENT_ALLOCATION_GENERATION_MASK) as u32;
    let segment_id = ((raw
        >> (SEGMENT_ALLOCATION_GENERATION_BITS + SEGMENT_ALLOCATION_OFFSET_BITS))
        & SEGMENT_ALLOCATION_SEGMENT_MASK) as u32;
    Ok((segment_id, generation, offset_blocks))
}

fn is_segment_allocation_id(allocation_id: u64) -> bool {
    allocation_id & SEGMENT_ALLOCATION_ID_TAG != 0
}

fn derive_segment_layout(
    total_blocks: u64,
    block_size: u32,
    journal_start_block: u64,
    journal_block_count: u64,
) -> Option<(u64, u64, u64, u64)> {
    let total_bytes = total_blocks.saturating_mul(u64::from(block_size));
    if total_bytes < SEGMENT_POOL_MIN_DEVICE_BYTES {
        return None;
    }

    let base_data_start = journal_start_block.saturating_add(journal_block_count);
    let available_after_journal = total_blocks.saturating_sub(base_data_start);
    if available_after_journal == 0 {
        return None;
    }

    let segment_size_bytes = DEFAULT_SEGMENT_SIZE_BYTES;
    let segment_block_count = div_ceil(segment_size_bytes, u64::from(block_size));
    if segment_block_count == 0 || available_after_journal <= segment_block_count {
        return None;
    }

    let mut catalog_block_count = 1u64;
    let mut metadata_pool_block_count = 0u64;

    for _ in 0..8 {
        let data_start_block = base_data_start.saturating_add(catalog_block_count);
        let available_after_catalog = total_blocks.saturating_sub(data_start_block);
        if available_after_catalog <= segment_block_count {
            return None;
        }

        let recommended_metadata_blocks =
            div_ceil(recommended_metadata_pool_bytes(total_bytes), u64::from(block_size));
        metadata_pool_block_count = recommended_metadata_blocks.min(available_after_catalog / 4);
        if metadata_pool_block_count >= available_after_catalog {
            metadata_pool_block_count = 0;
        }

        let segment_pool_blocks = available_after_catalog.saturating_sub(metadata_pool_block_count);
        let segment_count = segment_pool_blocks / segment_block_count;
        if segment_count == 0 {
            let reduced_metadata = available_after_catalog.saturating_sub(segment_block_count);
            metadata_pool_block_count = metadata_pool_block_count.min(reduced_metadata);
        }

        let segment_pool_blocks = available_after_catalog.saturating_sub(metadata_pool_block_count);
        let segment_count = segment_pool_blocks / segment_block_count;
        if segment_count == 0 {
            return None;
        }

        let needed_catalog_blocks = div_ceil(
            segment_count.saturating_mul(SEGMENT_RECORD_SLOT_BYTES),
            u64::from(block_size),
        );
        if needed_catalog_blocks == catalog_block_count {
            break;
        }
        catalog_block_count = needed_catalog_blocks;
    }

    let data_start_block = base_data_start.saturating_add(catalog_block_count);
    let available_after_catalog = total_blocks.saturating_sub(data_start_block);
    if available_after_catalog <= metadata_pool_block_count.saturating_add(segment_block_count) {
        return None;
    }

    Some((
        data_start_block,
        catalog_block_count,
        metadata_pool_block_count,
        segment_size_bytes,
    ))
}

pub struct BlockManager {
    path: PathBuf,
    meta_file: File,
    direct_file: Arc<File>,
    header: DeviceHeader,
    allocation_state: AllocationState,
    allocations: HashMap<u64, Extent>,
    allocation_generations: HashMap<u64, u32>,
    segment_pool: Option<SegmentPoolState>,
    next_allocation_id: u64,
    next_journal_txn_id: u64,
    last_applied_journal_txn_id: u64,
    journal_cursor: u64,
    journal_dirty: bool,
    segment_catalog_dirty: bool,
}

impl BlockManager {
    pub fn format<P: AsRef<Path>>(
        path: P,
        options: FormatOptions,
    ) -> Result<Self, BlockManagerError> {
        Self::format_with_policy(path, options, false, false)
    }

    pub fn issue_format_nonce<P: AsRef<Path>>(
        path: P,
    ) -> Result<FormatNonceChallenge, BlockManagerError> {
        let preflight = Self::format_preflight(path)?;
        let issued_unix_secs = current_unix_secs()?;
        let expires_unix_secs = issued_unix_secs.saturating_add(FORMAT_NONCE_TTL_SECS);
        let nonce = Uuid::new_v4().to_string();
        let fingerprint = format_target_fingerprint(&preflight);
        let state_path = nonce_state_file_path(&preflight.canonical_path);
        let state = FormatNonceState {
            nonce: nonce.clone(),
            fingerprint,
            issued_unix_secs,
            expires_unix_secs,
        };
        persist_nonce_state(&state_path, &state)?;

        Ok(FormatNonceChallenge {
            nonce,
            preflight,
            issued_unix_secs,
            expires_unix_secs,
        })
    }

    pub fn format_with_nonce<P: AsRef<Path>>(
        path: P,
        options: FormatOptions,
        nonce: &str,
        allow_foreign_signature: bool,
    ) -> Result<Self, BlockManagerError> {
        let path_ref = path.as_ref();
        let preflight = Self::format_preflight(path_ref)?;
        let state_path = nonce_state_file_path(&preflight.canonical_path);
        let state = read_nonce_state(&state_path)?;

        // Single-use nonce: consume state regardless of verification result.
        let _ = std::fs::remove_file(&state_path);

        let now = current_unix_secs()?;
        if now > state.expires_unix_secs {
            return Err(BlockManagerError::InvalidArgument(format!(
                "format nonce expired at {} (now={})",
                state.expires_unix_secs, now
            )));
        }
        if nonce != state.nonce {
            return Err(BlockManagerError::InvalidArgument(
                "format nonce mismatch".to_string(),
            ));
        }
        let observed_fingerprint = format_target_fingerprint(&preflight);
        if observed_fingerprint != state.fingerprint {
            return Err(BlockManagerError::InvalidArgument(format!(
                "format target fingerprint drifted (expected={} observed={})",
                state.fingerprint, observed_fingerprint
            )));
        }

        Self::format_with_policy(path_ref, options, allow_foreign_signature, false)
    }

    pub fn format_with_force<P: AsRef<Path>>(
        path: P,
        options: FormatOptions,
    ) -> Result<Self, BlockManagerError> {
        Self::format_with_policy(path, options, true, false)
    }

    pub fn format_preflight<P: AsRef<Path>>(
        path: P,
    ) -> Result<FormatTargetPreflight, BlockManagerError> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)?;
        let meta = file.metadata()?;
        let canonical_path = std::fs::canonicalize(&path).unwrap_or(path);

        #[cfg(unix)]
        let is_block_device = meta.file_type().is_block_device();
        #[cfg(not(unix))]
        let is_block_device = false;
        let is_mounted = if is_block_device {
            is_mounted_block_device_path(&canonical_path)
        } else {
            false
        };
        let signature = detect_format_target_signature(&file, meta.len())?;

        Ok(FormatTargetPreflight {
            canonical_path,
            total_bytes: meta.len(),
            is_block_device,
            is_mounted,
            signature,
        })
    }

    fn format_with_policy<P: AsRef<Path>>(
        path: P,
        options: FormatOptions,
        allow_foreign_signature: bool,
        allow_mounted_target: bool,
    ) -> Result<Self, BlockManagerError> {
        let path = path.as_ref().to_path_buf();
        let preflight = Self::format_preflight(&path)?;

        if preflight.is_mounted && !allow_mounted_target {
            return Err(BlockManagerError::InvalidArgument(format!(
                "refusing to format mounted block device target {}",
                preflight.canonical_path.display()
            )));
        }
        if preflight.signature == FormatTargetSignature::Foreign && !allow_foreign_signature {
            return Err(BlockManagerError::InvalidArgument(format!(
                "refusing to format foreign-signature target {}; call format_with_force to override",
                preflight.canonical_path.display()
            )));
        }

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)?;
        let meta = file.metadata()?;

        let block_size = validate_block_size(options.block_size)?;
        let total_bytes = match options.total_bytes {
            Some(bytes) => {
                if should_resize_backing_store(&meta) {
                    file.set_len(bytes)?;
                }
                bytes
            }
            None => {
                let meta_len = meta.len();
                if meta_len == 0 {
                    return Err(BlockManagerError::InvalidArgument(
                        "format requires total_bytes for a new loopback file".to_string(),
                    ));
                }
                meta_len
            }
        };

        let total_blocks = total_bytes / u64::from(block_size);
        let journal_start_block = 1u64.saturating_add(SECONDARY_SUPERBLOCK_BLOCK);
        let journal_block_count = options.journal_blocks.max(1);
        if total_blocks <= journal_start_block.saturating_add(journal_block_count).saturating_add(1)
        {
            return Err(BlockManagerError::InvalidArgument(format!(
                "device too small for block manager: total_blocks={} journal_blocks={}",
                total_blocks, options.journal_blocks
            )));
        }

        let (data_start_block, segment_catalog_block_count, metadata_pool_block_count, segment_size_bytes) =
            if let Some((data_start_block, segment_catalog_block_count, metadata_pool_block_count, segment_size_bytes)) =
                derive_segment_layout(total_blocks, block_size, journal_start_block, journal_block_count)
            {
                (
                    data_start_block,
                    segment_catalog_block_count,
                    metadata_pool_block_count,
                    segment_size_bytes,
                )
            } else {
                (
                    journal_start_block.saturating_add(journal_block_count),
                    0,
                    total_blocks.saturating_sub(journal_start_block.saturating_add(journal_block_count)),
                    0,
                )
            };

        let header = DeviceHeader {
            device_id: Uuid::new_v4(),
            superblock_generation: 1,
            block_size,
            total_blocks,
            journal_start_block,
            journal_block_count,
            data_start_block,
            segment_catalog_start_block: journal_start_block.saturating_add(journal_block_count),
            segment_catalog_block_count,
            metadata_pool_block_count,
            segment_size_bytes,
        };

        zero_region_with_failpoint(
            &file,
            header.block_size,
            block_offset(&header, header.journal_start_block),
            header
                .journal_block_count
                .saturating_mul(u64::from(header.block_size)),
            "format.zero_journal",
        )?;
        if header.segment_catalog_block_count > 0 {
            zero_region_with_failpoint(
                &file,
                header.block_size,
                block_offset(&header, header.segment_catalog_start_block),
                header
                    .segment_catalog_block_count
                    .saturating_mul(u64::from(header.block_size)),
                "format.zero_segment_catalog",
            )?;
        }

        let mut header_block = vec![0u8; header.block_size as usize];
        encode_header(&header, &mut header_block)?;
        // Atomic-ish formatter protocol:
        // 1) write backup superblock copy
        // 2) sync
        // 3) write primary superblock
        // 4) sync
        write_all_at_with_failpoint(
            &file,
            &header_block,
            block_offset(&header, SECONDARY_SUPERBLOCK_BLOCK),
            "format.backup_superblock.write",
        )?;
        sync_all_with_failpoint(&file, "format.backup_superblock.sync")?;
        write_all_at_with_failpoint(&file, &header_block, 0, "format.primary_superblock.write")?;
        sync_all_with_failpoint(&file, "format.primary_superblock.sync")?;

        Self::open(path)
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, BlockManagerError> {
        let path = path.as_ref().to_path_buf();
        let meta_file = OpenOptions::new().read(true).write(true).open(&path)?;
        let header = read_header(&meta_file)?;
        let direct_file = Arc::new(open_direct_file(&path)?);
        let metadata_blocks = if header.metadata_pool_block_count > 0 {
            header.metadata_pool_block_count
        } else {
            header.total_blocks.saturating_sub(header.data_start_block)
        };
        let ag_count = derive_ag_count(metadata_blocks, header.block_size);

        let mut manager = Self {
            path,
            meta_file,
            direct_file,
            allocation_state: AllocationState::new(header.data_start_block, metadata_blocks, ag_count),
            allocations: HashMap::new(),
            allocation_generations: HashMap::new(),
            segment_pool: None,
            next_allocation_id: 1,
            next_journal_txn_id: 1,
            last_applied_journal_txn_id: 0,
            journal_cursor: 0,
            journal_dirty: false,
            segment_catalog_dirty: false,
            header,
        };
        manager.load_segment_pool_state()?;
        manager.replay_journal()?;
        manager.validate_allocator_state()?;
        Ok(manager)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn header(&self) -> &DeviceHeader {
        &self.header
    }

    fn segment_pool_enabled(&self) -> bool {
        self.header.segment_catalog_block_count > 0
            && self.header.segment_size_bytes > 0
            && self.header.metadata_pool_block_count
                < self
                    .header
                    .total_blocks
                    .saturating_sub(self.header.data_start_block)
    }

    fn segment_data_start_block(&self) -> u64 {
        self.header
            .data_start_block
            .saturating_add(self.header.metadata_pool_block_count)
    }

    fn segment_pool_total_blocks(&self) -> u64 {
        self.header
            .total_blocks
            .saturating_sub(self.segment_data_start_block())
    }

    fn segment_block_count(&self) -> u64 {
        if self.header.segment_size_bytes == 0 {
            0
        } else {
            div_ceil(self.header.segment_size_bytes, u64::from(self.header.block_size))
        }
    }

    fn segment_catalog_slot_offset(&self, segment_id: u32, copy_idx: u64) -> u64 {
        block_offset(&self.header, self.header.segment_catalog_start_block)
            .saturating_add(u64::from(segment_id).saturating_mul(SEGMENT_RECORD_SLOT_BYTES))
            .saturating_add(copy_idx.saturating_mul(SEGMENT_RECORD_BYTES as u64))
    }

    fn load_segment_pool_state(&mut self) -> Result<(), BlockManagerError> {
        if !self.segment_pool_enabled() {
            self.segment_pool = None;
            return Ok(());
        }
        let segment_block_count = self.segment_block_count();
        if segment_block_count == 0 {
            self.segment_pool = None;
            return Ok(());
        }
        let segment_count = self.segment_pool_total_blocks() / segment_block_count;
        if segment_count == 0 {
            self.segment_pool = None;
            return Ok(());
        }

        let mut entries = Vec::with_capacity(segment_count as usize);
        let mut active_segment_id = None;
        for segment_idx in 0..segment_count {
            let mut best: Option<SegmentCatalogEntry> = None;
            for copy_idx in 0..2 {
                let mut buf = [0u8; SEGMENT_RECORD_BYTES];
                read_exact_at(
                    &self.meta_file,
                    &mut buf,
                    self.segment_catalog_slot_offset(segment_idx as u32, copy_idx),
                )?;
                let entry = decode_segment_record(&buf)?;
                if best
                    .as_ref()
                    .map(|current| entry.sequence > current.sequence)
                    .unwrap_or(true)
                {
                    best = Some(entry);
                }
            }
            let mut entry = best.unwrap_or_default();
            let max_write = self.header.segment_size_bytes.min(u64::from(u32::MAX)) as u32;
            entry.write_pointer_bytes = entry.write_pointer_bytes.min(max_write);
            entry.dead_bytes = entry.dead_bytes.min(entry.write_pointer_bytes);
            if entry.generation == 0 {
                entry.generation = 1;
            }
            if entry.state == SEGMENT_STATE_OPEN {
                if active_segment_id.is_none() {
                    active_segment_id = Some(segment_idx as u32);
                } else {
                    entry.state = SEGMENT_STATE_SEALED;
                }
            }
            entries.push(entry);
        }

        self.segment_pool = Some(SegmentPoolState {
            segment_start_block: self.segment_data_start_block(),
            segment_block_count,
            segment_size_bytes: self.header.segment_size_bytes,
            entries,
            active_segment_id,
        });
        Ok(())
    }

    fn persist_segment_entry(
        &mut self,
        segment_id: u32,
        sync_immediately: bool,
    ) -> Result<(), BlockManagerError> {
        let entry = self
            .segment_pool
            .as_ref()
            .and_then(|pool| pool.entries.get(segment_id as usize).copied())
            .ok_or_else(|| {
                BlockManagerError::InvalidArgument(format!(
                    "segment {} is outside segment pool",
                    segment_id
                ))
            })?;
        let copy_idx = entry.sequence & 1;
        let buf = encode_segment_record(entry);
        write_all_at_with_failpoint(
            &self.meta_file,
            &buf,
            self.segment_catalog_slot_offset(segment_id, copy_idx),
            "segment.catalog.write",
        )?;
        self.segment_catalog_dirty = true;
        if sync_immediately {
            sync_all_with_failpoint(&self.meta_file, "segment.catalog.sync")?;
            self.segment_catalog_dirty = false;
        }
        Ok(())
    }

    fn recycle_fully_dead_segments(&mut self) -> Result<bool, BlockManagerError> {
        let mut recycled_any = false;
        let active_segment_id = self
            .segment_pool
            .as_ref()
            .and_then(|pool| pool.active_segment_id);
        let mut recycle_ids = Vec::new();
        if let Some(pool) = self.segment_pool.as_ref() {
            for (segment_idx, entry) in pool.entries.iter().enumerate() {
                if Some(segment_idx as u32) == active_segment_id {
                    continue;
                }
                if entry.state == SEGMENT_STATE_SEALED
                    && entry.write_pointer_bytes > 0
                    && entry.dead_bytes >= entry.write_pointer_bytes
                {
                    recycle_ids.push(segment_idx as u32);
                }
            }
        }
        for segment_id in recycle_ids {
            if let Some(entry) = self
                .segment_pool
                .as_mut()
                .and_then(|pool| pool.entries.get_mut(segment_id as usize))
            {
                entry.sequence = entry.sequence.saturating_add(1);
                entry.generation = entry.generation.saturating_add(1).max(1);
                entry.write_pointer_bytes = 0;
                entry.dead_bytes = 0;
                entry.state = SEGMENT_STATE_FREE;
                self.persist_segment_entry(segment_id, true)?;
                recycled_any = true;
            }
        }
        Ok(recycled_any)
    }

    fn ensure_open_segment(&mut self, required_bytes: u32) -> Result<u32, BlockManagerError> {
        if !self.segment_pool_enabled() {
            return Err(BlockManagerError::NoSpace {
                requested_blocks: div_ceil(u64::from(required_bytes), u64::from(self.header.block_size)),
                available_blocks: 0,
            });
        }

        if let Some(pool) = self.segment_pool.as_ref() {
            if let Some(active_segment_id) = pool.active_segment_id {
                let entry = pool.entries[active_segment_id as usize];
                if entry.state == SEGMENT_STATE_OPEN
                    && u64::from(entry.write_pointer_bytes).saturating_add(u64::from(required_bytes))
                        <= pool.segment_size_bytes
                {
                    return Ok(active_segment_id);
                }
            }
        }

        if let Some(active_segment_id) = self
            .segment_pool
            .as_ref()
            .and_then(|pool| pool.active_segment_id)
        {
            if let Some(entry) = self
                .segment_pool
                .as_mut()
                .and_then(|pool| pool.entries.get_mut(active_segment_id as usize))
            {
                entry.sequence = entry.sequence.saturating_add(1);
                entry.state = SEGMENT_STATE_SEALED;
                self.persist_segment_entry(active_segment_id, true)?;
            }
            if let Some(pool) = self.segment_pool.as_mut() {
                pool.active_segment_id = None;
            }
        }

        let _ = self.recycle_fully_dead_segments()?;

        let mut free_segment_id = None;
        if let Some(pool) = self.segment_pool.as_ref() {
            for (segment_idx, entry) in pool.entries.iter().enumerate() {
                if entry.state == SEGMENT_STATE_FREE {
                    free_segment_id = Some(segment_idx as u32);
                    break;
                }
            }
        }
        let segment_id = free_segment_id.ok_or_else(|| BlockManagerError::NoSpace {
            requested_blocks: div_ceil(u64::from(required_bytes), u64::from(self.header.block_size)),
            available_blocks: self.available_blocks(),
        })?;
        if let Some(entry) = self
            .segment_pool
            .as_mut()
            .and_then(|pool| pool.entries.get_mut(segment_id as usize))
        {
            entry.sequence = entry.sequence.saturating_add(1);
            entry.write_pointer_bytes = 0;
            entry.dead_bytes = 0;
            entry.state = SEGMENT_STATE_OPEN;
            if entry.generation == 0 {
                entry.generation = 1;
            }
            self.persist_segment_entry(segment_id, true)?;
        }
        if let Some(pool) = self.segment_pool.as_mut() {
            pool.active_segment_id = Some(segment_id);
        }
        Ok(segment_id)
    }

    fn segment_pool_available_blocks(&self) -> u64 {
        let Some(pool) = self.segment_pool.as_ref() else {
            return 0;
        };
        let mut free_blocks = 0u64;
        for entry in &pool.entries {
            match entry.state {
                SEGMENT_STATE_FREE => free_blocks = free_blocks.saturating_add(pool.segment_block_count),
                SEGMENT_STATE_OPEN => {
                    let free_bytes = pool
                        .segment_size_bytes
                        .saturating_sub(u64::from(entry.write_pointer_bytes));
                    free_blocks = free_blocks
                        .saturating_add(div_ceil(free_bytes, u64::from(self.header.block_size)));
                }
                _ => {}
            }
        }
        free_blocks
    }

    fn segment_pool_used_blocks(&self) -> u64 {
        let Some(pool) = self.segment_pool.as_ref() else {
            return 0;
        };
        pool.entries
            .iter()
            .filter(|entry| entry.state != SEGMENT_STATE_FREE)
            .map(|entry| {
                div_ceil(
                    u64::from(entry.write_pointer_bytes),
                    u64::from(self.header.block_size),
                )
            })
            .sum()
    }

    fn metadata_pool_used_blocks(&self) -> u64 {
        self.header
            .metadata_pool_block_count
            .saturating_sub(self.allocation_state.available_blocks())
    }

    pub fn available_blocks(&self) -> u64 {
        self.allocation_state
            .available_blocks()
            .saturating_add(self.segment_pool_available_blocks())
    }

    pub fn available_bytes(&self) -> u64 {
        self.available_blocks()
            .saturating_mul(u64::from(self.header.block_size))
    }

    pub fn allocation_count(&self) -> usize {
        self.allocations.len()
    }

    pub fn allocation_handle(&self, allocation_id: u64) -> Option<AllocationHandle> {
        self.allocations
            .get(&allocation_id)
            .copied()
            .map(|extent| AllocationHandle {
                allocation_id,
                extent,
            })
    }

    pub fn allocation_handles(&self) -> Vec<AllocationHandle> {
        let mut handles = self
            .allocations
            .iter()
            .map(|(allocation_id, extent)| AllocationHandle {
                allocation_id: *allocation_id,
                extent: *extent,
            })
            .collect::<Vec<_>>();
        handles.sort_by_key(|h| h.allocation_id);
        handles
    }

    pub fn used_blocks(&self) -> u64 {
        self.metadata_pool_used_blocks()
            .saturating_add(self.segment_pool_used_blocks())
    }

    pub fn used_bytes(&self) -> u64 {
        self.used_blocks()
            .saturating_mul(u64::from(self.header.block_size))
    }

    pub fn journal_cursor(&self) -> u64 {
        self.journal_cursor
    }

    pub fn allocator_health_snapshot(&self) -> AllocatorHealthSnapshot {
        let journal_capacity_records = self.max_journal_records();
        let journal_used_records = self.journal_cursor.min(journal_capacity_records);
        let journal_free_records = journal_capacity_records.saturating_sub(journal_used_records);
        let data_blocks = self
            .header
            .total_blocks
            .saturating_sub(self.header.data_start_block);
        let free_blocks = self.available_blocks();
        let used_blocks = self.used_blocks();

        let mut largest_free_extent_blocks = 0u64;
        let mut ags = Vec::with_capacity(self.allocation_state.ags.len());
        for ag in &self.allocation_state.ags {
            let ag_largest_free = ag.by_size.keys().next_back().copied().unwrap_or(0);
            largest_free_extent_blocks = largest_free_extent_blocks.max(ag_largest_free);
            ags.push(AllocationGroupHealth {
                ag_id: ag.ag_id,
                start_block: ag.start_block,
                total_blocks: ag.block_count,
                free_blocks: ag.free_blocks,
                largest_free_extent_blocks: ag_largest_free,
                fragmentation_ppm: fragmentation_ppm(ag.free_blocks, ag_largest_free),
            });
        }
        if let Some(pool) = self.segment_pool.as_ref() {
            for entry in &pool.entries {
                let segment_largest_free = match entry.state {
                    SEGMENT_STATE_FREE => pool.segment_block_count,
                    SEGMENT_STATE_OPEN => div_ceil(
                        pool.segment_size_bytes
                            .saturating_sub(u64::from(entry.write_pointer_bytes)),
                        u64::from(self.header.block_size),
                    ),
                    _ => 0,
                };
                largest_free_extent_blocks = largest_free_extent_blocks.max(segment_largest_free);
            }
        }

        AllocatorHealthSnapshot {
            superblock_generation: self.header.superblock_generation,
            replay_last_txid: self.last_applied_journal_txn_id,
            next_txid: self.next_journal_txn_id,
            journal_used_records,
            journal_capacity_records,
            journal_free_records,
            data_blocks,
            used_blocks,
            free_blocks,
            allocation_count: self.allocation_count(),
            largest_free_extent_blocks,
            fragmentation_ppm: fragmentation_ppm(free_blocks, largest_free_extent_blocks),
            ags,
        }
    }

    pub fn allocate_bytes(
        &mut self,
        size_bytes: u64,
    ) -> Result<AllocationHandle, BlockManagerError> {
        if size_bytes == 0 {
            return Err(BlockManagerError::InvalidArgument(
                "allocation size must be greater than 0".to_string(),
            ));
        }
        let requested_blocks = div_ceil(size_bytes, u64::from(self.header.block_size));
        let chosen = self
            .allocation_state
            .find_best_fit_extent(requested_blocks)
            .ok_or_else(|| BlockManagerError::NoSpace {
                requested_blocks,
                available_blocks: self.available_blocks(),
            })?;

        let allocation_id = self.next_allocation_id;
        self.append_journal_txn(JournalOp::Allocate, allocation_id, chosen)?;

        self.allocation_state.remove_known_extent(chosen)?;

        self.allocations.insert(allocation_id, chosen);
        self.allocation_generations.insert(allocation_id, 1);
        self.next_allocation_id = self.next_allocation_id.saturating_add(1);

        Ok(AllocationHandle {
            allocation_id,
            extent: chosen,
        })
    }

    pub fn free(&mut self, allocation_id: u64) -> Result<(), BlockManagerError> {
        let extent = *self
            .allocations
            .get(&allocation_id)
            .ok_or(BlockManagerError::UnknownAllocation(allocation_id))?;

        self.append_journal_txn(JournalOp::Free, allocation_id, extent)?;

        self.allocations.remove(&allocation_id);
        self.allocation_generations.remove(&allocation_id);
        self.allocation_state.free_extent(extent)?;
        Ok(())
    }

    pub fn free_addresses(
        &mut self,
        addresses: &[RawExtentAddress],
    ) -> Result<usize, BlockManagerError> {
        if addresses.is_empty() {
            return Ok(0);
        }

        let mut seen = std::collections::HashSet::<u64>::new();
        let mut pending = Vec::<(u64, Extent)>::with_capacity(addresses.len());
        for address in addresses {
            if is_segment_allocation_id(address.allocation_id) {
                if seen.insert(address.allocation_id) {
                    self.free_address(address)?;
                }
                continue;
            }
            let extent = self.validate_raw_address(address)?;
            if seen.insert(address.allocation_id) {
                pending.push((address.allocation_id, extent));
            }
        }

        self.reserve_journal_capacity(pending.len().saturating_mul(2))?;

        for (allocation_id, extent) in &pending {
            self.append_journal_txn(JournalOp::Free, *allocation_id, *extent)?;

            self.allocations
                .remove(allocation_id)
                .ok_or(BlockManagerError::UnknownAllocation(*allocation_id))?;
            self.allocation_generations.remove(allocation_id);
            self.allocation_state.free_extent(*extent)?;
        }

        Ok(pending.len())
    }

    pub fn write(
        &self,
        handle: AllocationHandle,
        offset: u64,
        data: &[u8],
    ) -> Result<(), BlockManagerError> {
        let capacity = handle.capacity_bytes(self.header.block_size);
        if offset.saturating_add(data.len() as u64) > capacity {
            return Err(BlockManagerError::OutOfBounds {
                allocation_id: handle.allocation_id,
                offset,
                len: data.len(),
                capacity,
            });
        }
        let absolute = block_offset(&self.header, handle.extent.start_block).saturating_add(offset);
        write_all_at_direct(&self.direct_file, self.header.block_size, absolute, data)?;
        Ok(())
    }

    pub fn read(
        &self,
        handle: AllocationHandle,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, BlockManagerError> {
        let capacity = handle.capacity_bytes(self.header.block_size);
        if offset.saturating_add(len as u64) > capacity {
            return Err(BlockManagerError::OutOfBounds {
                allocation_id: handle.allocation_id,
                offset,
                len,
                capacity,
            });
        }
        let absolute = block_offset(&self.header, handle.extent.start_block).saturating_add(offset);
        let mut out = vec![0u8; len];
        read_exact_at_direct(
            &self.direct_file,
            self.header.block_size,
            absolute,
            &mut out,
        )?;
        Ok(out)
    }

    pub fn flush(&self) -> Result<(), BlockManagerError> {
        self.direct_file.sync_all()?;
        self.meta_file.sync_all()?;
        Ok(())
    }

    pub fn flush_data(&self) -> Result<(), BlockManagerError> {
        self.direct_file.sync_all()?;
        Ok(())
    }

    pub fn flush_meta(&mut self) -> Result<(), BlockManagerError> {
        if self.journal_dirty || self.segment_catalog_dirty {
            sync_all_with_failpoint(&self.meta_file, "flush_meta.sync")?;
            self.journal_dirty = false;
            self.segment_catalog_dirty = false;
        }
        Ok(())
    }

    pub fn raw_extent_address(
        &self,
        handle: AllocationHandle,
        size_bytes: u64,
    ) -> Result<RawExtentAddress, BlockManagerError> {
        let capacity = handle.capacity_bytes(self.header.block_size);
        if size_bytes == 0 || size_bytes > capacity {
            return Err(BlockManagerError::InvalidArgument(format!(
                "raw extent size_bytes {} exceeds allocation capacity {}",
                size_bytes, capacity
            )));
        }
        Ok(RawExtentAddress {
            device_id: self.header.device_id,
            allocation_id: handle.allocation_id,
            extent_gen: self
                .allocation_generations
                .get(&handle.allocation_id)
                .copied()
                .unwrap_or(1),
            offset_bytes: block_offset(&self.header, handle.extent.start_block),
            size_bytes,
        })
    }

    fn validate_raw_address(
        &self,
        address: &RawExtentAddress,
    ) -> Result<Extent, BlockManagerError> {
        if address.device_id != self.header.device_id {
            return Err(BlockManagerError::InvalidArgument(format!(
                "address device {} does not match manager device {}",
                address.device_id, self.header.device_id
            )));
        }

        if is_segment_allocation_id(address.allocation_id) {
            let (segment_id, generation, offset_blocks) =
                decode_segment_allocation_id(address.allocation_id)?;
            let pool = self
                .segment_pool
                .as_ref()
                .ok_or(BlockManagerError::UnknownAllocation(address.allocation_id))?;
            let Some(entry) = pool.entries.get(segment_id as usize) else {
                return Err(BlockManagerError::UnknownAllocation(address.allocation_id));
            };
            if entry.generation != generation
                || address.extent_gen != generation
                || entry.state == SEGMENT_STATE_FREE
            {
                return Err(BlockManagerError::UnknownAllocation(address.allocation_id));
            }
            let expected_offset = block_offset(
                &self.header,
                pool.segment_start_block
                    .saturating_add(u64::from(segment_id).saturating_mul(pool.segment_block_count))
                    .saturating_add(u64::from(offset_blocks)),
            );
            if address.offset_bytes != expected_offset {
                return Err(BlockManagerError::Corrupt(format!(
                    "segment-backed address offset {} does not match expected offset {}",
                    address.offset_bytes, expected_offset
                )));
            }
            let start_offset_bytes =
                u64::from(offset_blocks).saturating_mul(u64::from(self.header.block_size));
            let end_offset_bytes = start_offset_bytes.saturating_add(address.size_bytes);
            if address.size_bytes == 0
                || end_offset_bytes > u64::from(entry.write_pointer_bytes)
                || end_offset_bytes > pool.segment_size_bytes
            {
                return Err(BlockManagerError::OutOfBounds {
                    allocation_id: address.allocation_id,
                    offset: 0,
                    len: address.size_bytes as usize,
                    capacity: u64::from(entry.write_pointer_bytes),
                });
            }
            return Ok(Extent {
                start_block: pool
                    .segment_start_block
                    .saturating_add(u64::from(segment_id).saturating_mul(pool.segment_block_count))
                    .saturating_add(u64::from(offset_blocks)),
                block_count: div_ceil(address.size_bytes, u64::from(self.header.block_size)),
            });
        }

        let extent = *self
            .allocations
            .get(&address.allocation_id)
            .ok_or(BlockManagerError::UnknownAllocation(address.allocation_id))?;
        let expected_gen = self
            .allocation_generations
            .get(&address.allocation_id)
            .copied()
            .ok_or_else(|| {
                BlockManagerError::Corrupt(format!(
                    "allocation generation missing for allocation {}",
                    address.allocation_id
                ))
            })?;
        if address.extent_gen != expected_gen {
            return Err(BlockManagerError::Corrupt(format!(
                "address generation {} does not match allocation {} generation {}",
                address.extent_gen, address.allocation_id, expected_gen
            )));
        }

        let expected_offset = block_offset(&self.header, extent.start_block);
        if address.offset_bytes != expected_offset {
            return Err(BlockManagerError::Corrupt(format!(
                "address offset {} does not match allocation {} offset {}",
                address.offset_bytes, address.allocation_id, expected_offset
            )));
        }

        let capacity = extent
            .block_count
            .saturating_mul(u64::from(self.header.block_size));
        if address.size_bytes == 0 || address.size_bytes > capacity {
            return Err(BlockManagerError::OutOfBounds {
                allocation_id: address.allocation_id,
                offset: 0,
                len: address.size_bytes as usize,
                capacity,
            });
        }

        Ok(extent)
    }

    pub fn read_address(
        &self,
        address: &RawExtentAddress,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, BlockManagerError> {
        let _extent = self.validate_raw_address(address)?;
        if offset.saturating_add(len as u64) > address.size_bytes {
            return Err(BlockManagerError::OutOfBounds {
                allocation_id: address.allocation_id,
                offset,
                len,
                capacity: address.size_bytes,
            });
        }
        let mut out = vec![0u8; len];
        read_exact_at_direct(
            &self.direct_file,
            self.header.block_size,
            address.offset_bytes.saturating_add(offset),
            &mut out,
        )?;
        Ok(out)
    }

    pub fn write_address(
        &self,
        address: &RawExtentAddress,
        offset: u64,
        data: &[u8],
    ) -> Result<(), BlockManagerError> {
        let _extent = self.validate_raw_address(address)?;
        if offset.saturating_add(data.len() as u64) > address.size_bytes {
            return Err(BlockManagerError::OutOfBounds {
                allocation_id: address.allocation_id,
                offset,
                len: data.len(),
                capacity: address.size_bytes,
            });
        }
        write_all_at_direct(
            &self.direct_file,
            self.header.block_size,
            address.offset_bytes.saturating_add(offset),
            data,
        )?;
        Ok(())
    }

    pub fn direct_extent_io(
        &self,
        address: &RawExtentAddress,
    ) -> Result<DirectExtentIo, BlockManagerError> {
        let _extent = self.validate_raw_address(address)?;
        Ok(DirectExtentIo {
            file: Arc::clone(&self.direct_file),
            block_size: self.header.block_size,
            address: address.clone(),
        })
    }

    pub fn allocate_raw_extent_address(
        &mut self,
        size_bytes: u64,
    ) -> Result<RawExtentAddress, BlockManagerError> {
        if self.segment_pool_enabled() && size_bytes <= self.header.segment_size_bytes {
            let aligned_size =
                div_ceil(size_bytes, u64::from(self.header.block_size))
                    .saturating_mul(u64::from(self.header.block_size));
            let aligned_size_u32 = u32::try_from(aligned_size).map_err(|_| {
                BlockManagerError::InvalidArgument(format!(
                    "allocation size {} exceeds supported segment allocation range",
                    size_bytes
                ))
            })?;
            loop {
                let segment_id = self.ensure_open_segment(aligned_size_u32)?;
                let segment_base_offset = {
                    let pool = self.segment_pool.as_ref().expect("segment pool enabled");
                    block_offset(
                        &self.header,
                        pool.segment_start_block
                            .saturating_add(u64::from(segment_id).saturating_mul(pool.segment_block_count)),
                    )
                };
                let Some(entry) = self
                    .segment_pool
                    .as_ref()
                    .and_then(|pool| pool.entries.get(segment_id as usize))
                    .copied()
                else {
                    return Err(BlockManagerError::Corrupt(format!(
                        "active segment {} missing from segment pool",
                        segment_id
                    )));
                };
                if u64::from(entry.write_pointer_bytes).saturating_add(aligned_size)
                    > self.header.segment_size_bytes
                {
                    if let Some(entry) = self
                        .segment_pool
                        .as_mut()
                        .and_then(|pool| pool.entries.get_mut(segment_id as usize))
                    {
                        entry.sequence = entry.sequence.saturating_add(1);
                        entry.state = SEGMENT_STATE_SEALED;
                    }
                    self.persist_segment_entry(segment_id, true)?;
                    if let Some(pool) = self.segment_pool.as_mut() {
                        pool.active_segment_id = None;
                    }
                    continue;
                }

                let start_offset_bytes = entry.write_pointer_bytes;
                let start_offset_blocks = start_offset_bytes / self.header.block_size;
                let generation = entry.generation;
                if let Some(entry) = self
                    .segment_pool
                    .as_mut()
                    .and_then(|pool| pool.entries.get_mut(segment_id as usize))
                {
                    entry.sequence = entry.sequence.saturating_add(1);
                    entry.write_pointer_bytes =
                        entry.write_pointer_bytes.saturating_add(aligned_size_u32);
                }
                self.persist_segment_entry(segment_id, true)?;
                return Ok(RawExtentAddress {
                    device_id: self.header.device_id,
                    allocation_id: encode_segment_allocation_id(
                        segment_id,
                        generation,
                        start_offset_blocks,
                    )?,
                    extent_gen: generation,
                    offset_bytes: segment_base_offset.saturating_add(u64::from(start_offset_bytes)),
                    size_bytes: aligned_size,
                });
            }
        }

        let handle = self.allocate_bytes(size_bytes)?;
        match self.raw_extent_address(handle, size_bytes) {
            Ok(address) => Ok(address),
            Err(err) => {
                let _ = self.free(handle.allocation_id);
                Err(err)
            }
        }
    }

    pub fn allocate_direct_extent_io(
        &mut self,
        size_bytes: u64,
    ) -> Result<(RawExtentAddress, DirectExtentIo), BlockManagerError> {
        let address = self.allocate_raw_extent_address(size_bytes)?;
        let io = DirectExtentIo {
            file: Arc::clone(&self.direct_file),
            block_size: self.header.block_size,
            address: address.clone(),
        };
        Ok((address, io))
    }

    pub fn free_address(&mut self, address: &RawExtentAddress) -> Result<(), BlockManagerError> {
        let _extent = self.validate_raw_address(address)?;
        if is_segment_allocation_id(address.allocation_id) {
            let (segment_id, _generation, _offset_blocks) =
                decode_segment_allocation_id(address.allocation_id)?;
            let active_segment_id = self
                .segment_pool
                .as_ref()
                .and_then(|pool| pool.active_segment_id);
            let mut should_recycle = false;
            if let Some(entry) = self
                .segment_pool
                .as_mut()
                .and_then(|pool| pool.entries.get_mut(segment_id as usize))
            {
                entry.sequence = entry.sequence.saturating_add(1);
                entry.dead_bytes = entry
                    .dead_bytes
                    .saturating_add(address.size_bytes.min(u64::from(u32::MAX)) as u32)
                    .min(entry.write_pointer_bytes);
                if entry.state == SEGMENT_STATE_SEALED
                    && entry.dead_bytes >= entry.write_pointer_bytes
                    && Some(segment_id) != active_segment_id
                {
                    entry.sequence = entry.sequence.saturating_add(1);
                    entry.generation = entry.generation.saturating_add(1).max(1);
                    entry.write_pointer_bytes = 0;
                    entry.dead_bytes = 0;
                    entry.state = SEGMENT_STATE_FREE;
                    should_recycle = true;
                }
            }
            self.persist_segment_entry(segment_id, should_recycle)?;
            return Ok(());
        }
        self.free(address.allocation_id)
    }

    fn replay_journal(&mut self) -> Result<(), BlockManagerError> {
        let journal_bytes = self
            .header
            .journal_block_count
            .saturating_mul(u64::from(self.header.block_size));
        let max_records = journal_bytes / JOURNAL_RECORD_SIZE as u64;
        let mut pending_prepare = HashMap::<u64, JournalRecord>::new();
        let mut committed_by_txid = HashMap::<u64, JournalRecord>::new();
        let mut last_seen_txn_id = 0u64;

        for idx in 0..max_records {
            let offset = block_offset(&self.header, self.header.journal_start_block)
                .saturating_add(idx.saturating_mul(JOURNAL_RECORD_SIZE as u64));
            let mut record_buf = [0u8; JOURNAL_RECORD_SIZE];
            read_exact_at(&self.meta_file, &mut record_buf, offset)?;
            if record_buf[0..4] == [0u8; 4] {
                self.journal_cursor = idx;
                return Ok(());
            }
            let mut record = decode_record(&record_buf)?;
            if record.txn_id == 0 {
                // Legacy journals (without txn ids) are replayed in cursor order.
                record.txn_id = idx.saturating_add(1);
            }

            if record.txn_id < last_seen_txn_id {
                return Err(BlockManagerError::Corrupt(format!(
                    "journal txid regression at record {}: txid={} last_seen={}",
                    idx, record.txn_id, last_seen_txn_id
                )));
            }
            last_seen_txn_id = record.txn_id;
            self.next_journal_txn_id = self
                .next_journal_txn_id
                .max(record.txn_id.saturating_add(1));

            match record.op {
                JournalOp::Allocate | JournalOp::Free => {
                    if let Some(existing) = committed_by_txid.get(&record.txn_id) {
                        if existing.op == record.op
                            && existing.allocation_id == record.allocation_id
                            && existing.extent == record.extent
                        {
                            // Idempotent duplicate committed record.
                            self.journal_cursor = idx.saturating_add(1);
                            continue;
                        }
                        return Err(BlockManagerError::Corrupt(format!(
                            "duplicate committed txid={} payload mismatch",
                            record.txn_id
                        )));
                    }
                    // Legacy committed records are applied directly.
                    self.apply_record(record)?;
                    committed_by_txid.insert(record.txn_id, record);
                }
                JournalOp::PrepareAllocate | JournalOp::PrepareFree => {
                    if committed_by_txid.contains_key(&record.txn_id) {
                        return Err(BlockManagerError::Corrupt(format!(
                            "prepare record encountered after committed txid={}",
                            record.txn_id
                        )));
                    }
                    if let Some(existing) = pending_prepare.get(&record.txn_id) {
                        if existing.op == record.op
                            && existing.allocation_id == record.allocation_id
                            && existing.extent == record.extent
                        {
                            // Idempotent duplicate prepare for same txid/payload.
                        } else {
                            return Err(BlockManagerError::Corrupt(format!(
                                "duplicate prepare txid={} payload mismatch",
                                record.txn_id
                            )));
                        }
                    } else {
                        pending_prepare.insert(record.txn_id, record);
                    }
                }
                JournalOp::CommitAllocate | JournalOp::CommitFree => {
                    let duplicate_op = match record.op {
                        JournalOp::CommitAllocate => JournalOp::Allocate,
                        JournalOp::CommitFree => JournalOp::Free,
                        _ => unreachable!("non-commit op in commit branch"),
                    };
                    if let Some(existing) = committed_by_txid.get(&record.txn_id) {
                        if existing.op == duplicate_op
                            && existing.allocation_id == record.allocation_id
                            && existing.extent == record.extent
                        {
                            // Idempotent duplicate commit record.
                            self.journal_cursor = idx.saturating_add(1);
                            continue;
                        }
                        return Err(BlockManagerError::Corrupt(format!(
                            "duplicate committed txid={} payload mismatch",
                            record.txn_id
                        )));
                    }
                    let prepare = pending_prepare.remove(&record.txn_id).ok_or_else(|| {
                        BlockManagerError::Corrupt(format!(
                            "journal commit record has no matching prepare txid={}",
                            record.txn_id
                        ))
                    })?;
                    if prepare.allocation_id != record.allocation_id
                        || prepare.extent != record.extent
                    {
                        return Err(BlockManagerError::Corrupt(format!(
                            "journal prepare/commit payload mismatch txid={} prepare_alloc={} commit_alloc={}",
                            record.txn_id, prepare.allocation_id, record.allocation_id
                        )));
                    }
                    let op = JournalOp::from_prepare_commit_pair(prepare.op, record.op)
                        .map_err(|msg| BlockManagerError::Corrupt(msg.to_string()))?;
                    let committed = JournalRecord {
                        txn_id: record.txn_id,
                        op,
                        allocation_id: record.allocation_id,
                        extent: record.extent,
                    };
                    self.apply_record(committed)?;
                    committed_by_txid.insert(record.txn_id, committed);
                }
            }
            self.journal_cursor = idx.saturating_add(1);
        }

        Ok(())
    }

    fn append_journal_txn(
        &mut self,
        op: JournalOp,
        allocation_id: u64,
        extent: Extent,
    ) -> Result<(), BlockManagerError> {
        self.reserve_journal_capacity(2)?;
        let txn_id = self.next_journal_txn_id;
        let prepare_op = op
            .to_prepare()
            .map_err(|msg| BlockManagerError::InvalidArgument(msg.to_string()))?;
        let commit_op = op
            .to_commit()
            .map_err(|msg| BlockManagerError::InvalidArgument(msg.to_string()))?;
        self.append_journal_record(JournalRecord {
            txn_id,
            op: prepare_op,
            allocation_id,
            extent,
        })?;
        self.append_journal_record(JournalRecord {
            txn_id,
            op: commit_op,
            allocation_id,
            extent,
        })?;
        self.next_journal_txn_id = self.next_journal_txn_id.saturating_add(1);
        Ok(())
    }

    fn append_journal_record(&mut self, record: JournalRecord) -> Result<(), BlockManagerError> {
        self.ensure_journal_capacity(1)?;
        let offset = block_offset(&self.header, self.header.journal_start_block).saturating_add(
            self.journal_cursor
                .saturating_mul(JOURNAL_RECORD_SIZE as u64),
        );

        let buf = encode_record(record);
        write_all_at_with_failpoint(&self.meta_file, &buf, offset, "journal.record.write")?;
        self.journal_cursor = self.journal_cursor.saturating_add(1);
        self.journal_dirty = true;
        Ok(())
    }

    fn ensure_journal_capacity(&self, additional_records: usize) -> Result<(), BlockManagerError> {
        let max_records = self.max_journal_records();
        let required = self
            .journal_cursor
            .saturating_add(additional_records as u64);
        if required > max_records {
            return Err(BlockManagerError::JournalFull);
        }
        Ok(())
    }

    fn reserve_journal_capacity(
        &mut self,
        additional_records: usize,
    ) -> Result<(), BlockManagerError> {
        if self.ensure_journal_capacity(additional_records).is_ok() {
            return Ok(());
        }
        self.compact_journal_checkpoint()?;
        self.ensure_journal_capacity(additional_records)
    }

    fn max_journal_records(&self) -> u64 {
        self.header
            .journal_block_count
            .saturating_mul(u64::from(self.header.block_size))
            / JOURNAL_RECORD_SIZE as u64
    }

    fn persist_superblock_copies(&mut self) -> Result<(), BlockManagerError> {
        let mut header_block = vec![0u8; self.header.block_size as usize];
        encode_header(&self.header, &mut header_block)?;
        write_all_at_with_failpoint(
            &self.meta_file,
            &header_block,
            block_offset(&self.header, SECONDARY_SUPERBLOCK_BLOCK),
            "persist.backup_superblock.write",
        )?;
        sync_all_with_failpoint(&self.meta_file, "persist.backup_superblock.sync")?;
        write_all_at_with_failpoint(
            &self.meta_file,
            &header_block,
            0,
            "persist.primary_superblock.write",
        )?;
        sync_all_with_failpoint(&self.meta_file, "persist.primary_superblock.sync")?;
        Ok(())
    }

    fn compact_journal_checkpoint(&mut self) -> Result<(), BlockManagerError> {
        let mut live = self
            .allocations
            .iter()
            .map(|(allocation_id, extent)| (*allocation_id, *extent))
            .collect::<Vec<_>>();
        live.sort_by_key(|(allocation_id, _)| *allocation_id);

        let needed_records = live.len().saturating_mul(2) as u64;
        if needed_records > self.max_journal_records() {
            return Err(BlockManagerError::JournalFull);
        }

        let journal_offset = block_offset(&self.header, self.header.journal_start_block);
        let journal_bytes_len = self
            .header
            .journal_block_count
            .saturating_mul(u64::from(self.header.block_size));
        let mut prior_journal = vec![0u8; journal_bytes_len as usize];
        read_exact_at(&self.meta_file, &mut prior_journal, journal_offset)?;

        let prev_journal_cursor = self.journal_cursor;
        let prev_next_journal_txn_id = self.next_journal_txn_id;
        let prev_last_applied_journal_txn_id = self.last_applied_journal_txn_id;
        let prev_journal_dirty = self.journal_dirty;
        let prev_header = self.header.clone();

        let restore_prior_journal = |mgr: &mut Self| -> Result<(), BlockManagerError> {
            write_all_at(&mgr.meta_file, &prior_journal, journal_offset)?;
            let mut prev_header_block = vec![0u8; prev_header.block_size as usize];
            encode_header(&prev_header, &mut prev_header_block)?;
            write_all_at(
                &mgr.meta_file,
                &prev_header_block,
                block_offset(&prev_header, SECONDARY_SUPERBLOCK_BLOCK),
            )?;
            write_all_at(&mgr.meta_file, &prev_header_block, 0)?;
            mgr.meta_file.sync_all()?;
            mgr.journal_cursor = prev_journal_cursor;
            mgr.next_journal_txn_id = prev_next_journal_txn_id;
            mgr.last_applied_journal_txn_id = prev_last_applied_journal_txn_id;
            mgr.journal_dirty = prev_journal_dirty;
            mgr.header = prev_header.clone();
            Ok(())
        };

        let compaction_result = (|| -> Result<(), BlockManagerError> {
            zero_region_with_failpoint(
                &self.meta_file,
                self.header.block_size,
                journal_offset,
                journal_bytes_len,
                "checkpoint.zero_journal",
            )?;

            self.journal_cursor = 0;
            self.next_journal_txn_id = 1;
            self.last_applied_journal_txn_id = 0;
            self.journal_dirty = false;

            for (allocation_id, extent) in &live {
                let txn_id = self.next_journal_txn_id;
                self.append_journal_record(JournalRecord {
                    txn_id,
                    op: JournalOp::PrepareAllocate,
                    allocation_id: *allocation_id,
                    extent: *extent,
                })?;
                self.append_journal_record(JournalRecord {
                    txn_id,
                    op: JournalOp::CommitAllocate,
                    allocation_id: *allocation_id,
                    extent: *extent,
                })?;
                self.next_journal_txn_id = self.next_journal_txn_id.saturating_add(1);
            }
            self.last_applied_journal_txn_id = self.next_journal_txn_id.saturating_sub(1);
            self.header.superblock_generation = self.header.superblock_generation.saturating_add(1);
            self.persist_superblock_copies()?;
            self.journal_dirty = false;
            Ok(())
        })();

        if let Err(err) = compaction_result {
            if let Err(restore_err) = restore_prior_journal(self) {
                return Err(BlockManagerError::Corrupt(format!(
                    "checkpoint compaction failed ({}) and journal rollback failed ({})",
                    err, restore_err
                )));
            }
            return Err(err);
        }

        Ok(())
    }

    fn apply_record(&mut self, record: JournalRecord) -> Result<(), BlockManagerError> {
        if record.txn_id <= self.last_applied_journal_txn_id {
            return Ok(());
        }
        match record.op {
            JournalOp::Allocate => {
                self.allocation_state.remove_known_extent(record.extent)?;
                self.allocations.insert(record.allocation_id, record.extent);
                self.allocation_generations.insert(record.allocation_id, 1);
                self.next_allocation_id = self
                    .next_allocation_id
                    .max(record.allocation_id.saturating_add(1));
            }
            JournalOp::Free => {
                let existing = self.allocations.remove(&record.allocation_id).ok_or(
                    BlockManagerError::Corrupt(format!(
                        "journal free for unknown allocation {}",
                        record.allocation_id
                    )),
                )?;
                if existing != record.extent {
                    return Err(BlockManagerError::Corrupt(format!(
                        "journal free extent mismatch for allocation {}",
                        record.allocation_id
                    )));
                }
                self.allocation_generations.remove(&record.allocation_id);
                self.allocation_state.free_extent(record.extent)?;
            }
            JournalOp::PrepareAllocate
            | JournalOp::PrepareFree
            | JournalOp::CommitAllocate
            | JournalOp::CommitFree => {
                return Err(BlockManagerError::Corrupt(format!(
                    "apply_record received non-committed journal op {:?}",
                    record.op
                )));
            }
        }
        self.last_applied_journal_txn_id = record.txn_id;
        Ok(())
    }

    fn validate_allocator_state(&self) -> Result<(), BlockManagerError> {
        let data_start = self.header.data_start_block;
        let data_blocks = if self.segment_pool_enabled() {
            self.header.metadata_pool_block_count
        } else {
            self.header.total_blocks.saturating_sub(data_start)
        };
        let data_end = data_start.saturating_add(data_blocks);

        if data_blocks > 0 && self.allocation_state.ags.is_empty() {
            return Err(BlockManagerError::Corrupt(
                "allocator has no allocation groups for non-empty data region".to_string(),
            ));
        }
        if self.allocation_state.ag_max_free_len.len() != self.allocation_state.ags.len() {
            return Err(BlockManagerError::Corrupt(format!(
                "allocator AG max-free index length mismatch: got {} expected {}",
                self.allocation_state.ag_max_free_len.len(),
                self.allocation_state.ags.len()
            )));
        }
        if self.allocation_state.ag_start_blocks.len() != self.allocation_state.ags.len() {
            return Err(BlockManagerError::Corrupt(format!(
                "allocator AG start index length mismatch: got {} expected {}",
                self.allocation_state.ag_start_blocks.len(),
                self.allocation_state.ags.len()
            )));
        }

        let mut expected_bucket_index = BTreeMap::<u64, BTreeSet<usize>>::new();
        let mut expected_ag_start = data_start;
        for (idx, ag) in self.allocation_state.ags.iter().enumerate() {
            if ag.ag_id != idx {
                return Err(BlockManagerError::Corrupt(format!(
                    "allocator AG id mismatch: slot {} has ag_id {}",
                    idx, ag.ag_id
                )));
            }
            if ag.block_count == 0 {
                return Err(BlockManagerError::Corrupt(format!(
                    "allocator AG {} has zero block count",
                    ag.ag_id
                )));
            }
            if ag.start_block != expected_ag_start {
                return Err(BlockManagerError::Corrupt(format!(
                    "allocator AG {} start mismatch: got {} expected {}",
                    ag.ag_id, ag.start_block, expected_ag_start
                )));
            }
            let indexed_start = self.allocation_state.ag_start_blocks[idx];
            if indexed_start != ag.start_block {
                return Err(BlockManagerError::Corrupt(format!(
                    "allocator AG {} start index mismatch: got {} expected {}",
                    ag.ag_id, indexed_start, ag.start_block
                )));
            }
            expected_ag_start = expected_ag_start.saturating_add(ag.block_count);

            let mut expected_free = 0u64;
            let mut last_end = ag.start_block;
            let mut expected_by_size = BTreeMap::<u64, BTreeSet<u64>>::new();
            for (start, extent) in &ag.by_addr {
                if *start != extent.start_block {
                    return Err(BlockManagerError::Corrupt(format!(
                        "allocator AG {} free index start mismatch key={} extent_start={}",
                        ag.ag_id, start, extent.start_block
                    )));
                }
                if extent.block_count == 0 {
                    return Err(BlockManagerError::Corrupt(format!(
                        "allocator AG {} contains zero-length free extent at block {}",
                        ag.ag_id, extent.start_block
                    )));
                }
                let extent_end = extent.start_block.saturating_add(extent.block_count);
                let ag_end = ag.start_block.saturating_add(ag.block_count);
                if extent.start_block < ag.start_block || extent_end > ag_end {
                    return Err(BlockManagerError::Corrupt(format!(
                        "allocator AG {} free extent {:?} escapes AG range {}..{}",
                        ag.ag_id, extent, ag.start_block, ag_end
                    )));
                }
                if extent.start_block < last_end {
                    return Err(BlockManagerError::Corrupt(format!(
                        "allocator AG {} has overlapping free extents near block {}",
                        ag.ag_id, extent.start_block
                    )));
                }
                last_end = extent_end;
                expected_free = expected_free.saturating_add(extent.block_count);
                expected_by_size
                    .entry(extent.block_count)
                    .or_default()
                    .insert(extent.start_block);
            }
            if ag.free_blocks != expected_free {
                return Err(BlockManagerError::Corrupt(format!(
                    "allocator AG {} free block accounting mismatch: got {} expected {}",
                    ag.ag_id, ag.free_blocks, expected_free
                )));
            }
            if ag.by_size != expected_by_size {
                return Err(BlockManagerError::Corrupt(format!(
                    "allocator AG {} free index by-size drift detected",
                    ag.ag_id
                )));
            }
            let expected_max = ag.by_size.keys().next_back().copied().unwrap_or(0);
            let indexed_max = self.allocation_state.ag_max_free_len[idx];
            if indexed_max != expected_max {
                return Err(BlockManagerError::Corrupt(format!(
                    "allocator AG {} max-free mismatch: got {} expected {}",
                    ag.ag_id, indexed_max, expected_max
                )));
            }
            if expected_max > 0 {
                expected_bucket_index
                    .entry(expected_max)
                    .or_default()
                    .insert(idx);
            }
        }
        if expected_ag_start != data_end {
            return Err(BlockManagerError::Corrupt(format!(
                "allocator AG coverage mismatch: ended at block {} expected {}",
                expected_ag_start, data_end
            )));
        }
        if self.allocation_state.ags_by_max_free_len != expected_bucket_index {
            return Err(BlockManagerError::Corrupt(
                "allocator AG max-free reverse index drift detected".to_string(),
            ));
        }

        let mut live_extents = self
            .allocations
            .iter()
            .map(|(allocation_id, extent)| (*allocation_id, *extent))
            .collect::<Vec<_>>();
        if self.allocation_generations.len() != self.allocations.len() {
            return Err(BlockManagerError::Corrupt(format!(
                "allocation generation map size mismatch: gens={} allocs={}",
                self.allocation_generations.len(),
                self.allocations.len()
            )));
        }
        live_extents.sort_by_key(|(_, extent)| extent.start_block);

        let mut used_blocks = 0u64;
        let mut prev_live_end = data_start;
        for (allocation_id, extent) in live_extents {
            if !self.allocation_generations.contains_key(&allocation_id) {
                return Err(BlockManagerError::Corrupt(format!(
                    "missing generation entry for allocation {}",
                    allocation_id
                )));
            }
            if extent.block_count == 0 {
                return Err(BlockManagerError::Corrupt(format!(
                    "allocation {} has zero block length",
                    allocation_id
                )));
            }
            let extent_end = extent.start_block.saturating_add(extent.block_count);
            if extent.start_block < data_start || extent_end > data_end {
                return Err(BlockManagerError::Corrupt(format!(
                    "allocation {} extent {:?} escapes data range {}..{}",
                    allocation_id, extent, data_start, data_end
                )));
            }
            if extent.start_block < prev_live_end {
                return Err(BlockManagerError::Corrupt(format!(
                    "live allocation overlap detected around allocation {}",
                    allocation_id
                )));
            }
            prev_live_end = extent_end;
            used_blocks = used_blocks.saturating_add(extent.block_count);

            let ag_idx = self
                .allocation_state
                .ag_index_for_block(extent.start_block)
                .ok_or_else(|| {
                    BlockManagerError::Corrupt(format!(
                        "allocation {} starts outside AG ranges",
                        allocation_id
                    ))
                })?;
            let ag = &self.allocation_state.ags[ag_idx];
            let ag_end = ag.start_block.saturating_add(ag.block_count);
            if extent_end > ag_end {
                return Err(BlockManagerError::Corrupt(format!(
                    "allocation {} extent {:?} crosses AG {} boundary {}..{}",
                    allocation_id, extent, ag.ag_id, ag.start_block, ag_end
                )));
            }

            if let Some((_start, free_extent)) = ag.by_addr.range(..=extent.start_block).next_back()
            {
                let free_end = free_extent
                    .start_block
                    .saturating_add(free_extent.block_count);
                if extent.start_block < free_end && extent_end > free_extent.start_block {
                    return Err(BlockManagerError::Corrupt(format!(
                        "allocation {} extent {:?} overlaps free extent {:?}",
                        allocation_id, extent, free_extent
                    )));
                }
            }
        }

        let free_blocks = self.allocation_state.available_blocks();
        if used_blocks.saturating_add(free_blocks) != data_blocks {
            return Err(BlockManagerError::Corrupt(format!(
                "allocator accounting mismatch: used {} + free {} != data {}",
                used_blocks, free_blocks, data_blocks
            )));
        }

        Ok(())
    }
}

impl DirectExtentIo {
    pub fn raw_address(&self) -> &RawExtentAddress {
        &self.address
    }

    pub fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>, BlockManagerError> {
        if offset.saturating_add(len as u64) > self.address.size_bytes {
            return Err(BlockManagerError::OutOfBounds {
                allocation_id: self.address.allocation_id,
                offset,
                len,
                capacity: self.address.size_bytes,
            });
        }

        let mut out = vec![0u8; len];
        read_exact_at_direct(
            self.file.as_ref(),
            self.block_size,
            self.address.offset_bytes.saturating_add(offset),
            &mut out,
        )?;
        Ok(out)
    }

    pub fn read_into(&self, offset: u64, buf: &mut [u8]) -> Result<(), BlockManagerError> {
        if offset.saturating_add(buf.len() as u64) > self.address.size_bytes {
            return Err(BlockManagerError::OutOfBounds {
                allocation_id: self.address.allocation_id,
                offset,
                len: buf.len(),
                capacity: self.address.size_bytes,
            });
        }

        read_exact_at_direct(
            self.file.as_ref(),
            self.block_size,
            self.address.offset_bytes.saturating_add(offset),
            buf,
        )?;
        Ok(())
    }

    pub fn write(&self, offset: u64, data: &[u8]) -> Result<(), BlockManagerError> {
        if offset.saturating_add(data.len() as u64) > self.address.size_bytes {
            return Err(BlockManagerError::OutOfBounds {
                allocation_id: self.address.allocation_id,
                offset,
                len: data.len(),
                capacity: self.address.size_bytes,
            });
        }
        write_all_at_direct(
            self.file.as_ref(),
            self.block_size,
            self.address.offset_bytes.saturating_add(offset),
            data,
        )?;
        Ok(())
    }

    pub fn flush_data(&self) -> Result<(), BlockManagerError> {
        self.file.sync_all()?;
        Ok(())
    }
}

fn should_resize_backing_store(meta: &std::fs::Metadata) -> bool {
    if meta.is_file() {
        return true;
    }

    #[cfg(unix)]
    if meta.file_type().is_block_device() {
        return false;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crc32fast::Hasher;
    use std::collections::{BTreeMap, BTreeSet};
    use std::fs;
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::FileExt;
    use std::sync::{LazyLock, Mutex};
    use std::time::{SystemTime, UNIX_EPOCH};

    static FAILPOINT_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    fn temp_loopback(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "block-manager-{}-{}-{}",
            std::process::id(),
            unique,
            name
        ))
    }

    fn encode_legacy_v1_header_for_test(header: &DeviceHeader) -> Vec<u8> {
        let mut out = vec![0u8; header.block_size as usize];
        out[0..16].copy_from_slice(&crate::constants::SUPERBLOCK_MAGIC);
        out[16..20].copy_from_slice(&crate::constants::HEADER_VERSION_V1.to_le_bytes());
        out[20..24].copy_from_slice(&header.block_size.to_le_bytes());
        out[24..32].copy_from_slice(&header.total_blocks.to_le_bytes());
        out[32..40].copy_from_slice(&header.journal_start_block.to_le_bytes());
        out[40..48].copy_from_slice(&header.journal_block_count.to_le_bytes());
        out[48..56].copy_from_slice(&header.data_start_block.to_le_bytes());
        out[56..72].copy_from_slice(header.device_id.as_bytes());

        let mut hasher = Hasher::new();
        hasher.update(&out[..crate::constants::HEADER_CRC_OFFSET_V1]);
        let crc = hasher.finalize();
        out[crate::constants::HEADER_CRC_OFFSET_V1..crate::constants::HEADER_CRC_OFFSET_V1 + 4]
            .copy_from_slice(&crc.to_le_bytes());
        out
    }

    fn assert_allocation_state_consistent(state: &AllocationState) {
        assert_eq!(
            state.ag_max_free_len.len(),
            state.ags.len(),
            "ag_max_free_len should have one entry per AG"
        );
        assert_eq!(
            state.ag_start_blocks.len(),
            state.ags.len(),
            "ag_start_blocks should have one entry per AG"
        );

        let mut expected_reverse = BTreeMap::<u64, BTreeSet<usize>>::new();
        for (idx, ag) in state.ags.iter().enumerate() {
            assert_eq!(ag.ag_id, idx, "AG id should match index");
            assert_eq!(
                state.ag_start_blocks[idx], ag.start_block,
                "AG {} start index mismatch",
                ag.ag_id
            );
            assert!(
                ag.block_count > 0,
                "AG {} must contain at least one block",
                ag.ag_id
            );

            let mut expected_free = 0u64;
            let mut last_end = ag.start_block;
            let mut expected_by_size = BTreeMap::<u64, BTreeSet<u64>>::new();
            for (start, extent) in &ag.by_addr {
                assert_eq!(*start, extent.start_block, "by_addr key mismatch");
                assert!(
                    extent.block_count > 0,
                    "free extent must not have zero length"
                );
                let extent_end = extent.start_block.saturating_add(extent.block_count);
                let ag_end = ag.start_block.saturating_add(ag.block_count);
                assert!(
                    extent.start_block >= ag.start_block && extent_end <= ag_end,
                    "free extent {:?} escaped AG {} range {}..{}",
                    extent,
                    ag.ag_id,
                    ag.start_block,
                    ag_end
                );
                assert!(
                    extent.start_block >= last_end,
                    "free extents must not overlap in AG {}",
                    ag.ag_id
                );
                last_end = extent_end;
                expected_free = expected_free.saturating_add(extent.block_count);
                expected_by_size
                    .entry(extent.block_count)
                    .or_default()
                    .insert(extent.start_block);
            }
            assert_eq!(
                ag.free_blocks, expected_free,
                "AG {} free block accounting mismatch",
                ag.ag_id
            );
            assert_eq!(
                ag.by_size, expected_by_size,
                "AG {} by_size index drift",
                ag.ag_id
            );

            let expected_max = ag.by_size.keys().next_back().copied().unwrap_or(0);
            assert_eq!(
                state.ag_max_free_len[idx], expected_max,
                "AG {} max-free index mismatch",
                ag.ag_id
            );
            if expected_max > 0 {
                expected_reverse
                    .entry(expected_max)
                    .or_default()
                    .insert(idx);
            }
        }

        assert_eq!(
            state.ags_by_max_free_len, expected_reverse,
            "reverse AG max-free index drift"
        );

        let total_free: u64 = state.ags.iter().map(|ag| ag.free_blocks).sum();
        assert_eq!(
            total_free,
            state.available_blocks(),
            "available block drift"
        );
    }

    fn allocator_state_fingerprint(mgr: &BlockManager) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&mgr.header.superblock_generation.to_le_bytes());
        hasher.update(&mgr.journal_cursor.to_le_bytes());
        hasher.update(&mgr.last_applied_journal_txn_id.to_le_bytes());
        hasher.update(&mgr.next_journal_txn_id.to_le_bytes());
        hasher.update(&(mgr.allocations.len() as u64).to_le_bytes());

        let mut allocs = mgr
            .allocations
            .iter()
            .map(|(id, extent)| {
                (
                    *id,
                    *mgr.allocation_generations.get(id).unwrap_or(&0),
                    extent.start_block,
                    extent.block_count,
                )
            })
            .collect::<Vec<_>>();
        allocs.sort_unstable_by_key(|(id, ..)| *id);
        for (id, generation, start, len) in allocs {
            hasher.update(&id.to_le_bytes());
            hasher.update(&generation.to_le_bytes());
            hasher.update(&start.to_le_bytes());
            hasher.update(&len.to_le_bytes());
        }

        for ag in &mgr.allocation_state.ags {
            hasher.update(&(ag.ag_id as u64).to_le_bytes());
            hasher.update(&ag.start_block.to_le_bytes());
            hasher.update(&ag.block_count.to_le_bytes());
            hasher.update(&ag.free_blocks.to_le_bytes());
            hasher.update(&(ag.by_addr.len() as u64).to_le_bytes());
            for (start, extent) in &ag.by_addr {
                hasher.update(&start.to_le_bytes());
                hasher.update(&extent.block_count.to_le_bytes());
            }
        }

        hasher.finalize()
    }

    fn run_deterministic_faulted_workload(seed: u64, name: &str) -> u32 {
        clear_failpoints();
        let path = temp_loopback(name);
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(16 * 1024 * 1024),
            },
        )
        .expect("format device");
        let mut s = seed;

        for step in 0..320usize {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);

            match s & 0x0F {
                0 => set_failpoint("journal.record.write", 0),
                1 => set_failpoint("flush_meta.sync", 0),
                2 => set_failpoint("persist.primary_superblock.sync", 0),
                _ => {}
            }

            let op_selector = (s >> 4) & 0x07;
            if op_selector < 4 || mgr.allocations.is_empty() {
                let blocks = 1 + ((s >> 9) % 8);
                let size = blocks.saturating_mul(4096);
                match mgr.allocate_bytes(size) {
                    Ok(_) => {}
                    Err(BlockManagerError::Io(_))
                    | Err(BlockManagerError::NoSpace { .. })
                    | Err(BlockManagerError::JournalFull) => {}
                    Err(err) => panic!("allocate failed in deterministic fault loop: {:?}", err),
                }
            } else {
                let mut ids = mgr.allocations.keys().copied().collect::<Vec<_>>();
                ids.sort_unstable();
                let idx = ((s >> 17) as usize) % ids.len();
                let id = ids[idx];
                match mgr.free(id) {
                    Ok(()) => {}
                    Err(BlockManagerError::Io(_))
                    | Err(BlockManagerError::JournalFull)
                    | Err(BlockManagerError::UnknownAllocation(_)) => {}
                    Err(err) => panic!("free failed in deterministic fault loop: {:?}", err),
                }
            }

            if step % 11 == 0 {
                match mgr.flush_meta() {
                    Ok(()) | Err(BlockManagerError::Io(_)) => {}
                    Err(err) => panic!("flush_meta failed in fault loop: {:?}", err),
                }
            }

            if step % 29 == 0 {
                match mgr.compact_journal_checkpoint() {
                    Ok(())
                    | Err(BlockManagerError::Io(_))
                    | Err(BlockManagerError::JournalFull) => {}
                    Err(err) => panic!("checkpoint failed in fault loop: {:?}", err),
                }
            }

            clear_failpoints();
            mgr.flush().expect("flush before reopen in fault loop");
            drop(mgr);
            mgr = BlockManager::open(&path).expect("reopen in deterministic fault loop");
            mgr.validate_allocator_state()
                .expect("allocator state must stay valid in fault loop");
        }

        clear_failpoints();
        let fingerprint = allocator_state_fingerprint(&mgr);
        drop(mgr);
        let _ = fs::remove_file(path);
        fingerprint
    }

    #[test]
    fn ag_count_scales_with_capacity_and_stays_bounded() {
        assert_eq!(derive_ag_count(0, 4096), 1);
        assert_eq!(derive_ag_count((4 * 1024 * 1024) / 4096, 4096), 1);
        assert_eq!(
            derive_ag_count((64 * 1024 * 1024 * 1024u64) / 4096, 4096),
            2
        );
        assert_eq!(
            derive_ag_count((8 * 1024 * 1024 * 1024 * 1024u64) / 4096, 4096),
            16
        );
    }

    #[test]
    fn split_data_blocks_into_ags_is_contiguous_and_conservative() {
        let ranges = split_data_blocks_into_ags(100, 103, 8);
        assert_eq!(ranges.first().copied(), Some((100, 13)));
        assert_eq!(ranges.last().copied(), Some((191, 12)));
        let total: u64 = ranges.iter().map(|(_, blocks)| *blocks).sum();
        assert_eq!(total, 103);
        for pair in ranges.windows(2) {
            let (start_a, count_a) = pair[0];
            let (start_b, _) = pair[1];
            assert_eq!(start_a + count_a, start_b);
        }
    }

    #[test]
    fn ag_index_for_block_uses_start_index_boundaries() {
        let state = AllocationState::new(100, 10, 3);
        assert_eq!(state.ags.len(), 3);
        assert_eq!(state.ags[0].start_block, 100);
        assert_eq!(state.ags[1].start_block, 104);
        assert_eq!(state.ags[2].start_block, 107);

        assert_eq!(state.ag_index_for_block(99), None);
        assert_eq!(state.ag_index_for_block(100), Some(0));
        assert_eq!(state.ag_index_for_block(103), Some(0));
        assert_eq!(state.ag_index_for_block(104), Some(1));
        assert_eq!(state.ag_index_for_block(106), Some(1));
        assert_eq!(state.ag_index_for_block(107), Some(2));
        assert_eq!(state.ag_index_for_block(109), Some(2));
        assert_eq!(state.ag_index_for_block(110), None);
    }

    #[test]
    fn large_segment_pool_device_formats_and_reopens_cleanly() {
        let path = temp_loopback("segment-pool-open");
        let total_bytes = 2 * 1024 * 1024 * 1024u64;
        BlockManager::format(
            &path,
            FormatOptions {
                total_bytes: Some(total_bytes),
                ..Default::default()
            },
        )
        .expect("format large loopback");

        let reopened = BlockManager::open(&path).expect("reopen large loopback");
        assert!(
            reopened.segment_pool.is_some(),
            "segment pool should be enabled on large device"
        );
        reopened
            .validate_allocator_state()
            .expect("allocator state should validate on segment-pooled device");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn allocation_state_prefers_best_fit_extent() {
        let mut state = AllocationState::new(0, 128, 1);
        state
            .remove_known_extent(Extent {
                start_block: 0,
                block_count: 8,
            })
            .expect("reserve first");
        state
            .remove_known_extent(Extent {
                start_block: 8,
                block_count: 16,
            })
            .expect("reserve second");
        state
            .remove_known_extent(Extent {
                start_block: 24,
                block_count: 8,
            })
            .expect("reserve third");
        state
            .remove_known_extent(Extent {
                start_block: 32,
                block_count: 64,
            })
            .expect("reserve tail");
        state
            .free_extent(Extent {
                start_block: 8,
                block_count: 8,
            })
            .expect("free middle");
        state
            .free_extent(Extent {
                start_block: 24,
                block_count: 8,
            })
            .expect("free small");

        let chosen = state
            .find_best_fit_extent(8)
            .expect("find best fit for one 8-block extent");
        assert_eq!(
            chosen,
            Extent {
                start_block: 8,
                block_count: 8
            }
        );
    }

    #[test]
    fn allocation_state_indexes_remain_consistent_under_churn() {
        let mut state = AllocationState::new(100, 4096, 8);
        let mut allocated = Vec::<Extent>::new();
        let mut seed: u64 = 0xA5A5_0123_89AB_CDEF;

        for _step in 0..5000usize {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let should_free = !allocated.is_empty() && (seed & 0b11) == 0;
            if should_free {
                let idx = (seed as usize) % allocated.len();
                let extent = allocated.swap_remove(idx);
                state.free_extent(extent).expect("free extent");
            } else {
                let requested = 1 + ((seed >> 8) % 32);
                if let Some(extent) = state.find_best_fit_extent(requested) {
                    state.remove_known_extent(extent).expect("reserve extent");
                    allocated.push(extent);
                } else if let Some(extent) = allocated.pop() {
                    state.free_extent(extent).expect("free fallback extent");
                }
            }
            assert_allocation_state_consistent(&state);
        }

        for extent in allocated {
            state.free_extent(extent).expect("cleanup free");
        }
        assert_allocation_state_consistent(&state);
        assert_eq!(state.available_blocks(), 4096);
    }

    #[test]
    fn format_and_open_roundtrip_preserves_header() {
        let path = temp_loopback("header");
        let mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 32,
                total_bytes: Some(8 * 1024 * 1024),
            },
        )
        .expect("format device");

        let header = mgr.header().clone();
        drop(mgr);

        let reopened = BlockManager::open(&path).expect("reopen device");
        assert_eq!(reopened.header(), &header);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn format_resizes_regular_files() {
        let path = temp_loopback("resize");
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .expect("create loopback file");
        let meta = file.metadata().expect("stat loopback file");
        assert!(should_resize_backing_store(&meta));
        drop(file);

        let mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format loopback file");
        assert_eq!(mgr.header().total_blocks * 4096, 4 * 1024 * 1024);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn format_preflight_reports_empty_for_new_file() {
        let path = temp_loopback("preflight-empty");
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .expect("create loopback file");
        file.sync_all().expect("sync empty file");
        drop(file);

        let preflight = BlockManager::format_preflight(&path).expect("preflight");
        assert_eq!(preflight.total_bytes, 0);
        assert!(!preflight.is_block_device);
        assert!(!preflight.is_mounted);
        assert_eq!(preflight.signature, FormatTargetSignature::Empty);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn format_rejects_foreign_signature_without_force() {
        let path = temp_loopback("foreign-signature-reject");
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .expect("create loopback file");
        file.set_len(1024 * 1024).expect("set file len");
        file.write_at(b"FOREIGN-SIGNATURE", 0)
            .expect("write foreign header");
        file.sync_all().expect("sync foreign file");
        drop(file);

        let err = match BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        ) {
            Ok(_) => panic!("expected foreign signature rejection"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::InvalidArgument(msg) => {
                assert!(
                    msg.contains("foreign-signature"),
                    "unexpected error: {}",
                    msg
                );
            }
            other => panic!("expected invalid argument, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn format_with_force_allows_foreign_signature() {
        let path = temp_loopback("foreign-signature-force");
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .expect("create loopback file");
        file.set_len(1024 * 1024).expect("set file len");
        file.write_at(b"FOREIGN-SIGNATURE", 0)
            .expect("write foreign header");
        file.sync_all().expect("sync foreign file");
        drop(file);

        let mgr = BlockManager::format_with_force(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("force format should override foreign signature");
        assert_eq!(mgr.header().total_blocks * 4096, 4 * 1024 * 1024);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn format_preflight_reports_block_manager_signature_after_format() {
        let path = temp_loopback("preflight-block-manager-signature");
        let mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format");
        let formatted_device = mgr.header().device_id;
        drop(mgr);

        let preflight = BlockManager::format_preflight(&path).expect("preflight");
        assert_eq!(preflight.signature, FormatTargetSignature::BlockManager);
        assert!(preflight.total_bytes > 0);

        let reopened = BlockManager::open(&path).expect("reopen");
        assert_eq!(reopened.header().device_id, formatted_device);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn format_nonce_issue_and_response_is_single_use() {
        let path = temp_loopback("format-nonce-single-use");
        let challenge = BlockManager::issue_format_nonce(&path).expect("issue nonce");
        assert!(!challenge.nonce.is_empty());
        assert!(challenge.expires_unix_secs >= challenge.issued_unix_secs);

        let mgr = BlockManager::format_with_nonce(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
            &challenge.nonce,
            false,
        )
        .expect("format with valid nonce");
        assert_eq!(mgr.header().total_blocks * 4096, 4 * 1024 * 1024);

        let err = match BlockManager::format_with_nonce(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
            &challenge.nonce,
            false,
        ) {
            Ok(_) => panic!("expected single-use nonce rejection"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::InvalidArgument(msg) => {
                assert!(
                    msg.contains("failed to read format nonce state"),
                    "unexpected error: {}",
                    msg
                );
            }
            other => panic!("expected invalid argument, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn format_with_nonce_rejects_mismatched_nonce() {
        let path = temp_loopback("format-nonce-mismatch");
        let _challenge = BlockManager::issue_format_nonce(&path).expect("issue nonce");

        let err = match BlockManager::format_with_nonce(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
            "definitely-wrong-nonce",
            false,
        ) {
            Ok(_) => panic!("expected nonce mismatch rejection"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::InvalidArgument(msg) => {
                assert!(msg.contains("nonce mismatch"), "unexpected error: {}", msg);
            }
            other => panic!("expected invalid argument, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn format_with_nonce_rejects_fingerprint_drift() {
        let path = temp_loopback("format-nonce-fingerprint-drift");
        let challenge = BlockManager::issue_format_nonce(&path).expect("issue nonce");

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open target");
        file.set_len(1024 * 1024).expect("resize");
        file.write_at(b"FOREIGN-SIGNATURE", 0).expect("write drift");
        file.sync_all().expect("sync drift");
        drop(file);

        let err = match BlockManager::format_with_nonce(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
            &challenge.nonce,
            true,
        ) {
            Ok(_) => panic!("expected fingerprint drift rejection"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::InvalidArgument(msg) => {
                assert!(
                    msg.contains("fingerprint drifted"),
                    "unexpected error: {}",
                    msg
                );
            }
            other => panic!("expected invalid argument, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn allocate_write_read_roundtrip() {
        let path = temp_loopback("read-write");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");

        let handle = mgr.allocate_bytes(32 * 1024).expect("allocate");
        let payload = (0..16384).map(|i| (i % 251) as u8).collect::<Vec<_>>();
        mgr.write(handle, 0, &payload).expect("write payload");
        mgr.flush().expect("flush");

        let read_back = mgr.read(handle, 0, payload.len()).expect("read payload");
        assert_eq!(read_back, payload);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn free_reuses_extent_space() {
        let path = temp_loopback("reuse");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");

        let first = mgr.allocate_bytes(24 * 1024).expect("first alloc");
        let _second = mgr.allocate_bytes(24 * 1024).expect("second alloc");
        mgr.free(first.allocation_id).expect("free first");

        let reused = mgr.allocate_bytes(8 * 1024).expect("reuse alloc");
        assert_eq!(reused.extent.start_block, first.extent.start_block);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn journal_replay_restores_allocation_map() {
        let path = temp_loopback("replay");
        let device_id;
        let alloc_id;
        {
            let mut mgr = BlockManager::format(
                &path,
                FormatOptions {
                    block_size: 4096,
                    journal_blocks: 16,
                    total_bytes: Some(4 * 1024 * 1024),
                },
            )
            .expect("format device");

            let handle = mgr.allocate_bytes(12 * 1024).expect("allocate");
            let payload = b"replay-check-data";
            mgr.write(handle, 512, payload).expect("write payload");
            mgr.flush().expect("flush");

            device_id = mgr.header().device_id;
            alloc_id = handle.allocation_id;
        }

        let reopened = BlockManager::open(&path).expect("reopen");
        assert_eq!(reopened.allocation_count(), 1);
        let extent = reopened
            .allocations
            .get(&alloc_id)
            .copied()
            .expect("allocation restored");
        let handle = AllocationHandle {
            allocation_id: alloc_id,
            extent,
        };
        let read_back = reopened
            .read(handle, 512, b"replay-check-data".len())
            .expect("read replayed payload");
        assert_eq!(read_back, b"replay-check-data");
        assert_eq!(reopened.header().device_id, device_id);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn free_persists_across_reopen() {
        let path = temp_loopback("free-replay");
        let original_start;
        {
            let mut mgr = BlockManager::format(
                &path,
                FormatOptions {
                    block_size: 4096,
                    journal_blocks: 16,
                    total_bytes: Some(4 * 1024 * 1024),
                },
            )
            .expect("format device");

            let handle = mgr.allocate_bytes(20 * 1024).expect("allocate");
            original_start = handle.extent.start_block;
            mgr.free(handle.allocation_id).expect("free");
            mgr.flush().expect("flush");
        }

        let mut reopened = BlockManager::open(&path).expect("reopen");
        let handle = reopened.allocate_bytes(20 * 1024).expect("reallocate");
        assert_eq!(handle.extent.start_block, original_start);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn raw_extent_address_read_write_and_free_roundtrip() {
        let path = temp_loopback("raw-address");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");

        let handle = mgr.allocate_bytes(16 * 1024).expect("allocate");
        let address = mgr.raw_extent_address(handle, 16 * 1024).expect("address");
        let payload = b"raw-address-payload";

        mgr.write_address(&address, 1024, payload)
            .expect("write via address");
        mgr.flush().expect("flush");

        let got = mgr
            .read_address(&address, 1024, payload.len())
            .expect("read via address");
        assert_eq!(got, payload);

        mgr.free_address(&address).expect("free via address");
        assert_eq!(mgr.allocation_count(), 0);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn allocate_direct_extent_io_roundtrip() {
        let path = temp_loopback("alloc-direct-io");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");

        let (address, io) = mgr
            .allocate_direct_extent_io(16 * 1024)
            .expect("allocate direct io");
        let payload = b"allocate-direct-extent-io";

        io.write(2048, payload).expect("write via direct io");
        io.flush_data().expect("flush direct io data");

        let got = io.read(2048, payload.len()).expect("read via direct io");
        assert_eq!(got, payload);

        mgr.free_address(&address).expect("free via address");
        assert_eq!(mgr.allocation_count(), 0);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn allocate_raw_extent_address_roundtrip() {
        let path = temp_loopback("alloc-raw-address");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");

        let address = mgr
            .allocate_raw_extent_address(16 * 1024)
            .expect("allocate raw address");
        let payload = b"allocate-raw-extent-address";

        mgr.write_address(&address, 4096, payload)
            .expect("write via raw address");
        mgr.flush_data().expect("flush data");

        let got = mgr
            .read_address(&address, 4096, payload.len())
            .expect("read via raw address");
        assert_eq!(got, payload);

        mgr.free_address(&address).expect("free via raw address");
        assert_eq!(mgr.allocation_count(), 0);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn batch_free_addresses_releases_unique_allocations() {
        let path = temp_loopback("batch-free-addresses");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");

        let a = mgr.allocate_bytes(8 * 1024).expect("allocate a");
        let b = mgr.allocate_bytes(8 * 1024).expect("allocate b");
        let a_addr = mgr.raw_extent_address(a, 8 * 1024).expect("address a");
        let b_addr = mgr.raw_extent_address(b, 8 * 1024).expect("address b");

        let freed = mgr
            .free_addresses(&[a_addr.clone(), b_addr.clone(), a_addr.clone()])
            .expect("batch free");
        assert_eq!(freed, 2);
        assert_eq!(mgr.allocation_count(), 0);
        mgr.flush_meta().expect("flush metadata");

        match mgr.read_address(&a_addr, 0, 1) {
            Err(BlockManagerError::UnknownAllocation(id)) => assert_eq!(id, a_addr.allocation_id),
            other => panic!("expected freed allocation a to be unknown, got {:?}", other),
        }
        match mgr.read_address(&b_addr, 0, 1) {
            Err(BlockManagerError::UnknownAllocation(id)) => assert_eq!(id, b_addr.allocation_id),
            other => panic!("expected freed allocation b to be unknown, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn raw_data_file_uses_o_direct() {
        let path = temp_loopback("o-direct");
        let mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");

        let flags = unsafe { libc::fcntl(mgr.direct_file.as_raw_fd(), libc::F_GETFL) };
        assert!(flags >= 0, "fcntl(F_GETFL) should succeed");
        assert_ne!(flags & libc::O_DIRECT, 0, "raw data fd should use O_DIRECT");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn open_recovers_when_primary_superblock_is_corrupt() {
        let path = temp_loopback("corrupt-superblock");
        let mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");
        let device_id = mgr.header().device_id;
        drop(mgr);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open loopback");
        file.write_at(&[0xFF], 20)
            .expect("corrupt checksummed header byte");
        file.sync_all().expect("sync corruption");

        let reopened = BlockManager::open(&path).expect("reopen using backup superblock");
        assert_eq!(reopened.header().device_id, device_id);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn open_rejects_when_primary_and_backup_superblocks_are_corrupt() {
        let path = temp_loopback("corrupt-both-superblocks");
        let mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");
        let backup_offset = block_offset(mgr.header(), SECONDARY_SUPERBLOCK_BLOCK);
        drop(mgr);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open loopback");
        file.write_at(&[0xFF], 20)
            .expect("corrupt primary header byte");
        file.write_at(&[0xAA], backup_offset + 20)
            .expect("corrupt backup header byte");
        file.sync_all().expect("sync corruption");

        let err = match BlockManager::open(&path) {
            Ok(_) => panic!("expected corrupt superblock error"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Corrupt(msg) => {
                assert!(
                    msg.contains("superblock checksum mismatch"),
                    "unexpected error: {}",
                    msg
                );
            }
            _ => panic!("expected corrupt superblock error"),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn open_rejects_corrupt_journal_record_checksum() {
        let path = temp_loopback("corrupt-journal");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");

        let handle = mgr.allocate_bytes(12 * 1024).expect("allocate");
        mgr.write(handle, 0, b"journal-corruption")
            .expect("write payload");
        mgr.flush().expect("flush");
        let journal_offset = block_offset(mgr.header(), mgr.header().journal_start_block);
        drop(mgr);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open loopback");
        let corrupt_offset = journal_offset.saturating_add(32);
        file.write_at(&[0x00], corrupt_offset)
            .expect("corrupt journal crc");
        file.sync_all().expect("sync corruption");

        let err = match BlockManager::open(&path) {
            Ok(_) => panic!("expected corrupt journal error"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Corrupt(msg) => {
                assert!(
                    msg.contains("journal record checksum mismatch"),
                    "unexpected error: {}",
                    msg
                );
            }
            _ => panic!("expected corrupt journal error"),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn format_primary_sync_failpoint_returns_error() {
        let _guard = FAILPOINT_TEST_LOCK.lock().expect("failpoint test lock");
        clear_failpoints();
        let path = temp_loopback("format-failpoint-primary-sync");
        set_failpoint("format.primary_superblock.sync", 0);

        let err = match BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        ) {
            Ok(_) => panic!("expected injected format sync failure"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Io(e) => {
                assert!(
                    e.to_string().contains("format.primary_superblock.sync"),
                    "unexpected io error: {}",
                    e
                );
            }
            other => panic!("expected io error, got {:?}", other),
        }

        clear_failpoints();
        let _ = fs::remove_file(path);
    }

    #[test]
    fn format_backup_write_failpoint_returns_error() {
        let _guard = FAILPOINT_TEST_LOCK.lock().expect("failpoint test lock");
        clear_failpoints();
        let path = temp_loopback("format-failpoint-backup-write");
        set_failpoint("format.backup_superblock.write", 0);

        let err = match BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        ) {
            Ok(_) => panic!("expected injected format backup-write failure"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Io(e) => {
                assert!(
                    e.to_string().contains("format.backup_superblock.write"),
                    "unexpected io error: {}",
                    e
                );
            }
            other => panic!("expected io error, got {:?}", other),
        }

        clear_failpoints();
        let _ = fs::remove_file(path);
    }

    #[test]
    fn format_zero_journal_failpoint_returns_error() {
        let _guard = FAILPOINT_TEST_LOCK.lock().expect("failpoint test lock");
        clear_failpoints();
        let path = temp_loopback("format-failpoint-zero-journal");
        set_failpoint("format.zero_journal", 0);

        let err = match BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        ) {
            Ok(_) => panic!("expected injected format zero-journal failure"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Io(e) => {
                assert!(
                    e.to_string().contains("format.zero_journal"),
                    "unexpected io error: {}",
                    e
                );
            }
            other => panic!("expected io error, got {:?}", other),
        }

        clear_failpoints();
        let _ = fs::remove_file(path);
    }

    #[test]
    fn format_backup_sync_failpoint_returns_error() {
        let _guard = FAILPOINT_TEST_LOCK.lock().expect("failpoint test lock");
        clear_failpoints();
        let path = temp_loopback("format-failpoint-backup-sync");
        set_failpoint("format.backup_superblock.sync", 0);

        let err = match BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        ) {
            Ok(_) => panic!("expected injected format backup-sync failure"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Io(e) => {
                assert!(
                    e.to_string().contains("format.backup_superblock.sync"),
                    "unexpected io error: {}",
                    e
                );
            }
            other => panic!("expected io error, got {:?}", other),
        }

        clear_failpoints();
        let _ = fs::remove_file(path);
    }

    #[test]
    fn format_primary_write_failpoint_returns_error() {
        let _guard = FAILPOINT_TEST_LOCK.lock().expect("failpoint test lock");
        clear_failpoints();
        let path = temp_loopback("format-failpoint-primary-write");
        set_failpoint("format.primary_superblock.write", 0);

        let err = match BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        ) {
            Ok(_) => panic!("expected injected format primary-write failure"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Io(e) => {
                assert!(
                    e.to_string().contains("format.primary_superblock.write"),
                    "unexpected io error: {}",
                    e
                );
            }
            other => panic!("expected io error, got {:?}", other),
        }

        clear_failpoints();
        let _ = fs::remove_file(path);
    }

    #[test]
    fn allocate_commit_write_failpoint_keeps_state_and_replay_stable() {
        let _guard = FAILPOINT_TEST_LOCK.lock().expect("failpoint test lock");
        clear_failpoints();
        let path = temp_loopback("alloc-failpoint-commit-write");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");

        let before_free = mgr.available_blocks();
        set_failpoint("journal.record.write", 1);
        let err = match mgr.allocate_bytes(4096) {
            Ok(_) => panic!("expected injected commit-write failure"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Io(e) => {
                assert!(
                    e.to_string().contains("journal.record.write"),
                    "unexpected io error: {}",
                    e
                );
            }
            other => panic!("expected io error, got {:?}", other),
        }
        clear_failpoints();

        assert_eq!(mgr.allocation_count(), 0);
        assert_eq!(
            mgr.available_blocks(),
            before_free,
            "failed allocate must not consume free space"
        );
        drop(mgr);

        let reopened = BlockManager::open(&path).expect("reopen after failed allocate");
        assert_eq!(
            reopened.allocation_count(),
            0,
            "prepare-only tail from failed allocate must not materialize live allocations"
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn free_commit_write_failpoint_keeps_state_and_replay_stable() {
        let _guard = FAILPOINT_TEST_LOCK.lock().expect("failpoint test lock");
        clear_failpoints();
        let path = temp_loopback("free-failpoint-commit-write");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");

        let handle = mgr.allocate_bytes(4096).expect("allocate");
        let before_free = mgr.available_blocks();
        assert_eq!(mgr.allocation_count(), 1);

        set_failpoint("journal.record.write", 1);
        let err = match mgr.free(handle.allocation_id) {
            Ok(_) => panic!("expected injected commit-write failure on free"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Io(e) => {
                assert!(
                    e.to_string().contains("journal.record.write"),
                    "unexpected io error: {}",
                    e
                );
            }
            other => panic!("expected io error, got {:?}", other),
        }
        clear_failpoints();

        assert_eq!(
            mgr.allocation_count(),
            1,
            "failed free must keep allocation live in-memory"
        );
        assert_eq!(
            mgr.available_blocks(),
            before_free,
            "failed free must not mutate free-space accounting"
        );
        drop(mgr);

        let reopened = BlockManager::open(&path).expect("reopen after failed free");
        assert_eq!(
            reopened.allocation_count(),
            1,
            "prepare-only tail from failed free must not remove live allocation"
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn flush_meta_failpoint_is_fail_stop_and_recoverable() {
        let _guard = FAILPOINT_TEST_LOCK.lock().expect("failpoint test lock");
        clear_failpoints();
        let path = temp_loopback("flush-meta-failpoint");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");

        let handle = mgr.allocate_bytes(4096).expect("allocate");
        assert!(handle.allocation_id > 0);

        set_failpoint("flush_meta.sync", 0);
        let err = match mgr.flush_meta() {
            Ok(_) => panic!("expected injected flush_meta sync failure"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Io(e) => {
                assert!(
                    e.to_string().contains("flush_meta.sync"),
                    "unexpected io error: {}",
                    e
                );
            }
            other => panic!("expected io error, got {:?}", other),
        }
        clear_failpoints();

        mgr.flush_meta().expect("flush_meta should recover");
        drop(mgr);
        let reopened = BlockManager::open(&path).expect("reopen after recovered flush_meta");
        assert_eq!(reopened.allocation_count(), 1);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn checkpoint_compaction_primary_write_failpoint_is_fail_stop_and_recoverable() {
        let _guard = FAILPOINT_TEST_LOCK.lock().expect("failpoint test lock");
        clear_failpoints();
        let path = temp_loopback("checkpoint-failpoint-primary-write");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 1,
                total_bytes: Some(8 * 1024 * 1024),
            },
        )
        .expect("format device");

        let keep = mgr.allocate_bytes(8 * 1024).expect("allocate keep");
        let expected_allocations = mgr.allocations.clone();
        assert!(expected_allocations.contains_key(&keep.allocation_id));
        let before_generation = mgr.header().superblock_generation;
        let before_cursor = mgr.journal_cursor();

        set_failpoint("persist.primary_superblock.write", 0);
        let err = match mgr.compact_journal_checkpoint() {
            Ok(_) => panic!("expected injected persist primary write failure"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Io(e) => {
                assert!(
                    e.to_string().contains("persist.primary_superblock.write"),
                    "unexpected io error: {}",
                    e
                );
            }
            other => panic!("expected io error, got {:?}", other),
        }
        clear_failpoints();

        assert_eq!(mgr.allocations, expected_allocations);
        assert_eq!(mgr.journal_cursor(), before_cursor);
        assert_eq!(mgr.header().superblock_generation, before_generation);

        drop(mgr);
        let reopened = BlockManager::open(&path).expect("reopen after failed compaction persist");
        assert_eq!(reopened.allocations, expected_allocations);
        assert_eq!(reopened.journal_cursor(), before_cursor);
        assert_eq!(reopened.header().superblock_generation, before_generation);
        reopened
            .validate_allocator_state()
            .expect("allocator must stay valid after fail-stop compaction failure");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn checkpoint_compaction_mid_rewrite_failure_rolls_back_journal_and_state() {
        let _guard = FAILPOINT_TEST_LOCK.lock().expect("failpoint test lock");
        clear_failpoints();
        let path = temp_loopback("checkpoint-mid-rewrite-rollback");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 1,
                total_bytes: Some(8 * 1024 * 1024),
            },
        )
        .expect("format device");

        let keep = mgr.allocate_bytes(8 * 1024).expect("allocate keep");
        let expected_allocations = mgr.allocations.clone();
        let before_generation = mgr.header().superblock_generation;
        let before_cursor = mgr.journal_cursor();
        assert!(expected_allocations.contains_key(&keep.allocation_id));

        // Allow one journal record write during compaction rewrite, then fail on the next one.
        set_failpoint("journal.record.write", 1);
        let err = match mgr.compact_journal_checkpoint() {
            Ok(_) => panic!("expected injected journal rewrite failure"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Io(e) => {
                assert!(
                    e.to_string().contains("journal.record.write"),
                    "unexpected io error: {}",
                    e
                );
            }
            other => panic!("expected io error, got {:?}", other),
        }
        clear_failpoints();

        // In-memory state should be restored immediately after rollback.
        assert_eq!(mgr.allocations, expected_allocations);
        assert_eq!(mgr.journal_cursor(), before_cursor);
        assert_eq!(mgr.header().superblock_generation, before_generation);

        drop(mgr);
        let reopened = BlockManager::open(&path).expect("reopen after failed compaction rewrite");
        assert_eq!(reopened.allocations, expected_allocations);
        assert_eq!(reopened.header().superblock_generation, before_generation);
        reopened
            .validate_allocator_state()
            .expect("allocator must stay valid after compaction rollback");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn checkpoint_compaction_backup_sync_failpoint_is_fail_stop_and_recoverable() {
        let _guard = FAILPOINT_TEST_LOCK.lock().expect("failpoint test lock");
        clear_failpoints();
        let path = temp_loopback("checkpoint-failpoint-backup-sync");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 1,
                total_bytes: Some(8 * 1024 * 1024),
            },
        )
        .expect("format device");

        let keep = mgr.allocate_bytes(8 * 1024).expect("allocate keep");
        let expected_allocations = mgr.allocations.clone();
        assert!(expected_allocations.contains_key(&keep.allocation_id));
        let before_generation = mgr.header().superblock_generation;
        let before_cursor = mgr.journal_cursor();

        set_failpoint("persist.backup_superblock.sync", 0);
        let err = match mgr.compact_journal_checkpoint() {
            Ok(_) => panic!("expected injected persist backup sync failure"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Io(e) => {
                assert!(
                    e.to_string().contains("persist.backup_superblock.sync"),
                    "unexpected io error: {}",
                    e
                );
            }
            other => panic!("expected io error, got {:?}", other),
        }
        clear_failpoints();

        assert_eq!(mgr.allocations, expected_allocations);
        assert_eq!(mgr.journal_cursor(), before_cursor);
        assert_eq!(mgr.header().superblock_generation, before_generation);

        drop(mgr);
        let reopened = BlockManager::open(&path).expect("reopen after failed compaction persist");
        assert_eq!(reopened.allocations, expected_allocations);
        assert_eq!(reopened.journal_cursor(), before_cursor);
        assert_eq!(reopened.header().superblock_generation, before_generation);
        reopened
            .validate_allocator_state()
            .expect("allocator must stay valid after fail-stop compaction failure");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn checkpoint_compaction_backup_write_failpoint_is_fail_stop_and_recoverable() {
        let _guard = FAILPOINT_TEST_LOCK.lock().expect("failpoint test lock");
        clear_failpoints();
        let path = temp_loopback("checkpoint-failpoint-backup-write");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 1,
                total_bytes: Some(8 * 1024 * 1024),
            },
        )
        .expect("format device");

        let keep = mgr.allocate_bytes(8 * 1024).expect("allocate keep");
        let expected_allocations = mgr.allocations.clone();
        assert!(expected_allocations.contains_key(&keep.allocation_id));
        let before_generation = mgr.header().superblock_generation;
        let before_cursor = mgr.journal_cursor();

        set_failpoint("persist.backup_superblock.write", 0);
        let err = match mgr.compact_journal_checkpoint() {
            Ok(_) => panic!("expected injected persist backup write failure"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Io(e) => {
                assert!(
                    e.to_string().contains("persist.backup_superblock.write"),
                    "unexpected io error: {}",
                    e
                );
            }
            other => panic!("expected io error, got {:?}", other),
        }
        clear_failpoints();

        assert_eq!(mgr.allocations, expected_allocations);
        assert_eq!(mgr.journal_cursor(), before_cursor);
        assert_eq!(mgr.header().superblock_generation, before_generation);

        drop(mgr);
        let reopened = BlockManager::open(&path).expect("reopen after failed compaction persist");
        assert_eq!(reopened.allocations, expected_allocations);
        assert_eq!(reopened.journal_cursor(), before_cursor);
        assert_eq!(reopened.header().superblock_generation, before_generation);
        reopened
            .validate_allocator_state()
            .expect("allocator must stay valid after fail-stop compaction failure");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn checkpoint_compaction_primary_sync_failpoint_is_fail_stop_and_recoverable() {
        let _guard = FAILPOINT_TEST_LOCK.lock().expect("failpoint test lock");
        clear_failpoints();
        let path = temp_loopback("checkpoint-failpoint-primary-sync");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 1,
                total_bytes: Some(8 * 1024 * 1024),
            },
        )
        .expect("format device");

        let keep = mgr.allocate_bytes(8 * 1024).expect("allocate keep");
        let expected_allocations = mgr.allocations.clone();
        assert!(expected_allocations.contains_key(&keep.allocation_id));
        let before_generation = mgr.header().superblock_generation;
        let before_cursor = mgr.journal_cursor();

        set_failpoint("persist.primary_superblock.sync", 0);
        let err = match mgr.compact_journal_checkpoint() {
            Ok(_) => panic!("expected injected persist primary sync failure"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Io(e) => {
                assert!(
                    e.to_string().contains("persist.primary_superblock.sync"),
                    "unexpected io error: {}",
                    e
                );
            }
            other => panic!("expected io error, got {:?}", other),
        }
        clear_failpoints();

        assert_eq!(mgr.allocations, expected_allocations);
        assert_eq!(mgr.journal_cursor(), before_cursor);
        assert_eq!(mgr.header().superblock_generation, before_generation);

        drop(mgr);
        let reopened = BlockManager::open(&path).expect("reopen after failed compaction persist");
        assert_eq!(reopened.allocations, expected_allocations);
        assert_eq!(reopened.journal_cursor(), before_cursor);
        assert_eq!(reopened.header().superblock_generation, before_generation);
        reopened
            .validate_allocator_state()
            .expect("allocator must stay valid after fail-stop compaction failure");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn checkpoint_compaction_zero_journal_failpoint_is_fail_stop_and_recoverable() {
        let _guard = FAILPOINT_TEST_LOCK.lock().expect("failpoint test lock");
        clear_failpoints();
        let path = temp_loopback("checkpoint-failpoint-zero-journal");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 1,
                total_bytes: Some(8 * 1024 * 1024),
            },
        )
        .expect("format device");

        let keep = mgr.allocate_bytes(8 * 1024).expect("allocate keep");
        let expected_allocations = mgr.allocations.clone();
        assert!(expected_allocations.contains_key(&keep.allocation_id));
        let before_generation = mgr.header().superblock_generation;
        let before_cursor = mgr.journal_cursor();

        set_failpoint("checkpoint.zero_journal", 0);
        let err = match mgr.compact_journal_checkpoint() {
            Ok(_) => panic!("expected injected checkpoint zero-journal failure"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Io(e) => {
                assert!(
                    e.to_string().contains("checkpoint.zero_journal"),
                    "unexpected io error: {}",
                    e
                );
            }
            other => panic!("expected io error, got {:?}", other),
        }
        clear_failpoints();

        assert_eq!(mgr.allocations, expected_allocations);
        assert_eq!(mgr.journal_cursor(), before_cursor);
        assert_eq!(mgr.header().superblock_generation, before_generation);

        drop(mgr);
        let reopened = BlockManager::open(&path).expect("reopen after failed compaction persist");
        assert_eq!(reopened.allocations, expected_allocations);
        assert_eq!(reopened.journal_cursor(), before_cursor);
        assert_eq!(reopened.header().superblock_generation, before_generation);
        reopened
            .validate_allocator_state()
            .expect("allocator must stay valid after fail-stop compaction failure");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn deterministic_fault_schedule_replay_fingerprint_is_stable() {
        let _guard = FAILPOINT_TEST_LOCK.lock().expect("failpoint test lock");
        clear_failpoints();

        let first =
            run_deterministic_faulted_workload(0xC0FF_EE11_2233_4455, "fault-schedule-pass-a");
        let second =
            run_deterministic_faulted_workload(0xC0FF_EE11_2233_4455, "fault-schedule-pass-b");
        assert_eq!(
            first, second,
            "deterministic fault schedule should converge to identical replayed allocator state"
        );
    }

    #[test]
    fn replay_ignores_garbage_after_first_zero_tail_marker() {
        let path = temp_loopback("journal-tail-garbage-after-zero");
        let (allocation_id, journal_offset, journal_cursor) = {
            let mut mgr = BlockManager::format(
                &path,
                FormatOptions {
                    block_size: 4096,
                    journal_blocks: 16,
                    total_bytes: Some(4 * 1024 * 1024),
                },
            )
            .expect("format device");
            let handle = mgr.allocate_bytes(8 * 1024).expect("allocate");
            mgr.flush().expect("flush");
            (
                handle.allocation_id,
                block_offset(mgr.header(), mgr.header().journal_start_block),
                mgr.journal_cursor(),
            )
        };

        // Keep first zero tail marker intact at `journal_cursor`, but inject garbage in the
        // following slot. Replay must stop at first zero and ignore later bytes.
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open loopback");
        let garbage = [0xA5u8; JOURNAL_RECORD_SIZE];
        file.write_at(
            &garbage,
            journal_offset.saturating_add((journal_cursor + 1) * JOURNAL_RECORD_SIZE as u64),
        )
        .expect("inject garbage past zero tail");
        file.sync_all().expect("sync tail garbage");

        let reopened = BlockManager::open(&path).expect("reopen");
        assert_eq!(reopened.allocation_count(), 1);
        assert!(
            reopened.allocations.contains_key(&allocation_id),
            "replay should ignore bytes beyond first zero tail marker"
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn replay_rejects_partial_tail_record_with_nonzero_magic() {
        let path = temp_loopback("journal-tail-partial-record");
        let (journal_offset, journal_cursor, template_record) = {
            let mut mgr = BlockManager::format(
                &path,
                FormatOptions {
                    block_size: 4096,
                    journal_blocks: 16,
                    total_bytes: Some(4 * 1024 * 1024),
                },
            )
            .expect("format device");
            let _handle = mgr.allocate_bytes(8 * 1024).expect("allocate");
            mgr.flush().expect("flush");
            let journal_offset = block_offset(mgr.header(), mgr.header().journal_start_block);
            let journal_cursor = mgr.journal_cursor();
            drop(mgr);

            let file = OpenOptions::new()
                .read(true)
                .open(&path)
                .expect("open loopback for template record");
            let mut template_record = [0u8; JOURNAL_RECORD_SIZE];
            file.read_at(&mut template_record, journal_offset)
                .expect("read template journal record");
            (journal_offset, journal_cursor, template_record)
        };

        // Simulate torn write of the next tail record by writing only the first bytes (including
        // non-zero magic/op) and leaving the rest zero; replay must hard-fail on checksum.
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open loopback for partial write");
        let tail_offset =
            journal_offset.saturating_add(journal_cursor * JOURNAL_RECORD_SIZE as u64);
        file.write_at(&template_record[..20], tail_offset)
            .expect("write partial tail record");
        file.sync_all().expect("sync partial record");

        let err = match BlockManager::open(&path) {
            Ok(_) => panic!("expected corrupt journal tail error"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Corrupt(msg) => {
                assert!(
                    msg.contains("journal record checksum mismatch"),
                    "unexpected error: {}",
                    msg
                );
            }
            other => panic!("expected checksum corruption error, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn replay_accepts_idempotent_duplicate_prepare_for_same_txn() {
        let path = temp_loopback("duplicate-prepare-idempotent");
        let allocation_id = 5_001u64;
        let extent = {
            let mut mgr = BlockManager::format(
                &path,
                FormatOptions {
                    block_size: 4096,
                    journal_blocks: 16,
                    total_bytes: Some(4 * 1024 * 1024),
                },
            )
            .expect("format device");
            let extent = Extent {
                start_block: mgr.header().data_start_block,
                block_count: 1,
            };
            let txn_id = 123;
            let prepare = JournalRecord {
                txn_id,
                op: JournalOp::PrepareAllocate,
                allocation_id,
                extent,
            };
            mgr.append_journal_record(prepare)
                .expect("append first prepare");
            mgr.append_journal_record(prepare)
                .expect("append duplicate prepare");
            mgr.append_journal_record(JournalRecord {
                txn_id,
                op: JournalOp::CommitAllocate,
                allocation_id,
                extent,
            })
            .expect("append commit");
            mgr.flush().expect("flush");
            extent
        };

        let reopened = BlockManager::open(&path).expect("reopen");
        assert_eq!(reopened.allocation_count(), 1);
        assert_eq!(reopened.allocations.get(&allocation_id), Some(&extent));
        assert_eq!(reopened.last_applied_journal_txn_id, 123);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn replay_rejects_duplicate_prepare_with_mismatched_payload() {
        let path = temp_loopback("duplicate-prepare-mismatch");
        {
            let mut mgr = BlockManager::format(
                &path,
                FormatOptions {
                    block_size: 4096,
                    journal_blocks: 16,
                    total_bytes: Some(4 * 1024 * 1024),
                },
            )
            .expect("format device");
            let base = mgr.header().data_start_block;
            mgr.append_journal_record(JournalRecord {
                txn_id: 777,
                op: JournalOp::PrepareAllocate,
                allocation_id: 11,
                extent: Extent {
                    start_block: base,
                    block_count: 1,
                },
            })
            .expect("append first prepare");
            mgr.append_journal_record(JournalRecord {
                txn_id: 777,
                op: JournalOp::PrepareAllocate,
                allocation_id: 11,
                extent: Extent {
                    start_block: base.saturating_add(1),
                    block_count: 1,
                },
            })
            .expect("append mismatched duplicate prepare");
            mgr.flush().expect("flush");
        }

        let err = match BlockManager::open(&path) {
            Ok(_) => panic!("expected replay corruption"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Corrupt(msg) => {
                assert!(
                    msg.contains("duplicate prepare"),
                    "unexpected error: {}",
                    msg
                );
            }
            other => panic!("expected corruption error, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn replay_rejects_duplicate_commit_with_mismatched_payload() {
        let path = temp_loopback("duplicate-commit-mismatch");
        {
            let mut mgr = BlockManager::format(
                &path,
                FormatOptions {
                    block_size: 4096,
                    journal_blocks: 16,
                    total_bytes: Some(4 * 1024 * 1024),
                },
            )
            .expect("format device");
            let base = mgr.header().data_start_block;
            mgr.append_journal_record(JournalRecord {
                txn_id: 900,
                op: JournalOp::PrepareAllocate,
                allocation_id: 71,
                extent: Extent {
                    start_block: base,
                    block_count: 1,
                },
            })
            .expect("append prepare");
            mgr.append_journal_record(JournalRecord {
                txn_id: 900,
                op: JournalOp::CommitAllocate,
                allocation_id: 71,
                extent: Extent {
                    start_block: base,
                    block_count: 1,
                },
            })
            .expect("append commit");
            mgr.append_journal_record(JournalRecord {
                txn_id: 900,
                op: JournalOp::CommitAllocate,
                allocation_id: 72,
                extent: Extent {
                    start_block: base.saturating_add(1),
                    block_count: 1,
                },
            })
            .expect("append mismatched duplicate commit");
            mgr.flush().expect("flush");
        }

        let err = match BlockManager::open(&path) {
            Ok(_) => panic!("expected replay corruption"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Corrupt(msg) => {
                assert!(
                    msg.contains("duplicate committed"),
                    "unexpected error: {}",
                    msg
                );
            }
            other => panic!("expected corruption error, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn replay_rejects_txid_regression() {
        let path = temp_loopback("txid-regression");
        {
            let mut mgr = BlockManager::format(
                &path,
                FormatOptions {
                    block_size: 4096,
                    journal_blocks: 16,
                    total_bytes: Some(4 * 1024 * 1024),
                },
            )
            .expect("format device");
            let base = mgr.header().data_start_block;
            mgr.append_journal_record(JournalRecord {
                txn_id: 10,
                op: JournalOp::PrepareAllocate,
                allocation_id: 301,
                extent: Extent {
                    start_block: base,
                    block_count: 1,
                },
            })
            .expect("append prepare");
            mgr.append_journal_record(JournalRecord {
                txn_id: 10,
                op: JournalOp::CommitAllocate,
                allocation_id: 301,
                extent: Extent {
                    start_block: base,
                    block_count: 1,
                },
            })
            .expect("append commit");
            mgr.append_journal_record(JournalRecord {
                txn_id: 9,
                op: JournalOp::Allocate,
                allocation_id: 302,
                extent: Extent {
                    start_block: base.saturating_add(2),
                    block_count: 1,
                },
            })
            .expect("append regressed txid");
            mgr.flush().expect("flush");
        }

        let err = match BlockManager::open(&path) {
            Ok(_) => panic!("expected txid regression corruption"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Corrupt(msg) => {
                assert!(msg.contains("txid regression"), "unexpected error: {}", msg);
            }
            other => panic!("expected corruption error, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn replay_rejects_prepare_only_txid_regression() {
        let path = temp_loopback("prepare-only-txid-regression");
        {
            let mut mgr = BlockManager::format(
                &path,
                FormatOptions {
                    block_size: 4096,
                    journal_blocks: 16,
                    total_bytes: Some(4 * 1024 * 1024),
                },
            )
            .expect("format device");
            let base = mgr.header().data_start_block;
            mgr.append_journal_record(JournalRecord {
                txn_id: 100,
                op: JournalOp::PrepareAllocate,
                allocation_id: 400,
                extent: Extent {
                    start_block: base,
                    block_count: 1,
                },
            })
            .expect("append prepare high txid");
            mgr.append_journal_record(JournalRecord {
                txn_id: 99,
                op: JournalOp::PrepareAllocate,
                allocation_id: 401,
                extent: Extent {
                    start_block: base.saturating_add(1),
                    block_count: 1,
                },
            })
            .expect("append prepare low txid");
            mgr.flush().expect("flush");
        }

        let err = match BlockManager::open(&path) {
            Ok(_) => panic!("expected txid regression corruption"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Corrupt(msg) => {
                assert!(msg.contains("txid regression"), "unexpected error: {}", msg);
            }
            other => panic!("expected corruption error, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn replay_rejects_prepare_after_committed_txid() {
        let path = temp_loopback("prepare-after-committed");
        {
            let mut mgr = BlockManager::format(
                &path,
                FormatOptions {
                    block_size: 4096,
                    journal_blocks: 16,
                    total_bytes: Some(4 * 1024 * 1024),
                },
            )
            .expect("format device");
            let base = mgr.header().data_start_block;
            mgr.append_journal_record(JournalRecord {
                txn_id: 501,
                op: JournalOp::PrepareAllocate,
                allocation_id: 91,
                extent: Extent {
                    start_block: base,
                    block_count: 1,
                },
            })
            .expect("append prepare");
            mgr.append_journal_record(JournalRecord {
                txn_id: 501,
                op: JournalOp::CommitAllocate,
                allocation_id: 91,
                extent: Extent {
                    start_block: base,
                    block_count: 1,
                },
            })
            .expect("append commit");
            mgr.append_journal_record(JournalRecord {
                txn_id: 501,
                op: JournalOp::PrepareAllocate,
                allocation_id: 91,
                extent: Extent {
                    start_block: base,
                    block_count: 1,
                },
            })
            .expect("append illegal trailing prepare");
            mgr.flush().expect("flush");
        }

        let err = match BlockManager::open(&path) {
            Ok(_) => panic!("expected replay corruption"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Corrupt(msg) => {
                assert!(
                    msg.contains("prepare record encountered after committed"),
                    "unexpected error: {}",
                    msg
                );
            }
            other => panic!("expected corruption error, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn replay_applies_legacy_txn_zero_allocate_record() {
        let path = temp_loopback("legacy-txn-zero-allocate");
        let allocation_id = 42_u64;
        let data_start = {
            let mut mgr = BlockManager::format(
                &path,
                FormatOptions {
                    block_size: 4096,
                    journal_blocks: 16,
                    total_bytes: Some(4 * 1024 * 1024),
                },
            )
            .expect("format device");
            let data_start = mgr.header().data_start_block;
            mgr.append_journal_record(JournalRecord {
                txn_id: 0,
                op: JournalOp::Allocate,
                allocation_id,
                extent: Extent {
                    start_block: data_start,
                    block_count: 1,
                },
            })
            .expect("append legacy allocate record");
            mgr.flush().expect("flush legacy journal record");
            data_start
        };

        let reopened = BlockManager::open(&path).expect("reopen legacy txn-zero journal");
        let extent = reopened
            .allocations
            .get(&allocation_id)
            .expect("legacy allocation should replay");
        assert_eq!(extent.start_block, data_start);
        assert_eq!(extent.block_count, 1);
        assert_eq!(reopened.allocation_count(), 1);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn stale_raw_address_is_rejected_after_free_and_reopen() {
        let path = temp_loopback("stale-address");
        let address = {
            let mut mgr = BlockManager::format(
                &path,
                FormatOptions {
                    block_size: 4096,
                    journal_blocks: 16,
                    total_bytes: Some(4 * 1024 * 1024),
                },
            )
            .expect("format device");

            let handle = mgr.allocate_bytes(16 * 1024).expect("allocate");
            let address = mgr.raw_extent_address(handle, 16 * 1024).expect("address");
            mgr.write_address(&address, 0, b"live-bytes")
                .expect("write via address");
            mgr.free_address(&address).expect("free address");
            mgr.flush().expect("flush");
            address
        };

        let reopened = BlockManager::open(&path).expect("reopen");
        match reopened.read_address(&address, 0, 8) {
            Err(BlockManagerError::UnknownAllocation(id)) => {
                assert_eq!(id, address.allocation_id);
            }
            other => panic!("expected stale address rejection, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn replay_skips_duplicate_txn_record_idempotently() {
        let path = temp_loopback("duplicate-txn");
        let (allocation_id, journal_offset) = {
            let mut mgr = BlockManager::format(
                &path,
                FormatOptions {
                    block_size: 4096,
                    journal_blocks: 16,
                    total_bytes: Some(4 * 1024 * 1024),
                },
            )
            .expect("format device");
            let handle = mgr.allocate_bytes(16 * 1024).expect("allocate");
            mgr.flush().expect("flush");
            let journal_offset = block_offset(mgr.header(), mgr.header().journal_start_block);
            (handle.allocation_id, journal_offset)
        };

        // Copy the committed journal record into the next slot to simulate replaying
        // a duplicated committed transaction after crash/retry windows.
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open loopback");
        let mut rec = [0u8; crate::constants::JOURNAL_RECORD_SIZE];
        file.read_at(
            &mut rec,
            journal_offset + crate::constants::JOURNAL_RECORD_SIZE as u64,
        )
        .expect("read committed record");
        file.write_at(
            &rec,
            journal_offset + (2 * crate::constants::JOURNAL_RECORD_SIZE) as u64,
        )
        .expect("write duplicate record");
        file.sync_all().expect("sync duplicate record");

        let reopened = BlockManager::open(&path).expect("reopen with duplicate txn");
        assert_eq!(reopened.allocation_count(), 1);
        assert!(reopened.allocations.contains_key(&allocation_id));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn replay_ignores_prepare_without_commit() {
        let path = temp_loopback("prepare-without-commit");
        {
            let mut mgr = BlockManager::format(
                &path,
                FormatOptions {
                    block_size: 4096,
                    journal_blocks: 16,
                    total_bytes: Some(4 * 1024 * 1024),
                },
            )
            .expect("format device");
            let extent = Extent {
                start_block: mgr.header().data_start_block,
                block_count: 1,
            };
            mgr.append_journal_record(JournalRecord {
                txn_id: 42,
                op: JournalOp::PrepareAllocate,
                allocation_id: 777,
                extent,
            })
            .expect("append prepare");
            mgr.flush().expect("flush");
        }

        let reopened = BlockManager::open(&path).expect("reopen");
        assert_eq!(
            reopened.allocation_count(),
            0,
            "prepare-only txn must not materialize allocations"
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn replay_rejects_commit_without_prepare() {
        let path = temp_loopback("commit-without-prepare");
        {
            let mut mgr = BlockManager::format(
                &path,
                FormatOptions {
                    block_size: 4096,
                    journal_blocks: 16,
                    total_bytes: Some(4 * 1024 * 1024),
                },
            )
            .expect("format device");
            let extent = Extent {
                start_block: mgr.header().data_start_block,
                block_count: 1,
            };
            mgr.append_journal_record(JournalRecord {
                txn_id: 7,
                op: JournalOp::CommitAllocate,
                allocation_id: 888,
                extent,
            })
            .expect("append commit");
            mgr.flush().expect("flush");
        }

        let err = match BlockManager::open(&path) {
            Ok(_) => panic!("expected replay failure"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Corrupt(msg) => {
                assert!(
                    msg.contains("no matching prepare"),
                    "unexpected error: {}",
                    msg
                );
            }
            other => panic!("expected corruption error, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn replay_rejects_prepare_commit_payload_mismatch() {
        let path = temp_loopback("prepare-commit-payload-mismatch");
        {
            let mut mgr = BlockManager::format(
                &path,
                FormatOptions {
                    block_size: 4096,
                    journal_blocks: 16,
                    total_bytes: Some(4 * 1024 * 1024),
                },
            )
            .expect("format device");
            let base = mgr.header().data_start_block;
            let prepare_extent = Extent {
                start_block: base,
                block_count: 1,
            };
            let commit_extent = Extent {
                start_block: base.saturating_add(1),
                block_count: 1,
            };
            mgr.append_journal_record(JournalRecord {
                txn_id: 9,
                op: JournalOp::PrepareAllocate,
                allocation_id: 321,
                extent: prepare_extent,
            })
            .expect("append prepare");
            mgr.append_journal_record(JournalRecord {
                txn_id: 9,
                op: JournalOp::CommitAllocate,
                allocation_id: 321,
                extent: commit_extent,
            })
            .expect("append commit");
            mgr.flush().expect("flush");
        }

        let err = match BlockManager::open(&path) {
            Ok(_) => panic!("expected replay corruption"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Corrupt(msg) => {
                assert!(
                    msg.contains("payload mismatch"),
                    "unexpected error: {}",
                    msg
                );
            }
            other => panic!("expected corruption error, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn replay_rejects_prepare_commit_op_mismatch() {
        let path = temp_loopback("prepare-commit-op-mismatch");
        {
            let mut mgr = BlockManager::format(
                &path,
                FormatOptions {
                    block_size: 4096,
                    journal_blocks: 16,
                    total_bytes: Some(4 * 1024 * 1024),
                },
            )
            .expect("format device");
            let extent = Extent {
                start_block: mgr.header().data_start_block,
                block_count: 1,
            };
            mgr.append_journal_record(JournalRecord {
                txn_id: 10,
                op: JournalOp::PrepareFree,
                allocation_id: 444,
                extent,
            })
            .expect("append prepare");
            mgr.append_journal_record(JournalRecord {
                txn_id: 10,
                op: JournalOp::CommitAllocate,
                allocation_id: 444,
                extent,
            })
            .expect("append commit");
            mgr.flush().expect("flush");
        }

        let err = match BlockManager::open(&path) {
            Ok(_) => panic!("expected replay corruption"),
            Err(err) => err,
        };
        match err {
            BlockManagerError::Corrupt(msg) => {
                assert!(msg.contains("op mismatch"), "unexpected error: {}", msg);
            }
            other => panic!("expected corruption error, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn replay_applies_committed_allocate_without_runtime_apply() {
        let path = temp_loopback("commit-allocate-crash-window");
        let (allocation_id, extent) = {
            let mut mgr = BlockManager::format(
                &path,
                FormatOptions {
                    block_size: 4096,
                    journal_blocks: 16,
                    total_bytes: Some(4 * 1024 * 1024),
                },
            )
            .expect("format device");
            let extent = Extent {
                start_block: mgr.header().data_start_block,
                block_count: 2,
            };
            let allocation_id = 7_001;
            let txn_id = 91;
            mgr.append_journal_record(JournalRecord {
                txn_id,
                op: JournalOp::PrepareAllocate,
                allocation_id,
                extent,
            })
            .expect("append prepare allocate");
            mgr.append_journal_record(JournalRecord {
                txn_id,
                op: JournalOp::CommitAllocate,
                allocation_id,
                extent,
            })
            .expect("append commit allocate");
            mgr.flush().expect("flush");
            (allocation_id, extent)
        };

        let reopened = BlockManager::open(&path).expect("reopen");
        assert_eq!(reopened.allocation_count(), 1);
        assert_eq!(reopened.allocations.get(&allocation_id), Some(&extent));
        assert_eq!(
            reopened.last_applied_journal_txn_id, 91,
            "replay should advance last applied txid to committed allocate"
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn replay_applies_committed_free_without_runtime_apply() {
        let path = temp_loopback("commit-free-crash-window");
        let (allocation_id, extent) = {
            let mut mgr = BlockManager::format(
                &path,
                FormatOptions {
                    block_size: 4096,
                    journal_blocks: 16,
                    total_bytes: Some(4 * 1024 * 1024),
                },
            )
            .expect("format device");

            let handle = mgr.allocate_bytes(8 * 1024).expect("allocate");
            let allocation_id = handle.allocation_id;
            let extent = handle.extent;
            let txn_id = mgr.next_journal_txn_id;
            mgr.append_journal_record(JournalRecord {
                txn_id,
                op: JournalOp::PrepareFree,
                allocation_id,
                extent,
            })
            .expect("append prepare free");
            mgr.append_journal_record(JournalRecord {
                txn_id,
                op: JournalOp::CommitFree,
                allocation_id,
                extent,
            })
            .expect("append commit free");
            mgr.flush().expect("flush");
            (allocation_id, extent)
        };

        let mut reopened = BlockManager::open(&path).expect("reopen");
        assert_eq!(reopened.allocation_count(), 0);
        assert!(
            !reopened.allocations.contains_key(&allocation_id),
            "allocation should be removed by replayed free txn"
        );
        let reallocated = reopened.allocate_bytes(8 * 1024).expect("reallocate");
        assert_eq!(
            reallocated.extent.start_block, extent.start_block,
            "freed extent should be reusable after replay"
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn allocate_zero_bytes_is_rejected() {
        let path = temp_loopback("alloc-zero");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");

        match mgr.allocate_bytes(0) {
            Err(BlockManagerError::InvalidArgument(msg)) => {
                assert!(msg.contains("greater than 0"), "unexpected error: {}", msg);
            }
            other => panic!("expected invalid argument, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn raw_extent_address_rejects_invalid_sizes() {
        let path = temp_loopback("raw-size-invalid");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");

        let handle = mgr.allocate_bytes(8 * 1024).expect("allocate");
        match mgr.raw_extent_address(handle, 0) {
            Err(BlockManagerError::InvalidArgument(msg)) => {
                assert!(
                    msg.contains("exceeds allocation capacity"),
                    "unexpected error: {}",
                    msg
                );
            }
            other => panic!("expected invalid argument, got {:?}", other),
        }
        match mgr.raw_extent_address(handle, 16 * 1024) {
            Err(BlockManagerError::InvalidArgument(msg)) => {
                assert!(
                    msg.contains("exceeds allocation capacity"),
                    "unexpected error: {}",
                    msg
                );
            }
            other => panic!("expected invalid argument, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn handle_io_rejects_out_of_bounds_accesses() {
        let path = temp_loopback("handle-oob");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");

        let handle = mgr.allocate_bytes(4096).expect("allocate");
        match mgr.write(handle, 4090, &[1, 2, 3, 4, 5, 6, 7, 8]) {
            Err(BlockManagerError::OutOfBounds { .. }) => {}
            other => panic!("expected out-of-bounds write rejection, got {:?}", other),
        }
        match mgr.read(handle, 4090, 8) {
            Err(BlockManagerError::OutOfBounds { .. }) => {}
            other => panic!("expected out-of-bounds read rejection, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn raw_address_io_rejects_out_of_bounds_accesses() {
        let path = temp_loopback("address-oob");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");

        let handle = mgr.allocate_bytes(8192).expect("allocate");
        let address = mgr.raw_extent_address(handle, 4096).expect("address");

        match mgr.write_address(&address, 4090, &[1, 2, 3, 4, 5, 6, 7, 8]) {
            Err(BlockManagerError::OutOfBounds { .. }) => {}
            other => panic!(
                "expected out-of-bounds raw-address write rejection, got {:?}",
                other
            ),
        }
        match mgr.read_address(&address, 4090, 8) {
            Err(BlockManagerError::OutOfBounds { .. }) => {}
            other => panic!(
                "expected out-of-bounds raw-address read rejection, got {:?}",
                other
            ),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn raw_address_rejects_mismatched_device_and_offset() {
        let path = temp_loopback("address-mismatch");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");

        let handle = mgr.allocate_bytes(4096).expect("allocate");
        let mut address = mgr.raw_extent_address(handle, 4096).expect("address");

        address.device_id = Uuid::new_v4();
        match mgr.read_address(&address, 0, 1) {
            Err(BlockManagerError::InvalidArgument(msg)) => {
                assert!(
                    msg.contains("does not match manager device"),
                    "unexpected error: {}",
                    msg
                );
            }
            other => panic!("expected device mismatch rejection, got {:?}", other),
        }

        let mut address = mgr.raw_extent_address(handle, 4096).expect("address");
        address.offset_bytes = address.offset_bytes.saturating_add(4096);
        match mgr.read_address(&address, 0, 1) {
            Err(BlockManagerError::Corrupt(msg)) => {
                assert!(
                    msg.contains("does not match allocation"),
                    "unexpected error: {}",
                    msg
                );
            }
            other => panic!("expected offset mismatch rejection, got {:?}", other),
        }

        let mut address = mgr.raw_extent_address(handle, 4096).expect("address");
        address.extent_gen = address.extent_gen.saturating_add(1);
        match mgr.read_address(&address, 0, 1) {
            Err(BlockManagerError::Corrupt(msg)) => {
                assert!(msg.contains("generation"), "unexpected error: {}", msg);
            }
            other => panic!("expected generation mismatch rejection, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn free_addresses_validation_error_is_atomic() {
        let path = temp_loopback("free-addresses-atomic");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");

        let a = mgr.allocate_bytes(4096).expect("allocate a");
        let b = mgr.allocate_bytes(4096).expect("allocate b");
        let a_addr = mgr.raw_extent_address(a, 4096).expect("address a");
        let mut b_addr = mgr.raw_extent_address(b, 4096).expect("address b");
        b_addr.device_id = Uuid::new_v4();

        match mgr.free_addresses(&[a_addr.clone(), b_addr]) {
            Err(BlockManagerError::InvalidArgument(_)) => {}
            other => panic!(
                "expected validation rejection before free mutation, got {:?}",
                other
            ),
        }

        assert_eq!(
            mgr.allocation_count(),
            2,
            "validation failures must not partially free allocations"
        );
        let got = mgr
            .read_address(&a_addr, 0, 1)
            .expect("a should still exist");
        assert_eq!(got.len(), 1);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn free_unknown_allocation_is_rejected() {
        let path = temp_loopback("free-unknown");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");

        match mgr.free(999_999) {
            Err(BlockManagerError::UnknownAllocation(id)) => assert_eq!(id, 999_999),
            other => panic!("expected unknown allocation error, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn journal_full_is_reported_when_checkpoint_cannot_fit_live_set() {
        let path = temp_loopback("journal-full-live-set");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 1,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");

        let mut saw_full = false;
        for _ in 0..1024 {
            match mgr.allocate_bytes(4096) {
                Ok(_handle) => {}
                Err(BlockManagerError::JournalFull) => {
                    saw_full = true;
                    break;
                }
                Err(err) => panic!("unexpected allocate error before journal full: {:?}", err),
            }
        }

        assert!(
            saw_full,
            "journal full should be observable once live allocations exceed compactable journal capacity"
        );
        assert!(
            mgr.allocation_count() > 0,
            "live set should be non-empty in this pressure case"
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn checkpoint_compacts_journal_under_churn_and_bumps_superblock_generation() {
        let path = temp_loopback("checkpoint-churn");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 1,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");
        let initial_generation = mgr.header().superblock_generation;

        for _ in 0..512 {
            let handle = mgr.allocate_bytes(4096).expect("allocate under churn");
            mgr.free(handle.allocation_id).expect("free under churn");
        }
        mgr.flush().expect("flush after churn");

        assert!(
            mgr.header().superblock_generation > initial_generation,
            "checkpoint compaction should bump superblock generation"
        );
        assert_eq!(mgr.allocation_count(), 0);

        let reopened = BlockManager::open(&path).expect("reopen");
        assert_eq!(reopened.allocation_count(), 0);
        assert!(
            reopened.header().superblock_generation > initial_generation,
            "generation bump must persist across reopen"
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn checkpoint_compaction_survives_primary_generation_stale_crash_window() {
        let path = temp_loopback("checkpoint-crash-window");
        let (latest_header, live_allocations) = {
            let mut mgr = BlockManager::format(
                &path,
                FormatOptions {
                    block_size: 4096,
                    journal_blocks: 1,
                    total_bytes: Some(8 * 1024 * 1024),
                },
            )
            .expect("format device");

            // Keep a small live set so checkpoint compaction has committed state to preserve.
            let keep = mgr.allocate_bytes(8 * 1024).expect("allocate keep");
            for _ in 0..256 {
                let h = mgr.allocate_bytes(4096).expect("allocate churn");
                mgr.free(h.allocation_id).expect("free churn");
            }
            mgr.flush().expect("flush");
            assert!(
                mgr.header().superblock_generation > 1,
                "checkpoint compaction should bump generation during churn"
            );
            assert!(mgr.allocations.contains_key(&keep.allocation_id));
            (mgr.header().clone(), mgr.allocations.clone())
        };

        // Simulate crash window after backup superblock write but before primary update by
        // rewriting only primary to a stale generation while preserving backup as latest.
        let mut stale_primary = latest_header.clone();
        stale_primary.superblock_generation =
            stale_primary.superblock_generation.saturating_sub(1).max(1);
        let mut stale_header_block = vec![0u8; stale_primary.block_size as usize];
        encode_header(&stale_primary, &mut stale_header_block).expect("encode stale primary");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open loopback");
        file.write_at(&stale_header_block, 0)
            .expect("rewrite stale primary only");
        file.sync_all().expect("sync stale primary");

        let reopened = BlockManager::open(&path).expect("reopen");
        assert_eq!(
            reopened.header().superblock_generation,
            latest_header.superblock_generation,
            "reopen should pick newest valid superblock copy"
        );
        assert_eq!(
            reopened.allocations, live_allocations,
            "live allocation set must survive checkpoint crash window"
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn open_prefers_newer_valid_superblock_generation() {
        let path = temp_loopback("newer-superblock-generation");
        let mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");
        let mut newer = mgr.header().clone();
        newer.superblock_generation = newer.superblock_generation.saturating_add(5);
        let backup_offset = block_offset(mgr.header(), SECONDARY_SUPERBLOCK_BLOCK);
        drop(mgr);

        let mut header_block = vec![0u8; newer.block_size as usize];
        encode_header(&newer, &mut header_block).expect("encode newer header");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open loopback");
        file.write_at(&header_block, backup_offset)
            .expect("write newer backup superblock");
        file.sync_all().expect("sync newer backup");

        let reopened = BlockManager::open(&path).expect("reopen");
        assert_eq!(
            reopened.header().superblock_generation,
            newer.superblock_generation
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn open_supports_legacy_v1_superblock_layout() {
        let path = temp_loopback("legacy-v1-superblock");
        let mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");
        let legacy = encode_legacy_v1_header_for_test(mgr.header());
        let backup_offset = block_offset(mgr.header(), SECONDARY_SUPERBLOCK_BLOCK);
        let device_id = mgr.header().device_id;
        drop(mgr);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open loopback");
        file.write_at(&legacy, 0)
            .expect("write legacy primary superblock");
        file.write_at(&legacy, backup_offset)
            .expect("write legacy backup superblock");
        file.sync_all().expect("sync legacy headers");

        let reopened = BlockManager::open(&path).expect("reopen legacy header");
        assert_eq!(reopened.header().device_id, device_id);
        assert_eq!(reopened.header().superblock_generation, 1);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn allocator_health_snapshot_reports_journal_and_ag_state() {
        let path = temp_loopback("health-snapshot");
        let mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");

        let health = mgr.allocator_health_snapshot();
        assert_eq!(
            health.superblock_generation,
            mgr.header().superblock_generation
        );
        assert_eq!(health.replay_last_txid, 0);
        assert_eq!(health.next_txid, 1);
        assert_eq!(health.journal_used_records, mgr.journal_cursor());
        assert!(health.journal_capacity_records >= health.journal_used_records);
        assert_eq!(
            health.journal_capacity_records,
            health.journal_used_records + health.journal_free_records
        );
        assert_eq!(health.data_blocks, health.used_blocks + health.free_blocks);
        assert_eq!(health.allocation_count, 0);
        assert!(!health.ags.is_empty());
        let ag_free_sum: u64 = health.ags.iter().map(|ag| ag.free_blocks).sum();
        assert_eq!(ag_free_sum, health.free_blocks);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn allocator_health_snapshot_tracks_fragmentation() {
        let path = temp_loopback("health-fragmentation");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");

        let a = mgr.allocate_bytes(4096).expect("allocate a");
        let b = mgr.allocate_bytes(4096).expect("allocate b");
        let _c = mgr.allocate_bytes(4096).expect("allocate c");
        mgr.free(b.allocation_id).expect("free middle extent");

        let health = mgr.allocator_health_snapshot();
        assert!(health.journal_used_records > 0);
        assert!(health.fragmentation_ppm > 0);
        assert_eq!(health.allocation_count, 2);
        let ag0 = health
            .ags
            .iter()
            .find(|ag| ag.ag_id == 0)
            .expect("AG0 present");
        assert!(ag0.fragmentation_ppm > 0);
        assert!(ag0.free_blocks > ag0.largest_free_extent_blocks);

        // Keep one extent handle in-scope to ensure allocation id accounting stays stable.
        assert!(a.allocation_id > 0);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn format_writes_backup_superblock_magic_copy() {
        let path = temp_loopback("backup-superblock-magic");
        let mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");
        let backup_offset = block_offset(mgr.header(), SECONDARY_SUPERBLOCK_BLOCK);
        drop(mgr);

        let file = OpenOptions::new()
            .read(true)
            .open(&path)
            .expect("open loopback");

        let mut primary_magic = [0u8; 16];
        file.read_at(&mut primary_magic, 0)
            .expect("read primary superblock magic");
        assert_eq!(
            primary_magic,
            crate::constants::SUPERBLOCK_MAGIC,
            "primary superblock magic should match expected signature"
        );

        let mut backup_magic = [0u8; 16];
        file.read_at(&mut backup_magic, backup_offset)
            .expect("read backup superblock magic");
        assert_eq!(
            backup_magic,
            crate::constants::SUPERBLOCK_MAGIC,
            "backup superblock magic should match expected signature"
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn open_recovers_when_primary_superblock_magic_is_invalid() {
        let path = temp_loopback("corrupt-superblock-magic");
        let mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");
        let device_id = mgr.header().device_id;
        drop(mgr);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open loopback");
        file.write_at(&[0u8; 16], 0)
            .expect("corrupt primary magic bytes");
        file.sync_all().expect("sync corruption");

        let reopened = BlockManager::open(&path).expect("reopen using backup superblock");
        assert_eq!(reopened.header().device_id, device_id);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn allocation_state_free_coalesces_adjacent_extents() {
        let mut state = AllocationState::new(0, 64, 1);
        state
            .remove_known_extent(Extent {
                start_block: 0,
                block_count: 16,
            })
            .expect("reserve leading span");
        assert_eq!(state.available_blocks(), 48);

        state
            .free_extent(Extent {
                start_block: 0,
                block_count: 8,
            })
            .expect("free first half");
        state
            .free_extent(Extent {
                start_block: 8,
                block_count: 8,
            })
            .expect("free second half");

        assert_eq!(state.available_blocks(), 64);
        assert_eq!(
            state.find_best_fit_extent(64),
            Some(Extent {
                start_block: 0,
                block_count: 64
            })
        );
    }

    #[test]
    fn batch_free_addresses_persists_and_coalesces_across_reopen() {
        let path = temp_loopback("batch-free-replay");

        let (first_start, first_addr, second_addr) = {
            let mut mgr = BlockManager::format(
                &path,
                FormatOptions {
                    block_size: 4096,
                    journal_blocks: 16,
                    total_bytes: Some(4 * 1024 * 1024),
                },
            )
            .expect("format device");

            let first = mgr.allocate_bytes(8 * 1024).expect("allocate first");
            let second = mgr.allocate_bytes(8 * 1024).expect("allocate second");
            let first_addr = mgr
                .raw_extent_address(first, 8 * 1024)
                .expect("address first");
            let second_addr = mgr
                .raw_extent_address(second, 8 * 1024)
                .expect("address second");

            mgr.free_addresses(&[first_addr.clone(), second_addr.clone()])
                .expect("batch free addresses");
            mgr.flush().expect("flush metadata");

            (first.extent.start_block, first_addr, second_addr)
        };

        let mut reopened = BlockManager::open(&path).expect("reopen");
        assert_eq!(reopened.allocation_count(), 0);
        match reopened.read_address(&first_addr, 0, 1) {
            Err(BlockManagerError::UnknownAllocation(id)) => {
                assert_eq!(id, first_addr.allocation_id);
            }
            other => panic!("expected first address to stay freed, got {:?}", other),
        }
        match reopened.read_address(&second_addr, 0, 1) {
            Err(BlockManagerError::UnknownAllocation(id)) => {
                assert_eq!(id, second_addr.allocation_id);
            }
            other => panic!("expected second address to stay freed, got {:?}", other),
        }

        let merged = reopened
            .allocate_bytes(16 * 1024)
            .expect("reallocate merged");
        assert_eq!(merged.extent.start_block, first_start);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn journal_capacity_boundary_is_exact() {
        let path = temp_loopback("journal-capacity-boundary");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 1,
                total_bytes: Some(16 * 1024 * 1024),
            },
        )
        .expect("format device");

        let max_records = mgr.max_journal_records();
        let max_txns = max_records / 2;
        for _ in 0..max_txns {
            mgr.allocate_bytes(4096)
                .expect("allocation should succeed until journal is exactly full");
        }
        match mgr.allocate_bytes(4096) {
            Err(BlockManagerError::JournalFull) => {}
            other => panic!("expected journal full at boundary+1, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn churn_reopen_preserves_allocator_invariants() {
        let path = temp_loopback("churn-reopen-invariants");

        let expected_allocations = {
            let mut mgr = BlockManager::format(
                &path,
                FormatOptions {
                    block_size: 4096,
                    journal_blocks: 32,
                    total_bytes: Some(32 * 1024 * 1024),
                },
            )
            .expect("format device");

            let data_blocks = mgr
                .header()
                .total_blocks
                .saturating_sub(mgr.header().data_start_block);
            let mut live_ids = Vec::<u64>::new();
            let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;

            for step in 0..1200usize {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);

                let should_free = !live_ids.is_empty() && (seed & 0b11) == 0;
                if should_free {
                    let idx = (seed as usize) % live_ids.len();
                    let id = live_ids.swap_remove(idx);
                    match mgr.free(id) {
                        Ok(()) => {}
                        Err(BlockManagerError::JournalFull) => break,
                        Err(err) => panic!("free during churn failed: {:?}", err),
                    }
                } else {
                    let blocks = 1 + ((seed >> 8) % 16);
                    let size_bytes = blocks.saturating_mul(4096);
                    match mgr.allocate_bytes(size_bytes) {
                        Ok(handle) => live_ids.push(handle.allocation_id),
                        Err(BlockManagerError::NoSpace { .. }) => {
                            if let Some(id) = live_ids.pop() {
                                match mgr.free(id) {
                                    Ok(()) | Err(BlockManagerError::JournalFull) => {}
                                    Err(err) => panic!("free to make room failed: {:?}", err),
                                }
                            }
                        }
                        Err(BlockManagerError::JournalFull) => break,
                        Err(err) => {
                            panic!("unexpected allocation error at step {}: {:?}", step, err)
                        }
                    }
                }
            }

            let mut extents = mgr.allocations.values().copied().collect::<Vec<_>>();
            extents.sort_by_key(|e| e.start_block);
            for pair in extents.windows(2) {
                let left = pair[0];
                let right = pair[1];
                assert!(
                    left.start_block.saturating_add(left.block_count) <= right.start_block,
                    "live allocation overlap detected: {:?} vs {:?}",
                    left,
                    right
                );
            }

            let live_used_blocks: u64 = extents.iter().map(|e| e.block_count).sum();
            assert_eq!(
                live_used_blocks,
                mgr.used_blocks(),
                "used block accounting should match live allocations"
            );
            assert_eq!(
                mgr.used_blocks().saturating_add(mgr.available_blocks()),
                data_blocks,
                "used + free blocks should remain constant"
            );
            mgr.flush().expect("flush before reopen");
            mgr.allocations.clone()
        };

        let reopened = BlockManager::open(&path).expect("reopen churned device");
        let data_blocks = reopened
            .header()
            .total_blocks
            .saturating_sub(reopened.header().data_start_block);
        assert_eq!(reopened.allocations, expected_allocations);
        assert_eq!(
            reopened
                .used_blocks()
                .saturating_add(reopened.available_blocks()),
            data_blocks,
            "accounting invariant should survive replay"
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn deterministic_replay_remains_stable_across_periodic_reopen_crash_loops() {
        let path = temp_loopback("deterministic-reopen-loops");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 32,
                total_bytes: Some(64 * 1024 * 1024),
            },
        )
        .expect("format device");
        let mut expected = HashMap::<u64, Extent>::new();
        let mut seed: u64 = 0x1BAD_F00D_CAFE_BEEF;
        let total_steps = 2400usize;

        for step in 0..total_steps {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let should_free = !expected.is_empty() && (seed & 0b11) == 0;
            if should_free {
                let idx = (seed as usize) % expected.len();
                let id = *expected.keys().nth(idx).expect("existing key");
                match mgr.free(id) {
                    Ok(()) => {
                        expected.remove(&id);
                    }
                    Err(BlockManagerError::JournalFull) => {}
                    Err(err) => panic!("free failed during reopen loop: {:?}", err),
                }
            } else {
                let blocks = 1 + ((seed >> 8) % 32);
                let size_bytes = blocks.saturating_mul(4096);
                match mgr.allocate_bytes(size_bytes) {
                    Ok(handle) => {
                        expected.insert(handle.allocation_id, handle.extent);
                    }
                    Err(BlockManagerError::NoSpace { .. }) => {
                        // Make room by freeing one tracked allocation.
                        if let Some(id) = expected.keys().next().copied() {
                            if mgr.free(id).is_ok() {
                                expected.remove(&id);
                            }
                        }
                    }
                    Err(BlockManagerError::JournalFull) => {}
                    Err(err) => panic!("allocate failed during reopen loop: {:?}", err),
                }
            }

            if (step + 1) % 80 == 0 {
                mgr.flush().expect("flush before reopen boundary");
                drop(mgr);
                mgr = BlockManager::open(&path).expect("reopen loop manager");
                assert_eq!(
                    mgr.allocations,
                    expected,
                    "allocation map drift after reopen boundary {}",
                    step + 1
                );
                mgr.validate_allocator_state()
                    .expect("allocator state valid after reopen");
            }
        }

        mgr.flush().expect("final flush");
        drop(mgr);
        let reopened = BlockManager::open(&path).expect("final reopen");
        assert_eq!(reopened.allocations, expected);
        reopened
            .validate_allocator_state()
            .expect("final allocator validation");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn validate_allocator_state_rejects_internal_index_drift() {
        let path = temp_loopback("validate-index-drift");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");

        mgr.allocation_state.ag_max_free_len[0] =
            mgr.allocation_state.ag_max_free_len[0].saturating_add(1);
        match mgr.validate_allocator_state() {
            Err(BlockManagerError::Corrupt(msg)) => {
                assert!(
                    msg.contains("max-free"),
                    "unexpected validation error: {}",
                    msg
                );
            }
            other => panic!("expected allocator validation failure, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn validate_allocator_state_rejects_live_allocation_overlap() {
        let path = temp_loopback("validate-live-overlap");
        let mut mgr = BlockManager::format(
            &path,
            FormatOptions {
                block_size: 4096,
                journal_blocks: 16,
                total_bytes: Some(4 * 1024 * 1024),
            },
        )
        .expect("format device");

        let first = mgr.allocate_bytes(8 * 1024).expect("allocate first");
        let second = mgr.allocate_bytes(8 * 1024).expect("allocate second");
        let overlapping = Extent {
            start_block: first.extent.start_block.saturating_add(1),
            block_count: second.extent.block_count,
        };
        mgr.allocations.insert(second.allocation_id, overlapping);

        match mgr.validate_allocator_state() {
            Err(BlockManagerError::Corrupt(msg)) => {
                assert!(
                    msg.contains("overlap"),
                    "unexpected validation error: {}",
                    msg
                );
            }
            other => panic!("expected overlap validation failure, got {:?}", other),
        }

        let _ = fs::remove_file(path);
    }
}
