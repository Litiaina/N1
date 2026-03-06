use crate::codec::{decode_record, encode_header, encode_record, read_header};
use crate::constants::JOURNAL_RECORD_SIZE;
use crate::device_io::{
    block_offset, div_ceil, open_direct_file, read_exact_at, read_exact_at_direct,
    validate_block_size, write_all_at, write_all_at_direct, zero_region,
};
use crate::error::BlockManagerError;
use crate::types::{
    AllocationHandle, DeviceHeader, DirectExtentIo, Extent, FormatOptions, JournalOp,
    JournalRecord, RawExtentAddress,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;

#[cfg(unix)]
use std::os::unix::fs::FileTypeExt;

const MIN_AG_COUNT: usize = 1;
const MAX_AG_COUNT: usize = 16;
const TARGET_AG_BYTES: u64 = 32 * 1024 * 1024 * 1024;

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
            next_ag_hint: 0,
            ag_max_free_len: Vec::new(),
            ags_by_max_free_len: BTreeMap::new(),
        };
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
        self.ags.iter().position(|ag| ag.contains_block(block))
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

pub struct BlockManager {
    path: PathBuf,
    meta_file: File,
    direct_file: File,
    header: DeviceHeader,
    allocation_state: AllocationState,
    allocations: HashMap<u64, Extent>,
    next_allocation_id: u64,
    next_journal_txn_id: u64,
    last_applied_journal_txn_id: u64,
    journal_cursor: u64,
    journal_dirty: bool,
}

impl BlockManager {
    pub fn format<P: AsRef<Path>>(
        path: P,
        options: FormatOptions,
    ) -> Result<Self, BlockManagerError> {
        let path = path.as_ref().to_path_buf();
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
        if total_blocks <= options.journal_blocks.saturating_add(2) {
            return Err(BlockManagerError::InvalidArgument(format!(
                "device too small for block manager: total_blocks={} journal_blocks={}",
                total_blocks, options.journal_blocks
            )));
        }

        let header = DeviceHeader {
            device_id: Uuid::new_v4(),
            block_size,
            total_blocks,
            journal_start_block: 1,
            journal_block_count: options.journal_blocks.max(1),
            data_start_block: 1u64.saturating_add(options.journal_blocks.max(1)),
        };

        zero_region(
            &file,
            header.block_size,
            block_offset(&header, header.journal_start_block),
            header
                .journal_block_count
                .saturating_mul(u64::from(header.block_size)),
        )?;

        let mut header_block = vec![0u8; header.block_size as usize];
        encode_header(&header, &mut header_block)?;
        write_all_at(&file, &header_block, 0)?;
        file.sync_all()?;

        Self::open(path)
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, BlockManagerError> {
        let path = path.as_ref().to_path_buf();
        let meta_file = OpenOptions::new().read(true).write(true).open(&path)?;
        let header = read_header(&meta_file)?;
        let direct_file = open_direct_file(&path)?;
        let data_blocks = header.total_blocks.saturating_sub(header.data_start_block);
        let ag_count = derive_ag_count(data_blocks, header.block_size);

        let mut manager = Self {
            path,
            meta_file,
            direct_file,
            allocation_state: AllocationState::new(header.data_start_block, data_blocks, ag_count),
            allocations: HashMap::new(),
            next_allocation_id: 1,
            next_journal_txn_id: 1,
            last_applied_journal_txn_id: 0,
            journal_cursor: 0,
            journal_dirty: false,
            header,
        };
        manager.replay_journal()?;
        Ok(manager)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn header(&self) -> &DeviceHeader {
        &self.header
    }

    pub fn available_blocks(&self) -> u64 {
        self.allocation_state.available_blocks()
    }

    pub fn available_bytes(&self) -> u64 {
        self.available_blocks()
            .saturating_mul(u64::from(self.header.block_size))
    }

    pub fn allocation_count(&self) -> usize {
        self.allocations.len()
    }

    pub fn used_blocks(&self) -> u64 {
        self.header
            .total_blocks
            .saturating_sub(self.header.data_start_block)
            .saturating_sub(self.available_blocks())
    }

    pub fn used_bytes(&self) -> u64 {
        self.used_blocks()
            .saturating_mul(u64::from(self.header.block_size))
    }

    pub fn journal_cursor(&self) -> u64 {
        self.journal_cursor
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
        self.append_journal(JournalOp::Allocate, allocation_id, chosen)?;

        self.allocation_state.remove_known_extent(chosen)?;

        self.allocations.insert(allocation_id, chosen);
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

        self.append_journal(JournalOp::Free, allocation_id, extent)?;

        self.allocations.remove(&allocation_id);
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
            let extent = self.validate_raw_address(address)?;
            if seen.insert(address.allocation_id) {
                pending.push((address.allocation_id, extent));
            }
        }

        self.ensure_journal_capacity(pending.len())?;

        for (allocation_id, extent) in &pending {
            self.append_journal(JournalOp::Free, *allocation_id, *extent)?;

            self.allocations
                .remove(allocation_id)
                .ok_or(BlockManagerError::UnknownAllocation(*allocation_id))?;
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
        if self.journal_dirty {
            self.meta_file.sync_all()?;
            self.journal_dirty = false;
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

        let extent = *self
            .allocations
            .get(&address.allocation_id)
            .ok_or(BlockManagerError::UnknownAllocation(address.allocation_id))?;

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
            file: Arc::new(self.direct_file.try_clone()?),
            block_size: self.header.block_size,
            address: address.clone(),
        })
    }

    pub fn free_address(&mut self, address: &RawExtentAddress) -> Result<(), BlockManagerError> {
        let _extent = self.validate_raw_address(address)?;
        self.free(address.allocation_id)
    }

    fn replay_journal(&mut self) -> Result<(), BlockManagerError> {
        let journal_bytes = self
            .header
            .journal_block_count
            .saturating_mul(u64::from(self.header.block_size));
        let max_records = journal_bytes / JOURNAL_RECORD_SIZE as u64;

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
            self.next_journal_txn_id = self
                .next_journal_txn_id
                .max(record.txn_id.saturating_add(1));
            self.apply_record(record)?;
            self.journal_cursor = idx.saturating_add(1);
        }

        Ok(())
    }

    fn append_journal(
        &mut self,
        op: JournalOp,
        allocation_id: u64,
        extent: Extent,
    ) -> Result<(), BlockManagerError> {
        self.ensure_journal_capacity(1)?;
        let offset = block_offset(&self.header, self.header.journal_start_block).saturating_add(
            self.journal_cursor
                .saturating_mul(JOURNAL_RECORD_SIZE as u64),
        );

        let record = JournalRecord {
            txn_id: self.next_journal_txn_id,
            op,
            allocation_id,
            extent,
        };
        let buf = encode_record(record);
        write_all_at(&self.meta_file, &buf, offset)?;
        self.journal_cursor = self.journal_cursor.saturating_add(1);
        self.next_journal_txn_id = self.next_journal_txn_id.saturating_add(1);
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

    fn max_journal_records(&self) -> u64 {
        self.header
            .journal_block_count
            .saturating_mul(u64::from(self.header.block_size))
            / JOURNAL_RECORD_SIZE as u64
    }

    fn apply_record(&mut self, record: JournalRecord) -> Result<(), BlockManagerError> {
        if record.txn_id <= self.last_applied_journal_txn_id {
            return Ok(());
        }
        match record.op {
            JournalOp::Allocate => {
                self.allocation_state.remove_known_extent(record.extent)?;
                self.allocations.insert(record.allocation_id, record.extent);
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
                self.allocation_state.free_extent(record.extent)?;
            }
        }
        self.last_applied_journal_txn_id = record.txn_id;
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
    use std::fs;
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::FileExt;
    use std::time::{SystemTime, UNIX_EPOCH};

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
    fn open_rejects_corrupt_superblock_checksum() {
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
        drop(mgr);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open loopback");
        file.write_at(&[0xFF], 20)
            .expect("corrupt checksummed header byte");
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
        drop(mgr);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open loopback");
        let corrupt_offset = 4096 + 32;
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
        let allocation_id = {
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
            handle.allocation_id
        };

        // Copy the first journal record into the second slot to simulate replaying
        // a duplicated committed transaction after crash/retry windows.
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open loopback");
        let mut rec = [0u8; crate::constants::JOURNAL_RECORD_SIZE];
        file.read_at(&mut rec, 4096).expect("read first record");
        file.write_at(&rec, 4096 + crate::constants::JOURNAL_RECORD_SIZE as u64)
            .expect("write duplicate record");
        file.sync_all().expect("sync duplicate record");

        let reopened = BlockManager::open(&path).expect("reopen with duplicate txn");
        assert_eq!(reopened.allocation_count(), 1);
        assert!(reopened.allocations.contains_key(&allocation_id));

        let _ = fs::remove_file(path);
    }
}
