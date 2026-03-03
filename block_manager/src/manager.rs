use crate::codec::{decode_record, encode_header, encode_record, read_header};
use crate::constants::JOURNAL_RECORD_SIZE;
use crate::device_io::{
    block_offset, div_ceil, open_direct_file, read_exact_at, read_exact_at_direct,
    validate_block_size, write_all_at, write_all_at_direct, zero_region,
};
use crate::error::BlockManagerError;
use crate::extent_map::{insert_and_merge_extent, remove_extent};
use crate::types::{
    AllocationHandle, DeviceHeader, DirectExtentIo, Extent, FormatOptions, JournalOp,
    JournalRecord, RawExtentAddress,
};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;

#[cfg(unix)]
use std::os::unix::fs::FileTypeExt;

pub struct BlockManager {
    path: PathBuf,
    meta_file: File,
    direct_file: File,
    header: DeviceHeader,
    free_extents: Vec<Extent>,
    allocations: HashMap<u64, Extent>,
    next_allocation_id: u64,
    journal_cursor: u64,
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

        let mut manager = Self {
            path,
            meta_file,
            direct_file,
            free_extents: vec![Extent {
                start_block: header.data_start_block,
                block_count: header.total_blocks.saturating_sub(header.data_start_block),
            }],
            allocations: HashMap::new(),
            next_allocation_id: 1,
            journal_cursor: 0,
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
        self.free_extents.iter().map(|e| e.block_count).sum()
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
        let (free_idx, chosen) = self
            .free_extents
            .iter()
            .enumerate()
            .find(|(_, extent)| extent.block_count >= requested_blocks)
            .map(|(idx, extent)| {
                (
                    idx,
                    Extent {
                        start_block: extent.start_block,
                        block_count: requested_blocks,
                    },
                )
            })
            .ok_or_else(|| BlockManagerError::NoSpace {
                requested_blocks,
                available_blocks: self.available_blocks(),
            })?;

        let allocation_id = self.next_allocation_id;
        self.append_journal(JournalRecord {
            op: JournalOp::Allocate,
            allocation_id,
            extent: chosen,
        })?;

        let free = &mut self.free_extents[free_idx];
        free.start_block = free.start_block.saturating_add(requested_blocks);
        free.block_count = free.block_count.saturating_sub(requested_blocks);
        if free.block_count == 0 {
            self.free_extents.remove(free_idx);
        }

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

        self.append_journal(JournalRecord {
            op: JournalOp::Free,
            allocation_id,
            extent,
        })?;

        self.allocations.remove(&allocation_id);
        insert_and_merge_extent(&mut self.free_extents, extent);
        Ok(())
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
            let record = decode_record(&record_buf)?;
            self.apply_record(record)?;
            self.journal_cursor = idx.saturating_add(1);
        }

        Ok(())
    }

    fn append_journal(&mut self, record: JournalRecord) -> Result<(), BlockManagerError> {
        let journal_bytes = self
            .header
            .journal_block_count
            .saturating_mul(u64::from(self.header.block_size));
        let offset = block_offset(&self.header, self.header.journal_start_block).saturating_add(
            self.journal_cursor
                .saturating_mul(JOURNAL_RECORD_SIZE as u64),
        );
        if offset.saturating_add(JOURNAL_RECORD_SIZE as u64)
            > block_offset(&self.header, self.header.journal_start_block)
                .saturating_add(journal_bytes)
        {
            return Err(BlockManagerError::JournalFull);
        }

        let buf = encode_record(record);
        write_all_at(&self.meta_file, &buf, offset)?;
        // Raw block devices can reject fdatasync even though fsync is valid.
        self.meta_file.sync_all()?;
        self.journal_cursor = self.journal_cursor.saturating_add(1);
        Ok(())
    }

    fn apply_record(&mut self, record: JournalRecord) -> Result<(), BlockManagerError> {
        match record.op {
            JournalOp::Allocate => {
                remove_extent(&mut self.free_extents, record.extent)?;
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
                insert_and_merge_extent(&mut self.free_extents, record.extent);
            }
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
}
