use std::fs::File;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FormatOptions {
    pub block_size: u32,
    pub journal_blocks: u64,
    pub total_bytes: Option<u64>,
}

impl Default for FormatOptions {
    fn default() -> Self {
        Self {
            block_size: 4096,
            journal_blocks: 128,
            total_bytes: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Extent {
    pub start_block: u64,
    pub block_count: u64,
}

impl Extent {
    pub fn byte_len(self, block_size: u32) -> u64 {
        self.block_count.saturating_mul(u64::from(block_size))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AllocationHandle {
    pub allocation_id: u64,
    pub extent: Extent,
}

impl AllocationHandle {
    pub fn capacity_bytes(self, block_size: u32) -> u64 {
        self.extent.byte_len(block_size)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeviceHeader {
    pub device_id: Uuid,
    pub block_size: u32,
    pub total_blocks: u64,
    pub journal_start_block: u64,
    pub journal_block_count: u64,
    pub data_start_block: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawExtentAddress {
    pub device_id: Uuid,
    pub allocation_id: u64,
    pub offset_bytes: u64,
    pub size_bytes: u64,
}

#[derive(Clone)]
pub struct DirectExtentIo {
    pub(crate) file: Arc<File>,
    pub(crate) block_size: u32,
    pub(crate) address: RawExtentAddress,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JournalOp {
    Allocate = 1,
    Free = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct JournalRecord {
    pub(crate) op: JournalOp,
    pub(crate) allocation_id: u64,
    pub(crate) extent: Extent,
}
