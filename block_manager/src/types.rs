use std::fs::File;
use std::path::PathBuf;
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
    pub superblock_generation: u64,
    pub block_size: u32,
    pub total_blocks: u64,
    pub journal_start_block: u64,
    pub journal_block_count: u64,
    pub data_start_block: u64,
    pub segment_catalog_start_block: u64,
    pub segment_catalog_block_count: u64,
    pub metadata_pool_block_count: u64,
    pub segment_size_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawExtentAddress {
    pub device_id: Uuid,
    pub allocation_id: u64,
    pub extent_gen: u32,
    pub offset_bytes: u64,
    pub size_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AllocationGroupHealth {
    pub ag_id: usize,
    pub start_block: u64,
    pub total_blocks: u64,
    pub free_blocks: u64,
    pub largest_free_extent_blocks: u64,
    pub fragmentation_ppm: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AllocatorHealthSnapshot {
    pub superblock_generation: u64,
    pub replay_last_txid: u64,
    pub next_txid: u64,
    pub journal_used_records: u64,
    pub journal_capacity_records: u64,
    pub journal_free_records: u64,
    pub data_blocks: u64,
    pub used_blocks: u64,
    pub free_blocks: u64,
    pub allocation_count: usize,
    pub largest_free_extent_blocks: u64,
    pub fragmentation_ppm: u32,
    pub ags: Vec<AllocationGroupHealth>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FormatTargetSignature {
    Empty,
    BlockManager,
    Foreign,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormatTargetPreflight {
    pub canonical_path: PathBuf,
    pub total_bytes: u64,
    pub is_block_device: bool,
    pub is_mounted: bool,
    pub signature: FormatTargetSignature,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormatNonceChallenge {
    pub nonce: String,
    pub preflight: FormatTargetPreflight,
    pub issued_unix_secs: u64,
    pub expires_unix_secs: u64,
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
    PrepareAllocate = 3,
    PrepareFree = 4,
    CommitAllocate = 5,
    CommitFree = 6,
}

impl JournalOp {
    pub(crate) fn to_prepare(self) -> Result<Self, &'static str> {
        match self {
            Self::Allocate => Ok(Self::PrepareAllocate),
            Self::Free => Ok(Self::PrepareFree),
            _ => Err("journal op is not a logical allocate/free op"),
        }
    }

    pub(crate) fn to_commit(self) -> Result<Self, &'static str> {
        match self {
            Self::Allocate => Ok(Self::CommitAllocate),
            Self::Free => Ok(Self::CommitFree),
            _ => Err("journal op is not a logical allocate/free op"),
        }
    }

    pub(crate) fn from_prepare_commit_pair(
        prepare: Self,
        commit: Self,
    ) -> Result<Self, &'static str> {
        match (prepare, commit) {
            (Self::PrepareAllocate, Self::CommitAllocate) => Ok(Self::Allocate),
            (Self::PrepareFree, Self::CommitFree) => Ok(Self::Free),
            _ => Err("journal prepare/commit op mismatch"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct JournalRecord {
    pub(crate) txn_id: u64,
    pub(crate) op: JournalOp,
    pub(crate) allocation_id: u64,
    pub(crate) extent: Extent,
}
