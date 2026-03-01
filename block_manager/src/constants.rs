pub(crate) const SUPERBLOCK_MAGIC: [u8; 16] = *b"LITIAINA-BM-V1\0\0";
pub(crate) const HEADER_VERSION: u32 = 1;
pub(crate) const HEADER_CRC_OFFSET: usize = 72;
pub(crate) const JOURNAL_RECORD_SIZE: usize = 64;
pub(crate) const JOURNAL_MAGIC: u32 = 0x4C42_4D4A;
