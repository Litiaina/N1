use crate::constants::{
    HEADER_CRC_OFFSET, HEADER_CRC_OFFSET_V1, HEADER_CRC_OFFSET_V2, HEADER_DECODE_BYTES,
    HEADER_FALLBACK_SCAN_LIMIT_BYTES, HEADER_VERSION, HEADER_VERSION_V1, HEADER_VERSION_V2,
    HEADER_VERSION_V3, JOURNAL_MAGIC, JOURNAL_RECORD_SIZE, SUPERBLOCK_MAGIC,
};
use crate::device_io::{read_exact_at, validate_block_size};
use crate::error::BlockManagerError;
use crate::types::{DeviceHeader, Extent, JournalOp, JournalRecord};
use crc32fast::Hasher;
use std::fs::File;
use std::io;
use uuid::Uuid;

pub(crate) fn encode_header(
    header: &DeviceHeader,
    out: &mut [u8],
) -> Result<(), BlockManagerError> {
    if out.len() < HEADER_CRC_OFFSET + 4 {
        return Err(BlockManagerError::InvalidArgument(
            "header buffer too small".to_string(),
        ));
    }
    out.fill(0);
    out[0..16].copy_from_slice(&SUPERBLOCK_MAGIC);
    out[16..20].copy_from_slice(&HEADER_VERSION.to_le_bytes());
    out[20..24].copy_from_slice(&header.block_size.to_le_bytes());
    out[24..32].copy_from_slice(&header.total_blocks.to_le_bytes());
    out[32..40].copy_from_slice(&header.journal_start_block.to_le_bytes());
    out[40..48].copy_from_slice(&header.journal_block_count.to_le_bytes());
    out[48..56].copy_from_slice(&header.data_start_block.to_le_bytes());
    out[56..72].copy_from_slice(header.device_id.as_bytes());
    out[72..80].copy_from_slice(&header.superblock_generation.to_le_bytes());
    out[80..88].copy_from_slice(&header.segment_catalog_start_block.to_le_bytes());
    out[88..96].copy_from_slice(&header.segment_catalog_block_count.to_le_bytes());
    out[96..104].copy_from_slice(&header.metadata_pool_block_count.to_le_bytes());
    out[104..112].copy_from_slice(&header.segment_size_bytes.to_le_bytes());

    let crc = crc32(&out[..HEADER_CRC_OFFSET]);
    out[HEADER_CRC_OFFSET..HEADER_CRC_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());
    Ok(())
}

pub(crate) fn read_header(file: &File) -> Result<DeviceHeader, BlockManagerError> {
    let mut best: Option<(DeviceHeader, u64)> = None;
    let mut first_error: Option<BlockManagerError> = None;

    let mut offset = 0u64;
    while offset <= HEADER_FALLBACK_SCAN_LIMIT_BYTES {
        match read_header_at(file, offset) {
            Ok(header) => {
                let replace = match &best {
                    None => true,
                    Some((current, current_offset)) => {
                        header.superblock_generation > current.superblock_generation
                            || (header.superblock_generation == current.superblock_generation
                                && offset < *current_offset)
                    }
                };
                if replace {
                    best = Some((header, offset));
                }
            }
            Err(BlockManagerError::Io(ref e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
                if offset == 0 && first_error.is_none() {
                    first_error = Some(BlockManagerError::Io(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "short read while reading primary superblock",
                    )));
                }
                break;
            }
            Err(err) => {
                if offset == 0 && first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }
        offset = offset.saturating_add(HEADER_DECODE_BYTES as u64);
    }

    if let Some((header, _)) = best {
        return Ok(header);
    }
    Err(first_error.unwrap_or_else(|| {
        BlockManagerError::InvalidDevice("no valid superblock copy found".to_string())
    }))
}

fn read_header_at(file: &File, offset: u64) -> Result<DeviceHeader, BlockManagerError> {
    let mut prefix = [0u8; HEADER_DECODE_BYTES];
    read_exact_at(file, &mut prefix, offset)?;
    decode_header_prefix(&prefix)
}

fn decode_header_prefix(
    prefix: &[u8; HEADER_DECODE_BYTES],
) -> Result<DeviceHeader, BlockManagerError> {
    if prefix[0..16] != SUPERBLOCK_MAGIC {
        return Err(BlockManagerError::InvalidDevice(
            "missing block manager superblock magic".to_string(),
        ));
    }

    let version = u32::from_le_bytes(prefix[16..20].try_into().expect("version bytes"));
    if version != HEADER_VERSION_V1 && version != HEADER_VERSION_V2 && version != HEADER_VERSION_V3
    {
        return Err(BlockManagerError::InvalidDevice(format!(
            "unsupported header version {}",
            version
        )));
    }

    let crc_offset = if version == HEADER_VERSION_V1 {
        HEADER_CRC_OFFSET_V1
    } else if version == HEADER_VERSION_V2 {
        HEADER_CRC_OFFSET_V2
    } else {
        HEADER_CRC_OFFSET
    };
    let expected_crc = u32::from_le_bytes(
        prefix[crc_offset..crc_offset + 4]
            .try_into()
            .expect("crc bytes"),
    );
    let actual_crc = crc32(&prefix[..crc_offset]);
    if expected_crc != actual_crc {
        return Err(BlockManagerError::Corrupt(
            "superblock checksum mismatch".to_string(),
        ));
    }

    let block_size = u32::from_le_bytes(prefix[20..24].try_into().expect("block size bytes"));
    validate_block_size(block_size)?;
    let total_blocks = u64::from_le_bytes(prefix[24..32].try_into().expect("total blocks"));
    let journal_start_block = u64::from_le_bytes(prefix[32..40].try_into().expect("journal start"));
    let journal_block_count = u64::from_le_bytes(prefix[40..48].try_into().expect("journal count"));
    let data_start_block = u64::from_le_bytes(prefix[48..56].try_into().expect("data start"));
    let device_id = Uuid::from_slice(&prefix[56..72])
        .map_err(|e| BlockManagerError::Corrupt(format!("invalid device uuid in header: {}", e)))?;
    let superblock_generation = if version == HEADER_VERSION_V2 || version == HEADER_VERSION_V3 {
        u64::from_le_bytes(prefix[72..80].try_into().expect("generation bytes"))
    } else {
        1
    };
    let segment_catalog_start_block = if version == HEADER_VERSION_V3 {
        u64::from_le_bytes(prefix[80..88].try_into().expect("segment catalog start"))
    } else {
        0
    };
    let segment_catalog_block_count = if version == HEADER_VERSION_V3 {
        u64::from_le_bytes(prefix[88..96].try_into().expect("segment catalog count"))
    } else {
        0
    };
    let metadata_pool_block_count = if version == HEADER_VERSION_V3 {
        u64::from_le_bytes(prefix[96..104].try_into().expect("metadata pool block count"))
    } else {
        total_blocks.saturating_sub(data_start_block)
    };
    let segment_size_bytes = if version == HEADER_VERSION_V3 {
        u64::from_le_bytes(prefix[104..112].try_into().expect("segment size bytes"))
    } else {
        0
    };

    if journal_block_count == 0 || data_start_block <= journal_start_block {
        return Err(BlockManagerError::Corrupt(
            "invalid journal/data boundaries in header".to_string(),
        ));
    }

    Ok(DeviceHeader {
        device_id,
        superblock_generation: superblock_generation.max(1),
        block_size,
        total_blocks,
        journal_start_block,
        journal_block_count,
        data_start_block,
        segment_catalog_start_block,
        segment_catalog_block_count,
        metadata_pool_block_count,
        segment_size_bytes,
    })
}

pub(crate) fn encode_record(record: JournalRecord) -> [u8; JOURNAL_RECORD_SIZE] {
    let mut out = [0u8; JOURNAL_RECORD_SIZE];
    out[0..4].copy_from_slice(&JOURNAL_MAGIC.to_le_bytes());
    out[4] = record.op as u8;
    out[8..16].copy_from_slice(&record.allocation_id.to_le_bytes());
    out[16..24].copy_from_slice(&record.extent.start_block.to_le_bytes());
    out[24..32].copy_from_slice(&record.extent.block_count.to_le_bytes());
    out[36..44].copy_from_slice(&record.txn_id.to_le_bytes());
    let crc = record_crc32(&out);
    out[32..36].copy_from_slice(&crc.to_le_bytes());
    out
}

pub(crate) fn decode_record(
    buf: &[u8; JOURNAL_RECORD_SIZE],
) -> Result<JournalRecord, BlockManagerError> {
    let magic = u32::from_le_bytes(buf[0..4].try_into().expect("magic bytes"));
    if magic != JOURNAL_MAGIC {
        return Err(BlockManagerError::Corrupt(
            "journal record magic mismatch".to_string(),
        ));
    }
    let crc_expected = u32::from_le_bytes(buf[32..36].try_into().expect("crc bytes"));
    let crc_actual = record_crc32(buf);
    if crc_expected != crc_actual {
        return Err(BlockManagerError::Corrupt(
            "journal record checksum mismatch".to_string(),
        ));
    }
    let op = match buf[4] {
        1 => JournalOp::Allocate,
        2 => JournalOp::Free,
        3 => JournalOp::PrepareAllocate,
        4 => JournalOp::PrepareFree,
        5 => JournalOp::CommitAllocate,
        6 => JournalOp::CommitFree,
        other => {
            return Err(BlockManagerError::Corrupt(format!(
                "unknown journal op {}",
                other
            )));
        }
    };
    Ok(JournalRecord {
        txn_id: u64::from_le_bytes(buf[36..44].try_into().expect("txn id")),
        op,
        allocation_id: u64::from_le_bytes(buf[8..16].try_into().expect("alloc id")),
        extent: Extent {
            start_block: u64::from_le_bytes(buf[16..24].try_into().expect("extent start")),
            block_count: u64::from_le_bytes(buf[24..32].try_into().expect("extent count")),
        },
    })
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

fn record_crc32(buf: &[u8; JOURNAL_RECORD_SIZE]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(&buf[..32]);
    hasher.update(&buf[36..44]);
    hasher.finalize()
}
