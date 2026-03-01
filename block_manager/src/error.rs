use std::fmt::{Display, Formatter};
use std::io;

#[derive(Debug)]
pub enum BlockManagerError {
    Io(io::Error),
    InvalidArgument(String),
    InvalidDevice(String),
    Corrupt(String),
    NoSpace {
        requested_blocks: u64,
        available_blocks: u64,
    },
    UnknownAllocation(u64),
    OutOfBounds {
        allocation_id: u64,
        offset: u64,
        len: usize,
        capacity: u64,
    },
    JournalFull,
}

impl Display for BlockManagerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io error: {}", e),
            Self::InvalidArgument(msg) => write!(f, "invalid argument: {}", msg),
            Self::InvalidDevice(msg) => write!(f, "invalid device: {}", msg),
            Self::Corrupt(msg) => write!(f, "corrupt block manager state: {}", msg),
            Self::NoSpace {
                requested_blocks,
                available_blocks,
            } => write!(
                f,
                "insufficient free blocks: requested {}, available {}",
                requested_blocks, available_blocks
            ),
            Self::UnknownAllocation(id) => write!(f, "unknown allocation id {}", id),
            Self::OutOfBounds {
                allocation_id,
                offset,
                len,
                capacity,
            } => write!(
                f,
                "allocation {} out of bounds: offset={} len={} capacity={}",
                allocation_id, offset, len, capacity
            ),
            Self::JournalFull => write!(f, "allocation journal is full"),
        }
    }
}

impl std::error::Error for BlockManagerError {}

impl From<io::Error> for BlockManagerError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
