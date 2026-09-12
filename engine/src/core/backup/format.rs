use serde::{Deserialize, Serialize};

pub const FORMAT: &str = "n1-backup-v1";
pub const BLOCK_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_RECORD_BYTES: usize = 8 * 1024 * 1024;
/// Portable restore decodes one internal metadata file at a time. Separate
/// this limit from the streamed manifest record limit (large objects can have
/// many part/shard descriptors). Reject unsupported captures before publication.
pub const MAX_METADATA_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Root {
    pub path: String,
    pub index: bool,
    pub blob: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub format: String,
    pub engine_format: u16,
    pub id: String,
    pub created_at: i64,
    pub roots: Vec<Root>,
    pub records: u64,
    pub source_bytes: u64,
    pub manifest: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub root: usize,
    pub path: String,
    pub modified_seconds: i64,
    pub modified_nanos: u32,
    pub kind: Kind,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Kind {
    Directory,
    Link { target: String },
    File { size: u64, blocks: Vec<String> },
}
