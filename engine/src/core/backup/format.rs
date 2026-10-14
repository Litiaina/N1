use serde::{Deserialize, Serialize};

pub const FORMAT: &str = "n1-backup-v1";
pub const ENGINE_FORMAT: u16 = crate::core::index_store::N1_ENGINE_FORMAT_VERSION;
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
    /// Original address prefix for untouched pre-portability metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_path: Option<String>,
    pub index: bool,
    pub blob: bool,
    /// Logical membership, never inferred from lexicographic mount-path order.
    #[serde(default)]
    pub member: Option<Member>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Member {
    pub pool_id: String,
    pub generation: u64,
    pub data_shards: u16,
    pub parity_shards: u16,
    pub slot: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub format: String,
    pub engine_format: u16,
    pub id: String,
    pub created_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_mode: Option<String>,
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
