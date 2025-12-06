use std::time::Duration;

pub const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024;
pub const AUTH_COLLECTION: &str = "auth";
pub const INITIAL_CHUNK_SIZE: u64 = 2 * 1024 * 1024;
pub const CHUNK_PREFIX: &str = "chunk_";
pub const IO_TIMEOUT: Duration = Duration::from_secs(120);
pub const MAX_CHUNK_SIZE: usize = 16 * 1024 * 1024;
pub const MIN_BUFFER_SIZE: usize = 5 * 1024 * 1024;
pub const MAX_BUFFER_SIZE: usize = 64 * 1024 * 1024;
pub const STORAGE_ROOT: &str = "storage-files";
pub const TMP_EXT: &str = "tmp";
pub const MERGE_CONCURRENCY: usize = 4;
pub const METADATA_FILENAME: &str = "metadata.json";
pub const OBJECT_METADATA_FILENAME: &str = "object-metadata.json";
pub const TEMP_UPLOAD_ROOT: &str = ".tmp";
pub const MAX_FILE_SIZE: usize = 5 * 1024 * 1024 * 1024;
