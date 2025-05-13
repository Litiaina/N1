use std::time::Duration;

pub const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024;
pub const MAX_CHUNK_SIZE: usize = 50 * 1024 * 1024;
pub const IO_TIMEOUT: Duration = Duration::from_secs(30);
pub const CLEANUP_INTERVAL: Duration = Duration::from_secs(60 * 60);
pub const FILE_TTL: Duration = Duration::from_secs(60 * 60 * 24);
pub const ENV_FILE: &str = ".env";
pub const AUTH_COLLECTION: &str = "auth";