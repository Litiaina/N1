pub const FORMAT_VERSION: u32 = 1;
pub const MAX_KEY_BYTES: usize = 4113;
pub const MAX_VALUE_BYTES: usize = 1024 * 1024;
pub const MAX_TRANSACTION_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_TRANSACTION_WRITES: usize = 4096;

#[derive(Clone, Debug)]
pub struct Options {
    pub cache_bytes: u64,
    pub write_buffer_bytes: u64,
    pub journal_bytes: u64,
    pub background_workers: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            cache_bytes: 64 * 1024 * 1024,
            write_buffer_bytes: 8 * 1024 * 1024,
            journal_bytes: 256 * 1024 * 1024,
            background_workers: 2,
        }
    }
}
