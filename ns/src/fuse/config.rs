#[derive(Clone, Debug)]
pub struct FsdConfig {
    pub upload_part_size_bytes: usize,
}

impl Default for FsdConfig {
    fn default() -> Self {
        Self {
            upload_part_size_bytes: 4 * 1024 * 1024,
        }
    }
}
