use std::env;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct FsdConfig {
    pub enabled: bool,
    pub staging_root: PathBuf,
    pub upload_part_size_bytes: usize,
}

impl Default for FsdConfig {
    fn default() -> Self {
        let staging_root = env::var("N1_FSD_STAGING_ROOT")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| env::temp_dir().join("n1-fsd").join("staging"));

        Self {
            enabled: true,
            staging_root,
            upload_part_size_bytes: 4 * 1024 * 1024,
        }
    }
}
