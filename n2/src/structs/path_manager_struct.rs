use crate::fs::fs_constants::{CHUNK_PREFIX, STORAGE_ROOT};
use crate::validation::directory_validation::sanitize_fragmented_path;
use std::path::PathBuf;

pub struct PathManager {
    pub uid: String,
    pub relative_path: PathBuf,
    pub file_id: String,
}

impl PathManager {
    pub fn new(uid: String, relative_path: String, file_id: String) -> Self {
        Self {
            uid,
            relative_path: sanitize_fragmented_path(&relative_path),
            file_id,
        }
    }

    pub fn upload_dir(&self) -> PathBuf {
        PathBuf::from(STORAGE_ROOT)
            .join(&self.uid)
            .join(&self.relative_path)
            .join(&self.file_id)
    }

    pub fn final_dir(&self) -> PathBuf {
        self.upload_dir()
            .parent()
            .unwrap_or_else(|| std::path::Path::new("/"))
            .to_path_buf()
    }

    pub fn chunk_path(&self, index: usize) -> PathBuf {
        self.upload_dir().join(format!("{}{}", CHUNK_PREFIX, index))
    }

    pub fn metadata_path(&self) -> PathBuf {
        self.upload_dir().join("session.json")
    }
}
