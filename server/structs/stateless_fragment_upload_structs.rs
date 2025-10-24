use crate::fs::fs_constants::{CHUNK_PREFIX, METADATA_FILENAME, STORAGE_ROOT, TEMP_UPLOAD_ROOT};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Deserialize)]
pub struct InitiateUploadParams {
    pub original_filename: String,
    pub total_size_bytes: u64,
    pub total_chunks: u64,
    pub mime_type: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct UploadMetadata {
    pub file_id: String,
    pub uid: String,
    pub original_filename: String,
    pub final_relative_path: String,
    pub total_size_bytes: u64,
    pub total_chunks: u64,
    pub mime_type: Option<String>,
    pub creation_timestamp: String,
}

#[derive(Serialize)]
pub struct UploadStatusResponse {
    pub original_filename: String,
    pub total_size_bytes: u64,
    pub uploaded_chunks: Vec<u64>,
    pub total_chunks: u64,
}

#[derive(Serialize)]
pub struct InitiateUploadResponse {
    pub file_id: String,
}

pub struct PathManager<'a> {
    pub file_id: &'a str,
}

impl<'a> PathManager<'a> {
    pub fn new(file_id: &'a str) -> Self {
        Self { file_id }
    }

    pub fn temp_upload_dir(&self) -> PathBuf {
        PathBuf::from(STORAGE_ROOT)
            .join(TEMP_UPLOAD_ROOT)
            .join(self.file_id)
    }

    pub fn chunk_path(&self, index: u64) -> PathBuf {
        self.temp_upload_dir()
            .join(format!("{}{}", CHUNK_PREFIX, index))
    }

    pub fn metadata_path(&self) -> PathBuf {
        self.temp_upload_dir().join(METADATA_FILENAME)
    }
}
