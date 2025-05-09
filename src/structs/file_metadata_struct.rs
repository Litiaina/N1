use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct FileMetadata {
    pub file_name: String,
    pub file_size: u64,
    pub last_modified: u64,
}