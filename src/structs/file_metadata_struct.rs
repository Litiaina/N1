use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct FileMetadata {
    pub original_filename: Option<String>,
    pub mime_type: Option<String>,
    pub uploaded_at: String,
    pub merged_path: String,
}