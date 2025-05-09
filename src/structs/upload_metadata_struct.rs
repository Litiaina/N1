use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct UploadMetadata {
    pub original_filename: Option<String>,
    pub mime_type: Option<String>,
    pub uploaded_at: String,
    pub merged_path: String,
}