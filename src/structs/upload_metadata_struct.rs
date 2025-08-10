use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct UploadMetadata {
    pub original_filename: String,
    pub mime_type: Option<String>,
    pub uploaded_at: String,
}