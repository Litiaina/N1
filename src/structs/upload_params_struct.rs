use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct UploadParams {
    pub file_id: String,
    pub chunk_index: usize,
    pub total_chunks: usize,
    pub original_filename: Option<String>,
    pub mime_type: Option<String>,
}