use serde::Deserialize;

#[derive(Deserialize)]
pub struct UploadParams {
    pub file_id: String,
    pub total_chunks: usize,
    pub chunk_index: usize,
    pub original_filename: String,
    pub mime_type: Option<String>,
    pub checksum: Option<String>,
}
