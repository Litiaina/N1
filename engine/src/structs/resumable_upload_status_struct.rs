use serde::Serialize;

#[derive(Serialize)]
pub struct ResumableUploadStatus {
    pub uploaded_chunks: Vec<bool>,
}
