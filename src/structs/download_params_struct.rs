use serde::Deserialize;

#[derive(Deserialize)]
pub struct DownloadParams {
    pub uid: String,
    pub file_name: String,
    pub offset: u64,
    pub chunk_size: usize,
}