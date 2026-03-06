use crate::enums::app_error_enum::AppError;
use crate::structs::path_manager_struct::PathManager;
use crate::structs::upload_params_struct::UploadParams;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::fs;
use tracing::error;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UploadSession {
    pub file_id: String,
    pub original_filename: String,
    pub total_chunks: usize,
    pub uploaded_chunks: Vec<bool>,
    pub mime_type: Option<String>,
    #[serde(skip)]
    pub metadata_path: PathBuf,
}

impl UploadSession {
    pub async fn new(paths: &PathManager, params: &UploadParams) -> Result<Self, AppError> {
        let session = UploadSession {
            file_id: params.file_id.clone(),
            original_filename: params.original_filename.clone(),
            total_chunks: params.total_chunks,
            uploaded_chunks: vec![false; params.total_chunks],
            mime_type: params.mime_type.clone(),
            metadata_path: paths.metadata_path(),
        };
        session.save().await?;
        Ok(session)
    }

    pub async fn from_disk(paths: &PathManager) -> Result<Self, AppError> {
        let metadata_path = paths.metadata_path();
        let content = crate::fs::fragmented_upload::with_io_timeout(
            "reading session metadata",
            fs::read_to_string(&metadata_path),
        )
        .await?;
        let mut session: UploadSession = serde_json::from_str(&content).map_err(|e| {
            error!("Failed to parse session metadata: {}", e);
            AppError::Conflict("Failed to parse session metadata.".to_string())
        })?;
        session.metadata_path = metadata_path;
        Ok(session)
    }

    pub async fn save(&self) -> Result<(), AppError> {
        let content = serde_json::to_string_pretty(self).map_err(|e| {
            error!("Failed to serialize session: {}", e);
            AppError::BadRequest("Failed to serialize session.".to_string())
        })?;
        crate::fs::fragmented_upload::with_io_timeout(
            "saving session metadata",
            fs::write(&self.metadata_path, content),
        )
        .await
    }

    pub fn mark_chunk_as_uploaded(&mut self, chunk_index: usize) {
        if chunk_index < self.total_chunks {
            self.uploaded_chunks[chunk_index] = true;
        }
    }

    pub fn is_complete(&self) -> bool {
        !self.uploaded_chunks.is_empty() && self.uploaded_chunks.iter().all(|&v| v)
    }
}
