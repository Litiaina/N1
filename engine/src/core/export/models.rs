use serde::{Deserialize, Serialize};

use crate::core::disk_manager::DownloadArchiveRequest;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportPhase {
    Queued,
    CapturingManifest,
    Building,
    Ready,
    Failed,
    Cancelled,
}

impl ExportPhase {
    pub(super) fn is_terminal(self) -> bool {
        matches!(self, Self::Ready | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ArtifactChunk {
    pub index: u64,
    pub path: String,
    pub logical_offset: u64,
    pub size_bytes: u64,
    #[serde(default)]
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct PersistedExport {
    pub id: String,
    pub fragment: String,
    pub cluster_id: String,
    pub membership_epoch: u64,
    pub binding_epoch: u64,
    pub request: DownloadArchiveRequest,
    pub phase: ExportPhase,
    pub format: String,
    pub filename: String,
    pub created_at_unix: i64,
    pub updated_at_unix: i64,
    pub expires_at_unix: i64,
    #[serde(default)]
    pub manifest_ready: bool,
    #[serde(default)]
    pub objects_total: u64,
    #[serde(default)]
    pub objects_completed: u64,
    #[serde(default)]
    pub source_bytes_total: u64,
    #[serde(default)]
    pub source_bytes_completed: u64,
    #[serde(default)]
    pub artifact_bytes_completed: u64,
    #[serde(default)]
    pub reserved_bytes: u64,
    #[serde(default)]
    pub artifact_size_bytes: Option<u64>,
    #[serde(default)]
    pub etag_sha256: Option<String>,
    #[serde(default)]
    pub chunks: Vec<ArtifactChunk>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ExportManifestEntry {
    pub archive_path: String,
    pub object_id: String,
    pub version_id: String,
    #[serde(default)]
    pub source_offset_bytes: u64,
    pub size_bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct CreateExportResponse {
    pub export_id: String,
    pub phase: ExportPhase,
    pub status_url: String,
    pub download_url: String,
    pub expires_at_unix: i64,
}

#[derive(Debug, Serialize)]
pub struct ExportStatusResponse {
    pub export_id: String,
    pub phase: ExportPhase,
    pub format: String,
    pub filename: String,
    pub created_at_unix: i64,
    pub updated_at_unix: i64,
    pub expires_at_unix: i64,
    pub objects_total: u64,
    pub objects_completed: u64,
    pub source_bytes_total: u64,
    pub source_bytes_completed: u64,
    pub artifact_bytes_completed: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_url: Option<String>,
}

impl ExportStatusResponse {
    pub(super) fn from_state(state: &PersistedExport) -> Self {
        Self {
            export_id: state.id.clone(),
            phase: state.phase,
            format: state.format.clone(),
            filename: state.filename.clone(),
            created_at_unix: state.created_at_unix,
            updated_at_unix: state.updated_at_unix,
            expires_at_unix: state.expires_at_unix,
            objects_total: state.objects_total,
            objects_completed: state.objects_completed,
            source_bytes_total: state.source_bytes_total,
            source_bytes_completed: state.source_bytes_completed,
            artifact_bytes_completed: if state.phase == ExportPhase::Ready {
                state
                    .artifact_size_bytes
                    .unwrap_or(state.artifact_bytes_completed)
            } else {
                state.artifact_bytes_completed
            },
            artifact_size_bytes: state.artifact_size_bytes,
            etag: state.etag_sha256.as_ref().map(|etag| format!("\"{etag}\"")),
            error: state.error.clone(),
            download_url: (state.phase == ExportPhase::Ready)
                .then(|| format!("/noa/v1/exports/{}/download", state.id)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportConfig {
    pub retention_seconds: u64,
    pub cleanup_interval_seconds: u64,
    pub max_concurrent_jobs: usize,
    pub max_queued_jobs: usize,
    pub manifest_validation_concurrency: usize,
    pub object_read_concurrency: usize,
    pub artifact_chunk_size_bytes: usize,
    pub artifact_write_concurrency: usize,
    pub max_buffered_bytes_per_job: usize,
    pub max_buffered_bytes_global: usize,
    pub max_concurrent_downloads: usize,
    pub max_retained_bytes: u64,
}

impl Default for ExportConfig {
    fn default() -> Self {
        let parallelism = std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(4);
        Self {
            retention_seconds: 24 * 60 * 60,
            cleanup_interval_seconds: 60,
            max_concurrent_jobs: 2,
            max_queued_jobs: 128,
            manifest_validation_concurrency: parallelism.saturating_mul(2).clamp(2, 32),
            object_read_concurrency: parallelism.saturating_mul(2).clamp(2, 32),
            artifact_chunk_size_bytes: 16 * 1024 * 1024,
            artifact_write_concurrency: 4,
            max_buffered_bytes_per_job: 128 * 1024 * 1024,
            max_buffered_bytes_global: 256 * 1024 * 1024,
            max_concurrent_downloads: 32,
            max_retained_bytes: 0,
        }
    }
}
