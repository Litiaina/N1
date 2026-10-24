use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use ini::Ini;
use tokio::fs;
use ulid::Ulid;

use crate::core::disk_manager::{FS_STORAGE_ROOT, ObjectPathManager};
use crate::core::linux_io;
use crate::enums::app_error_enum::AppError;
use crate::settings::constants::CONFIG_FILE;

use super::models::{ExportConfig, PersistedExport};

const EXPORT_STATE_DIRECTORY: &str = ".n1-exports";
const EXPORT_BLOB_DIRECTORY: &str = ".n1-export-artifacts";
const STATE_FILE: &str = "state.json";
const MANIFEST_FILE: &str = "manifest.ndjson";

static EXPORT_CONFIG: LazyLock<ExportConfig> = LazyLock::new(load_export_config);

fn parsed_u64(section: Option<&ini::Properties>, key: &str, default: u64) -> u64 {
    section
        .and_then(|values| values.get(key))
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

fn parsed_usize(section: Option<&ini::Properties>, key: &str, default: usize) -> usize {
    section
        .and_then(|values| values.get(key))
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(default)
}

fn load_export_config() -> ExportConfig {
    let defaults = ExportConfig::default();
    let ini = Ini::load_from_file(CONFIG_FILE).ok();
    let section = ini
        .as_ref()
        .and_then(|config| config.section(Some("export")));

    ExportConfig {
        retention_seconds: parsed_u64(section, "retention_seconds", defaults.retention_seconds)
            .clamp(60, 30 * 24 * 60 * 60),
        cleanup_interval_seconds: parsed_u64(
            section,
            "cleanup_interval_seconds",
            defaults.cleanup_interval_seconds,
        )
        .clamp(5, 3600),
        max_concurrent_jobs: parsed_usize(
            section,
            "max_concurrent_jobs",
            defaults.max_concurrent_jobs,
        )
        .clamp(1, 32),
        max_queued_jobs: parsed_usize(section, "max_queued_jobs", defaults.max_queued_jobs)
            .clamp(1, 4096),
        manifest_validation_concurrency: parsed_usize(
            section,
            "manifest_validation_concurrency",
            defaults.manifest_validation_concurrency,
        )
        .clamp(1, 128),
        object_read_concurrency: parsed_usize(
            section,
            "object_read_concurrency",
            defaults.object_read_concurrency,
        )
        .clamp(1, 128),
        artifact_chunk_size_bytes: parsed_usize(
            section,
            "artifact_chunk_size_bytes",
            defaults.artifact_chunk_size_bytes,
        )
        .clamp(1024 * 1024, 1024 * 1024 * 1024),
        artifact_write_concurrency: parsed_usize(
            section,
            "artifact_write_concurrency",
            defaults.artifact_write_concurrency,
        )
        .clamp(1, 32),
        max_buffered_bytes_per_job: parsed_usize(
            section,
            "max_buffered_bytes_per_job",
            defaults.max_buffered_bytes_per_job,
        )
        .clamp(16 * 1024 * 1024, 8 * 1024 * 1024 * 1024usize),
        max_buffered_bytes_global: parsed_usize(
            section,
            "max_buffered_bytes_global",
            defaults.max_buffered_bytes_global,
        )
        .clamp(16 * 1024 * 1024, 64 * 1024 * 1024 * 1024usize),
        max_concurrent_downloads: parsed_usize(
            section,
            "max_concurrent_downloads",
            defaults.max_concurrent_downloads,
        )
        .clamp(1, 4096),
        max_retained_bytes: parsed_u64(section, "max_retained_bytes", defaults.max_retained_bytes),
    }
}

pub(super) fn export_config() -> &'static ExportConfig {
    &EXPORT_CONFIG
}

pub(super) fn validate_export_id(id: &str) -> Result<(), AppError> {
    Ulid::from_string(id)
        .map(|_| ())
        .map_err(|_| AppError::BadRequest("invalid export id".to_string()))
}

pub(super) fn state_root() -> Result<PathBuf, AppError> {
    let root = FS_STORAGE_ROOT.get().ok_or_else(|| {
        AppError::InternalServerError("N1 export state root is not initialized".to_string())
    })?;
    Ok(PathBuf::from(root).join(EXPORT_STATE_DIRECTORY))
}

pub(super) fn job_directory(id: &str) -> Result<PathBuf, AppError> {
    validate_export_id(id)?;
    Ok(state_root()?.join(id))
}

pub(super) fn manifest_path(id: &str) -> Result<PathBuf, AppError> {
    Ok(job_directory(id)?.join(MANIFEST_FILE))
}

pub(super) fn manifest_partial_path(id: &str) -> Result<PathBuf, AppError> {
    Ok(job_directory(id)?.join(format!("{MANIFEST_FILE}.partial")))
}

fn state_path(id: &str) -> Result<PathBuf, AppError> {
    Ok(job_directory(id)?.join(STATE_FILE))
}

pub(super) async fn ensure_state_root() -> Result<(), AppError> {
    fs::create_dir_all(state_root()?)
        .await
        .map_err(|error| AppError::Io(error, "create N1 export state root".to_string()))
}

pub(super) async fn save_state(state: &PersistedExport) -> Result<(), AppError> {
    let directory = job_directory(&state.id)?;
    fs::create_dir_all(&directory)
        .await
        .map_err(|error| AppError::Io(error, "create N1 export job directory".to_string()))?;
    let bytes = serde_json::to_vec(state).map_err(|error| {
        AppError::InternalServerError(format!("serialize N1 export state: {error}"))
    })?;
    linux_io::atomic_write(&state_path(&state.id)?, bytes).await
}

pub(super) async fn load_state(id: &str) -> Result<PersistedExport, AppError> {
    let bytes = fs::read(state_path(id)?)
        .await
        .map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound => AppError::NotFound,
            _ => AppError::Io(error, "read N1 export state".to_string()),
        })?;
    serde_json::from_slice(&bytes)
        .map_err(|error| AppError::InternalServerError(format!("decode N1 export state: {error}")))
}

pub(super) async fn list_states() -> Result<Vec<PersistedExport>, AppError> {
    list_states_with_policy(false).await
}

/// GC may only reclaim when absence of an active export is proven. Unlike
/// operator listings, it must not turn a corrupt/unreadable job into absence.
pub(super) async fn list_states_for_gc() -> Result<Vec<PersistedExport>, AppError> {
    list_states_with_policy(true).await
}

async fn list_states_with_policy(strict: bool) -> Result<Vec<PersistedExport>, AppError> {
    let root = state_root()?;
    list_states_at(&root, strict).await
}

#[cfg(test)]
pub(super) async fn list_states_for_gc_at(root: &Path) -> Result<Vec<PersistedExport>, AppError> {
    list_states_at(root, true).await
}

async fn list_states_at(root: &Path, strict: bool) -> Result<Vec<PersistedExport>, AppError> {
    let mut states = Vec::new();
    let mut entries = match fs::read_dir(root).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(states),
        Err(error) => return Err(AppError::Io(error, "scan N1 export states".to_string())),
    };
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|error| AppError::Io(error, "scan N1 export state entry".to_string()))?
    {
        let kind = entry
            .file_type()
            .await
            .map_err(|error| AppError::Io(error, "stat N1 export state entry".to_string()))?;
        let id = entry.file_name().to_string_lossy().to_string();
        if validate_export_id(&id).is_err() {
            continue;
        }
        if !kind.is_dir() {
            if strict {
                return Err(AppError::Conflict(
                    "export job is not a real directory; preserving GC candidates".into(),
                ));
            }
            continue;
        }
        let path = entry.path().join(STATE_FILE);
        if strict {
            match fs::symlink_metadata(&path).await {
                Ok(metadata) if metadata.file_type().is_file() => {}
                Ok(_) => {
                    return Err(AppError::Conflict(
                        "export state is not a real file; preserving GC candidates".into(),
                    ));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(AppError::Io(error, "stat N1 export state".into())),
            }
        }
        let state = match fs::read(path).await {
            Ok(bytes) => serde_json::from_slice::<PersistedExport>(&bytes).map_err(|error| {
                AppError::InternalServerError(format!("decode N1 export state: {error}"))
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Err(AppError::NotFound),
            Err(error) => Err(AppError::Io(error, "read N1 export state".into())),
        };
        match state {
            Ok(state) if strict && state.id != id => {
                return Err(AppError::Conflict(
                    "export state identity mismatch; preserving GC candidates".into(),
                ));
            }
            Ok(state) => states.push(state),
            Err(AppError::NotFound) => {}
            Err(error) if strict => return Err(error),
            Err(error) => {
                tracing::error!(export_id = %id, error = %error, "failed to load N1 export state")
            }
        }
    }
    Ok(states)
}

pub(super) fn blob_roots() -> Result<Vec<PathBuf>, AppError> {
    let roots = ObjectPathManager::new().get_blob_roots();
    if roots.is_empty() {
        return Err(AppError::ServiceUnavailable(
            "N1 export requires at least one healthy blob root".to_string(),
        ));
    }
    Ok(roots)
}

pub(super) fn artifact_job_directory(root: &Path, id: &str) -> PathBuf {
    root.join(EXPORT_BLOB_DIRECTORY).join(id)
}

pub(super) async fn cleanup_artifact_chunks(id: &str) {
    let Ok(roots) = blob_roots() else {
        return;
    };
    for root in roots {
        let _ = fs::remove_dir_all(artifact_job_directory(&root, id)).await;
    }
}

pub(super) async fn cleanup_orphan_artifact_jobs() -> Result<u64, AppError> {
    let mut removed = 0_u64;
    for root in blob_roots()? {
        let artifact_root = root.join(EXPORT_BLOB_DIRECTORY);
        let mut entries = match fs::read_dir(&artifact_root).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(AppError::Io(
                    error,
                    "scan N1 export artifact root".to_string(),
                ));
            }
        };
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|error| AppError::Io(error, "scan N1 export artifact job".to_string()))?
        {
            if !entry
                .file_type()
                .await
                .map_err(|error| AppError::Io(error, "stat N1 export artifact job".to_string()))?
                .is_dir()
            {
                continue;
            }
            let id = entry.file_name().to_string_lossy().to_string();
            if validate_export_id(&id).is_err() {
                continue;
            }
            match load_state(&id).await {
                Ok(_) => continue,
                Err(AppError::NotFound) => {}
                Err(error) => {
                    tracing::warn!(
                        export_id = %id,
                        error = %error,
                        "preserving export artifact whose state could not be verified"
                    );
                    continue;
                }
            }
            fs::remove_dir_all(entry.path()).await.map_err(|error| {
                AppError::Io(error, "remove orphan N1 export artifact".to_string())
            })?;
            removed = removed.saturating_add(1);
        }
    }
    Ok(removed)
}

pub(super) async fn remove_job(id: &str) {
    cleanup_artifact_chunks(id).await;
    if let Ok(directory) = job_directory(id) {
        let _ = fs::remove_dir_all(directory).await;
    }
}

pub(super) fn path_is_on_configured_blob_root(path: &Path) -> bool {
    ObjectPathManager::new()
        .get_blob_roots()
        .iter()
        .any(|root| path.starts_with(root))
}

#[cfg(test)]
pub(super) fn load_export_config_from(ini: &Ini) -> ExportConfig {
    let defaults = ExportConfig::default();
    let section = ini.section(Some("export"));
    ExportConfig {
        retention_seconds: parsed_u64(section, "retention_seconds", defaults.retention_seconds)
            .clamp(60, 30 * 24 * 60 * 60),
        cleanup_interval_seconds: parsed_u64(
            section,
            "cleanup_interval_seconds",
            defaults.cleanup_interval_seconds,
        )
        .clamp(5, 3600),
        max_concurrent_jobs: parsed_usize(
            section,
            "max_concurrent_jobs",
            defaults.max_concurrent_jobs,
        )
        .clamp(1, 32),
        max_queued_jobs: parsed_usize(section, "max_queued_jobs", defaults.max_queued_jobs)
            .clamp(1, 4096),
        manifest_validation_concurrency: parsed_usize(
            section,
            "manifest_validation_concurrency",
            defaults.manifest_validation_concurrency,
        )
        .clamp(1, 128),
        object_read_concurrency: parsed_usize(
            section,
            "object_read_concurrency",
            defaults.object_read_concurrency,
        )
        .clamp(1, 128),
        artifact_chunk_size_bytes: parsed_usize(
            section,
            "artifact_chunk_size_bytes",
            defaults.artifact_chunk_size_bytes,
        )
        .clamp(1024 * 1024, 1024 * 1024 * 1024),
        artifact_write_concurrency: parsed_usize(
            section,
            "artifact_write_concurrency",
            defaults.artifact_write_concurrency,
        )
        .clamp(1, 32),
        max_buffered_bytes_per_job: parsed_usize(
            section,
            "max_buffered_bytes_per_job",
            defaults.max_buffered_bytes_per_job,
        )
        .clamp(16 * 1024 * 1024, 8 * 1024 * 1024 * 1024usize),
        max_buffered_bytes_global: parsed_usize(
            section,
            "max_buffered_bytes_global",
            defaults.max_buffered_bytes_global,
        )
        .clamp(16 * 1024 * 1024, 64 * 1024 * 1024 * 1024usize),
        max_concurrent_downloads: parsed_usize(
            section,
            "max_concurrent_downloads",
            defaults.max_concurrent_downloads,
        )
        .clamp(1, 4096),
        max_retained_bytes: parsed_u64(section, "max_retained_bytes", defaults.max_retained_bytes),
    }
}
