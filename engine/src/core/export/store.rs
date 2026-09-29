use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use ini::Ini;
use tokio::fs;

use crate::core::disk_manager::{FS_STORAGE_ROOT, ObjectPathManager};
use crate::core::fragment_metadata::durable_atomic_write;
use crate::enums::app_error_enum::AppError;
use crate::settings::constants::CONFIG_FILE;

use super::models::{ExportConfig, PersistedExport};

const EXPORT_STATE_DIRECTORY: &str = ".n2-exports";
const EXPORT_BLOB_DIRECTORY: &str = ".n2-export-artifacts";
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
    super::pins::parse_id(id).map(|_| ())
}

pub(super) fn state_root() -> Result<PathBuf, AppError> {
    let root = FS_STORAGE_ROOT.get().ok_or_else(|| {
        AppError::InternalServerError("N2 export state root is not initialized".to_string())
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
    let root = state_root()?;
    fs::create_dir_all(&root)
        .await
        .map_err(|error| AppError::Io(error, "create N2 export state root".to_string()))?;
    if let Some(parent) = root.parent() {
        fs::File::open(parent)
            .await
            .map_err(|error| AppError::Io(error, "open export runtime root".into()))?
            .sync_all()
            .await
            .map_err(|error| AppError::Io(error, "sync export runtime root".into()))?;
    }
    Ok(())
}

pub(super) async fn save_state(state: &PersistedExport) -> Result<(), AppError> {
    let directory = job_directory(&state.id)?;
    fs::create_dir_all(&directory)
        .await
        .map_err(|error| AppError::Io(error, "create N2 export job directory".to_string()))?;
    let bytes = serde_json::to_vec(state).map_err(|error| {
        AppError::InternalServerError(format!("serialize N2 export state: {error}"))
    })?;
    durable_atomic_write(&state_path(&state.id)?, &bytes).await?;
    // Persist the new job directory entry as well as the state-file rename.
    fs::File::open(state_root()?)
        .await
        .map_err(|e| AppError::Io(e, "open export state root".into()))?
        .sync_all()
        .await
        .map_err(|e| AppError::Io(e, "sync export state root".into()))?;
    Ok(())
}

pub(super) async fn load_state(id: &str) -> Result<PersistedExport, AppError> {
    super::pins::ensure_owner(id)?;
    let bytes = fs::read(state_path(id)?)
        .await
        .map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound => AppError::NotFound,
            _ => AppError::Io(error, "read N2 export state".to_string()),
        })?;
    serde_json::from_slice(&bytes)
        .map_err(|error| AppError::InternalServerError(format!("decode N2 export state: {error}")))
}

pub(super) async fn list_states() -> Result<Vec<PersistedExport>, AppError> {
    let root = state_root()?;
    let mut states = Vec::new();
    let mut entries = match fs::read_dir(&root).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(states),
        Err(error) => return Err(AppError::Io(error, "scan N2 export states".to_string())),
    };
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|error| AppError::Io(error, "scan N2 export state entry".to_string()))?
    {
        if !entry
            .file_type()
            .await
            .map_err(|error| AppError::Io(error, "stat N2 export state entry".to_string()))?
            .is_dir()
        {
            continue;
        }
        let id = entry.file_name().to_string_lossy().to_string();
        if validate_export_id(&id).is_err() {
            continue;
        }
        match load_state(&id).await {
            Ok(state) => states.push(state),
            Err(AppError::NotFound) => {}
            Err(error) => {
                tracing::error!(export_id = %id, error = %error, "failed to load N2 export state")
            }
        }
    }
    Ok(states)
}

pub(super) fn blob_roots() -> Result<Vec<PathBuf>, AppError> {
    let runtime = crate::distribution::membership::active_cluster_runtime()?;
    let roots = runtime
        .local_storage_roots
        .iter()
        .filter(|root| {
            crate::core::root_identity::verify_local_root_identity(&runtime, root).is_ok()
        })
        .map(|root| PathBuf::from(&root.path))
        .collect::<Vec<_>>();
    if roots.is_empty() {
        return Err(AppError::ServiceUnavailable(
            "N2 export requires at least one healthy blob root".to_string(),
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
                    "scan N2 export artifact root".to_string(),
                ));
            }
        };
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|error| AppError::Io(error, "scan N2 export artifact job".to_string()))?
        {
            if !entry
                .file_type()
                .await
                .map_err(|error| AppError::Io(error, "stat N2 export artifact job".to_string()))?
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
                AppError::Io(error, "remove orphan N2 export artifact".to_string())
            })?;
            removed = removed.saturating_add(1);
        }
    }
    Ok(removed)
}

pub(super) async fn remove_job(id: &str) {
    tokio::spawn(super::pins::release(id.to_string()));
    cleanup_artifact_chunks(id).await;
    if let Ok(directory) = job_directory(id) {
        let _ = fs::remove_dir_all(directory).await;
    }
}

pub(super) fn path_is_on_configured_blob_root(path: &Path) -> bool {
    ObjectPathManager::new()
        .get_storage_roots()
        .iter()
        .any(|root| {
            path.strip_prefix(root.join(EXPORT_BLOB_DIRECTORY))
                .is_ok_and(|relative| {
                    relative
                        .components()
                        .all(|component| matches!(component, std::path::Component::Normal(_)))
                })
        })
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
