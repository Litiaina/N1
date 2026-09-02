mod builder;
mod download;
mod manifest;
mod models;
mod store;

use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::Ordering;

use axum::Json;
use axum::extract::Path;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use chrono::Utc;
use dashmap::DashMap;
use tokio::sync::{Mutex, Semaphore};
use ulid::Ulid;

use crate::core::disk_manager::DownloadArchiveRequest;
use crate::core::io_scheduler::{DiskIoClass, acquire_disk_io};
use crate::enums::app_error_enum::AppError;
use crate::structs::auth_structs::Claims;

use builder::{ActiveProgress, build_artifact};
pub(crate) use download::safe_download_filename as sanitize_export_filename;
use download::{build_download_response, safe_download_filename};
use manifest::capture_manifest;
use models::PersistedExport;
pub use models::{CreateExportResponse, ExportPhase, ExportStatusResponse};
use store::{
    blob_roots, cleanup_artifact_chunks, ensure_state_root, export_config, list_states, load_state,
    remove_job, save_state,
};

static ACTIVE_EXPORTS: LazyLock<DashMap<String, Arc<ActiveProgress>>> = LazyLock::new(DashMap::new);
static EXPORT_JOB_LIMIT: LazyLock<Semaphore> =
    LazyLock::new(|| Semaphore::new(export_config().max_concurrent_jobs));
static EXPORT_QUOTA_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

const MAX_EXPLICIT_EXPORT_KEYS: usize = 10_000;
const MAX_EXPORT_PREFIXES: usize = 1_024;
const MAX_EXPORT_FILENAME_CHARS: usize = 255;

fn now_unix() -> i64 {
    Utc::now().timestamp()
}

fn validate_create_request(request: &DownloadArchiveRequest) -> Result<(String, String), AppError> {
    if request.include_internal {
        return Err(AppError::BadRequest(
            "bulk export cannot include N1 internal namespace state".to_string(),
        ));
    }
    if request.keys.len().saturating_add(request.entries.len()) > MAX_EXPLICIT_EXPORT_KEYS {
        return Err(AppError::BadRequest(format!(
            "bulk export accepts at most {MAX_EXPLICIT_EXPORT_KEYS} explicit keys; use prefixes for larger selections"
        )));
    }
    if !request.entries.is_empty()
        && (!request.keys.is_empty()
            || request.prefix.is_some()
            || !request.prefixes.is_empty()
            || request.export_all)
    {
        return Err(AppError::BadRequest(
            "mapped export entries cannot be combined with keys, prefixes, or export_all"
                .to_string(),
        ));
    }
    let prefix_count = request.prefixes.len() + usize::from(request.prefix.is_some());
    if prefix_count > MAX_EXPORT_PREFIXES {
        return Err(AppError::BadRequest(format!(
            "bulk export accepts at most {MAX_EXPORT_PREFIXES} prefixes"
        )));
    }
    if request
        .filename
        .as_deref()
        .is_some_and(|filename| filename.chars().count() > MAX_EXPORT_FILENAME_CHARS)
    {
        return Err(AppError::BadRequest(format!(
            "bulk export filename exceeds {MAX_EXPORT_FILENAME_CHARS} characters"
        )));
    }
    let format = request.format.trim().to_ascii_lowercase();
    if format != "zip" && format != "tar" {
        return Err(AppError::BadRequest(
            "export format must be zip or tar".to_string(),
        ));
    }
    if request.entries.is_empty()
        && !request.export_all
        && request.keys.is_empty()
        && request
            .prefix
            .as_deref()
            .is_none_or(|prefix| prefix.trim().is_empty())
        && request
            .prefixes
            .iter()
            .all(|prefix| prefix.trim().is_empty())
    {
        return Err(AppError::BadRequest(
            "export requires object keys, prefixes, or export_all=true".to_string(),
        ));
    }
    let filename = safe_download_filename(request.filename.as_deref().unwrap_or("export"), &format);
    Ok((format, filename))
}

fn ensure_export_access(claims: &Claims, state: &PersistedExport) -> Result<(), AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".to_string()));
    }
    if claims.fragment != state.fragment {
        return Err(AppError::Forbidden(
            "Access denied to this export".to_string(),
        ));
    }
    Ok(())
}

pub(crate) async fn signed_export_context(
    claims: &Claims,
    id: &str,
) -> Result<(String, i64), AppError> {
    let state = load_state(id).await?;
    ensure_export_access(claims, &state)?;
    if state.expires_at_unix <= now_unix() {
        return Err(AppError::NotFound);
    }
    if state.phase != ExportPhase::Ready {
        return Err(AppError::Conflict(
            "export is not ready for download".to_string(),
        ));
    }
    Ok((state.fragment, state.expires_at_unix))
}

pub(crate) async fn download_export_with_signed_authority(
    fragment: &str,
    id: &str,
    headers: &HeaderMap,
) -> Result<Response, AppError> {
    let state = load_state(id).await?;
    if state.fragment != fragment {
        return Err(AppError::Unauthorized);
    }
    if state.expires_at_unix <= now_unix() {
        if !ACTIVE_EXPORTS.contains_key(id) {
            remove_job(id).await;
        }
        return Err(AppError::NotFound);
    }
    build_download_response(&state, headers).await
}

fn estimated_artifact_bytes(state: &PersistedExport) -> u64 {
    state
        .source_bytes_total
        .saturating_add(state.objects_total.saturating_mul(2048))
        .saturating_add(1024 * 1024)
}

async fn retained_bytes_excluding(id: &str) -> Result<u64, AppError> {
    Ok(list_states()
        .await?
        .into_iter()
        .filter(|state| state.id != id)
        .fold(0_u64, |total, state| {
            let bytes = if state.phase == ExportPhase::Ready {
                state.artifact_size_bytes.unwrap_or(0)
            } else if !state.phase.is_terminal() {
                state.reserved_bytes
            } else {
                0
            };
            total.saturating_add(bytes)
        }))
}

async fn reserve_export_capacity(state: &mut PersistedExport) -> Result<(), AppError> {
    let _guard = EXPORT_QUOTA_LOCK.lock().await;
    let estimate = estimated_artifact_bytes(state);
    let limit = export_config().max_retained_bytes;
    if limit > 0 {
        let used = retained_bytes_excluding(&state.id).await?;
        if used.saturating_add(estimate) > limit {
            return Err(AppError::ServiceUnavailable(format!(
                "N1 export retention quota cannot admit this job: used={used}, requested={estimate}, limit={limit}"
            )));
        }
    }
    state.reserved_bytes = estimate;
    save_state(state).await
}

async fn validate_final_capacity(
    state: &PersistedExport,
    artifact_size: u64,
) -> Result<(), AppError> {
    let limit = export_config().max_retained_bytes;
    if limit == 0 {
        return Ok(());
    }
    let _guard = EXPORT_QUOTA_LOCK.lock().await;
    let used = retained_bytes_excluding(&state.id).await?;
    if used.saturating_add(artifact_size) > limit {
        return Err(AppError::ServiceUnavailable(
            "N1 export artifact exceeded the configured retention quota".to_string(),
        ));
    }
    Ok(())
}

fn public_export_error(error: &AppError) -> String {
    match error {
        AppError::NotFound => "one or more selected object versions are unavailable".to_string(),
        AppError::Forbidden(_) => "export access was denied".to_string(),
        AppError::Conflict(message) if message == "export cancelled" => message.clone(),
        AppError::ServiceUnavailable(message) => message.clone(),
        AppError::BadRequest(message) => message.clone(),
        _ => "export failed while reading or materializing immutable object data".to_string(),
    }
}

async fn admit_new_export(state: &PersistedExport) -> Result<(), AppError> {
    let _guard = EXPORT_QUOTA_LOCK.lock().await;
    let queued = list_states()
        .await?
        .into_iter()
        .filter(|existing| !existing.phase.is_terminal())
        .count();
    if queued >= export_config().max_queued_jobs {
        return Err(AppError::ServiceUnavailable(format!(
            "N1 export queue is full; maximum queued jobs is {}",
            export_config().max_queued_jobs
        )));
    }
    save_state(state).await
}

async fn run_export_job(id: String, progress: Arc<ActiveProgress>) {
    let result = run_export_job_inner(&id, Arc::clone(&progress)).await;
    if let Err(error) = result {
        cleanup_artifact_chunks(&id).await;
        match load_state(&id).await {
            Ok(mut state) => {
                state.updated_at_unix = now_unix();
                state.reserved_bytes = 0;
                state.chunks.clear();
                state.artifact_size_bytes = None;
                state.etag_sha256 = None;
                if progress.cancel.load(Ordering::Acquire) {
                    state.phase = ExportPhase::Cancelled;
                    state.error = None;
                } else {
                    tracing::error!(export_id = %id, fragment = %state.fragment, error = %error, "N1 export job failed");
                    state.phase = ExportPhase::Failed;
                    state.error = Some(public_export_error(&error));
                }
                let _ = save_state(&state).await;
            }
            Err(load_error) => {
                tracing::error!(export_id = %id, error = %load_error, "failed to persist N1 export failure")
            }
        }
    }
    ACTIVE_EXPORTS.remove(&id);
}

async fn run_export_job_inner(id: &str, progress: Arc<ActiveProgress>) -> Result<(), AppError> {
    let _permit = EXPORT_JOB_LIMIT
        .acquire()
        .await
        .map_err(|_| AppError::InternalServerError("N1 export job scheduler closed".to_string()))?;
    progress.ensure_running()?;
    let mut state = load_state(id).await?;
    if state.phase.is_terminal() {
        return Ok(());
    }
    let io_guard = acquire_disk_io(
        DiskIoClass::BackgroundExport,
        Some(&state.fragment),
        "bulk export",
    )
    .await?;
    progress.attach_scheduler_progress(io_guard.progress_handle());

    if !state.manifest_ready {
        state.phase = ExportPhase::CapturingManifest;
        state.updated_at_unix = now_unix();
        state.error = None;
        save_state(&state).await?;
        let summary = capture_manifest(
            id,
            &state.fragment,
            &state.request,
            export_config().object_read_concurrency,
            Arc::clone(&progress),
        )
        .await?;
        progress.ensure_running()?;
        state.manifest_ready = true;
        state.objects_total = summary.objects_total;
        state.source_bytes_total = summary.source_bytes_total;
        state.objects_completed = 0;
        state.source_bytes_completed = 0;
        state.updated_at_unix = now_unix();
        reserve_export_capacity(&mut state).await?;
    }

    progress.ensure_running()?;
    state.phase = ExportPhase::Building;
    state.objects_completed = 0;
    state.source_bytes_completed = 0;
    state.updated_at_unix = now_unix();
    state.error = None;
    save_state(&state).await?;
    let artifact = build_artifact(
        id,
        &state.fragment,
        &state.format,
        blob_roots()?,
        export_config(),
        Arc::clone(&progress),
    )
    .await?;
    progress.ensure_running()?;
    validate_final_capacity(&state, artifact.size_bytes).await?;

    state.phase = ExportPhase::Ready;
    state.objects_completed = state.objects_total;
    state.source_bytes_completed = state.source_bytes_total;
    state.artifact_size_bytes = Some(artifact.size_bytes);
    state.etag_sha256 = Some(artifact.sha256);
    state.chunks = artifact.chunks;
    state.reserved_bytes = 0;
    state.updated_at_unix = now_unix();
    state.expires_at_unix = state
        .updated_at_unix
        .saturating_add(export_config().retention_seconds as i64);
    save_state(&state).await?;
    tracing::info!(
        export_id = %id,
        fragment = %state.fragment,
        objects = state.objects_total,
        source_bytes = state.source_bytes_total,
        artifact_bytes = state.artifact_size_bytes.unwrap_or(0),
        format = %state.format,
        "N1 export job is ready"
    );
    io_guard.complete();
    Ok(())
}

fn spawn_export_job(id: String) {
    if ACTIVE_EXPORTS.contains_key(&id) {
        return;
    }
    let progress = Arc::new(ActiveProgress::new());
    match ACTIVE_EXPORTS.entry(id.clone()) {
        dashmap::mapref::entry::Entry::Occupied(_) => return,
        dashmap::mapref::entry::Entry::Vacant(entry) => {
            entry.insert(Arc::clone(&progress));
        }
    }
    tokio::spawn(run_export_job(id, progress));
}

pub async fn create_export(
    claims: Claims,
    Json(mut request): Json<DownloadArchiveRequest>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".to_string()));
    }
    let (format, filename) = validate_create_request(&request)?;
    request.format = format.clone();
    request.filename = Some(filename.clone());
    let id = Ulid::new().to_string();
    let created_at = now_unix();
    let state = PersistedExport {
        id: id.clone(),
        fragment: claims.fragment,
        request,
        phase: ExportPhase::Queued,
        format,
        filename,
        created_at_unix: created_at,
        updated_at_unix: created_at,
        expires_at_unix: created_at.saturating_add(export_config().retention_seconds as i64),
        manifest_ready: false,
        objects_total: 0,
        objects_completed: 0,
        source_bytes_total: 0,
        source_bytes_completed: 0,
        reserved_bytes: 0,
        artifact_size_bytes: None,
        etag_sha256: None,
        chunks: Vec::new(),
        error: None,
    };
    ensure_state_root().await?;
    admit_new_export(&state).await?;
    spawn_export_job(id.clone());
    Ok((
        StatusCode::ACCEPTED,
        Json(CreateExportResponse {
            export_id: id.clone(),
            phase: ExportPhase::Queued,
            status_url: format!("/noa/v1/exports/{id}"),
            download_url: format!("/noa/v1/exports/{id}/download"),
            expires_at_unix: state.expires_at_unix,
        }),
    ))
}

pub async fn export_status(
    claims: Claims,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let mut state = load_state(&id).await?;
    ensure_export_access(&claims, &state)?;
    if state.expires_at_unix <= now_unix() && !ACTIVE_EXPORTS.contains_key(&id) {
        remove_job(&id).await;
        return Err(AppError::NotFound);
    }
    if let Some(active) = ACTIVE_EXPORTS.get(&id) {
        state.objects_completed = active.objects_completed.load(Ordering::Relaxed);
        state.source_bytes_completed = active.source_bytes_completed.load(Ordering::Relaxed);
    }
    Ok(Json(ExportStatusResponse::from_state(&state)))
}

pub async fn download_export(
    claims: Claims,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let state = load_state(&id).await?;
    ensure_export_access(&claims, &state)?;
    if state.expires_at_unix <= now_unix() {
        if !ACTIVE_EXPORTS.contains_key(&id) {
            remove_job(&id).await;
        }
        return Err(AppError::NotFound);
    }
    build_download_response(&state, &headers).await
}

pub async fn cancel_export(
    claims: Claims,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let mut state = load_state(&id).await?;
    ensure_export_access(&claims, &state)?;
    if let Some(active) = ACTIVE_EXPORTS.get(&id) {
        active.cancel.store(true, Ordering::Release);
        state.phase = ExportPhase::Cancelled;
        state.error = None;
        state.updated_at_unix = now_unix();
        save_state(&state).await?;
    } else if state.phase != ExportPhase::Cancelled {
        state.phase = ExportPhase::Cancelled;
        state.error = None;
        state.updated_at_unix = now_unix();
        state.reserved_bytes = 0;
        state.chunks.clear();
        state.artifact_size_bytes = None;
        state.etag_sha256 = None;
        cleanup_artifact_chunks(&id).await;
        save_state(&state).await?;
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(ExportStatusResponse::from_state(&state)),
    ))
}

async fn recover_and_cleanup_exports() -> Result<(), AppError> {
    ensure_state_root().await?;
    let now = now_unix();
    for state in list_states().await? {
        if state.expires_at_unix <= now {
            if let Some(active) = ACTIVE_EXPORTS.get(&state.id) {
                active.cancel.store(true, Ordering::Release);
            } else {
                remove_job(&state.id).await;
            }
            continue;
        }
        if !state.phase.is_terminal() {
            spawn_export_job(state.id);
        }
    }
    Ok(())
}

pub async fn run_export_manager() {
    if let Err(error) = recover_and_cleanup_exports().await {
        tracing::error!(error = %error, "N1 export manager startup recovery failed");
    }
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(
        export_config().cleanup_interval_seconds,
    ));
    interval.tick().await;
    loop {
        interval.tick().await;
        if let Err(error) = recover_and_cleanup_exports().await {
            tracing::error!(error = %error, "N1 export retention sweep failed");
        }
    }
}

#[cfg(test)]
mod tests;
