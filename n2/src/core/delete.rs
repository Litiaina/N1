use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::Json as AxumJson;
use axum::extract::{Path as AxumPath, State};
use axum::{http::StatusCode, response::IntoResponse};

use chrono::Utc;
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::core::disk_manager::ObjectPathManager;
use crate::core::index;
use crate::core::operation_gate::{MaintenanceAccessMode, acquire_maintenance_mode};
use crate::enums::app_error_enum::AppError;
use crate::namespace::models::NamespaceMutationSummary;
use crate::namespace::runtime::global_namespace_runtime;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::ws_structs::{NamespaceChangeEvent, StorageEventPayload};

#[derive(Debug, Clone, Serialize, Default)]
pub struct TrashLifecycleJobStatus {
    pub operation: String,
    pub is_running: bool,
    pub phase: String,
    pub fragment: String,
    pub total_objects: u64,
    pub processed_objects: u64,
    pub successful_objects: u64,
    pub failed_objects: u64,
    pub total_trash_roots: u64,
    pub processed_trash_roots: u64,
    pub current_object: Option<String>,
    pub last_error: Option<String>,
    pub start_timestamp: i64,
    pub duration_seconds: u64,
}

static TRASH_LIFECYCLE_RUNNING: LazyLock<AtomicBool> = LazyLock::new(|| AtomicBool::new(false));
static RECOVER_ALL_TRASH_STATUS: LazyLock<Arc<RwLock<TrashLifecycleJobStatus>>> =
    LazyLock::new(|| Arc::new(RwLock::new(TrashLifecycleJobStatus::default())));
static EMPTY_TRASH_STATUS: LazyLock<Arc<RwLock<TrashLifecycleJobStatus>>> =
    LazyLock::new(|| Arc::new(RwLock::new(TrashLifecycleJobStatus::default())));
const PERSISTENT_TRASH_JOB_FILE: &str = "trash_job.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistentTrashJobRecord {
    operation: String,
    fragment: String,
    started_at: i64,
}

fn refresh_trash_job_duration(status: &mut TrashLifecycleJobStatus) {
    if status.start_timestamp > 0 {
        status.duration_seconds = Utc::now()
            .timestamp()
            .saturating_sub(status.start_timestamp)
            .max(0) as u64;
    }
}

async fn read_trash_job_status(
    slot: &'static LazyLock<Arc<RwLock<TrashLifecycleJobStatus>>>,
) -> TrashLifecycleJobStatus {
    let mut status = slot.read().await.clone();
    refresh_trash_job_duration(&mut status);
    status
}

async fn mutate_trash_job_status<F>(
    slot: &'static LazyLock<Arc<RwLock<TrashLifecycleJobStatus>>>,
    f: F,
) where
    F: FnOnce(&mut TrashLifecycleJobStatus),
{
    let mut status = slot.write().await;
    f(&mut status);
    refresh_trash_job_duration(&mut status);
}

pub async fn get_recover_all_trash_status() -> TrashLifecycleJobStatus {
    read_trash_job_status(&RECOVER_ALL_TRASH_STATUS).await
}

pub async fn get_empty_trash_status() -> TrashLifecycleJobStatus {
    read_trash_job_status(&EMPTY_TRASH_STATUS).await
}

pub async fn get_recover_all_trash_status_api(
    claims: Claims,
) -> Result<AxumJson<TrashLifecycleJobStatus>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    Ok(AxumJson(get_recover_all_trash_status().await))
}

pub async fn get_empty_trash_status_api(
    claims: Claims,
) -> Result<AxumJson<TrashLifecycleJobStatus>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    Ok(AxumJson(get_empty_trash_status().await))
}

async fn begin_trash_job(
    slot: &'static LazyLock<Arc<RwLock<TrashLifecycleJobStatus>>>,
    other_slot: &'static LazyLock<Arc<RwLock<TrashLifecycleJobStatus>>>,
    operation: &'static str,
    fragment: &str,
) -> Result<TrashLifecycleJobStatus, AppError> {
    let existing_same = read_trash_job_status(slot).await;
    if existing_same.is_running {
        if existing_same.fragment == fragment {
            return Ok(existing_same);
        }
        return Err(AppError::Conflict(format!(
            "{} is already running for fragment {}",
            operation.replace('_', " "),
            existing_same.fragment
        )));
    }

    let existing_other = read_trash_job_status(other_slot).await;
    if existing_other.is_running {
        return Err(AppError::Conflict(format!(
            "{} is already running for fragment {}",
            existing_other.operation.replace('_', " "),
            existing_other.fragment
        )));
    }

    if TRASH_LIFECYCLE_RUNNING.swap(true, Ordering::SeqCst) {
        let existing_same = read_trash_job_status(slot).await;
        if existing_same.is_running {
            return Ok(existing_same);
        }
        let existing_other = read_trash_job_status(other_slot).await;
        if existing_other.is_running {
            return Err(AppError::Conflict(format!(
                "{} is already running for fragment {}",
                existing_other.operation.replace('_', " "),
                existing_other.fragment
            )));
        }
        return Err(AppError::Conflict(
            "Another trash lifecycle job is already running".into(),
        ));
    }

    mutate_trash_job_status(slot, |status| {
        *status = TrashLifecycleJobStatus {
            operation: operation.to_string(),
            is_running: true,
            phase: "Initializing".to_string(),
            fragment: fragment.to_string(),
            start_timestamp: Utc::now().timestamp(),
            ..Default::default()
        };
    })
    .await;

    if let Err(err) = persist_persistent_trash_job_record(operation, fragment).await {
        mutate_trash_job_status(slot, |status| {
            status.is_running = false;
            status.phase = "Failed".to_string();
            status.last_error = Some(err.to_string());
        })
        .await;
        TRASH_LIFECYCLE_RUNNING.store(false, Ordering::SeqCst);
        return Err(err);
    }

    Ok(read_trash_job_status(slot).await)
}

async fn fail_trash_job(
    slot: &'static LazyLock<Arc<RwLock<TrashLifecycleJobStatus>>>,
    error: AppError,
) {
    let fragment = {
        let status = slot.read().await;
        status.fragment.clone()
    };
    mutate_trash_job_status(slot, |status| {
        status.is_running = false;
        status.phase = "Failed".to_string();
        status.failed_objects = status.failed_objects.max(
            status
                .total_objects
                .saturating_sub(status.processed_objects),
        );
        status.last_error = Some(error.to_string());
    })
    .await;
    clear_persistent_trash_job_record(&fragment).await;
}

pub(crate) fn persistent_trash_job_paths(fragment: &str) -> Vec<std::path::PathBuf> {
    ObjectPathManager::new()
        .get_all_index_roots()
        .into_iter()
        .map(|root| {
            root.join(fragment)
                .join(".trash-jobs")
                .join(PERSISTENT_TRASH_JOB_FILE)
        })
        .collect()
}

async fn persist_persistent_trash_job_record(
    operation: &str,
    fragment: &str,
) -> Result<(), AppError> {
    let record = PersistentTrashJobRecord {
        operation: operation.to_string(),
        fragment: fragment.to_string(),
        started_at: Utc::now().timestamp(),
    };
    let bytes = serde_json::to_vec_pretty(&record)
        .map_err(|e| AppError::InternalServerError(format!("serialize trash job record: {}", e)))?;
    for path in persistent_trash_job_paths(fragment) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                AppError::Io(
                    e,
                    format!("create trash job directory {}", parent.display()),
                )
            })?;
        }
        let tmp_path = path.with_extension("json.tmp");
        fs::write(&tmp_path, &bytes).await.map_err(|e| {
            AppError::Io(e, format!("write trash job record {}", tmp_path.display()))
        })?;
        fs::rename(&tmp_path, &path).await.map_err(|e| {
            AppError::Io(
                e,
                format!(
                    "rename trash job record {} -> {}",
                    tmp_path.display(),
                    path.display()
                ),
            )
        })?;
    }
    Ok(())
}

async fn clear_persistent_trash_job_record(fragment: &str) {
    for path in persistent_trash_job_paths(fragment) {
        let _ = fs::remove_file(path).await;
    }
}

async fn discover_persistent_trash_jobs() -> Result<Vec<PersistentTrashJobRecord>, AppError> {
    let mut jobs = Vec::new();
    let mut seen = HashSet::<(String, String)>::new();

    for root in ObjectPathManager::new().get_all_index_roots() {
        let mut fragments = match fs::read_dir(&root).await {
            Ok(read_dir) => read_dir,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(AppError::Io(
                    err,
                    format!("read index root for trash jobs {}", root.display()),
                ));
            }
        };

        while let Some(entry) = fragments.next_entry().await.map_err(|e| {
            AppError::Io(
                e,
                format!("iterate index root for trash jobs {}", root.display()),
            )
        })? {
            let file_type = entry.file_type().await.map_err(|e| {
                AppError::Io(
                    e,
                    format!("read index root entry type {}", entry.path().display()),
                )
            })?;
            if !file_type.is_dir() {
                continue;
            }

            let job_path = entry
                .path()
                .join(".trash-jobs")
                .join(PERSISTENT_TRASH_JOB_FILE);
            let raw = match fs::read(&job_path).await {
                Ok(raw) => raw,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(AppError::Io(
                        err,
                        format!("read persistent trash job {}", job_path.display()),
                    ));
                }
            };

            let record: PersistentTrashJobRecord = serde_json::from_slice(&raw).map_err(|e| {
                AppError::InternalServerError(format!(
                    "parse persistent trash job {}: {}",
                    job_path.display(),
                    e
                ))
            })?;
            let dedupe_key = (record.fragment.clone(), record.operation.clone());
            if seen.insert(dedupe_key) {
                jobs.push(record);
            }
        }
    }

    jobs.sort_by(|a, b| {
        a.started_at
            .cmp(&b.started_at)
            .then_with(|| a.fragment.cmp(&b.fragment))
            .then_with(|| a.operation.cmp(&b.operation))
    });
    Ok(jobs)
}

fn slot_for_trash_operation(
    operation: &str,
) -> Option<&'static LazyLock<Arc<RwLock<TrashLifecycleJobStatus>>>> {
    match operation {
        "recover_all_trash" => Some(&RECOVER_ALL_TRASH_STATUS),
        "empty_trash" => Some(&EMPTY_TRASH_STATUS),
        _ => None,
    }
}

pub async fn resume_persistent_trash_jobs(state: Arc<AppState>) -> Result<usize, AppError> {
    let jobs = discover_persistent_trash_jobs().await?;
    let mut resumed = 0usize;

    for record in jobs {
        let Some(slot) = slot_for_trash_operation(&record.operation) else {
            warn!(
                "Removing unknown persistent trash job operation={} fragment={}",
                record.operation, record.fragment
            );
            clear_persistent_trash_job_record(&record.fragment).await;
            continue;
        };

        if TRASH_LIFECYCLE_RUNNING.swap(true, Ordering::SeqCst) {
            warn!(
                "Skipping resume of persistent trash job operation={} fragment={} because another trash job is active",
                record.operation, record.fragment
            );
            break;
        }

        resumed = resumed.saturating_add(1);
        mutate_trash_job_status(slot, |status| {
            *status = TrashLifecycleJobStatus {
                operation: record.operation.clone(),
                is_running: true,
                phase: "Resuming after restart".to_string(),
                fragment: record.fragment.clone(),
                start_timestamp: record.started_at,
                ..Default::default()
            };
        })
        .await;

        let result = match record.operation.as_str() {
            "recover_all_trash" => {
                run_recover_all_trash_job(state.clone(), record.fragment.clone()).await
            }
            "empty_trash" => run_empty_trash_job(state.clone(), record.fragment.clone()).await,
            _ => Ok(()),
        };

        if let Err(err) = result {
            warn!(
                "Resumed trash job failed operation={} fragment={}: {}",
                record.operation, record.fragment, err
            );
            fail_trash_job(slot, err).await;
        }
        TRASH_LIFECYCLE_RUNNING.store(false, Ordering::SeqCst);
    }

    Ok(resumed)
}

fn get_trash_root(disk_root: &std::path::Path, fragment_hash: &str) -> std::path::PathBuf {
    disk_root.join(fragment_hash).join(".trash")
}

async fn retire_trash_roots(
    trash_roots: &[std::path::PathBuf],
) -> Vec<(String, Option<std::path::PathBuf>, Option<String>)> {
    let suffix = format!("{}-{}", Utc::now().timestamp_millis(), std::process::id());
    stream::iter(trash_roots.iter().cloned().map(|trash_root| {
        let suffix = suffix.clone();
        async move {
            let root_label = trash_root.display().to_string();
            let Some(fragment_root) = trash_root.parent() else {
                return (
                    root_label,
                    None,
                    Some("trash root missing parent".to_string()),
                );
            };
            let retired_root = fragment_root.join(
                trash_root
                    .file_name()
                    .map(|name| format!("{}.retired.{}", name.to_string_lossy(), suffix))
                    .unwrap_or_else(|| format!(".trash.retired.{}", suffix)),
            );
            if !fs::try_exists(&trash_root).await.unwrap_or(false) {
                return (root_label, None, None);
            }
            if fs::try_exists(&retired_root).await.unwrap_or(false) {
                let _ = fs::remove_dir_all(&retired_root).await;
            }
            match fs::rename(&trash_root, &retired_root).await {
                Ok(_) => (root_label, Some(retired_root), None),
                Err(err) => (root_label, None, Some(err.to_string())),
            }
        }
    }))
    .buffer_unordered(4)
    .collect::<Vec<_>>()
    .await
}

fn spawn_background_trash_purge(retired_roots: Vec<std::path::PathBuf>) {
    if retired_roots.is_empty() {
        return;
    }
    tokio::spawn(async move {
        stream::iter(retired_roots.into_iter().map(|retired_root| async move {
            if let Err(err) = fs::remove_dir_all(&retired_root).await {
                warn!(
                    "Failed to purge retired trash root {}: {}",
                    retired_root.display(),
                    err
                );
            }
        }))
        .buffer_unordered(4)
        .collect::<Vec<_>>()
        .await;
    });
}

async fn purge_delete_markers_for_key(fragment: &str, object_key: &str) {
    let physical_key = resolve_namespace_object_id_or_self(fragment, object_key)
        .await
        .unwrap_or_else(|_| object_key.to_string());
    if let Ok(metas) = index::list_live_object_versions_metadata(fragment, &physical_key).await {
        for meta in metas {
            if !meta.is_delete_marker {
                continue;
            }
            let _ = index::delete_object_version(fragment, &physical_key, &meta.version_id).await;
        }
    }
}

async fn resolve_namespace_object_id_or_self(
    fragment: &str,
    object_key: &str,
) -> Result<String, AppError> {
    Ok(global_namespace_runtime()
        .resolve_any_object_id(fragment, object_key)
        .await?
        .unwrap_or_else(|| object_key.to_string()))
}

#[derive(Debug, Deserialize)]
pub struct BulkObjectKeysRequest {
    pub object_keys: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct BulkObjectFailure {
    pub object_key: String,
    pub error: String,
}

#[derive(Debug, Serialize)]
pub struct BulkObjectOperationResponse {
    pub requested: usize,
    pub unique: usize,
    pub succeeded: usize,
    pub not_found: usize,
    pub failed: Vec<BulkObjectFailure>,
}

async fn delete_object_core(
    fragment: &str,
    object_key: &str,
) -> Result<NamespaceMutationSummary, AppError> {
    let _object_lock = index::acquire_object_operation_lock(fragment, object_key).await?;
    global_namespace_runtime().trash(fragment, object_key).await
}

async fn recover_object_core(
    fragment: &str,
    object_key: &str,
) -> Result<NamespaceMutationSummary, AppError> {
    let _object_lock = index::acquire_object_operation_lock(fragment, object_key).await?;
    global_namespace_runtime()
        .recover(fragment, object_key)
        .await
}

async fn delete_object_permanently_core(
    fragment: &str,
    object_key: &str,
) -> Result<NamespaceMutationSummary, AppError> {
    let _object_lock = index::acquire_object_operation_lock(fragment, object_key).await?;
    match global_namespace_runtime()
        .purge_trash_path(fragment, object_key)
        .await
    {
        Ok(summary) => Ok(summary),
        Err(AppError::NotFound) => {
            global_namespace_runtime()
                .trash(fragment, object_key)
                .await?;
            Ok(global_namespace_runtime()
                .purge_trash_path(fragment, object_key)
                .await?)
        }
        Err(err) => Err(err),
    }
}

pub async fn delete_object_version(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumPath((fragment, version_id, object_key)): AxumPath<(String, String, String)>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let physical_key = resolve_namespace_object_id_or_self(&fragment, &object_key).await?;
    let _object_lock = index::acquire_object_operation_lock(&fragment, &physical_key).await?;
    match index::delete_trashed_object_version(&fragment, &physical_key, &version_id).await {
        Ok(()) => {}
        Err(AppError::NotFound) => {
            index::delete_object_version(&fragment, &physical_key, &version_id).await?;
        }
        Err(err) => return Err(err),
    }

    let _ = index::remove_session(&fragment, &version_id, "permanent-delete-version").await;

    info!(
        "Permanently deleted version: {}/{} (v: {})",
        fragment, object_key, version_id
    );

    let event = StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::version_removed(
        fragment.clone(),
        object_key,
        version_id,
    ));
    state.emit(&fragment, event);

    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_object(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumPath((fragment, object_key)): AxumPath<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let summary = delete_object_core(&fragment, &object_key).await?;
    let moved_count = summary.affected_objects.max(summary.affected_paths);

    info!(
        "Soft deleted {} versions for object {}/{}",
        moved_count, fragment, object_key
    );

    state.emit(
        &fragment,
        StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::from_summary(
            &fragment, summary,
        )),
    );

    Ok(StatusCode::OK)
}

pub async fn delete_objects_bulk(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumJson(payload): AxumJson<BulkObjectKeysRequest>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }

    let fragment = claims.fragment.clone();
    let requested = payload.object_keys.len();
    let unique_keys: Vec<String> = payload
        .object_keys
        .into_iter()
        .filter(|key| !key.is_empty())
        .collect::<BTreeSet<String>>()
        .into_iter()
        .collect();

    if unique_keys.is_empty() {
        return Err(AppError::BadRequest("object_keys must not be empty".into()));
    }

    let mut failed = Vec::new();
    let mut succeeded = 0usize;
    let mut not_found = 0usize;
    let mut summaries = Vec::new();
    for object_key in &unique_keys {
        match delete_object_core(&fragment, object_key).await {
            Ok(summary) => {
                succeeded = succeeded.saturating_add(1);
                summaries.push(summary);
            }
            Err(AppError::NotFound) => not_found = not_found.saturating_add(1),
            Err(err) => failed.push(BulkObjectFailure {
                object_key: object_key.clone(),
                error: err.to_string(),
            }),
        }
    }

    if succeeded > 0 {
        info!(
            "Soft deleted {} namespace object(s) for fragment {} via bulk manifest",
            succeeded, fragment
        );
    }

    for summary in summaries {
        state.emit(
            &fragment,
            StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::from_summary(
                &fragment, summary,
            )),
        );
    }

    Ok((
        StatusCode::OK,
        AxumJson(BulkObjectOperationResponse {
            requested,
            unique: unique_keys.len(),
            succeeded,
            not_found,
            failed,
        }),
    ))
}

pub async fn empty_trash(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumPath(fragment): AxumPath<String>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.delete {
        return Err(AppError::Forbidden("Delete permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let started = begin_trash_job(
        &EMPTY_TRASH_STATUS,
        &RECOVER_ALL_TRASH_STATUS,
        "empty_trash",
        &fragment,
    )
    .await?;

    tokio::spawn(async move {
        let result = run_empty_trash_job(state, fragment.clone()).await;
        if let Err(err) = result {
            warn!("Empty trash job failed for {}: {}", fragment, err);
            fail_trash_job(&EMPTY_TRASH_STATUS, err).await;
        }
        TRASH_LIFECYCLE_RUNNING.store(false, Ordering::SeqCst);
    });

    Ok((StatusCode::ACCEPTED, AxumJson(started)))
}

async fn run_empty_trash_job(state: Arc<AppState>, fragment: String) -> Result<(), AppError> {
    let _maintenance_guard = acquire_maintenance_mode(MaintenanceAccessMode::ReadOnly);
    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();

    mutate_trash_job_status(&EMPTY_TRASH_STATUS, |status| {
        status.phase = "Loading trash control plane".to_string();
    })
    .await;

    let total_objects = global_namespace_runtime()
        .list_objects_page(
            &fragment,
            &HashMap::from([("limit".to_string(), "1000".to_string())]),
            true,
        )
        .await?
        .objects
        .len() as u64;
    let mut trash_targets = BTreeSet::new();
    for root in &index_roots {
        trash_targets.insert(get_trash_root(root, &fragment));
    }

    mutate_trash_job_status(&EMPTY_TRASH_STATUS, |status| {
        status.phase = "Clearing trash metadata".to_string();
        status.total_objects = total_objects;
        status.total_trash_roots = trash_targets.len() as u64;
        status.current_object = None;
    })
    .await;

    let summary = match global_namespace_runtime().empty_trash(&fragment).await {
        Ok(summary) => summary,
        Err(AppError::NotFound) => {
            mutate_trash_job_status(&EMPTY_TRASH_STATUS, |status| {
                status.is_running = false;
                status.phase = "Completed".to_string();
                status.current_object = None;
            })
            .await;
            clear_persistent_trash_job_record(&fragment).await;
            return Ok(());
        }
        Err(err) => return Err(err),
    };
    mutate_trash_job_status(&EMPTY_TRASH_STATUS, |status| {
        status.processed_objects = summary.affected_paths as u64;
        status.successful_objects = summary.affected_paths as u64;
        status.phase = "Trash metadata cleared; blob bytes will be reclaimed by GC".to_string();
    })
    .await;

    mutate_trash_job_status(&EMPTY_TRASH_STATUS, |status| {
        status.phase = "Retiring trash roots".to_string();
    })
    .await;

    let root_results = retire_trash_roots(&trash_targets.into_iter().collect::<Vec<_>>()).await;
    let retired_roots = root_results
        .iter()
        .filter_map(|(_, retired_root, _)| retired_root.clone())
        .collect::<Vec<_>>();

    for (root_label, _retired_root, maybe_error) in root_results {
        mutate_trash_job_status(&EMPTY_TRASH_STATUS, |status| {
            status.processed_trash_roots = status.processed_trash_roots.saturating_add(1);
            status.current_object = Some(root_label.clone());
            if let Some(err) = &maybe_error {
                status.last_error = Some(err.clone());
            }
        })
        .await;
    }

    spawn_background_trash_purge(retired_roots);

    mutate_trash_job_status(&EMPTY_TRASH_STATUS, |status| {
        status.is_running = false;
        status.phase = "Completed".to_string();
        status.current_object = None;
    })
    .await;
    clear_persistent_trash_job_record(&fragment).await;

    state.emit(
        &fragment,
        StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::from_summary(
            &fragment, summary,
        )),
    );
    Ok(())
}

pub async fn delete_object_permanently(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumPath((fragment, object_key)): AxumPath<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.delete {
        return Err(AppError::Forbidden("Delete permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let summary = delete_object_permanently_core(&fragment, &object_key).await?;

    info!(
        "Permanently deleted {} versions for {}/{}",
        summary.affected_objects.max(summary.affected_paths),
        fragment,
        object_key
    );

    state.emit(
        &fragment,
        StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::from_summary(
            &fragment, summary,
        )),
    );

    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_objects_bulk_permanently(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumJson(payload): AxumJson<BulkObjectKeysRequest>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.delete {
        return Err(AppError::Forbidden("Delete permission required".into()));
    }

    let fragment = claims.fragment.clone();
    let requested = payload.object_keys.len();
    let unique_keys: Vec<String> = payload
        .object_keys
        .into_iter()
        .filter(|key| !key.is_empty())
        .collect::<BTreeSet<String>>()
        .into_iter()
        .collect();

    if unique_keys.is_empty() {
        return Err(AppError::BadRequest("object_keys must not be empty".into()));
    }

    let mut succeeded = 0usize;
    let mut not_found = 0usize;
    let mut failed = Vec::new();

    let mut summaries = Vec::new();
    for object_key in &unique_keys {
        match delete_object_permanently_core(&fragment, object_key).await {
            Ok(summary) => {
                succeeded = succeeded.saturating_add(1);
                summaries.push(summary);
            }
            Err(AppError::NotFound) => not_found = not_found.saturating_add(1),
            Err(err) => failed.push(BulkObjectFailure {
                object_key: object_key.clone(),
                error: err.to_string(),
            }),
        }
    }

    for summary in summaries {
        state.emit(
            &fragment,
            StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::from_summary(
                &fragment, summary,
            )),
        );
    }

    Ok((
        StatusCode::OK,
        AxumJson(BulkObjectOperationResponse {
            requested,
            unique: unique_keys.len(),
            succeeded,
            not_found,
            failed,
        }),
    ))
}

pub async fn recover_object_version(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumPath((fragment, version_id, object_key)): AxumPath<(String, String, String)>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden(
            "Write permission required to recover".into(),
        ));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    purge_delete_markers_for_key(&fragment, &object_key).await;
    let physical_key = resolve_namespace_object_id_or_self(&fragment, &object_key).await?;
    let _object_lock = index::acquire_object_operation_lock(&fragment, &physical_key).await?;
    let _restored_meta = index::restore_object_version_from_trash_with_metadata(
        &fragment,
        &physical_key,
        &version_id,
    )
    .await?;

    info!(
        "Recovered object version: {}/{} (v: {})",
        fragment, object_key, version_id
    );

    if let Ok(mut meta) = index::read_object_version(&fragment, &physical_key, &version_id).await {
        meta.object_key = object_key.clone();
        state.emit(
            &fragment,
            StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::file_upsert(
                &fragment,
                &object_key,
                &physical_key,
                meta.total_size_bytes,
                &meta.version_id,
                &meta.content_type,
                meta.created_at,
            )),
        );
    }

    Ok(StatusCode::OK)
}

pub async fn recover_object(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumPath((fragment, object_key)): AxumPath<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden(
            "Write permission required to recover".into(),
        ));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let summary = recover_object_core(&fragment, &object_key).await?;
    let restored_count = summary.affected_objects.max(summary.affected_paths);

    info!(
        "Recovered {} versions for object {}/{}",
        restored_count, fragment, object_key
    );

    state.emit(
        &fragment,
        StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::from_summary(
            &fragment, summary,
        )),
    );

    Ok(StatusCode::OK)
}

pub async fn recover_objects_bulk(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumJson(payload): AxumJson<BulkObjectKeysRequest>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden(
            "Write permission required to recover".into(),
        ));
    }

    let fragment = claims.fragment.clone();
    let requested = payload.object_keys.len();
    let unique_keys: Vec<String> = payload
        .object_keys
        .into_iter()
        .filter(|key| !key.is_empty())
        .collect::<BTreeSet<String>>()
        .into_iter()
        .collect();

    if unique_keys.is_empty() {
        return Err(AppError::BadRequest("object_keys must not be empty".into()));
    }

    let mut failed = Vec::new();
    let mut succeeded = 0usize;
    let mut not_found = 0usize;
    let mut summaries = Vec::new();
    for (object_key, result) in global_namespace_runtime()
        .recover_paths_bulk(&fragment, unique_keys.clone())
        .await?
    {
        match result {
            Ok(summary) => {
                succeeded = succeeded.saturating_add(1);
                summaries.push(summary);
            }
            Err(AppError::NotFound) => not_found = not_found.saturating_add(1),
            Err(err) => failed.push(BulkObjectFailure {
                object_key,
                error: err.to_string(),
            }),
        }
    }

    if succeeded > 0 {
        info!(
            "Recovered {} namespace object(s) for fragment {} via bulk manifest",
            succeeded, fragment
        );
    }

    for summary in summaries {
        state.emit(
            &fragment,
            StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::from_summary(
                &fragment, summary,
            )),
        );
    }

    Ok((
        StatusCode::OK,
        AxumJson(BulkObjectOperationResponse {
            requested,
            unique: unique_keys.len(),
            succeeded,
            not_found,
            failed,
        }),
    ))
}

pub async fn recover_all_trash(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumPath(fragment): AxumPath<String>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden(
            "Write permission required to recover all".into(),
        ));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }

    let started = begin_trash_job(
        &RECOVER_ALL_TRASH_STATUS,
        &EMPTY_TRASH_STATUS,
        "recover_all_trash",
        &fragment,
    )
    .await?;

    tokio::spawn(async move {
        let result = run_recover_all_trash_job(state, fragment.clone()).await;
        if let Err(err) = result {
            warn!("Recover all trash job failed for {}: {}", fragment, err);
            fail_trash_job(&RECOVER_ALL_TRASH_STATUS, err).await;
        }
        TRASH_LIFECYCLE_RUNNING.store(false, Ordering::SeqCst);
    });

    Ok((StatusCode::ACCEPTED, AxumJson(started)))
}

async fn run_recover_all_trash_job(state: Arc<AppState>, fragment: String) -> Result<(), AppError> {
    let _maintenance_guard = acquire_maintenance_mode(MaintenanceAccessMode::ReadOnly);
    mutate_trash_job_status(&RECOVER_ALL_TRASH_STATUS, |status| {
        status.phase = "Loading trash control plane".to_string();
    })
    .await;

    let summary = match global_namespace_runtime().recover_all(&fragment).await {
        Ok(summary) => summary,
        Err(AppError::NotFound) => {
            mutate_trash_job_status(&RECOVER_ALL_TRASH_STATUS, |status| {
                status.is_running = false;
                status.phase = "Completed".to_string();
            })
            .await;
            clear_persistent_trash_job_record(&fragment).await;
            return Ok(());
        }
        Err(err) => return Err(err),
    };
    let total_objects = summary.affected_paths as u64;

    if total_objects == 0 {
        mutate_trash_job_status(&RECOVER_ALL_TRASH_STATUS, |status| {
            status.is_running = false;
            status.phase = "Completed".to_string();
        })
        .await;
        clear_persistent_trash_job_record(&fragment).await;
        return Ok(());
    }

    mutate_trash_job_status(&RECOVER_ALL_TRASH_STATUS, |status| {
        status.phase = "Recovering trashed objects".to_string();
        status.total_objects = total_objects;
    })
    .await;

    mutate_trash_job_status(&RECOVER_ALL_TRASH_STATUS, |status| {
        status.processed_objects = total_objects;
        status.successful_objects = total_objects;
        status.current_object = None;
    })
    .await;

    info!(
        "Recovered {} trashed item(s) for fragment {}",
        total_objects, fragment
    );

    mutate_trash_job_status(&RECOVER_ALL_TRASH_STATUS, |status| {
        status.is_running = false;
        status.phase = "Completed".to_string();
        status.current_object = None;
    })
    .await;
    clear_persistent_trash_job_record(&fragment).await;

    state.emit(
        &fragment,
        StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::from_summary(
            &fragment, summary,
        )),
    );

    Ok(())
}
