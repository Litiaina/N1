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
use walkdir::WalkDir;

use crate::core::disk_manager::ObjectPathManager;
use crate::core::index;
use crate::core::operation_gate::{MaintenanceAccessMode, acquire_maintenance_mode};
use crate::enums::app_error_enum::AppError;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::websocket::ws_structs::StorageEventPayload;

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
                .join("indexdb")
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

            let job_path = entry.path().join("indexdb").join(PERSISTENT_TRASH_JOB_FILE);
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

fn is_cross_device_rename(err: &std::io::Error) -> bool {
    #[cfg(unix)]
    {
        err.raw_os_error() == Some(libc::EXDEV)
    }
    #[cfg(not(unix))]
    {
        let _ = err;
        false
    }
}

async fn move_dir_or_copy(src: &std::path::Path, dst: &std::path::Path) -> Result<(), AppError> {
    if let Some(parent) = dst.parent() {
        let _ = fs::create_dir_all(parent).await;
    }

    match fs::rename(src, dst).await {
        Ok(_) => Ok(()),
        Err(e) => {
            if !is_cross_device_rename(&e) {
                return Err(AppError::Io(
                    e,
                    format!("rename dir {} -> {}", src.display(), dst.display()),
                ));
            }

            let src = src.to_path_buf();
            let dst = dst.to_path_buf();

            tokio::task::spawn_blocking(move || -> Result<(), AppError> {
                use std::fs as stdfs;

                stdfs::create_dir_all(&dst).map_err(|e| {
                    AppError::Io(e, format!("mkdir {} (copydir fallback)", dst.display()))
                })?;

                for entry in WalkDir::new(&src).into_iter().filter_map(|e| e.ok()) {
                    let p = entry.path();
                    let rel = match p.strip_prefix(&src) {
                        Ok(r) => r,
                        Err(_) => continue,
                    };
                    if rel.as_os_str().is_empty() {
                        continue;
                    }
                    let target = dst.join(rel);

                    if entry.file_type().is_dir() {
                        let _ = stdfs::create_dir_all(&target);
                        continue;
                    }

                    if entry.file_type().is_file() {
                        if let Some(parent) = target.parent() {
                            let _ = stdfs::create_dir_all(parent);
                        }
                        stdfs::copy(p, &target).map_err(|e| {
                            AppError::Io(e, format!("copy {} -> {}", p.display(), target.display()))
                        })?;
                    }
                }

                stdfs::remove_dir_all(&src).map_err(|e| {
                    AppError::Io(
                        e,
                        format!("remove_dir_all {} (after copydir)", src.display()),
                    )
                })?;

                Ok(())
            })
            .await
            .map_err(|e| AppError::InternalServerError(format!("copydir task: {}", e)))??;

            Ok(())
        }
    }
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
    if let Ok(metas) = index::list_live_object_versions_metadata(fragment, object_key).await {
        for meta in metas {
            if !meta.is_delete_marker {
                continue;
            }
            let _ = index::delete_object_version(fragment, object_key, &meta.version_id).await;
        }
    }
}

async fn list_object_versions_any(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<String>, AppError> {
    index::list_live_object_version_ids(fragment, object_key).await
}

async fn list_trashed_object_versions_any(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<String>, AppError> {
    index::list_trashed_object_version_ids(fragment, object_key).await
}

async fn move_blob_versions_between_states(
    path_manager: &ObjectPathManager,
    blob_roots: &[std::path::PathBuf],
    fragment: &str,
    version_ids: &[String],
    from_trash: bool,
) {
    let mut moves = Vec::with_capacity(version_ids.len().saturating_mul(blob_roots.len()));
    for version_id in version_ids {
        for root in blob_roots {
            let src = if from_trash {
                get_trash_root(root, fragment).join("blob").join(version_id)
            } else {
                path_manager.get_shard_dir_on_disk(root, fragment, version_id)
            };
            let dst = if from_trash {
                path_manager.get_shard_dir_on_disk(root, fragment, version_id)
            } else {
                get_trash_root(root, fragment).join("blob").join(version_id)
            };
            moves.push((src, dst));
        }
    }

    stream::iter(moves.into_iter().map(|(src, dst)| async move {
        if !fs::try_exists(&src).await.unwrap_or(false) {
            return;
        }
        let _ = move_dir_or_copy(&src, &dst).await;
    }))
    .buffer_unordered(bulk_partition_concurrency_limit())
    .collect::<Vec<_>>()
    .await;
}

async fn remove_blob_versions(
    path_manager: &ObjectPathManager,
    blob_roots: &[std::path::PathBuf],
    fragment: &str,
    version_ids: &[String],
) {
    let mut removals = Vec::with_capacity(version_ids.len().saturating_mul(blob_roots.len()));
    for version_id in version_ids {
        for disk in blob_roots {
            removals.push((
                path_manager.get_shard_dir_on_disk(disk, fragment, version_id),
                get_trash_root(disk, fragment).join("blob").join(version_id),
            ));
        }
    }

    stream::iter(
        removals
            .into_iter()
            .map(|(live_shard, trash_shard)| async move {
                let _ = fs::remove_dir_all(&live_shard).await;
                let _ = fs::remove_dir_all(&trash_shard).await;
            }),
    )
    .buffer_unordered(bulk_partition_concurrency_limit())
    .collect::<Vec<_>>()
    .await;
}

async fn acquire_bulk_object_operation_locks(
    fragment: &str,
    object_keys: &[String],
) -> Result<Vec<index::ObjectOperationGuard>, AppError> {
    let mut guards = Vec::with_capacity(object_keys.len());
    for object_key in object_keys {
        guards.push(index::acquire_object_operation_lock(fragment, object_key).await?);
    }
    Ok(guards)
}

fn flatten_version_ids(version_ids_by_key: &HashMap<String, Vec<String>>) -> Vec<String> {
    version_ids_by_key
        .values()
        .flat_map(|version_ids| version_ids.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn bulk_partition_concurrency_limit() -> usize {
    std::thread::available_parallelism()
        .map(|n| (n.get() * 4).clamp(8, 64))
        .unwrap_or(16)
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

async fn delete_object_core(fragment: &str, object_key: &str) -> Result<usize, AppError> {
    let _object_lock = index::acquire_object_operation_lock(fragment, object_key).await?;
    let path_manager = ObjectPathManager::new();
    let versions_found = list_object_versions_any(fragment, object_key).await?;

    if versions_found.is_empty() {
        return Err(AppError::NotFound);
    }

    let blob_roots = path_manager.get_blob_roots();
    let moved_versions = match index::move_object_to_trash_fast(fragment, object_key).await {
        Ok(Some(version_ids)) => version_ids,
        Ok(None) => {
            let versions_found = list_object_versions_any(fragment, object_key).await?;
            if versions_found.is_empty() {
                return Err(AppError::NotFound);
            }
            let mut moved = Vec::new();
            for version_id in &versions_found {
                let _moved_meta = match index::move_object_version_to_trash_with_metadata(
                    fragment, object_key, version_id,
                )
                .await
                {
                    Ok(meta) => meta,
                    Err(e) => {
                        warn!(
                            "Partition trash move failed for {}/{}#{}: {}",
                            fragment, object_key, version_id, e
                        );
                        continue;
                    }
                };
                moved.push(version_id.clone());
            }
            moved
        }
        Err(e) => return Err(e),
    };

    move_blob_versions_between_states(&path_manager, &blob_roots, fragment, &moved_versions, false)
        .await;
    Ok(moved_versions.len())
}

async fn recover_object_core(fragment: &str, object_key: &str) -> Result<usize, AppError> {
    let _object_lock = index::acquire_object_operation_lock(fragment, object_key).await?;
    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    purge_delete_markers_for_key(fragment, object_key).await;

    let mut found_versions = list_trashed_object_versions_any(fragment, object_key).await?;
    if found_versions.is_empty() {
        return Err(AppError::NotFound);
    }
    found_versions.sort();
    found_versions.dedup();

    move_blob_versions_between_states(&path_manager, &blob_roots, fragment, &found_versions, true)
        .await;

    let restored_versions = match index::restore_object_from_trash_fast(fragment, object_key).await
    {
        Ok(Some(version_ids)) => version_ids,
        Ok(None) => {
            let mut restored = Vec::new();
            for version_id in &found_versions {
                if let Err(e) = index::restore_object_version_from_trash_with_metadata(
                    fragment, object_key, version_id,
                )
                .await
                {
                    warn!(
                        "Partition restore failed for {}/{}#{}: {}",
                        fragment, object_key, version_id, e
                    );
                    continue;
                }
                restored.push(version_id.clone());
            }
            restored
        }
        Err(e) => return Err(e),
    };
    Ok(restored_versions.len())
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

    let _object_lock = index::acquire_object_operation_lock(&fragment, &object_key).await?;
    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    if let Ok(_meta) = index::read_trashed_object_version(&fragment, &object_key, &version_id).await
    {
        for root in &blob_roots {
            let trash_blob_dir = get_trash_root(root, &fragment)
                .join("blob")
                .join(&version_id);
            let _ = fs::remove_dir_all(&trash_blob_dir).await;
        }
        let _ = index::delete_trashed_object_version(&fragment, &object_key, &version_id).await;
        let _ = index::remove_session(&fragment, &version_id, "permanent-delete-version").await;
        state.emit(
            &fragment,
            StorageEventPayload::FileDeleted {
                fragment: fragment.clone(),
                object_key,
                version_id: Some(version_id),
            },
        );
        return Ok(StatusCode::NO_CONTENT);
    }

    let _moved_meta =
        index::move_object_version_to_trash_with_metadata(&fragment, &object_key, &version_id)
            .await?;

    for root in &blob_roots {
        let src_blob_dir = path_manager.get_shard_dir_on_disk(root, &fragment, &version_id);
        if !fs::try_exists(&src_blob_dir).await.unwrap_or(false) {
            continue;
        }

        let trash_root = get_trash_root(root, &fragment);
        let dest_blob_dir = trash_root.join("blob").join(&version_id);

        match move_dir_or_copy(&src_blob_dir, &dest_blob_dir).await {
            Ok(_) => {}
            Err(e) => warn!("Failed to move blob to trash: {}", e),
        }
    }

    info!(
        "Soft deleted (moved to trash) version: {}/{} (v: {})",
        fragment, object_key, version_id
    );

    state.emit(
        &fragment,
        StorageEventPayload::FileDeleted {
            fragment: fragment.clone(),
            object_key,
            version_id: Some(version_id),
        },
    );

    Ok(StatusCode::OK)
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

    let moved_count = delete_object_core(&fragment, &object_key).await?;

    info!(
        "Soft deleted {} versions for object {}/{}",
        moved_count, fragment, object_key
    );

    state.emit(
        &fragment,
        StorageEventPayload::FileDeleted {
            fragment: fragment.clone(),
            object_key,
            version_id: None,
        },
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

    let _guards = acquire_bulk_object_operation_locks(&fragment, &unique_keys).await?;
    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    let mut failed = Vec::new();
    let moved_by_key = match index::move_objects_to_trash_batch(&fragment, &unique_keys).await {
        Ok(changed) => changed,
        Err(err) => {
            for object_key in &unique_keys {
                failed.push(BulkObjectFailure {
                    object_key: object_key.clone(),
                    error: err.to_string(),
                });
            }
            HashMap::new()
        }
    };

    let version_ids = flatten_version_ids(&moved_by_key);
    move_blob_versions_between_states(&path_manager, &blob_roots, &fragment, &version_ids, false)
        .await;

    let succeeded = moved_by_key.len();
    let not_found = unique_keys
        .len()
        .saturating_sub(succeeded.saturating_add(failed.len()));

    if succeeded > 0 {
        info!(
            "Soft deleted {} object(s) and {} version(s) for fragment {} via bulk manifest",
            succeeded,
            version_ids.len(),
            fragment
        );
    }

    if succeeded > 0 {
        state.emit(&fragment, StorageEventPayload::StateInvalidated);
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
    let blob_roots = path_manager.get_blob_roots();

    mutate_trash_job_status(&EMPTY_TRASH_STATUS, |status| {
        status.phase = "Loading trash control plane".to_string();
    })
    .await;

    let total_objects = index::list_fragment_object_keys_with_trash_versions(&fragment)
        .await
        .map(|keys| keys.len() as u64)
        .unwrap_or(0);

    let mut trash_targets = BTreeSet::new();
    for root in &index_roots {
        trash_targets.insert(get_trash_root(root, &fragment));
    }
    for root in &blob_roots {
        trash_targets.insert(get_trash_root(root, &fragment));
    }

    mutate_trash_job_status(&EMPTY_TRASH_STATUS, |status| {
        status.phase = "Clearing trash metadata".to_string();
        status.total_objects = total_objects;
        status.total_trash_roots = trash_targets.len() as u64;
        status.current_object = None;
    })
    .await;

    index::clear_fragment_trash_in_partitions(&fragment, None).await?;
    mutate_trash_job_status(&EMPTY_TRASH_STATUS, |status| {
        status.processed_objects = total_objects;
        status.successful_objects = total_objects;
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

    state.emit(&fragment, StorageEventPayload::StateInvalidated);
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

    let _object_lock = index::acquire_object_operation_lock(&fragment, &object_key).await?;
    let path_manager = ObjectPathManager::new();
    let versions_to_delete = match index::delete_object_fast(&fragment, &object_key).await? {
        Some(version_ids) => version_ids,
        None => return Err(AppError::NotFound),
    };
    let blob_roots = path_manager.get_blob_roots();
    remove_blob_versions(&path_manager, &blob_roots, &fragment, &versions_to_delete).await;
    for version_id in &versions_to_delete {
        let _ = index::remove_session(&fragment, version_id, "permanent-delete").await;
    }

    info!(
        "Permanently deleted {} versions for {}/{}",
        versions_to_delete.len(),
        fragment,
        object_key
    );

    state.emit(
        &fragment,
        StorageEventPayload::FileDeleted {
            fragment: fragment.clone(),
            object_key,
            version_id: None,
        },
    );

    Ok(StatusCode::NO_CONTENT)
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

    let _object_lock = index::acquire_object_operation_lock(&fragment, &object_key).await?;
    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    purge_delete_markers_for_key(&fragment, &object_key).await;
    let _trashed_meta =
        index::read_trashed_object_version(&fragment, &object_key, &version_id).await?;

    for root in &blob_roots {
        let trash_blob_dir = get_trash_root(root, &fragment)
            .join("blob")
            .join(&version_id);

        if !fs::try_exists(&trash_blob_dir).await.unwrap_or(false) {
            continue;
        }

        let live_blob_dir = path_manager.get_shard_dir_on_disk(root, &fragment, &version_id);
        if let Some(parent) = live_blob_dir.parent() {
            let _ = fs::create_dir_all(parent).await;
        }

        let _ = move_dir_or_copy(&trash_blob_dir, &live_blob_dir).await;
    }

    let _restored_meta =
        index::restore_object_version_from_trash_with_metadata(&fragment, &object_key, &version_id)
            .await?;

    info!(
        "Recovered object version: {}/{} (v: {})",
        fragment, object_key, version_id
    );

    if let Ok(meta) = index::read_object_version(&fragment, &object_key, &version_id).await {
        state.emit(&fragment, StorageEventPayload::FileUploaded(meta));
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

    let restored_count = recover_object_core(&fragment, &object_key).await?;

    info!(
        "Recovered {} versions for object {}/{}",
        restored_count, fragment, object_key
    );

    state.emit(&fragment, StorageEventPayload::StateInvalidated);

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

    let _guards = acquire_bulk_object_operation_locks(&fragment, &unique_keys).await?;
    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    let mut failed = Vec::new();
    for object_key in &unique_keys {
        purge_delete_markers_for_key(&fragment, object_key).await;
    }

    let recoverable_versions =
        match index::list_trashed_object_version_ids_batch(&fragment, &unique_keys).await {
            Ok(found) => found,
            Err(err) => {
                return Err(err);
            }
        };

    let version_ids = flatten_version_ids(&recoverable_versions);
    move_blob_versions_between_states(&path_manager, &blob_roots, &fragment, &version_ids, true)
        .await;

    let mut restored_by_key = HashMap::<String, Vec<String>>::new();
    match index::restore_objects_from_trash_manifest_batch(&fragment, &recoverable_versions).await {
        Ok(changed) => restored_by_key.extend(changed),
        Err(err) => {
            for object_key in recoverable_versions.keys() {
                failed.push(BulkObjectFailure {
                    object_key: object_key.clone(),
                    error: err.to_string(),
                });
            }
        }
    }

    let succeeded = restored_by_key.len();
    let not_found = unique_keys
        .len()
        .saturating_sub(succeeded.saturating_add(failed.len()));

    if succeeded > 0 {
        info!(
            "Recovered {} object(s) and {} version(s) for fragment {} via bulk manifest",
            succeeded,
            version_ids.len(),
            fragment
        );
    }

    if succeeded > 0 {
        state.emit(&fragment, StorageEventPayload::StateInvalidated);
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

    let trash_keys = index::list_fragment_object_keys_with_trash_versions(&fragment).await?;
    let recoverable_versions =
        index::list_trashed_object_version_ids_batch(&fragment, &trash_keys).await?;
    let total_objects = recoverable_versions.len() as u64;

    if recoverable_versions.is_empty() {
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

    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    for object_key in &trash_keys {
        purge_delete_markers_for_key(&fragment, object_key).await;
    }
    let version_ids = flatten_version_ids(&recoverable_versions);
    move_blob_versions_between_states(&path_manager, &blob_roots, &fragment, &version_ids, true)
        .await;

    let mut recovered_count = 0usize;
    match index::restore_objects_from_trash_manifest_batch(&fragment, &recoverable_versions).await {
        Ok(restored) => {
            recovered_count = version_ids.len();
            mutate_trash_job_status(&RECOVER_ALL_TRASH_STATUS, |status| {
                status.processed_objects = total_objects;
                status.successful_objects = restored.len() as u64;
                status.current_object = trash_keys.last().cloned();
            })
            .await;
        }
        Err(err) => {
            warn!(
                "Recover-all restore failed for fragment {} keys={} error={}",
                fragment,
                trash_keys.len(),
                err
            );
            mutate_trash_job_status(&RECOVER_ALL_TRASH_STATUS, |status| {
                status.processed_objects = total_objects;
                status.failed_objects = total_objects;
                status.current_object = trash_keys.last().cloned();
                status.last_error = Some(err.to_string());
            })
            .await;
        }
    }

    info!(
        "Recovered {} trashed item(s) for fragment {}",
        recovered_count, fragment
    );

    mutate_trash_job_status(&RECOVER_ALL_TRASH_STATUS, |status| {
        status.is_running = false;
        status.phase = "Completed".to_string();
        status.current_object = None;
    })
    .await;
    clear_persistent_trash_job_record(&fragment).await;

    state.emit(&fragment, StorageEventPayload::StateInvalidated);

    Ok(())
}
