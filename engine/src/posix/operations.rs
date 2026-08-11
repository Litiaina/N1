use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use chrono::Utc;
use once_cell::sync::Lazy;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use crate::core::disk_manager::FS_STORAGE_ROOT;
use crate::distribution::membership::active_cluster_runtime;
use crate::distribution::namespace::{
    NamespaceMutationControl, NamespaceMutationKind, NamespaceMutationPhase,
    PeerNamespaceApplyRequest, commit_namespace_summary_controlled,
};
use crate::enums::app_error_enum::AppError;
use crate::namespace::models::NamespaceMutationSummary;
use crate::posix::models::{
    NamespaceOperationAccepted, NamespaceOperationState, NamespaceOperationStatus, PosixNodeKind,
};
use crate::routes::storage_routes::AppState;
use crate::websocket::ws_structs::{NamespaceChangeEvent, StorageEventPayload};

const PROGRESS_PUBLISH_INTERVAL: Duration = Duration::from_millis(250);
const STATUS_ENDPOINT_PREFIX: &str = "/noa/v1/namespace/operations";

static OPERATIONS: Lazy<Mutex<HashMap<String, Arc<ActiveNamespaceOperation>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

struct ActiveNamespaceOperation {
    status: RwLock<NamespaceOperationStatus>,
    cancel: CancellationToken,
    revision: AtomicU64,
}

impl ActiveNamespaceOperation {
    fn new(status: NamespaceOperationStatus) -> Self {
        Self {
            status: RwLock::new(status),
            cancel: CancellationToken::new(),
            revision: AtomicU64::new(1),
        }
    }

    fn snapshot(&self) -> NamespaceOperationStatus {
        self.status
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn update(&self, update: impl FnOnce(&mut NamespaceOperationStatus)) {
        let mut status = self
            .status
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        update(&mut status);
        status.updated_at_unix_nanos = now_unix_nanos();
        self.revision.fetch_add(1, Ordering::Release);
    }
}

fn now_unix_nanos() -> i64 {
    Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX)
}

fn operation_kind_name(kind: &NamespaceMutationKind) -> Result<&'static str, AppError> {
    match kind {
        NamespaceMutationKind::Rename => Ok("rename"),
        NamespaceMutationKind::Trash => Ok("trash"),
        NamespaceMutationKind::Recover => Ok("recover"),
        NamespaceMutationKind::Mkdir | NamespaceMutationKind::UploadCommit => {
            Err(AppError::BadRequest(
                "only rename, trash, and recover can run as namespace jobs".into(),
            ))
        }
    }
}

fn parse_operation_kind(value: &str) -> Result<NamespaceMutationKind, AppError> {
    match value {
        "rename" => Ok(NamespaceMutationKind::Rename),
        "trash" => Ok(NamespaceMutationKind::Trash),
        "recover" => Ok(NamespaceMutationKind::Recover),
        _ => Err(AppError::InternalServerError(format!(
            "persisted namespace operation has unknown kind '{value}'"
        ))),
    }
}

fn operations_dir() -> Result<PathBuf, AppError> {
    FS_STORAGE_ROOT
        .get()
        .map(|root| PathBuf::from(root).join(".n2").join("namespace-operations"))
        .ok_or_else(|| AppError::InternalServerError("N2 runtime path is not initialized".into()))
}

fn operation_path(operation_id: &str) -> Result<PathBuf, AppError> {
    if Ulid::from_string(operation_id).is_err() {
        return Err(AppError::BadRequest(
            "namespace operation_id must be a ULID".into(),
        ));
    }
    Ok(operations_dir()?.join(format!("{operation_id}.json")))
}

async fn sync_directory(path: &Path) -> Result<(), AppError> {
    let directory = fs::File::open(path)
        .await
        .map_err(|error| AppError::Io(error, format!("open {}", path.display())))?;
    directory
        .sync_all()
        .await
        .map_err(|error| AppError::Io(error, format!("sync {}", path.display())))
}

async fn persist_status(status: &NamespaceOperationStatus) -> Result<(), AppError> {
    let directory = operations_dir()?;
    fs::create_dir_all(&directory)
        .await
        .map_err(|error| AppError::Io(error, format!("create {}", directory.display())))?;
    let path = operation_path(&status.operation_id)?;
    let temporary = directory.join(format!(".{}-{}.tmp", status.operation_id, Ulid::new()));
    let bytes = serde_json::to_vec_pretty(status).map_err(|error| {
        AppError::InternalServerError(format!("encode namespace operation: {error}"))
    })?;
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .await
        .map_err(|error| AppError::Io(error, format!("create {}", temporary.display())))?;
    if let Err(error) = file.write_all(&bytes).await {
        let _ = fs::remove_file(&temporary).await;
        return Err(AppError::Io(
            error,
            format!("write {}", temporary.display()),
        ));
    }
    if let Err(error) = file.sync_all().await {
        let _ = fs::remove_file(&temporary).await;
        return Err(AppError::Io(error, format!("sync {}", temporary.display())));
    }
    drop(file);
    if let Err(error) = fs::rename(&temporary, &path).await {
        let _ = fs::remove_file(&temporary).await;
        return Err(AppError::Io(error, format!("publish {}", path.display())));
    }
    sync_directory(&directory).await
}

async fn load_status(operation_id: &str) -> Result<NamespaceOperationStatus, AppError> {
    let path = operation_path(operation_id)?;
    let bytes = match fs::read(&path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(AppError::NotFound);
        }
        Err(error) => return Err(AppError::Io(error, format!("read {}", path.display()))),
    };
    let status = serde_json::from_slice::<NamespaceOperationStatus>(&bytes).map_err(|error| {
        AppError::InternalServerError(format!(
            "decode namespace operation {}: {error}",
            path.display()
        ))
    })?;
    if status.operation_id != operation_id {
        return Err(AppError::Conflict(
            "namespace operation record identity mismatch".into(),
        ));
    }
    Ok(status)
}

fn registered_operation(operation_id: &str) -> Option<Arc<ActiveNamespaceOperation>> {
    OPERATIONS
        .lock()
        .ok()
        .and_then(|operations| operations.get(operation_id).cloned())
}

fn register_operation(active: Arc<ActiveNamespaceOperation>) -> Result<(), AppError> {
    let operation_id = active.snapshot().operation_id;
    let mut operations = OPERATIONS.lock().map_err(|_| {
        AppError::InternalServerError("namespace operation registry poisoned".into())
    })?;
    if operations.contains_key(&operation_id) {
        return Err(AppError::Conflict(format!(
            "namespace operation '{operation_id}' is already active"
        )));
    }
    operations.insert(operation_id, active);
    Ok(())
}

fn unregister_operation(operation_id: &str) {
    if let Ok(mut operations) = OPERATIONS.lock() {
        operations.remove(operation_id);
    }
}

fn accepted_from_status(status: &NamespaceOperationStatus) -> NamespaceOperationAccepted {
    let endpoint = format!("{STATUS_ENDPOINT_PREFIX}/{}", status.operation_id);
    NamespaceOperationAccepted {
        accepted: true,
        operation_id: status.operation_id.clone(),
        mutation_id: status.mutation_id.clone(),
        coordinator_node_id: status.coordinator_node_id.clone(),
        operation: status.operation.clone(),
        kind: status.kind,
        state: status.state,
        phase: status.phase.clone(),
        status_endpoint: endpoint.clone(),
        cancel_endpoint: endpoint,
        source_path: status.source_path.clone(),
        destination_path: status.destination_path.clone(),
    }
}

pub async fn start_namespace_operation(
    state: Arc<AppState>,
    fragment: String,
    kind: NamespaceMutationKind,
    source_path: String,
    destination_path: Option<String>,
) -> Result<NamespaceOperationAccepted, AppError> {
    let runtime = active_cluster_runtime()?;
    let operation_id = Ulid::new().to_string();
    let mutation_id = Ulid::new().to_string();
    let created_at = now_unix_nanos();
    let status = NamespaceOperationStatus {
        operation_id,
        mutation_id,
        coordinator_node_id: runtime.local_node_id.clone(),
        fragment,
        operation: operation_kind_name(&kind)?.to_string(),
        kind: PosixNodeKind::Directory,
        state: NamespaceOperationState::Planning,
        phase: "planning".to_string(),
        source_path: Some(source_path),
        destination_path,
        discovered: 0,
        total: None,
        completed: 0,
        succeeded: 0,
        failed: 0,
        affected_paths: 0,
        current_path: None,
        cancel_requested: false,
        created_at_unix_nanos: created_at,
        updated_at_unix_nanos: created_at,
        finished_at_unix_nanos: None,
        trash_entry_id: None,
        deleted_at_unix: None,
        error: None,
    };
    persist_status(&status).await?;
    let accepted = accepted_from_status(&status);
    let active = Arc::new(ActiveNamespaceOperation::new(status));
    register_operation(active.clone())?;
    spawn_operation_tasks(state, active);
    Ok(accepted)
}

fn spawn_operation_tasks(state: Arc<AppState>, active: Arc<ActiveNamespaceOperation>) {
    let progress_state = state.clone();
    let progress_active = active.clone();
    tokio::spawn(async move {
        publish_operation_progress(progress_state, progress_active).await;
    });
    tokio::spawn(async move {
        execute_operation(state, active).await;
    });
}

async fn publish_operation_progress(state: Arc<AppState>, active: Arc<ActiveNamespaceOperation>) {
    let mut last_published_revision = 0u64;
    loop {
        tokio::time::sleep(PROGRESS_PUBLISH_INTERVAL).await;
        let revision = active.revision.load(Ordering::Acquire);
        let status = active.snapshot();
        if revision != last_published_revision {
            last_published_revision = revision;
            state.emit(
                &status.fragment,
                StorageEventPayload::NamespaceOperationUpdated(status.clone()),
            );
        }
        if status.state.is_terminal() {
            break;
        }
    }
}

async fn execute_operation(state: Arc<AppState>, active: Arc<ActiveNamespaceOperation>) {
    let initial = active.snapshot();
    let base_completed = initial.completed;
    let base_succeeded = initial.succeeded;
    let base_failed = initial.failed;
    let observed = active.clone();
    let observer = Arc::new(
        move |progress: crate::distribution::namespace::NamespaceMutationProgress| {
            observed.update(|status| {
                status.phase = match progress.phase {
                    NamespaceMutationPhase::Planning => "planning",
                    NamespaceMutationPhase::Executing => "executing",
                }
                .to_string();
                status.state = match progress.phase {
                    NamespaceMutationPhase::Planning => NamespaceOperationState::Planning,
                    NamespaceMutationPhase::Executing => NamespaceOperationState::Running,
                };
                status.discovered = status.discovered.max(progress.discovered);
                if status.total.is_none() {
                    status.total = progress.total;
                }
                if matches!(progress.phase, NamespaceMutationPhase::Executing) {
                    let total = status.total.unwrap_or(progress.total.unwrap_or_default());
                    status.completed = base_completed.saturating_add(progress.completed).min(total);
                    status.succeeded = base_succeeded.saturating_add(progress.succeeded).min(total);
                    status.failed = base_failed.saturating_add(progress.failed).min(total);
                }
                status.current_path = progress.current_path;
            });
        },
    );
    let control = NamespaceMutationControl::new(observer, active.cancel.clone());
    let kind = match parse_operation_kind(&initial.operation) {
        Ok(kind) => kind,
        Err(error) => {
            finish_with_error(&active, error.to_string());
            return;
        }
    };
    let request = PeerNamespaceApplyRequest {
        mutation_id: Some(initial.mutation_id.clone()),
        kind,
        fragment: initial.fragment.clone(),
        source_path: initial.source_path.clone(),
        destination_path: initial.destination_path.clone(),
        object_id: None,
        version_id: None,
        metadata: None,
    };
    let result =
        commit_namespace_summary_controlled(request, control, initial.created_at_unix_nanos).await;
    match result {
        Ok(summary) => finish_success(&state, &active, summary).await,
        Err(_error) if active.cancel.is_cancelled() => {
            active.update(|status| {
                status.cancel_requested = true;
                status.state = NamespaceOperationState::Cancelled;
                status.phase = "cancelled".to_string();
                status.finished_at_unix_nanos = Some(now_unix_nanos());
                status.error = None;
            });
            persist_terminal(&state, &active).await;
        }
        Err(error) => {
            finish_with_error(&active, error.to_string());
            persist_terminal(&state, &active).await;
        }
    }
}

async fn finish_success(
    state: &Arc<AppState>,
    active: &Arc<ActiveNamespaceOperation>,
    summary: NamespaceMutationSummary,
) {
    active.update(|status| {
        let total = status.total.unwrap_or(summary.affected_paths);
        status.total = Some(total);
        status.discovered = status.discovered.max(total);
        status.completed = total;
        status.succeeded = total.saturating_sub(status.failed);
        status.affected_paths = total;
        status.state = if status.failed == 0 {
            NamespaceOperationState::Completed
        } else {
            NamespaceOperationState::Partial
        };
        status.phase = if status.failed == 0 {
            "completed".to_string()
        } else {
            "partial".to_string()
        };
        status.current_path = None;
        status.finished_at_unix_nanos = Some(now_unix_nanos());
        status.trash_entry_id = summary.trash_entry_id.clone();
        status.deleted_at_unix = summary.deleted_at_unix;
        status.error = None;
    });
    state.emit(
        &active.snapshot().fragment,
        StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::from_summary(
            active.snapshot().fragment,
            summary,
        )),
    );
    persist_terminal(state, active).await;
}

fn finish_with_error(active: &Arc<ActiveNamespaceOperation>, error: String) {
    active.update(|status| {
        status.state = if status.succeeded > 0 {
            NamespaceOperationState::Partial
        } else {
            NamespaceOperationState::Failed
        };
        status.phase = if status.succeeded > 0 {
            "partial".to_string()
        } else {
            "failed".to_string()
        };
        status.finished_at_unix_nanos = Some(now_unix_nanos());
        status.error = Some(error);
    });
}

async fn persist_terminal(state: &Arc<AppState>, active: &Arc<ActiveNamespaceOperation>) {
    let status = active.snapshot();
    let persisted = match persist_status(&status).await {
        Ok(()) => true,
        Err(error) => {
            tracing::error!(
                operation_id = %status.operation_id,
                error = %error,
                "failed to persist terminal namespace operation"
            );
            false
        }
    };
    let fragment = status.fragment.clone();
    state.emit(
        &fragment,
        StorageEventPayload::NamespaceOperationUpdated(status.clone()),
    );
    if persisted {
        unregister_operation(&status.operation_id);
    }
}

pub async fn namespace_operation_status(
    fragment: &str,
    operation_id: &str,
) -> Result<NamespaceOperationStatus, AppError> {
    let status = match registered_operation(operation_id) {
        Some(active) => active.snapshot(),
        None => load_status(operation_id).await?,
    };
    if status.fragment != fragment {
        return Err(AppError::NotFound);
    }
    Ok(status)
}

pub async fn cancel_namespace_operation(
    fragment: &str,
    operation_id: &str,
) -> Result<NamespaceOperationStatus, AppError> {
    let Some(active) = registered_operation(operation_id) else {
        let status = load_status(operation_id).await?;
        if status.fragment != fragment {
            return Err(AppError::NotFound);
        }
        if status.state.is_terminal() {
            return Ok(status);
        }
        return Err(AppError::Conflict(
            "namespace operation coordinator is recovering; retry cancellation".into(),
        ));
    };
    if active.snapshot().fragment != fragment {
        return Err(AppError::NotFound);
    }
    active.update(|status| {
        if !status.state.is_terminal() {
            status.cancel_requested = true;
        }
    });
    active.cancel.cancel();
    Ok(active.snapshot())
}

/// Resume node-local coordinator jobs after restart. The mutation ULID and
/// timestamp are reused, so already-published NCNX copies remain idempotent.
pub async fn recover_pending_namespace_operations(state: Arc<AppState>) {
    let Ok(runtime) = active_cluster_runtime() else {
        return;
    };
    let Ok(directory) = operations_dir() else {
        return;
    };
    let mut entries = match fs::read_dir(&directory).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => {
            tracing::error!(error = %error, "failed to scan namespace operation records");
            return;
        }
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let bytes = match fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(error) => {
                tracing::error!(path = %path.display(), error = %error, "failed to read namespace operation record");
                continue;
            }
        };
        let status = match serde_json::from_slice::<NamespaceOperationStatus>(&bytes) {
            Ok(status) => status,
            Err(error) => {
                tracing::error!(path = %path.display(), error = %error, "invalid namespace operation record");
                continue;
            }
        };
        if status.state.is_terminal()
            || status.coordinator_node_id != runtime.local_node_id
            || registered_operation(&status.operation_id).is_some()
        {
            continue;
        }
        let active = Arc::new(ActiveNamespaceOperation::new(status));
        if let Err(error) = register_operation(active.clone()) {
            tracing::warn!(error = %error, "namespace operation recovery skipped");
            continue;
        }
        spawn_operation_tasks(state.clone(), active);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepted_response_points_status_and_cancel_to_the_same_operation() {
        let status = NamespaceOperationStatus {
            operation_id: "01J00000000000000000000000".to_string(),
            mutation_id: "01J00000000000000000000001".to_string(),
            coordinator_node_id: "node-a".to_string(),
            fragment: "media".to_string(),
            operation: "rename".to_string(),
            kind: PosixNodeKind::Directory,
            state: NamespaceOperationState::Planning,
            phase: "planning".to_string(),
            source_path: Some("videos".to_string()),
            destination_path: Some("archive".to_string()),
            discovered: 0,
            total: None,
            completed: 0,
            succeeded: 0,
            failed: 0,
            affected_paths: 0,
            current_path: None,
            cancel_requested: false,
            created_at_unix_nanos: 1,
            updated_at_unix_nanos: 1,
            finished_at_unix_nanos: None,
            trash_entry_id: None,
            deleted_at_unix: None,
            error: None,
        };
        let accepted = accepted_from_status(&status);
        assert!(accepted.accepted);
        assert_eq!(accepted.coordinator_node_id, "node-a");
        assert_eq!(accepted.status_endpoint, accepted.cancel_endpoint);
        assert!(accepted.status_endpoint.ends_with(&status.operation_id));
    }

    #[test]
    fn terminal_states_are_explicit() {
        assert!(!NamespaceOperationState::Planning.is_terminal());
        assert!(!NamespaceOperationState::Running.is_terminal());
        assert!(NamespaceOperationState::Completed.is_terminal());
        assert!(NamespaceOperationState::Partial.is_terminal());
        assert!(NamespaceOperationState::Failed.is_terminal());
        assert!(NamespaceOperationState::Cancelled.is_terminal());
    }

    #[test]
    fn only_prefix_mutation_kinds_can_be_scheduled() {
        assert_eq!(
            operation_kind_name(&NamespaceMutationKind::Rename).unwrap(),
            "rename"
        );
        assert_eq!(
            operation_kind_name(&NamespaceMutationKind::Trash).unwrap(),
            "trash"
        );
        assert_eq!(
            operation_kind_name(&NamespaceMutationKind::Recover).unwrap(),
            "recover"
        );
        assert!(operation_kind_name(&NamespaceMutationKind::UploadCommit).is_err());
    }

    #[test]
    fn completed_operation_survives_json_round_trip() {
        let mut status = NamespaceOperationStatus {
            operation_id: "01J00000000000000000000000".to_string(),
            mutation_id: "01J00000000000000000000001".to_string(),
            coordinator_node_id: "node-a".to_string(),
            fragment: "media".to_string(),
            operation: "trash".to_string(),
            kind: PosixNodeKind::Directory,
            state: NamespaceOperationState::Completed,
            phase: "completed".to_string(),
            source_path: Some("videos".to_string()),
            destination_path: None,
            discovered: 10,
            total: Some(10),
            completed: 10,
            succeeded: 10,
            failed: 0,
            affected_paths: 10,
            current_path: None,
            cancel_requested: false,
            created_at_unix_nanos: 1,
            updated_at_unix_nanos: 2,
            finished_at_unix_nanos: Some(2),
            trash_entry_id: Some("01J00000000000000000000001".to_string()),
            deleted_at_unix: Some(0),
            error: None,
        };
        status.error = None;
        let bytes = serde_json::to_vec(&status).unwrap();
        assert_eq!(
            serde_json::from_slice::<NamespaceOperationStatus>(&bytes).unwrap(),
            status
        );
    }
}
