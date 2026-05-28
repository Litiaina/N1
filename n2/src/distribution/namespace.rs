use crate::core::disk_manager::{FS_STORAGE_ROOT, ObjectMetadata};
use crate::core::index;
use crate::distribution::membership::active_cluster_runtime;
use crate::distribution::peer::{authorize_peer, cluster_secret, peer_http_client};
use crate::enums::app_error_enum::AppError;
use crate::namespace::models::NamespaceMutationSummary;
use crate::namespace::runtime::global_namespace_runtime;
use axum::extract::Json as AxumJson;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::sync::{Arc, LazyLock, Mutex as StdMutex};
use tokio::sync::Mutex;
use ulid::Ulid;

use crate::routes::storage_routes::AppState;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NamespaceMutationKind {
    Mkdir,
    Rename,
    Trash,
    Recover,
    RecoverAll,
    Purge,
    EmptyTrash,
    UploadCommit,
    QuotaEdit,
}

#[derive(Clone, Debug)]
pub enum NamespaceApplyOutcome {
    None,
    Created(bool),
    Summary(NamespaceMutationSummary),
    CommittedObject(String),
}

impl NamespaceApplyOutcome {
    pub fn created(&self) -> Option<bool> {
        match self {
            Self::Created(created) => Some(*created),
            _ => None,
        }
    }

    pub fn summary(&self) -> Option<NamespaceMutationSummary> {
        match self {
            Self::Summary(summary) => Some(summary.clone()),
            _ => None,
        }
    }

    pub fn committed_object(&self) -> Option<String> {
        match self {
            Self::CommittedObject(object_id) => Some(object_id.clone()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NamespaceMutationRecord {
    pub mutation_id: String,
    pub fragment: String,
    pub kind: NamespaceMutationKind,
    pub source_path: Option<String>,
    pub destination_path: Option<String>,
    pub object_id: Option<String>,
    pub version_id: Option<String>,
    pub membership_epoch: u64,
    pub created_at_unix: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerNamespaceApplyRequest {
    pub kind: NamespaceMutationKind,
    pub fragment: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ObjectMetadata>,
}

static NAMESPACE_COMMIT_LOCKS: LazyLock<StdMutex<HashMap<String, Arc<Mutex<()>>>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

fn namespace_commit_lock(fragment: &str) -> Result<Arc<Mutex<()>>, AppError> {
    let mut locks = NAMESPACE_COMMIT_LOCKS.lock().map_err(|_| {
        AppError::InternalServerError("N2 namespace commit lock table is poisoned".into())
    })?;
    Ok(locks
        .entry(fragment.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone())
}

fn namespace_ledger_root() -> Result<std::path::PathBuf, AppError> {
    let root = FS_STORAGE_ROOT.get().ok_or_else(|| {
        AppError::InternalServerError("N2 fs storage root is not initialized".into())
    })?;
    Ok(std::path::PathBuf::from(root)
        .join(".n2")
        .join("namespace-ledger"))
}

pub fn append_namespace_mutation(
    mut record: NamespaceMutationRecord,
) -> Result<NamespaceMutationRecord, AppError> {
    if record.mutation_id.trim().is_empty() {
        record.mutation_id = Ulid::new().to_string();
    }
    if record.created_at_unix == 0 {
        record.created_at_unix = Utc::now().timestamp();
    }
    let root = namespace_ledger_root()?;
    fs::create_dir_all(&root)
        .map_err(|e| AppError::Io(e, format!("create N2 namespace ledger {}", root.display())))?;
    let path = root.join(format!("{}.jsonl", record.fragment));
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|e| AppError::Io(e, format!("open N2 namespace ledger {}", path.display())))?;
    let line = serde_json::to_vec(&record)
        .map_err(|e| AppError::InternalServerError(format!("encode N2 namespace ledger: {e}")))?;
    file.write_all(&line)
        .and_then(|_| file.write_all(b"\n"))
        .map_err(|e| AppError::Io(e, format!("append N2 namespace ledger {}", path.display())))?;
    Ok(record)
}

pub fn build_namespace_record(
    fragment: &str,
    kind: NamespaceMutationKind,
    membership_epoch: u64,
) -> NamespaceMutationRecord {
    NamespaceMutationRecord {
        mutation_id: String::new(),
        fragment: fragment.to_string(),
        kind,
        source_path: None,
        destination_path: None,
        object_id: None,
        version_id: None,
        membership_epoch,
        created_at_unix: 0,
    }
}

fn required_quorum(total: usize) -> usize {
    (total / 2).saturating_add(1).max(1)
}

fn namespace_peer_url(endpoint: &str) -> String {
    format!("{}/n2/peer/namespace/apply", endpoint.trim_end_matches('/'))
}

fn namespace_ledger_record(
    request: &PeerNamespaceApplyRequest,
) -> Result<NamespaceMutationRecord, AppError> {
    let runtime = active_cluster_runtime()?;
    let mut record = build_namespace_record(
        &request.fragment,
        request.kind.clone(),
        runtime.membership.epoch,
    );
    record.source_path = request.source_path.clone();
    record.destination_path = request.destination_path.clone();
    record.object_id = request.object_id.clone();
    record.version_id = request.version_id.clone();
    Ok(record)
}

fn append_request_ledger(request: &PeerNamespaceApplyRequest) -> Result<(), AppError> {
    let _ = append_namespace_mutation(namespace_ledger_record(request)?)?;
    Ok(())
}

fn validate_namespace_request(request: &PeerNamespaceApplyRequest) -> Result<(), AppError> {
    if request.fragment.trim().is_empty() {
        return Err(AppError::BadRequest(
            "namespace mutation requires fragment".into(),
        ));
    }

    let require_source = |name: &str| -> Result<(), AppError> {
        if request
            .source_path
            .as_deref()
            .unwrap_or_default()
            .trim_matches('/')
            .is_empty()
        {
            return Err(AppError::BadRequest(format!(
                "{name} requires a non-empty source_path"
            )));
        }
        Ok(())
    };
    let require_destination = |name: &str| -> Result<(), AppError> {
        if request
            .destination_path
            .as_deref()
            .unwrap_or_default()
            .trim_matches('/')
            .is_empty()
        {
            return Err(AppError::BadRequest(format!(
                "{name} requires a non-empty destination_path"
            )));
        }
        Ok(())
    };

    match request.kind {
        NamespaceMutationKind::Mkdir
        | NamespaceMutationKind::Trash
        | NamespaceMutationKind::Recover
        | NamespaceMutationKind::Purge => require_source("namespace mutation")?,
        NamespaceMutationKind::Rename => {
            require_source("rename")?;
            require_destination("rename")?;
        }
        NamespaceMutationKind::RecoverAll
        | NamespaceMutationKind::EmptyTrash
        | NamespaceMutationKind::QuotaEdit => {}
        NamespaceMutationKind::UploadCommit => {
            require_source("upload commit")?;
            if request.object_id.as_deref().unwrap_or_default().is_empty() {
                return Err(AppError::BadRequest(
                    "upload commit requires object_id".into(),
                ));
            }
            if request.metadata.is_none() {
                return Err(AppError::BadRequest(
                    "upload commit requires object metadata".into(),
                ));
            }
        }
    }
    Ok(())
}

async fn post_namespace_to_peer(
    endpoint: &str,
    request: &PeerNamespaceApplyRequest,
) -> Result<(), AppError> {
    let client = peer_http_client()?;
    let mut req = client.post(namespace_peer_url(endpoint)).json(request);
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = req.send().await.map_err(|e| {
        AppError::ServiceUnavailable(format!("namespace peer apply failed at {endpoint}: {e}"))
    })?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "namespace peer apply returned {} from {}: {}",
            status, endpoint, text
        )));
    }
    Ok(())
}

async fn apply_namespace_request_core(
    request: &PeerNamespaceApplyRequest,
) -> Result<NamespaceApplyOutcome, AppError> {
    validate_namespace_request(request)?;
    match request.kind {
        NamespaceMutationKind::Mkdir => {
            let created = global_namespace_runtime()
                .mkdir(
                    &request.fragment,
                    request.source_path.as_deref().unwrap_or_default(),
                )
                .await?;
            Ok(NamespaceApplyOutcome::Created(created))
        }
        NamespaceMutationKind::Rename => {
            let summary = global_namespace_runtime()
                .rename(
                    &request.fragment,
                    request.source_path.as_deref().unwrap_or_default(),
                    request.destination_path.as_deref().unwrap_or_default(),
                )
                .await?;
            Ok(NamespaceApplyOutcome::Summary(summary))
        }
        NamespaceMutationKind::Trash => {
            let summary = global_namespace_runtime()
                .trash(
                    &request.fragment,
                    request.source_path.as_deref().unwrap_or_default(),
                )
                .await?;
            Ok(NamespaceApplyOutcome::Summary(summary))
        }
        NamespaceMutationKind::Recover => {
            let summary = global_namespace_runtime()
                .recover(
                    &request.fragment,
                    request.source_path.as_deref().unwrap_or_default(),
                )
                .await?;
            Ok(NamespaceApplyOutcome::Summary(summary))
        }
        NamespaceMutationKind::RecoverAll => {
            let summary = global_namespace_runtime()
                .recover_all(&request.fragment)
                .await?;
            Ok(NamespaceApplyOutcome::Summary(summary))
        }
        NamespaceMutationKind::Purge => {
            let summary = global_namespace_runtime()
                .purge_trash_path(
                    &request.fragment,
                    request.source_path.as_deref().unwrap_or_default(),
                )
                .await?;
            Ok(NamespaceApplyOutcome::Summary(summary))
        }
        NamespaceMutationKind::EmptyTrash => {
            let summary = global_namespace_runtime()
                .empty_trash(&request.fragment)
                .await?;
            Ok(NamespaceApplyOutcome::Summary(summary))
        }
        NamespaceMutationKind::QuotaEdit => Ok(NamespaceApplyOutcome::None),
        NamespaceMutationKind::UploadCommit => {
            let metadata = request.metadata.as_ref().ok_or_else(|| {
                AppError::BadRequest("upload commit requires object metadata".into())
            })?;
            let object_id = request
                .object_id
                .as_deref()
                .ok_or_else(|| AppError::BadRequest("upload commit requires object_id".into()))?;
            let logical_path = request
                .source_path
                .as_deref()
                .ok_or_else(|| AppError::BadRequest("upload commit requires source_path".into()))?;
            index::publish_object(metadata).await?;
            let committed = global_namespace_runtime()
                .finalize_logical_upload(&request.fragment, logical_path, object_id, metadata)
                .await?;
            Ok(NamespaceApplyOutcome::CommittedObject(committed))
        }
    }
}

async fn apply_namespace_request(request: &PeerNamespaceApplyRequest) -> Result<(), AppError> {
    let lock = namespace_commit_lock(&request.fragment)?;
    let _guard = lock.lock().await;
    apply_namespace_request_core(request).await?;
    append_request_ledger(request)?;
    Ok(())
}

async fn apply_peers_quorum(request: &PeerNamespaceApplyRequest) -> Result<(), AppError> {
    let runtime = active_cluster_runtime()?;
    let active = runtime.membership.active_write_nodes();
    let required = required_quorum(active.len());
    let mut success = 1usize;
    let mut failures = Vec::new();

    for node in active
        .iter()
        .filter(|node| node.id != runtime.local_node_id)
    {
        match post_namespace_to_peer(&node.endpoint, request).await {
            Ok(()) => success = success.saturating_add(1),
            Err(err) => failures.push(format!("{}: {}", node.id, err)),
        }
    }

    if success < required {
        return Err(AppError::ServiceUnavailable(format!(
            "N2 namespace quorum failed: {} successful, {} required; {}",
            success,
            required,
            failures.join("; ")
        )));
    }
    Ok(())
}

pub async fn commit_upload_quorum(
    fragment: &str,
    logical_path: &str,
    physical_object_id: &str,
    metadata: &ObjectMetadata,
) -> Result<String, AppError> {
    let request = PeerNamespaceApplyRequest {
        kind: NamespaceMutationKind::UploadCommit,
        fragment: fragment.to_string(),
        source_path: Some(logical_path.to_string()),
        destination_path: None,
        object_id: Some(physical_object_id.to_string()),
        version_id: Some(metadata.version_id.clone()),
        metadata: Some(metadata.clone()),
    };
    let outcome = commit_namespace_quorum(request).await?;
    Ok(outcome
        .committed_object()
        .unwrap_or_else(|| physical_object_id.to_string()))
}

pub async fn commit_namespace_quorum(
    request: PeerNamespaceApplyRequest,
) -> Result<NamespaceApplyOutcome, AppError> {
    validate_namespace_request(&request)?;
    apply_peers_quorum(&request).await?;
    let lock = namespace_commit_lock(&request.fragment)?;
    let _guard = lock.lock().await;
    let outcome = apply_namespace_request_core(&request).await?;
    append_request_ledger(&request)?;
    Ok(outcome)
}

pub async fn commit_namespace_created_quorum(
    request: PeerNamespaceApplyRequest,
) -> Result<bool, AppError> {
    Ok(commit_namespace_quorum(request)
        .await?
        .created()
        .unwrap_or(false))
}

pub async fn commit_namespace_summary_quorum(
    request: PeerNamespaceApplyRequest,
) -> Result<NamespaceMutationSummary, AppError> {
    commit_namespace_quorum(request)
        .await?
        .summary()
        .ok_or_else(|| {
            AppError::InternalServerError("namespace mutation did not return a summary".into())
        })
}

pub async fn replicate_namespace_quorum(
    request: PeerNamespaceApplyRequest,
) -> Result<(), AppError> {
    validate_namespace_request(&request)?;
    apply_peers_quorum(&request).await?;
    let lock = namespace_commit_lock(&request.fragment)?;
    let _guard = lock.lock().await;
    append_request_ledger(&request)?;
    Ok(())
}

pub async fn peer_apply_namespace(
    headers: HeaderMap,
    AxumJson(request): AxumJson<PeerNamespaceApplyRequest>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    authorize_peer(&headers)?;
    apply_namespace_request(&request).await?;
    Ok((StatusCode::OK, Json(serde_json::json!({"applied": true}))))
}

pub fn build_namespace_peer_routes() -> Router<Arc<AppState>> {
    Router::new().route("/n2/peer/namespace/apply", post(peer_apply_namespace))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(kind: NamespaceMutationKind) -> PeerNamespaceApplyRequest {
        PeerNamespaceApplyRequest {
            kind,
            fragment: "frag".into(),
            source_path: Some("dir/file.bin".into()),
            destination_path: None,
            object_id: None,
            version_id: None,
            metadata: None,
        }
    }

    #[test]
    fn two_node_cluster_requires_both_namespace_votes() {
        assert_eq!(required_quorum(1), 1);
        assert_eq!(required_quorum(2), 2);
        assert_eq!(required_quorum(3), 2);
        assert_eq!(required_quorum(4), 3);
    }

    #[test]
    fn namespace_mutation_validation_rejects_empty_paths() {
        let mut rename = request(NamespaceMutationKind::Rename);
        rename.destination_path = Some(String::new());
        assert!(validate_namespace_request(&rename).is_err());

        let mut trash = request(NamespaceMutationKind::Trash);
        trash.source_path = Some("/".into());
        assert!(validate_namespace_request(&trash).is_err());
    }

    #[test]
    fn namespace_recover_all_and_empty_trash_are_fragment_scoped() {
        let mut recover_all = request(NamespaceMutationKind::RecoverAll);
        recover_all.source_path = None;
        assert!(validate_namespace_request(&recover_all).is_ok());

        let mut empty_trash = request(NamespaceMutationKind::EmptyTrash);
        empty_trash.source_path = None;
        assert!(validate_namespace_request(&empty_trash).is_ok());
    }
}
