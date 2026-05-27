use crate::core::disk_manager::{FS_STORAGE_ROOT, ObjectMetadata};
use crate::core::index;
use crate::distribution::membership::active_cluster_runtime;
use crate::distribution::peer::{authorize_peer, cluster_secret, peer_http_client};
use crate::enums::app_error_enum::AppError;
use crate::namespace::runtime::global_namespace_runtime;
use axum::extract::Json as AxumJson;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::sync::Arc;
use ulid::Ulid;

use crate::routes::storage_routes::AppState;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NamespaceMutationKind {
    Mkdir,
    Rename,
    Trash,
    Recover,
    Purge,
    EmptyTrash,
    UploadCommit,
    QuotaEdit,
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

async fn apply_namespace_request(request: &PeerNamespaceApplyRequest) -> Result<(), AppError> {
    match request.kind {
        NamespaceMutationKind::Mkdir => {
            global_namespace_runtime()
                .mkdir(
                    &request.fragment,
                    request.source_path.as_deref().unwrap_or_default(),
                )
                .await?;
        }
        NamespaceMutationKind::Rename => {
            global_namespace_runtime()
                .rename(
                    &request.fragment,
                    request.source_path.as_deref().unwrap_or_default(),
                    request.destination_path.as_deref().unwrap_or_default(),
                )
                .await?;
        }
        NamespaceMutationKind::Trash => {
            global_namespace_runtime()
                .trash(
                    &request.fragment,
                    request.source_path.as_deref().unwrap_or_default(),
                )
                .await?;
        }
        NamespaceMutationKind::Recover => {
            global_namespace_runtime()
                .recover(
                    &request.fragment,
                    request.source_path.as_deref().unwrap_or_default(),
                )
                .await?;
        }
        NamespaceMutationKind::Purge => {
            global_namespace_runtime()
                .purge_trash_path(
                    &request.fragment,
                    request.source_path.as_deref().unwrap_or_default(),
                )
                .await?;
        }
        NamespaceMutationKind::EmptyTrash => {
            global_namespace_runtime()
                .empty_trash(&request.fragment)
                .await?;
        }
        NamespaceMutationKind::QuotaEdit => {}
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
            global_namespace_runtime()
                .finalize_logical_upload(&request.fragment, logical_path, object_id, metadata)
                .await?;
        }
    }

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
    let _ = append_namespace_mutation(record)?;
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
    let committed = global_namespace_runtime()
        .finalize_logical_upload(fragment, logical_path, physical_object_id, metadata)
        .await?;
    apply_peers_quorum(&request).await?;
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
    let _ = append_namespace_mutation(record)?;
    Ok(committed)
}

pub async fn commit_namespace_quorum(request: PeerNamespaceApplyRequest) -> Result<(), AppError> {
    apply_namespace_request(&request).await?;
    apply_peers_quorum(&request).await
}

pub async fn replicate_namespace_quorum(
    request: PeerNamespaceApplyRequest,
) -> Result<(), AppError> {
    apply_peers_quorum(&request).await?;
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
    let _ = append_namespace_mutation(record)?;
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
