use crate::core::disk_manager::{FS_STORAGE_ROOT, ObjectMetadata};
use crate::core::index;
use crate::distribution::membership::active_cluster_runtime;
use crate::distribution::peer::{authorize_peer, cluster_secret, peer_http_client};
use crate::enums::app_error_enum::AppError;
use crate::namespace::models::NamespaceMutationSummary;
use crate::namespace::runtime::global_namespace_runtime;
use axum::extract::{Json as AxumJson, Path};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, Write};
use std::sync::{Arc, LazyLock, Mutex as StdMutex};
use tokio::sync::Mutex;
use tokio::time::{Duration, timeout};
use ulid::Ulid;

use crate::routes::storage_routes::AppState;

const NAMESPACE_PEER_APPLY_TIMEOUT: Duration = Duration::from_secs(3);

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mutation_id: Option<String>,
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

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NamespaceLedgerSummary {
    pub fragment: String,
    pub local_requests: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_mutation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_kind: Option<NamespaceMutationKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_source_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_destination_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_object_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_version_id: Option<String>,
    pub request_ledger_path: String,
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

fn namespace_requests_peer_url(endpoint: &str, fragment: &str) -> String {
    format!(
        "{}/n2/peer/namespace/requests/{}",
        endpoint.trim_end_matches('/'),
        percent_encoding::utf8_percent_encode(fragment, percent_encoding::NON_ALPHANUMERIC)
    )
}

fn ensure_request_mutation_id(mut request: PeerNamespaceApplyRequest) -> PeerNamespaceApplyRequest {
    if request
        .mutation_id
        .as_deref()
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        request.mutation_id = Some(Ulid::new().to_string());
    }
    request
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
    record.mutation_id = request.mutation_id.clone().unwrap_or_default();
    Ok(record)
}

fn namespace_request_ledger_path(fragment: &str) -> Result<std::path::PathBuf, AppError> {
    Ok(namespace_ledger_root()?.join(format!("{fragment}.requests.jsonl")))
}

fn read_request_ledger(fragment: &str) -> Result<Vec<PeerNamespaceApplyRequest>, AppError> {
    let path = namespace_request_ledger_path(fragment)?;
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = fs::File::open(&path).map_err(|e| {
        AppError::Io(
            e,
            format!("open N2 namespace request ledger {}", path.display()),
        )
    })?;
    let mut out = Vec::new();
    for line in std::io::BufReader::new(file).lines() {
        let line = line.map_err(|e| {
            AppError::Io(
                e,
                format!("read N2 namespace request ledger {}", path.display()),
            )
        })?;
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(request) = serde_json::from_str::<PeerNamespaceApplyRequest>(&line) {
            out.push(request);
        }
    }
    out.sort_by(|left, right| left.mutation_id.cmp(&right.mutation_id));
    Ok(out)
}

pub fn namespace_local_ledger_summary(fragment: &str) -> Result<NamespaceLedgerSummary, AppError> {
    let path = namespace_request_ledger_path(fragment)?;
    let requests = read_request_ledger(fragment)?;
    let last = requests.last();
    Ok(NamespaceLedgerSummary {
        fragment: fragment.to_string(),
        local_requests: requests.len() as u64,
        last_mutation_id: last.and_then(|request| request.mutation_id.clone()),
        last_kind: last.map(|request| request.kind.clone()),
        last_source_path: last.and_then(|request| request.source_path.clone()),
        last_destination_path: last.and_then(|request| request.destination_path.clone()),
        last_object_id: last.and_then(|request| request.object_id.clone()),
        last_version_id: last.and_then(|request| request.version_id.clone()),
        request_ledger_path: path.to_string_lossy().to_string(),
    })
}

fn request_seen(fragment: &str, mutation_id: Option<&str>) -> Result<bool, AppError> {
    let Some(mutation_id) = mutation_id.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(false);
    };
    Ok(read_request_ledger(fragment)?.iter().any(|request| {
        request
            .mutation_id
            .as_deref()
            .is_some_and(|value| value == mutation_id)
    }))
}

fn append_request_ledger(request: &PeerNamespaceApplyRequest) -> Result<(), AppError> {
    let _ = append_namespace_mutation(namespace_ledger_record(request)?)?;
    let path = namespace_request_ledger_path(&request.fragment)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            AppError::Io(
                e,
                format!("create N2 namespace ledger {}", parent.display()),
            )
        })?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|e| {
            AppError::Io(
                e,
                format!("open N2 namespace request ledger {}", path.display()),
            )
        })?;
    let line = serde_json::to_vec(request).map_err(|e| {
        AppError::InternalServerError(format!("encode N2 namespace request ledger: {e}"))
    })?;
    file.write_all(&line)
        .and_then(|_| file.write_all(b"\n"))
        .map_err(|e| {
            AppError::Io(
                e,
                format!("append N2 namespace request ledger {}", path.display()),
            )
        })?;
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
    let response = timeout(NAMESPACE_PEER_APPLY_TIMEOUT, req.send())
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!("namespace peer apply timed out at {endpoint}"))
        })?
        .map_err(|e| {
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
    if request_seen(&request.fragment, request.mutation_id.as_deref())? {
        return Ok(());
    }
    let lock = namespace_commit_lock(&request.fragment)?;
    let _guard = lock.lock().await;
    if request_seen(&request.fragment, request.mutation_id.as_deref())? {
        return Ok(());
    }
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
        mutation_id: None,
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
    let request = ensure_request_mutation_id(request);
    validate_namespace_request(&request)?;
    apply_peers_quorum(&request).await?;
    let lock = namespace_commit_lock(&request.fragment)?;
    let _guard = lock.lock().await;
    if request_seen(&request.fragment, request.mutation_id.as_deref())? {
        return Ok(NamespaceApplyOutcome::None);
    }
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
    let request = ensure_request_mutation_id(request);
    validate_namespace_request(&request)?;
    apply_peers_quorum(&request).await?;
    let lock = namespace_commit_lock(&request.fragment)?;
    let _guard = lock.lock().await;
    if request_seen(&request.fragment, request.mutation_id.as_deref())? {
        return Ok(());
    }
    append_request_ledger(&request)?;
    Ok(())
}

pub async fn peer_apply_namespace(
    headers: HeaderMap,
    AxumJson(request): AxumJson<PeerNamespaceApplyRequest>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    authorize_peer(&headers)?;
    let request = ensure_request_mutation_id(request);
    apply_namespace_request(&request).await?;
    Ok((StatusCode::OK, Json(serde_json::json!({"applied": true}))))
}

pub async fn peer_namespace_requests(
    headers: HeaderMap,
    Path(fragment): Path<String>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(serde_json::json!({
        "fragment": fragment,
        "requests": read_request_ledger(&fragment)?,
    })))
}

pub async fn peer_namespace_summary(
    headers: HeaderMap,
    Path(fragment): Path<String>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(namespace_local_ledger_summary(&fragment)?))
}

async fn fetch_peer_namespace_requests(
    endpoint: &str,
    fragment: &str,
) -> Result<Vec<PeerNamespaceApplyRequest>, AppError> {
    let client = peer_http_client()?;
    let mut req = client.get(namespace_requests_peer_url(endpoint, fragment));
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(NAMESPACE_PEER_APPLY_TIMEOUT, req.send())
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "namespace request ledger fetch timed out at {endpoint}"
            ))
        })?
        .map_err(|e| {
            AppError::ServiceUnavailable(format!(
                "namespace request ledger fetch failed at {endpoint}: {e}"
            ))
        })?;
    if !response.status().is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "namespace request ledger fetch returned {} from {}",
            response.status(),
            endpoint
        )));
    }
    let payload = response.json::<serde_json::Value>().await.map_err(|e| {
        AppError::InternalServerError(format!("decode namespace request ledger: {e}"))
    })?;
    serde_json::from_value(payload.get("requests").cloned().unwrap_or_default()).map_err(|e| {
        AppError::InternalServerError(format!("decode namespace request records: {e}"))
    })
}

pub async fn sync_namespace_from_peers(fragment: &str) -> Result<u64, AppError> {
    let runtime = active_cluster_runtime()?;
    let mut applied = 0u64;
    for node in runtime
        .membership
        .readable_nodes()
        .into_iter()
        .filter(|node| node.id != runtime.local_node_id)
    {
        let Ok(mut requests) = fetch_peer_namespace_requests(&node.endpoint, fragment).await else {
            continue;
        };
        requests.sort_by(|left, right| left.mutation_id.cmp(&right.mutation_id));
        for request in requests {
            if request.fragment != fragment {
                continue;
            }
            if request_seen(fragment, request.mutation_id.as_deref())? {
                continue;
            }
            apply_namespace_request(&request).await?;
            applied = applied.saturating_add(1);
        }
    }
    Ok(applied)
}

pub fn build_namespace_peer_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/n2/peer/namespace/apply", post(peer_apply_namespace))
        .route(
            "/n2/peer/namespace/requests/{fragment}",
            get(peer_namespace_requests),
        )
        .route(
            "/n2/peer/namespace/summary/{fragment}",
            get(peer_namespace_summary),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(kind: NamespaceMutationKind) -> PeerNamespaceApplyRequest {
        PeerNamespaceApplyRequest {
            mutation_id: None,
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
