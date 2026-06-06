use crate::core::disk_manager::{FS_STORAGE_ROOT, ObjectMetadata, ObjectPathManager};
use crate::core::index_store::{
    TRASH_DIR, fragment_root_on, internal_namespace_entry_name, read_namespace_symlink_object_id,
};
use crate::distribution::membership::active_cluster_runtime;
use crate::distribution::namespace_group::{
    NamespaceGroupAssignment, assignment_nodes, load_namespace_group, persist_namespace_group,
    namespace_partition_count, namespace_partition_for_path, resolve_namespace_group,
};
use crate::distribution::peer::{authorize_peer, cluster_secret, peer_http_client};
use crate::enums::app_error_enum::AppError;
use crate::namespace::models::{NamespaceEntry, NamespaceMutationSummary};
use crate::namespace::runtime::{NamespaceObjectRefPage, global_namespace_runtime};
use axum::extract::{Json as AxumJson, Path};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, Write};
use std::os::unix::fs as unix_fs;
use std::path::{Path as FsPath, PathBuf};
use std::sync::{Arc, LazyLock, Mutex as StdMutex};
use tokio::sync::Mutex;
use tokio::time::{Duration, timeout};
use tracing::warn;
use ulid::Ulid;

use crate::routes::storage_routes::AppState;

const NAMESPACE_PEER_APPLY_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerNamespaceReadKind {
    ListDir,
    Entry,
    TrashedEntry,
    ResolveLiveObject,
    ObjectRefs,
    Reachability,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerNamespaceReadRequest {
    pub fragment: String,
    pub kind: PeerNamespaceReadKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default)]
    pub include_trashed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerNamespaceReadResponse {
    ListDir {
        entries: Vec<NamespaceEntry>,
        continuation_token: Option<String>,
    },
    Entry(Option<NamespaceEntry>),
    ObjectId(Option<String>),
    ObjectRefs(NamespaceObjectRefPage),
    Reachability(BTreeSet<String>),
}

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

#[derive(Clone, Debug, Serialize, Deserialize)]
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
    #[serde(default)]
    pub partition_id: u16,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PreparedNamespaceMutation {
    log_index: u64,
    assignment: NamespaceGroupAssignment,
    request: PeerNamespaceApplyRequest,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NamespaceSnapshotEntry {
    path: String,
    trashed: bool,
    directory: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    object_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NamespaceSnapshot {
    format_version: u32,
    fragment: String,
    partition_id: u16,
    committed_log_index: u64,
    requests: Vec<PeerNamespaceApplyRequest>,
    entries: Vec<NamespaceSnapshotEntry>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NamespaceLedgerSummary {
    pub fragment: String,
    pub partition_id: u16,
    pub local_requests: u64,
    pub committed_log_index: u64,
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

tokio::task_local! {
    static FORCE_LOCAL_NAMESPACE_READ: bool;
}

fn namespace_partition_commit_lock(
    fragment: &str,
    partition_id: u16,
) -> Result<Arc<Mutex<()>>, AppError> {
    let mut locks = NAMESPACE_COMMIT_LOCKS.lock().map_err(|_| {
        AppError::InternalServerError("N2 namespace commit lock table is poisoned".into())
    })?;
    let key = format!("{fragment}:{partition_id:04}");
    Ok(locks
        .entry(key)
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone())
}

fn namespace_request_partition(request: &PeerNamespaceApplyRequest) -> u16 {
    let path = request
        .source_path
        .as_deref()
        .or(request.destination_path.as_deref())
        .unwrap_or_default();
    namespace_partition_for_path(&request.fragment, path)
}

fn namespace_read_partition(request: &PeerNamespaceReadRequest) -> u16 {
    namespace_partition_for_path(&request.fragment, request.path.as_deref().unwrap_or_default())
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
    let path = root
        .join(&record.fragment)
        .join(format!("{:04}.jsonl", record.partition_id));
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            AppError::Io(e, format!("create N2 namespace ledger {}", parent.display()))
        })?;
    }
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
        partition_id: 0,
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

fn namespace_prepare_peer_url(endpoint: &str) -> String {
    format!(
        "{}/n2/peer/namespace/prepare",
        endpoint.trim_end_matches('/')
    )
}

fn namespace_commit_peer_url(endpoint: &str) -> String {
    format!(
        "{}/n2/peer/namespace/commit",
        endpoint.trim_end_matches('/')
    )
}

fn namespace_abort_peer_url(endpoint: &str) -> String {
    format!("{}/n2/peer/namespace/abort", endpoint.trim_end_matches('/'))
}

fn namespace_coordinate_peer_url(endpoint: &str) -> String {
    format!(
        "{}/n2/peer/namespace/coordinate",
        endpoint.trim_end_matches('/')
    )
}

fn namespace_requests_peer_url(endpoint: &str, fragment: &str, partition_id: u16) -> String {
    format!(
        "{}/n2/peer/namespace/requests/{}/{}",
        endpoint.trim_end_matches('/'),
        percent_encoding::utf8_percent_encode(fragment, percent_encoding::NON_ALPHANUMERIC),
        partition_id
    )
}

fn namespace_read_peer_url(endpoint: &str) -> String {
    format!("{}/n2/peer/namespace/read", endpoint.trim_end_matches('/'))
}

fn namespace_summary_peer_url(endpoint: &str, fragment: &str, partition_id: u16) -> String {
    format!(
        "{}/n2/peer/namespace/summary/{}/{}",
        endpoint.trim_end_matches('/'),
        percent_encoding::utf8_percent_encode(fragment, percent_encoding::NON_ALPHANUMERIC),
        partition_id
    )
}

fn namespace_prepared_peer_url(
    endpoint: &str,
    fragment: &str,
    partition_id: u16,
    log_index: u64,
) -> String {
    format!(
        "{}/n2/peer/namespace/prepared/{}/{}/{}",
        endpoint.trim_end_matches('/'),
        percent_encoding::utf8_percent_encode(fragment, percent_encoding::NON_ALPHANUMERIC),
        partition_id,
        log_index
    )
}

fn namespace_snapshot_peer_url(endpoint: &str, fragment: &str, partition_id: u16) -> String {
    format!(
        "{}/n2/peer/namespace/snapshot/{}/{}",
        endpoint.trim_end_matches('/'),
        percent_encoding::utf8_percent_encode(fragment, percent_encoding::NON_ALPHANUMERIC),
        partition_id
    )
}

async fn fetch_namespace_summary(
    endpoint: &str,
    fragment: &str,
    partition_id: u16,
) -> Result<NamespaceLedgerSummary, AppError> {
    let client = peer_http_client()?;
    let mut request = client.get(namespace_summary_peer_url(endpoint, fragment, partition_id));
    if let Some(secret) = cluster_secret() {
        request = request.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(NAMESPACE_PEER_APPLY_TIMEOUT, request.send())
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!("namespace summary timed out at {endpoint}"))
        })?
        .map_err(|err| {
            AppError::ServiceUnavailable(format!("namespace summary failed at {endpoint}: {err}"))
        })?;
    if !response.status().is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "namespace summary returned {} from {}",
            response.status(),
            endpoint
        )));
    }
    response.json().await.map_err(|err| {
        AppError::InternalServerError(format!("decode namespace summary from {endpoint}: {err}"))
    })
}

async fn most_current_reachable_voter(
    runtime: &crate::distribution::membership::ClusterRuntime,
    assignment: &NamespaceGroupAssignment,
) -> Result<crate::distribution::membership::ClusterNode, AppError> {
    let mut candidates = Vec::new();
    for node in assignment_nodes(runtime, assignment)? {
        let summary = if node.id == runtime.local_node_id {
            namespace_local_ledger_summary(&assignment.fragment, assignment.partition_id)
        } else {
            fetch_namespace_summary(&node.endpoint, &assignment.fragment, assignment.partition_id).await
        };
        if let Ok(summary) = summary {
            candidates.push((summary.committed_log_index, node));
        }
    }
    candidates
        .into_iter()
        .max_by(|(left_pos, left_node), (right_pos, right_node)| {
            left_pos
                .cmp(right_pos)
                .then_with(|| {
                    (left_node.id == runtime.local_node_id)
                        .cmp(&(right_node.id == runtime.local_node_id))
                })
                .then_with(|| right_node.id.cmp(&left_node.id))
        })
        .map(|(_, node)| node)
        .ok_or_else(|| {
            AppError::ServiceUnavailable(
                "no reachable namespace voter can serve this fragment".into(),
            )
        })
}

pub async fn namespace_read_if_remote(
    request: PeerNamespaceReadRequest,
) -> Result<Option<PeerNamespaceReadResponse>, AppError> {
    if FORCE_LOCAL_NAMESPACE_READ
        .try_with(|force_local| *force_local)
        .unwrap_or(false)
    {
        return Ok(None);
    }
    let Some(runtime) = crate::distribution::membership::try_active_cluster_runtime() else {
        return Ok(None);
    };
    let partition_id = namespace_read_partition(&request);
    let assignment = resolve_namespace_group(&runtime, &request.fragment, partition_id).await?;
    let voter = most_current_reachable_voter(&runtime, &assignment).await?;
    if voter.id == runtime.local_node_id {
        return Ok(None);
    }
    let client = peer_http_client()?;
    let mut builder = client
        .post(namespace_read_peer_url(&voter.endpoint))
        .json(&request);
    if let Some(secret) = cluster_secret() {
        builder = builder.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(Duration::from_secs(20), builder.send())
        .await
        .map_err(|_| AppError::ServiceUnavailable("namespace voter read timed out".into()))?
        .map_err(|err| {
            AppError::ServiceUnavailable(format!("namespace voter read failed: {err}"))
        })?;
    if !response.status().is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "namespace voter read returned {}",
            response.status()
        )));
    }
    response
        .json::<PeerNamespaceReadResponse>()
        .await
        .map(Some)
        .map_err(|err| {
            AppError::InternalServerError(format!("decode namespace voter read response: {err}"))
        })
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
    record.partition_id = namespace_request_partition(request);
    Ok(record)
}

fn namespace_request_ledger_path(
    fragment: &str,
    partition_id: u16,
) -> Result<std::path::PathBuf, AppError> {
    Ok(namespace_ledger_root()?
        .join(fragment)
        .join(format!("{partition_id:04}.requests.jsonl")))
}

fn namespace_prepared_path(
    fragment: &str,
    partition_id: u16,
    mutation_id: &str,
) -> Result<std::path::PathBuf, AppError> {
    Ok(namespace_ledger_root()?
        .join("prepared")
        .join(fragment)
        .join(format!("{partition_id:04}"))
        .join(format!("{mutation_id}.json")))
}

fn namespace_snapshot_path(fragment: &str, partition_id: u16) -> Result<PathBuf, AppError> {
    Ok(namespace_ledger_root()?
        .join("snapshots")
        .join(fragment)
        .join(format!("{partition_id:04}.json")))
}

fn remove_path_no_follow(path: &FsPath) -> Result<(), AppError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(AppError::Io(
                err,
                format!("inspect namespace snapshot path {}", path.display()),
            ));
        }
    };
    if metadata.file_type().is_symlink() || metadata.is_file() {
        fs::remove_file(path).map_err(|err| {
            AppError::Io(
                err,
                format!("remove namespace snapshot path {}", path.display()),
            )
        })
    } else {
        fs::remove_dir_all(path).map_err(|err| {
            AppError::Io(
                err,
                format!("remove namespace snapshot tree {}", path.display()),
            )
        })
    }
}

fn scan_snapshot_tree(
    root: &FsPath,
    fragment: &str,
    base: &FsPath,
    trashed: bool,
    relative: &str,
    entries: &mut Vec<NamespaceSnapshotEntry>,
) -> Result<(), AppError> {
    let dir = if relative.is_empty() {
        base.to_path_buf()
    } else {
        base.join(relative)
    };
    let children = match fs::read_dir(&dir) {
        Ok(children) => children,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(AppError::Io(
                err,
                format!("scan namespace snapshot directory {}", dir.display()),
            ));
        }
    };
    for child in children {
        let child = child.map_err(|err| {
            AppError::Io(
                err,
                format!("scan namespace snapshot directory {}", dir.display()),
            )
        })?;
        let name = child.file_name().to_string_lossy().to_string();
        if !trashed && internal_namespace_entry_name(&name) {
            continue;
        }
        let child_relative = if relative.is_empty() {
            name
        } else {
            format!("{relative}/{name}")
        };
        let path = child.path();
        let metadata = fs::symlink_metadata(&path).map_err(|err| {
            AppError::Io(
                err,
                format!("inspect namespace snapshot entry {}", path.display()),
            )
        })?;
        if metadata.file_type().is_symlink() {
            let object_id =
                read_namespace_symlink_object_id(root, fragment, &path)?.ok_or_else(|| {
                    AppError::InternalServerError(format!(
                        "namespace link {} is not a canonical @o_ pointer",
                        path.display()
                    ))
                })?;
            entries.push(NamespaceSnapshotEntry {
                path: child_relative,
                trashed,
                directory: false,
                object_id: Some(object_id),
            });
        } else if metadata.is_dir() {
            entries.push(NamespaceSnapshotEntry {
                path: child_relative.clone(),
                trashed,
                directory: true,
                object_id: None,
            });
            scan_snapshot_tree(root, fragment, base, trashed, &child_relative, entries)?;
        }
    }
    Ok(())
}

fn build_namespace_snapshot(fragment: &str, partition_id: u16) -> Result<NamespaceSnapshot, AppError> {
    let requests = read_request_ledger(fragment, partition_id)?;
    let mut entries = Vec::new();
    let roots = ObjectPathManager::new().get_all_namespace_roots(fragment);
    if roots.is_empty() {
        return Err(AppError::InternalServerError(
            "N2 namespace has no configured local root".into(),
        ));
    }
    for root in &roots {
        let fragment_root = fragment_root_on(root, fragment);
        scan_snapshot_tree(root, fragment, &fragment_root, false, "", &mut entries)?;
        scan_snapshot_tree(
            root,
            fragment,
            &fragment_root.join(TRASH_DIR),
            true,
            "",
            &mut entries,
        )?;
    }
    entries.sort_by(|left, right| {
        left.trashed
            .cmp(&right.trashed)
            .then(left.path.cmp(&right.path))
            .then(left.directory.cmp(&right.directory))
            .then(left.object_id.cmp(&right.object_id))
    });
    entries.dedup_by(|left, right| {
        left.trashed == right.trashed
            && left.path == right.path
            && left.directory == right.directory
            && left.object_id == right.object_id
    });
    Ok(NamespaceSnapshot {
        format_version: 1,
        fragment: fragment.to_string(),
        partition_id,
        committed_log_index: requests.len() as u64,
        requests,
        entries,
    })
}

fn namespace_restore_roots(
    fragment: &str,
    path: &str,
    directory: bool,
) -> Result<Vec<PathBuf>, AppError> {
    let manager = ObjectPathManager::new();
    let roots = if directory {
        manager.get_all_namespace_roots(fragment)
    } else {
        manager.get_namespace_roots_for_path(fragment, path)
    };
    if roots.is_empty() {
        return Err(AppError::InternalServerError(
            "N2 namespace has no configured local root".into(),
        ));
    }
    Ok(roots)
}

fn namespace_snapshot_roots(fragment: &str) -> Result<Vec<PathBuf>, AppError> {
    let roots = ObjectPathManager::new().get_all_namespace_roots(fragment);
    if roots.is_empty() {
        return Err(AppError::InternalServerError(
            "N2 namespace has no configured local root".into(),
        ));
    }
    Ok(roots)
}

fn clear_namespace_snapshot_roots(fragment: &str) -> Result<(), AppError> {
    for root in namespace_snapshot_roots(fragment)? {
        let fragment_root = fragment_root_on(&root, fragment);
        fs::create_dir_all(&fragment_root).map_err(|err| {
            AppError::Io(
                err,
                format!("create namespace root {}", fragment_root.display()),
            )
        })?;
        for child in fs::read_dir(&fragment_root).map_err(|err| {
            AppError::Io(
                err,
                format!("scan namespace root {}", fragment_root.display()),
            )
        })? {
            let child = child.map_err(|err| {
                AppError::Io(
                    err,
                    format!("scan namespace root {}", fragment_root.display()),
                )
            })?;
            let name = child.file_name().to_string_lossy().to_string();
            if internal_namespace_entry_name(&name) {
                continue;
            }
            remove_path_no_follow(&child.path())?;
        }
        remove_path_no_follow(&fragment_root.join(TRASH_DIR))?;
        fs::create_dir_all(fragment_root.join(TRASH_DIR))
            .map_err(|err| AppError::Io(err, "create namespace trash snapshot root".into()))?;
    }
    Ok(())
}

fn restore_namespace_snapshot_entry(
    fragment: &str,
    entry: NamespaceSnapshotEntry,
) -> Result<(), AppError> {
    let object_id = if entry.directory {
        None
    } else {
        Some(entry.object_id.ok_or_else(|| {
            AppError::InternalServerError("namespace snapshot file lacks object_id".into())
        })?)
    };
    for root in namespace_restore_roots(fragment, &entry.path, entry.directory)? {
        let fragment_root = fragment_root_on(&root, fragment);
        let base = if entry.trashed {
            fragment_root.join(TRASH_DIR)
        } else {
            fragment_root
        };
        let path = base.join(&entry.path);
        if entry.directory {
            fs::create_dir_all(&path).map_err(|err| {
                AppError::Io(
                    err,
                    format!("restore namespace directory {}", path.display()),
                )
            })?;
            continue;
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                AppError::Io(
                    err,
                    format!("restore namespace parent {}", parent.display()),
                )
            })?;
        }
        unix_fs::symlink(format!("@o_{}", object_id.as_deref().unwrap_or_default()), &path)
            .map_err(|err| {
                AppError::Io(err, format!("restore namespace link {}", path.display()))
            })?;
    }
    Ok(())
}

fn write_request_ledger_atomic(
    fragment: &str,
    partition_id: u16,
    requests: &[PeerNamespaceApplyRequest],
) -> Result<(), AppError> {
    let path = namespace_request_ledger_path(fragment, partition_id)?;
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError("namespace request ledger has no parent".into())
    })?;
    fs::create_dir_all(parent).map_err(|err| {
        AppError::Io(
            err,
            format!("create namespace request ledger {}", parent.display()),
        )
    })?;
    let tmp = parent.join(format!(".{fragment}.{}.snapshot.tmp", Ulid::new()));
    {
        let mut file = fs::File::create(&tmp).map_err(|err| {
            AppError::Io(
                err,
                format!("create namespace request ledger {}", tmp.display()),
            )
        })?;
        for request in requests {
            let bytes = serde_json::to_vec(request).map_err(|err| {
                AppError::InternalServerError(format!("encode namespace snapshot request: {err}"))
            })?;
            file.write_all(&bytes)
                .and_then(|_| file.write_all(b"\n"))
                .map_err(|err| {
                    AppError::Io(
                        err,
                        format!("write namespace request ledger {}", tmp.display()),
                    )
                })?;
        }
        file.sync_all().map_err(|err| {
            AppError::Io(
                err,
                format!("sync namespace request ledger {}", tmp.display()),
            )
        })?;
    }
    fs::rename(&tmp, &path).map_err(|err| {
        AppError::Io(
            err,
            format!("publish namespace request ledger {}", path.display()),
        )
    })?;
    Ok(())
}

fn persist_namespace_snapshot(snapshot: &NamespaceSnapshot) -> Result<(), AppError> {
    let path = namespace_snapshot_path(&snapshot.fragment, snapshot.partition_id)?;
    let parent = path
        .parent()
        .ok_or_else(|| AppError::InternalServerError("namespace snapshot has no parent".into()))?;
    fs::create_dir_all(parent).map_err(|err| {
        AppError::Io(
            err,
            format!("create namespace snapshot directory {}", parent.display()),
        )
    })?;
    let bytes = serde_json::to_vec(snapshot).map_err(|err| {
        AppError::InternalServerError(format!("encode namespace snapshot: {err}"))
    })?;
    let tmp = parent.join(format!(".{}.snapshot.tmp", Ulid::new()));
    {
        let mut file = fs::File::create(&tmp).map_err(|err| {
            AppError::Io(err, format!("create namespace snapshot {}", tmp.display()))
        })?;
        file.write_all(&bytes).map_err(|err| {
            AppError::Io(err, format!("write namespace snapshot {}", tmp.display()))
        })?;
        file.sync_all().map_err(|err| {
            AppError::Io(err, format!("sync namespace snapshot {}", tmp.display()))
        })?;
    }
    fs::rename(&tmp, &path).map_err(|err| {
        AppError::Io(
            err,
            format!("publish namespace snapshot {}", path.display()),
        )
    })?;
    Ok(())
}

fn maybe_persist_namespace_snapshot(fragment: &str, partition_id: u16) -> Result<(), AppError> {
    let interval = std::env::var("N2_NAMESPACE_SNAPSHOT_INTERVAL")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(1_024)
        .max(1);
    let position = read_request_ledger(fragment, partition_id)?.len() as u64;
    if position == 0 || position % interval != 0 {
        return Ok(());
    }
    let snapshot = build_namespace_snapshot(fragment, partition_id)?;
    persist_namespace_snapshot(&snapshot)
}

fn apply_namespace_snapshot(snapshot: &NamespaceSnapshot) -> Result<(), AppError> {
    if snapshot.format_version != 1
        || snapshot.committed_log_index != snapshot.requests.len() as u64
    {
        return Err(AppError::Conflict(
            "namespace snapshot format or committed position is invalid".into(),
        ));
    }
    clear_namespace_snapshot_roots(&snapshot.fragment)?;

    let mut entries = snapshot.entries.clone();
    entries.sort_by_key(|entry| {
        (
            !entry.directory,
            entry.path.split('/').count(),
            entry.trashed,
            entry.path.clone(),
        )
    });
    for entry in entries {
        restore_namespace_snapshot_entry(&snapshot.fragment, entry)?;
    }
    write_request_ledger_atomic(&snapshot.fragment, snapshot.partition_id, &snapshot.requests)?;
    persist_namespace_snapshot(snapshot)?;
    Ok(())
}

fn read_request_ledger(fragment: &str, partition_id: u16) -> Result<Vec<PeerNamespaceApplyRequest>, AppError> {
    let path = namespace_request_ledger_path(fragment, partition_id)?;
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
    Ok(out)
}

pub fn namespace_local_ledger_summary(fragment: &str, partition_id: u16) -> Result<NamespaceLedgerSummary, AppError> {
    let path = namespace_request_ledger_path(fragment, partition_id)?;
    let requests = read_request_ledger(fragment, partition_id)?;
    let last = requests.last();
    Ok(NamespaceLedgerSummary {
        fragment: fragment.to_string(),
        partition_id,
        local_requests: requests.len() as u64,
        committed_log_index: requests.len() as u64,
        last_mutation_id: last.and_then(|request| request.mutation_id.clone()),
        last_kind: last.map(|request| request.kind.clone()),
        last_source_path: last.and_then(|request| request.source_path.clone()),
        last_destination_path: last.and_then(|request| request.destination_path.clone()),
        last_object_id: last.and_then(|request| request.object_id.clone()),
        last_version_id: last.and_then(|request| request.version_id.clone()),
        request_ledger_path: path.to_string_lossy().to_string(),
    })
}

fn request_seen(fragment: &str, partition_id: u16, mutation_id: Option<&str>) -> Result<bool, AppError> {
    let Some(mutation_id) = mutation_id.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(false);
    };
    Ok(read_request_ledger(fragment, partition_id)?.iter().any(|request| {
        request
            .mutation_id
            .as_deref()
            .is_some_and(|value| value == mutation_id)
    }))
}

fn append_request_ledger(request: &PeerNamespaceApplyRequest) -> Result<(), AppError> {
    let _ = append_namespace_mutation(namespace_ledger_record(request)?)?;
    let path = namespace_request_ledger_path(&request.fragment, namespace_request_partition(request))?;
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
    file.sync_data().map_err(|e| {
        AppError::Io(
            e,
            format!("sync N2 namespace request ledger {}", path.display()),
        )
    })?;
    Ok(())
}

fn persist_prepared_mutation(prepared: &PreparedNamespaceMutation) -> Result<(), AppError> {
    let mutation_id = prepared
        .request
        .mutation_id
        .as_deref()
        .ok_or_else(|| AppError::BadRequest("prepared mutation requires mutation_id".into()))?;
    let path = namespace_prepared_path(
        &prepared.request.fragment,
        prepared.assignment.partition_id,
        mutation_id,
    )?;
    let bytes = serde_json::to_vec(prepared).map_err(|err| {
        AppError::InternalServerError(format!("encode prepared namespace mutation: {err}"))
    })?;
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError("prepared namespace path has no parent".into())
    })?;
    fs::create_dir_all(parent).map_err(|err| {
        AppError::Io(
            err,
            format!("create prepared namespace directory {}", parent.display()),
        )
    })?;
    let tmp = parent.join(format!(".{mutation_id}.{}.tmp", Ulid::new()));
    {
        let mut file = fs::File::create(&tmp).map_err(|err| {
            AppError::Io(
                err,
                format!("create prepared namespace mutation {}", tmp.display()),
            )
        })?;
        file.write_all(&bytes).map_err(|err| {
            AppError::Io(
                err,
                format!("write prepared namespace mutation {}", tmp.display()),
            )
        })?;
        file.sync_all().map_err(|err| {
            AppError::Io(
                err,
                format!("sync prepared namespace mutation {}", tmp.display()),
            )
        })?;
    }
    fs::rename(&tmp, &path).map_err(|err| {
        AppError::Io(
            err,
            format!("publish prepared namespace mutation {}", path.display()),
        )
    })?;
    if let Ok(dir) = fs::File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(())
}

fn load_prepared_mutation(
    fragment: &str,
    partition_id: u16,
    mutation_id: &str,
) -> Result<Option<PreparedNamespaceMutation>, AppError> {
    let path = namespace_prepared_path(fragment, partition_id, mutation_id)?;
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(AppError::Io(
                err,
                format!("read prepared namespace mutation {}", path.display()),
            ));
        }
    };
    serde_json::from_slice(&bytes).map(Some).map_err(|err| {
        AppError::InternalServerError(format!(
            "decode prepared namespace mutation {}: {err}",
            path.display()
        ))
    })
}

fn prepared_mutation_for_log_index(
    fragment: &str,
    partition_id: u16,
    log_index: u64,
) -> Result<Option<PreparedNamespaceMutation>, AppError> {
    let dir = namespace_ledger_root()?
        .join("prepared")
        .join(fragment)
        .join(format!("{partition_id:04}"));
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(AppError::Io(
                err,
                format!("scan prepared namespace directory {}", dir.display()),
            ));
        }
    };
    for entry in entries.filter_map(Result::ok) {
        let bytes = fs::read(entry.path()).map_err(|err| {
            AppError::Io(
                err,
                format!(
                    "read prepared namespace mutation {}",
                    entry.path().display()
                ),
            )
        })?;
        let prepared =
            serde_json::from_slice::<PreparedNamespaceMutation>(&bytes).map_err(|err| {
                AppError::InternalServerError(format!(
                    "decode prepared namespace mutation {}: {err}",
                    entry.path().display()
                ))
            })?;
        if prepared.log_index == log_index {
            return Ok(Some(prepared));
        }
    }
    Ok(None)
}

fn remove_prepared_mutation(fragment: &str, partition_id: u16, mutation_id: &str) {
    if let Ok(path) = namespace_prepared_path(fragment, partition_id, mutation_id) {
        let _ = fs::remove_file(path);
    }
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
        }
    }
    Ok(())
}

async fn post_namespace_prepare(
    endpoint: &str,
    prepared: &PreparedNamespaceMutation,
) -> Result<(), AppError> {
    let client = peer_http_client()?;
    let mut req = client
        .post(namespace_prepare_peer_url(endpoint))
        .json(prepared);
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(NAMESPACE_PEER_APPLY_TIMEOUT, req.send())
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!("namespace prepare timed out at {endpoint}"))
        })?
        .map_err(|e| {
            AppError::ServiceUnavailable(format!("namespace prepare failed at {endpoint}: {e}"))
        })?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "namespace prepare returned {} from {}: {}",
            status, endpoint, text
        )));
    }
    Ok(())
}

async fn post_namespace_commit(
    endpoint: &str,
    prepared: &PreparedNamespaceMutation,
) -> Result<NamespaceApplyOutcome, AppError> {
    let client = peer_http_client()?;
    let mut req = client
        .post(namespace_commit_peer_url(endpoint))
        .json(prepared);
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(NAMESPACE_PEER_APPLY_TIMEOUT, req.send())
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!("namespace commit timed out at {endpoint}"))
        })?
        .map_err(|err| {
            AppError::ServiceUnavailable(format!("namespace commit failed at {endpoint}: {err}"))
        })?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "namespace commit returned {status} from {endpoint}: {body}"
        )));
    }
    let payload = response.json::<serde_json::Value>().await.map_err(|err| {
        AppError::InternalServerError(format!(
            "decode namespace commit response from {endpoint}: {err}"
        ))
    })?;
    serde_json::from_value(
        payload
            .get("outcome")
            .cloned()
            .unwrap_or(serde_json::Value::Null),
    )
    .map_err(|err| {
        AppError::InternalServerError(format!(
            "decode namespace commit outcome from {endpoint}: {err}"
        ))
    })
}

async fn post_namespace_abort(
    endpoint: &str,
    prepared: &PreparedNamespaceMutation,
) -> Result<(), AppError> {
    let client = peer_http_client()?;
    let mut req = client
        .post(namespace_abort_peer_url(endpoint))
        .json(prepared);
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(NAMESPACE_PEER_APPLY_TIMEOUT, req.send())
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!("namespace abort timed out at {endpoint}"))
        })?
        .map_err(|err| {
            AppError::ServiceUnavailable(format!("namespace abort failed at {endpoint}: {err}"))
        })?;
    if !response.status().is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "namespace abort returned {} from {}",
            response.status(),
            endpoint
        )));
    }
    Ok(())
}

async fn fetch_namespace_prepared(
    endpoint: &str,
    fragment: &str,
    partition_id: u16,
    log_index: u64,
) -> Result<Option<PreparedNamespaceMutation>, AppError> {
    let client = peer_http_client()?;
    let mut request =
        client.get(namespace_prepared_peer_url(endpoint, fragment, partition_id, log_index));
    if let Some(secret) = cluster_secret() {
        request = request.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(NAMESPACE_PEER_APPLY_TIMEOUT, request.send())
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "namespace prepared-state fetch timed out at {endpoint}"
            ))
        })?
        .map_err(|err| {
            AppError::ServiceUnavailable(format!(
                "namespace prepared-state fetch failed at {endpoint}: {err}"
            ))
        })?;
    if !response.status().is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "namespace prepared-state fetch returned {} from {}",
            response.status(),
            endpoint
        )));
    }
    response.json().await.map_err(|err| {
        AppError::InternalServerError(format!(
            "decode namespace prepared state from {endpoint}: {err}"
        ))
    })
}

async fn abort_prepared_on_voters(
    prepared: &PreparedNamespaceMutation,
    voters: &[crate::distribution::membership::ClusterNode],
    local_node_id: &str,
) {
    if let Some(mutation_id) = prepared.request.mutation_id.as_deref() {
        remove_prepared_mutation(
            &prepared.request.fragment,
            prepared.assignment.partition_id,
            mutation_id,
        );
    }
    for node in voters.iter().filter(|node| node.id != local_node_id) {
        if let Err(err) = post_namespace_abort(&node.endpoint, prepared).await {
            warn!(
                "namespace abort {} did not reach voter {}: {}",
                prepared.request.mutation_id.as_deref().unwrap_or("unknown"),
                node.id,
                err
            );
        }
    }
}

async fn recover_prepared_slot(
    runtime: &crate::distribution::membership::ClusterRuntime,
    assignment: &NamespaceGroupAssignment,
    voters: &[crate::distribution::membership::ClusterNode],
) -> Result<bool, AppError> {
    let partition_id = assignment.partition_id;
    let log_index = read_request_ledger(&assignment.fragment, partition_id)?.len() as u64 + 1;
    let mut candidates: HashMap<String, (PreparedNamespaceMutation, usize)> = HashMap::new();
    for node in voters {
        let prepared = if node.id == runtime.local_node_id {
            prepared_mutation_for_log_index(&assignment.fragment, partition_id, log_index)?
        } else {
            fetch_namespace_prepared(&node.endpoint, &assignment.fragment, partition_id, log_index)
                .await
                .unwrap_or(None)
        };
        let Some(prepared) = prepared else {
            continue;
        };
        let Some(mutation_id) = prepared.request.mutation_id.clone() else {
            continue;
        };
        let entry = candidates
            .entry(mutation_id)
            .or_insert_with(|| (prepared.clone(), 0));
        entry.1 = entry.1.saturating_add(1);
    }
    if candidates.is_empty() {
        return Ok(false);
    }

    let required = required_quorum(voters.len());
    let (selected_id, (selected, prepared_votes)) = candidates
        .iter()
        .max_by(|(left_id, (_, left_votes)), (right_id, (_, right_votes))| {
            left_votes
                .cmp(right_votes)
                .then_with(|| right_id.cmp(left_id))
        })
        .map(|(id, value)| (id.clone(), value.clone()))
        .expect("candidate map is not empty");

    if prepared_votes < required {
        for (_, (candidate, _)) in candidates {
            abort_prepared_on_voters(&candidate, voters, &runtime.local_node_id).await;
        }
        return Ok(false);
    }
    if selected.assignment != *assignment || selected.log_index != log_index {
        return Err(AppError::Conflict(
            "durable prepared namespace mutation has an incompatible assignment".into(),
        ));
    }

    if let Some(local) = prepared_mutation_for_log_index(&assignment.fragment, partition_id, log_index)?
        && local.request.mutation_id.as_deref() != Some(selected_id.as_str())
        && let Some(mutation_id) = local.request.mutation_id.as_deref()
    {
        remove_prepared_mutation(&assignment.fragment, partition_id, mutation_id);
    }
    persist_prepared_mutation(&selected)?;

    let mut prepare_acks = 1usize;
    for node in voters
        .iter()
        .filter(|node| node.id != runtime.local_node_id)
    {
        if post_namespace_prepare(&node.endpoint, &selected)
            .await
            .is_ok()
        {
            prepare_acks = prepare_acks.saturating_add(1);
        }
    }
    if prepare_acks < required {
        abort_prepared_on_voters(&selected, voters, &runtime.local_node_id).await;
        return Err(AppError::ServiceUnavailable(format!(
            "N2 could not recover prepared namespace mutation {selected_id}: {prepare_acks}/{required} voters acknowledged"
        )));
    }

    if !request_seen(
        &selected.request.fragment,
        partition_id,
        selected.request.mutation_id.as_deref(),
    )? {
        materialize_namespace_request(&selected.request).await?;
        append_request_ledger(&selected.request)?;
        maybe_persist_namespace_snapshot(&selected.request.fragment, partition_id)?;
    }
    remove_prepared_mutation(&assignment.fragment, partition_id, &selected_id);
    for node in voters
        .iter()
        .filter(|node| node.id != runtime.local_node_id)
    {
        if let Err(err) = post_namespace_commit(&node.endpoint, &selected).await {
            warn!(
                "recovered namespace mutation {} is committed locally but voter {} remains behind: {}",
                selected_id, node.id, err
            );
        }
    }
    Ok(true)
}

async fn post_namespace_coordinate(
    endpoint: &str,
    request: &PeerNamespaceApplyRequest,
) -> Result<NamespaceApplyOutcome, AppError> {
    let client = peer_http_client()?;
    let mut req = client
        .post(namespace_coordinate_peer_url(endpoint))
        .json(request);
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(Duration::from_secs(20), req.send())
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!("namespace coordinator timed out at {endpoint}"))
        })?
        .map_err(|err| {
            AppError::ServiceUnavailable(format!(
                "namespace coordinator failed at {endpoint}: {err}"
            ))
        })?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "namespace coordinator returned {status}: {body}"
        )));
    }
    response
        .json::<NamespaceApplyOutcome>()
        .await
        .map_err(|err| {
            AppError::InternalServerError(format!("decode namespace coordinator outcome: {err}"))
        })
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
            let committed = global_namespace_runtime()
                .finalize_logical_upload(&request.fragment, logical_path, object_id, metadata)
                .await?;
            Ok(NamespaceApplyOutcome::CommittedObject(committed))
        }
    }
}

async fn local_namespace_entry(
    fragment: &str,
    path: &str,
    trashed: bool,
) -> Result<Option<NamespaceEntry>, AppError> {
    FORCE_LOCAL_NAMESPACE_READ
        .scope(true, async {
            if trashed {
                global_namespace_runtime()
                    .trashed_entry(fragment, path)
                    .await
            } else {
                global_namespace_runtime().entry(fragment, path).await
            }
        })
        .await
}

async fn local_trash_is_empty(fragment: &str) -> Result<bool, AppError> {
    FORCE_LOCAL_NAMESPACE_READ
        .scope(true, async {
            global_namespace_runtime()
                .list_dir_page(fragment, "", true, 1, None)
                .await
                .map(|(entries, _)| entries.is_empty())
        })
        .await
}

async fn namespace_request_materialized(
    request: &PeerNamespaceApplyRequest,
) -> Result<bool, AppError> {
    let source = request.source_path.as_deref().unwrap_or_default();
    match request.kind {
        NamespaceMutationKind::Mkdir => Ok(local_namespace_entry(&request.fragment, source, false)
            .await?
            .is_some_and(|entry| {
                entry.kind == crate::namespace::models::NamespaceEntryKind::Directory
            })),
        NamespaceMutationKind::Rename => {
            let destination = request.destination_path.as_deref().unwrap_or_default();
            Ok(local_namespace_entry(&request.fragment, source, false)
                .await?
                .is_none()
                && local_namespace_entry(&request.fragment, destination, false)
                    .await?
                    .is_some())
        }
        NamespaceMutationKind::Trash => Ok(local_namespace_entry(&request.fragment, source, false)
            .await?
            .is_none()
            && local_namespace_entry(&request.fragment, source, true)
                .await?
                .is_some()),
        NamespaceMutationKind::Recover => {
            Ok(local_namespace_entry(&request.fragment, source, false)
                .await?
                .is_some()
                && local_namespace_entry(&request.fragment, source, true)
                    .await?
                    .is_none())
        }
        NamespaceMutationKind::RecoverAll | NamespaceMutationKind::EmptyTrash => {
            local_trash_is_empty(&request.fragment).await
        }
        NamespaceMutationKind::Purge => Ok(local_namespace_entry(&request.fragment, source, true)
            .await?
            .is_none()),
        NamespaceMutationKind::QuotaEdit => Ok(true),
        NamespaceMutationKind::UploadCommit => {
            let Some(expected_object_id) = request.object_id.as_deref() else {
                return Ok(false);
            };
            let actual = FORCE_LOCAL_NAMESPACE_READ
                .scope(true, async {
                    global_namespace_runtime()
                        .resolve_live_object_id(&request.fragment, source)
                        .await
                })
                .await?;
            Ok(actual.as_deref() == Some(expected_object_id))
        }
    }
}

async fn materialize_namespace_request(
    request: &PeerNamespaceApplyRequest,
) -> Result<NamespaceApplyOutcome, AppError> {
    if namespace_request_materialized(request).await? {
        return Ok(NamespaceApplyOutcome::None);
    }
    apply_namespace_request_core(request).await
}

async fn apply_namespace_request(
    request: &PeerNamespaceApplyRequest,
) -> Result<NamespaceApplyOutcome, AppError> {
    let partition_id = namespace_request_partition(request);
    if request_seen(&request.fragment, partition_id, request.mutation_id.as_deref())? {
        return Ok(NamespaceApplyOutcome::None);
    }
    let lock = namespace_partition_commit_lock(&request.fragment, partition_id)?;
    let _guard = lock.lock().await;
    if request_seen(&request.fragment, partition_id, request.mutation_id.as_deref())? {
        return Ok(NamespaceApplyOutcome::None);
    }
    let outcome = materialize_namespace_request(request).await?;
    append_request_ledger(request)?;
    maybe_persist_namespace_snapshot(&request.fragment, partition_id)?;
    Ok(outcome)
}

fn validate_prepared_assignment(
    prepared: &PreparedNamespaceMutation,
    local_node_id: &str,
) -> Result<(), AppError> {
    if prepared.assignment.fragment != prepared.request.fragment {
        return Err(AppError::BadRequest(
            "prepared namespace assignment has the wrong fragment".into(),
        ));
    }
    if prepared.assignment.partition_id != namespace_request_partition(&prepared.request) {
        return Err(AppError::BadRequest(
            "prepared namespace assignment has the wrong partition".into(),
        ));
    }
    if !prepared
        .assignment
        .voter_node_ids
        .iter()
        .any(|node_id| node_id == local_node_id)
    {
        return Err(AppError::Forbidden(
            "this node is not a voter for the fragment namespace".into(),
        ));
    }
    if let Some(existing) = load_namespace_group(
        &prepared.request.fragment,
        prepared.assignment.partition_id,
    )? {
        if existing != prepared.assignment {
            return Err(AppError::Conflict(
                "namespace group assignment differs from durable local assignment".into(),
            ));
        }
    } else {
        persist_namespace_group(&prepared.assignment)?;
    }
    Ok(())
}

async fn coordinate_namespace_commit(
    request: PeerNamespaceApplyRequest,
) -> Result<NamespaceApplyOutcome, AppError> {
    let runtime = active_cluster_runtime()?;
    let partition_id = namespace_request_partition(&request);
    let assignment = resolve_namespace_group(&runtime, &request.fragment, partition_id).await?;
    let coordinator = most_current_reachable_voter(&runtime, &assignment).await?;
    if coordinator.id != runtime.local_node_id {
        return post_namespace_coordinate(&coordinator.endpoint, &request).await;
    }

    let lock = namespace_partition_commit_lock(&request.fragment, partition_id)?;
    let _guard = lock.lock().await;
    let voters = assignment_nodes(&runtime, &assignment)?;
    while recover_prepared_slot(&runtime, &assignment, &voters).await? {}
    if request_seen(&request.fragment, partition_id, request.mutation_id.as_deref())? {
        return Ok(NamespaceApplyOutcome::None);
    }
    validate_namespace_request(&request)?;

    let required = required_quorum(voters.len());
    let prepared = PreparedNamespaceMutation {
        log_index: read_request_ledger(&request.fragment, partition_id)?.len() as u64 + 1,
        assignment: assignment.clone(),
        request,
    };
    validate_prepared_assignment(&prepared, &runtime.local_node_id)?;
    if let Some(existing) =
        prepared_mutation_for_log_index(&prepared.request.fragment, partition_id, prepared.log_index)?
    {
        if existing.request.mutation_id != prepared.request.mutation_id {
            return Err(AppError::Conflict(format!(
                "namespace log index {} is already prepared by mutation {}",
                prepared.log_index,
                existing.request.mutation_id.as_deref().unwrap_or("unknown")
            )));
        }
    }
    persist_prepared_mutation(&prepared)?;

    let mut prepared_count = 1usize;
    let mut failures = Vec::new();
    for node in voters
        .iter()
        .filter(|node| node.id != runtime.local_node_id)
    {
        match post_namespace_prepare(&node.endpoint, &prepared).await {
            Ok(()) => prepared_count = prepared_count.saturating_add(1),
            Err(err) => failures.push(format!("{}: {}", node.id, err)),
        }
    }
    if prepared_count < required {
        abort_prepared_on_voters(&prepared, &voters, &runtime.local_node_id).await;
        return Err(AppError::ServiceUnavailable(format!(
            "N2 namespace prepare quorum failed: {} prepared, {} required; {}",
            prepared_count,
            required,
            failures.join("; ")
        )));
    }

    let outcome = match materialize_namespace_request(&prepared.request).await {
        Ok(outcome) => outcome,
        Err(err) => {
            abort_prepared_on_voters(&prepared, &voters, &runtime.local_node_id).await;
            return Err(err);
        }
    };
    append_request_ledger(&prepared.request)?;
    maybe_persist_namespace_snapshot(&prepared.request.fragment, partition_id)?;
    if let Some(mutation_id) = prepared.request.mutation_id.as_deref() {
        remove_prepared_mutation(&prepared.request.fragment, partition_id, mutation_id);
    }

    let mut committed = 1usize;
    for node in voters
        .iter()
        .filter(|node| node.id != runtime.local_node_id)
    {
        match post_namespace_commit(&node.endpoint, &prepared).await {
            Ok(_) => committed = committed.saturating_add(1),
            Err(err) => warn!(
                "namespace commit {} is durable on the leader but node {} is lagging: {}",
                prepared.request.mutation_id.as_deref().unwrap_or("unknown"),
                node.id,
                err
            ),
        }
    }
    if committed < required {
        warn!(
            "namespace mutation {} committed on the leader after prepare quorum, but only {committed}/{required} voters acknowledged apply",
            prepared.request.mutation_id.as_deref().unwrap_or("unknown")
        );
    }
    Ok(outcome)
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
    coordinate_namespace_commit(request).await
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
    let _ = commit_namespace_quorum(request).await?;
    Ok(())
}

pub async fn peer_namespace_group(
    headers: HeaderMap,
    Path((fragment, partition_id)): Path<(String, u16)>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    authorize_peer(&headers)?;
    let assignment = load_namespace_group(&fragment, partition_id)?.ok_or(AppError::NotFound)?;
    Ok(Json(assignment))
}

async fn peer_prepare_namespace(
    headers: HeaderMap,
    AxumJson(prepared): AxumJson<PreparedNamespaceMutation>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    authorize_peer(&headers)?;
    validate_namespace_request(&prepared.request)?;
    let runtime = active_cluster_runtime()?;
    validate_prepared_assignment(&prepared, &runtime.local_node_id)?;
    let partition_id = prepared.assignment.partition_id;
    let lock = namespace_partition_commit_lock(&prepared.request.fragment, partition_id)?;
    let _guard = lock.lock().await;
    if request_seen(
        &prepared.request.fragment,
        partition_id,
        prepared.request.mutation_id.as_deref(),
    )? {
        return Ok(StatusCode::NO_CONTENT);
    }
    let expected = read_request_ledger(&prepared.request.fragment, partition_id)?.len() as u64 + 1;
    if prepared.log_index != expected {
        return Err(AppError::Conflict(format!(
            "namespace prepare index {} does not follow local committed index {}",
            prepared.log_index,
            expected.saturating_sub(1)
        )));
    }
    if let Some(existing) =
        prepared_mutation_for_log_index(&prepared.request.fragment, partition_id, prepared.log_index)?
    {
        if existing.request.mutation_id == prepared.request.mutation_id {
            return Ok(StatusCode::NO_CONTENT);
        }
        return Err(AppError::Conflict(format!(
            "namespace log index {} is already prepared by mutation {}",
            prepared.log_index,
            existing.request.mutation_id.as_deref().unwrap_or("unknown")
        )));
    }
    if let Some(mutation_id) = prepared.request.mutation_id.as_deref()
        && let Some(existing) = load_prepared_mutation(&prepared.request.fragment, partition_id, mutation_id)?
    {
        if existing.log_index == prepared.log_index
            && existing.request.mutation_id == prepared.request.mutation_id
        {
            return Ok(StatusCode::NO_CONTENT);
        }
        return Err(AppError::Conflict(
            "namespace mutation id already has a different prepared record".into(),
        ));
    }
    persist_prepared_mutation(&prepared)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn peer_commit_namespace(
    headers: HeaderMap,
    AxumJson(prepared): AxumJson<PreparedNamespaceMutation>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    validate_prepared_assignment(&prepared, &runtime.local_node_id)?;
    let partition_id = prepared.assignment.partition_id;
    let lock = namespace_partition_commit_lock(&prepared.request.fragment, partition_id)?;
    let _guard = lock.lock().await;
    if request_seen(
        &prepared.request.fragment,
        partition_id,
        prepared.request.mutation_id.as_deref(),
    )? {
        return Ok((
            StatusCode::OK,
            Json(serde_json::json!({"applied": true, "outcome": NamespaceApplyOutcome::None})),
        ));
    }
    let mutation_id = prepared
        .request
        .mutation_id
        .as_deref()
        .ok_or_else(|| AppError::BadRequest("namespace commit requires mutation_id".into()))?;
    let durable = load_prepared_mutation(&prepared.request.fragment, partition_id, mutation_id)?
        .ok_or_else(|| AppError::Conflict("namespace mutation was not prepared locally".into()))?;
    if durable.log_index != prepared.log_index
        || durable.request.mutation_id != prepared.request.mutation_id
    {
        return Err(AppError::Conflict(
            "namespace commit differs from the durable prepared record".into(),
        ));
    }
    let expected = read_request_ledger(&prepared.request.fragment, partition_id)?.len() as u64 + 1;
    if prepared.log_index != expected {
        return Err(AppError::Conflict(format!(
            "namespace commit index {} does not follow local committed index {}",
            prepared.log_index,
            expected.saturating_sub(1)
        )));
    }
    let outcome = materialize_namespace_request(&prepared.request).await?;
    append_request_ledger(&prepared.request)?;
    maybe_persist_namespace_snapshot(&prepared.request.fragment, partition_id)?;
    remove_prepared_mutation(&prepared.request.fragment, partition_id, mutation_id);
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({"applied": true, "outcome": outcome})),
    ))
}

async fn peer_abort_namespace(
    headers: HeaderMap,
    AxumJson(prepared): AxumJson<PreparedNamespaceMutation>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    validate_prepared_assignment(&prepared, &runtime.local_node_id)?;
    let partition_id = prepared.assignment.partition_id;
    let lock = namespace_partition_commit_lock(&prepared.request.fragment, partition_id)?;
    let _guard = lock.lock().await;
    if request_seen(
        &prepared.request.fragment,
        partition_id,
        prepared.request.mutation_id.as_deref(),
    )? {
        return Ok(StatusCode::NO_CONTENT);
    }
    if let Some(mutation_id) = prepared.request.mutation_id.as_deref()
        && let Some(durable) = load_prepared_mutation(&prepared.request.fragment, partition_id, mutation_id)?
        && durable.log_index == prepared.log_index
        && durable.request.mutation_id == prepared.request.mutation_id
    {
        remove_prepared_mutation(&prepared.request.fragment, partition_id, mutation_id);
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn peer_coordinate_namespace(
    headers: HeaderMap,
    AxumJson(request): AxumJson<PeerNamespaceApplyRequest>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(
        coordinate_namespace_commit(ensure_request_mutation_id(request)).await?,
    ))
}

pub async fn peer_namespace_read(
    headers: HeaderMap,
    AxumJson(request): AxumJson<PeerNamespaceReadRequest>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    let partition_id = namespace_read_partition(&request);
    let assignment = resolve_namespace_group(&runtime, &request.fragment, partition_id).await?;
    if !assignment
        .voter_node_ids
        .iter()
        .any(|node_id| node_id == &runtime.local_node_id)
    {
        return Err(AppError::Forbidden(
            "this node is not a voter for the fragment namespace".into(),
        ));
    }
    let namespace = global_namespace_runtime();
    let response = FORCE_LOCAL_NAMESPACE_READ
        .scope(true, async {
            Ok::<PeerNamespaceReadResponse, AppError>(match request.kind {
                PeerNamespaceReadKind::ListDir => {
                    let (entries, continuation_token) = namespace
                        .list_dir_page(
                            &request.fragment,
                            request.path.as_deref().unwrap_or_default(),
                            request.include_trashed,
                            request.limit.unwrap_or(100).clamp(1, 10_000),
                            request.continuation_token.as_deref(),
                        )
                        .await?;
                    PeerNamespaceReadResponse::ListDir {
                        entries,
                        continuation_token,
                    }
                }
                PeerNamespaceReadKind::Entry => PeerNamespaceReadResponse::Entry(
                    namespace
                        .entry(
                            &request.fragment,
                            request.path.as_deref().unwrap_or_default(),
                        )
                        .await?,
                ),
                PeerNamespaceReadKind::TrashedEntry => PeerNamespaceReadResponse::Entry(
                    namespace
                        .trashed_entry(
                            &request.fragment,
                            request.path.as_deref().unwrap_or_default(),
                        )
                        .await?,
                ),
                PeerNamespaceReadKind::ResolveLiveObject => PeerNamespaceReadResponse::ObjectId(
                    namespace
                        .resolve_live_object_id(
                            &request.fragment,
                            request.path.as_deref().unwrap_or_default(),
                        )
                        .await?,
                ),
                PeerNamespaceReadKind::ObjectRefs => {
                    PeerNamespaceReadResponse::ObjectRefs(if request.include_trashed {
                        namespace
                            .list_trash_object_refs_page(
                                &request.fragment,
                                request.prefix.as_deref(),
                                request.continuation_token.as_deref(),
                                request.limit.unwrap_or(100).clamp(1, 10_000),
                            )
                            .await?
                    } else {
                        namespace
                            .list_live_object_refs_page(
                                &request.fragment,
                                request.prefix.as_deref(),
                                request.continuation_token.as_deref(),
                                request.limit.unwrap_or(100).clamp(1, 10_000),
                            )
                            .await?
                    })
                }
                PeerNamespaceReadKind::Reachability => PeerNamespaceReadResponse::Reachability(
                    crate::core::index_store::referenced_object_ids(&request.fragment)?,
                ),
            })
        })
        .await?;
    Ok(Json(response))
}

pub async fn peer_namespace_requests(
    headers: HeaderMap,
    Path((fragment, partition_id)): Path<(String, u16)>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(serde_json::json!({
        "fragment": fragment,
        "partition_id": partition_id,
        "requests": read_request_ledger(&fragment, partition_id)?,
    })))
}

pub async fn peer_namespace_summary(
    headers: HeaderMap,
    Path((fragment, partition_id)): Path<(String, u16)>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(namespace_local_ledger_summary(&fragment, partition_id)?))
}

pub async fn peer_namespace_prepared(
    headers: HeaderMap,
    Path((fragment, partition_id, log_index)): Path<(String, u16, u64)>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(prepared_mutation_for_log_index(&fragment, partition_id, log_index)?))
}

async fn fetch_peer_namespace_requests(
    endpoint: &str,
    fragment: &str,
    partition_id: u16,
) -> Result<Vec<PeerNamespaceApplyRequest>, AppError> {
    let client = peer_http_client()?;
    let mut req = client.get(namespace_requests_peer_url(endpoint, fragment, partition_id));
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

async fn fetch_peer_namespace_snapshot(
    endpoint: &str,
    fragment: &str,
    partition_id: u16,
) -> Result<NamespaceSnapshot, AppError> {
    let client = peer_http_client()?;
    let mut request = client.get(namespace_snapshot_peer_url(endpoint, fragment, partition_id));
    if let Some(secret) = cluster_secret() {
        request = request.header("x-n2-cluster-secret", secret);
    }
    let response = timeout(Duration::from_secs(30), request.send())
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "namespace snapshot fetch timed out at {endpoint}"
            ))
        })?
        .map_err(|err| {
            AppError::ServiceUnavailable(format!(
                "namespace snapshot fetch failed at {endpoint}: {err}"
            ))
        })?;
    if !response.status().is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "namespace snapshot fetch returned {} from {}",
            response.status(),
            endpoint
        )));
    }
    response.json().await.map_err(|err| {
        AppError::InternalServerError(format!("decode namespace snapshot from {endpoint}: {err}"))
    })
}

async fn sync_namespace_from_peers_with_snapshot(
    fragment: &str,
    force_snapshot: bool,
) -> Result<u64, AppError> {
    let runtime = active_cluster_runtime()?;
    let mut applied = 0u64;
    for partition_id in 0..namespace_partition_count() {
        let assignment = resolve_namespace_group(&runtime, fragment, partition_id).await?;
        let source = most_current_reachable_voter(&runtime, &assignment).await?;
        if source.id == runtime.local_node_id {
            continue;
        }
        let snapshot = fetch_peer_namespace_snapshot(&source.endpoint, fragment, partition_id).await?;
        let local_position = read_request_ledger(fragment, partition_id)?.len() as u64;
        if force_snapshot || snapshot.committed_log_index > local_position {
            apply_namespace_snapshot(&snapshot)?;
            applied = applied.saturating_add(snapshot.committed_log_index.saturating_sub(local_position));
        }
        let requests = fetch_peer_namespace_requests(&source.endpoint, fragment, partition_id).await?;
        for request in requests {
            if request.fragment != fragment {
                continue;
            }
            if request_seen(fragment, partition_id, request.mutation_id.as_deref())? {
                continue;
            }
            apply_namespace_request(&request).await?;
            applied = applied.saturating_add(1);
        }
    }
    Ok(applied)
}

pub async fn sync_namespace_from_peers(fragment: &str) -> Result<u64, AppError> {
    sync_namespace_from_peers_with_snapshot(fragment, false).await
}

pub async fn repair_namespace_from_peers(fragment: &str) -> Result<u64, AppError> {
    sync_namespace_from_peers_with_snapshot(fragment, true).await
}

pub async fn peer_namespace_snapshot(
    headers: HeaderMap,
    Path((fragment, partition_id)): Path<(String, u16)>,
) -> Result<impl axum::response::IntoResponse, AppError> {
    authorize_peer(&headers)?;
    let lock = namespace_partition_commit_lock(&fragment, partition_id)?;
    let _guard = lock.lock().await;
    let snapshot = build_namespace_snapshot(&fragment, partition_id)?;
    persist_namespace_snapshot(&snapshot)?;
    Ok(Json(snapshot))
}

pub fn build_namespace_peer_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/n2/peer/namespace/prepare", post(peer_prepare_namespace))
        .route("/n2/peer/namespace/commit", post(peer_commit_namespace))
        .route("/n2/peer/namespace/abort", post(peer_abort_namespace))
        .route(
            "/n2/peer/namespace/coordinate",
            post(peer_coordinate_namespace),
        )
        .route(
            "/n2/peer/namespace/requests/{fragment}/{partition_id}",
            get(peer_namespace_requests),
        )
        .route(
            "/n2/peer/namespace/summary/{fragment}/{partition_id}",
            get(peer_namespace_summary),
        )
        .route(
            "/n2/peer/namespace/prepared/{fragment}/{partition_id}/{log_index}",
            get(peer_namespace_prepared),
        )
        .route(
            "/n2/peer/namespace/snapshot/{fragment}/{partition_id}",
            get(peer_namespace_snapshot),
        )
        .route(
            "/n2/peer/namespace/group/{fragment}/{partition_id}",
            get(peer_namespace_group),
        )
        .route("/n2/peer/namespace/read", post(peer_namespace_read))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::disk_manager::{BLOB_STORE_DISKS, INDEX_STORE_DISKS};
    use crate::core::index_store::{publish_namespace_link, read_namespace_symlink_object_id};

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

    #[test]
    fn namespace_snapshot_restores_live_and_trash_links() {
        let _guard = crate::test_support::global_test_lock().blocking_lock();
        let base = std::env::temp_dir().join(format!("n2-namespace-snapshot-{}", Ulid::new()));
        let index = base.join("index");
        let blob = base.join("blob");
        fs::create_dir_all(&index).expect("create index root");
        fs::create_dir_all(&blob).expect("create blob root");
        let _ = INDEX_STORE_DISKS.set(vec![index.to_string_lossy().to_string()]);
        let _ = BLOB_STORE_DISKS.set(vec![blob.to_string_lossy().to_string()]);
        let _ = FS_STORAGE_ROOT.set(base.join("state").to_string_lossy().to_string());

        let fragment = format!("snapshot-{}", Ulid::new());
        let partition_id = namespace_partition_for_path(&fragment, "docs/live.bin");
        let roots = ObjectPathManager::new().get_namespace_roots_for_path(&fragment, "docs/live.bin");
        let root = roots.first().expect("namespace root");
        publish_namespace_link(
            root,
            &fragment,
            "docs/live.bin",
            "01KLIVE0000000000000000000",
        )
        .expect("publish live link");
        let fragment_root = fragment_root_on(root, &fragment);
        let trash_path = fragment_root.join(TRASH_DIR).join("docs/trash.bin");
        fs::create_dir_all(trash_path.parent().expect("trash parent")).expect("create trash");
        unix_fs::symlink("@o_01KTRASH000000000000000000", &trash_path).expect("publish trash link");

        let snapshot = build_namespace_snapshot(&fragment, partition_id).expect("build snapshot");
        remove_path_no_follow(&fragment_root.join("docs")).expect("remove live namespace");
        remove_path_no_follow(&fragment_root.join(TRASH_DIR)).expect("remove trash namespace");
        apply_namespace_snapshot(&snapshot).expect("restore snapshot");

        assert_eq!(
            read_namespace_symlink_object_id(root, &fragment, &fragment_root.join("docs/live.bin"))
                .expect("read live link"),
            Some("01KLIVE0000000000000000000".to_string())
        );
        assert_eq!(
            {
                let trash_roots = ObjectPathManager::new()
                    .get_namespace_roots_for_path(&fragment, "docs/trash.bin");
                let trash_root = trash_roots.first().expect("trash namespace root");
                let trash_path = fragment_root_on(trash_root, &fragment)
                    .join(TRASH_DIR)
                    .join("docs/trash.bin");
                read_namespace_symlink_object_id(trash_root, &fragment, &trash_path)
            }
                .expect("read trash link"),
            Some("01KTRASH000000000000000000".to_string())
        );
        let _ = fs::remove_dir_all(base);
    }
}
