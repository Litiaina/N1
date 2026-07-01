use std::collections::{BTreeSet, HashMap};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::core::disk_manager::{ListObjectsResult, ObjectMetadata};
use crate::core::{index, index_store};
use crate::distribution::namespace::{
    NamespaceMutationKind, PeerNamespaceApplyRequest, PeerNamespaceReadKind,
    PeerNamespaceReadRequest, PeerNamespaceReadResponse, commit_namespace_created_quorum,
    commit_namespace_summary_quorum, namespace_read_if_remote,
};
use crate::enums::app_error_enum::AppError;
use crate::namespace::models::{
    LifecycleState, NamespaceEntry, NamespaceEntryKind, NamespaceError, NamespaceMutationSummary,
    NamespaceReachabilitySnapshot, ObjectHead,
};
use crate::namespace::paths::normalize_path;

static GLOBAL_NAMESPACE_RUNTIME: LazyLock<NamespaceRuntime> = LazyLock::new(NamespaceRuntime::new);
static OBJECT_META_CACHE: LazyLock<Mutex<HashMap<String, CachedObjectMeta>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

const OBJECT_META_CACHE_TTL: Duration = Duration::from_secs(30);
const OBJECT_META_CACHE_MAX: usize = 65_536;

#[derive(Clone, Debug)]
struct CachedObjectMeta {
    size_bytes: u64,
    version_id: String,
    content_type: String,
    created_at_unix: i64,
    inserted_at: Instant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceObjectRef {
    pub path: String,
    pub object_id: String,
    pub version_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceObjectRefPage {
    pub entries: Vec<NamespaceObjectRef>,
    pub continuation_token: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct NamespaceRuntimeStats {
    pub loaded_namespace_facades: u64,
    pub max_loaded_namespace_facades: u64,
    pub sampled_namespace_facades: u64,
    pub busy_namespace_facades: u64,
    pub loaded_namespace_handles: u64,
    pub max_loaded_namespace_handles: u64,
    pub nodes: u64,
    pub child_index_entries: u64,
    pub objects: u64,
    pub object_versions: u64,
    pub object_nodes: u64,
    pub trash_anchor_entries: u64,
    pub trash_entry_entries: u64,
    pub subtree_path_counters: u64,
    pub subtree_object_counters: u64,
    pub largest_namespace_nodes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub largest_namespace_fragment: Option<String>,
}

pub struct NamespaceRuntime;

pub fn global_namespace_runtime() -> &'static NamespaceRuntime {
    &GLOBAL_NAMESPACE_RUNTIME
}

impl NamespaceRuntime {
    pub fn new() -> Self {
        Self
    }

    pub fn evict_fragment(&self, fragment: &str) {
        let prefix = format!("{fragment}\0");
        if let Ok(mut cache) = OBJECT_META_CACHE.lock() {
            cache.retain(|key, _| !key.starts_with(&prefix));
        }
    }

    pub fn runtime_stats(&self) -> NamespaceRuntimeStats {
        NamespaceRuntimeStats {
            loaded_namespace_facades: 1,
            max_loaded_namespace_facades: 1,
            loaded_namespace_handles: 1,
            max_loaded_namespace_handles: 1,
            ..NamespaceRuntimeStats::default()
        }
    }

    pub async fn list_dir(
        &self,
        fragment: &str,
        path: &str,
        trash: bool,
    ) -> Result<Vec<NamespaceEntry>, AppError> {
        Ok(self
            .list_dir_page(fragment, path, trash, 1000, None)
            .await?
            .0)
    }

    pub async fn list_dir_page(
        &self,
        fragment: &str,
        path: &str,
        trash: bool,
        limit: usize,
        continuation_token: Option<&str>,
    ) -> Result<(Vec<NamespaceEntry>, Option<String>), AppError> {
        let response = quorum_read(PeerNamespaceReadRequest {
            fragment: fragment.to_string(),
            kind: PeerNamespaceReadKind::ListDir,
            path: Some(path.to_string()),
            include_trashed: trash,
            limit: Some(limit.clamp(1, 1000)),
            continuation_token: continuation_token.map(ToOwned::to_owned),
            prefix: None,
        })
        .await?;
        let PeerNamespaceReadResponse::ListDir {
            mut entries,
            continuation_token,
        } = response
        else {
            return Err(wrong_response("directory listing"));
        };
        hydrate_entries(fragment, &mut entries).await?;
        Ok((entries, continuation_token))
    }

    pub async fn entry(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<Option<NamespaceEntry>, AppError> {
        let path = normalize_user_path(path)?;
        if path.is_empty() {
            return Ok(Some(root_entry(fragment)));
        }
        let response = quorum_read(PeerNamespaceReadRequest {
            fragment: fragment.to_string(),
            kind: PeerNamespaceReadKind::Entry,
            path: Some(path),
            include_trashed: false,
            limit: None,
            continuation_token: None,
            prefix: None,
        })
        .await?;
        let PeerNamespaceReadResponse::Entry(mut entry) = response else {
            return Err(wrong_response("namespace entry"));
        };
        if let Some(entry) = entry.as_mut() {
            hydrate_entries(fragment, std::slice::from_mut(entry)).await?;
        }
        Ok(entry)
    }

    pub async fn trashed_entry(
        &self,
        fragment: &str,
        path_or_id: &str,
    ) -> Result<Option<NamespaceEntry>, AppError> {
        let response = quorum_read(PeerNamespaceReadRequest {
            fragment: fragment.to_string(),
            kind: PeerNamespaceReadKind::TrashedEntry,
            path: Some(path_or_id.to_string()),
            include_trashed: true,
            limit: None,
            continuation_token: None,
            prefix: None,
        })
        .await?;
        let PeerNamespaceReadResponse::Entry(mut entry) = response else {
            return Err(wrong_response("trash entry"));
        };
        if let Some(entry) = entry.as_mut() {
            hydrate_entries(fragment, std::slice::from_mut(entry)).await?;
        }
        Ok(entry)
    }

    pub async fn head_for_path(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<Option<ObjectHead>, AppError> {
        let Some(object_id) = self.resolve_live_object_id(fragment, path).await? else {
            return Ok(None);
        };
        Ok(self
            .object_heads(fragment, std::slice::from_ref(&object_id))
            .await?
            .remove(&object_id))
    }

    pub async fn object_heads(
        &self,
        fragment: &str,
        object_ids: &[String],
    ) -> Result<HashMap<String, ObjectHead>, AppError> {
        let mut out = HashMap::new();
        let mut missing = Vec::new();
        for object_id in object_ids {
            if let Some(metadata) = cached_object_meta(fragment, object_id) {
                out.insert(
                    object_id.clone(),
                    object_head(fragment, object_id, metadata),
                );
            } else {
                missing.push(object_id.clone());
            }
        }
        if !missing.is_empty() {
            let resolved =
                crate::distribution::peer::resolve_cluster_object_heads(fragment, &missing).await?;
            for (object_id, metadata) in resolved {
                cache_object_metadata(&metadata);
                out.insert(
                    object_id.clone(),
                    object_head(fragment, &object_id, cached_from_metadata(&metadata)),
                );
            }
        }
        Ok(out)
    }

    pub async fn mkdir(&self, fragment: &str, path: &str) -> Result<bool, AppError> {
        commit_namespace_created_quorum(mutation_request(
            NamespaceMutationKind::Mkdir,
            fragment,
            Some(path),
            None,
        ))
        .await
    }

    pub async fn rename(
        &self,
        fragment: &str,
        source_path: &str,
        destination_path: &str,
    ) -> Result<NamespaceMutationSummary, AppError> {
        commit_namespace_summary_quorum(mutation_request(
            NamespaceMutationKind::Rename,
            fragment,
            Some(source_path),
            Some(destination_path),
        ))
        .await
    }

    pub async fn trash(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<NamespaceMutationSummary, AppError> {
        commit_namespace_summary_quorum(mutation_request(
            NamespaceMutationKind::Trash,
            fragment,
            Some(path),
            None,
        ))
        .await
    }

    pub async fn recover(
        &self,
        fragment: &str,
        path_or_id: &str,
    ) -> Result<NamespaceMutationSummary, AppError> {
        commit_namespace_summary_quorum(mutation_request(
            NamespaceMutationKind::Recover,
            fragment,
            Some(path_or_id),
            None,
        ))
        .await
    }

    pub async fn recover_paths_bulk(
        &self,
        fragment: &str,
        paths: Vec<String>,
    ) -> Result<Vec<(String, Result<NamespaceMutationSummary, AppError>)>, AppError> {
        let mut out = Vec::with_capacity(paths.len());
        for path in paths {
            let result = self.recover(fragment, &path).await;
            out.push((path, result));
        }
        Ok(out)
    }

    pub async fn recover_trash_entry_id(
        &self,
        fragment: &str,
        trash_entry_id: &str,
    ) -> Result<NamespaceMutationSummary, AppError> {
        self.recover(fragment, trash_entry_id).await
    }

    pub async fn reachability_snapshot_if_present(
        &self,
        fragment: &str,
    ) -> Result<Option<NamespaceReachabilitySnapshot>, AppError> {
        let response = quorum_read(PeerNamespaceReadRequest {
            fragment: fragment.to_string(),
            kind: PeerNamespaceReadKind::Reachability,
            path: None,
            include_trashed: true,
            limit: None,
            continuation_token: None,
            prefix: None,
        })
        .await?;
        let PeerNamespaceReadResponse::Reachability(object_ids) = response else {
            return Err(wrong_response("namespace reachability"));
        };
        let mut snapshot = NamespaceReachabilitySnapshot::default();
        for object_id in object_ids {
            for metadata in index::list_live_object_versions_metadata(fragment, &object_id).await? {
                snapshot.referenced_object_ids.insert(object_id.clone());
                snapshot.referenced_version_ids.insert(metadata.version_id);
            }
        }
        Ok(Some(snapshot))
    }

    pub async fn purged_object_ids_for_gc(
        &self,
        _fragment: &str,
    ) -> Result<BTreeSet<String>, AppError> {
        Ok(BTreeSet::new())
    }

    pub async fn resolve_live_object_id(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<Option<String>, AppError> {
        let response = quorum_read(PeerNamespaceReadRequest {
            fragment: fragment.to_string(),
            kind: PeerNamespaceReadKind::ResolveLiveObject,
            path: Some(path.to_string()),
            include_trashed: false,
            limit: None,
            continuation_token: None,
            prefix: None,
        })
        .await?;
        let PeerNamespaceReadResponse::ObjectId(object_id) = response else {
            return Err(wrong_response("object resolution"));
        };
        Ok(object_id)
    }

    pub async fn resolve_any_object_id(
        &self,
        fragment: &str,
        path_or_id: &str,
    ) -> Result<Option<String>, AppError> {
        if let Some(object_id) = self.resolve_live_object_id(fragment, path_or_id).await? {
            return Ok(Some(object_id));
        }
        Ok(self
            .trashed_entry(fragment, path_or_id)
            .await?
            .and_then(|entry| entry.object_id))
    }

    pub async fn select_object_id_for_write(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<String, AppError> {
        let path = normalize_user_path(path)?;
        if path.is_empty() {
            return Err(AppError::BadRequest(
                "root cannot be written as a file".into(),
            ));
        }
        let existing = match self.entry(fragment, &path).await {
            Ok(entry) => entry,
            Err(AppError::NotFound) => None,
            Err(err) => return Err(err),
        };
        if let Some(entry) = existing {
            if entry.kind == NamespaceEntryKind::Directory {
                return Err(AppError::Conflict(format!(
                    "logical path '{path}' already exists as a directory"
                )));
            }
            if let Some(object_id) = entry.object_id {
                return Ok(object_id);
            }
        }
        Ok(index_store::new_object_id())
    }

    pub async fn bind_pending_upload(
        &self,
        fragment: &str,
        logical_path: &str,
        physical_object_id: &str,
        version_id: &str,
        session_id: &str,
    ) -> Result<(), AppError> {
        index_store::write_pending_binding(&index_store::PendingUploadBinding {
            fragment: fragment.to_string(),
            logical_path: normalize_user_path(logical_path)?,
            physical_object_id: physical_object_id.to_string(),
            version_id: version_id.to_string(),
            session_id: session_id.to_string(),
            created_at_unix: Utc::now().timestamp(),
        })
    }

    pub async fn pending_upload(
        &self,
        fragment: &str,
        version_id: &str,
    ) -> Result<Option<index_store::PendingUploadBinding>, AppError> {
        index_store::read_pending_binding(fragment, version_id)
    }

    pub async fn list_pending_uploads(
        &self,
        fragment: &str,
    ) -> Result<Vec<index_store::PendingUploadBinding>, AppError> {
        index_store::list_pending_bindings(fragment)
    }

    pub async fn find_pending_upload_by_logical_path(
        &self,
        fragment: &str,
        logical_path: &str,
    ) -> Result<Option<index_store::PendingUploadBinding>, AppError> {
        let logical_path = normalize_user_path(logical_path)?;
        Ok(index_store::list_pending_bindings(fragment)?
            .into_iter()
            .find(|binding| binding.logical_path == logical_path))
    }

    pub async fn clear_pending_upload(
        &self,
        fragment: &str,
        version_id: &str,
    ) -> Result<(), AppError> {
        index_store::clear_pending_binding(fragment, version_id)
    }

    pub async fn list_objects_page(
        &self,
        fragment: &str,
        params: &HashMap<String, String>,
        trash: bool,
    ) -> Result<ListObjectsResult, AppError> {
        let limit = params
            .get("limit")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(100)
            .clamp(1, 1000);
        let prefix = params.get("prefix").cloned();
        let delimiter = params.get("delimiter").cloned();
        let continuation = params.get("continuation_token").cloned();
        if trash {
            index::scan_fragment_trash_metadata_page(
                fragment,
                prefix,
                delimiter,
                limit,
                continuation,
                true,
            )
            .await
        } else {
            index::scan_fragment_live_metadata_page(
                fragment,
                prefix,
                delimiter,
                limit,
                continuation,
                true,
            )
            .await
        }
    }

    pub async fn list_live_object_refs_page(
        &self,
        fragment: &str,
        prefix: Option<&str>,
        continuation_token: Option<&str>,
        limit: usize,
    ) -> Result<NamespaceObjectRefPage, AppError> {
        object_refs_page(
            fragment,
            PeerNamespaceReadKind::ObjectRefs,
            prefix,
            continuation_token,
            limit,
        )
        .await
    }

    pub async fn list_trash_object_refs_page(
        &self,
        fragment: &str,
        prefix: Option<&str>,
        continuation_token: Option<&str>,
        limit: usize,
    ) -> Result<NamespaceObjectRefPage, AppError> {
        object_refs_page(
            fragment,
            PeerNamespaceReadKind::TrashObjectRefs,
            prefix,
            continuation_token,
            limit,
        )
        .await
    }
}

async fn quorum_read(
    request: PeerNamespaceReadRequest,
) -> Result<PeerNamespaceReadResponse, AppError> {
    let timeout_duration = namespace_read_timeout();
    match tokio::time::timeout(timeout_duration, namespace_read_if_remote(request)).await {
        Ok(Ok(Some(response))) => Ok(response),
        Ok(Ok(None)) => Err(AppError::InternalServerError(
            "namespace quorum returned no response".into(),
        )),
        Ok(Err(err)) => Err(err),
        Err(_) => Err(AppError::ServiceUnavailable(format!(
            "namespace quorum read exceeded {timeout_duration:?}"
        ))),
    }
}

async fn object_refs_page(
    fragment: &str,
    kind: PeerNamespaceReadKind,
    prefix: Option<&str>,
    continuation_token: Option<&str>,
    limit: usize,
) -> Result<NamespaceObjectRefPage, AppError> {
    let response = quorum_read(PeerNamespaceReadRequest {
        fragment: fragment.to_string(),
        kind,
        path: None,
        include_trashed: matches!(kind, PeerNamespaceReadKind::TrashObjectRefs),
        limit: Some(limit.clamp(1, 1000)),
        continuation_token: continuation_token.map(ToOwned::to_owned),
        prefix: prefix.map(ToOwned::to_owned),
    })
    .await?;
    let PeerNamespaceReadResponse::ObjectRefs(mut page) = response else {
        return Err(wrong_response("object reference page"));
    };
    let object_ids = page
        .entries
        .iter()
        .map(|entry| entry.object_id.clone())
        .collect::<Vec<_>>();
    let heads =
        crate::distribution::peer::resolve_cluster_object_heads(fragment, &object_ids).await?;
    for entry in &mut page.entries {
        if let Some(metadata) = heads.get(&entry.object_id) {
            entry.version_id = metadata.version_id.clone();
            cache_object_metadata(metadata);
        }
    }
    Ok(page)
}

async fn hydrate_entries(fragment: &str, entries: &mut [NamespaceEntry]) -> Result<(), AppError> {
    let object_ids = entries
        .iter()
        .filter_map(|entry| entry.object_id.clone())
        .collect::<Vec<_>>();
    if object_ids.is_empty() {
        return Ok(());
    }
    let heads =
        crate::distribution::peer::resolve_cluster_object_heads(fragment, &object_ids).await?;
    for entry in entries {
        let Some(object_id) = entry.object_id.as_ref() else {
            continue;
        };
        if let Some(metadata) = heads.get(object_id) {
            entry.size_bytes = Some(metadata.total_size_bytes);
            entry.version_id = Some(metadata.version_id.clone());
            entry.created_at_unix = metadata.created_at;
            entry.updated_at_unix = metadata.created_at;
            cache_object_metadata(metadata);
        }
    }
    Ok(())
}

fn mutation_request(
    kind: NamespaceMutationKind,
    fragment: &str,
    source_path: Option<&str>,
    destination_path: Option<&str>,
) -> PeerNamespaceApplyRequest {
    PeerNamespaceApplyRequest {
        mutation_id: None,
        kind,
        fragment: fragment.to_string(),
        source_path: source_path.map(ToOwned::to_owned),
        destination_path: destination_path.map(ToOwned::to_owned),
        object_id: None,
        version_id: None,
        metadata: None,
    }
}

fn root_entry(fragment: &str) -> NamespaceEntry {
    NamespaceEntry {
        fragment: fragment.to_string(),
        path: String::new(),
        parent_path: String::new(),
        name: String::new(),
        kind: NamespaceEntryKind::Directory,
        object_id: None,
        size_bytes: None,
        version_id: None,
        packed_ext_u64: None,
        lifecycle: LifecycleState::Live,
        trash_entry_id: None,
        deleted_at_unix: None,
        created_at_unix: 0,
        updated_at_unix: 0,
    }
}

fn object_head(fragment: &str, object_id: &str, metadata: CachedObjectMeta) -> ObjectHead {
    ObjectHead {
        fragment: fragment.to_string(),
        object_id: object_id.to_string(),
        current_version_id: metadata.version_id,
        storage_object_id: object_id.to_string(),
        content_type: metadata.content_type,
        total_size_bytes: metadata.size_bytes,
        lifecycle: LifecycleState::Live,
        created_at_unix: metadata.created_at_unix,
        updated_at_unix: metadata.created_at_unix,
    }
}

fn cached_from_metadata(metadata: &ObjectMetadata) -> CachedObjectMeta {
    CachedObjectMeta {
        size_bytes: metadata.total_size_bytes,
        version_id: metadata.version_id.clone(),
        content_type: metadata.content_type.clone(),
        created_at_unix: metadata.created_at,
        inserted_at: Instant::now(),
    }
}

fn cache_object_metadata(metadata: &ObjectMetadata) {
    let Ok(mut cache) = OBJECT_META_CACHE.lock() else {
        return;
    };
    if cache.len() >= OBJECT_META_CACHE_MAX {
        cache.retain(|_, value| value.inserted_at.elapsed() <= OBJECT_META_CACHE_TTL);
        if cache.len() >= OBJECT_META_CACHE_MAX {
            let remove = cache.len() - OBJECT_META_CACHE_MAX + 1;
            let keys = cache.keys().take(remove).cloned().collect::<Vec<_>>();
            for key in keys {
                cache.remove(&key);
            }
        }
    }
    cache.insert(
        cache_key(&metadata.fragment, &metadata.object_key),
        cached_from_metadata(metadata),
    );
}

fn cached_object_meta(fragment: &str, object_id: &str) -> Option<CachedObjectMeta> {
    let key = cache_key(fragment, object_id);
    let mut cache = OBJECT_META_CACHE.lock().ok()?;
    match cache.get(&key) {
        Some(value) if value.inserted_at.elapsed() <= OBJECT_META_CACHE_TTL => Some(value.clone()),
        Some(_) => {
            cache.remove(&key);
            None
        }
        None => None,
    }
}

fn cache_key(fragment: &str, object_id: &str) -> String {
    format!("{fragment}\0{object_id}")
}

fn normalize_user_path(path: &str) -> Result<String, AppError> {
    let normalized = normalize_path(path).map_err(namespace_error_to_app)?;
    index_store::reject_reserved_user_path(&normalized)?;
    Ok(normalized)
}

fn namespace_error_to_app(error: NamespaceError) -> AppError {
    match error {
        NamespaceError::InvalidPath(message) => AppError::BadRequest(message),
        NamespaceError::Conflict(message) => AppError::Conflict(message),
        NamespaceError::NotFound(_) => AppError::NotFound,
        NamespaceError::Invariant(message) => AppError::InternalServerError(message),
    }
}

fn namespace_read_timeout() -> Duration {
    static TIMEOUT: LazyLock<Duration> = LazyLock::new(|| {
        let millis = std::env::var("N2_NAMESPACE_READ_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(3_000);
        Duration::from_millis(millis)
    });
    *TIMEOUT
}

fn wrong_response(operation: &str) -> AppError {
    AppError::InternalServerError(format!(
        "namespace quorum returned the wrong response for {operation}"
    ))
}

pub fn logical_not_found() -> AppError {
    AppError::NotFound
}

pub fn unexpected_response(status: axum::http::StatusCode, context: &str) -> AppError {
    AppError::InternalServerError(format!("{context}: unexpected response status {status}"))
}
