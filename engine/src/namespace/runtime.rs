use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use chrono::Utc;
use rustix::fs::{CWD, renameat};
use serde::Serialize;

use crate::core::disk_manager::{ListObjectsResult, ObjectMetadata};
use crate::core::index_store::{
    self, PendingUploadBinding, TRASH_DIR, TrashKind, TrashRecord, fragment_root_on,
    internal_namespace_entry_name, namespace_path, new_object_id, publish_namespace_link,
    read_namespace_symlink_object_id, reject_reserved_user_path, scan_trash_records,
    trash_generation_name, visit_dir_names, visit_trash_records, write_pending_binding,
};
use crate::enums::app_error_enum::AppError;
use crate::namespace::models::{
    LifecycleState, NamespaceEntry, NamespaceEntryKind, NamespaceError, NamespaceMutationSummary,
    NamespaceReachabilitySnapshot, ObjectHead,
};
use crate::namespace::paths::{base_name, is_same_or_descendant, normalize_path, parent_path};
use walkdir::WalkDir;

static GLOBAL_NAMESPACE_RUNTIME: LazyLock<NamespaceRuntime> = LazyLock::new(NamespaceRuntime::new);
static FINALIZE_LOCKS: LazyLock<tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> =
    LazyLock::new(|| tokio::sync::Mutex::new(HashMap::new()));
pub fn global_namespace_runtime() -> &'static NamespaceRuntime {
    &GLOBAL_NAMESPACE_RUNTIME
}

#[derive(Debug, Clone)]
pub struct NamespaceObjectRef {
    pub path: String,
    pub object_id: String,
    pub version_id: String,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone, Default, Serialize)]
pub struct TrashLifecycleSweepSummary {
    pub expired_entries: u64,
    pub expired_versions: u64,
    pub reclaimed_objects: u64,
}

pub struct NamespaceRuntime;

impl NamespaceRuntime {
    pub fn new() -> Self {
        Self
    }

    pub fn evict_fragment(&self, _fragment: &str) {}

    pub fn runtime_stats(&self) -> NamespaceRuntimeStats {
        NamespaceRuntimeStats {
            max_loaded_namespace_facades: 1,
            max_loaded_namespace_handles: 1,
            ..NamespaceRuntimeStats::default()
        }
    }

    pub async fn list_dir(
        &self,
        fragment: &str,
        path: &str,
        include_trashed: bool,
    ) -> Result<Vec<NamespaceEntry>, AppError> {
        Ok(self
            .list_dir_page(fragment, path, include_trashed, usize::MAX / 2, None)
            .await?
            .0)
    }

    pub async fn list_dir_page(
        &self,
        fragment: &str,
        path: &str,
        include_trashed: bool,
        limit: usize,
        continuation_token: Option<&str>,
    ) -> Result<(Vec<NamespaceEntry>, Option<String>), AppError> {
        let fragment = fragment.to_string();
        let path = normalize_user_path(path)?;
        let after = continuation_token.map(ToOwned::to_owned);
        tokio::task::spawn_blocking(move || {
            if include_trashed {
                return list_trash_dir_page(&fragment, &path, limit, after.as_deref());
            }
            list_live_dir_page(&fragment, &path, limit, after.as_deref())
        })
        .await
        .map_err(|err| AppError::InternalServerError(format!("N1 namespace worker: {err}")))?
    }

    pub async fn entry(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<Option<NamespaceEntry>, AppError> {
        let fragment = fragment.to_string();
        let path = normalize_user_path(path)?;
        tokio::task::spawn_blocking(move || live_entry(&fragment, &path))
            .await
            .map_err(|err| AppError::InternalServerError(format!("N1 namespace worker: {err}")))?
    }

    pub async fn trashed_entry(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<Option<NamespaceEntry>, AppError> {
        let fragment = fragment.to_string();
        let path = normalize_user_path(path)?;
        tokio::task::spawn_blocking(move || trash_entry(&fragment, &path))
            .await
            .map_err(|err| AppError::InternalServerError(format!("N1 namespace worker: {err}")))?
    }

    pub async fn head_for_path(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<Option<ObjectHead>, AppError> {
        let Some(object_id) = self.resolve_live_object_id(fragment, path).await? else {
            return Ok(None);
        };
        let Some(meta) = index_store::read_current_live_head(fragment, &object_id)? else {
            return Ok(None);
        };
        Ok(Some(object_head_from_meta(
            fragment,
            LifecycleState::Live,
            meta,
        )))
    }

    pub async fn object_heads(
        &self,
        fragment: &str,
        object_ids: &[String],
    ) -> Result<HashMap<String, ObjectHead>, AppError> {
        let mut out = HashMap::new();
        for object_id in object_ids {
            if let Some(meta) = index_store::read_current_live_head(fragment, object_id)? {
                out.insert(
                    object_id.clone(),
                    object_head_from_meta(fragment, LifecycleState::Live, meta),
                );
            }
        }
        Ok(out)
    }

    pub async fn mkdir(&self, fragment: &str, path: &str) -> Result<bool, AppError> {
        let fragment = fragment.to_string();
        let path = normalize_user_path(path)?;
        if path.is_empty() {
            return Ok(false);
        }
        tokio::task::spawn_blocking(move || {
            if live_entry(&fragment, &path)?.is_some() {
                return Ok(false);
            }
            let mut created = false;
            for root in index_roots() {
                let dir = fragment_root_on(&root, &fragment).join(&path);
                if !dir.exists() {
                    created = true;
                }
                fs::create_dir_all(&dir).map_err(|err| io_err(err, "create N1 namespace dir"))?;
            }
            Ok(created)
        })
        .await
        .map_err(|err| AppError::InternalServerError(format!("N1 namespace worker: {err}")))?
    }

    pub async fn rename(
        &self,
        fragment: &str,
        source_path: &str,
        destination_path: &str,
    ) -> Result<NamespaceMutationSummary, AppError> {
        let fragment = fragment.to_string();
        let source = normalize_user_path(source_path)?;
        let destination = normalize_user_path(destination_path)?;
        if source.is_empty() || destination.is_empty() {
            return Err(AppError::BadRequest("root cannot be renamed".to_string()));
        }
        if is_same_or_descendant(&destination, &source) {
            return Err(AppError::Conflict(
                "destination cannot be inside the source subtree".to_string(),
            ));
        }
        tokio::task::spawn_blocking(move || rename_live_path(&fragment, &source, &destination))
            .await
            .map_err(|err| AppError::InternalServerError(format!("N1 namespace worker: {err}")))?
    }

    pub async fn trash(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<NamespaceMutationSummary, AppError> {
        let fragment = fragment.to_string();
        let path = normalize_user_path(path)?;
        tokio::task::spawn_blocking(move || trash_live_path(&fragment, &path))
            .await
            .map_err(|err| AppError::InternalServerError(format!("N1 namespace worker: {err}")))?
    }

    pub async fn recover(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<NamespaceMutationSummary, AppError> {
        let fragment = fragment.to_string();
        let path = normalize_user_path(path)?;
        tokio::task::spawn_blocking(move || recover_trash_path(&fragment, &path, None))
            .await
            .map_err(|err| AppError::InternalServerError(format!("N1 namespace worker: {err}")))?
    }

    pub async fn recover_paths_bulk(
        &self,
        fragment: &str,
        paths: Vec<String>,
    ) -> Result<Vec<(String, Result<NamespaceMutationSummary, AppError>)>, AppError> {
        let mut out = Vec::new();
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
        let fragment = fragment.to_string();
        let trash_entry_id = trash_entry_id.to_string();
        tokio::task::spawn_blocking(move || {
            recover_trash_path(&fragment, "", Some(&trash_entry_id))
        })
        .await
        .map_err(|err| AppError::InternalServerError(format!("N1 namespace worker: {err}")))?
    }

    pub async fn sweep_expired_trash(
        &self,
        fragment: &str,
        retention_seconds: u64,
        now_unix: i64,
    ) -> Result<TrashLifecycleSweepSummary, AppError> {
        let fragment = fragment.to_string();
        tokio::task::spawn_blocking(move || {
            sweep_expired_trash_sync(&fragment, retention_seconds, now_unix)
        })
        .await
        .map_err(|err| AppError::InternalServerError(format!("N1 lifecycle worker: {err}")))?
    }

    pub async fn recover_all(&self, fragment: &str) -> Result<NamespaceMutationSummary, AppError> {
        let fragment_name = fragment.to_string();
        let (affected_paths, affected_objects) = tokio::task::spawn_blocking(move || {
            let mut latest_by_path = HashMap::<String, TrashRecord>::new();
            for record in scan_trash_records(&fragment_name)? {
                let replace = latest_by_path
                    .get(&record.original_path)
                    .is_none_or(|current| {
                        (record.deleted_at_unix, record.trash_entry_id.as_str())
                            > (current.deleted_at_unix, current.trash_entry_id.as_str())
                    });
                if replace {
                    latest_by_path.insert(record.original_path.clone(), record);
                }
            }
            let mut affected_paths = 0usize;
            let mut affected_objects = 0usize;
            for record in latest_by_path.into_values() {
                let summary = recover_trash_path(
                    &fragment_name,
                    &record.original_path,
                    Some(&record.trash_entry_id),
                )?;
                affected_paths += summary.affected_paths;
                affected_objects += summary.affected_objects;
            }
            Ok::<_, AppError>((affected_paths, affected_objects))
        })
        .await
        .map_err(|err| AppError::InternalServerError(format!("N1 namespace worker: {err}")))??;
        Ok(NamespaceMutationSummary {
            operation: "recover_all".to_string(),
            kind: NamespaceEntryKind::Directory,
            affected_paths,
            affected_objects,
            source_path: None,
            destination_path: None,
            trash_entry_id: None,
            deleted_at_unix: None,
        })
    }

    pub async fn reachability_snapshot_if_present(
        &self,
        fragment: &str,
    ) -> Result<Option<NamespaceReachabilitySnapshot>, AppError> {
        Ok(Some(NamespaceReachabilitySnapshot {
            referenced_object_ids: index_store::referenced_object_ids(fragment)?,
        }))
    }

    pub async fn resolve_live_object_id(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<Option<String>, AppError> {
        let fragment = fragment.to_string();
        let path = normalize_user_path(path)?;
        tokio::task::spawn_blocking(move || resolve_live_object_id_sync(&fragment, &path))
            .await
            .map_err(|err| AppError::InternalServerError(format!("N1 namespace worker: {err}")))?
    }

    pub async fn resolve_any_object_id(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<Option<String>, AppError> {
        if let Some(id) = self.resolve_live_object_id(fragment, path).await? {
            return Ok(Some(id));
        }
        let normalized = normalize_user_path(path)?;
        Ok(scan_trash_records(fragment)?
            .into_iter()
            .filter(|record| record.original_path == normalized)
            .max_by(|left, right| {
                left.deleted_at_unix
                    .cmp(&right.deleted_at_unix)
                    .then_with(|| left.trash_entry_id.cmp(&right.trash_entry_id))
            })
            .and_then(|record| record.object_keys.first().cloned()))
    }

    pub async fn select_object_id_for_write(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<String, AppError> {
        let normalized = normalize_user_path(path)?;
        if normalized.is_empty() {
            return Err(AppError::BadRequest(
                "root cannot be written as a file".to_string(),
            ));
        }
        if let Some(entry) = self.entry(fragment, &normalized).await? {
            if entry.kind == NamespaceEntryKind::Directory {
                return Err(AppError::Conflict(format!(
                    "logical path '{}' already exists as a directory",
                    normalized
                )));
            }
            if let Some(object_id) = entry.object_id {
                return Ok(object_id);
            }
        }
        Ok(new_object_id())
    }

    pub async fn finalize_logical_upload(
        &self,
        fragment: &str,
        logical_path: &str,
        physical_object_id: &str,
        metadata: &ObjectMetadata,
    ) -> Result<String, AppError> {
        let fragment = fragment.to_string();
        let logical_path = normalize_user_path(logical_path)?;
        let physical_object_id = physical_object_id.to_string();
        let metadata = metadata.clone();
        let lock_key = format!("{fragment}\0{logical_path}");
        let lock = {
            let mut locks = FINALIZE_LOCKS.lock().await;
            locks
                .entry(lock_key.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let guard = lock.lock().await;

        let result = tokio::task::spawn_blocking(move || {
            let final_object_id = match live_entry(&fragment, &logical_path)? {
                Some(entry) if entry.kind == NamespaceEntryKind::Directory => {
                    return Err(AppError::Conflict(format!(
                        "logical path '{}' already exists as a directory",
                        logical_path
                    )));
                }
                Some(entry) => entry
                    .object_id
                    .unwrap_or_else(|| physical_object_id.clone()),
                None => physical_object_id.clone(),
            };

            let mut namespace_placements = Vec::new();
            for root in index_roots() {
                let link = namespace_path(&root, &fragment, &logical_path);
                let parent = link.parent().ok_or_else(|| {
                    AppError::InternalServerError("N1 namespace link has no parent".to_string())
                })?;
                fs::create_dir_all(parent)
                    .map_err(|err| io_err(err, "create N1 namespace parent"))?;
                let existing = read_namespace_symlink_object_id(&root, &fragment, &link)?;
                match existing {
                    Some(existing) if existing == final_object_id => {
                        namespace_placements.push((root, link, true));
                    }
                    Some(_) => {
                        return Err(AppError::Conflict(format!(
                            "logical path '{}' already points to another object",
                            logical_path
                        )));
                    }
                    None if path_exists_no_follow(&link) => {
                        return Err(AppError::Conflict(format!(
                            "logical path '{}' already exists",
                            logical_path
                        )));
                    }
                    None => namespace_placements.push((root, link, false)),
                }
            }

            if final_object_id != physical_object_id {
                let _ =
                    index_store::rebind_unpublished_object_version(&metadata, &final_object_id)?;
            }

            let mut published: Vec<PathBuf> = Vec::new();
            for (root, link, already_present) in namespace_placements {
                if already_present {
                    continue;
                }
                if let Err(err) =
                    publish_namespace_link(&root, &fragment, &logical_path, &final_object_id)
                {
                    for published_link in published.into_iter().rev() {
                        let _ = fs::remove_file(&published_link);
                        if let Some(parent) = published_link.parent() {
                            index_store::sync_dir(parent);
                        }
                    }
                    return Err(err);
                }
                published.push(link);
            }
            Ok(final_object_id)
        })
        .await
        .map_err(|err| AppError::InternalServerError(format!("N1 namespace worker: {err}")))?;

        drop(guard);
        let mut locks = FINALIZE_LOCKS.lock().await;
        if locks.get(&lock_key).is_some_and(|existing| {
            Arc::ptr_eq(existing, &lock) && Arc::strong_count(existing) <= 2
        }) {
            locks.remove(&lock_key);
        }
        result
    }

    pub async fn bind_pending_upload(
        &self,
        fragment: &str,
        logical_path: &str,
        physical_object_id: &str,
        version_id: &str,
        session_id: &str,
    ) -> Result<(), AppError> {
        let binding = PendingUploadBinding {
            fragment: fragment.to_string(),
            logical_path: normalize_user_path(logical_path)?,
            physical_object_id: physical_object_id.to_string(),
            version_id: version_id.to_string(),
            session_id: session_id.to_string(),
            created_at_unix: Utc::now().timestamp(),
        };
        write_pending_binding(&binding)
    }

    pub async fn pending_upload(
        &self,
        fragment: &str,
        version_id: &str,
    ) -> Result<Option<PendingUploadBinding>, AppError> {
        index_store::read_pending_binding(fragment, version_id)
    }

    pub async fn list_pending_uploads(
        &self,
        fragment: &str,
    ) -> Result<Vec<PendingUploadBinding>, AppError> {
        index_store::list_pending_bindings(fragment)
    }

    pub async fn find_pending_upload_by_logical_path(
        &self,
        fragment: &str,
        logical_path: &str,
    ) -> Result<Option<PendingUploadBinding>, AppError> {
        let normalized = normalize_user_path(logical_path)?;
        Ok(index_store::list_pending_bindings(fragment)?
            .into_iter()
            .find(|binding| binding.logical_path == normalized))
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
        scan_trash: bool,
    ) -> Result<ListObjectsResult, AppError> {
        let limit = params
            .get("limit")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(100)
            .clamp(1, 1000);
        let prefix = params.get("prefix").cloned();
        let delimiter = params.get("delimiter").cloned();
        let after = params.get("continuation_token").cloned();
        if scan_trash {
            index_store::scan_trash_heads_page(fragment, prefix, delimiter, limit, after, true)
        } else {
            index_store::scan_live_heads_page(fragment, prefix, delimiter, limit, after, true)
        }
    }

    pub async fn list_live_object_refs_page(
        &self,
        fragment: &str,
        prefix: Option<&str>,
        continuation_token: Option<&str>,
        limit: usize,
    ) -> Result<NamespaceObjectRefPage, AppError> {
        let fragment = fragment.to_string();
        let prefix = prefix
            .map(normalize_user_path)
            .transpose()?
            .unwrap_or_default();
        let after = continuation_token.map(ToOwned::to_owned);
        tokio::task::spawn_blocking(move || {
            let candidate_limit = limit.saturating_add(1);
            let mut candidates = std::collections::BTreeMap::<String, String>::new();
            for root in index_roots() {
                let fragment_root = fragment_root_on(&root, &fragment);
                let base = fragment_root.join(&prefix);
                if !base.is_dir() {
                    continue;
                }
                let walker = WalkDir::new(&base)
                    .min_depth(1)
                    .follow_links(false)
                    .into_iter()
                    .filter_entry(|entry| {
                        entry.depth() == 0
                            || !internal_namespace_entry_name(
                                entry.file_name().to_string_lossy().as_ref(),
                            )
                    });
                for entry in walker {
                    let entry = entry.map_err(|error| {
                        AppError::InternalServerError(format!(
                            "walk N1 namespace directory {}: {error}",
                            base.display()
                        ))
                    })?;
                    if !entry.file_type().is_symlink() {
                        continue;
                    }
                    let relative = entry.path().strip_prefix(&base).map_err(|_| {
                        AppError::InternalServerError(format!(
                            "N1 namespace entry {} escaped {}",
                            entry.path().display(),
                            base.display()
                        ))
                    })?;
                    let relative = relative.to_string_lossy().replace('\\', "/");
                    let child_rel = if prefix.is_empty() {
                        relative
                    } else {
                        format!("{prefix}/{relative}")
                    };
                    if after
                        .as_ref()
                        .is_some_and(|cursor| child_rel.as_str() <= cursor.as_str())
                    {
                        continue;
                    }
                    let Some(object_id) =
                        read_namespace_symlink_object_id(&root, &fragment, entry.path())?
                    else {
                        continue;
                    };
                    candidates.entry(child_rel).or_insert(object_id);
                    if candidates.len() > candidate_limit {
                        candidates.pop_last();
                    }
                }
            }
            let has_more = candidates.len() > limit;
            if has_more {
                candidates.pop_last();
            }
            let mut entries = Vec::with_capacity(candidates.len());
            for (path, object_id) in candidates {
                let Some(meta) = index_store::read_current_live_head(&fragment, &object_id)? else {
                    continue;
                };
                entries.push(NamespaceObjectRef {
                    path,
                    object_id,
                    version_id: meta.version_id,
                });
            }
            let continuation_token = has_more
                .then(|| entries.last().map(|entry| entry.path.clone()))
                .flatten();
            Ok(NamespaceObjectRefPage {
                entries,
                continuation_token,
            })
        })
        .await
        .map_err(|err| AppError::InternalServerError(format!("N1 namespace worker: {err}")))?
    }
}

fn index_roots() -> Vec<PathBuf> {
    crate::core::disk_manager::ObjectPathManager::new().get_all_index_roots()
}

fn io_err(err: std::io::Error, context: &str) -> AppError {
    AppError::Io(err, context.to_string())
}

fn path_exists_no_follow(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok()
}

fn namespace_error_to_app(err: NamespaceError) -> AppError {
    match err {
        NamespaceError::InvalidPath(message) => AppError::BadRequest(message),
        NamespaceError::Conflict(message) => AppError::Conflict(message),
        NamespaceError::NotFound(_) => AppError::NotFound,
        NamespaceError::Invariant(message) => AppError::InternalServerError(message),
    }
}

fn normalize_user_path(path: &str) -> Result<String, AppError> {
    let normalized = normalize_path(path).map_err(namespace_error_to_app)?;
    reject_reserved_user_path(&normalized)?;
    Ok(normalized)
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

fn namespace_entry(
    fragment: &str,
    path: String,
    kind: NamespaceEntryKind,
    object_id: Option<String>,
    lifecycle: LifecycleState,
    trash_entry_id: Option<String>,
    deleted_at_unix: Option<i64>,
    size_time: Option<(i64, i64)>,
) -> NamespaceEntry {
    let parent = parent_path(&path);
    let name = base_name(&path);
    let (created, updated) = size_time.unwrap_or_else(|| {
        let now = Utc::now().timestamp();
        (now, now)
    });
    NamespaceEntry {
        fragment: fragment.to_string(),
        path,
        parent_path: parent,
        name,
        kind,
        object_id,
        size_bytes: None,
        version_id: None,
        packed_ext_u64: None,
        lifecycle,
        trash_entry_id,
        deleted_at_unix,
        created_at_unix: created,
        updated_at_unix: updated,
    }
}

fn object_head_from_meta(
    fragment: &str,
    lifecycle: LifecycleState,
    meta: ObjectMetadata,
) -> ObjectHead {
    ObjectHead {
        fragment: fragment.to_string(),
        object_id: meta.object_key.clone(),
        current_version_id: meta.version_id,
        storage_object_id: meta.object_key,
        content_type: meta.content_type,
        total_size_bytes: meta.total_size_bytes,
        lifecycle,
        created_at_unix: meta.created_at,
        updated_at_unix: meta.created_at,
    }
}

fn live_entry(fragment: &str, path: &str) -> Result<Option<NamespaceEntry>, AppError> {
    if path.is_empty() {
        return Ok(Some(root_entry(fragment)));
    }
    if internal_namespace_entry_name(base_name(path).as_str()) {
        return Ok(None);
    }
    for root in index_roots() {
        let direct = namespace_path(&root, fragment, path);
        if let Some(object_id) = read_namespace_symlink_object_id(&root, fragment, &direct)? {
            let Some(meta) = index_store::read_current_live_head(fragment, &object_id)? else {
                continue;
            };
            let size_time = Some((meta.created_at, meta.created_at));
            let mut entry = namespace_entry(
                fragment,
                path.to_string(),
                NamespaceEntryKind::File,
                Some(object_id),
                LifecycleState::Live,
                None,
                None,
                size_time,
            );
            entry.size_bytes = Some(meta.total_size_bytes);
            entry.version_id = Some(meta.version_id);
            return Ok(Some(entry));
        }
        if direct.is_dir() && !internal_namespace_entry_name(base_name(path).as_str()) {
            return Ok(Some(namespace_entry(
                fragment,
                path.to_string(),
                NamespaceEntryKind::Directory,
                None,
                LifecycleState::Live,
                None,
                None,
                None,
            )));
        }
    }
    Ok(None)
}

fn list_live_dir_page(
    fragment: &str,
    path: &str,
    limit: usize,
    after: Option<&str>,
) -> Result<(Vec<NamespaceEntry>, Option<String>), AppError> {
    if !path.is_empty() && live_entry(fragment, path)?.is_none() {
        return Ok((Vec::new(), None));
    }
    let candidate_limit = limit.saturating_add(1);
    let mut names = BTreeSet::new();
    for root in index_roots() {
        let dir = fragment_root_on(&root, fragment).join(path);
        if !dir.is_dir() {
            continue;
        }
        let scan = visit_dir_names(&dir, |name| {
            if internal_namespace_entry_name(&name) || name == ".trash_objects" {
                return Ok(true);
            }
            if after.is_some_and(|cursor| name.as_str() <= cursor) {
                return Ok(true);
            }
            let child = dir.join(&name);
            if read_namespace_symlink_object_id(&root, fragment, &child)?.is_none()
                && !child.is_dir()
            {
                return Ok(true);
            }
            names.insert(name);
            if names.len() > candidate_limit {
                names.pop_last();
            }
            Ok(true)
        });
        match scan {
            Ok(()) => {}
            Err(_) if !path_exists_no_follow(&dir) => continue,
            Err(err) => return Err(err),
        }
    }
    let has_more = names.len() > limit;
    if has_more {
        names.pop_last();
    }
    let mut entries = Vec::with_capacity(names.len());
    for name in names {
        let child_path = if path.is_empty() {
            name
        } else {
            format!("{path}/{name}")
        };
        if let Some(entry) = live_entry(fragment, &child_path)? {
            entries.push(entry);
        }
    }
    let next = has_more
        .then(|| entries.last().map(|entry| entry.name.clone()))
        .flatten();
    Ok((entries, next))
}

fn list_trash_dir_page(
    fragment: &str,
    path: &str,
    limit: usize,
    after: Option<&str>,
) -> Result<(Vec<NamespaceEntry>, Option<String>), AppError> {
    let candidate_limit = limit.saturating_add(1);
    let mut records = std::collections::BTreeMap::<String, TrashRecord>::new();
    visit_trash_records(fragment, |record| {
        if !path.is_empty()
            && record.original_path != path
            && !record.original_path.starts_with(&format!("{path}/"))
        {
            return Ok(true);
        }
        if after.is_some_and(|cursor| record.trash_entry_id.as_str() <= cursor) {
            return Ok(true);
        }
        records.insert(record.trash_entry_id.clone(), record);
        if records.len() > candidate_limit {
            records.pop_last();
        }
        Ok(true)
    })?;
    let has_more = records.len() > limit;
    if has_more {
        records.pop_last();
    }
    let mut entries = Vec::with_capacity(records.len());
    for record in records.into_values() {
        entries.push(trash_record_entry(fragment, &record)?);
    }
    let next = has_more
        .then(|| {
            entries
                .last()
                .and_then(|entry| entry.trash_entry_id.clone())
        })
        .flatten();
    Ok((entries, next))
}

fn trash_entry(fragment: &str, path: &str) -> Result<Option<NamespaceEntry>, AppError> {
    if path.is_empty() {
        return Ok(Some(namespace_entry(
            fragment,
            String::new(),
            NamespaceEntryKind::Directory,
            None,
            LifecycleState::trashed(0),
            None,
            None,
            Some((0, 0)),
        )));
    }
    if internal_namespace_entry_name(base_name(path).as_str()) {
        return Ok(None);
    }
    let record = scan_trash_records(fragment)?
        .into_iter()
        .filter(|record| record.original_path == path)
        .max_by(|left, right| {
            left.deleted_at_unix
                .cmp(&right.deleted_at_unix)
                .then_with(|| left.trash_entry_id.cmp(&right.trash_entry_id))
        });
    record
        .as_ref()
        .map(|record| trash_record_entry(fragment, record))
        .transpose()
}

fn trash_record_entry(fragment: &str, record: &TrashRecord) -> Result<NamespaceEntry, AppError> {
    let kind = match record.kind {
        TrashKind::File => NamespaceEntryKind::File,
        TrashKind::Directory => NamespaceEntryKind::Directory,
    };
    let object_id = record.object_keys.first().cloned();
    let mut entry = namespace_entry(
        fragment,
        record.original_path.clone(),
        kind,
        object_id.clone(),
        LifecycleState::trashed(record.deleted_at_unix),
        Some(record.trash_entry_id.clone()),
        Some(record.deleted_at_unix),
        Some((record.deleted_at_unix, record.deleted_at_unix)),
    );
    if let Some(object_id) = object_id
        && let Some(head) = index_store::read_current_live_head(fragment, &object_id)?
    {
        entry.size_bytes = Some(head.total_size_bytes);
        entry.version_id = Some(head.version_id);
        entry.created_at_unix = head.created_at;
        entry.updated_at_unix = head.created_at;
    }
    Ok(entry)
}

fn resolve_live_object_id_sync(fragment: &str, path: &str) -> Result<Option<String>, AppError> {
    let Some(entry) = live_entry(fragment, path)? else {
        return Ok(None);
    };
    if entry.kind == NamespaceEntryKind::File {
        return Ok(entry.object_id);
    }
    Ok(None)
}

fn rename_live_path(
    fragment: &str,
    source: &str,
    destination: &str,
) -> Result<NamespaceMutationSummary, AppError> {
    if live_entry(fragment, destination)?.is_some() {
        return Err(AppError::Conflict(format!(
            "destination path '{}' already exists",
            destination
        )));
    }
    let Some(entry) = live_entry(fragment, source)? else {
        return Err(AppError::NotFound);
    };
    for root in index_roots() {
        let fragment_root = fragment_root_on(&root, fragment);
        let destination_path = if entry.kind == NamespaceEntryKind::File {
            namespace_path(&root, fragment, destination)
        } else {
            fragment_root.join(destination)
        };
        if path_exists_no_follow(&destination_path) {
            return Err(AppError::Conflict(format!(
                "destination path '{}' already exists",
                destination
            )));
        }
    }
    for root in index_roots() {
        let fragment_root = fragment_root_on(&root, fragment);
        let source_path = if entry.kind == NamespaceEntryKind::File {
            namespace_path(&root, fragment, source)
        } else {
            fragment_root.join(source)
        };
        if !path_exists_no_follow(&source_path) {
            continue;
        }
        let destination_path = if entry.kind == NamespaceEntryKind::File {
            namespace_path(&root, fragment, destination)
        } else {
            fragment_root.join(destination)
        };
        if let Some(parent) = destination_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| io_err(err, "create rename destination parent"))?;
        }
        renameat(CWD, &source_path, CWD, &destination_path)
            .map_err(|err| AppError::InternalServerError(format!("N1 namespace rename: {err}")))?;
    }
    Ok(NamespaceMutationSummary {
        operation: "rename".to_string(),
        kind: entry.kind,
        affected_paths: 1,
        affected_objects: usize::from(entry.kind == NamespaceEntryKind::File),
        source_path: Some(source.to_string()),
        destination_path: Some(destination.to_string()),
        trash_entry_id: None,
        deleted_at_unix: None,
    })
}

fn trash_live_path(fragment: &str, path: &str) -> Result<NamespaceMutationSummary, AppError> {
    let Some(entry) = live_entry(fragment, path)? else {
        return Err(AppError::NotFound);
    };
    let affected_objects = usize::from(entry.kind == NamespaceEntryKind::File);
    let roots = index_roots();
    if roots.is_empty() {
        return Err(AppError::ServiceUnavailable(
            "N1 has no configured namespace roots".to_string(),
        ));
    }

    for root in &roots {
        let source = fragment_root_on(root, fragment).join(path);
        if !path_exists_no_follow(&source) {
            return Err(AppError::ServiceUnavailable(format!(
                "namespace replica for '{}' is missing on {}; repair the namespace before deleting",
                path,
                root.display()
            )));
        }
    }

    let path_depth = path.split('/').count();
    let mut unix_nanos = Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| Utc::now().timestamp().saturating_mul(1_000_000_000));
    let generation = loop {
        let candidate = trash_generation_name(unix_nanos, path_depth);
        let primary_trash = fragment_root_on(&roots[0], fragment).join(TRASH_DIR);
        fs::create_dir_all(&primary_trash).map_err(|err| io_err(err, "create N1 trash root"))?;
        match fs::create_dir(primary_trash.join(&candidate)) {
            Ok(()) => break candidate,
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                unix_nanos = unix_nanos.saturating_add(1);
            }
            Err(err) => return Err(io_err(err, "reserve N1 trash generation")),
        }
    };

    let mut reserved_roots = vec![roots[0].clone()];
    for root in roots.iter().skip(1) {
        let trash_root = fragment_root_on(root, fragment).join(TRASH_DIR);
        if let Err(err) = fs::create_dir_all(&trash_root)
            .and_then(|()| fs::create_dir(trash_root.join(&generation)))
        {
            for reserved in reserved_roots {
                let _ = fs::remove_dir(
                    fragment_root_on(&reserved, fragment)
                        .join(TRASH_DIR)
                        .join(&generation),
                );
            }
            return Err(io_err(err, "reserve replicated N1 trash generation"));
        }
        reserved_roots.push(root.clone());
    }

    let source_relative = format!("{generation}/{path}");
    for root in &roots {
        let destination = fragment_root_on(root, fragment)
            .join(TRASH_DIR)
            .join(&source_relative);
        if path_exists_no_follow(&destination) {
            return Err(AppError::Conflict(format!(
                "trash path '{}' already exists; recover it or wait for TTL reclamation",
                source_relative
            )));
        }
    }
    for root in roots {
        let fragment_root = fragment_root_on(&root, fragment);
        let src = fragment_root.join(path);
        if !path_exists_no_follow(&src) {
            continue;
        }
        let dst = fragment_root.join(TRASH_DIR).join(&source_relative);
        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent).map_err(|err| io_err(err, "create trash destination"))?;
        }
        renameat(CWD, &src, CWD, &dst)
            .map_err(|err| AppError::InternalServerError(format!("N1 namespace trash: {err}")))?;
    }
    Ok(NamespaceMutationSummary {
        operation: "trash".to_string(),
        kind: entry.kind,
        affected_paths: 1,
        affected_objects,
        source_path: Some(path.to_string()),
        destination_path: None,
        trash_entry_id: Some(source_relative),
        deleted_at_unix: Some(unix_nanos / 1_000_000_000),
    })
}

fn sweep_expired_trash_sync(
    fragment: &str,
    retention_seconds: u64,
    now_unix: i64,
) -> Result<TrashLifecycleSweepSummary, AppError> {
    let mut summary = TrashLifecycleSweepSummary::default();
    let retention = i64::try_from(retention_seconds).unwrap_or(i64::MAX);
    let mut reclaim_candidates = BTreeSet::new();
    for record in scan_trash_records(fragment)? {
        if now_unix < record.deleted_at_unix.saturating_add(retention) {
            continue;
        }
        if !index_store::remove_trash_payload(fragment, &record)? {
            continue;
        }
        reclaim_candidates.extend(record.object_keys);
        summary.expired_entries = summary.expired_entries.saturating_add(1);
        summary.expired_versions = summary
            .expired_versions
            .saturating_add(record.version_ids.len() as u64);
    }
    if !reclaim_candidates.is_empty() {
        summary.reclaimed_objects = index_store::delete_unreferenced_object_indexes_batch(
            fragment,
            &reclaim_candidates.into_iter().collect::<Vec<_>>(),
        )?
        .len() as u64;
    }
    Ok(summary)
}

fn select_trash_record(
    fragment: &str,
    path: &str,
    trash_entry_id: Option<&str>,
) -> Result<TrashRecord, AppError> {
    if path.is_empty() && trash_entry_id.is_none_or(|id| id.trim().is_empty()) {
        return Err(AppError::NotFound);
    }
    let records = scan_trash_records(fragment)?;
    if let Some(id) = trash_entry_id.filter(|id| !id.trim().is_empty()) {
        return records
            .into_iter()
            .find(|record| record.trash_entry_id == id)
            .ok_or(AppError::NotFound);
    }
    records
        .into_iter()
        .filter(|record| record.original_path == path)
        .max_by(|left, right| {
            left.deleted_at_unix
                .cmp(&right.deleted_at_unix)
                .then_with(|| left.trash_entry_id.cmp(&right.trash_entry_id))
        })
        .ok_or(AppError::NotFound)
}

fn recover_trash_path(
    fragment: &str,
    path: &str,
    trash_entry_id: Option<&str>,
) -> Result<NamespaceMutationSummary, AppError> {
    let record = select_trash_record(fragment, path, trash_entry_id)?;
    let source_relative = record.source_relative_path.as_str();
    let destination_relative = record.original_path.as_str();
    let roots = index_roots();
    let mut source_roots = Vec::new();
    for root in &roots {
        let source = fragment_root_on(root, fragment)
            .join(TRASH_DIR)
            .join(source_relative);
        match fs::symlink_metadata(&source) {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(io_err(err, "inspect N1 recover source")),
        }
        source_roots.push(root.clone());
    }
    if source_roots.is_empty() {
        return Err(AppError::NotFound);
    }
    let kind = match record.kind {
        TrashKind::File => NamespaceEntryKind::File,
        TrashKind::Directory => NamespaceEntryKind::Directory,
    };
    for root in &roots {
        let destination = fragment_root_on(root, fragment).join(destination_relative);
        if path_exists_no_follow(&destination) {
            return Err(AppError::Conflict(format!(
                "recover destination '{}' already exists",
                destination_relative
            )));
        }
    }
    let mut moved = false;
    for root in source_roots {
        let fragment_root = fragment_root_on(&root, fragment);
        let source = fragment_root.join(TRASH_DIR).join(source_relative);
        let destination = fragment_root.join(destination_relative);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).map_err(|err| io_err(err, "create recover destination"))?;
        }
        match renameat(CWD, &source, CWD, &destination) {
            Ok(()) => moved = true,
            Err(err) if err == rustix::io::Errno::NOENT && !moved => {
                return Err(AppError::NotFound);
            }
            Err(err) if err == rustix::io::Errno::NOENT => continue,
            Err(err) => {
                return Err(AppError::InternalServerError(format!(
                    "N1 namespace recover: {err}"
                )));
            }
        }
        prune_empty_trash_parents(&fragment_root.join(TRASH_DIR), source_relative);
    }
    if !moved {
        return Err(AppError::NotFound);
    }
    Ok(NamespaceMutationSummary {
        operation: "recover".to_string(),
        kind,
        affected_paths: 1,
        affected_objects: usize::from(kind == NamespaceEntryKind::File),
        source_path: Some(record.original_path.clone()),
        destination_path: Some(record.original_path.clone()),
        trash_entry_id: Some(record.trash_entry_id),
        deleted_at_unix: Some(record.deleted_at_unix),
    })
}

fn prune_empty_trash_parents(trash_root: &Path, source_relative_path: &str) {
    let mut current = trash_root.join(source_relative_path);
    current.pop();
    while current != trash_root {
        match fs::remove_dir(&current) {
            Ok(()) => {
                if !current.pop() {
                    break;
                }
            }
            Err(_) => break,
        }
    }
}

pub fn logical_not_found() -> AppError {
    AppError::NotFound
}

pub fn unexpected_response(status: axum::http::StatusCode, context: &str) -> AppError {
    AppError::InternalServerError(format!("{context}: unexpected response status {status}"))
}

#[cfg(test)]
mod tests;
