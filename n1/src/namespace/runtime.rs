use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use chrono::Utc;
use rustix::fs::{CWD, renameat};
use serde::Serialize;

use crate::core::disk_manager::{ListObjectsResult, ObjectMetadata};
use crate::core::index_store::{
    self, EdgeTrashKind, EdgeTrashRecord, PendingUploadBinding, TRASH_DIR, fragment_root_on,
    internal_namespace_entry_name, latest_summary_metadata_in_object_dir, live_object_dir,
    namespace_path, new_object_id, publish_namespace_link, read_dir_names,
    read_namespace_symlink_object_id, reject_reserved_user_path, scan_trash_records,
    write_pending_binding,
};
use crate::enums::app_error_enum::AppError;
use crate::namespace::models::{
    LifecycleState, NamespaceEntry, NamespaceEntryKind, NamespaceError, NamespaceMutationSummary,
    NamespaceReachabilitySnapshot, ObjectHead,
};
use crate::namespace::paths::{base_name, is_same_or_descendant, normalize_path, parent_path};

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
        tokio::task::spawn_blocking(move || {
            Ok(scan_trash_records(&fragment)?
                .into_iter()
                .find(|record| record.original_path == path)
                .map(|record| trash_record_entry(&fragment, &record)))
        })
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
                fs::create_dir_all(&dir).map_err(|e| io_err(e, "create N1 namespace dir"))?;
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

    pub async fn purge_trash_path(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<NamespaceMutationSummary, AppError> {
        let fragment = fragment.to_string();
        let path = normalize_user_path(path)?;
        tokio::task::spawn_blocking(move || purge_trash(&fragment, &path, None))
            .await
            .map_err(|err| AppError::InternalServerError(format!("N1 namespace worker: {err}")))?
    }

    pub async fn purge_trash_entry_id(
        &self,
        fragment: &str,
        trash_entry_id: &str,
    ) -> Result<NamespaceMutationSummary, AppError> {
        let fragment = fragment.to_string();
        let trash_entry_id = trash_entry_id.to_string();
        tokio::task::spawn_blocking(move || purge_trash(&fragment, "", Some(&trash_entry_id)))
            .await
            .map_err(|err| AppError::InternalServerError(format!("N1 namespace worker: {err}")))?
    }

    pub async fn recover_all(&self, fragment: &str) -> Result<NamespaceMutationSummary, AppError> {
        let records = scan_trash_records(fragment)?;
        let mut affected = 0usize;
        for record in records {
            affected += self
                .recover_trash_entry_id(fragment, &record.trash_entry_id)
                .await?
                .affected_objects;
        }
        Ok(NamespaceMutationSummary {
            operation: "recover_all".to_string(),
            kind: NamespaceEntryKind::Directory,
            affected_paths: affected,
            affected_objects: affected,
            source_path: None,
            destination_path: None,
            trash_entry_id: None,
            deleted_at_unix: None,
        })
    }

    pub async fn empty_trash(&self, fragment: &str) -> Result<NamespaceMutationSummary, AppError> {
        let records = scan_trash_records(fragment)?;
        let object_ids = records
            .iter()
            .flat_map(|record| record.object_keys.iter().cloned())
            .collect::<BTreeSet<_>>();
        index_store::drop_fragment_trash_namespace(fragment)?;
        for object_id in &object_ids {
            let _ = index_store::delete_unreferenced_object_index(fragment, object_id)?;
        }
        Ok(NamespaceMutationSummary {
            operation: "empty_trash".to_string(),
            kind: NamespaceEntryKind::Directory,
            affected_paths: 0,
            affected_objects: object_ids.len(),
            source_path: None,
            destination_path: None,
            trash_entry_id: None,
            deleted_at_unix: Some(Utc::now().timestamp()),
        })
    }

    pub async fn reachability_snapshot_if_present(
        &self,
        fragment: &str,
    ) -> Result<Option<NamespaceReachabilitySnapshot>, AppError> {
        let mut snapshot = NamespaceReachabilitySnapshot::default();
        for object_id in index_store::referenced_object_ids(fragment)? {
            for meta in index_store::list_live_versions(fragment, &object_id)? {
                snapshot.referenced_object_ids.insert(object_id.clone());
                snapshot.referenced_version_ids.insert(meta.version_id);
            }
        }
        for record in scan_trash_records(fragment)? {
            snapshot
                .referenced_object_ids
                .extend(record.object_keys.iter().cloned());
            snapshot
                .referenced_version_ids
                .extend(record.version_ids.iter().cloned());
        }
        Ok(Some(snapshot))
    }

    pub async fn purged_object_ids_for_gc(
        &self,
        _fragment: &str,
    ) -> Result<BTreeSet<String>, AppError> {
        Ok(BTreeSet::new())
    }

    pub async fn purge_unreferenced_object_ids(
        &self,
        _fragment: &str,
        _object_ids: &BTreeSet<String>,
    ) -> Result<usize, AppError> {
        Ok(0)
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
        for record in scan_trash_records(fragment)? {
            if record.original_path == normalized {
                return Ok(record.object_keys.first().cloned());
            }
        }
        Ok(None)
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
        let mut metadata = metadata.clone();
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

            if final_object_id != physical_object_id {
                metadata.object_key = final_object_id.clone();
                index_store::upsert_live_version(&metadata)?;
                index_store::delete_unreferenced_object_index(&fragment, &physical_object_id)?;
            }

            for root in index_roots() {
                publish_namespace_link(&root, &fragment, &logical_path, &final_object_id)?;
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
            let mut entries = Vec::new();
            let mut seen = HashSet::new();
            for root in index_roots() {
                let fragment_root = fragment_root_on(&root, &fragment);
                let base = fragment_root.join(&prefix);
                let mut stack = vec![(base, prefix.clone())];
                while let Some((dir, rel)) = stack.pop() {
                    if !dir.is_dir() {
                        continue;
                    }
                    for name in read_dir_names(&dir)? {
                        if internal_namespace_entry_name(&name) {
                            continue;
                        }
                        let child = dir.join(&name);
                        let child_rel = if rel.is_empty() {
                            name.clone()
                        } else {
                            format!("{rel}/{name}")
                        };
                        if after
                            .as_ref()
                            .is_some_and(|cursor| child_rel.as_str() <= cursor.as_str())
                        {
                            continue;
                        }
                        if let Some(object_id) =
                            read_namespace_symlink_object_id(&root, &fragment, &child)?
                        {
                            if !seen.insert(child_rel.clone()) {
                                continue;
                            }
                            let object_dir = live_object_dir(&root, &fragment, &object_id)?;
                            let Some(meta) = latest_summary_metadata_in_object_dir(
                                &fragment,
                                &object_id,
                                &object_dir,
                            )?
                            else {
                                continue;
                            };
                            entries.push(NamespaceObjectRef {
                                path: child_rel,
                                object_id,
                                version_id: meta.version_id,
                            });
                        } else if child.is_dir() {
                            stack.push((child, child_rel));
                        }
                    }
                }
            }
            entries.sort_by(|a, b| a.path.cmp(&b.path));
            let continuation_token = if entries.len() > limit {
                entries.truncate(limit);
                entries.last().map(|entry| entry.path.clone())
            } else {
                None
            };
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
            let object_dir = live_object_dir(&root, fragment, &object_id)?;
            let Some(meta) =
                latest_summary_metadata_in_object_dir(fragment, &object_id, &object_dir)?
            else {
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
    let mut entries = Vec::new();
    let mut seen = HashSet::new();
    for root in index_roots() {
        let dir = fragment_root_on(&root, fragment).join(path);
        if !dir.is_dir() {
            continue;
        }
        for name in read_dir_names(&dir)? {
            if internal_namespace_entry_name(&name) || name == ".trash_objects" {
                continue;
            }
            let display_name = name.clone();
            if after.is_some_and(|cursor| display_name.as_str() <= cursor) {
                continue;
            }
            let child_path = if path.is_empty() {
                display_name.clone()
            } else {
                format!("{path}/{display_name}")
            };
            let child = dir.join(&name);
            if let Some(object_id) = read_namespace_symlink_object_id(&root, fragment, &child)? {
                if !seen.insert(display_name.clone()) {
                    continue;
                }
                let object_dir = live_object_dir(&root, fragment, &object_id)?;
                let Some(meta) =
                    latest_summary_metadata_in_object_dir(fragment, &object_id, &object_dir)?
                else {
                    continue;
                };
                let size_time = Some((meta.created_at, meta.created_at));
                let mut entry = namespace_entry(
                    fragment,
                    child_path,
                    NamespaceEntryKind::File,
                    Some(object_id),
                    LifecycleState::Live,
                    None,
                    None,
                    size_time,
                );
                entry.size_bytes = Some(meta.total_size_bytes);
                entry.version_id = Some(meta.version_id);
                entries.push(entry);
            } else if child.is_dir() {
                if !seen.insert(display_name.clone()) {
                    continue;
                }
                entries.push(namespace_entry(
                    fragment,
                    child_path,
                    NamespaceEntryKind::Directory,
                    None,
                    LifecycleState::Live,
                    None,
                    None,
                    None,
                ));
            }
        }
    }
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    let next = if entries.len() > limit {
        entries.truncate(limit);
        entries.last().map(|entry| entry.name.clone())
    } else {
        None
    };
    Ok((entries, next))
}

fn list_trash_dir_page(
    fragment: &str,
    path: &str,
    limit: usize,
    after: Option<&str>,
) -> Result<(Vec<NamespaceEntry>, Option<String>), AppError> {
    let mut entries = Vec::new();
    for record in scan_trash_records(fragment)? {
        if !path.is_empty() && !record.original_path.starts_with(path) {
            continue;
        }
        if after.is_some_and(|cursor| record.original_path.as_str() <= cursor) {
            continue;
        }
        entries.push(trash_record_entry(fragment, &record));
    }
    entries.sort_by(|a, b| {
        a.path
            .cmp(&b.path)
            .then(a.trash_entry_id.cmp(&b.trash_entry_id))
    });
    let next = if entries.len() > limit {
        entries.truncate(limit);
        entries.last().map(|entry| entry.path.clone())
    } else {
        None
    };
    Ok((entries, next))
}

fn trash_record_entry(fragment: &str, record: &EdgeTrashRecord) -> NamespaceEntry {
    namespace_entry(
        fragment,
        record.original_path.clone(),
        match record.kind {
            EdgeTrashKind::File => NamespaceEntryKind::File,
            EdgeTrashKind::Directory => NamespaceEntryKind::Directory,
        },
        record.object_keys.first().cloned(),
        LifecycleState::trashed(record.deleted_at_unix),
        Some(record.trash_entry_id.clone()),
        Some(record.deleted_at_unix),
        Some((record.deleted_at_unix, record.deleted_at_unix)),
    )
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
                .map_err(|e| io_err(e, "create rename destination parent"))?;
        }
        renameat(CWD, &source_path, CWD, &destination_path)
            .map_err(|e| AppError::InternalServerError(format!("N1 namespace rename: {e}")))?;
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

fn collect_object_refs_under(
    fragment: &str,
    path: &str,
    kind: NamespaceEntryKind,
) -> Result<(Vec<String>, Vec<String>), AppError> {
    let mut object_keys = BTreeSet::new();
    let mut version_ids = BTreeSet::new();
    if kind == NamespaceEntryKind::File {
        if let Some(object_id) = resolve_live_object_id_sync(fragment, path)? {
            object_keys.insert(object_id.clone());
            for meta in index_store::list_live_versions(fragment, &object_id)? {
                version_ids.insert(meta.version_id);
            }
        }
    } else {
        for root in index_roots() {
            let mut stack = vec![namespace_path(&root, fragment, path)];
            while let Some(dir) = stack.pop() {
                if !dir.is_dir() {
                    continue;
                }
                for name in read_dir_names(&dir)? {
                    if internal_namespace_entry_name(&name) {
                        continue;
                    }
                    let child = dir.join(&name);
                    if let Some(object_id) =
                        read_namespace_symlink_object_id(&root, fragment, &child)?
                    {
                        if object_keys.insert(object_id.clone()) {
                            for meta in index_store::list_live_versions(fragment, &object_id)? {
                                version_ids.insert(meta.version_id);
                            }
                        }
                    } else if child.is_dir() {
                        stack.push(child);
                    }
                }
            }
        }
    }
    Ok((
        object_keys.into_iter().collect(),
        version_ids.into_iter().collect(),
    ))
}

fn trash_live_path(fragment: &str, path: &str) -> Result<NamespaceMutationSummary, AppError> {
    let Some(entry) = live_entry(fragment, path)? else {
        return Err(AppError::NotFound);
    };
    let deleted_at = Utc::now().timestamp();
    let source_relative = path.to_string();
    let (object_keys, version_ids) = collect_object_refs_under(fragment, path, entry.kind)?;
    for root in index_roots() {
        let fragment_root = fragment_root_on(&root, fragment);
        let src = fragment_root.join(&source_relative);
        if !path_exists_no_follow(&src) {
            continue;
        }
        let dst = fragment_root.join(TRASH_DIR).join(&source_relative);
        if path_exists_no_follow(&dst) {
            return Err(AppError::Conflict(format!(
                "trash path '{}' already exists; hard delete or recover it first",
                source_relative
            )));
        }
        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent).map_err(|e| io_err(e, "create trash destination"))?;
        }
        renameat(CWD, &src, CWD, &dst)
            .map_err(|e| AppError::InternalServerError(format!("N1 namespace trash: {e}")))?;
    }
    let record = EdgeTrashRecord {
        trash_entry_id: source_relative.clone(),
        original_path: path.to_string(),
        source_relative_path: source_relative,
        kind: if entry.kind == NamespaceEntryKind::File {
            EdgeTrashKind::File
        } else {
            EdgeTrashKind::Directory
        },
        object_keys,
        version_ids,
        deleted_at_unix: deleted_at,
    };
    index_store::write_trash_record(fragment, &record)?;
    Ok(NamespaceMutationSummary {
        operation: "trash".to_string(),
        kind: entry.kind,
        affected_paths: 1,
        affected_objects: record.object_keys.len(),
        source_path: Some(path.to_string()),
        destination_path: None,
        trash_entry_id: Some(record.trash_entry_id),
        deleted_at_unix: Some(deleted_at),
    })
}

fn select_trash_record(
    fragment: &str,
    path: &str,
    trash_entry_id: Option<&str>,
) -> Result<EdgeTrashRecord, AppError> {
    let mut records = scan_trash_records(fragment)?
        .into_iter()
        .filter(|record| {
            trash_entry_id
                .map(|id| record.trash_entry_id == id)
                .unwrap_or(record.original_path == path)
        })
        .collect::<Vec<_>>();
    if records.is_empty() {
        return Err(AppError::NotFound);
    }
    if trash_entry_id.is_none() && records.len() > 1 {
        return Err(AppError::Conflict(format!(
            "trash path '{}' is ambiguous; use trash_entry_id",
            path
        )));
    }
    Ok(records.remove(0))
}

fn recover_trash_path(
    fragment: &str,
    path: &str,
    trash_entry_id: Option<&str>,
) -> Result<NamespaceMutationSummary, AppError> {
    let record = select_trash_record(fragment, path, trash_entry_id)?;
    for root in index_roots() {
        let fragment_root = fragment_root_on(&root, fragment);
        let src = fragment_root
            .join(TRASH_DIR)
            .join(&record.source_relative_path);
        if !path_exists_no_follow(&src) {
            continue;
        }
        let dst = fragment_root.join(&record.source_relative_path);
        if path_exists_no_follow(&dst) {
            return Err(AppError::Conflict(format!(
                "recover destination '{}' already exists",
                record.original_path
            )));
        }
        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent).map_err(|e| io_err(e, "create recover destination"))?;
        }
        renameat(CWD, &src, CWD, &dst)
            .map_err(|e| AppError::InternalServerError(format!("N1 namespace recover: {e}")))?;
        prune_empty_trash_parents(&fragment_root.join(TRASH_DIR), &record.source_relative_path);
    }
    let _ = index_store::remove_trash_record(fragment, &record.trash_entry_id)?;
    Ok(NamespaceMutationSummary {
        operation: "recover".to_string(),
        kind: if record.kind == EdgeTrashKind::File {
            NamespaceEntryKind::File
        } else {
            NamespaceEntryKind::Directory
        },
        affected_paths: 1,
        affected_objects: record.object_keys.len(),
        source_path: Some(record.original_path.clone()),
        destination_path: Some(record.original_path),
        trash_entry_id: Some(record.trash_entry_id),
        deleted_at_unix: Some(record.deleted_at_unix),
    })
}

fn purge_trash(
    fragment: &str,
    path: &str,
    trash_entry_id: Option<&str>,
) -> Result<NamespaceMutationSummary, AppError> {
    let record = select_trash_record(fragment, path, trash_entry_id)?;
    for root in index_roots() {
        let trash_root = fragment_root_on(&root, fragment)
            .join(TRASH_DIR)
            .join(&record.source_relative_path);
        if path_exists_no_follow(&trash_root) {
            let meta = fs::symlink_metadata(&trash_root)
                .map_err(|e| io_err(e, "inspect N1 trash target"))?;
            if meta.file_type().is_symlink() || meta.is_file() {
                fs::remove_file(&trash_root).map_err(|e| io_err(e, "purge N1 trash file"))?;
            } else {
                fs::remove_dir_all(&trash_root).map_err(|e| io_err(e, "purge N1 trash"))?;
            }
            prune_empty_trash_parents(
                &fragment_root_on(&root, fragment).join(TRASH_DIR),
                &record.source_relative_path,
            );
        }
    }
    let _ = index_store::remove_trash_record(fragment, &record.trash_entry_id)?;
    for object_id in &record.object_keys {
        let _ = index_store::delete_unreferenced_object_index(fragment, object_id)?;
    }
    Ok(NamespaceMutationSummary {
        operation: "purge_trash".to_string(),
        kind: if record.kind == EdgeTrashKind::File {
            NamespaceEntryKind::File
        } else {
            NamespaceEntryKind::Directory
        },
        affected_paths: 1,
        affected_objects: record.object_keys.len(),
        source_path: Some(record.original_path),
        destination_path: None,
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
mod tests {
    use super::*;
    use crate::core::disk_manager::{INDEX_STORE_DISKS, ObjectMetadata};
    use crate::core::index_store;
    use ulid::Ulid;

    fn ensure_test_index_roots() -> Vec<PathBuf> {
        let base = std::env::temp_dir().join("n1-index-tests");
        let roots = vec![base.join("index-00"), base.join("index-01")];
        if INDEX_STORE_DISKS.get().is_none() {
            INDEX_STORE_DISKS
                .set(
                    roots
                        .iter()
                        .map(|path| path.display().to_string())
                        .collect::<Vec<_>>(),
                )
                .expect("set N1 test index roots");
        }
        let configured = INDEX_STORE_DISKS.get().expect("N1 test roots");
        for root in configured {
            std::fs::create_dir_all(root).expect("create N1 test root");
        }
        configured.iter().map(PathBuf::from).collect()
    }

    fn test_metadata(fragment: &str, object_key: &str, size: u64) -> ObjectMetadata {
        ObjectMetadata {
            object_key: object_key.to_string(),
            fragment: fragment.to_string(),
            version_id: Ulid::new().to_string(),
            upload_session_id: "session".to_string(),
            owner_uid: "owner".to_string(),
            content_type: "application/octet-stream".to_string(),
            total_size_bytes: size,
            created_at: Utc::now().timestamp(),
            parts: Vec::new(),
            is_delete_marker: false,
            stripe_size_bytes: crate::core::disk_manager::default_stripe_size(),
        }
    }

    fn publish_logical_object(
        roots: &[PathBuf],
        fragment: &str,
        logical_path: &str,
        size: u64,
    ) -> String {
        let object_id = new_object_id();
        index_store::upsert_live_version(&test_metadata(fragment, &object_id, size))
            .expect("publish N1 ndx");
        for root in roots {
            publish_namespace_link(root, fragment, logical_path, &object_id)
                .expect("publish namespace link");
        }
        object_id
    }

    #[tokio::test]
    async fn n1_file_rename_preserves_object_id_and_version_lineage() {
        let _guard = crate::test_support::global_test_lock().lock().await;
        let roots = ensure_test_index_roots();
        let fragment = format!("n1-rename-{}", Ulid::new());
        let object_id = publish_logical_object(&roots, &fragment, "video/old.mp4", 123);

        let renamed = global_namespace_runtime()
            .rename(&fragment, "video/old.mp4", "video/new.mp4")
            .await
            .expect("rename file symlink");
        assert_eq!(renamed.operation, "rename");

        assert_eq!(
            global_namespace_runtime()
                .resolve_live_object_id(&fragment, "video/new.mp4")
                .await
                .expect("resolve renamed object"),
            Some(object_id.clone())
        );
        assert!(
            global_namespace_runtime()
                .entry(&fragment, "video/old.mp4")
                .await
                .expect("old entry lookup")
                .is_none()
        );

        let selected = global_namespace_runtime()
            .select_object_id_for_write(&fragment, "video/new.mp4")
            .await
            .expect("select object id for overwrite");
        assert_eq!(selected, object_id);

        index_store::upsert_live_version(&test_metadata(&fragment, &selected, 456))
            .expect("publish second version");
        assert_eq!(
            index_store::list_live_versions(&fragment, &selected)
                .expect("live versions")
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn n1_concurrent_finalize_keeps_same_path_version_lineage() {
        let _guard = crate::test_support::global_test_lock().lock().await;
        let _roots = ensure_test_index_roots();
        let fragment = format!("n1-finalize-race-{}", Ulid::new());
        let logical_path = "docs/shared.bin";
        let first_object_id = new_object_id();
        let second_object_id = new_object_id();
        let first = test_metadata(&fragment, &first_object_id, 111);
        let second = test_metadata(&fragment, &second_object_id, 222);
        index_store::upsert_live_version(&first).expect("publish first provisional version");
        index_store::upsert_live_version(&second).expect("publish second provisional version");

        let committed_first = global_namespace_runtime()
            .finalize_logical_upload(&fragment, logical_path, &first_object_id, &first)
            .await
            .expect("finalize first writer");
        assert_eq!(committed_first, first_object_id);

        let committed_second = global_namespace_runtime()
            .finalize_logical_upload(&fragment, logical_path, &second_object_id, &second)
            .await
            .expect("finalize second writer under existing path");
        assert_eq!(committed_second, first_object_id);

        assert_eq!(
            global_namespace_runtime()
                .resolve_live_object_id(&fragment, logical_path)
                .await
                .expect("resolve shared path"),
            Some(first_object_id.clone())
        );
        let versions =
            index_store::list_live_versions(&fragment, &first_object_id).expect("live versions");
        assert_eq!(versions.len(), 2);
        assert!(
            versions
                .iter()
                .all(|meta| meta.object_key == first_object_id)
        );
        assert!(
            index_store::read_current_live_head(&fragment, &second_object_id)
                .expect("read stale provisional object")
                .is_none()
        );
    }

    #[tokio::test]
    async fn n1_folder_move_keeps_symlink_object_pointers_valid() {
        let _guard = crate::test_support::global_test_lock().lock().await;
        let roots = ensure_test_index_roots();
        let fragment = format!("n1-folder-move-{}", Ulid::new());
        let object_id = publish_logical_object(&roots, &fragment, "docs/a/video.mp4", 77);

        global_namespace_runtime()
            .mkdir(&fragment, "archive")
            .await
            .expect("mkdir archive");
        global_namespace_runtime()
            .rename(&fragment, "docs", "archive/docs")
            .await
            .expect("rename folder");

        assert_eq!(
            global_namespace_runtime()
                .resolve_live_object_id(&fragment, "archive/docs/a/video.mp4")
                .await
                .expect("resolve moved symlink"),
            Some(object_id.clone())
        );
        for root in &roots {
            let link = root.join(&fragment).join("archive/docs/a/video.mp4");
            let target = fs::read_link(&link).expect("read namespace link");
            assert_eq!(
                target,
                PathBuf::from(format!("@o_{object_id}")),
                "namespace links store stable object ids, not depth-relative paths"
            );
        }
    }

    #[tokio::test]
    async fn n1_soft_delete_moves_symlink_to_trash_and_purge_removes_unreferenced_object_index() {
        let _guard = crate::test_support::global_test_lock().lock().await;
        let roots = ensure_test_index_roots();
        let fragment = format!("n1-trash-{}", Ulid::new());
        let object_id = publish_logical_object(&roots, &fragment, "video/my-video.mp4", 123);

        let summary = global_namespace_runtime()
            .trash(&fragment, "video/my-video.mp4")
            .await
            .expect("soft delete");
        assert_eq!(summary.operation, "trash");
        assert_eq!(summary.affected_objects, 1);

        for root in &roots {
            assert!(!path_exists_no_follow(
                &root.join(&fragment).join("video/my-video.mp4")
            ));
            assert!(path_exists_no_follow(
                &root
                    .join(&fragment)
                    .join(TRASH_DIR)
                    .join("video/my-video.mp4")
            ));
            assert!(
                live_object_dir(root, &fragment, &object_id)
                    .expect("object dir")
                    .is_dir(),
                "soft delete must not move immutable object index"
            );
        }

        global_namespace_runtime()
            .purge_trash_path(&fragment, "video/my-video.mp4")
            .await
            .expect("hard delete single trash item");
        for root in &roots {
            assert!(!path_exists_no_follow(
                &root
                    .join(&fragment)
                    .join(TRASH_DIR)
                    .join("video/my-video.mp4")
            ));
            assert!(
                !live_object_dir(root, &fragment, &object_id)
                    .expect("object dir")
                    .exists(),
                "hard delete removes unreferenced object index so GC can reclaim blobs"
            );
        }
    }

    #[tokio::test]
    async fn n1_posix_pagination_uses_display_names_not_raw_object_dirs() {
        let _guard = crate::test_support::global_test_lock().lock().await;
        let roots = ensure_test_index_roots();
        let fragment = format!("n1-posix-page-{}", Ulid::new());
        for (name, size) in [
            ("docs/a.svg", 10u64),
            ("docs/b.svg", 20u64),
            ("docs/c.svg", 30u64),
        ] {
            publish_logical_object(&roots, &fragment, name, size);
        }

        let (first, token) = global_namespace_runtime()
            .list_dir_page(&fragment, "docs", false, 2, None)
            .await
            .expect("first page");
        assert_eq!(
            first
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["a.svg", "b.svg"]
        );
        let (second, token2) = global_namespace_runtime()
            .list_dir_page(&fragment, "docs", false, 2, token.as_deref())
            .await
            .expect("second page");
        assert_eq!(
            second
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["c.svg"]
        );
        assert!(token2.is_none());

        for root in &roots {
            let empty_object_dir =
                crate::core::index_store::live_object_dir(root, &fragment, &new_object_id())
                    .expect("empty object dir");
            std::fs::create_dir_all(empty_object_dir).expect("empty object dir");
        }
        let (entries, _) = global_namespace_runtime()
            .list_dir_page(&fragment, "docs", false, 10, None)
            .await
            .expect("page with empty object dir");
        assert!(!entries.iter().any(|entry| entry.name == "empty.svg"));
    }

    #[tokio::test]
    async fn n1_trash_list_shows_only_explicit_file_delete_anchor() {
        let _guard = crate::test_support::global_test_lock().lock().await;
        let roots = ensure_test_index_roots();
        let fragment = format!("n1-trash-file-anchor-{}", Ulid::new());
        publish_logical_object(&roots, &fragment, "videos/clip.mp4", 123);

        global_namespace_runtime()
            .trash(&fragment, "videos/clip.mp4")
            .await
            .expect("trash file");

        let (entries, token) = global_namespace_runtime()
            .list_dir_page(&fragment, "", true, 10, None)
            .await
            .expect("list trash");
        assert!(token.is_none());
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "videos/clip.mp4");
        assert_eq!(entries[0].kind, NamespaceEntryKind::File);
    }

    #[tokio::test]
    async fn n1_trash_list_shows_only_explicit_folder_delete_anchor() {
        let _guard = crate::test_support::global_test_lock().lock().await;
        let roots = ensure_test_index_roots();
        let fragment = format!("n1-trash-folder-anchor-{}", Ulid::new());
        publish_logical_object(&roots, &fragment, "videos/clip.mp4", 123);
        publish_logical_object(&roots, &fragment, "videos/deep/other.mp4", 456);

        global_namespace_runtime()
            .trash(&fragment, "videos")
            .await
            .expect("trash folder");

        let (entries, token) = global_namespace_runtime()
            .list_dir_page(&fragment, "", true, 10, None)
            .await
            .expect("list trash");
        assert!(token.is_none());
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "videos");
        assert_eq!(entries[0].kind, NamespaceEntryKind::Directory);
    }
}
