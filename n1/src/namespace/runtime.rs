use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use chrono::Utc;
use rustix::fs::{CWD, renameat};
use serde::Serialize;

use crate::core::disk_manager::{ListObjectsResult, ObjectMetadata};
use crate::core::index_store::{
    self, PendingUploadBinding, TRASH_DIR, TRASH_ENTRIES_DIR, TrashKind, TrashRecord,
    fragment_root_on, internal_namespace_entry_name, namespace_path, new_object_id,
    publish_namespace_link, read_dir_names, read_namespace_symlink_object_id,
    reject_reserved_user_path, scan_trash_records, write_pending_binding,
};
use crate::enums::app_error_enum::AppError;
use crate::namespace::models::{
    LifecycleState, NamespaceEntry, NamespaceEntryKind, NamespaceError, NamespaceMutationSummary,
    NamespaceReachabilitySnapshot, ObjectHead,
};
use crate::namespace::paths::{base_name, is_same_or_descendant, normalize_path, parent_path};
#[cfg(not(test))]
use crate::settings::config::CONFIG;
use ulid::Ulid;

static GLOBAL_NAMESPACE_RUNTIME: LazyLock<NamespaceRuntime> = LazyLock::new(NamespaceRuntime::new);
static FINALIZE_LOCKS: LazyLock<tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> =
    LazyLock::new(|| tokio::sync::Mutex::new(HashMap::new()));
const NAMESPACE_MUTATION_LOCK_STRIPES: usize = 257;
static NAMESPACE_MUTATION_LOCKS: LazyLock<Vec<Arc<tokio::sync::Mutex<()>>>> = LazyLock::new(|| {
    (0..NAMESPACE_MUTATION_LOCK_STRIPES)
        .map(|_| Arc::new(tokio::sync::Mutex::new(())))
        .collect()
});

pub(crate) async fn namespace_mutation_guard(fragment: &str) -> tokio::sync::OwnedMutexGuard<()> {
    let mut hasher = DefaultHasher::new();
    fragment.hash(&mut hasher);
    let stripe = (hasher.finish() as usize) % NAMESPACE_MUTATION_LOCKS.len();
    NAMESPACE_MUTATION_LOCKS[stripe].clone().lock_owned().await
}

#[cfg(not(test))]
fn configured_trash_retention_seconds() -> u64 {
    CONFIG.garbage_collector.trash_retention_seconds
}

#[cfg(test)]
fn configured_trash_retention_seconds() -> u64 {
    30 * 24 * 60 * 60
}

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
    pub pending_receipts_replayed: u64,
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
        let _mutation_guard = namespace_mutation_guard(&fragment).await;
        tokio::task::spawn_blocking(move || {
            if live_entry(&fragment, &path)?.is_some() {
                return Ok(false);
            }
            let roots = index_roots();
            let created = roots
                .iter()
                .any(|root| !fragment_root_on(root, &fragment).join(&path).exists());
            if created {
                index_store::advance_namespace_generation(&fragment)?;
            }
            for root in roots {
                let fragment_root = fragment_root_on(&root, &fragment);
                let dir = fragment_root.join(&path);
                index_store::ensure_namespace_directory(&fragment_root, &dir)?;
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
        let _mutation_guard = namespace_mutation_guard(&fragment).await;
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
        let _mutation_guard = namespace_mutation_guard(&fragment).await;
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
        let _mutation_guard = namespace_mutation_guard(&fragment).await;
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
        let _mutation_guard = namespace_mutation_guard(&fragment).await;
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
        let _mutation_guard = namespace_mutation_guard(&fragment).await;
        tokio::task::spawn_blocking(move || {
            sweep_expired_trash_sync(&fragment, retention_seconds, now_unix)
        })
        .await
        .map_err(|err| AppError::InternalServerError(format!("N1 lifecycle worker: {err}")))?
    }

    pub async fn recover_all(&self, fragment: &str) -> Result<NamespaceMutationSummary, AppError> {
        let fragment_name = fragment.to_string();
        let _mutation_guard = namespace_mutation_guard(&fragment_name).await;
        let (affected_paths, affected_objects) = tokio::task::spawn_blocking(move || {
            let trash_entry_ids = index_store::stored_trash_records(&fragment_name)?
                .into_iter()
                .map(|record| record.trash_entry_id)
                .collect::<Vec<_>>();
            let mut affected_paths = 0usize;
            let mut affected_objects = 0usize;
            for trash_entry_id in trash_entry_ids {
                let summary = recover_trash_path(&fragment_name, "", Some(&trash_entry_id))?;
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
        let _mutation_guard = namespace_mutation_guard(&fragment).await;
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
                let fragment_root = fragment_root_on(&root, &fragment);
                let link = namespace_path(&root, &fragment, &logical_path);
                index_store::ensure_namespace_parent(&fragment_root, &link)?;
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

            index_store::advance_namespace_generation(&fragment)?;

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
                        if let Some(object_id) =
                            read_namespace_symlink_object_id(&root, &fragment, &child)?
                        {
                            if after
                                .as_ref()
                                .is_some_and(|cursor| child_rel.as_str() <= cursor.as_str())
                            {
                                continue;
                            }
                            if !seen.insert(child_rel.clone()) {
                                continue;
                            }
                            let Some(meta) =
                                index_store::read_current_live_head(&fragment, &object_id)?
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
                let Some(meta) = index_store::read_current_live_head(fragment, &object_id)? else {
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
    for record in index_store::stored_trash_records(fragment)? {
        if !path.is_empty()
            && record.original_path != path
            && !record.original_path.starts_with(&format!("{path}/"))
        {
            continue;
        }
        let Some(entry) = trash_entry_from_record(fragment, &record)? else {
            continue;
        };
        let cursor_key = format!("{}\u{001f}{}", entry.path, record.trash_entry_id);
        if after.is_some_and(|cursor| cursor_key.as_str() <= cursor) {
            continue;
        }
        entries.push(entry);
    }
    entries.sort_by(|a, b| {
        a.path
            .cmp(&b.path)
            .then_with(|| a.trash_entry_id.cmp(&b.trash_entry_id))
    });
    let next = if entries.len() > limit {
        entries.truncate(limit);
        entries.last().map(|entry| {
            format!(
                "{}\u{001f}{}",
                entry.path,
                entry.trash_entry_id.clone().unwrap_or_default()
            )
        })
    } else {
        None
    };
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
    let record = select_trash_record(fragment, path, Some(path))
        .or_else(|_| select_trash_record(fragment, path, None));
    match record {
        Ok(record) => trash_entry_from_record(fragment, &record),
        Err(AppError::NotFound) => Ok(None),
        Err(err) => Err(err),
    }
}

fn trash_entry_from_record(
    fragment: &str,
    record: &TrashRecord,
) -> Result<Option<NamespaceEntry>, AppError> {
    let object_id = record.object_keys.first().cloned();
    let mut entry = namespace_entry(
        fragment,
        record.original_path.clone(),
        if record.kind == TrashKind::File {
            NamespaceEntryKind::File
        } else {
            NamespaceEntryKind::Directory
        },
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
    Ok(Some(entry))
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
    let roots = index_roots();
    for root in &roots {
        let fragment_root = fragment_root_on(root, fragment);
        let source_path = fragment_root.join(source);
        if path_exists_no_follow(&source_path) {
            index_store::validate_namespace_parent(&fragment_root, &source_path)?;
        }
    }
    if live_entry(fragment, destination)?.is_some() {
        return Err(AppError::Conflict(format!(
            "destination path '{}' already exists",
            destination
        )));
    }
    let Some(entry) = live_entry(fragment, source)? else {
        return Err(AppError::NotFound);
    };
    for root in &roots {
        let fragment_root = fragment_root_on(root, fragment);
        let destination_path = if entry.kind == NamespaceEntryKind::File {
            namespace_path(root, fragment, destination)
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
    let mut placements = Vec::new();
    for root in &roots {
        let fragment_root = fragment_root_on(root, fragment);
        let source_path = if entry.kind == NamespaceEntryKind::File {
            namespace_path(root, fragment, source)
        } else {
            fragment_root.join(source)
        };
        if !path_exists_no_follow(&source_path) {
            continue;
        }
        let destination_path = if entry.kind == NamespaceEntryKind::File {
            namespace_path(root, fragment, destination)
        } else {
            fragment_root.join(destination)
        };
        index_store::ensure_namespace_parent(&fragment_root, &destination_path)?;
        placements.push((source_path, destination_path));
    }
    index_store::advance_namespace_generation(fragment)?;
    let mut moved: Vec<(PathBuf, PathBuf)> = Vec::new();
    for (source_path, destination_path) in &placements {
        #[cfg(test)]
        if test_fail_rename_after(moved.len()) {
            for (rollback_source, rollback_destination) in moved.into_iter().rev() {
                let _ = renameat(CWD, &rollback_destination, CWD, &rollback_source);
                sync_rename_parents(&rollback_destination, &rollback_source);
            }
            return Err(AppError::InternalServerError(
                "N1 namespace rename: injected replica failure".to_string(),
            ));
        }
        if let Err(err) = renameat(CWD, source_path, CWD, destination_path) {
            for (rollback_source, rollback_destination) in moved.into_iter().rev() {
                let _ = renameat(CWD, &rollback_destination, CWD, &rollback_source);
                sync_rename_parents(&rollback_destination, &rollback_source);
            }
            return Err(AppError::InternalServerError(format!(
                "N1 namespace rename: {err}"
            )));
        }
        sync_rename_parents(source_path, destination_path);
        moved.push((source_path.clone(), destination_path.clone()));
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
    let deleted_at = Utc::now().timestamp();
    let trash_entry_id = Ulid::new().to_string();
    let source_relative = format!("{TRASH_ENTRIES_DIR}/{trash_entry_id}");
    let affected_objects = usize::from(entry.kind == NamespaceEntryKind::File);
    let mut object_keys = Vec::new();
    let mut version_ids = Vec::new();
    if let Some(object_id) = entry.object_id.clone() {
        version_ids.extend(
            index_store::list_live_versions(fragment, &object_id)?
                .into_iter()
                .map(|metadata| metadata.version_id),
        );
        object_keys.push(object_id);
    }
    let record = TrashRecord {
        trash_entry_id: trash_entry_id.clone(),
        original_path: path.to_string(),
        source_relative_path: source_relative.clone(),
        kind: if entry.kind == NamespaceEntryKind::File {
            TrashKind::File
        } else {
            TrashKind::Directory
        },
        object_keys,
        version_ids,
        deleted_at_unix: deleted_at,
    };
    let mut placements = Vec::new();
    for root in index_roots() {
        let fragment_root = fragment_root_on(&root, fragment);
        let src = fragment_root.join(path);
        if !path_exists_no_follow(&src) {
            continue;
        }
        let dst = fragment_root.join(TRASH_DIR).join(&source_relative);
        if path_exists_no_follow(&dst) {
            return Err(AppError::Conflict(format!(
                "trash entry '{}' already exists",
                trash_entry_id
            )));
        }
        index_store::validate_namespace_parent(&fragment_root, &src)?;
        index_store::ensure_namespace_parent(&fragment_root, &dst)?;
        placements.push((src, dst));
    }
    if placements.is_empty() {
        return Err(AppError::NotFound);
    }
    index_store::advance_namespace_generation(fragment)?;
    index_store::write_trash_record_for_fragment(fragment, &record)?;
    let mut moved: Vec<(PathBuf, PathBuf)> = Vec::new();
    for (src, dst) in &placements {
        if let Err(err) = renameat(CWD, src, CWD, dst) {
            for (rollback_src, rollback_dst) in moved.into_iter().rev() {
                let _ = renameat(CWD, &rollback_dst, CWD, &rollback_src);
                sync_rename_parents(&rollback_dst, &rollback_src);
            }
            let _ = index_store::remove_trash_record(fragment, &trash_entry_id);
            return Err(AppError::InternalServerError(format!(
                "N1 namespace trash {} -> {}: {err}",
                src.display(),
                dst.display()
            )));
        }
        sync_rename_parents(src, dst);
        moved.push((src.clone(), dst.clone()));
    }
    Ok(NamespaceMutationSummary {
        operation: "trash".to_string(),
        kind: entry.kind,
        affected_paths: 1,
        affected_objects,
        source_path: Some(path.to_string()),
        destination_path: None,
        trash_entry_id: Some(trash_entry_id),
        deleted_at_unix: Some(deleted_at),
    })
}

fn replay_trash_reclaim_receipt(
    fragment: &str,
    receipt: &index_store::TrashReclaimReceipt,
) -> Result<u64, AppError> {
    if let Some(record) = index_store::stored_trash_records(fragment)?
        .into_iter()
        .find(|record| record.trash_entry_id == receipt.trash_entry_id)
    {
        index_store::remove_trash_payload(fragment, &record)?;
    }
    let reclaimed =
        index_store::delete_unreferenced_object_indexes_batch(fragment, &receipt.object_ids)?;
    index_store::remove_trash_reclaim_receipt(fragment, &receipt.operation_id)?;
    Ok(reclaimed.len() as u64)
}

fn sweep_expired_trash_sync(
    fragment: &str,
    retention_seconds: u64,
    now_unix: i64,
) -> Result<TrashLifecycleSweepSummary, AppError> {
    let mut summary = TrashLifecycleSweepSummary::default();
    let receipts = index_store::list_trash_reclaim_receipts(fragment)?;
    let retention = i64::try_from(retention_seconds).unwrap_or(i64::MAX);
    let expired_records = index_store::stored_trash_records(fragment)?
        .into_iter()
        .filter(|record| now_unix >= record.deleted_at_unix.saturating_add(retention))
        .collect::<Vec<_>>();
    if receipts.is_empty() && expired_records.is_empty() {
        return Ok(summary);
    }
    index_store::advance_namespace_generation(fragment)?;

    for receipt in receipts {
        summary.reclaimed_objects = summary
            .reclaimed_objects
            .saturating_add(replay_trash_reclaim_receipt(fragment, &receipt)?);
        summary.pending_receipts_replayed = summary.pending_receipts_replayed.saturating_add(1);
    }

    for record in expired_records {
        let record = index_store::hydrate_trash_record(fragment, record)?;
        let operation_id = format!("ttl-{}-{}", record.trash_entry_id, Ulid::new());
        let receipt = index_store::TrashReclaimReceipt {
            operation_id,
            fragment: fragment.to_string(),
            trash_entry_id: record.trash_entry_id.clone(),
            object_ids: record.object_keys.clone(),
            version_ids: record.version_ids.clone(),
            prepared_at_unix: now_unix,
        };
        index_store::write_trash_reclaim_receipt(&receipt)?;
        index_store::remove_trash_payload(fragment, &record)?;
        summary.reclaimed_objects = summary.reclaimed_objects.saturating_add(
            index_store::delete_unreferenced_object_indexes_batch(fragment, &receipt.object_ids)?
                .len() as u64,
        );
        index_store::remove_trash_reclaim_receipt(fragment, &receipt.operation_id)?;
        summary.expired_entries = summary.expired_entries.saturating_add(1);
        summary.expired_versions = summary
            .expired_versions
            .saturating_add(receipt.version_ids.len() as u64);
    }
    Ok(summary)
}

fn select_trash_record(
    fragment: &str,
    path: &str,
    trash_entry_id: Option<&str>,
) -> Result<TrashRecord, AppError> {
    let target = trash_entry_id
        .filter(|id| !id.trim().is_empty())
        .unwrap_or(path);
    if target.is_empty() {
        return Err(AppError::NotFound);
    }
    index_store::stored_trash_records(fragment)?
        .into_iter()
        .filter(|record| record.trash_entry_id == target || record.original_path == target)
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
    let expires_at = record
        .deleted_at_unix
        .saturating_add(i64::try_from(configured_trash_retention_seconds()).unwrap_or(i64::MAX));
    if Utc::now().timestamp() >= expires_at {
        return Err(AppError::Conflict(
            "trash retention expired; lifecycle owns permanent reclamation".to_string(),
        ));
    }
    let mut placements = Vec::new();
    for root in index_roots() {
        let fragment_root = fragment_root_on(&root, fragment);
        let src = fragment_root
            .join(TRASH_DIR)
            .join(&record.source_relative_path);
        if !path_exists_no_follow(&src) {
            continue;
        }
        let dst = fragment_root.join(&record.original_path);
        if path_exists_no_follow(&dst) {
            return Err(AppError::Conflict(format!(
                "recover destination '{}' already exists",
                record.original_path
            )));
        }
        index_store::validate_namespace_parent(&fragment_root, &src)?;
        index_store::ensure_namespace_parent(&fragment_root, &dst)?;
        placements.push((src, dst, fragment_root.join(TRASH_DIR)));
    }
    if placements.is_empty() {
        return Err(AppError::InternalServerError(format!(
            "N1 namespace invariant: trash record '{}' has no recoverable payload placement",
            record.trash_entry_id
        )));
    }
    index_store::advance_namespace_generation(fragment)?;
    let mut moved: Vec<(PathBuf, PathBuf)> = Vec::new();
    for (src, dst, _) in &placements {
        if let Err(err) = renameat(CWD, src, CWD, dst) {
            for (rollback_src, rollback_dst) in moved.into_iter().rev() {
                let _ = renameat(CWD, &rollback_dst, CWD, &rollback_src);
                sync_rename_parents(&rollback_dst, &rollback_src);
            }
            return Err(AppError::InternalServerError(format!(
                "N1 namespace recover: {err}"
            )));
        }
        sync_rename_parents(src, dst);
        moved.push((src.clone(), dst.clone()));
    }
    for (_, _, trash_root) in &placements {
        prune_empty_trash_parents(trash_root, &record.source_relative_path);
    }
    index_store::remove_trash_record(fragment, &record.trash_entry_id)?;
    Ok(NamespaceMutationSummary {
        operation: "recover".to_string(),
        kind: if record.kind == TrashKind::File {
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

fn sync_rename_parents(source: &Path, destination: &Path) {
    if let Some(parent) = source.parent() {
        index_store::sync_dir(parent);
    }
    if let Some(parent) = destination.parent()
        && source.parent() != Some(parent)
    {
        index_store::sync_dir(parent);
    }
}

#[cfg(test)]
static TEST_RENAME_FAIL_AFTER: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(usize::MAX);

#[cfg(test)]
fn test_fail_rename_after(completed_placements: usize) -> bool {
    TEST_RENAME_FAIL_AFTER.load(std::sync::atomic::Ordering::SeqCst) == completed_placements
}

fn prune_empty_trash_parents(trash_root: &Path, source_relative_path: &str) {
    // `.entries` is a structural lifecycle directory shared by concurrent
    // trash and recovery operations. Removing it while another trash move is
    // between preflight and rename can invalidate that move's destination.
    let entries_root = trash_root.join(TRASH_ENTRIES_DIR);
    let mut current = trash_root.join(source_relative_path);
    current.pop();
    while current != trash_root && current != entries_root {
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
    use crate::core::disk_manager::{
        BLOB_STORE_DISKS, INDEX_STORE_DISKS, ObjectMetadata, ObjectPathManager, PartMetadata,
        ShardInfo,
    };
    use crate::core::index_store;
    use ulid::Ulid;

    #[test]
    fn trash_parent_pruning_preserves_shared_entries_directory() {
        let base = std::env::temp_dir().join(format!(
            "n1-trash-prune-{}-{}",
            std::process::id(),
            Ulid::new()
        ));
        let trash_root = base.join(TRASH_DIR);
        let entries_root = trash_root.join(TRASH_ENTRIES_DIR);
        std::fs::create_dir_all(&entries_root).expect("create trash entries directory");

        prune_empty_trash_parents(&trash_root, ".entries/01KTESTENTRY");

        assert!(
            entries_root.is_dir(),
            "concurrent trash moves require the shared .entries directory to remain stable"
        );
        let _ = std::fs::remove_dir_all(base);
    }

    fn ensure_test_index_roots() -> Vec<PathBuf> {
        let base = std::env::temp_dir().join("n1-index-tests");
        let roots = vec![base.join("index-00"), base.join("index-01")];
        let blob_roots = vec![base.join("blob-00"), base.join("blob-01")];
        if BLOB_STORE_DISKS.get().is_none() {
            BLOB_STORE_DISKS
                .set(
                    blob_roots
                        .iter()
                        .map(|path| path.display().to_string())
                        .collect::<Vec<_>>(),
                )
                .expect("set N1 test blob roots");
        }
        for root in BLOB_STORE_DISKS.get().expect("N1 test blob roots") {
            std::fs::create_dir_all(root).expect("create N1 test blob root");
        }
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
        let version_id = Ulid::new().to_string();
        let blob_root = ObjectPathManager::new()
            .get_blob_roots()
            .into_iter()
            .next()
            .expect("N1 test blob root");
        let version_dir = ObjectPathManager::new().get_shard_dir_on_disk(
            &blob_root,
            fragment,
            object_key,
            &version_id,
        );
        std::fs::create_dir_all(&version_dir).expect("create N1 test placement");
        let shard_path = version_dir.join("part_0_shard_0.shard");
        std::fs::write(&shard_path, vec![0u8; size.min(4096) as usize])
            .expect("write N1 test shard");
        ObjectMetadata {
            object_key: object_key.to_string(),
            fragment: fragment.to_string(),
            version_id,
            upload_session_id: "session".to_string(),
            owner_uid: "owner".to_string(),
            content_type: "application/octet-stream".to_string(),
            total_size_bytes: size,
            created_at: Utc::now().timestamp(),
            data_shards: 1,
            parity_shards: 0,
            parts: vec![PartMetadata {
                part_index: 0,
                size_bytes: size,
                created_at: Utc::now().timestamp(),
                shards: vec![ShardInfo {
                    shard_index: 0,
                    file_name: "part_0_shard_0.shard".to_string(),
                    absolute_path: shard_path.to_string_lossy().into_owned(),
                    size_bytes: size.min(4096),
                    checksum: String::new(),
                    is_parity: false,
                }],
                checksum: None,
            }],
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
    async fn n1_namespace_mutations_advance_the_replicated_authority_generation() {
        let _guard = crate::test_support::global_test_lock().lock().await;
        let roots = ensure_test_index_roots();
        let fragment = format!("n1-namespace-generation-{}", Ulid::new());
        for root in &roots {
            std::fs::create_dir_all(fragment_root_on(root, &fragment))
                .expect("create generation test fragment root");
        }

        global_namespace_runtime()
            .mkdir(&fragment, "clinical")
            .await
            .expect("mkdir advances generation");
        for root in &roots {
            assert_eq!(
                index_store::namespace_generation_on_root(root, &fragment)
                    .expect("read mkdir generation"),
                Some(1)
            );
        }

        publish_logical_object(&roots, &fragment, "clinical/report.bin", 123);
        global_namespace_runtime()
            .rename(&fragment, "clinical/report.bin", "clinical/renamed.bin")
            .await
            .expect("rename advances generation");
        global_namespace_runtime()
            .trash(&fragment, "clinical/renamed.bin")
            .await
            .expect("trash advances generation");
        global_namespace_runtime()
            .recover(&fragment, "clinical/renamed.bin")
            .await
            .expect("recover advances generation");

        for root in &roots {
            assert_eq!(
                index_store::namespace_generation_on_root(root, &fragment)
                    .expect("read final mutation generation"),
                Some(4)
            );
        }
    }

    #[tokio::test]
    async fn n1_namespace_mutation_is_blocked_until_stale_generation_is_reconciled() {
        let _guard = crate::test_support::global_test_lock().lock().await;
        let roots = ensure_test_index_roots();
        assert!(roots.len() >= 2);
        let fragment = format!("n1-stale-generation-write-gate-{}", Ulid::new());
        let object_id = publish_logical_object(&roots, &fragment, "docs/report.bin", 123);
        index_store::set_namespace_generation_on_root(&roots[0], &fragment, 8)
            .expect("set active generation");
        index_store::set_namespace_generation_on_root(&roots[1], &fragment, 7)
            .expect("set stale generation");

        assert!(matches!(
            global_namespace_runtime()
                .rename(&fragment, "docs/report.bin", "docs/renamed.bin")
                .await,
            Err(AppError::Conflict(_))
        ));
        for root in &roots {
            assert_eq!(
                read_namespace_symlink_object_id(
                    root,
                    &fragment,
                    &fragment_root_on(root, &fragment).join("docs/report.bin")
                )
                .expect("read source after gated mutation"),
                Some(object_id.clone())
            );
            assert!(
                !path_exists_no_follow(&fragment_root_on(root, &fragment).join("docs/renamed.bin")),
                "a mutation must not start while replica authority generations disagree"
            );
        }
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
    async fn n1_finalize_preflights_every_replica_before_namespace_publication() {
        let _guard = crate::test_support::global_test_lock().lock().await;
        let roots = ensure_test_index_roots();
        assert!(roots.len() >= 2);
        let fragment = format!("n1-finalize-preflight-{}", Ulid::new());
        let object_id = new_object_id();
        let metadata = test_metadata(&fragment, &object_id, 99);
        index_store::upsert_live_version(&metadata).expect("publish provisional object metadata");
        let outside = std::env::temp_dir().join(format!("n1-finalize-outside-{}", Ulid::new()));
        std::fs::create_dir_all(&outside).expect("create finalize outside directory");
        for root in &roots {
            std::fs::create_dir_all(fragment_root_on(root, &fragment))
                .expect("create finalize fragment root");
        }
        std::os::unix::fs::symlink(
            &outside,
            fragment_root_on(&roots[1], &fragment).join("docs"),
        )
        .expect("inject later-root parent symlink");

        assert!(matches!(
            global_namespace_runtime()
                .finalize_logical_upload(&fragment, "docs/item.bin", &object_id, &metadata)
                .await,
            Err(AppError::InternalServerError(_))
        ));
        assert!(
            !path_exists_no_follow(&fragment_root_on(&roots[0], &fragment).join("docs/item.bin")),
            "failed finalize preflight must not publish a partial namespace replica"
        );
        assert!(!outside.join("item.bin").exists());
        let _ = std::fs::remove_dir_all(outside);
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
            let link = fragment_root_on(root, &fragment).join("archive/docs/a/video.mp4");
            let target = fs::read_link(&link).expect("read namespace link");
            assert_eq!(
                target,
                PathBuf::from(format!("@o_{object_id}")),
                "namespace links store stable object ids, not depth-relative paths"
            );
        }
    }

    #[tokio::test]
    async fn n1_soft_delete_ttl_reclaims_only_after_namespace_expiry() {
        let _guard = crate::test_support::global_test_lock().lock().await;
        let roots = ensure_test_index_roots();
        let blob_roots = ObjectPathManager::new().get_blob_roots();
        let fragment = format!("n1-trash-{}", Ulid::new());
        let object_id = publish_logical_object(&roots, &fragment, "video/my-video.mp4", 123);

        let summary = global_namespace_runtime()
            .trash(&fragment, "video/my-video.mp4")
            .await
            .expect("soft delete");
        assert_eq!(summary.operation, "trash");
        assert_eq!(summary.affected_objects, 1);
        let trash_entry_id = summary.trash_entry_id.clone().expect("trash entry id");

        for root in &roots {
            assert!(!path_exists_no_follow(
                &fragment_root_on(root, &fragment).join("video/my-video.mp4")
            ));
            assert!(path_exists_no_follow(
                &fragment_root_on(root, &fragment)
                    .join(TRASH_DIR)
                    .join(TRASH_ENTRIES_DIR)
                    .join(&trash_entry_id)
            ));
            assert!(
                !fragment_root_on(root, &fragment)
                    .join(index_store::OBJECTS_DIR)
                    .exists()
            );
        }
        assert!(blob_roots.iter().any(|root| {
            index_store::live_object_dir(root, &fragment, &object_id)
                .map(|path| path.is_dir())
                .unwrap_or(false)
        }));

        let before_expiry = sweep_expired_trash_sync(
            &fragment,
            60,
            summary.deleted_at_unix.expect("deleted timestamp") + 59,
        )
        .expect("pre-expiry lifecycle sweep");
        assert_eq!(before_expiry.expired_entries, 0);

        let expired = sweep_expired_trash_sync(
            &fragment,
            60,
            summary.deleted_at_unix.expect("deleted timestamp") + 60,
        )
        .expect("expiry lifecycle sweep");
        assert_eq!(expired.expired_entries, 1);
        assert_eq!(expired.reclaimed_objects, 1);
        for root in &roots {
            assert!(!path_exists_no_follow(
                &fragment_root_on(root, &fragment)
                    .join(TRASH_DIR)
                    .join(TRASH_ENTRIES_DIR)
                    .join(&trash_entry_id)
            ));
        }
        assert!(!blob_roots.iter().any(|root| {
            index_store::live_object_dir(root, &fragment, &object_id)
                .map(|path| path.exists())
                .unwrap_or(false)
        }));
        assert!(
            !index_store::referenced_object_ids(&fragment)
                .expect("referenced object ids")
                .contains(&object_id),
            "purged trash item must leave object index headless for GC"
        );
    }

    #[tokio::test]
    async fn n1_trash_ids_allow_repeated_delete_of_the_same_logical_path() {
        let _guard = crate::test_support::global_test_lock().lock().await;
        let roots = ensure_test_index_roots();
        let fragment = format!("n1-trash-repeat-{}", Ulid::new());
        let path = "clinical/report.bin";
        let first_object = publish_logical_object(&roots, &fragment, path, 11);
        let first = global_namespace_runtime()
            .trash(&fragment, path)
            .await
            .expect("trash first object");

        let second_object = publish_logical_object(&roots, &fragment, path, 22);
        let second = global_namespace_runtime()
            .trash(&fragment, path)
            .await
            .expect("trash replacement object");
        assert_ne!(first_object, second_object);
        assert_ne!(first.trash_entry_id, second.trash_entry_id);

        let (entries, _) = global_namespace_runtime()
            .list_dir_page(&fragment, "", true, 10, None)
            .await
            .expect("list trash");
        assert_eq!(
            entries.iter().filter(|entry| entry.path == path).count(),
            2,
            "both retained generations must remain independently recoverable"
        );

        global_namespace_runtime()
            .recover_trash_entry_id(
                &fragment,
                second.trash_entry_id.as_deref().expect("second trash id"),
            )
            .await
            .expect("recover latest generation");
        assert_eq!(
            global_namespace_runtime()
                .resolve_live_object_id(&fragment, path)
                .await
                .expect("resolve recovered path"),
            Some(second_object)
        );
        assert!(matches!(
            global_namespace_runtime()
                .recover_trash_entry_id(
                    &fragment,
                    first.trash_entry_id.as_deref().expect("first trash id"),
                )
                .await,
            Err(AppError::Conflict(_))
        ));
    }

    #[tokio::test]
    async fn n1_ttl_reclaim_preserves_an_object_still_referenced_live() {
        let _guard = crate::test_support::global_test_lock().lock().await;
        let roots = ensure_test_index_roots();
        let blob_roots = ObjectPathManager::new().get_blob_roots();
        let fragment = format!("n1-trash-shared-{}", Ulid::new());
        let object_id = publish_logical_object(&roots, &fragment, "records/a.bin", 33);
        for root in &roots {
            publish_namespace_link(root, &fragment, "records/b.bin", &object_id)
                .expect("publish second live reference");
        }

        let deleted = global_namespace_runtime()
            .trash(&fragment, "records/a.bin")
            .await
            .expect("trash one reference");
        let expired = sweep_expired_trash_sync(
            &fragment,
            1,
            deleted.deleted_at_unix.expect("deleted timestamp") + 1,
        )
        .expect("lifecycle sweep");
        assert_eq!(expired.expired_entries, 1);
        assert_eq!(expired.reclaimed_objects, 0);
        assert_eq!(
            global_namespace_runtime()
                .resolve_live_object_id(&fragment, "records/b.bin")
                .await
                .expect("resolve retained reference"),
            Some(object_id.clone())
        );
        assert!(blob_roots.iter().any(|root| {
            index_store::live_object_dir(root, &fragment, &object_id)
                .map(|path| path.is_dir())
                .unwrap_or(false)
        }));
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

    #[tokio::test]
    async fn n1_recursive_reference_pagination_never_skips_cursor_directory_descendants() {
        let _guard = crate::test_support::global_test_lock().lock().await;
        let roots = ensure_test_index_roots();
        let fragment = format!("n1-recursive-page-{}", Ulid::new());
        publish_logical_object(&roots, &fragment, "a/first.bin", 1);
        publish_logical_object(&roots, &fragment, "a/z-last.bin", 2);
        publish_logical_object(&roots, &fragment, "b.bin", 3);

        let first = global_namespace_runtime()
            .list_live_object_refs_page(&fragment, None, None, 1)
            .await
            .expect("first recursive reference page");
        assert_eq!(first.entries[0].path, "a/first.bin");
        let second = global_namespace_runtime()
            .list_live_object_refs_page(&fragment, None, first.continuation_token.as_deref(), 10)
            .await
            .expect("second recursive reference page");
        assert_eq!(
            second
                .entries
                .iter()
                .map(|entry| entry.path.as_str())
                .collect::<Vec<_>>(),
            vec!["a/z-last.bin", "b.bin"]
        );
    }

    #[tokio::test]
    async fn n1_resolve_any_selects_the_same_latest_trash_generation_as_recovery() {
        let _guard = crate::test_support::global_test_lock().lock().await;
        let roots = ensure_test_index_roots();
        let fragment = format!("n1-resolve-trash-generation-{}", Ulid::new());
        let path = "clinical/report.bin";
        let first_object = publish_logical_object(&roots, &fragment, path, 11);
        global_namespace_runtime()
            .trash(&fragment, path)
            .await
            .expect("trash first generation");
        let second_object = publish_logical_object(&roots, &fragment, path, 22);
        global_namespace_runtime()
            .trash(&fragment, path)
            .await
            .expect("trash second generation");
        assert_ne!(first_object, second_object);

        assert_eq!(
            global_namespace_runtime()
                .resolve_any_object_id(&fragment, path)
                .await
                .expect("resolve latest trashed generation"),
            Some(second_object)
        );
    }

    #[tokio::test]
    async fn n1_recover_fails_closed_when_record_has_no_payload_placement() {
        let _guard = crate::test_support::global_test_lock().lock().await;
        let roots = ensure_test_index_roots();
        let fragment = format!("n1-recover-headless-record-{}", Ulid::new());
        publish_logical_object(&roots, &fragment, "records/report.bin", 44);
        let deleted = global_namespace_runtime()
            .trash(&fragment, "records/report.bin")
            .await
            .expect("trash object");
        let trash_entry_id = deleted.trash_entry_id.expect("trash entry id");
        let record = index_store::stored_trash_records(&fragment)
            .expect("trash records")
            .into_iter()
            .find(|record| record.trash_entry_id == trash_entry_id)
            .expect("matching trash record");
        for root in &roots {
            let payload = fragment_root_on(root, &fragment)
                .join(TRASH_DIR)
                .join(&record.source_relative_path);
            if path_exists_no_follow(&payload) {
                std::fs::remove_file(payload).expect("remove test trash payload");
            }
        }

        assert!(matches!(
            global_namespace_runtime()
                .recover_trash_entry_id(&fragment, &trash_entry_id)
                .await,
            Err(AppError::InternalServerError(_))
        ));
        assert!(
            index_store::stored_trash_records(&fragment)
                .expect("trash record retained after fail-closed recovery")
                .iter()
                .any(|record| record.trash_entry_id == trash_entry_id),
            "recovery must never delete lifecycle metadata when no payload was moved"
        );
    }

    #[tokio::test]
    async fn n1_live_rename_rolls_back_every_replica_after_later_root_failure() {
        let _guard = crate::test_support::global_test_lock().lock().await;
        let roots = ensure_test_index_roots();
        assert!(roots.len() >= 2);
        let fragment = format!("n1-rename-rollback-{}", Ulid::new());
        let object_id = publish_logical_object(&roots, &fragment, "before/item.bin", 55);

        TEST_RENAME_FAIL_AFTER.store(1, std::sync::atomic::Ordering::SeqCst);
        let result = global_namespace_runtime()
            .rename(&fragment, "before/item.bin", "after/item.bin")
            .await;
        TEST_RENAME_FAIL_AFTER.store(usize::MAX, std::sync::atomic::Ordering::SeqCst);
        assert!(matches!(result, Err(AppError::InternalServerError(_))));
        for root in &roots {
            assert_eq!(
                read_namespace_symlink_object_id(
                    root,
                    &fragment,
                    &fragment_root_on(root, &fragment).join("before/item.bin")
                )
                .expect("read rolled-back source"),
                Some(object_id.clone())
            );
            assert!(!path_exists_no_follow(
                &fragment_root_on(root, &fragment).join("after/item.bin")
            ));
        }
    }

    #[tokio::test]
    async fn n1_namespace_mutations_reject_parent_symlink_escape() {
        let _guard = crate::test_support::global_test_lock().lock().await;
        let roots = ensure_test_index_roots();
        let fragment = format!("n1-parent-symlink-{}", Ulid::new());
        let outside = std::env::temp_dir().join(format!("n1-outside-{}", Ulid::new()));
        std::fs::create_dir_all(&outside).expect("create outside test directory");
        for root in &roots {
            let fragment_root = fragment_root_on(root, &fragment);
            std::fs::create_dir_all(&fragment_root).expect("create fragment root");
            std::os::unix::fs::symlink(&outside, fragment_root.join("clinical"))
                .expect("inject corrupt namespace parent symlink");
        }

        assert!(matches!(
            global_namespace_runtime()
                .mkdir(&fragment, "clinical/new-folder")
                .await,
            Err(AppError::InternalServerError(_))
        ));
        assert!(
            !outside.join("new-folder").exists(),
            "namespace mutation must not create data outside its physical fragment root"
        );
        let _ = std::fs::remove_dir_all(outside);
    }

    #[tokio::test]
    async fn n1_recover_and_ttl_sweep_are_serialized_per_fragment() {
        let _guard = crate::test_support::global_test_lock().lock().await;
        let roots = ensure_test_index_roots();
        for iteration in 0..32u64 {
            let fragment = format!("n1-recover-sweep-race-{iteration}-{}", Ulid::new());
            let path = "race/item.bin";
            publish_logical_object(&roots, &fragment, path, iteration + 1);
            global_namespace_runtime()
                .trash(&fragment, path)
                .await
                .expect("trash race object");

            let (recover, sweep) = tokio::join!(
                global_namespace_runtime().recover(&fragment, path),
                global_namespace_runtime().sweep_expired_trash(
                    &fragment,
                    0,
                    Utc::now().timestamp()
                )
            );
            let live = global_namespace_runtime()
                .resolve_live_object_id(&fragment, path)
                .await
                .expect("resolve race result")
                .is_some();
            let records = index_store::stored_trash_records(&fragment).expect("race records");
            assert!(
                records.is_empty(),
                "race must leave no orphan lifecycle record"
            );
            match (recover, sweep, live) {
                (Ok(_), Ok(summary), true) => assert_eq!(summary.expired_entries, 0),
                (Err(AppError::NotFound), Ok(summary), false) => {
                    assert_eq!(summary.expired_entries, 1)
                }
                outcome => panic!("invalid recover/sweep outcome: {outcome:?}"),
            }
        }
    }
}
