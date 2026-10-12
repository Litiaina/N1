use std::collections::{BTreeMap, BTreeSet, HashMap};
#[cfg(test)]
use std::fs;
#[cfg(test)]
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;
use std::sync::LazyLock;

use chrono::Utc;
use futures_util::{StreamExt, stream};
use serde::Serialize;

use crate::core::disk_manager::{ListObjectsResult, ObjectMetadata};
use crate::core::index_store::{
    self, PendingUploadBinding, TrashRecord, new_object_id, reject_reserved_user_path,
    scan_trash_records, write_pending_binding,
};
#[cfg(test)]
use crate::core::index_store::{TRASH_DIR, fragment_root_on, publish_namespace_link};
use crate::enums::app_error_enum::AppError;
use crate::namespace::paths::{base_name, normalize_path, parent_path};
use crate::namespace::{
    flat,
    models::{
        LifecycleState, NamespaceEntry, NamespaceEntryKind, NamespaceError,
        NamespaceMutationSummary, NamespaceReachabilitySnapshot, ObjectHead,
    },
};

static GLOBAL_NAMESPACE_RUNTIME: LazyLock<NamespaceRuntime> = LazyLock::new(NamespaceRuntime::new);
mod finalize_guard;
use finalize_guard::FinalizeGuard;
pub fn global_namespace_runtime() -> &'static NamespaceRuntime {
    &GLOBAL_NAMESPACE_RUNTIME
}

#[derive(Debug, Clone)]
pub struct NamespaceObjectRef {
    pub path: String,
    pub object_id: String,
    pub version_id: String,
    pub content_type: String,
    pub total_size_bytes: u64,
    pub created_at: i64,
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
        NamespaceRuntimeStats::default()
    }

    pub async fn list_dir(
        &self,
        fragment: &str,
        path: &str,
        include_trashed: bool,
    ) -> Result<Vec<NamespaceEntry>, AppError> {
        Ok(self
            .list_dir_page(fragment, path, include_trashed, usize::MAX - 1, None)
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
        let after = continuation_token.map(str::to_string);
        tokio::task::spawn_blocking(move || {
            if include_trashed {
                return list_trash_dir_page(&fragment, &path, limit, after.as_deref());
            }
            let prefix = if path.is_empty() {
                path.clone()
            } else {
                format!("{path}/")
            };
            let (names, next) = flat::page(&fragment, &prefix, "/", after.as_deref(), limit)?;
            let entries = names
                .into_iter()
                .map(|(key, directory)| {
                    let mut entry = namespace_entry(
                        &fragment,
                        key.trim_end_matches('/').into(),
                        if directory {
                            NamespaceEntryKind::Directory
                        } else {
                            NamespaceEntryKind::File
                        },
                        None,
                        LifecycleState::Live,
                        None,
                        None,
                    );
                    // `a` and virtual prefix `a/` may coexist. Preserve the
                    // delimiter on listing paths so clients can distinguish
                    // them and continuation tokens retain their ordering.
                    if directory {
                        entry.path = key;
                    }
                    entry
                })
                .collect();
            Ok((entries, next))
        })
        .await
        .map_err(worker_error)?
    }

    pub async fn entry(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<Option<NamespaceEntry>, AppError> {
        let prefix_only = path.trim().ends_with('/');
        let fragment = fragment.to_string();
        let path = normalize_user_path(path)?;
        tokio::task::spawn_blocking(move || live_entry(&fragment, &path, prefix_only))
            .await
            .map_err(worker_error)?
    }

    pub async fn trashed_entry(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<Option<NamespaceEntry>, AppError> {
        let fragment = fragment.to_string();
        let path = normalize_user_path(path)?;
        tokio::task::spawn_blocking(move || {
            Ok(flat::trash_records(&fragment)?
                .into_iter()
                .filter(|record| record.original_path == path)
                .max_by(|a, b| a.trash_entry_id.cmp(&b.trash_entry_id))
                .map(|record| trash_record_entry(&fragment, &record)))
        })
        .await
        .map_err(worker_error)?
    }

    pub async fn head_for_path(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<Option<ObjectHead>, AppError> {
        let Some(object_id) = self.resolve_live_object_id(fragment, path).await? else {
            return Ok(None);
        };
        Ok(index_store::read_current_live_head(fragment, &object_id)?
            .map(|meta| object_head_from_meta(fragment, meta)))
    }

    pub async fn object_heads(
        &self,
        fragment: &str,
        object_ids: &[String],
    ) -> Result<HashMap<String, ObjectHead>, AppError> {
        let mut out = HashMap::new();
        for id in object_ids {
            if let Some(meta) = index_store::read_current_live_head(fragment, id)? {
                out.insert(id.clone(), object_head_from_meta(fragment, meta));
            }
        }
        Ok(out)
    }

    pub async fn mkdir(&self, _fragment: &str, _path: &str) -> Result<bool, AppError> {
        Err(AppError::BadRequest(
            "N1 4 uses object-key prefixes; empty directories are not storage objects".into(),
        ))
    }

    pub async fn rename(
        &self,
        fragment: &str,
        source_path: &str,
        destination_path: &str,
    ) -> Result<NamespaceMutationSummary, AppError> {
        let prefix_only = source_path.trim().ends_with('/');
        let source = normalize_user_path(source_path)?;
        let destination = normalize_user_path(destination_path)?;
        if source.is_empty() || destination.is_empty() {
            return Err(AppError::BadRequest("root cannot be renamed".into()));
        }
        if !prefix_only
            && self
                .resolve_live_object_id(fragment, &source)
                .await?
                .is_some()
        {
            let (f, s, d) = (fragment.to_string(), source.clone(), destination.clone());
            let moved = tokio::task::spawn_blocking(move || {
                if s == d {
                    Ok(true)
                } else {
                    flat::move_key(&f, &s, &d)
                }
            })
            .await
            .map_err(worker_error)??;
            if !moved {
                return Err(AppError::NotFound);
            }
            return Ok(summary(
                "rename",
                NamespaceEntryKind::File,
                1,
                Some(source),
                Some(destination),
            ));
        }
        if destination.starts_with(&format!("{source}/")) {
            return Err(AppError::Conflict(
                "prefix destination cannot be inside its source".into(),
            ));
        }
        let prefix = format!("{source}/");
        let mut result = summary(
            "rename",
            NamespaceEntryKind::Directory,
            0,
            Some(source.clone()),
            Some(destination.clone()),
        );
        let mut after = None;
        loop {
            let (f, p, a) = (fragment.to_string(), prefix.clone(), after.clone());
            let (names, next) =
                tokio::task::spawn_blocking(move || flat::page(&f, &p, "", a.as_deref(), 128))
                    .await
                    .map_err(worker_error)??;
            let operations = stream::iter(names.into_iter().map(|(key, _)| {
                let dest = format!("{}{}", destination, &key[source.len()..]);
                let f = fragment.to_string();
                async move {
                    let k = key.clone();
                    let outcome =
                        tokio::task::spawn_blocking(move || flat::move_key(&f, &k, &dest))
                            .await
                            .map_err(worker_error)
                            .and_then(|r| r);
                    (key, outcome)
                }
            }))
            .buffer_unordered(16)
            .collect::<Vec<_>>()
            .await;
            for (key, outcome) in operations {
                record_outcome(&mut result, key, outcome);
            }
            after = next;
            if after.is_none() {
                break;
            }
        }
        if result.affected_paths == 0
            && result.conflicted_paths.is_empty()
            && result.failed_paths.is_empty()
        {
            return Err(AppError::NotFound);
        }
        Ok(result)
    }

    pub async fn trash(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<NamespaceMutationSummary, AppError> {
        let prefix_only = path.trim().ends_with('/');
        let path = normalize_user_path(path)?;
        if path.is_empty() {
            return Err(AppError::BadRequest("root cannot be trashed".into()));
        }
        if !prefix_only
            && self
                .resolve_live_object_id(fragment, &path)
                .await?
                .is_some()
        {
            let (f, p) = (fragment.to_string(), path.clone());
            let record = tokio::task::spawn_blocking(move || flat::trash_key(&f, &p))
                .await
                .map_err(worker_error)??
                .ok_or(AppError::NotFound)?;
            let mut result = summary("trash", NamespaceEntryKind::File, 1, Some(path), None);
            result.trash_entry_id = Some(record.trash_entry_id);
            result.deleted_at_unix = Some(record.deleted_at_unix);
            return Ok(result);
        }
        let prefix = format!("{path}/");
        let mut result = summary("trash", NamespaceEntryKind::Directory, 0, Some(path), None);
        let mut after = None;
        loop {
            let (f, p, a) = (fragment.to_string(), prefix.clone(), after.clone());
            let (names, next) =
                tokio::task::spawn_blocking(move || flat::page(&f, &p, "", a.as_deref(), 128))
                    .await
                    .map_err(worker_error)??;
            let outcomes = stream::iter(names.into_iter().map(|(key, _)| {
                let f = fragment.to_string();
                async move {
                    let k = key.clone();
                    (
                        key,
                        tokio::task::spawn_blocking(move || {
                            flat::trash_key(&f, &k).map(|r| r.is_some())
                        })
                        .await
                        .map_err(worker_error)
                        .and_then(|r| r),
                    )
                }
            }))
            .buffer_unordered(16)
            .collect::<Vec<_>>()
            .await;
            for (key, outcome) in outcomes {
                record_outcome(&mut result, key, outcome);
            }
            after = next;
            if after.is_none() {
                break;
            }
        }
        if result.affected_paths == 0
            && result.conflicted_paths.is_empty()
            && result.failed_paths.is_empty()
        {
            return Err(AppError::NotFound);
        }
        Ok(result)
    }

    pub async fn recover(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<NamespaceMutationSummary, AppError> {
        let prefix_only = path.trim().ends_with('/');
        let fragment = fragment.to_string();
        let path = normalize_user_path(path)?;
        let f = fragment.clone();
        let records = tokio::task::spawn_blocking(move || flat::trash_records(&f))
            .await
            .map_err(worker_error)??;
        if !prefix_only
            && let Some(record) = records
                .iter()
                .filter(|record| record.original_path == path)
                .max_by(|a, b| a.trash_entry_id.cmp(&b.trash_entry_id))
                .cloned()
        {
            return tokio::task::spawn_blocking(move || recover_record(&fragment, &record))
                .await
                .map_err(worker_error)?;
        }
        let prefix = format!("{path}/");
        let mut latest = BTreeMap::new();
        for record in records
            .into_iter()
            .filter(|record| record.original_path.starts_with(&prefix))
        {
            let slot = latest
                .entry(record.original_path.clone())
                .or_insert(record.clone());
            if record.trash_entry_id > slot.trash_entry_id {
                *slot = record;
            }
        }
        if latest.is_empty() {
            return Err(AppError::NotFound);
        }
        let result = summary(
            "recover",
            NamespaceEntryKind::Directory,
            0,
            Some(path.clone()),
            Some(path),
        );
        recover_records(&fragment, latest.into_values(), result).await
    }

    pub async fn recover_paths_bulk(
        &self,
        fragment: &str,
        paths: Vec<String>,
    ) -> Result<Vec<(String, Result<NamespaceMutationSummary, AppError>)>, AppError> {
        Ok(stream::iter(paths.into_iter().map(|path| async move {
            let result = self.recover(fragment, &path).await;
            (path, result)
        }))
        .buffer_unordered(16)
        .collect()
        .await)
    }

    pub async fn recover_trash_entry_id(
        &self,
        fragment: &str,
        trash_entry_id: &str,
    ) -> Result<NamespaceMutationSummary, AppError> {
        let (fragment, id) = (fragment.to_string(), trash_entry_id.to_string());
        tokio::task::spawn_blocking(move || recover_trash_path(&fragment, "", Some(&id)))
            .await
            .map_err(worker_error)?
    }

    pub async fn recover_all(&self, fragment: &str) -> Result<NamespaceMutationSummary, AppError> {
        let f = fragment.to_string();
        let records = tokio::task::spawn_blocking(move || flat::trash_records(&f))
            .await
            .map_err(worker_error)??;
        let mut latest = HashMap::<String, TrashRecord>::new();
        for record in records {
            if latest
                .get(&record.original_path)
                .is_none_or(|old| old.trash_entry_id < record.trash_entry_id)
            {
                latest.insert(record.original_path.clone(), record);
            }
        }
        let result = summary("recover_all", NamespaceEntryKind::Directory, 0, None, None);
        recover_records(fragment, latest.into_values(), result).await
    }

    pub async fn sweep_expired_trash(
        &self,
        fragment: &str,
        retention_seconds: u64,
        now_unix: i64,
    ) -> Result<TrashLifecycleSweepSummary, AppError> {
        let fragment = fragment.to_string();
        tokio::task::spawn_blocking(move || {
            let mut result = TrashLifecycleSweepSummary::default();
            let retention = i64::try_from(retention_seconds).unwrap_or(i64::MAX);
            let mut candidates = BTreeSet::new();
            for record in scan_trash_records(&fragment)? {
                if now_unix < record.deleted_at_unix.saturating_add(retention) {
                    continue;
                }
                if flat::remove_trash(&fragment, &record)? {
                    result.expired_versions = result
                        .expired_versions
                        .saturating_add(record.version_ids.len() as u64);
                    candidates.extend(record.object_keys);
                    result.expired_entries += 1;
                }
            }
            if !candidates.is_empty() {
                result.reclaimed_objects = index_store::delete_unreferenced_object_indexes_batch(
                    &fragment,
                    &candidates.into_iter().collect::<Vec<_>>(),
                )?
                .len() as u64;
            }
            Ok(result)
        })
        .await
        .map_err(worker_error)?
    }

    pub async fn reachability_snapshot_if_present(
        &self,
        fragment: &str,
    ) -> Result<Option<NamespaceReachabilitySnapshot>, AppError> {
        Ok(Some(NamespaceReachabilitySnapshot {
            referenced_object_ids: flat::referenced_objects(fragment)?,
        }))
    }

    pub async fn resolve_live_object_id(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<Option<String>, AppError> {
        let fragment = fragment.to_string();
        let path = normalize_user_path(path)?;
        tokio::task::spawn_blocking(move || {
            Ok(flat::lookup(&fragment, &path)?.map(|pointer| pointer.object_id))
        })
        .await
        .map_err(worker_error)?
    }

    pub async fn resolve_any_object_id(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<Option<String>, AppError> {
        if let Some(id) = self.resolve_live_object_id(fragment, path).await? {
            return Ok(Some(id));
        }
        let path = normalize_user_path(path)?;
        Ok(flat::trash_records(fragment)?
            .into_iter()
            .filter(|r| r.original_path == path)
            .max_by(|a, b| a.trash_entry_id.cmp(&b.trash_entry_id))
            .and_then(|r| r.object_keys.first().cloned()))
    }

    pub async fn select_object_id_for_write(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<String, AppError> {
        let path = normalize_user_path(path)?;
        if path.is_empty() {
            return Err(AppError::BadRequest("root cannot be an object".into()));
        }
        Ok(self
            .resolve_live_object_id(fragment, &path)
            .await?
            .unwrap_or_else(new_object_id))
    }

    pub async fn finalize_logical_upload(
        &self,
        fragment: &str,
        logical_path: &str,
        physical_object_id: &str,
        metadata: &ObjectMetadata,
    ) -> Result<String, AppError> {
        self.finalize_logical_upload_with_durability(
            fragment,
            logical_path,
            physical_object_id,
            metadata,
            true,
        )
        .await
    }

    pub async fn finalize_logical_upload_with_durability(
        &self,
        fragment: &str,
        logical_path: &str,
        physical_object_id: &str,
        metadata: &ObjectMetadata,
        _strict_durability: bool,
    ) -> Result<String, AppError> {
        let fragment = index_store::stable_fragment_id_for_fragment(fragment);
        let logical_path = normalize_user_path(logical_path)?;
        let physical_object_id = physical_object_id.to_string();
        let metadata = metadata.clone();
        let activity =
            crate::core::upload::UploadActivityGuard::begin(&fragment, &physical_object_id)?;
        let guard = FinalizeGuard::acquire(format!("{fragment}\0{logical_path}")).await?;
        tokio::task::spawn_blocking(move || {
            let _activity = activity;
            let _guard = guard;
            flat::with_keys(&fragment, &[&logical_path], || {
                let final_id = flat::lookup(&fragment, &logical_path)?
                    .map(|p| p.object_id)
                    .unwrap_or_else(|| physical_object_id.clone());
                let _target =
                    crate::core::upload::UploadActivityGuard::begin(&fragment, &final_id)?;
                if final_id != physical_object_id {
                    index_store::rebind_unpublished_object_version(&metadata, &final_id)?;
                }
                let pool = metadata
                    .parts
                    .iter()
                    .flat_map(|part| &part.shards)
                    .find_map(|shard| {
                        crate::core::storage_pool::resolve_address(&shard.absolute_path)
                            .ok()
                            .and_then(|path| crate::core::storage_pool::pool_for_root(&path))
                    });
                flat::publish(&fragment, &logical_path, &final_id, pool)?;
                Ok(final_id)
            })
        })
        .await
        .map_err(worker_error)?
    }

    pub async fn bind_pending_upload(
        &self,
        fragment: &str,
        logical_path: &str,
        physical_object_id: &str,
        version_id: &str,
        session_id: &str,
    ) -> Result<(), AppError> {
        write_pending_binding(&PendingUploadBinding {
            fragment: fragment.into(),
            logical_path: normalize_user_path(logical_path)?,
            physical_object_id: physical_object_id.into(),
            version_id: version_id.into(),
            session_id: session_id.into(),
            created_at_unix: Utc::now().timestamp(),
        })
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
        let path = normalize_user_path(logical_path)?;
        Ok(index_store::list_pending_bindings(fragment)?
            .into_iter()
            .filter(|b| b.logical_path == path)
            .max_by(|a, b| a.created_at_unix.cmp(&b.created_at_unix)))
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
        let (fragment, params) = (fragment.to_string(), params.clone());
        tokio::task::spawn_blocking(move || {
            let limit = params
                .get("limit")
                .and_then(|s| s.parse().ok())
                .unwrap_or(100)
                .clamp(1, 1000);
            let prefix = params.get("prefix").cloned();
            let delimiter = params.get("delimiter").cloned();
            let after = params.get("continuation_token").cloned();
            if scan_trash {
                index_store::scan_trash_heads_page(&fragment, prefix, delimiter, limit, after, true)
            } else {
                index_store::scan_live_heads_page(&fragment, prefix, delimiter, limit, after, true)
            }
        })
        .await
        .map_err(worker_error)?
    }

    pub async fn list_live_object_refs_page(
        &self,
        fragment: &str,
        prefix: Option<&str>,
        continuation_token: Option<&str>,
        limit: usize,
    ) -> Result<NamespaceObjectRefPage, AppError> {
        let f = fragment.to_string();
        let p = prefix
            .map(normalize_user_path)
            .transpose()?
            .unwrap_or_default();
        let after = continuation_token.map(str::to_string);
        tokio::task::spawn_blocking(move || {
            let prefix = if p.is_empty() { p } else { format!("{p}/") };
            let (names, next) = flat::page(&f, &prefix, "", after.as_deref(), limit)?;
            let mut entries = Vec::with_capacity(names.len());
            for (key, _) in names {
                entries.push(object_ref(&f, &key)?);
            }
            Ok(NamespaceObjectRefPage {
                entries,
                continuation_token: next,
            })
        })
        .await
        .map_err(worker_error)?
    }

    pub fn stream_live_object_refs(
        &self,
        fragment: &str,
        prefix: Option<&str>,
        channel_capacity: usize,
    ) -> tokio::sync::mpsc::Receiver<Result<NamespaceObjectRef, AppError>> {
        let f = fragment.to_string();
        let p = prefix.map(normalize_user_path).transpose();
        let (tx, rx) = tokio::sync::mpsc::channel(channel_capacity.max(1));
        tokio::task::spawn_blocking(move || {
            let result = (|| -> Result<(), AppError> {
                let p = p?.unwrap_or_default();
                let prefix = if p.is_empty() { p } else { format!("{p}/") };
                flat::visit(&f, &prefix, |key| {
                    Ok(tx.blocking_send(Ok(object_ref(&f, key)?)).is_ok())
                })?;
                Ok(())
            })();
            if let Err(error) = result {
                let _ = tx.blocking_send(Err(error));
            }
        });
        rx
    }
}

fn worker_error(error: tokio::task::JoinError) -> AppError {
    AppError::InternalServerError(format!("N1 namespace worker: {error}"))
}
fn normalize_user_path(path: &str) -> Result<String, AppError> {
    let path = normalize_path(path).map_err(|e| match e {
        NamespaceError::InvalidPath(s) => AppError::BadRequest(s),
        _ => AppError::BadRequest(e.to_string()),
    })?;
    reject_reserved_user_path(&path)?;
    Ok(path)
}
fn namespace_entry(
    fragment: &str,
    path: String,
    kind: NamespaceEntryKind,
    object_id: Option<String>,
    lifecycle: LifecycleState,
    trash_entry_id: Option<String>,
    deleted_at_unix: Option<i64>,
) -> NamespaceEntry {
    NamespaceEntry {
        fragment: fragment.into(),
        parent_path: parent_path(&path),
        name: base_name(&path),
        path,
        kind,
        object_id,
        size_bytes: None,
        version_id: None,
        packed_ext_u64: None,
        lifecycle,
        trash_entry_id,
        deleted_at_unix,
        created_at_unix: 0,
        updated_at_unix: 0,
    }
}
fn object_head_from_meta(fragment: &str, meta: ObjectMetadata) -> ObjectHead {
    ObjectHead {
        fragment: fragment.into(),
        object_id: meta.object_key.clone(),
        storage_object_id: meta.object_key,
        current_version_id: meta.version_id,
        content_type: meta.content_type,
        total_size_bytes: meta.total_size_bytes,
        lifecycle: LifecycleState::Live,
        created_at_unix: meta.created_at,
        updated_at_unix: meta.created_at,
    }
}
fn live_entry(
    fragment: &str,
    path: &str,
    prefix_only: bool,
) -> Result<Option<NamespaceEntry>, AppError> {
    if path.is_empty() {
        return Ok(Some(namespace_entry(
            fragment,
            "".into(),
            NamespaceEntryKind::Directory,
            None,
            LifecycleState::Live,
            None,
            None,
        )));
    }
    if let Some(pointer) = if prefix_only {
        None
    } else {
        flat::lookup(fragment, path)?
    } {
        let mut entry = namespace_entry(
            fragment,
            path.into(),
            NamespaceEntryKind::File,
            Some(pointer.object_id.clone()),
            LifecycleState::Live,
            None,
            None,
        );
        if let Some(meta) = index_store::read_current_live_head(fragment, &pointer.object_id)? {
            entry.size_bytes = Some(meta.total_size_bytes);
            entry.version_id = Some(meta.version_id);
            entry.created_at_unix = meta.created_at;
            entry.updated_at_unix = meta.created_at;
        }
        return Ok(Some(entry));
    }
    if !flat::page(fragment, &format!("{path}/"), "", None, 1)?
        .0
        .is_empty()
    {
        return Ok(Some(namespace_entry(
            fragment,
            path.into(),
            NamespaceEntryKind::Directory,
            None,
            LifecycleState::Live,
            None,
            None,
        )));
    }
    Ok(None)
}
fn object_ref(fragment: &str, key: &str) -> Result<NamespaceObjectRef, AppError> {
    let pointer = flat::lookup(fragment, key)?.ok_or(AppError::NotFound)?;
    let meta = index_store::read_current_live_head(fragment, &pointer.object_id)?
        .ok_or(AppError::NotFound)?;
    Ok(NamespaceObjectRef {
        path: key.into(),
        object_id: pointer.object_id,
        version_id: meta.version_id,
        content_type: meta.content_type,
        total_size_bytes: meta.total_size_bytes,
        created_at: meta.created_at,
    })
}
fn summary(
    operation: &str,
    kind: NamespaceEntryKind,
    count: usize,
    source: Option<String>,
    destination: Option<String>,
) -> NamespaceMutationSummary {
    NamespaceMutationSummary {
        operation: operation.into(),
        kind,
        affected_paths: count,
        affected_objects: count,
        source_path: source,
        destination_path: destination,
        trash_entry_id: None,
        deleted_at_unix: None,
        conflicted_paths: Vec::new(),
        failed_paths: Vec::new(),
    }
}
fn record_outcome(
    result: &mut NamespaceMutationSummary,
    key: String,
    outcome: Result<bool, AppError>,
) {
    match outcome {
        Ok(true) => {
            result.affected_paths += 1;
            result.affected_objects += 1;
        }
        Ok(false) => (),
        Err(AppError::Conflict(_)) => result.conflicted_paths.push(key),
        Err(error) => result.failed_paths.push(format!("{key}: {error}")),
    }
}
fn trash_record_entry(fragment: &str, record: &TrashRecord) -> NamespaceEntry {
    namespace_entry(
        fragment,
        record.original_path.clone(),
        NamespaceEntryKind::File,
        record.object_keys.first().cloned(),
        LifecycleState::trashed(record.deleted_at_unix),
        Some(record.trash_entry_id.clone()),
        Some(record.deleted_at_unix),
    )
}
fn list_trash_dir_page(
    fragment: &str,
    path: &str,
    limit: usize,
    after: Option<&str>,
) -> Result<(Vec<NamespaceEntry>, Option<String>), AppError> {
    let records = flat::trash_records(fragment)?
        .into_iter()
        .filter(|r| {
            (path.is_empty()
                || r.original_path == path
                || r.original_path.starts_with(&format!("{path}/")))
                && after.is_none_or(|a| r.trash_entry_id.as_str() > a)
        })
        .take(limit.saturating_add(1))
        .collect::<Vec<_>>();
    let more = records.len() > limit;
    let entries = records
        .into_iter()
        .take(limit)
        .map(|r| trash_record_entry(fragment, &r))
        .collect::<Vec<_>>();
    let next = more
        .then(|| entries.last().and_then(|e| e.trash_entry_id.clone()))
        .flatten();
    Ok((entries, next))
}
fn recover_trash_path(
    fragment: &str,
    path: &str,
    id: Option<&str>,
) -> Result<NamespaceMutationSummary, AppError> {
    let record = match id {
        Some(id) => flat::trash_record_by_id(fragment, id)?,
        None => flat::trash_records(fragment)?
            .into_iter()
            .filter(|r| r.original_path == path)
            .max_by(|a, b| a.trash_entry_id.cmp(&b.trash_entry_id)),
    }
    .ok_or(AppError::NotFound)?;
    recover_record(fragment, &record)
}

fn recover_record(
    fragment: &str,
    record: &TrashRecord,
) -> Result<NamespaceMutationSummary, AppError> {
    if !flat::recover(fragment, record)? {
        return Err(AppError::NotFound);
    }
    let mut result = summary(
        "recover",
        NamespaceEntryKind::File,
        1,
        Some(record.original_path.clone()),
        Some(record.original_path.clone()),
    );
    result.trash_entry_id = Some(record.trash_entry_id.clone());
    result.deleted_at_unix = Some(record.deleted_at_unix);
    Ok(result)
}

async fn recover_records(
    fragment: &str,
    records: impl Iterator<Item = TrashRecord>,
    mut result: NamespaceMutationSummary,
) -> Result<NamespaceMutationSummary, AppError> {
    // The same bounded per-key concurrency as prefix rename/trash. A directory
    // is a selection of independent keys, never one global writer/lock.
    let mut outcomes = stream::iter(records.map(|record| {
        let fragment = fragment.to_string();
        let key = record.original_path.clone();
        async move {
            let outcome = tokio::task::spawn_blocking(move || flat::recover(&fragment, &record))
                .await
                .map_err(worker_error)
                .and_then(|r| r);
            (key, outcome)
        }
    }))
    .buffer_unordered(16);
    while let Some((key, outcome)) = outcomes.next().await {
        record_outcome(&mut result, key, outcome);
    }
    Ok(result)
}
#[cfg(test)]
fn prune_empty_trash_parents(trash_root: &Path, source_relative_path: &str) {
    let mut current = trash_root.join(source_relative_path);
    current.pop();
    while current != trash_root && current.starts_with(trash_root) {
        if fs::remove_dir(&current).is_err() {
            break;
        }
        current.pop();
    }
}
pub fn logical_not_found() -> AppError {
    AppError::NotFound
}
pub fn unexpected_response(status: axum::http::StatusCode, context: &str) -> AppError {
    AppError::InternalServerError(format!("{context}: unexpected response {status}"))
}
#[cfg(test)]
mod tests;
