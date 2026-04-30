use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, LazyLock};

use axum::http::StatusCode;
use chrono::Utc;
use dashmap::DashMap;
use indexdb::Database;
use indexdb::n1 as n1_indexdb;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::warn;
use ulid::Ulid;

use crate::core::disk_manager::{ListObjectsResult, ObjectMetadata, ObjectPathManager};
use crate::core::indexdb_store::{
    PartitionMeta, next_indexdb_metadata_generation, open_n1_indexdb,
    replay_indexdb_metadata_generation, seed_fragment_owner_indexdb_from_authoritative_copy,
    with_partition_commit_lock,
};
use crate::enums::app_error_enum::AppError;
use crate::namespace::models::{
    LifecycleState, NamespaceEntry, NamespaceEntryKind, NamespaceError, NamespaceMutationSummary,
    NamespaceReachabilitySnapshot, ObjectHead,
};
use crate::namespace::paths::{base_name, is_same_or_descendant, normalize_path, parent_path};

const INDEXDB_DIR: &str = "indexdb";
const INDEXDB_FILE: &str = "index.ndxdb";
const NAMESPACE_ROOT_NODE_ID: &str = "root";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingUploadBinding {
    pub fragment: String,
    pub logical_path: String,
    pub physical_object_id: String,
    pub version_id: String,
    pub session_id: String,
    pub created_at_unix: i64,
}

pub struct NamespaceRuntime {
    dbs: DashMap<PathBuf, Arc<Database>>,
    db_touch_ticks: DashMap<PathBuf, u64>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct NamespaceRuntimeStats {
    pub loaded_namespace_facades: u64,
    pub max_loaded_namespace_facades: u64,
    pub sampled_namespace_facades: u64,
    pub busy_namespace_facades: u64,
    pub loaded_namespace_dbs: u64,
    pub max_loaded_namespace_dbs: u64,
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

#[derive(Debug, Clone)]
struct NamespaceReplicaFileInfo {
    path: PathBuf,
    modified_at: std::time::SystemTime,
    len: u64,
    metadata_hash: [u8; 32],
}

#[derive(Debug, Clone, Default)]
struct NamespaceReplicaMutation {
    partition_meta: Option<PartitionMeta>,
    upsert_nodes: Vec<n1_indexdb::NamespaceNode>,
    move_nodes: Vec<NamespaceNodeMoveReplay>,
    upsert_trash_roots: Vec<n1_indexdb::NamespaceTrashRoot>,
    set_trash_root_states: Vec<(String, n1_indexdb::TrashEntryState, i64)>,
    upsert_entries: Vec<n1_indexdb::NamespaceEntry>,
    delete_paths: Vec<String>,
    rename_subtrees: Vec<NamespaceRenameReplay>,
    lifecycle_subtrees: Vec<NamespaceLifecycleReplay>,
    purge_subtrees: Vec<NamespacePurgeReplay>,
    upsert_trash_entries: Vec<n1_indexdb::TrashEntry>,
    set_trash_states: Vec<(String, n1_indexdb::TrashEntryState, i64)>,
    upsert_pending_uploads: Vec<n1_indexdb::PendingUploadBinding>,
    delete_pending_uploads: Vec<String>,
    upsert_object_metadata: Vec<(String, ObjectMetadata, i64)>,
}

#[derive(Debug, Clone)]
struct NamespaceRenameReplay {
    source_path: String,
    destination_path: String,
    changed_at_unix: i64,
}

#[derive(Debug, Clone)]
struct NamespaceLifecycleReplay {
    root_path: String,
    from: n1_indexdb::NamespaceLifecycle,
    target: n1_indexdb::NamespaceLifecycle,
    changed_at_unix: i64,
    trash_entry_id: Option<String>,
}

#[derive(Debug, Clone)]
struct NamespacePurgeReplay {
    root_path: Option<String>,
    trash_entry_id: Option<String>,
}

#[derive(Debug, Clone)]
struct NamespaceNodeMoveReplay {
    node_id: String,
    parent_node_id: String,
    name: String,
    changed_at_unix: i64,
}

impl NamespaceReplicaMutation {
    fn is_empty(&self) -> bool {
        self.upsert_nodes.is_empty()
            && self.move_nodes.is_empty()
            && self.upsert_trash_roots.is_empty()
            && self.set_trash_root_states.is_empty()
            && self.upsert_entries.is_empty()
            && self.delete_paths.is_empty()
            && self.rename_subtrees.is_empty()
            && self.lifecycle_subtrees.is_empty()
            && self.purge_subtrees.is_empty()
            && self.upsert_trash_entries.is_empty()
            && self.set_trash_states.is_empty()
            && self.upsert_pending_uploads.is_empty()
            && self.delete_pending_uploads.is_empty()
            && self.upsert_object_metadata.is_empty()
    }
}

fn seal_namespace_mutation(
    write_txn: &indexdb::WriteTransaction,
    mutation: &mut NamespaceReplicaMutation,
    changed_at_unix: i64,
) -> Result<(), AppError> {
    if !mutation.is_empty() {
        mutation.partition_meta = Some(next_indexdb_metadata_generation(
            write_txn,
            changed_at_unix,
        )?);
    }
    Ok(())
}

static GLOBAL_NAMESPACE_RUNTIME: LazyLock<NamespaceRuntime> = LazyLock::new(NamespaceRuntime::new);
static NAMESPACE_CACHE_TOUCH_TICK: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(1));
static NAMESPACE_MAX_LOADED_FACADES: LazyLock<AtomicUsize> =
    LazyLock::new(|| AtomicUsize::new(DEFAULT_NAMESPACE_MAX_LOADED_FACADES));
static NAMESPACE_MAX_LOADED_DBS: LazyLock<AtomicUsize> =
    LazyLock::new(|| AtomicUsize::new(DEFAULT_NAMESPACE_MAX_LOADED_DBS));

pub const DEFAULT_NAMESPACE_MAX_LOADED_FACADES: usize = 1;
pub const DEFAULT_NAMESPACE_MAX_LOADED_DBS: usize = 16;

pub fn global_namespace_runtime() -> &'static NamespaceRuntime {
    &GLOBAL_NAMESPACE_RUNTIME
}

pub fn configure_namespace_cache(max_loaded_facades: usize, max_loaded_dbs: usize) {
    NAMESPACE_MAX_LOADED_FACADES.store(max_loaded_facades.max(1), AtomicOrdering::Relaxed);
    NAMESPACE_MAX_LOADED_DBS.store(max_loaded_dbs.max(1), AtomicOrdering::Relaxed);
}

pub fn namespace_cache_config() -> (usize, usize) {
    (
        NAMESPACE_MAX_LOADED_FACADES
            .load(AtomicOrdering::Relaxed)
            .max(1),
        NAMESPACE_MAX_LOADED_DBS
            .load(AtomicOrdering::Relaxed)
            .max(1),
    )
}

fn next_namespace_cache_tick() -> u64 {
    NAMESPACE_CACHE_TOUCH_TICK.fetch_add(1, AtomicOrdering::Relaxed)
}

async fn run_namespace_blocking<T>(
    op: impl FnOnce() -> Result<T, AppError> + Send + 'static,
) -> Result<T, AppError>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(op).await.map_err(|err| {
        AppError::InternalServerError(format!("namespace indexdb worker failed: {err}"))
    })?
}

impl NamespaceRuntime {
    pub fn new() -> Self {
        Self {
            dbs: DashMap::new(),
            db_touch_ticks: DashMap::new(),
        }
    }

    pub fn evict_fragment(&self, fragment: &str) {
        if let Ok(paths) = namespace_db_paths(fragment) {
            for path in paths {
                self.dbs.remove(&path);
                self.db_touch_ticks.remove(&path);
            }
        }
    }

    pub fn runtime_stats(&self) -> NamespaceRuntimeStats {
        let (max_loaded_namespace_facades, max_loaded_namespace_dbs) = namespace_cache_config();
        NamespaceRuntimeStats {
            loaded_namespace_facades: 0,
            max_loaded_namespace_facades: max_loaded_namespace_facades as u64,
            loaded_namespace_dbs: self.dbs.len() as u64,
            max_loaded_namespace_dbs: max_loaded_namespace_dbs as u64,
            ..NamespaceRuntimeStats::default()
        }
    }

    fn touch_db_cache(&self, path: &Path) {
        self.db_touch_ticks
            .insert(path.to_path_buf(), next_namespace_cache_tick());
    }

    fn prune_db_cache(&self) {
        let max_loaded_dbs = namespace_cache_config().1;
        if self.dbs.len() <= max_loaded_dbs {
            return;
        }

        let mut evictable = self
            .dbs
            .iter()
            .filter_map(|entry| {
                if Arc::strong_count(entry.value()) > 1 {
                    return None;
                }
                let path = entry.key().clone();
                let touch = self
                    .db_touch_ticks
                    .get(&path)
                    .map(|value| *value)
                    .unwrap_or(0);
                Some((path, touch))
            })
            .collect::<Vec<_>>();
        evictable.sort_by_key(|(_, touch)| *touch);

        for (path, _) in evictable {
            if self.dbs.len() <= max_loaded_dbs {
                break;
            }
            self.dbs.remove(&path);
            self.db_touch_ticks.remove(&path);
        }
    }

    pub async fn list_dir(
        &self,
        fragment: &str,
        path: &str,
        include_trashed: bool,
    ) -> Result<Vec<NamespaceEntry>, AppError> {
        let parent = normalize_path(path).map_err(namespace_error_to_app)?;
        let db = self.open_db(fragment)?;
        let fragment = fragment.to_string();
        run_namespace_blocking(move || {
            if include_trashed {
                let (entries, _) =
                    list_trash_dir_page_on_db(&db, &fragment, &parent, usize::MAX / 2, None, 1000)?;
                return Ok(entries);
            }
            let Some((resolved_path, parent_node)) = resolve_live_node_on_db(&db, &parent)? else {
                return Ok(Vec::new());
            };
            if parent_node.kind != n1_indexdb::NamespaceEntryKind::Directory {
                return Ok(Vec::new());
            }
            let mut out = Vec::new();
            let mut after = None;
            loop {
                let nodes = db
                    .n1_list_namespace_node_children(&parent_node.node_id, after.as_deref(), 1000)
                    .map_err(|e| map_indexdb_error("namespace node child list", e))?;
                if nodes.is_empty() {
                    break;
                }
                after = nodes.last().map(|node| node.name.clone());
                for node in nodes {
                    let path = join_path(&resolved_path, &node.name);
                    out.push(node_entry_from_indexdb(
                        &fragment,
                        path,
                        node,
                        LifecycleState::Live,
                        None,
                        None,
                    ));
                }
            }
            Ok(out)
        })
        .await
    }

    pub async fn list_dir_page(
        &self,
        fragment: &str,
        path: &str,
        include_trashed: bool,
        limit: usize,
        continuation_token: Option<&str>,
    ) -> Result<(Vec<NamespaceEntry>, Option<String>), AppError> {
        let parent = normalize_path(path).map_err(namespace_error_to_app)?;
        let after_name = continuation_token.map(ToOwned::to_owned);
        let db = self.open_db(fragment)?;
        let fragment = fragment.to_string();
        let fetch_limit = limit.clamp(1, 1000).saturating_add(1);
        run_namespace_blocking(move || {
            if include_trashed {
                return list_trash_dir_page_on_db(
                    &db,
                    &fragment,
                    &parent,
                    limit,
                    after_name.as_deref(),
                    fetch_limit,
                );
            }
            let Some((resolved_path, parent_node)) = resolve_live_node_on_db(&db, &parent)? else {
                return Ok((Vec::new(), None));
            };
            if parent_node.kind != n1_indexdb::NamespaceEntryKind::Directory {
                return Ok((Vec::new(), None));
            }
            let rows = db
                .n1_list_namespace_node_children(
                    &parent_node.node_id,
                    after_name.as_deref(),
                    fetch_limit,
                )
                .map_err(|e| map_indexdb_error("namespace node child page", e))?;
            let mut entries = rows
                .into_iter()
                .map(|node| {
                    let path = join_path(&resolved_path, &node.name);
                    node_entry_from_indexdb(&fragment, path, node, LifecycleState::Live, None, None)
                })
                .collect::<Vec<_>>();
            let next = if entries.len() > limit {
                entries.pop();
                entries.last().map(|entry| entry.name.clone())
            } else {
                None
            };
            Ok((entries, next))
        })
        .await
    }

    pub async fn entry(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<Option<NamespaceEntry>, AppError> {
        let normalized = normalize_path(path).map_err(namespace_error_to_app)?;
        if normalized.is_empty() {
            return Ok(Some(root_namespace_entry(fragment)));
        }
        let db = self.open_db(fragment)?;
        let fragment = fragment.to_string();
        run_namespace_blocking(move || {
            Ok(
                resolve_live_node_on_db(&db, &normalized)?.map(|(path, node)| {
                    node_entry_from_indexdb(&fragment, path, node, LifecycleState::Live, None, None)
                }),
            )
        })
        .await
    }

    pub async fn trashed_entry(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<Option<NamespaceEntry>, AppError> {
        let normalized = normalize_path(path).map_err(namespace_error_to_app)?;
        let db = self.open_db(fragment)?;
        let fragment = fragment.to_string();
        run_namespace_blocking(move || trashed_entry_on_db(&db, &fragment, &normalized)).await
    }

    pub async fn head_for_path(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<Option<ObjectHead>, AppError> {
        let normalized = normalize_path(path).map_err(namespace_error_to_app)?;
        let db = self.open_db(fragment)?;
        let fragment = fragment.to_string();
        run_namespace_blocking(move || {
            let Some((path, node)) = resolve_live_node_on_db(&db, &normalized)? else {
                return Ok(None);
            };
            let Some(object_id) = node.object_id.as_deref() else {
                return Ok(None);
            };
            let Some(head) = db
                .n1_get_object_head(object_id)
                .map_err(|e| map_indexdb_error("namespace object head get", e))?
            else {
                return Ok(None);
            };
            let entry =
                node_entry_from_indexdb(&fragment, path, node, LifecycleState::Live, None, None);
            object_head_from_entry(&fragment, &entry, head).map(Some)
        })
        .await
    }

    pub async fn object_heads(
        &self,
        fragment: &str,
        object_ids: &[String],
    ) -> Result<HashMap<String, ObjectHead>, AppError> {
        if object_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let ids = object_ids.iter().cloned().collect::<BTreeSet<_>>();
        let db = self.open_db(fragment)?;
        let fragment = fragment.to_string();
        run_namespace_blocking(move || {
            let mut out = HashMap::new();
            for object_id in ids {
                if let Some(head) = db
                    .n1_get_object_head(&object_id)
                    .map_err(|e| map_indexdb_error("namespace object head get", e))?
                {
                    out.insert(object_id, object_head_from_head_row(&fragment, head));
                }
            }
            Ok(out)
        })
        .await
    }

    pub async fn mkdir(&self, fragment: &str, path: &str) -> Result<bool, AppError> {
        let normalized = normalize_path(path).map_err(namespace_error_to_app)?;
        if normalized.is_empty() {
            return Ok(false);
        }
        let db = self.open_db(fragment)?;
        let fragment_owned = fragment.to_string();
        let owner_path = namespace_db_path(fragment)?;
        let changed_at = Utc::now().timestamp();
        let mutation = run_namespace_blocking(move || {
            with_partition_commit_lock(&owner_path, || {
                let write_txn = db
                    .begin_write()
                    .map_err(|e| map_indexdb_error("namespace mkdir begin_write", e))?;
                let created = ensure_directory_path_nodes(&write_txn, &normalized, changed_at)?;
                let mut mutation = NamespaceReplicaMutation {
                    upsert_nodes: created,
                    ..Default::default()
                };
                seal_namespace_mutation(&write_txn, &mut mutation, changed_at)?;
                write_txn
                    .commit()
                    .map_err(|e| map_indexdb_error("namespace mkdir commit", e))?;
                Ok(mutation)
            })
        })
        .await?;
        let created = !mutation.is_empty();
        if created {
            replicate_namespace_mutation_for_fragment(self, &fragment_owned, &mutation)?;
        }
        Ok(created)
    }

    pub async fn rename(
        &self,
        fragment: &str,
        source_path: &str,
        destination_path: &str,
    ) -> Result<NamespaceMutationSummary, AppError> {
        let source = normalize_path(source_path).map_err(namespace_error_to_app)?;
        let destination = normalize_path(destination_path).map_err(namespace_error_to_app)?;
        if source.is_empty() || destination.is_empty() {
            return Err(AppError::BadRequest("root cannot be renamed".to_string()));
        }
        if is_same_or_descendant(&destination, &source) {
            return Err(AppError::Conflict(
                "destination cannot be inside the source subtree".to_string(),
            ));
        }
        let db = self.open_db(fragment)?;
        let fragment_owned = fragment.to_string();
        let owner_path = namespace_db_path(fragment)?;
        let changed_at = Utc::now().timestamp();
        let (summary, mutation) = run_namespace_blocking(move || {
            with_partition_commit_lock(&owner_path, || {
                let write_txn = db
                    .begin_write()
                    .map_err(|e| map_indexdb_error("namespace rename begin_write", e))?;
                ensure_namespace_root(&write_txn, changed_at)?;
                let Some((_, source_node)) = resolve_live_node_on_txn(&write_txn, &source)? else {
                    return Err(AppError::NotFound);
                };
                let destination_parent_path = parent_path(&destination);
                let destination_name = base_name(&destination);
                let Some((_, destination_parent_node)) =
                    resolve_live_node_on_txn(&write_txn, &destination_parent_path)?
                else {
                    return Err(AppError::NotFound);
                };
                if destination_parent_node.kind != n1_indexdb::NamespaceEntryKind::Directory {
                    return Err(AppError::Conflict(format!(
                        "destination parent '{}' is not a directory",
                        destination_parent_path
                    )));
                }
                if write_txn
                    .n1_get_namespace_child(&destination_parent_node.node_id, &destination_name)
                    .map_err(|e| map_indexdb_error("namespace rename destination child get", e))?
                    .is_some()
                {
                    return Err(AppError::Conflict(format!(
                        "destination path '{}' already exists",
                        destination
                    )));
                }
                if source_node.kind == n1_indexdb::NamespaceEntryKind::Directory
                    && is_descendant_node(
                        &write_txn,
                        &source_node.node_id,
                        &destination_parent_node.node_id,
                    )?
                {
                    return Err(AppError::Conflict(
                        "destination cannot be inside the source subtree".to_string(),
                    ));
                }
                write_txn
                    .n1_move_namespace_node(
                        &source_node.node_id,
                        &destination_parent_node.node_id,
                        &destination_name,
                        changed_at,
                    )
                    .map_err(|e| map_indexdb_error("namespace rename node move", e))?;
                let summary = NamespaceMutationSummary {
                    operation: "rename".to_string(),
                    kind: namespace_kind_from_indexdb(source_node.kind),
                    affected_paths: 1,
                    affected_objects: usize::from(source_node.object_id.is_some()),
                    source_path: Some(source.clone()),
                    destination_path: Some(destination.clone()),
                    trash_entry_id: None,
                    deleted_at_unix: None,
                };
                let mut mutation = NamespaceReplicaMutation {
                    move_nodes: vec![NamespaceNodeMoveReplay {
                        node_id: source_node.node_id,
                        parent_node_id: destination_parent_node.node_id,
                        name: destination_name,
                        changed_at_unix: changed_at,
                    }],
                    ..Default::default()
                };
                seal_namespace_mutation(&write_txn, &mut mutation, changed_at)?;
                write_txn
                    .commit()
                    .map_err(|e| map_indexdb_error("namespace rename commit", e))?;
                Ok((summary, mutation))
            })
        })
        .await?;
        replicate_namespace_mutation_for_fragment(self, &fragment_owned, &mutation)?;
        Ok(summary)
    }

    pub async fn trash(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<NamespaceMutationSummary, AppError> {
        self.set_lifecycle_for_path(fragment, path, n1_indexdb::NamespaceLifecycle::Trashed)
            .await
    }

    pub async fn recover(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<NamespaceMutationSummary, AppError> {
        self.set_lifecycle_for_path(fragment, path, n1_indexdb::NamespaceLifecycle::Live)
            .await
    }

    pub async fn recover_trash_entry_id(
        &self,
        fragment: &str,
        trash_entry_id: &str,
    ) -> Result<NamespaceMutationSummary, AppError> {
        self.set_lifecycle_for_trash_entry_id(
            fragment,
            trash_entry_id,
            n1_indexdb::NamespaceLifecycle::Live,
        )
        .await
    }

    pub async fn purge_trash_path(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<NamespaceMutationSummary, AppError> {
        self.purge_path(fragment, path, None).await
    }

    pub async fn purge_trash_entry_id(
        &self,
        fragment: &str,
        trash_entry_id: &str,
    ) -> Result<NamespaceMutationSummary, AppError> {
        self.purge_path(fragment, "", Some(trash_entry_id.to_string()))
            .await
    }

    pub async fn recover_all(&self, fragment: &str) -> Result<NamespaceMutationSummary, AppError> {
        self.set_lifecycle_for_all_trash(fragment, n1_indexdb::NamespaceLifecycle::Live)
            .await
    }

    pub async fn empty_trash(&self, fragment: &str) -> Result<NamespaceMutationSummary, AppError> {
        self.purge_all_trash(fragment).await
    }

    pub async fn reachability_snapshot_if_present(
        &self,
        fragment: &str,
    ) -> Result<Option<NamespaceReachabilitySnapshot>, AppError> {
        let Some(db) = self.open_existing_db(fragment)? else {
            return Ok(None);
        };
        run_namespace_blocking(move || {
            let mut snapshot = NamespaceReachabilitySnapshot::default();
            let mut saw_namespace_rows = false;
            let mut after = None;
            loop {
                let rows = db
                    .n1_list_namespace_subtree_nodes(
                        NAMESPACE_ROOT_NODE_ID,
                        "",
                        after.as_deref(),
                        1000,
                    )
                    .map_err(|e| map_indexdb_error("namespace reachability live scan", e))?;
                if rows.is_empty() {
                    break;
                }
                saw_namespace_rows = true;
                let last_path = rows
                    .last()
                    .map(|(path, _)| path.clone())
                    .unwrap_or_default();
                for (_, node) in rows {
                    if let Some(object_id) = node.object_id {
                        snapshot.referenced_object_ids.insert(object_id);
                    }
                    if let Some(version_id) = node.head_version_id {
                        snapshot.referenced_version_ids.insert(version_id);
                    }
                }
                if last_path.is_empty() {
                    break;
                }
                after = Some(last_path);
            }
            let mut roots_after = None;
            loop {
                let roots = db
                    .n1_list_namespace_trash_roots_all(roots_after.as_deref(), 1000)
                    .map_err(|e| map_indexdb_error("namespace reachability trash roots", e))?;
                if roots.is_empty() {
                    break;
                }
                saw_namespace_rows = true;
                roots_after = roots.last().map(|root| root.trash_entry_id.clone());
                for root in roots
                    .into_iter()
                    .filter(|root| root.state == n1_indexdb::TrashEntryState::Trashed)
                {
                    let mut after = None;
                    loop {
                        let rows = db
                            .n1_list_namespace_subtree_nodes(
                                &root.root_node_id,
                                &root.original_path,
                                after.as_deref(),
                                1000,
                            )
                            .map_err(|e| {
                                map_indexdb_error("namespace reachability trash scan", e)
                            })?;
                        if rows.is_empty() {
                            break;
                        }
                        saw_namespace_rows = true;
                        after = rows.last().map(|(path, _)| path.clone());
                        for (_, node) in rows {
                            if let Some(object_id) = node.object_id {
                                snapshot.referenced_object_ids.insert(object_id);
                            }
                            if let Some(version_id) = node.head_version_id {
                                snapshot.referenced_version_ids.insert(version_id);
                            }
                        }
                    }
                }
            }
            for binding in db
                .n1_list_pending_uploads()
                .map_err(|e| map_indexdb_error("namespace reachability pending uploads", e))?
            {
                saw_namespace_rows = true;
                snapshot
                    .referenced_object_ids
                    .insert(binding.physical_object_id);
                snapshot.referenced_version_ids.insert(binding.version_id);
            }
            Ok(saw_namespace_rows.then_some(snapshot))
        })
        .await
    }

    pub async fn purged_object_ids_for_gc(
        &self,
        fragment: &str,
    ) -> Result<BTreeSet<String>, AppError> {
        let Some(db) = self.open_existing_db(fragment)? else {
            return Ok(BTreeSet::new());
        };
        run_namespace_blocking(move || {
            let mut object_ids = BTreeSet::new();
            let mut roots_after = None;
            loop {
                let roots = db
                    .n1_list_namespace_trash_roots_all(roots_after.as_deref(), 1000)
                    .map_err(|e| map_indexdb_error("namespace gc purged roots", e))?;
                if roots.is_empty() {
                    break;
                }
                roots_after = roots.last().map(|root| root.trash_entry_id.clone());
                for root in roots
                    .into_iter()
                    .filter(|root| root.state == n1_indexdb::TrashEntryState::Purged)
                {
                    if let Some(object_id) = root.object_id {
                        object_ids.insert(object_id);
                    }
                    let mut after = None;
                    loop {
                        let rows = db
                            .n1_list_namespace_subtree_nodes(
                                &root.root_node_id,
                                &root.original_path,
                                after.as_deref(),
                                1000,
                            )
                            .map_err(|e| map_indexdb_error("namespace gc purged subtree", e))?;
                        if rows.is_empty() {
                            break;
                        }
                        after = rows.last().map(|(path, _)| path.clone());
                        for (_, node) in rows {
                            if let Some(object_id) = node.object_id {
                                object_ids.insert(object_id);
                            }
                        }
                    }
                }
            }
            Ok(object_ids)
        })
        .await
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
        logical_path: &str,
    ) -> Result<Option<String>, AppError> {
        let normalized = normalize_path(logical_path).map_err(namespace_error_to_app)?;
        let db = self.open_db(fragment)?;
        run_namespace_blocking(move || {
            Ok(resolve_live_node_on_db(&db, &normalized)?.and_then(|(_, node)| node.object_id))
        })
        .await
    }

    pub async fn resolve_any_object_id(
        &self,
        fragment: &str,
        logical_path: &str,
    ) -> Result<Option<String>, AppError> {
        let normalized = normalize_path(logical_path).map_err(namespace_error_to_app)?;
        let db = self.open_db(fragment)?;
        run_namespace_blocking(move || {
            if let Some((_, node)) = resolve_live_node_on_db(&db, &normalized)? {
                return Ok(node.object_id);
            }
            Ok(trashed_entry_on_db(&db, "", &normalized)?.and_then(|entry| entry.object_id))
        })
        .await
    }

    pub async fn select_object_id_for_write(
        &self,
        fragment: &str,
        logical_path: &str,
    ) -> Result<String, AppError> {
        let normalized = normalize_path(logical_path).map_err(namespace_error_to_app)?;
        let db = self.open_db(fragment)?;
        run_namespace_blocking(move || {
            if let Some((_, existing)) = resolve_live_node_on_db(&db, &normalized)? {
                if existing.kind == n1_indexdb::NamespaceEntryKind::Directory {
                    return Err(AppError::Conflict(format!(
                        "logical path '{}' already exists as a directory",
                        normalized
                    )));
                }
                if let Some(object_id) = existing.object_id {
                    return Ok(object_id);
                }
            }
            Ok(new_object_id())
        })
        .await
    }

    pub async fn finalize_logical_upload(
        &self,
        fragment: &str,
        logical_path: &str,
        physical_object_id: &str,
        metadata: &ObjectMetadata,
    ) -> Result<(), AppError> {
        let path = normalize_path(logical_path).map_err(namespace_error_to_app)?;
        if path.is_empty() {
            return Err(AppError::BadRequest(
                "root cannot be written as a file".to_string(),
            ));
        }
        let db = self.open_db(fragment)?;
        let fragment_owned = fragment.to_string();
        let owner_path = namespace_db_path(fragment)?;
        let object_id = physical_object_id.to_string();
        let version_id = metadata.version_id.clone();
        let metadata = metadata.clone();
        let changed_at = metadata.created_at.max(Utc::now().timestamp());
        let mutation = run_namespace_blocking(move || {
            with_partition_commit_lock(&owner_path, || {
                let write_txn = db
                    .begin_write()
                    .map_err(|e| map_indexdb_error("namespace finalize begin_write", e))?;
                let mut upsert_nodes =
                    ensure_directory_path_nodes(&write_txn, &parent_path(&path), changed_at)?;
                let parent_path = parent_path(&path);
                let Some((_, parent_node)) = resolve_live_node_on_txn(&write_txn, &parent_path)?
                else {
                    return Err(AppError::NotFound);
                };
                let name = base_name(&path);
                let existing = write_txn
                    .n1_get_namespace_child(&parent_node.node_id, &name)
                    .map_err(|e| map_indexdb_error("namespace finalize existing node get", e))?;
                if existing
                    .as_ref()
                    .is_some_and(|entry| entry.kind == n1_indexdb::NamespaceEntryKind::Directory)
                {
                    return Err(AppError::Conflict(format!(
                        "logical path '{}' already exists as a directory",
                        path
                    )));
                }
                upsert_namespace_object_metadata(&write_txn, &object_id, &metadata, changed_at)?;
                let created_at = existing
                    .as_ref()
                    .map(|node| node.created_at_unix)
                    .unwrap_or(changed_at);
                let node = n1_indexdb::NamespaceNode {
                    node_id: existing
                        .as_ref()
                        .map(|node| node.node_id.clone())
                        .unwrap_or_else(new_namespace_node_id),
                    parent_node_id: Some(parent_node.node_id),
                    name,
                    kind: n1_indexdb::NamespaceEntryKind::File,
                    object_id: Some(object_id.clone()),
                    head_version_id: Some(version_id),
                    created_at_unix: created_at,
                    updated_at_unix: changed_at,
                };
                write_txn
                    .n1_upsert_namespace_node(&node)
                    .map_err(|e| map_indexdb_error("namespace finalize node upsert", e))?;
                upsert_nodes.push(node);
                let mut mutation = NamespaceReplicaMutation {
                    upsert_nodes,
                    upsert_object_metadata: vec![(object_id, metadata, changed_at)],
                    ..Default::default()
                };
                seal_namespace_mutation(&write_txn, &mut mutation, changed_at)?;
                write_txn
                    .commit()
                    .map_err(|e| map_indexdb_error("namespace finalize commit", e))?;
                Ok(mutation)
            })
        })
        .await?;
        replicate_namespace_mutation_for_fragment(self, &fragment_owned, &mutation)?;
        Ok(())
    }

    pub async fn bind_pending_upload(
        &self,
        fragment: &str,
        logical_path: &str,
        physical_object_id: &str,
        version_id: &str,
        session_id: &str,
    ) -> Result<(), AppError> {
        let binding = n1_indexdb::PendingUploadBinding {
            logical_path: normalize_path(logical_path).map_err(namespace_error_to_app)?,
            physical_object_id: physical_object_id.to_string(),
            version_id: version_id.to_string(),
            session_id: session_id.to_string(),
            created_at_unix: Utc::now().timestamp(),
        };
        let db = self.open_db(fragment)?;
        let fragment_owned = fragment.to_string();
        let owner_path = namespace_db_path(fragment)?;
        let mutation = run_namespace_blocking(move || {
            with_partition_commit_lock(&owner_path, || {
                let write_txn = db
                    .begin_write()
                    .map_err(|e| map_indexdb_error("namespace pending begin_write", e))?;
                write_txn
                    .n1_upsert_pending_upload(&binding)
                    .map_err(|e| map_indexdb_error("namespace pending upsert", e))?;
                let mut mutation = NamespaceReplicaMutation {
                    upsert_pending_uploads: vec![binding],
                    ..Default::default()
                };
                seal_namespace_mutation(&write_txn, &mut mutation, Utc::now().timestamp())?;
                write_txn
                    .commit()
                    .map_err(|e| map_indexdb_error("namespace pending commit", e))?;
                Ok(mutation)
            })
        })
        .await?;
        replicate_namespace_mutation_for_fragment(self, &fragment_owned, &mutation)?;
        Ok(())
    }

    pub async fn pending_upload(
        &self,
        fragment: &str,
        version_id: &str,
    ) -> Result<Option<PendingUploadBinding>, AppError> {
        let db = self.open_db(fragment)?;
        let fragment = fragment.to_string();
        let version_id = version_id.to_string();
        run_namespace_blocking(move || {
            Ok(db
                .n1_get_pending_upload(&version_id)
                .map_err(|e| map_indexdb_error("namespace pending get", e))?
                .map(|binding| pending_upload_from_indexdb(&fragment, binding)))
        })
        .await
    }

    pub async fn list_pending_uploads(
        &self,
        fragment: &str,
    ) -> Result<Vec<PendingUploadBinding>, AppError> {
        let db = self.open_db(fragment)?;
        let fragment = fragment.to_string();
        run_namespace_blocking(move || {
            let rows = db
                .n1_list_pending_uploads()
                .map_err(|e| map_indexdb_error("namespace pending list", e))?;
            Ok(rows
                .into_iter()
                .map(|binding| pending_upload_from_indexdb(&fragment, binding))
                .collect())
        })
        .await
    }

    pub async fn find_pending_upload_by_logical_path(
        &self,
        fragment: &str,
        logical_path: &str,
    ) -> Result<Option<PendingUploadBinding>, AppError> {
        let normalized = normalize_path(logical_path).map_err(namespace_error_to_app)?;
        let db = self.open_db(fragment)?;
        let fragment = fragment.to_string();
        run_namespace_blocking(move || {
            Ok(db
                .n1_find_pending_upload_by_logical_path(&normalized)
                .map_err(|e| map_indexdb_error("namespace pending path get", e))?
                .map(|binding| pending_upload_from_indexdb(&fragment, binding)))
        })
        .await
    }

    pub async fn clear_pending_upload(
        &self,
        fragment: &str,
        version_id: &str,
    ) -> Result<(), AppError> {
        let db = self.open_db(fragment)?;
        let fragment_owned = fragment.to_string();
        let version_id = version_id.to_string();
        let owner_path = namespace_db_path(fragment)?;
        let mutation = run_namespace_blocking(move || {
            with_partition_commit_lock(&owner_path, || {
                let write_txn = db
                    .begin_write()
                    .map_err(|e| map_indexdb_error("namespace pending clear begin_write", e))?;
                write_txn
                    .n1_delete_pending_upload(&version_id)
                    .map_err(|e| map_indexdb_error("namespace pending clear", e))?;
                let mut mutation = NamespaceReplicaMutation {
                    delete_pending_uploads: vec![version_id],
                    ..Default::default()
                };
                seal_namespace_mutation(&write_txn, &mut mutation, Utc::now().timestamp())?;
                write_txn
                    .commit()
                    .map_err(|e| map_indexdb_error("namespace pending clear commit", e))?;
                Ok(mutation)
            })
        })
        .await?;
        replicate_namespace_mutation_for_fragment(self, &fragment_owned, &mutation)?;
        Ok(())
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
        let prefix = params
            .get("prefix")
            .map(|value| normalize_listing_prefix(value))
            .transpose()?;
        let delimiter_is_slash = params
            .get("delimiter")
            .map(|value| value.as_str() == "/")
            .unwrap_or(false);
        if params.contains_key("delimiter") && !delimiter_is_slash {
            return Err(AppError::BadRequest(
                "only '/' delimiter is supported".to_string(),
            ));
        }
        let after = params.get("continuation_token").cloned();
        let db = self.open_db(fragment)?;
        let fragment = fragment.to_string();
        run_namespace_blocking(move || {
            if !delimiter_is_slash {
                let rows = if scan_trash {
                    list_trash_object_summaries_on_db(
                        &db,
                        prefix.as_deref(),
                        after.as_deref(),
                        limit,
                    )?
                } else {
                    db.n1_list_namespace_node_object_summaries(
                        NAMESPACE_ROOT_NODE_ID,
                        "",
                        prefix.as_deref(),
                        after.as_deref(),
                        limit,
                    )
                    .map_err(|e| map_indexdb_error("namespace node object summary list", e))?
                };
                let next_cursor = (rows.len() >= limit)
                    .then(|| rows.last().map(|row| row.path.clone()))
                    .flatten();
                let objects = rows
                    .into_iter()
                    .map(|row| object_metadata_from_namespace_summary(&fragment, row))
                    .collect::<Result<Vec<_>, _>>()?;
                return Ok(ListObjectsResult {
                    objects,
                    common_prefixes: Vec::new(),
                    continuation_token: next_cursor,
                });
            }

            let mut objects = Vec::new();
            let mut common_prefixes = Vec::new();
            let mut common_seen = BTreeSet::new();
            let mut cursor = after;
            let mut next_cursor = None;

            while objects.len() + common_prefixes.len() < limit {
                let rows = if scan_trash {
                    list_trash_object_summaries_on_db(
                        &db,
                        prefix.as_deref(),
                        cursor.as_deref(),
                        256,
                    )?
                } else {
                    db.n1_list_namespace_node_object_summaries(
                        NAMESPACE_ROOT_NODE_ID,
                        "",
                        prefix.as_deref(),
                        cursor.as_deref(),
                        256,
                    )
                    .map_err(|e| map_indexdb_error("namespace node object list", e))?
                };
                if rows.is_empty() {
                    break;
                }
                for summary in rows {
                    cursor = Some(summary.path.clone());
                    if delimiter_is_slash {
                        if let Some(common_prefix) =
                            common_prefix_for_entry(prefix.as_deref(), &summary.path)
                        {
                            if common_seen.insert(common_prefix.clone()) {
                                common_prefixes.push(common_prefix);
                            }
                            if objects.len() + common_prefixes.len() >= limit {
                                next_cursor = cursor.clone();
                                break;
                            }
                            continue;
                        }
                    }
                    objects.push(object_metadata_from_namespace_summary(&fragment, summary)?);
                    if objects.len() + common_prefixes.len() >= limit {
                        next_cursor = cursor.clone();
                        break;
                    }
                }
                if next_cursor.is_some() {
                    break;
                }
            }

            Ok(ListObjectsResult {
                objects,
                common_prefixes,
                continuation_token: next_cursor,
            })
        })
        .await
    }

    fn open_db(&self, fragment: &str) -> Result<Arc<Database>, AppError> {
        let owner_path = namespace_db_path(fragment)?;
        if let Some(existing) = self.dbs.get(&owner_path) {
            self.touch_db_cache(&owner_path);
            return Ok(existing.clone());
        }
        let path = self
            .ensure_owner_namespace_db(fragment)?
            .unwrap_or(owner_path);
        if let Some(existing) = self.dbs.get(&path) {
            self.touch_db_cache(&path);
            return Ok(existing.clone());
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| AppError::Io(e, format!("mkdir {}", parent.display())))?;
        }
        let db = Arc::new(open_n1_indexdb(&path)?);
        self.dbs.insert(path.clone(), db.clone());
        self.touch_db_cache(&path);
        self.prune_db_cache();
        Ok(db)
    }

    fn open_existing_db(&self, fragment: &str) -> Result<Option<Arc<Database>>, AppError> {
        let owner_path = namespace_db_path(fragment)?;
        if let Some(existing) = self.dbs.get(&owner_path) {
            self.touch_db_cache(&owner_path);
            return Ok(Some(existing.clone()));
        }
        let Some(path) = self.ensure_owner_namespace_db(fragment)? else {
            return Ok(None);
        };
        if let Some(existing) = self.dbs.get(&path) {
            self.touch_db_cache(&path);
            return Ok(Some(existing.clone()));
        }
        if !path.exists() {
            return Ok(None);
        }
        let db = Arc::new(open_n1_indexdb(&path)?);
        self.dbs.insert(path.clone(), db.clone());
        self.touch_db_cache(&path);
        self.prune_db_cache();
        Ok(Some(db))
    }

    fn open_db_path(&self, path: PathBuf) -> Result<Arc<Database>, AppError> {
        if let Some(existing) = self.dbs.get(&path) {
            self.touch_db_cache(&path);
            return Ok(existing.clone());
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| AppError::Io(e, format!("mkdir {}", parent.display())))?;
        }
        let db = Arc::new(open_n1_indexdb(&path)?);
        self.dbs.insert(path.clone(), db.clone());
        self.touch_db_cache(&path);
        self.prune_db_cache();
        Ok(db)
    }

    fn ensure_owner_namespace_db(&self, fragment: &str) -> Result<Option<PathBuf>, AppError> {
        let owner_path = namespace_db_path(fragment)?;
        let copied_authoritative = seed_fragment_owner_indexdb_from_authoritative_copy(fragment)?;
        if copied_authoritative {
            self.dbs.remove(&owner_path);
            self.db_touch_ticks.remove(&owner_path);
        }

        if owner_path.exists() {
            return Ok(Some(owner_path));
        }

        let Some(replica_path) = best_existing_namespace_db_path(fragment)? else {
            return Ok(None);
        };
        if replica_path == owner_path {
            return Ok(Some(owner_path));
        }
        copy_namespace_db_file(&replica_path, &owner_path)?;
        Ok(Some(owner_path))
    }

    async fn set_lifecycle_for_path(
        &self,
        fragment: &str,
        path: &str,
        target: n1_indexdb::NamespaceLifecycle,
    ) -> Result<NamespaceMutationSummary, AppError> {
        let normalized = normalize_path(path).map_err(namespace_error_to_app)?;
        if normalized.is_empty() {
            return Err(AppError::BadRequest(
                "root lifecycle cannot be changed".to_string(),
            ));
        }
        let db = self.open_db(fragment)?;
        let fragment_owned = fragment.to_string();
        let owner_path = namespace_db_path(fragment)?;
        let changed_at = Utc::now().timestamp();
        let (summary, mutation) = run_namespace_blocking(move || {
            with_partition_commit_lock(&owner_path, || {
                let write_txn = db
                    .begin_write()
                    .map_err(|e| map_indexdb_error("namespace lifecycle begin_write", e))?;
                ensure_namespace_root(&write_txn, changed_at)?;
                if matches!(target, n1_indexdb::NamespaceLifecycle::Trashed) {
                    let Some((source_path, source_node)) =
                        resolve_live_node_on_txn(&write_txn, &normalized)?
                    else {
                        return Err(AppError::NotFound);
                    };
                    let Some(original_parent_node_id) = source_node.parent_node_id.clone() else {
                        return Err(AppError::BadRequest(
                            "root lifecycle cannot be changed".to_string(),
                        ));
                    };
                    let trash_entry_id = Ulid::new().to_string();
                    let trash_root = n1_indexdb::NamespaceTrashRoot {
                        trash_entry_id: trash_entry_id.clone(),
                        root_node_id: source_node.node_id.clone(),
                        original_parent_node_id,
                        original_name: source_node.name.clone(),
                        original_path: source_path.clone(),
                        kind: source_node.kind,
                        object_id: source_node.object_id.clone(),
                        head_version_id: source_node.head_version_id.clone(),
                        deleted_at_unix: changed_at,
                        retired_at_unix: None,
                        purged_at_unix: None,
                        state: n1_indexdb::TrashEntryState::Trashed,
                    };
                    write_txn
                        .n1_upsert_namespace_trash_root(&trash_root)
                        .map_err(|e| map_indexdb_error("namespace trash root upsert", e))?;
                    let trash_parent = trash_parent_node_id(&trash_entry_id);
                    write_txn
                        .n1_move_namespace_node(
                            &source_node.node_id,
                            &trash_parent,
                            &source_node.name,
                            changed_at,
                        )
                        .map_err(|e| map_indexdb_error("namespace trash node detach", e))?;
                    let source_kind = namespace_kind_from_indexdb(source_node.kind);
                    let affected_objects = usize::from(source_node.object_id.is_some());
                    let mut mutation = NamespaceReplicaMutation {
                        upsert_trash_roots: vec![trash_root],
                        move_nodes: vec![NamespaceNodeMoveReplay {
                            node_id: source_node.node_id.clone(),
                            parent_node_id: trash_parent,
                            name: source_node.name.clone(),
                            changed_at_unix: changed_at,
                        }],
                        ..Default::default()
                    };
                    seal_namespace_mutation(&write_txn, &mut mutation, changed_at)?;
                    write_txn
                        .commit()
                        .map_err(|e| map_indexdb_error("namespace lifecycle commit", e))?;
                    return Ok((
                        NamespaceMutationSummary {
                            operation: "trash".to_string(),
                            kind: source_kind,
                            affected_paths: 1,
                            affected_objects,
                            source_path: Some(source_path),
                            destination_path: None,
                            trash_entry_id: Some(trash_entry_id.clone()),
                            deleted_at_unix: Some(changed_at),
                        },
                        mutation,
                    ));
                }

                if matches!(target, n1_indexdb::NamespaceLifecycle::Live) {
                    let roots = db
                        .n1_list_namespace_trash_roots_by_path(
                            &normalized,
                            n1_indexdb::TrashEntryState::Trashed,
                        )
                        .map_err(|e| map_indexdb_error("namespace trash root path get", e))?;
                    if roots.len() > 1 {
                        return Err(AppError::Conflict(format!(
                            "trash path '{}' is ambiguous; use trash_entry_id",
                            normalized
                        )));
                    }
                    let Some(root) = roots.into_iter().next() else {
                        return Err(AppError::NotFound);
                    };
                    let Some(parent_node) = write_txn
                        .n1_get_namespace_node(&root.original_parent_node_id)
                        .map_err(|e| map_indexdb_error("namespace recover parent get", e))?
                    else {
                        return Err(AppError::Conflict(format!(
                            "original parent for '{}' no longer exists",
                            root.original_path
                        )));
                    };
                    if parent_node.kind != n1_indexdb::NamespaceEntryKind::Directory {
                        return Err(AppError::Conflict(format!(
                            "original parent for '{}' is not a directory",
                            root.original_path
                        )));
                    }
                    if write_txn
                        .n1_get_namespace_child(&root.original_parent_node_id, &root.original_name)
                        .map_err(|e| map_indexdb_error("namespace recover conflict get", e))?
                        .is_some()
                    {
                        return Err(AppError::Conflict(format!(
                            "destination path '{}' already exists",
                            root.original_path
                        )));
                    }
                    write_txn
                        .n1_move_namespace_node(
                            &root.root_node_id,
                            &root.original_parent_node_id,
                            &root.original_name,
                            changed_at,
                        )
                        .map_err(|e| map_indexdb_error("namespace recover node attach", e))?;
                    write_txn
                        .n1_set_namespace_trash_root_state(
                            &root.trash_entry_id,
                            n1_indexdb::TrashEntryState::Retired,
                            changed_at,
                        )
                        .map_err(|e| map_indexdb_error("namespace trash root retire", e))?;
                    let source_kind = namespace_kind_from_indexdb(root.kind);
                    let affected_objects = usize::from(root.object_id.is_some());
                    let mut mutation = NamespaceReplicaMutation {
                        move_nodes: vec![NamespaceNodeMoveReplay {
                            node_id: root.root_node_id.clone(),
                            parent_node_id: root.original_parent_node_id.clone(),
                            name: root.original_name.clone(),
                            changed_at_unix: changed_at,
                        }],
                        set_trash_root_states: vec![(
                            root.trash_entry_id.clone(),
                            n1_indexdb::TrashEntryState::Retired,
                            changed_at,
                        )],
                        ..Default::default()
                    };
                    seal_namespace_mutation(&write_txn, &mut mutation, changed_at)?;
                    write_txn
                        .commit()
                        .map_err(|e| map_indexdb_error("namespace lifecycle commit", e))?;
                    return Ok((
                        NamespaceMutationSummary {
                            operation: "recover".to_string(),
                            kind: source_kind,
                            affected_paths: 1,
                            affected_objects,
                            source_path: Some(root.original_path.clone()),
                            destination_path: None,
                            trash_entry_id: Some(root.trash_entry_id.clone()),
                            deleted_at_unix: None,
                        },
                        mutation,
                    ));
                }

                Err(AppError::BadRequest(
                    "unsupported namespace lifecycle transition".to_string(),
                ))
            })
        })
        .await?;
        replicate_namespace_mutation_for_fragment(self, &fragment_owned, &mutation)?;
        Ok(summary)
    }

    async fn set_lifecycle_for_trash_entry_id(
        &self,
        fragment: &str,
        trash_entry_id: &str,
        target: n1_indexdb::NamespaceLifecycle,
    ) -> Result<NamespaceMutationSummary, AppError> {
        let db = self.open_db(fragment)?;
        let fragment_owned = fragment.to_string();
        let trash_entry_id = trash_entry_id.to_string();
        let owner_path = namespace_db_path(fragment)?;
        let changed_at = Utc::now().timestamp();
        let (summary, mutation) = run_namespace_blocking(move || {
            with_partition_commit_lock(&owner_path, || {
                let write_txn = db
                    .begin_write()
                    .map_err(|e| map_indexdb_error("namespace recover id begin_write", e))?;
                let Some(root) = db
                    .n1_get_namespace_trash_root(&trash_entry_id)
                    .map_err(|e| map_indexdb_error("namespace trash root id get", e))?
                else {
                    return Err(AppError::NotFound);
                };
                if root.state != n1_indexdb::TrashEntryState::Trashed {
                    return Err(AppError::NotFound);
                }
                if !matches!(target, n1_indexdb::NamespaceLifecycle::Live) {
                    return Err(AppError::BadRequest(
                        "trash_entry_id lifecycle supports recover only".to_string(),
                    ));
                }
                if write_txn
                    .n1_get_namespace_child(&root.original_parent_node_id, &root.original_name)
                    .map_err(|e| map_indexdb_error("namespace recover id conflict get", e))?
                    .is_some()
                {
                    return Err(AppError::Conflict(format!(
                        "destination path '{}' already exists",
                        root.original_path
                    )));
                }
                write_txn
                    .n1_move_namespace_node(
                        &root.root_node_id,
                        &root.original_parent_node_id,
                        &root.original_name,
                        changed_at,
                    )
                    .map_err(|e| map_indexdb_error("namespace recover id node attach", e))?;
                write_txn
                    .n1_set_namespace_trash_root_state(
                        &root.trash_entry_id,
                        n1_indexdb::TrashEntryState::Retired,
                        changed_at,
                    )
                    .map_err(|e| map_indexdb_error("namespace recover id root retire", e))?;
                let mut mutation = NamespaceReplicaMutation {
                    move_nodes: vec![NamespaceNodeMoveReplay {
                        node_id: root.root_node_id.clone(),
                        parent_node_id: root.original_parent_node_id.clone(),
                        name: root.original_name.clone(),
                        changed_at_unix: changed_at,
                    }],
                    set_trash_root_states: vec![(
                        root.trash_entry_id.clone(),
                        n1_indexdb::TrashEntryState::Retired,
                        changed_at,
                    )],
                    ..Default::default()
                };
                seal_namespace_mutation(&write_txn, &mut mutation, changed_at)?;
                write_txn
                    .commit()
                    .map_err(|e| map_indexdb_error("namespace recover id commit", e))?;
                Ok((
                    NamespaceMutationSummary {
                        operation: "recover".to_string(),
                        kind: namespace_kind_from_indexdb(root.kind),
                        affected_paths: 1,
                        affected_objects: usize::from(root.object_id.is_some()),
                        source_path: Some(root.original_path.clone()),
                        destination_path: None,
                        trash_entry_id: Some(root.trash_entry_id.clone()),
                        deleted_at_unix: None,
                    },
                    mutation,
                ))
            })
        })
        .await?;
        replicate_namespace_mutation_for_fragment(self, &fragment_owned, &mutation)?;
        Ok(summary)
    }

    async fn set_lifecycle_for_all_trash(
        &self,
        fragment: &str,
        target: n1_indexdb::NamespaceLifecycle,
    ) -> Result<NamespaceMutationSummary, AppError> {
        let db = self.open_db(fragment)?;
        let fragment_owned = fragment.to_string();
        let owner_path = namespace_db_path(fragment)?;
        let changed_at = Utc::now().timestamp();
        let (summary, mutation) = run_namespace_blocking(move || {
            with_partition_commit_lock(&owner_path, || {
                let write_txn = db
                    .begin_write()
                    .map_err(|e| map_indexdb_error("namespace lifecycle begin_write", e))?;
                if !matches!(target, n1_indexdb::NamespaceLifecycle::Live) {
                    return Err(AppError::BadRequest(
                        "recover_all supports live target only".to_string(),
                    ));
                }
                let mut roots = Vec::new();
                let mut after = None;
                loop {
                    let page = db
                        .n1_list_namespace_trash_roots_all(after.as_deref(), 1000)
                        .map_err(|e| map_indexdb_error("namespace recover all roots list", e))?;
                    if page.is_empty() {
                        break;
                    }
                    after = page.last().map(|root| root.trash_entry_id.clone());
                    roots.extend(
                        page.into_iter()
                            .filter(|root| root.state == n1_indexdb::TrashEntryState::Trashed),
                    );
                }
                let mut mutation = NamespaceReplicaMutation::default();
                let mut affected_objects = 0usize;
                for root in roots.iter().cloned() {
                    if write_txn
                        .n1_get_namespace_child(&root.original_parent_node_id, &root.original_name)
                        .map_err(|e| map_indexdb_error("namespace recover all conflict get", e))?
                        .is_some()
                    {
                        return Err(AppError::Conflict(format!(
                            "destination path '{}' already exists",
                            root.original_path
                        )));
                    }
                    write_txn
                        .n1_move_namespace_node(
                            &root.root_node_id,
                            &root.original_parent_node_id,
                            &root.original_name,
                            changed_at,
                        )
                        .map_err(|e| map_indexdb_error("namespace recover all node attach", e))?;
                    write_txn
                        .n1_set_namespace_trash_root_state(
                            &root.trash_entry_id,
                            n1_indexdb::TrashEntryState::Retired,
                            changed_at,
                        )
                        .map_err(|e| map_indexdb_error("namespace recover all root retire", e))?;
                    affected_objects += usize::from(root.object_id.is_some());
                    mutation.move_nodes.push(NamespaceNodeMoveReplay {
                        node_id: root.root_node_id,
                        parent_node_id: root.original_parent_node_id,
                        name: root.original_name,
                        changed_at_unix: changed_at,
                    });
                    mutation.set_trash_root_states.push((
                        root.trash_entry_id,
                        n1_indexdb::TrashEntryState::Retired,
                        changed_at,
                    ));
                }
                let affected_paths = mutation.move_nodes.len();
                seal_namespace_mutation(&write_txn, &mut mutation, changed_at)?;
                write_txn
                    .commit()
                    .map_err(|e| map_indexdb_error("namespace lifecycle commit", e))?;
                Ok((
                    NamespaceMutationSummary {
                        operation: "recover".to_string(),
                        kind: NamespaceEntryKind::Directory,
                        affected_paths,
                        affected_objects,
                        source_path: None,
                        destination_path: None,
                        trash_entry_id: None,
                        deleted_at_unix: None,
                    },
                    mutation,
                ))
            })
        })
        .await?;
        replicate_namespace_mutation_for_fragment(self, &fragment_owned, &mutation)?;
        Ok(summary)
    }

    async fn purge_path(
        &self,
        fragment: &str,
        path: &str,
        trash_entry_id: Option<String>,
    ) -> Result<NamespaceMutationSummary, AppError> {
        let normalized = normalize_path(path).map_err(namespace_error_to_app)?;
        let db = self.open_db(fragment)?;
        let fragment_owned = fragment.to_string();
        let owner_path = namespace_db_path(fragment)?;
        let (summary, mutation) = run_namespace_blocking(move || {
            with_partition_commit_lock(&owner_path, || {
                let write_txn = db
                    .begin_write()
                    .map_err(|e| map_indexdb_error("namespace purge begin_write", e))?;
                let root = if let Some(trash_entry_id) = trash_entry_id.as_deref() {
                    db.n1_get_namespace_trash_root(trash_entry_id)
                        .map_err(|e| map_indexdb_error("namespace purge root id get", e))?
                } else {
                    let roots = db
                        .n1_list_namespace_trash_roots_by_path(
                            &normalized,
                            n1_indexdb::TrashEntryState::Trashed,
                        )
                        .map_err(|e| map_indexdb_error("namespace purge root path get", e))?;
                    if roots.len() > 1 {
                        return Err(AppError::Conflict(format!(
                            "trash path '{}' is ambiguous; use trash_entry_id",
                            normalized
                        )));
                    }
                    roots.into_iter().next()
                };
                let Some(root) = root else {
                    return Err(AppError::NotFound);
                };
                if root.state != n1_indexdb::TrashEntryState::Trashed {
                    return Err(AppError::NotFound);
                }
                let changed_at = Utc::now().timestamp();
                write_txn
                    .n1_set_namespace_trash_root_state(
                        &root.trash_entry_id,
                        n1_indexdb::TrashEntryState::Purged,
                        changed_at,
                    )
                    .map_err(|e| map_indexdb_error("namespace trash root purge", e))?;
                let mut mutation = NamespaceReplicaMutation {
                    set_trash_root_states: vec![(
                        root.trash_entry_id.clone(),
                        n1_indexdb::TrashEntryState::Purged,
                        changed_at,
                    )],
                    ..Default::default()
                };
                seal_namespace_mutation(&write_txn, &mut mutation, changed_at)?;
                write_txn
                    .commit()
                    .map_err(|e| map_indexdb_error("namespace purge commit", e))?;
                Ok((
                    NamespaceMutationSummary {
                        operation: "purge_trash".to_string(),
                        kind: namespace_kind_from_indexdb(root.kind),
                        affected_paths: 1,
                        affected_objects: usize::from(root.object_id.is_some()),
                        source_path: Some(root.original_path.clone()),
                        destination_path: None,
                        trash_entry_id: Some(root.trash_entry_id.clone()),
                        deleted_at_unix: None,
                    },
                    mutation,
                ))
            })
        })
        .await?;
        replicate_namespace_mutation_for_fragment(self, &fragment_owned, &mutation)?;
        Ok(summary)
    }

    async fn purge_all_trash(&self, fragment: &str) -> Result<NamespaceMutationSummary, AppError> {
        let db = self.open_db(fragment)?;
        let fragment_owned = fragment.to_string();
        let owner_path = namespace_db_path(fragment)?;
        let (summary, mutation) = run_namespace_blocking(move || {
            with_partition_commit_lock(&owner_path, || {
                let write_txn = db
                    .begin_write()
                    .map_err(|e| map_indexdb_error("namespace empty trash begin_write", e))?;
                let mut roots = Vec::new();
                let mut after = None;
                loop {
                    let page = db
                        .n1_list_namespace_trash_roots_all(after.as_deref(), 1000)
                        .map_err(|e| map_indexdb_error("namespace empty trash roots list", e))?;
                    if page.is_empty() {
                        break;
                    }
                    after = page.last().map(|root| root.trash_entry_id.clone());
                    roots.extend(
                        page.into_iter()
                            .filter(|root| root.state == n1_indexdb::TrashEntryState::Trashed),
                    );
                }
                if roots.is_empty() {
                    return Err(AppError::NotFound);
                }
                let changed_at = Utc::now().timestamp();
                let mut mutation = NamespaceReplicaMutation::default();
                let mut affected_objects = 0usize;
                for root in roots {
                    write_txn
                        .n1_set_namespace_trash_root_state(
                            &root.trash_entry_id,
                            n1_indexdb::TrashEntryState::Purged,
                            changed_at,
                        )
                        .map_err(|e| map_indexdb_error("namespace trash root purge", e))?;
                    affected_objects += usize::from(root.object_id.is_some());
                    mutation.set_trash_root_states.push((
                        root.trash_entry_id,
                        n1_indexdb::TrashEntryState::Purged,
                        changed_at,
                    ));
                }
                let affected_paths = mutation.set_trash_root_states.len();
                seal_namespace_mutation(&write_txn, &mut mutation, changed_at)?;
                write_txn
                    .commit()
                    .map_err(|e| map_indexdb_error("namespace empty trash commit", e))?;
                Ok((
                    NamespaceMutationSummary {
                        operation: "purge_trash".to_string(),
                        kind: NamespaceEntryKind::Directory,
                        affected_paths,
                        affected_objects,
                        source_path: None,
                        destination_path: None,
                        trash_entry_id: None,
                        deleted_at_unix: None,
                    },
                    mutation,
                ))
            })
        })
        .await?;
        replicate_namespace_mutation_for_fragment(self, &fragment_owned, &mutation)?;
        Ok(summary)
    }
}

fn new_hash_id(scope: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(scope.as_bytes());
    hasher.update([0u8]);
    hasher.update(Ulid::new().to_string().as_bytes());
    hex::encode(hasher.finalize())
}

fn new_namespace_node_id() -> String {
    new_hash_id("namespace-node")
}

fn new_object_id() -> String {
    new_hash_id("object")
}

fn trash_parent_node_id(trash_entry_id: &str) -> String {
    format!("trash:{trash_entry_id}")
}

fn join_path(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{parent}/{name}")
    }
}

fn root_namespace_node(changed_at: i64) -> n1_indexdb::NamespaceNode {
    n1_indexdb::NamespaceNode {
        node_id: NAMESPACE_ROOT_NODE_ID.to_string(),
        parent_node_id: None,
        name: String::new(),
        kind: n1_indexdb::NamespaceEntryKind::Directory,
        object_id: None,
        head_version_id: None,
        created_at_unix: changed_at,
        updated_at_unix: changed_at,
    }
}

fn ensure_namespace_root(
    write_txn: &indexdb::WriteTransaction,
    changed_at: i64,
) -> Result<Option<n1_indexdb::NamespaceNode>, AppError> {
    if write_txn
        .n1_get_namespace_node(NAMESPACE_ROOT_NODE_ID)
        .map_err(|e| map_indexdb_error("namespace root get", e))?
        .is_some()
    {
        return Ok(None);
    }
    let root = root_namespace_node(changed_at);
    write_txn
        .n1_upsert_namespace_node(&root)
        .map_err(|e| map_indexdb_error("namespace root upsert", e))?;
    Ok(Some(root))
}

fn resolve_live_node_on_db(
    db: &Database,
    path: &str,
) -> Result<Option<(String, n1_indexdb::NamespaceNode)>, AppError> {
    if path.is_empty() {
        let root = db
            .n1_get_namespace_node(NAMESPACE_ROOT_NODE_ID)
            .map_err(|e| map_indexdb_error("namespace root get", e))?
            .unwrap_or_else(|| root_namespace_node(0));
        return Ok(Some((String::new(), root)));
    }

    let mut current = db
        .n1_get_namespace_node(NAMESPACE_ROOT_NODE_ID)
        .map_err(|e| map_indexdb_error("namespace root get", e))?
        .unwrap_or_else(|| root_namespace_node(0));
    let mut current_path = String::new();
    for segment in path.split('/').filter(|segment| !segment.is_empty()) {
        let Some(child) = db
            .n1_get_namespace_child(&current.node_id, segment)
            .map_err(|e| map_indexdb_error("namespace child get", e))?
        else {
            return Ok(None);
        };
        current_path = join_path(&current_path, segment);
        current = child;
    }
    Ok(Some((current_path, current)))
}

fn resolve_live_node_on_txn(
    write_txn: &indexdb::WriteTransaction,
    path: &str,
) -> Result<Option<(String, n1_indexdb::NamespaceNode)>, AppError> {
    if path.is_empty() {
        let root = write_txn
            .n1_get_namespace_node(NAMESPACE_ROOT_NODE_ID)
            .map_err(|e| map_indexdb_error("namespace root get", e))?
            .unwrap_or_else(|| root_namespace_node(0));
        return Ok(Some((String::new(), root)));
    }

    let mut current = write_txn
        .n1_get_namespace_node(NAMESPACE_ROOT_NODE_ID)
        .map_err(|e| map_indexdb_error("namespace root get", e))?
        .unwrap_or_else(|| root_namespace_node(0));
    let mut current_path = String::new();
    for segment in path.split('/').filter(|segment| !segment.is_empty()) {
        let Some(child) = write_txn
            .n1_get_namespace_child(&current.node_id, segment)
            .map_err(|e| map_indexdb_error("namespace child get", e))?
        else {
            return Ok(None);
        };
        current_path = join_path(&current_path, segment);
        current = child;
    }
    Ok(Some((current_path, current)))
}

fn resolve_relative_node_on_db(
    db: &Database,
    root_node_id: &str,
    relative_path: &str,
) -> Result<Option<n1_indexdb::NamespaceNode>, AppError> {
    let mut current = db
        .n1_get_namespace_node(root_node_id)
        .map_err(|e| map_indexdb_error("namespace relative root get", e))?;
    for segment in relative_path
        .split('/')
        .filter(|segment| !segment.is_empty())
    {
        let Some(node) = current else {
            return Ok(None);
        };
        current = db
            .n1_get_namespace_child(&node.node_id, segment)
            .map_err(|e| map_indexdb_error("namespace relative child get", e))?;
    }
    Ok(current)
}

fn ensure_directory_path_nodes(
    write_txn: &indexdb::WriteTransaction,
    path: &str,
    changed_at: i64,
) -> Result<Vec<n1_indexdb::NamespaceNode>, AppError> {
    let mut created = Vec::new();
    if let Some(root) = ensure_namespace_root(write_txn, changed_at)? {
        created.push(root);
    }
    if path.is_empty() {
        return Ok(created);
    }

    let mut current = write_txn
        .n1_get_namespace_node(NAMESPACE_ROOT_NODE_ID)
        .map_err(|e| map_indexdb_error("namespace root get", e))?
        .unwrap_or_else(|| root_namespace_node(changed_at));
    for segment in path.split('/').filter(|segment| !segment.is_empty()) {
        if let Some(existing) = write_txn
            .n1_get_namespace_child(&current.node_id, segment)
            .map_err(|e| map_indexdb_error("namespace mkdir child get", e))?
        {
            if existing.kind == n1_indexdb::NamespaceEntryKind::File {
                return Err(AppError::Conflict(format!(
                    "path '{}' already exists as a file",
                    segment
                )));
            }
            current = existing;
            continue;
        }
        let node = n1_indexdb::NamespaceNode {
            node_id: new_namespace_node_id(),
            parent_node_id: Some(current.node_id.clone()),
            name: segment.to_string(),
            kind: n1_indexdb::NamespaceEntryKind::Directory,
            object_id: None,
            head_version_id: None,
            created_at_unix: changed_at,
            updated_at_unix: changed_at,
        };
        write_txn
            .n1_upsert_namespace_node(&node)
            .map_err(|e| map_indexdb_error("namespace mkdir node upsert", e))?;
        current = node.clone();
        created.push(node);
    }
    Ok(created)
}

fn node_entry_from_indexdb(
    fragment: &str,
    path: String,
    node: n1_indexdb::NamespaceNode,
    lifecycle: LifecycleState,
    trash_entry_id: Option<String>,
    deleted_at_unix: Option<i64>,
) -> NamespaceEntry {
    NamespaceEntry {
        fragment: fragment.to_string(),
        parent_path: parent_path(&path),
        name: base_name(&path),
        path,
        kind: namespace_kind_from_indexdb(node.kind),
        object_id: node.object_id,
        lifecycle,
        trash_entry_id,
        deleted_at_unix,
        created_at_unix: node.created_at_unix,
        updated_at_unix: node.updated_at_unix,
    }
}

fn trash_root_to_namespace_entry(
    fragment: &str,
    root: &n1_indexdb::NamespaceTrashRoot,
) -> NamespaceEntry {
    NamespaceEntry {
        fragment: fragment.to_string(),
        path: root.original_path.clone(),
        parent_path: parent_path(&root.original_path),
        name: base_name(&root.original_path),
        kind: namespace_kind_from_indexdb(root.kind),
        object_id: root.object_id.clone(),
        lifecycle: LifecycleState::trashed(root.deleted_at_unix),
        trash_entry_id: Some(root.trash_entry_id.clone()),
        deleted_at_unix: Some(root.deleted_at_unix),
        created_at_unix: root.deleted_at_unix,
        updated_at_unix: root.deleted_at_unix,
    }
}

fn is_descendant_node(
    write_txn: &indexdb::WriteTransaction,
    ancestor_node_id: &str,
    node_id: &str,
) -> Result<bool, AppError> {
    let mut cursor = Some(node_id.to_string());
    while let Some(current_id) = cursor {
        if current_id == ancestor_node_id {
            return Ok(true);
        }
        cursor = write_txn
            .n1_get_namespace_node(&current_id)
            .map_err(|e| map_indexdb_error("namespace ancestor get", e))?
            .and_then(|node| node.parent_node_id);
    }
    Ok(false)
}

fn list_trash_dir_page_on_db(
    db: &Database,
    fragment: &str,
    parent: &str,
    limit: usize,
    continuation_token: Option<&str>,
    fetch_limit: usize,
) -> Result<(Vec<NamespaceEntry>, Option<String>), AppError> {
    if parent.is_empty() {
        let roots = db
            .n1_list_namespace_trash_roots(
                n1_indexdb::TrashEntryState::Trashed,
                continuation_token,
                fetch_limit,
            )
            .map_err(|e| map_indexdb_error("namespace trash roots list", e))?;
        let mut entries = roots
            .into_iter()
            .map(|root| trash_root_to_namespace_entry(fragment, &root))
            .collect::<Vec<_>>();
        let next = if entries.len() > limit {
            entries.pop();
            entries.last().map(|entry| entry.path.clone())
        } else {
            None
        };
        return Ok((entries, next));
    }

    let mut entries = Vec::new();
    let mut seen = BTreeSet::new();
    let mut roots_after = None;
    loop {
        let roots = db
            .n1_list_namespace_trash_roots_all(roots_after.as_deref(), 1000)
            .map_err(|e| map_indexdb_error("namespace trash roots scan", e))?;
        if roots.is_empty() {
            break;
        }
        roots_after = roots.last().map(|root| root.trash_entry_id.clone());
        for root in roots
            .into_iter()
            .filter(|root| root.state == n1_indexdb::TrashEntryState::Trashed)
        {
            if parent_path(&root.original_path) == parent {
                let entry = trash_root_to_namespace_entry(fragment, &root);
                let key = format!("{}\0{}", entry.path, root.trash_entry_id);
                if seen.insert(key) {
                    entries.push(entry);
                }
                continue;
            }

            if let Some(relative_to_parent) = root.original_path.strip_prefix(&format!("{parent}/"))
            {
                if let Some((next_segment, _)) = relative_to_parent.split_once('/') {
                    let path = join_path(parent, next_segment);
                    if seen.insert(path.clone()) {
                        entries.push(synthetic_trash_directory_entry(
                            fragment,
                            path,
                            root.deleted_at_unix,
                        ));
                    }
                    continue;
                }
            }

            let Some(relative_parent) = parent
                .strip_prefix(&format!("{}/", root.original_path))
                .or_else(|| (parent == root.original_path).then_some(""))
            else {
                continue;
            };
            let Some(parent_node) =
                resolve_relative_node_on_db(db, &root.root_node_id, relative_parent)?
            else {
                continue;
            };
            if parent_node.kind != n1_indexdb::NamespaceEntryKind::Directory {
                continue;
            }
            let children = db
                .n1_list_namespace_node_children(&parent_node.node_id, None, fetch_limit)
                .map_err(|e| map_indexdb_error("namespace trash child list", e))?;
            for child in children {
                let path = join_path(parent, &child.name);
                let key = format!("{}\0{}", path, root.trash_entry_id);
                if seen.insert(key) {
                    entries.push(node_entry_from_indexdb(
                        fragment,
                        path,
                        child,
                        LifecycleState::trashed(root.deleted_at_unix),
                        Some(root.trash_entry_id.clone()),
                        Some(root.deleted_at_unix),
                    ));
                }
            }
        }
    }

    entries.sort_by(|left, right| {
        left.path
            .cmp(&right.path)
            .then_with(|| left.trash_entry_id.cmp(&right.trash_entry_id))
    });
    if let Some(after) = continuation_token {
        entries.retain(|entry| entry.path.as_str() > after);
    }
    let mut entries = entries.into_iter().take(fetch_limit).collect::<Vec<_>>();
    let next = if entries.len() > limit {
        entries.pop();
        entries.last().map(|entry| entry.path.clone())
    } else {
        None
    };
    Ok((entries, next))
}

fn synthetic_trash_directory_entry(
    fragment: &str,
    path: String,
    deleted_at_unix: i64,
) -> NamespaceEntry {
    let name = path.rsplit('/').next().unwrap_or("").to_string();
    NamespaceEntry {
        fragment: fragment.to_string(),
        parent_path: parent_path(&path),
        path,
        name,
        kind: NamespaceEntryKind::Directory,
        object_id: None,
        lifecycle: LifecycleState::trashed(deleted_at_unix),
        trash_entry_id: None,
        deleted_at_unix: Some(deleted_at_unix),
        created_at_unix: deleted_at_unix,
        updated_at_unix: deleted_at_unix,
    }
}

fn trashed_entry_on_db(
    db: &Database,
    fragment: &str,
    path: &str,
) -> Result<Option<NamespaceEntry>, AppError> {
    let direct_roots = db
        .n1_list_namespace_trash_roots_by_path(path, n1_indexdb::TrashEntryState::Trashed)
        .map_err(|e| map_indexdb_error("namespace trash entry direct get", e))?;
    if let Some(root) = direct_roots.into_iter().next() {
        return Ok(Some(trash_root_to_namespace_entry(fragment, &root)));
    }

    let mut after = None;
    loop {
        let roots = db
            .n1_list_namespace_trash_roots_all(after.as_deref(), 1000)
            .map_err(|e| map_indexdb_error("namespace trash entry roots scan", e))?;
        if roots.is_empty() {
            break;
        }
        after = roots.last().map(|root| root.trash_entry_id.clone());
        for root in roots
            .into_iter()
            .filter(|root| root.state == n1_indexdb::TrashEntryState::Trashed)
        {
            let Some(relative) = path.strip_prefix(&format!("{}/", root.original_path)) else {
                continue;
            };
            if let Some(node) = resolve_relative_node_on_db(db, &root.root_node_id, relative)? {
                return Ok(Some(node_entry_from_indexdb(
                    fragment,
                    path.to_string(),
                    node,
                    LifecycleState::trashed(root.deleted_at_unix),
                    Some(root.trash_entry_id),
                    Some(root.deleted_at_unix),
                )));
            }
        }
    }
    Ok(None)
}

fn list_trash_object_summaries_on_db(
    db: &Database,
    prefix: Option<&str>,
    after_path: Option<&str>,
    limit: usize,
) -> Result<Vec<n1_indexdb::NamespaceObjectSummary>, AppError> {
    let mut summaries = Vec::new();
    let mut roots_after = None;
    loop {
        let roots = db
            .n1_list_namespace_trash_roots_all(roots_after.as_deref(), 1000)
            .map_err(|e| map_indexdb_error("namespace trash summary roots list", e))?;
        if roots.is_empty() {
            break;
        }
        roots_after = roots.last().map(|root| root.trash_entry_id.clone());
        for root in roots
            .into_iter()
            .filter(|root| root.state == n1_indexdb::TrashEntryState::Trashed)
        {
            let rows = db
                .n1_list_namespace_node_object_summaries(
                    &root.root_node_id,
                    &root.original_path,
                    prefix,
                    after_path,
                    limit,
                )
                .map_err(|e| map_indexdb_error("namespace trash summary subtree", e))?;
            summaries.extend(rows);
        }
    }
    summaries.sort_by(|left, right| left.path.cmp(&right.path));
    summaries.truncate(limit);
    Ok(summaries)
}

fn upsert_namespace_object_metadata(
    write_txn: &indexdb::WriteTransaction,
    object_id: &str,
    metadata: &ObjectMetadata,
    changed_at: i64,
) -> Result<(), AppError> {
    let mut parts = Vec::with_capacity(metadata.parts.len());
    for part in &metadata.parts {
        let mut shards = Vec::with_capacity(part.shards.len());
        for shard in &part.shards {
            shards.push(n1_indexdb::PartShard {
                shard_index: u64::try_from(shard.shard_index).map_err(|_| {
                    AppError::InternalServerError(format!(
                        "shard index {} does not fit in u64",
                        shard.shard_index
                    ))
                })?,
                file_name: shard.file_name.clone(),
                absolute_path: shard.absolute_path.clone(),
                relative_path: None,
                blob_root_id: None,
                size_bytes: shard.size_bytes,
                checksum: shard.checksum.clone(),
                is_parity: shard.is_parity,
                created_at_unix: part.created_at,
            });
        }
        parts.push(n1_indexdb::ObjectPart {
            part_index: part.part_index,
            size_bytes: part.size_bytes,
            created_at_unix: part.created_at,
            checksum: part.checksum.clone(),
            shards,
        });
    }

    let stripe_size_bytes = u64::try_from(metadata.stripe_size_bytes).map_err(|_| {
        AppError::InternalServerError(format!(
            "stripe size {} does not fit in u64",
            metadata.stripe_size_bytes
        ))
    })?;
    let version = n1_indexdb::ObjectVersion {
        object_id: object_id.to_string(),
        object_key: object_id.to_string(),
        version_id: metadata.version_id.clone(),
        upload_session_id: metadata.upload_session_id.clone(),
        owner_uid: metadata.owner_uid.clone(),
        content_type: metadata.content_type.clone(),
        total_size_bytes: metadata.total_size_bytes,
        created_at_unix: metadata.created_at,
        committed_at_unix: Some(changed_at),
        is_delete_marker: metadata.is_delete_marker,
        stripe_size_bytes,
        checksum: None,
        metadata_json: None,
        parts,
    };
    write_txn
        .n1_upsert_object_version(&version)
        .map_err(|e| map_indexdb_error("namespace object_version upsert", e))?;

    let state = if metadata.is_delete_marker {
        n1_indexdb::ObjectState::Deleted
    } else {
        n1_indexdb::ObjectState::Live
    };
    let generation = 1;
    write_txn
        .n1_set_object_version_state(&n1_indexdb::ObjectVersionState {
            object_id: object_id.to_string(),
            version_id: metadata.version_id.clone(),
            state,
            generation,
            updated_at_unix: changed_at,
        })
        .map_err(|e| map_indexdb_error("namespace object_version_state upsert", e))?;
    write_txn
        .n1_upsert_object_head(&n1_indexdb::ObjectHead {
            object_id: object_id.to_string(),
            object_key: object_id.to_string(),
            head_version_id: metadata.version_id.clone(),
            owner_uid: metadata.owner_uid.clone(),
            content_type: metadata.content_type.clone(),
            total_size_bytes: metadata.total_size_bytes,
            state,
            generation,
            created_at_unix: metadata.created_at,
            updated_at_unix: changed_at,
        })
        .map_err(|e| map_indexdb_error("namespace object_head upsert", e))?;
    Ok(())
}

fn root_namespace_entry(fragment: &str) -> NamespaceEntry {
    NamespaceEntry {
        fragment: fragment.to_string(),
        path: String::new(),
        parent_path: String::new(),
        name: String::new(),
        kind: NamespaceEntryKind::Directory,
        object_id: None,
        lifecycle: LifecycleState::Live,
        trash_entry_id: None,
        deleted_at_unix: None,
        created_at_unix: 0,
        updated_at_unix: 0,
    }
}

fn namespace_kind_from_indexdb(kind: n1_indexdb::NamespaceEntryKind) -> NamespaceEntryKind {
    match kind {
        n1_indexdb::NamespaceEntryKind::File => NamespaceEntryKind::File,
        n1_indexdb::NamespaceEntryKind::Directory => NamespaceEntryKind::Directory,
    }
}

fn pending_upload_from_indexdb(
    fragment: &str,
    binding: n1_indexdb::PendingUploadBinding,
) -> PendingUploadBinding {
    PendingUploadBinding {
        fragment: fragment.to_string(),
        logical_path: binding.logical_path,
        physical_object_id: binding.physical_object_id,
        version_id: binding.version_id,
        session_id: binding.session_id,
        created_at_unix: binding.created_at_unix,
    }
}

fn object_head_from_entry(
    fragment: &str,
    entry: &NamespaceEntry,
    head: n1_indexdb::ObjectHead,
) -> Result<ObjectHead, AppError> {
    Ok(ObjectHead {
        fragment: fragment.to_string(),
        object_id: entry
            .object_id
            .clone()
            .unwrap_or_else(|| head.object_id.clone()),
        current_version_id: head.head_version_id.clone(),
        storage_object_id: head.object_id,
        content_type: head.content_type,
        total_size_bytes: head.total_size_bytes,
        lifecycle: entry.lifecycle.clone(),
        created_at_unix: head.created_at_unix,
        updated_at_unix: head.updated_at_unix,
    })
}

fn object_head_from_head_row(fragment: &str, head: n1_indexdb::ObjectHead) -> ObjectHead {
    ObjectHead {
        fragment: fragment.to_string(),
        object_id: head.object_id.clone(),
        current_version_id: head.head_version_id.clone(),
        storage_object_id: head.object_id,
        content_type: head.content_type,
        total_size_bytes: head.total_size_bytes,
        lifecycle: lifecycle_from_object_state(head.state),
        created_at_unix: head.created_at_unix,
        updated_at_unix: head.updated_at_unix,
    }
}

fn lifecycle_from_object_state(state: n1_indexdb::ObjectState) -> LifecycleState {
    match state {
        n1_indexdb::ObjectState::Live => LifecycleState::Live,
        n1_indexdb::ObjectState::Trash | n1_indexdb::ObjectState::Deleted => {
            LifecycleState::Trashed { changed_at_unix: 0 }
        }
    }
}

fn object_metadata_from_namespace_summary(
    fragment: &str,
    summary: n1_indexdb::NamespaceObjectSummary,
) -> Result<ObjectMetadata, AppError> {
    Ok(ObjectMetadata {
        object_key: summary.path,
        fragment: fragment.to_string(),
        version_id: summary.version_id,
        upload_session_id: summary.upload_session_id,
        owner_uid: summary.owner_uid,
        content_type: summary.content_type,
        total_size_bytes: summary.total_size_bytes,
        created_at: summary.created_at_unix,
        parts: Vec::new(),
        is_delete_marker: summary.is_delete_marker,
        stripe_size_bytes: usize::try_from(summary.stripe_size_bytes).map_err(|_| {
            AppError::InternalServerError(format!(
                "stripe size {} does not fit in usize",
                summary.stripe_size_bytes
            ))
        })?,
    })
}

fn normalize_listing_prefix(value: &str) -> Result<String, AppError> {
    if value.trim().is_empty() {
        return Ok(String::new());
    }
    normalize_path(value).map_err(namespace_error_to_app)
}

fn common_prefix_for_entry(prefix: Option<&str>, path: &str) -> Option<String> {
    let suffix = match prefix {
        Some(prefix) if !prefix.is_empty() => {
            if path == prefix {
                ""
            } else if let Some(rest) = path.strip_prefix(prefix) {
                rest.strip_prefix('/').unwrap_or(rest)
            } else {
                path
            }
        }
        _ => path,
    };
    let slash = suffix.find('/')?;
    let base = prefix.filter(|prefix| !prefix.is_empty()).unwrap_or("");
    Some(if base.is_empty() {
        format!("{}/", &suffix[..slash])
    } else {
        format!("{}/{}/", base, &suffix[..slash])
    })
}

fn namespace_db_path(fragment: &str) -> Result<PathBuf, AppError> {
    let manager = ObjectPathManager;
    let roots = manager.get_all_index_roots();
    if roots.is_empty() {
        return Err(AppError::InternalServerError(
            "no index roots configured for namespace indexdb".to_string(),
        ));
    }
    let owner_idx = namespace_owner_index(fragment, roots.len());
    Ok(roots[owner_idx]
        .join(fragment)
        .join(INDEXDB_DIR)
        .join(INDEXDB_FILE))
}

fn namespace_db_paths(fragment: &str) -> Result<Vec<PathBuf>, AppError> {
    let manager = ObjectPathManager;
    let roots = manager.get_all_index_roots();
    if roots.is_empty() {
        return Err(AppError::InternalServerError(
            "no index roots configured for namespace indexdb".to_string(),
        ));
    }
    let owner_idx = namespace_owner_index(fragment, roots.len());
    let mut ordered = Vec::with_capacity(roots.len());
    ordered.push(
        roots[owner_idx]
            .join(fragment)
            .join(INDEXDB_DIR)
            .join(INDEXDB_FILE),
    );
    for (idx, root) in roots.into_iter().enumerate() {
        if idx == owner_idx {
            continue;
        }
        ordered.push(root.join(fragment).join(INDEXDB_DIR).join(INDEXDB_FILE));
    }
    Ok(ordered)
}

fn replicate_namespace_mutation_for_fragment(
    runtime: &NamespaceRuntime,
    fragment: &str,
    mutation: &NamespaceReplicaMutation,
) -> Result<(), AppError> {
    if mutation.is_empty() {
        return Ok(());
    }
    let paths = namespace_db_paths(fragment)?;
    if paths.len() <= 1 {
        return Ok(());
    }

    let owner_path = paths[0].clone();
    for replica_path in paths.into_iter().skip(1) {
        let apply_result = with_partition_commit_lock(&replica_path, || {
            if let Some(parent) = replica_path.parent() {
                fs::create_dir_all(parent)
                    .map_err(|err| AppError::Io(err, format!("mkdir {}", parent.display())))?;
            }
            let db = runtime.open_db_path(replica_path.clone())?;
            let write_txn = db
                .begin_write()
                .map_err(|err| map_indexdb_error("namespace replica begin_write", err))?;
            for node in &mutation.upsert_nodes {
                write_txn
                    .n1_upsert_namespace_node(node)
                    .map_err(|err| map_indexdb_error("namespace replica node upsert", err))?;
            }
            for move_node in &mutation.move_nodes {
                write_txn
                    .n1_move_namespace_node(
                        &move_node.node_id,
                        &move_node.parent_node_id,
                        &move_node.name,
                        move_node.changed_at_unix,
                    )
                    .map_err(|err| map_indexdb_error("namespace replica node move", err))?;
            }
            for root in &mutation.upsert_trash_roots {
                write_txn
                    .n1_upsert_namespace_trash_root(root)
                    .map_err(|err| map_indexdb_error("namespace replica trash root upsert", err))?;
            }
            for (trash_entry_id, state, changed_at_unix) in &mutation.set_trash_root_states {
                write_txn
                    .n1_set_namespace_trash_root_state(trash_entry_id, *state, *changed_at_unix)
                    .map_err(|err| map_indexdb_error("namespace replica trash root state", err))?;
            }
            for path in &mutation.delete_paths {
                write_txn
                    .n1_delete_namespace_entry(path)
                    .map_err(|err| map_indexdb_error("namespace replica delete", err))?;
            }
            for rename in &mutation.rename_subtrees {
                write_txn
                    .n1_rename_namespace_subtree(
                        &rename.source_path,
                        &rename.destination_path,
                        rename.changed_at_unix,
                    )
                    .map_err(|err| map_indexdb_error("namespace replica rename", err))?;
            }
            for lifecycle in &mutation.lifecycle_subtrees {
                write_txn
                    .n1_set_namespace_lifecycle_subtree(
                        &lifecycle.root_path,
                        lifecycle.from,
                        lifecycle.target,
                        lifecycle.changed_at_unix,
                        lifecycle.trash_entry_id.as_deref(),
                    )
                    .map_err(|err| map_indexdb_error("namespace replica lifecycle", err))?;
            }
            for purge in &mutation.purge_subtrees {
                write_txn
                    .n1_purge_namespace_subtree(
                        purge.root_path.as_deref(),
                        purge.trash_entry_id.as_deref(),
                    )
                    .map_err(|err| map_indexdb_error("namespace replica purge", err))?;
            }
            for (object_id, metadata, changed_at) in &mutation.upsert_object_metadata {
                upsert_namespace_object_metadata(&write_txn, object_id, metadata, *changed_at)?;
            }
            for entry in &mutation.upsert_entries {
                write_txn
                    .n1_upsert_namespace_entry(entry)
                    .map_err(|err| map_indexdb_error("namespace replica upsert", err))?;
            }
            for entry in &mutation.upsert_trash_entries {
                write_txn
                    .n1_upsert_trash_entry(entry)
                    .map_err(|err| map_indexdb_error("namespace replica trash upsert", err))?;
            }
            for (trash_entry_id, state, changed_at_unix) in &mutation.set_trash_states {
                write_txn
                    .n1_set_trash_entry_state(trash_entry_id, *state, *changed_at_unix)
                    .map_err(|err| map_indexdb_error("namespace replica trash state", err))?;
            }
            for binding in &mutation.upsert_pending_uploads {
                write_txn
                    .n1_upsert_pending_upload(binding)
                    .map_err(|err| map_indexdb_error("namespace replica pending upsert", err))?;
            }
            for version_id in &mutation.delete_pending_uploads {
                write_txn
                    .n1_delete_pending_upload(version_id)
                    .map_err(|err| map_indexdb_error("namespace replica pending delete", err))?;
            }
            if let Some(partition_meta) = &mutation.partition_meta {
                replay_indexdb_metadata_generation(&write_txn, partition_meta)?;
            }
            write_txn
                .commit()
                .map_err(|err| map_indexdb_error("namespace replica commit", err))
        });

        if let Err(err) = apply_result {
            warn!(
                "namespace replica logical apply failed for {}: {}; falling back to owner copy",
                replica_path.display(),
                err
            );
            runtime.dbs.remove(&replica_path);
            runtime.db_touch_ticks.remove(&replica_path);
            if let Err(copy_err) = copy_namespace_db_file(&owner_path, &replica_path) {
                warn!(
                    "namespace replica catch-up failed for {} -> {}: {}",
                    owner_path.display(),
                    replica_path.display(),
                    copy_err
                );
            }
            runtime.dbs.remove(&replica_path);
            runtime.db_touch_ticks.remove(&replica_path);
        }
    }

    Ok(())
}

fn best_existing_namespace_db_path(fragment: &str) -> Result<Option<PathBuf>, AppError> {
    let infos = namespace_db_paths(fragment)?
        .into_iter()
        .filter_map(|path| match inspect_namespace_db_file(&path) {
            Ok(Some(info)) => Some(Ok(info)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        })
        .collect::<Result<Vec<_>, AppError>>()?;
    let mut hash_counts: HashMap<[u8; 32], usize> = HashMap::new();
    for info in &infos {
        *hash_counts.entry(info.metadata_hash).or_default() += 1;
    }
    let Some(canonical_hash) = choose_canonical_metadata_hash(&hash_counts) else {
        if infos.is_empty() {
            return Ok(None);
        }
        return Err(AppError::InternalServerError(
            "ambiguous namespace indexdb replicas; no semantic majority exists".to_string(),
        ));
    };

    let mut best: Option<&NamespaceReplicaFileInfo> = None;
    for info in infos
        .iter()
        .filter(|info| info.metadata_hash == canonical_hash)
    {
        let replace = match best.as_ref() {
            Some(current) => compare_namespace_replica_info(info, current) == Ordering::Greater,
            None => true,
        };
        if replace {
            best = Some(info);
        }
    }
    Ok(best.map(|info| info.path.clone()))
}

fn choose_canonical_metadata_hash(hash_counts: &HashMap<[u8; 32], usize>) -> Option<[u8; 32]> {
    let mut counts = hash_counts
        .iter()
        .map(|(hash, count)| (*hash, *count))
        .collect::<Vec<_>>();
    counts.sort_by(|(left_hash, left_count), (right_hash, right_count)| {
        left_count
            .cmp(right_count)
            .then_with(|| left_hash.cmp(right_hash))
    });
    let (best_hash, best_count) = *counts.last()?;
    let tied = counts
        .iter()
        .rev()
        .skip(1)
        .any(|(_, count)| *count == best_count);
    if tied { None } else { Some(best_hash) }
}

fn inspect_namespace_db_file(path: &Path) -> Result<Option<NamespaceReplicaFileInfo>, AppError> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(AppError::Io(err, format!("stat {}", path.display()))),
    };
    let modified_at = metadata
        .modified()
        .map_err(|err| AppError::Io(err, format!("mtime {}", path.display())))?;
    let db = open_n1_indexdb(path)?;
    let metadata_hash = namespace_indexdb_semantic_hash(&db)?;
    Ok(Some(NamespaceReplicaFileInfo {
        path: path.to_path_buf(),
        modified_at,
        len: metadata.len(),
        metadata_hash,
    }))
}

fn namespace_indexdb_semantic_hash(db: &Database) -> Result<[u8; 32], AppError> {
    let mut hasher = Sha256::new();
    let mut after = None;
    loop {
        let nodes = db
            .n1_list_namespace_nodes(after.as_deref(), 1000)
            .map_err(|e| map_indexdb_error("namespace node semantic hash scan", e))?;
        if nodes.is_empty() {
            break;
        }
        after = nodes.last().map(|node| node.node_id.clone());
        for node in nodes {
            hash_field(&mut hasher, "namespace-node");
            hash_field(&mut hasher, &node.node_id);
            hash_field(&mut hasher, node.parent_node_id.as_deref().unwrap_or(""));
            hash_field(&mut hasher, &node.name);
            hash_field(&mut hasher, namespace_kind_for_hash(node.kind));
            hash_field(&mut hasher, node.object_id.as_deref().unwrap_or(""));
            hash_field(&mut hasher, node.head_version_id.as_deref().unwrap_or(""));
            hash_field(&mut hasher, &node.created_at_unix.to_string());
            hash_field(&mut hasher, &node.updated_at_unix.to_string());
        }
    }

    let mut after = None;
    loop {
        let roots = db
            .n1_list_namespace_trash_roots_all(after.as_deref(), 1000)
            .map_err(|e| map_indexdb_error("namespace trash semantic hash scan", e))?;
        if roots.is_empty() {
            break;
        }
        after = roots.last().map(|root| root.trash_entry_id.clone());
        for root in roots {
            hash_field(&mut hasher, "namespace-trash-root");
            hash_field(&mut hasher, &root.trash_entry_id);
            hash_field(&mut hasher, &root.root_node_id);
            hash_field(&mut hasher, &root.original_parent_node_id);
            hash_field(&mut hasher, &root.original_name);
            hash_field(&mut hasher, &root.original_path);
            hash_field(&mut hasher, namespace_kind_for_hash(root.kind));
            hash_field(&mut hasher, root.object_id.as_deref().unwrap_or(""));
            hash_field(&mut hasher, root.head_version_id.as_deref().unwrap_or(""));
            hash_field(&mut hasher, &root.deleted_at_unix.to_string());
            hash_field(
                &mut hasher,
                &root
                    .retired_at_unix
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
            );
            hash_field(
                &mut hasher,
                &root
                    .purged_at_unix
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
            );
            hash_field(&mut hasher, trash_state_for_hash(root.state));
        }
    }
    Ok(hasher.finalize().into())
}

fn namespace_kind_for_hash(kind: n1_indexdb::NamespaceEntryKind) -> &'static str {
    match kind {
        n1_indexdb::NamespaceEntryKind::File => "file",
        n1_indexdb::NamespaceEntryKind::Directory => "directory",
    }
}

fn trash_state_for_hash(state: n1_indexdb::TrashEntryState) -> &'static str {
    match state {
        n1_indexdb::TrashEntryState::Trashed => "trashed",
        n1_indexdb::TrashEntryState::Retired => "retired",
        n1_indexdb::TrashEntryState::Purged => "purged",
    }
}

fn hash_field(hasher: &mut Sha256, value: &str) {
    hasher.update(value.as_bytes());
    hasher.update([0]);
}

fn compare_namespace_replica_info(
    candidate: &NamespaceReplicaFileInfo,
    current: &NamespaceReplicaFileInfo,
) -> Ordering {
    candidate
        .modified_at
        .cmp(&current.modified_at)
        .then_with(|| candidate.len.cmp(&current.len))
        .then_with(|| candidate.metadata_hash.cmp(&current.metadata_hash))
        .then_with(|| candidate.path.cmp(&current.path))
}

fn copy_namespace_db_file(source: &Path, destination: &Path) -> Result<(), AppError> {
    if source == destination {
        return Ok(());
    }
    let Some(parent) = destination.parent() else {
        return Err(AppError::InternalServerError(format!(
            "namespace destination '{}' has no parent",
            destination.display()
        )));
    };
    fs::create_dir_all(parent)
        .map_err(|err| AppError::Io(err, format!("mkdir {}", parent.display())))?;
    let temp_path = destination.with_extension(format!(
        "{}.tmp-{}",
        destination
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or("ndxdb"),
        Ulid::new()
    ));
    let source_db = open_n1_indexdb(source)?;
    remove_indexdb_file_set(&temp_path)?;
    source_db
        .backup_to_path(&temp_path)
        .map_err(|err| map_indexdb_error("namespace replica indexdb backup", err))?;
    remove_indexdb_file_set(destination)?;
    fs::rename(&temp_path, destination).map_err(|err| {
        let _ = remove_indexdb_file_set(&temp_path);
        AppError::Io(
            err,
            format!(
                "rename namespace replica {} -> {}",
                temp_path.display(),
                destination.display()
            ),
        )
    })?;
    remove_indexdb_file_set(&temp_path)?;
    Ok(())
}

fn remove_indexdb_file_set(path: &Path) -> Result<(), AppError> {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(INDEXDB_FILE);
    for candidate in [
        path.to_path_buf(),
        path.with_file_name(format!("{file_name}-wal")),
        path.with_file_name(format!("{file_name}-shm")),
    ] {
        match fs::remove_file(&candidate) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(AppError::Io(err, format!("remove {}", candidate.display()))),
        }
    }
    Ok(())
}

fn namespace_owner_index(fragment: &str, root_count: usize) -> usize {
    if root_count <= 1 {
        return 0;
    }
    let mut hasher = Sha256::new();
    for part in ["fragment", fragment] {
        hasher.update(part.as_bytes());
        hasher.update([0u8]);
    }
    let hash = hasher.finalize();
    usize::from(u16::from(hash[0]) << 8 | u16::from(hash[1])) % root_count
}

fn namespace_error_to_app(error: NamespaceError) -> AppError {
    match error {
        NamespaceError::InvalidPath(message) => AppError::BadRequest(message),
        NamespaceError::Conflict(message) => AppError::Conflict(message),
        NamespaceError::NotFound(_) => AppError::NotFound,
        NamespaceError::Invariant(message) => AppError::InternalServerError(message),
    }
}

fn map_indexdb_error(context: &str, error: impl std::fmt::Display) -> AppError {
    AppError::InternalServerError(format!("{context}: {error}"))
}

pub fn logical_not_found() -> AppError {
    AppError::NotFound
}

pub fn unexpected_response(status: StatusCode, context: &str) -> AppError {
    AppError::InternalServerError(format!("{context} returned unexpected status {status}"))
}

#[cfg(test)]
mod tests {
    use super::namespace_owner_index;
    use sha2::{Digest, Sha256};

    fn indexdb_fragment_owner_index(fragment: &str, root_count: usize) -> usize {
        if root_count <= 1 {
            return 0;
        }
        let mut hasher = Sha256::new();
        for part in ["fragment", fragment] {
            hasher.update(part.as_bytes());
            hasher.update([0u8]);
        }
        let hash = hasher.finalize();
        usize::from(u16::from(hash[0]) << 8 | u16::from(hash[1])) % root_count
    }

    #[test]
    fn namespace_owner_hash_matches_indexdb_owner_hash_for_live_fragment() {
        let fragment = "0d6e4079e36703ebd37c00722f5891d28b0e2811dc114b129215123adcce3605";
        assert_eq!(
            namespace_owner_index(fragment, 2),
            indexdb_fragment_owner_index(fragment, 2)
        );
    }
}
