use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
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
use tracing::{debug, warn};
use ulid::Ulid;

use crate::core::disk_manager::{ListObjectsResult, ObjectMetadata, ObjectPathManager};
use crate::core::fragment_metadata::fragment_index_partition_count;
use crate::core::indexdb_store::{
    PartitionMeta, next_indexdb_metadata_generation, open_n1_indexdb, read_live_version,
    read_partition_meta_exact, replay_indexdb_metadata_generation, with_partition_commit_lock,
};
use crate::enums::app_error_enum::AppError;
use crate::namespace::models::{
    LifecycleState, NamespaceEntry, NamespaceEntryKind, NamespaceError, NamespaceMutationSummary,
    NamespaceReachabilitySnapshot, ObjectHead,
};
use crate::namespace::paths::{base_name, is_same_or_descendant, normalize_path, parent_path};

const INDEXDB_DIR: &str = "indexdb";
const INDEXDB_FILE: &str = "index.ndxdb";
const INDEXDB_SHARD_FILE_PREFIX: &str = "shard_";
const NAMESPACE_ROOT_NODE_ID: &str = "root";
const DIR_MARKER_NAME: &str = ".n1fsd_dir";
const TRASH_VIRTUAL_NAME: &str = ".n1-trash";

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
    verified_owner_shards: DashMap<PathBuf, ()>,
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
    generation: u64,
    metadata_hash: [u8; 32],
}

#[derive(Debug, Clone, Default)]
struct NamespaceReplicaMutation {
    partition_meta: Option<PartitionMeta>,
    upsert_nodes: Vec<n1_indexdb::NamespaceNode>,
    delete_nodes: Vec<String>,
    move_nodes: Vec<NamespaceNodeMoveReplay>,
    upsert_trash_roots: Vec<n1_indexdb::NamespaceTrashRoot>,
    set_trash_root_states: Vec<(String, n1_indexdb::TrashEntryState, i64)>,
    upsert_file_heads: Vec<n1_indexdb::NamespaceFileHead>,
}

#[derive(Debug, Clone)]
struct NamespaceNodeMoveReplay {
    node_id: String,
    parent_node_id: String,
    name: String,
    changed_at_unix: i64,
}

#[derive(Debug, Clone)]
struct ShardedNamespaceMove {
    moved_node: n1_indexdb::NamespaceNode,
    old_shard: u16,
    new_shard: u16,
    old_partition_meta: PartitionMeta,
    new_partition_meta: Option<PartitionMeta>,
    file_head: Option<n1_indexdb::NamespaceFileHead>,
}

impl NamespaceReplicaMutation {
    fn is_empty(&self) -> bool {
        self.upsert_nodes.is_empty()
            && self.delete_nodes.is_empty()
            && self.move_nodes.is_empty()
            && self.upsert_trash_roots.is_empty()
            && self.set_trash_root_states.is_empty()
            && self.upsert_file_heads.is_empty()
    }
}

fn bump_namespace_shard_generation(
    write_txn: &indexdb::WriteTransaction,
    changed_at_unix: i64,
) -> Result<PartitionMeta, AppError> {
    next_indexdb_metadata_generation(write_txn, changed_at_unix)
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
            verified_owner_shards: DashMap::new(),
        }
    }

    pub fn evict_fragment(&self, fragment: &str) {
        let manager = ObjectPathManager;
        for root in manager.get_all_index_roots() {
            let dir = root.join(fragment).join(INDEXDB_DIR);
            let Ok(entries) = fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                let name = path
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or("");
                if name.starts_with(INDEXDB_SHARD_FILE_PREFIX) && name.ends_with(".ndxdb") {
                    self.checkpoint_cached_db(&path);
                    self.dbs.remove(&path);
                    self.db_touch_ticks.remove(&path);
                    self.verified_owner_shards.remove(&path);
                }
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
            self.checkpoint_cached_db(&path);
            self.dbs.remove(&path);
            self.db_touch_ticks.remove(&path);
        }
    }

    fn checkpoint_cached_db(&self, path: &Path) {
        let Some(db) = self.dbs.get(path).map(|entry| entry.value().clone()) else {
            return;
        };
        if let Err(err) = db.checkpoint() {
            let message = err.to_string();
            if message.contains("database table is locked")
                || message.contains("database is locked")
            {
                debug!(
                    "namespace shard checkpoint skipped for {} before cache eviction: {}",
                    path.display(),
                    err
                );
            } else {
                warn!(
                    "namespace shard checkpoint failed for {} before cache eviction: {}",
                    path.display(),
                    err
                );
            }
        }
    }

    pub async fn list_dir(
        &self,
        fragment: &str,
        path: &str,
        include_trashed: bool,
    ) -> Result<Vec<NamespaceEntry>, AppError> {
        let parent = normalize_path(path).map_err(namespace_error_to_app)?;
        let fragment = fragment.to_string();
        run_namespace_blocking(move || {
            if include_trashed {
                let (entries, _) =
                    sharded_list_trash_dir_page(&fragment, &parent, usize::MAX / 2, None, 1000)?;
                return Ok(entries);
            }
            let Some((resolved_path, parent_node)) = sharded_resolve_live_node(&fragment, &parent)?
            else {
                return Ok(Vec::new());
            };
            if parent_node.kind != n1_indexdb::NamespaceEntryKind::Directory {
                return Ok(Vec::new());
            }
            let mut out = Vec::new();
            let mut after = None;
            loop {
                let nodes = sharded_list_namespace_children(
                    &fragment,
                    &parent_node.node_id,
                    after.as_deref(),
                    1000,
                )?;
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
        let fragment = fragment.to_string();
        let fetch_limit = limit.clamp(1, 1000).saturating_add(1);
        run_namespace_blocking(move || {
            if include_trashed {
                return sharded_list_trash_dir_page(
                    &fragment,
                    &parent,
                    limit,
                    after_name.as_deref(),
                    fetch_limit,
                );
            }
            let Some((resolved_path, parent_node)) = sharded_resolve_live_node(&fragment, &parent)?
            else {
                return Ok((Vec::new(), None));
            };
            if parent_node.kind != n1_indexdb::NamespaceEntryKind::Directory {
                return Ok((Vec::new(), None));
            }
            let rows = sharded_list_namespace_children(
                &fragment,
                &parent_node.node_id,
                after_name.as_deref(),
                fetch_limit,
            )?;
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
        let fragment = fragment.to_string();
        run_namespace_blocking(move || {
            Ok(
                sharded_resolve_live_node(&fragment, &normalized)?.map(|(path, node)| {
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
        let fragment = fragment.to_string();
        run_namespace_blocking(move || sharded_trashed_entry(&fragment, &normalized)).await
    }

    pub async fn head_for_path(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<Option<ObjectHead>, AppError> {
        let normalized = normalize_path(path).map_err(namespace_error_to_app)?;
        let fragment = fragment.to_string();
        run_namespace_blocking(move || {
            let Some((path, node)) = sharded_resolve_live_node(&fragment, &normalized)? else {
                return Ok(None);
            };
            let Some(object_id) = node.object_id.as_deref() else {
                return Ok(None);
            };
            let Some(head) = sharded_file_head_for_node(&fragment, &node)? else {
                return Ok(None);
            };
            if head.object_id != object_id {
                return Ok(None);
            }
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
        let fragment = fragment.to_string();
        run_namespace_blocking(move || {
            let mut out = HashMap::new();
            let shard_count = namespace_shard_count(&fragment)?;
            for shard_id in 0..shard_count {
                let Some(db) = open_existing_namespace_shard_db(&fragment, shard_id)? else {
                    continue;
                };
                let mut after = None;
                loop {
                    let nodes = db
                        .n1_list_namespace_nodes(after.as_deref(), 1000)
                        .map_err(|e| map_indexdb_error("namespace head node scan", e))?;
                    if nodes.is_empty() {
                        break;
                    }
                    after = nodes.last().map(|node| node.node_id.clone());
                    for node in nodes {
                        let Some(object_id) = node.object_id.clone() else {
                            continue;
                        };
                        if !ids.contains(&object_id) || out.contains_key(&object_id) {
                            continue;
                        }
                        let Some(head) = sharded_file_head_for_node(&fragment, &node)? else {
                            continue;
                        };
                        let entry = node_entry_from_indexdb(
                            &fragment,
                            node.name.clone(),
                            node,
                            LifecycleState::Live,
                            None,
                            None,
                        );
                        out.insert(object_id, object_head_from_entry(&fragment, &entry, head)?);
                    }
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
        let fragment_owned = fragment.to_string();
        let fragment_for_write = fragment.to_string();
        let changed_at = Utc::now().timestamp();
        let (created_count, touched_shards) = run_namespace_blocking(move || {
            let mut touched_shards = BTreeSet::new();
            let created = sharded_ensure_directory_path_nodes(
                &fragment_for_write,
                &normalized,
                changed_at,
                &mut touched_shards,
            )?;
            let mut mutations = BTreeMap::new();
            add_created_namespace_nodes_to_mutations(&fragment_for_write, created, &mut mutations)?;
            Ok((
                mutations
                    .values()
                    .map(|m| m.upsert_nodes.len())
                    .sum::<usize>(),
                touched_shards,
            ))
        })
        .await?;
        let created = created_count > 0;
        if created {
            replicate_namespace_shards_by_copy(&fragment_owned, &touched_shards);
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
        let fragment_owned = fragment.to_string();
        let fragment_for_write = fragment.to_string();
        let changed_at = Utc::now().timestamp();
        let (summary, mutations) = run_namespace_blocking(move || {
            let mut touched_shards = BTreeSet::new();
            let mut mutations = BTreeMap::new();
            let Some((_, source_node)) =
                sharded_resolve_live_node_home_or_fallback(&fragment_for_write, &source)?
            else {
                return Err(AppError::NotFound);
            };
            let destination_parent_path = parent_path(&destination);
            let destination_name = base_name(&destination);
            let Some((_, destination_parent_node)) = sharded_resolve_live_node_home_or_fallback(
                &fragment_for_write,
                &destination_parent_path,
            )?
            else {
                return Err(AppError::NotFound);
            };
            if destination_parent_node.kind != n1_indexdb::NamespaceEntryKind::Directory {
                return Err(AppError::Conflict(format!(
                    "destination parent '{}' is not a directory",
                    destination_parent_path
                )));
            }
            if sharded_get_namespace_child_home_or_fallback(
                &fragment_for_write,
                &destination_parent_node.node_id,
                &destination_name,
            )?
            .is_some()
            {
                return Err(AppError::Conflict(format!(
                    "destination path '{}' already exists",
                    destination
                )));
            }
            if source_node.kind == n1_indexdb::NamespaceEntryKind::Directory {
                let mut cursor = Some(destination_parent_node.node_id.clone());
                while let Some(current_id) = cursor {
                    if current_id == source_node.node_id {
                        return Err(AppError::Conflict(
                            "destination cannot be inside the source subtree".to_string(),
                        ));
                    }
                    cursor = sharded_get_namespace_node(&fragment_for_write, &current_id)?
                        .and_then(|node| node.parent_node_id);
                }
            }
            let effective_changed_at =
                changed_at.max(source_node.updated_at_unix.saturating_add(1));
            let move_result = sharded_move_namespace_node(
                &fragment_for_write,
                &source_node,
                &destination_parent_node.node_id,
                &destination_name,
                effective_changed_at,
                &mut touched_shards,
            )?;
            add_namespace_move_to_mutations(move_result, &mut mutations);
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
            Ok((summary, mutations))
        })
        .await?;
        replicate_namespace_shard_mutations(&fragment_owned, &mutations);
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

    pub async fn recover_paths_bulk(
        &self,
        fragment: &str,
        paths: Vec<String>,
    ) -> Result<Vec<(String, Result<usize, AppError>)>, AppError> {
        let mut normalized_paths = Vec::with_capacity(paths.len());
        for path in paths {
            match normalize_path(&path).map_err(namespace_error_to_app) {
                Ok(normalized) if !normalized.is_empty() => {
                    normalized_paths.push((normalized, Ok(())));
                }
                Ok(_) => normalized_paths.push((path, Err(AppError::NotFound))),
                Err(err) => normalized_paths.push((path, Err(err))),
            }
        }

        let fragment_owned = fragment.to_string();
        let fragment_for_write = fragment.to_string();
        let changed_at = Utc::now().timestamp();
        let (results, mutations) = run_namespace_blocking(move || {
            let roots = sharded_list_trash_roots(
                &fragment_for_write,
                Some(n1_indexdb::TrashEntryState::Trashed),
            )?;
            let mut roots_by_path: HashMap<String, Vec<n1_indexdb::NamespaceTrashRoot>> =
                HashMap::new();
            for root in roots {
                roots_by_path
                    .entry(root.original_path.clone())
                    .or_default()
                    .push(root);
            }

            let mut touched_shards = BTreeSet::new();
            let mut mutations = BTreeMap::new();
            let mut results = Vec::with_capacity(normalized_paths.len());
            for (path, normalized_result) in normalized_paths {
                if let Err(err) = normalized_result {
                    results.push((path, Err(err)));
                    continue;
                }
                let roots = roots_by_path.remove(&path).unwrap_or_default();
                if roots.is_empty() {
                    results.push((path, Err(AppError::NotFound)));
                    continue;
                }
                if roots.len() > 1 {
                    results.push((
                        path.clone(),
                        Err(AppError::Conflict(format!(
                            "trash path '{}' is ambiguous; use trash_entry_id",
                            path
                        ))),
                    ));
                    continue;
                }
                match sharded_recover_trash_root(
                    &fragment_for_write,
                    roots.into_iter().next().expect("checked non-empty"),
                    changed_at,
                    &mut touched_shards,
                    &mut mutations,
                ) {
                    Ok(summary) => results.push((
                        path,
                        Ok(summary.affected_objects.max(summary.affected_paths)),
                    )),
                    Err(err) => results.push((path, Err(err))),
                }
            }
            Ok((results, mutations))
        })
        .await?;
        replicate_namespace_shard_mutations(&fragment_owned, &mutations);
        Ok(results)
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
        let fragment = fragment.to_string();
        run_namespace_blocking(move || {
            let mut snapshot = NamespaceReachabilitySnapshot::default();
            let mut saw_namespace_rows = false;
            let live_rows = sharded_list_subtree_nodes(
                &fragment,
                NAMESPACE_ROOT_NODE_ID,
                "",
                None,
                usize::MAX / 2,
            )?;
            if !live_rows.is_empty() {
                saw_namespace_rows = true;
            }
            for (_, node) in live_rows {
                if let Some(object_id) = node.object_id {
                    snapshot.referenced_object_ids.insert(object_id);
                }
                if let Some(version_id) = node.head_version_id {
                    snapshot.referenced_version_ids.insert(version_id);
                }
            }
            for root in
                sharded_list_trash_roots(&fragment, Some(n1_indexdb::TrashEntryState::Trashed))?
            {
                saw_namespace_rows = true;
                if let Some(object_id) = root.object_id.clone() {
                    snapshot.referenced_object_ids.insert(object_id);
                }
                if let Some(version_id) = root.head_version_id.clone() {
                    snapshot.referenced_version_ids.insert(version_id);
                }
                for (_, node) in sharded_list_subtree_nodes(
                    &fragment,
                    &root.root_node_id,
                    &root.original_path,
                    None,
                    usize::MAX / 2,
                )? {
                    if let Some(object_id) = node.object_id {
                        snapshot.referenced_object_ids.insert(object_id);
                    }
                    if let Some(version_id) = node.head_version_id {
                        snapshot.referenced_version_ids.insert(version_id);
                    }
                }
            }
            for binding in sharded_list_pending_uploads_raw(&fragment)? {
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
        let fragment = fragment.to_string();
        run_namespace_blocking(move || {
            let mut object_ids = BTreeSet::new();
            for root in
                sharded_list_trash_roots(&fragment, Some(n1_indexdb::TrashEntryState::Purged))?
            {
                if let Some(object_id) = root.object_id {
                    object_ids.insert(object_id);
                }
                for (_, node) in sharded_list_subtree_nodes(
                    &fragment,
                    &root.root_node_id,
                    &root.original_path,
                    None,
                    usize::MAX / 2,
                )? {
                    if let Some(object_id) = node.object_id {
                        object_ids.insert(object_id);
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
        let fragment = fragment.to_string();
        run_namespace_blocking(move || {
            Ok(sharded_resolve_live_node(&fragment, &normalized)?
                .and_then(|(_, node)| node.object_id))
        })
        .await
    }

    pub async fn resolve_any_object_id(
        &self,
        fragment: &str,
        logical_path: &str,
    ) -> Result<Option<String>, AppError> {
        let normalized = normalize_path(logical_path).map_err(namespace_error_to_app)?;
        let fragment = fragment.to_string();
        run_namespace_blocking(move || {
            if let Some((_, node)) = sharded_resolve_live_node(&fragment, &normalized)? {
                return Ok(node.object_id);
            }
            Ok(sharded_trashed_entry(&fragment, &normalized)?.and_then(|entry| entry.object_id))
        })
        .await
    }

    pub async fn select_object_id_for_write(
        &self,
        fragment: &str,
        logical_path: &str,
    ) -> Result<String, AppError> {
        let normalized = normalize_path(logical_path).map_err(namespace_error_to_app)?;
        let fragment = fragment.to_string();
        run_namespace_blocking(move || {
            let parent = parent_path(&normalized);
            let name = base_name(&normalized);
            let existing = if normalized.is_empty() {
                sharded_resolve_live_node(&fragment, &normalized)?.map(|(_, node)| node)
            } else if let Some((_, parent_node)) =
                sharded_resolve_live_node_home(&fragment, &parent)?
            {
                sharded_get_namespace_child_home(&fragment, &parent_node.node_id, &name)?
            } else {
                None
            };
            if let Some(existing) = existing {
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
        let fragment_owned = fragment.to_string();
        let fragment_for_write = fragment.to_string();
        let object_id = physical_object_id.to_string();
        let version_id = metadata.version_id.clone();
        let metadata = metadata.clone();
        let changed_at = metadata.created_at.max(Utc::now().timestamp());
        let mutations = run_namespace_blocking(move || {
            let mut touched_shards = BTreeSet::new();
            let created = sharded_ensure_directory_path_nodes(
                &fragment_for_write,
                &parent_path(&path),
                changed_at,
                &mut touched_shards,
            )?;
            let mut mutations = BTreeMap::new();
            add_created_namespace_nodes_to_mutations(&fragment_for_write, created, &mut mutations)?;
            let parent_path = parent_path(&path);
            let Some((_, parent_node)) =
                sharded_resolve_live_node_home(&fragment_for_write, &parent_path)?
            else {
                return Err(AppError::NotFound);
            };
            let name = base_name(&path);
            let stripe_size_bytes = u64::try_from(metadata.stripe_size_bytes).map_err(|_| {
                AppError::InternalServerError(format!(
                    "stripe size {} does not fit in u64",
                    metadata.stripe_size_bytes
                ))
            })?;
            let shard_id =
                namespace_node_shard_id(&fragment_for_write, Some(&parent_node.node_id), &name)?;
            let db = open_namespace_owner_shard_db(&fragment_for_write, shard_id)?;
            let shard_path = namespace_shard_paths(&fragment_for_write, shard_id)?[0].clone();
            let mut replica_node = None;
            let mut replica_head = None;
            let mut replica_partition_meta = None;
            with_partition_commit_lock(&shard_path, || {
                let existing = db
                    .n1_get_namespace_child(&parent_node.node_id, &name)
                    .map_err(|e| map_indexdb_error("namespace finalize child get", e))?;
                if existing
                    .as_ref()
                    .is_some_and(|entry| entry.kind == n1_indexdb::NamespaceEntryKind::Directory)
                {
                    return Err(AppError::Conflict(format!(
                        "logical path '{}' already exists as a directory",
                        path
                    )));
                }
                if let Some(existing_file) = existing.as_ref() {
                    if let Some(current_head) = db
                        .n1_get_namespace_file_head(&existing_file.node_id)
                        .map_err(|e| map_indexdb_error("namespace finalize current head get", e))?
                    {
                        let current_is_newer = current_head.created_at_unix > metadata.created_at
                            || (current_head.created_at_unix == metadata.created_at
                                && current_head.version_id > version_id);
                        if current_is_newer {
                            return Ok(());
                        }
                    }
                }
                let write_txn = db
                    .begin_write()
                    .map_err(|e| map_indexdb_error("namespace finalize begin_write", e))?;
                let created_at = existing
                    .as_ref()
                    .map(|node| node.created_at_unix)
                    .unwrap_or(changed_at);
                let node = n1_indexdb::NamespaceNode {
                    node_id: existing
                        .as_ref()
                        .map(|node| node.node_id.clone())
                        .unwrap_or_else(new_namespace_node_id),
                    parent_node_id: Some(parent_node.node_id.clone()),
                    name: name.clone(),
                    kind: n1_indexdb::NamespaceEntryKind::File,
                    object_id: Some(object_id.clone()),
                    head_version_id: Some(version_id.clone()),
                    created_at_unix: created_at,
                    updated_at_unix: changed_at,
                };
                let file_head = n1_indexdb::NamespaceFileHead {
                    node_id: node.node_id.clone(),
                    object_id: object_id.clone(),
                    version_id: version_id.clone(),
                    upload_session_id: metadata.upload_session_id.clone(),
                    owner_uid: metadata.owner_uid.clone(),
                    content_type: metadata.content_type.clone(),
                    total_size_bytes: metadata.total_size_bytes,
                    created_at_unix: metadata.created_at,
                    is_delete_marker: metadata.is_delete_marker,
                    stripe_size_bytes,
                    updated_at_unix: changed_at,
                };
                write_txn
                    .n1_upsert_namespace_node(&node)
                    .map_err(|e| map_indexdb_error("namespace finalize node upsert", e))?;
                write_txn
                    .n1_upsert_namespace_file_head(&file_head)
                    .map_err(|e| map_indexdb_error("namespace file head upsert", e))?;
                replica_partition_meta =
                    Some(bump_namespace_shard_generation(&write_txn, changed_at)?);
                write_txn
                    .commit()
                    .map_err(|e| map_indexdb_error("namespace finalize commit", e))?;
                replica_node = Some(node);
                replica_head = Some(file_head);
                Ok(())
            })?;
            let mutation = mutations.entry(shard_id).or_default();
            mutation.partition_meta = replica_partition_meta;
            if let Some(node) = replica_node {
                mutation.upsert_nodes.push(node);
            }
            if let Some(file_head) = replica_head {
                mutation.upsert_file_heads.push(file_head);
            }
            Ok(mutations)
        })
        .await?;
        replicate_namespace_shard_mutations(&fragment_owned, &mutations);
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
        let fragment_owned = fragment.to_string();
        let fragment_for_write = fragment.to_string();
        let shard_id = namespace_pending_shard_id(fragment, version_id)?;
        let touched_shards = run_namespace_blocking(move || {
            let db = open_namespace_owner_shard_db(&fragment_for_write, shard_id)?;
            let owner_path = namespace_shard_paths(&fragment_for_write, shard_id)?[0].clone();
            with_partition_commit_lock(&owner_path, || {
                let write_txn = db
                    .begin_write()
                    .map_err(|e| map_indexdb_error("namespace pending begin_write", e))?;
                write_txn
                    .n1_upsert_pending_upload(&binding)
                    .map_err(|e| map_indexdb_error("namespace pending upsert", e))?;
                bump_namespace_shard_generation(&write_txn, binding.created_at_unix)?;
                write_txn
                    .commit()
                    .map_err(|e| map_indexdb_error("namespace pending commit", e))?;
                let mut touched = BTreeSet::new();
                touched.insert(shard_id);
                Ok(touched)
            })
        })
        .await?;
        replicate_namespace_shards_by_copy(&fragment_owned, &touched_shards);
        Ok(())
    }

    pub async fn pending_upload(
        &self,
        fragment: &str,
        version_id: &str,
    ) -> Result<Option<PendingUploadBinding>, AppError> {
        let fragment = fragment.to_string();
        let version_id = version_id.to_string();
        run_namespace_blocking(move || {
            let shard_id = namespace_pending_shard_id(&fragment, &version_id)?;
            let Some(db) = open_existing_namespace_shard_db(&fragment, shard_id)? else {
                return Ok(None);
            };
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
        let fragment = fragment.to_string();
        run_namespace_blocking(move || {
            let mut rows = Vec::new();
            let shard_count = namespace_shard_count(&fragment)?;
            for shard_id in 0..shard_count {
                let Some(db) = open_existing_namespace_shard_db(&fragment, shard_id)? else {
                    continue;
                };
                rows.extend(
                    db.n1_list_pending_uploads()
                        .map_err(|e| map_indexdb_error("namespace pending list", e))?,
                );
            }
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
        let fragment = fragment.to_string();
        run_namespace_blocking(move || {
            let shard_count = namespace_shard_count(&fragment)?;
            for shard_id in 0..shard_count {
                let Some(db) = open_existing_namespace_shard_db(&fragment, shard_id)? else {
                    continue;
                };
                if let Some(binding) = db
                    .n1_find_pending_upload_by_logical_path(&normalized)
                    .map_err(|e| map_indexdb_error("namespace pending path get", e))?
                {
                    return Ok(Some(pending_upload_from_indexdb(&fragment, binding)));
                }
            }
            Ok(None)
        })
        .await
    }

    pub async fn clear_pending_upload(
        &self,
        fragment: &str,
        version_id: &str,
    ) -> Result<(), AppError> {
        let fragment_owned = fragment.to_string();
        let fragment_for_write = fragment.to_string();
        let version_id = version_id.to_string();
        let shard_id = namespace_pending_shard_id(fragment, &version_id)?;
        let touched_shards = run_namespace_blocking(move || {
            let db = open_namespace_owner_shard_db(&fragment_for_write, shard_id)?;
            let owner_path = namespace_shard_paths(&fragment_for_write, shard_id)?[0].clone();
            with_partition_commit_lock(&owner_path, || {
                let write_txn = db
                    .begin_write()
                    .map_err(|e| map_indexdb_error("namespace pending clear begin_write", e))?;
                write_txn
                    .n1_delete_pending_upload(&version_id)
                    .map_err(|e| map_indexdb_error("namespace pending clear", e))?;
                bump_namespace_shard_generation(&write_txn, Utc::now().timestamp())?;
                write_txn
                    .commit()
                    .map_err(|e| map_indexdb_error("namespace pending clear commit", e))?;
                let mut touched = BTreeSet::new();
                touched.insert(shard_id);
                Ok(touched)
            })
        })
        .await?;
        replicate_namespace_shards_by_copy(&fragment_owned, &touched_shards);
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
        let include_internal = params
            .get("include_internal")
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false);
        let after = params.get("continuation_token").cloned();
        let fragment = fragment.to_string();
        run_namespace_blocking(move || {
            if !delimiter_is_slash {
                let fetch_limit = limit.saturating_add(1);
                let mut rows = if scan_trash {
                    sharded_list_trash_object_summaries(
                        &fragment,
                        prefix.as_deref(),
                        after.as_deref(),
                        fetch_limit,
                        include_internal,
                    )?
                } else {
                    sharded_list_live_object_summaries(
                        &fragment,
                        prefix.as_deref(),
                        after.as_deref(),
                        fetch_limit,
                        include_internal,
                    )?
                };
                let next_cursor = if rows.len() > limit {
                    rows.pop();
                    rows.last().map(|row| row.path.clone())
                } else {
                    None
                };
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
                    sharded_list_trash_object_summaries(
                        &fragment,
                        prefix.as_deref(),
                        cursor.as_deref(),
                        256,
                        include_internal,
                    )?
                } else {
                    sharded_list_live_object_summaries(
                        &fragment,
                        prefix.as_deref(),
                        cursor.as_deref(),
                        256,
                        include_internal,
                    )?
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

    pub async fn list_live_object_refs_page(
        &self,
        fragment: &str,
        prefix: Option<&str>,
        continuation_token: Option<&str>,
        limit: usize,
    ) -> Result<NamespaceObjectRefPage, AppError> {
        let prefix = prefix.map(str::to_string);
        let continuation_token = continuation_token.map(str::to_string);
        let fragment = fragment.to_string();
        let limit = limit.clamp(1, 1000);
        run_namespace_blocking(move || {
            let fetch_limit = limit.saturating_add(1);
            let mut rows = sharded_list_live_object_refs(
                &fragment,
                prefix.as_deref(),
                continuation_token.as_deref(),
                fetch_limit,
            )?;
            let next_cursor = if rows.len() > limit {
                rows.pop();
                rows.last().map(|row| row.path.clone())
            } else {
                None
            };
            Ok(NamespaceObjectRefPage {
                entries: rows,
                continuation_token: next_cursor,
            })
        })
        .await
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
        let fragment_owned = fragment.to_string();
        let fragment_for_write = fragment.to_string();
        let changed_at = Utc::now().timestamp();
        let (summary, mutations) = run_namespace_blocking(move || {
            let mut touched_shards = BTreeSet::new();
            let mut mutations = BTreeMap::new();
            if matches!(target, n1_indexdb::NamespaceLifecycle::Trashed) {
                let Some((source_path, source_node)) =
                    sharded_resolve_live_node_home_or_fallback(&fragment_for_write, &normalized)?
                else {
                    return Err(AppError::NotFound);
                };
                let Some(original_parent_node_id) = source_node.parent_node_id.clone() else {
                    return Err(AppError::BadRequest(
                        "root lifecycle cannot be changed".to_string(),
                    ));
                };
                let effective_changed_at =
                    changed_at.max(source_node.updated_at_unix.saturating_add(1));
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
                    deleted_at_unix: effective_changed_at,
                    retired_at_unix: None,
                    purged_at_unix: None,
                    state: n1_indexdb::TrashEntryState::Trashed,
                };
                let trash_partition_meta = sharded_upsert_trash_root(
                    &fragment_for_write,
                    &trash_root,
                    &mut touched_shards,
                )?;
                let trash_shard = namespace_trash_shard_id(&fragment_for_write, &trash_entry_id)?;
                let trash_mutation = mutations.entry(trash_shard).or_default();
                set_namespace_mutation_partition_meta(trash_mutation, trash_partition_meta);
                trash_mutation.upsert_trash_roots.push(trash_root);
                let trash_parent = trash_parent_node_id(&trash_entry_id);
                let move_result = sharded_move_namespace_node(
                    &fragment_for_write,
                    &source_node,
                    &trash_parent,
                    &source_node.name,
                    effective_changed_at,
                    &mut touched_shards,
                )?;
                add_namespace_move_to_mutations(move_result, &mut mutations);
                return Ok((
                    NamespaceMutationSummary {
                        operation: "trash".to_string(),
                        kind: namespace_kind_from_indexdb(source_node.kind),
                        affected_paths: 1,
                        affected_objects: usize::from(source_node.object_id.is_some()),
                        source_path: Some(source_path),
                        destination_path: None,
                        trash_entry_id: Some(trash_entry_id),
                        deleted_at_unix: Some(effective_changed_at),
                    },
                    mutations,
                ));
            }

            if matches!(target, n1_indexdb::NamespaceLifecycle::Live) {
                let roots = sharded_list_trash_roots(
                    &fragment_for_write,
                    Some(n1_indexdb::TrashEntryState::Trashed),
                )?
                .into_iter()
                .filter(|root| root.original_path == normalized)
                .collect::<Vec<_>>();
                if roots.len() > 1 {
                    return Err(AppError::Conflict(format!(
                        "trash path '{}' is ambiguous; use trash_entry_id",
                        normalized
                    )));
                }
                let Some(root) = roots.into_iter().next() else {
                    return Err(AppError::NotFound);
                };
                let summary = sharded_recover_trash_root(
                    &fragment_for_write,
                    root,
                    changed_at,
                    &mut touched_shards,
                    &mut mutations,
                )?;
                return Ok((summary, mutations));
            }

            Err(AppError::BadRequest(
                "unsupported namespace lifecycle transition".to_string(),
            ))
        })
        .await?;
        replicate_namespace_shard_mutations(&fragment_owned, &mutations);
        Ok(summary)
    }

    async fn set_lifecycle_for_trash_entry_id(
        &self,
        fragment: &str,
        trash_entry_id: &str,
        target: n1_indexdb::NamespaceLifecycle,
    ) -> Result<NamespaceMutationSummary, AppError> {
        let fragment_owned = fragment.to_string();
        let fragment_for_write = fragment.to_string();
        let trash_entry_id = trash_entry_id.to_string();
        let changed_at = Utc::now().timestamp();
        let (summary, mutations) = run_namespace_blocking(move || {
            if !matches!(target, n1_indexdb::NamespaceLifecycle::Live) {
                return Err(AppError::BadRequest(
                    "trash_entry_id lifecycle supports recover only".to_string(),
                ));
            }
            let Some(root) = sharded_get_trash_root(&fragment_for_write, &trash_entry_id)? else {
                return Err(AppError::NotFound);
            };
            if root.state != n1_indexdb::TrashEntryState::Trashed {
                return Err(AppError::NotFound);
            }
            let mut touched_shards = BTreeSet::new();
            let mut mutations = BTreeMap::new();
            let summary = sharded_recover_trash_root(
                &fragment_for_write,
                root,
                changed_at,
                &mut touched_shards,
                &mut mutations,
            )?;
            Ok((summary, mutations))
        })
        .await?;
        replicate_namespace_shard_mutations(&fragment_owned, &mutations);
        Ok(summary)
    }

    async fn set_lifecycle_for_all_trash(
        &self,
        fragment: &str,
        target: n1_indexdb::NamespaceLifecycle,
    ) -> Result<NamespaceMutationSummary, AppError> {
        let fragment_owned = fragment.to_string();
        let fragment_for_write = fragment.to_string();
        let changed_at = Utc::now().timestamp();
        let (summary, mutations) = run_namespace_blocking(move || {
            if !matches!(target, n1_indexdb::NamespaceLifecycle::Live) {
                return Err(AppError::BadRequest(
                    "recover_all supports live target only".to_string(),
                ));
            }
            let roots = sharded_list_trash_roots(
                &fragment_for_write,
                Some(n1_indexdb::TrashEntryState::Trashed),
            )?;
            let mut touched_shards = BTreeSet::new();
            let mut mutations = BTreeMap::new();
            let mut affected_objects = 0usize;
            let mut affected_paths = 0usize;
            for root in roots {
                let summary = sharded_recover_trash_root(
                    &fragment_for_write,
                    root,
                    changed_at,
                    &mut touched_shards,
                    &mut mutations,
                )?;
                affected_paths += summary.affected_paths;
                affected_objects += summary.affected_objects;
            }
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
                mutations,
            ))
        })
        .await?;
        replicate_namespace_shard_mutations(&fragment_owned, &mutations);
        Ok(summary)
    }

    async fn purge_path(
        &self,
        fragment: &str,
        path: &str,
        trash_entry_id: Option<String>,
    ) -> Result<NamespaceMutationSummary, AppError> {
        let normalized = normalize_path(path).map_err(namespace_error_to_app)?;
        let fragment_owned = fragment.to_string();
        let fragment_for_write = fragment.to_string();
        let (summary, mutations) = run_namespace_blocking(move || {
            let root = if let Some(trash_entry_id) = trash_entry_id.as_deref() {
                sharded_get_trash_root(&fragment_for_write, trash_entry_id)?
            } else {
                let roots = sharded_list_trash_roots(
                    &fragment_for_write,
                    Some(n1_indexdb::TrashEntryState::Trashed),
                )?
                .into_iter()
                .filter(|root| root.original_path == normalized)
                .collect::<Vec<_>>();
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
            let mut touched_shards = BTreeSet::new();
            let partition_meta = sharded_set_trash_root_state(
                &fragment_for_write,
                &root.trash_entry_id,
                n1_indexdb::TrashEntryState::Purged,
                changed_at,
                &mut touched_shards,
            )?;
            let mut mutations = BTreeMap::new();
            let trash_shard = namespace_trash_shard_id(&fragment_for_write, &root.trash_entry_id)?;
            let mutation = mutations.entry(trash_shard).or_default();
            set_namespace_mutation_partition_meta(mutation, partition_meta);
            mutation.set_trash_root_states.push((
                root.trash_entry_id.clone(),
                n1_indexdb::TrashEntryState::Purged,
                changed_at,
            ));
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
                mutations,
            ))
        })
        .await?;
        replicate_namespace_shard_mutations(&fragment_owned, &mutations);
        Ok(summary)
    }

    async fn purge_all_trash(&self, fragment: &str) -> Result<NamespaceMutationSummary, AppError> {
        let fragment_owned = fragment.to_string();
        let fragment_for_write = fragment.to_string();
        let (summary, mutations) = run_namespace_blocking(move || {
            let roots = sharded_list_trash_roots(
                &fragment_for_write,
                Some(n1_indexdb::TrashEntryState::Trashed),
            )?;
            if roots.is_empty() {
                return Err(AppError::NotFound);
            }
            let changed_at = Utc::now().timestamp();
            let mut touched_shards = BTreeSet::new();
            let mut mutations = BTreeMap::new();
            let mut affected_objects = 0usize;
            for root in &roots {
                let partition_meta = sharded_set_trash_root_state(
                    &fragment_for_write,
                    &root.trash_entry_id,
                    n1_indexdb::TrashEntryState::Purged,
                    changed_at,
                    &mut touched_shards,
                )?;
                let trash_shard =
                    namespace_trash_shard_id(&fragment_for_write, &root.trash_entry_id)?;
                let mutation = mutations.entry(trash_shard).or_default();
                set_namespace_mutation_partition_meta(mutation, partition_meta);
                mutation.set_trash_root_states.push((
                    root.trash_entry_id.clone(),
                    n1_indexdb::TrashEntryState::Purged,
                    changed_at,
                ));
                affected_objects += usize::from(root.object_id.is_some());
            }
            Ok((
                NamespaceMutationSummary {
                    operation: "purge_trash".to_string(),
                    kind: NamespaceEntryKind::Directory,
                    affected_paths: roots.len(),
                    affected_objects,
                    source_path: None,
                    destination_path: None,
                    trash_entry_id: None,
                    deleted_at_unix: None,
                },
                mutations,
            ))
        })
        .await?;
        replicate_namespace_shard_mutations(&fragment_owned, &mutations);
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

fn sharded_get_namespace_child(
    fragment: &str,
    parent_node_id: &str,
    name: &str,
) -> Result<Option<n1_indexdb::NamespaceNode>, AppError> {
    let home_shard_id = namespace_node_shard_id(fragment, Some(parent_node_id), name)?;
    if let Some(db) = open_existing_namespace_shard_db(fragment, home_shard_id)? {
        if let Some(node) = db
            .n1_get_namespace_child(parent_node_id, name)
            .map_err(|e| map_indexdb_error("sharded namespace child home get", e))?
        {
            return Ok(Some(node));
        }
    }

    let shard_count = namespace_shard_count(fragment)?;
    let mut best: Option<(n1_indexdb::NamespaceNode, bool)> = None;
    for shard_id in 0..shard_count {
        if shard_id == home_shard_id {
            continue;
        }
        for (db, is_owner_copy) in open_all_existing_namespace_shard_dbs(fragment, shard_id)? {
            let Some(node) = db
                .n1_get_namespace_child(parent_node_id, name)
                .map_err(|e| map_indexdb_error("sharded namespace child fallback get", e))?
            else {
                continue;
            };
            let preferred_copy = is_owner_copy;
            match best.as_mut() {
                Some((existing, existing_preferred)) => {
                    if node.updated_at_unix > existing.updated_at_unix
                        || (node.updated_at_unix == existing.updated_at_unix
                            && preferred_copy
                            && !*existing_preferred)
                    {
                        *existing = node;
                        *existing_preferred = preferred_copy;
                    }
                }
                None => best = Some((node, preferred_copy)),
            }
        }
    }
    Ok(best.map(|(node, _)| node))
}

fn sharded_get_namespace_child_home(
    fragment: &str,
    parent_node_id: &str,
    name: &str,
) -> Result<Option<n1_indexdb::NamespaceNode>, AppError> {
    let home_shard_id = namespace_node_shard_id(fragment, Some(parent_node_id), name)?;
    if let Some(db) = open_existing_namespace_shard_db(fragment, home_shard_id)? {
        if let Some(node) = db
            .n1_get_namespace_child(parent_node_id, name)
            .map_err(|e| map_indexdb_error("sharded namespace child get", e))?
        {
            return Ok(Some(node));
        }
    }
    Ok(None)
}

fn sharded_get_namespace_node(
    fragment: &str,
    node_id: &str,
) -> Result<Option<n1_indexdb::NamespaceNode>, AppError> {
    if node_id == NAMESPACE_ROOT_NODE_ID {
        return Ok(Some(root_namespace_node(0)));
    }
    let shard_count = namespace_shard_count(fragment)?;
    let mut best: Option<n1_indexdb::NamespaceNode> = None;
    for shard_id in 0..shard_count {
        for (db, _) in open_all_existing_namespace_shard_dbs(fragment, shard_id)? {
            if let Some(node) = db
                .n1_get_namespace_node(node_id)
                .map_err(|e| map_indexdb_error("sharded namespace node get", e))?
            {
                if best
                    .as_ref()
                    .map(|existing| node.updated_at_unix > existing.updated_at_unix)
                    .unwrap_or(true)
                {
                    best = Some(node);
                }
            }
        }
    }
    Ok(best)
}

fn sharded_resolve_live_node(
    fragment: &str,
    path: &str,
) -> Result<Option<(String, n1_indexdb::NamespaceNode)>, AppError> {
    if path.is_empty() {
        return Ok(Some((String::new(), root_namespace_node(0))));
    }
    let mut current = root_namespace_node(0);
    let mut current_path = String::new();
    for segment in path.split('/').filter(|segment| !segment.is_empty()) {
        let Some(child) = sharded_get_namespace_child(fragment, &current.node_id, segment)? else {
            return Ok(None);
        };
        current_path = join_path(&current_path, segment);
        current = child;
    }
    Ok(Some((current_path, current)))
}

fn sharded_resolve_live_node_home(
    fragment: &str,
    path: &str,
) -> Result<Option<(String, n1_indexdb::NamespaceNode)>, AppError> {
    if path.is_empty() {
        return Ok(Some((String::new(), root_namespace_node(0))));
    }
    let mut current = root_namespace_node(0);
    let mut current_path = String::new();
    for segment in path.split('/').filter(|segment| !segment.is_empty()) {
        let Some(child) = sharded_get_namespace_child_home(fragment, &current.node_id, segment)?
        else {
            return Ok(None);
        };
        current_path = join_path(&current_path, segment);
        current = child;
    }
    Ok(Some((current_path, current)))
}

fn sharded_resolve_live_node_home_or_fallback(
    fragment: &str,
    path: &str,
) -> Result<Option<(String, n1_indexdb::NamespaceNode)>, AppError> {
    match sharded_resolve_live_node_home(fragment, path)? {
        Some(node) => Ok(Some(node)),
        None => sharded_resolve_live_node(fragment, path),
    }
}

fn sharded_get_namespace_child_home_or_fallback(
    fragment: &str,
    parent_node_id: &str,
    name: &str,
) -> Result<Option<n1_indexdb::NamespaceNode>, AppError> {
    match sharded_get_namespace_child_home(fragment, parent_node_id, name)? {
        Some(node) => Ok(Some(node)),
        None => sharded_get_namespace_child(fragment, parent_node_id, name),
    }
}

fn sharded_get_current_namespace_child_home(
    fragment: &str,
    parent_node_id: &str,
    name: &str,
) -> Result<Option<n1_indexdb::NamespaceNode>, AppError> {
    let Some(node) = sharded_get_namespace_child_home(fragment, parent_node_id, name)? else {
        return Ok(None);
    };
    let Some(current) = sharded_get_namespace_node(fragment, &node.node_id)? else {
        return Ok(None);
    };
    if current.parent_node_id.as_deref() == Some(parent_node_id) && current.name == name {
        Ok(Some(current))
    } else {
        Ok(None)
    }
}

fn sharded_list_namespace_children(
    fragment: &str,
    parent_node_id: &str,
    after_name: Option<&str>,
    limit: usize,
) -> Result<Vec<n1_indexdb::NamespaceNode>, AppError> {
    let shard_count = namespace_shard_count(fragment)?;
    let mut nodes_by_id: HashMap<String, (n1_indexdb::NamespaceNode, bool)> = HashMap::new();
    for shard_id in 0..shard_count {
        for (db, is_owner_copy) in open_all_existing_namespace_shard_dbs(fragment, shard_id)? {
            for node in db
                .n1_list_namespace_node_children(parent_node_id, after_name, limit)
                .map_err(|e| map_indexdb_error("sharded namespace child list", e))?
            {
                let preferred_copy =
                    namespace_node_shard_id(fragment, node.parent_node_id.as_deref(), &node.name)
                        .map(|expected| expected == shard_id && is_owner_copy)
                        .unwrap_or(false);
                match nodes_by_id.get_mut(&node.node_id) {
                    Some((existing, existing_preferred)) => {
                        if node.updated_at_unix > existing.updated_at_unix
                            || (node.updated_at_unix == existing.updated_at_unix
                                && preferred_copy
                                && !*existing_preferred)
                        {
                            *existing = node;
                            *existing_preferred = preferred_copy;
                        }
                    }
                    None => {
                        nodes_by_id.insert(node.node_id.clone(), (node, preferred_copy));
                    }
                }
            }
        }
    }
    let mut nodes = nodes_by_id
        .into_values()
        .map(|(node, _)| node)
        .collect::<Vec<_>>();
    nodes.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then(left.node_id.cmp(&right.node_id))
    });
    nodes.truncate(limit);
    Ok(nodes)
}

struct NamespaceListingSnapshot {
    children_by_parent: HashMap<String, Vec<n1_indexdb::NamespaceNode>>,
    file_heads_by_node: HashMap<String, n1_indexdb::NamespaceFileHead>,
}

fn sharded_namespace_listing_snapshot(
    fragment: &str,
) -> Result<NamespaceListingSnapshot, AppError> {
    let shard_count = namespace_shard_count(fragment)?;
    let mut nodes_by_id: HashMap<String, (n1_indexdb::NamespaceNode, bool)> = HashMap::new();
    let mut file_heads_by_node: HashMap<String, n1_indexdb::NamespaceFileHead> = HashMap::new();

    for shard_id in 0..shard_count {
        for (db, is_owner_copy) in open_all_existing_namespace_shard_dbs(fragment, shard_id)? {
            let mut after_node_id = None;
            loop {
                let page = db
                    .n1_list_namespace_nodes(after_node_id.as_deref(), 1000)
                    .map_err(|e| map_indexdb_error("sharded namespace node snapshot", e))?;
                if page.is_empty() {
                    break;
                }
                after_node_id = page.last().map(|node| node.node_id.clone());
                for node in page {
                    let preferred_copy = namespace_node_shard_id(
                        fragment,
                        node.parent_node_id.as_deref(),
                        &node.name,
                    )
                    .map(|expected| expected == shard_id && is_owner_copy)
                    .unwrap_or(false);
                    match nodes_by_id.get_mut(&node.node_id) {
                        Some((existing, existing_preferred)) => {
                            if node.updated_at_unix > existing.updated_at_unix
                                || (node.updated_at_unix == existing.updated_at_unix
                                    && preferred_copy
                                    && !*existing_preferred)
                            {
                                *existing = node;
                                *existing_preferred = preferred_copy;
                            }
                        }
                        None => {
                            nodes_by_id.insert(node.node_id.clone(), (node, preferred_copy));
                        }
                    }
                }
            }

            let mut after_head_node_id = None;
            loop {
                let page = db
                    .n1_list_namespace_file_heads(after_head_node_id.as_deref(), 1000)
                    .map_err(|e| map_indexdb_error("sharded namespace head snapshot", e))?;
                if page.is_empty() {
                    break;
                }
                after_head_node_id = page.last().map(|head| head.node_id.clone());
                for head in page {
                    match file_heads_by_node.get_mut(&head.node_id) {
                        Some(existing) => {
                            if head.updated_at_unix > existing.updated_at_unix
                                || (head.updated_at_unix == existing.updated_at_unix
                                    && head.version_id > existing.version_id)
                            {
                                *existing = head;
                            }
                        }
                        None => {
                            file_heads_by_node.insert(head.node_id.clone(), head);
                        }
                    }
                }
            }
        }
    }

    let mut children_by_parent: HashMap<String, Vec<n1_indexdb::NamespaceNode>> = HashMap::new();
    for (node, _) in nodes_by_id.into_values() {
        let Some(parent) = node.parent_node_id.clone() else {
            continue;
        };
        children_by_parent.entry(parent).or_default().push(node);
    }
    for children in children_by_parent.values_mut() {
        children.sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then(left.node_id.cmp(&right.node_id))
        });
    }

    Ok(NamespaceListingSnapshot {
        children_by_parent,
        file_heads_by_node,
    })
}

fn sharded_get_or_create_directory_child(
    fragment: &str,
    parent_node: &n1_indexdb::NamespaceNode,
    name: &str,
    changed_at: i64,
    touched_shards: &mut BTreeSet<u16>,
) -> Result<(n1_indexdb::NamespaceNode, bool, Option<PartitionMeta>), AppError> {
    let shard_id = namespace_node_shard_id(fragment, Some(&parent_node.node_id), name)?;
    if let Some(db) = open_existing_namespace_shard_db(fragment, shard_id)? {
        if let Some(existing) = db
            .n1_get_namespace_child(&parent_node.node_id, name)
            .map_err(|e| map_indexdb_error("sharded namespace mkdir child read", e))?
        {
            if existing.kind == n1_indexdb::NamespaceEntryKind::File {
                return Err(AppError::Conflict(format!(
                    "path '{}' already exists as a file",
                    name
                )));
            }
            return Ok((existing, false, None));
        }
    }

    let db = open_namespace_owner_shard_db(fragment, shard_id)?;
    let path = namespace_shard_paths(fragment, shard_id)?[0].clone();
    let mut created = false;
    let mut partition_meta = None;
    let node = with_partition_commit_lock(&path, || {
        let write_txn = db
            .begin_write()
            .map_err(|e| map_indexdb_error("sharded namespace mkdir begin_write", e))?;
        if let Some(existing) = write_txn
            .n1_get_namespace_child(&parent_node.node_id, name)
            .map_err(|e| map_indexdb_error("sharded namespace mkdir child get", e))?
        {
            if existing.kind == n1_indexdb::NamespaceEntryKind::File {
                return Err(AppError::Conflict(format!(
                    "path '{}' already exists as a file",
                    name
                )));
            }
            write_txn
                .commit()
                .map_err(|e| map_indexdb_error("sharded namespace mkdir existing commit", e))?;
            return Ok(existing);
        }

        let node = n1_indexdb::NamespaceNode {
            node_id: new_namespace_node_id(),
            parent_node_id: Some(parent_node.node_id.clone()),
            name: name.to_string(),
            kind: n1_indexdb::NamespaceEntryKind::Directory,
            object_id: None,
            head_version_id: None,
            created_at_unix: changed_at,
            updated_at_unix: changed_at,
        };
        write_txn
            .n1_upsert_namespace_node(&node)
            .map_err(|e| map_indexdb_error("sharded namespace mkdir node upsert", e))?;
        partition_meta = Some(bump_namespace_shard_generation(&write_txn, changed_at)?);
        write_txn
            .commit()
            .map_err(|e| map_indexdb_error("sharded namespace mkdir commit", e))?;
        created = true;
        Ok(node)
    })?;
    if created {
        touched_shards.insert(shard_id);
    }
    Ok((node, created, partition_meta))
}

fn sharded_move_namespace_node(
    fragment: &str,
    node: &n1_indexdb::NamespaceNode,
    parent_node_id: &str,
    name: &str,
    changed_at: i64,
    touched_shards: &mut BTreeSet<u16>,
) -> Result<ShardedNamespaceMove, AppError> {
    let old_shard = namespace_node_shard_id(fragment, node.parent_node_id.as_deref(), &node.name)?;
    let new_shard = namespace_node_shard_id(fragment, Some(parent_node_id), name)?;
    let mut moved = node.clone();
    moved.parent_node_id = Some(parent_node_id.to_string());
    moved.name = name.to_string();
    moved.updated_at_unix = changed_at;
    if old_shard == new_shard {
        let db = open_namespace_owner_shard_db(fragment, old_shard)?;
        let path = namespace_shard_paths(fragment, old_shard)?[0].clone();
        let old_partition_meta = with_partition_commit_lock(&path, || {
            let write_txn = db
                .begin_write()
                .map_err(|e| map_indexdb_error("sharded namespace move begin_write", e))?;
            write_txn
                .n1_move_namespace_node(&node.node_id, parent_node_id, name, changed_at)
                .map_err(|e| map_indexdb_error("sharded namespace move", e))?;
            let partition_meta = bump_namespace_shard_generation(&write_txn, changed_at)?;
            write_txn
                .commit()
                .map_err(|e| map_indexdb_error("sharded namespace move commit", e))?;
            Ok(partition_meta)
        })?;
        touched_shards.insert(old_shard);
        return Ok(ShardedNamespaceMove {
            moved_node: moved,
            old_shard,
            new_shard,
            old_partition_meta,
            new_partition_meta: None,
            file_head: None,
        });
    } else {
        let file_head = sharded_file_head_for_node(fragment, node)?;
        let old_db = open_namespace_owner_shard_db(fragment, old_shard)?;
        let old_path = namespace_shard_paths(fragment, old_shard)?[0].clone();
        let old_partition_meta = with_partition_commit_lock(&old_path, || {
            let write_txn = old_db
                .begin_write()
                .map_err(|e| map_indexdb_error("sharded namespace delete begin_write", e))?;
            write_txn
                .n1_delete_namespace_node(&node.node_id)
                .map_err(|e| map_indexdb_error("sharded namespace old node delete", e))?;
            let partition_meta = bump_namespace_shard_generation(&write_txn, changed_at)?;
            write_txn
                .commit()
                .map_err(|e| map_indexdb_error("sharded namespace delete commit", e))?;
            Ok(partition_meta)
        })?;
        let new_db = open_namespace_owner_shard_db(fragment, new_shard)?;
        let new_path = namespace_shard_paths(fragment, new_shard)?[0].clone();
        let new_partition_meta = with_partition_commit_lock(&new_path, || {
            let write_txn = new_db
                .begin_write()
                .map_err(|e| map_indexdb_error("sharded namespace reinsert begin_write", e))?;
            write_txn
                .n1_upsert_namespace_node(&moved)
                .map_err(|e| map_indexdb_error("sharded namespace reinsert node", e))?;
            if let Some(head) = &file_head {
                write_txn
                    .n1_upsert_namespace_file_head(head)
                    .map_err(|e| map_indexdb_error("sharded namespace reinsert file head", e))?;
            }
            let partition_meta = bump_namespace_shard_generation(&write_txn, changed_at)?;
            write_txn
                .commit()
                .map_err(|e| map_indexdb_error("sharded namespace reinsert commit", e))?;
            Ok(partition_meta)
        })?;
        touched_shards.insert(old_shard);
        touched_shards.insert(new_shard);
        return Ok(ShardedNamespaceMove {
            moved_node: moved,
            old_shard,
            new_shard,
            old_partition_meta,
            new_partition_meta: Some(new_partition_meta),
            file_head,
        });
    }
}

fn sharded_ensure_directory_path_nodes(
    fragment: &str,
    path: &str,
    changed_at: i64,
    touched_shards: &mut BTreeSet<u16>,
) -> Result<Vec<(n1_indexdb::NamespaceNode, PartitionMeta)>, AppError> {
    let mut created = Vec::new();
    if path.is_empty() {
        return Ok(created);
    }
    let mut current = root_namespace_node(changed_at);
    for segment in path.split('/').filter(|segment| !segment.is_empty()) {
        let (node, was_created, partition_meta) = sharded_get_or_create_directory_child(
            fragment,
            &current,
            segment,
            changed_at,
            touched_shards,
        )?;
        if was_created && let Some(partition_meta) = partition_meta {
            created.push((node.clone(), partition_meta));
        }
        current = node;
    }
    Ok(created)
}

fn sharded_file_head_for_node(
    fragment: &str,
    node: &n1_indexdb::NamespaceNode,
) -> Result<Option<n1_indexdb::NamespaceFileHead>, AppError> {
    let shard_id = namespace_node_shard_id(fragment, node.parent_node_id.as_deref(), &node.name)?;
    let Some(db) = open_existing_namespace_shard_db(fragment, shard_id)? else {
        return Ok(None);
    };
    db.n1_get_namespace_file_head(&node.node_id)
        .map_err(|e| map_indexdb_error("sharded namespace file head get", e))
}

fn namespace_summary_from_node_head(
    path: String,
    head: n1_indexdb::NamespaceFileHead,
) -> n1_indexdb::NamespaceObjectSummary {
    n1_indexdb::NamespaceObjectSummary {
        path,
        version_id: head.version_id,
        upload_session_id: head.upload_session_id,
        owner_uid: head.owner_uid,
        content_type: head.content_type,
        total_size_bytes: head.total_size_bytes,
        created_at_unix: head.created_at_unix,
        is_delete_marker: head.is_delete_marker,
        stripe_size_bytes: head.stripe_size_bytes,
    }
}

fn namespace_summary_from_metadata(
    path: String,
    metadata: ObjectMetadata,
) -> Result<n1_indexdb::NamespaceObjectSummary, AppError> {
    let stripe_size_bytes = u64::try_from(metadata.stripe_size_bytes).map_err(|_| {
        AppError::InternalServerError(format!(
            "stripe size {} does not fit in u64",
            metadata.stripe_size_bytes
        ))
    })?;
    Ok(n1_indexdb::NamespaceObjectSummary {
        path,
        version_id: metadata.version_id,
        upload_session_id: metadata.upload_session_id,
        owner_uid: metadata.owner_uid,
        content_type: metadata.content_type,
        total_size_bytes: metadata.total_size_bytes,
        created_at_unix: metadata.created_at,
        is_delete_marker: metadata.is_delete_marker,
        stripe_size_bytes,
    })
}

fn sharded_list_live_object_summaries(
    fragment: &str,
    prefix: Option<&str>,
    after_path: Option<&str>,
    limit: usize,
    include_internal: bool,
) -> Result<Vec<n1_indexdb::NamespaceObjectSummary>, AppError> {
    let snapshot = sharded_namespace_listing_snapshot(fragment)?;
    let mut rows = BTreeMap::new();
    let mut stack = vec![(String::new(), root_namespace_node(0))];
    while let Some((base_path, node)) = stack.pop() {
        if node.kind == n1_indexdb::NamespaceEntryKind::File {
            let path = base_path;
            if let Some(prefix) = prefix {
                if !path.starts_with(prefix) {
                    continue;
                }
            }
            if !include_internal && is_internal_namespace_path(&path) {
                continue;
            }
            if let Some(after) = after_path {
                if path.as_str() <= after {
                    continue;
                }
            }
            let node_object_id = node.object_id.as_deref().unwrap_or_default();
            let node_version_id = node.head_version_id.as_deref().unwrap_or_default();
            let summary = match snapshot.file_heads_by_node.get(&node.node_id) {
                Some(head)
                    if head.object_id == node_object_id && head.version_id == node_version_id =>
                {
                    Some(namespace_summary_from_node_head(path.clone(), head.clone()))
                }
                _ if !node_object_id.is_empty() && !node_version_id.is_empty() => {
                    read_live_version(fragment, node_object_id, node_version_id)?
                        .map(|metadata| namespace_summary_from_metadata(path.clone(), metadata))
                        .transpose()?
                }
                _ => None,
            };
            if let Some(summary) = summary {
                rows.entry(path)
                    .and_modify(|existing: &mut n1_indexdb::NamespaceObjectSummary| {
                        if summary.created_at_unix > existing.created_at_unix
                            || (summary.created_at_unix == existing.created_at_unix
                                && summary.version_id > existing.version_id)
                        {
                            *existing = summary.clone();
                        }
                    })
                    .or_insert(summary);
            }
            continue;
        }
        if let Some(prefix) = prefix {
            let is_possible_prefix_parent = base_path.is_empty()
                || prefix.starts_with(&format!("{}/", base_path))
                || base_path.starts_with(prefix);
            if !is_possible_prefix_parent {
                continue;
            }
        }
        let Some(children) = snapshot.children_by_parent.get(&node.node_id) else {
            continue;
        };
        let mut children = children.clone();
        children.sort_by(|left, right| right.name.cmp(&left.name));
        for child in children {
            let child_path = join_path(&base_path, &child.name);
            stack.push((child_path, child));
        }
    }
    Ok(rows.into_values().take(limit).collect())
}

fn sharded_list_live_object_refs(
    fragment: &str,
    prefix: Option<&str>,
    after_path: Option<&str>,
    limit: usize,
) -> Result<Vec<NamespaceObjectRef>, AppError> {
    let snapshot = sharded_namespace_listing_snapshot(fragment)?;
    let mut rows = BTreeMap::new();
    let mut stack = vec![(String::new(), root_namespace_node(0))];
    while let Some((base_path, node)) = stack.pop() {
        if node.kind == n1_indexdb::NamespaceEntryKind::File {
            let path = base_path;
            if let Some(prefix) = prefix {
                if !path.starts_with(prefix) {
                    continue;
                }
            }
            if let Some(after) = after_path {
                if path.as_str() <= after {
                    continue;
                }
            }
            if let (Some(object_id), Some(version_id)) =
                (node.object_id.clone(), node.head_version_id.clone())
            {
                rows.entry(path.clone())
                    .and_modify(|existing: &mut NamespaceObjectRef| {
                        if version_id > existing.version_id {
                            existing.object_id = object_id.clone();
                            existing.version_id = version_id.clone();
                        }
                    })
                    .or_insert(NamespaceObjectRef {
                        path,
                        object_id,
                        version_id,
                    });
            }
            continue;
        }
        if let Some(prefix) = prefix {
            let is_possible_prefix_parent = base_path.is_empty()
                || prefix.starts_with(&format!("{}/", base_path))
                || base_path.starts_with(prefix);
            if !is_possible_prefix_parent {
                continue;
            }
        }
        let Some(children) = snapshot.children_by_parent.get(&node.node_id) else {
            continue;
        };
        let mut children = children.clone();
        children.sort_by(|left, right| right.name.cmp(&left.name));
        for child in children {
            let child_path = join_path(&base_path, &child.name);
            stack.push((child_path, child));
        }
    }
    Ok(rows.into_values().take(limit).collect())
}

fn sharded_list_all_namespace_children(
    fragment: &str,
    parent_node_id: &str,
) -> Result<Vec<n1_indexdb::NamespaceNode>, AppError> {
    let mut out = Vec::new();
    let mut after = None;
    loop {
        let page =
            sharded_list_namespace_children(fragment, parent_node_id, after.as_deref(), 1000)?;
        if page.is_empty() {
            break;
        }
        after = page.last().map(|node| node.name.clone());
        out.extend(page);
    }
    Ok(out)
}

fn sharded_list_trash_roots(
    fragment: &str,
    state: Option<n1_indexdb::TrashEntryState>,
) -> Result<Vec<n1_indexdb::NamespaceTrashRoot>, AppError> {
    let shard_count = namespace_shard_count(fragment)?;
    let mut roots_by_id: BTreeMap<String, (n1_indexdb::NamespaceTrashRoot, bool)> = BTreeMap::new();
    for shard_id in 0..shard_count {
        for (db, is_owner_copy) in open_all_existing_namespace_shard_dbs(fragment, shard_id)? {
            let mut after = None;
            loop {
                let page = db
                    .n1_list_namespace_trash_roots_all(after.as_deref(), 1000)
                    .map_err(|e| map_indexdb_error("sharded namespace trash roots", e))?;
                if page.is_empty() {
                    break;
                }
                after = page.last().map(|root| root.trash_entry_id.clone());
                for root in page {
                    let replace = match roots_by_id.get(&root.trash_entry_id) {
                        Some((existing, existing_is_owner)) => {
                            trash_root_updated_at(&root) > trash_root_updated_at(existing)
                                || (trash_root_updated_at(&root) == trash_root_updated_at(existing)
                                    && is_owner_copy
                                    && !*existing_is_owner)
                        }
                        None => true,
                    };
                    if replace {
                        roots_by_id.insert(root.trash_entry_id.clone(), (root, is_owner_copy));
                    }
                }
            }
        }
    }
    let mut roots = roots_by_id
        .into_values()
        .map(|(root, _)| root)
        .filter(|root| state.map(|expected| root.state == expected).unwrap_or(true))
        .collect::<Vec<_>>();
    roots.sort_by(|left, right| {
        left.original_path
            .cmp(&right.original_path)
            .then(left.trash_entry_id.cmp(&right.trash_entry_id))
    });
    Ok(roots)
}

fn trash_root_updated_at(root: &n1_indexdb::NamespaceTrashRoot) -> i64 {
    root.purged_at_unix
        .or(root.retired_at_unix)
        .unwrap_or(root.deleted_at_unix)
}

fn sharded_get_trash_root(
    fragment: &str,
    trash_entry_id: &str,
) -> Result<Option<n1_indexdb::NamespaceTrashRoot>, AppError> {
    let shard_id = namespace_trash_shard_id(fragment, trash_entry_id)?;
    let Some(db) = open_existing_namespace_shard_db(fragment, shard_id)? else {
        return Ok(None);
    };
    db.n1_get_namespace_trash_root(trash_entry_id)
        .map_err(|e| map_indexdb_error("sharded namespace trash root get", e))
}

fn sharded_upsert_trash_root(
    fragment: &str,
    root: &n1_indexdb::NamespaceTrashRoot,
    touched_shards: &mut BTreeSet<u16>,
) -> Result<PartitionMeta, AppError> {
    let shard_id = namespace_trash_shard_id(fragment, &root.trash_entry_id)?;
    let db = open_namespace_owner_shard_db(fragment, shard_id)?;
    let path = namespace_shard_paths(fragment, shard_id)?[0].clone();
    let partition_meta = with_partition_commit_lock(&path, || {
        let write_txn = db
            .begin_write()
            .map_err(|e| map_indexdb_error("sharded trash root begin_write", e))?;
        write_txn
            .n1_upsert_namespace_trash_root(root)
            .map_err(|e| map_indexdb_error("sharded trash root upsert", e))?;
        let partition_meta = bump_namespace_shard_generation(&write_txn, root.deleted_at_unix)?;
        write_txn
            .commit()
            .map_err(|e| map_indexdb_error("sharded trash root commit", e))?;
        Ok(partition_meta)
    })?;
    touched_shards.insert(shard_id);
    Ok(partition_meta)
}

fn sharded_set_trash_root_state(
    fragment: &str,
    trash_entry_id: &str,
    state: n1_indexdb::TrashEntryState,
    changed_at: i64,
    touched_shards: &mut BTreeSet<u16>,
) -> Result<PartitionMeta, AppError> {
    let shard_id = namespace_trash_shard_id(fragment, trash_entry_id)?;
    let db = open_namespace_owner_shard_db(fragment, shard_id)?;
    let path = namespace_shard_paths(fragment, shard_id)?[0].clone();
    let partition_meta = with_partition_commit_lock(&path, || {
        let write_txn = db
            .begin_write()
            .map_err(|e| map_indexdb_error("sharded trash state begin_write", e))?;
        write_txn
            .n1_set_namespace_trash_root_state(trash_entry_id, state, changed_at)
            .map_err(|e| map_indexdb_error("sharded trash state set", e))?;
        let partition_meta = bump_namespace_shard_generation(&write_txn, changed_at)?;
        write_txn
            .commit()
            .map_err(|e| map_indexdb_error("sharded trash state commit", e))?;
        Ok(partition_meta)
    })?;
    touched_shards.insert(shard_id);
    Ok(partition_meta)
}

fn sharded_recover_trash_root(
    fragment: &str,
    root: n1_indexdb::NamespaceTrashRoot,
    changed_at: i64,
    touched_shards: &mut BTreeSet<u16>,
    mutations: &mut BTreeMap<u16, NamespaceReplicaMutation>,
) -> Result<NamespaceMutationSummary, AppError> {
    let original_parent_path = parent_path(&root.original_path);
    let Some((_, parent_node)) =
        sharded_resolve_live_node_home_or_fallback(fragment, &original_parent_path)?
    else {
        return Err(AppError::Conflict(format!(
            "original parent for '{}' no longer exists",
            root.original_path
        )));
    };
    if parent_node.node_id != root.original_parent_node_id {
        return Err(AppError::Conflict(format!(
            "original parent for '{}' changed",
            root.original_path
        )));
    }
    if parent_node.kind != n1_indexdb::NamespaceEntryKind::Directory {
        return Err(AppError::Conflict(format!(
            "original parent for '{}' is not a directory",
            root.original_path
        )));
    }
    if sharded_get_current_namespace_child_home(
        fragment,
        &root.original_parent_node_id,
        &root.original_name,
    )?
    .is_some()
    {
        return Err(AppError::Conflict(format!(
            "destination path '{}' already exists",
            root.original_path
        )));
    }
    let trash_parent = trash_parent_node_id(&root.trash_entry_id);
    let root_node =
        match sharded_get_namespace_child_home(fragment, &trash_parent, &root.original_name)? {
            Some(node) => node,
            None => {
                let Some(node) = sharded_get_namespace_node(fragment, &root.root_node_id)? else {
                    return Err(AppError::Conflict(format!(
                        "trash root '{}' is missing namespace node '{}'",
                        root.trash_entry_id, root.root_node_id
                    )));
                };
                node
            }
        };
    if root_node.node_id != root.root_node_id {
        return Err(AppError::Conflict(format!(
            "trash root '{}' points to namespace node '{}' but found '{}'",
            root.trash_entry_id, root.root_node_id, root_node.node_id
        )));
    }
    let effective_changed_at = changed_at.max(root_node.updated_at_unix.saturating_add(1));
    let move_result = sharded_move_namespace_node(
        fragment,
        &root_node,
        &root.original_parent_node_id,
        &root.original_name,
        effective_changed_at,
        touched_shards,
    )?;
    add_namespace_move_to_mutations(move_result, mutations);
    let partition_meta = sharded_set_trash_root_state(
        fragment,
        &root.trash_entry_id,
        n1_indexdb::TrashEntryState::Retired,
        effective_changed_at,
        touched_shards,
    )?;
    let trash_shard = namespace_trash_shard_id(fragment, &root.trash_entry_id)?;
    let mutation = mutations.entry(trash_shard).or_default();
    set_namespace_mutation_partition_meta(mutation, partition_meta);
    mutation.set_trash_root_states.push((
        root.trash_entry_id.clone(),
        n1_indexdb::TrashEntryState::Retired,
        effective_changed_at,
    ));
    Ok(NamespaceMutationSummary {
        operation: "recover".to_string(),
        kind: namespace_kind_from_indexdb(root.kind),
        affected_paths: 1,
        affected_objects: usize::from(root.object_id.is_some()),
        source_path: Some(root.original_path.clone()),
        destination_path: None,
        trash_entry_id: Some(root.trash_entry_id.clone()),
        deleted_at_unix: None,
    })
}

fn sharded_list_pending_uploads_raw(
    fragment: &str,
) -> Result<Vec<n1_indexdb::PendingUploadBinding>, AppError> {
    let mut rows = Vec::new();
    let shard_count = namespace_shard_count(fragment)?;
    for shard_id in 0..shard_count {
        let Some(db) = open_existing_namespace_shard_db(fragment, shard_id)? else {
            continue;
        };
        rows.extend(
            db.n1_list_pending_uploads()
                .map_err(|e| map_indexdb_error("sharded pending upload scan", e))?,
        );
    }
    Ok(rows)
}

fn sharded_resolve_relative_node(
    fragment: &str,
    root_node_id: &str,
    relative_path: &str,
) -> Result<Option<n1_indexdb::NamespaceNode>, AppError> {
    let mut current = sharded_get_namespace_node(fragment, root_node_id)?;
    for segment in relative_path
        .split('/')
        .filter(|segment| !segment.is_empty())
    {
        let Some(node) = current else {
            return Ok(None);
        };
        current = sharded_get_namespace_child(fragment, &node.node_id, segment)?;
    }
    Ok(current)
}

fn sharded_visit_subtree_nodes(
    fragment: &str,
    root_node_id: &str,
    root_path: &str,
    after_path: Option<&str>,
    mut limit: usize,
    mut visit: impl FnMut(String, n1_indexdb::NamespaceNode) -> Result<(), AppError>,
) -> Result<usize, AppError> {
    if limit == 0 {
        return Ok(0);
    }
    let Some(root) = sharded_get_namespace_node(fragment, root_node_id)? else {
        return Ok(0);
    };
    let mut visited = 0usize;
    let mut stack = vec![(root_path.to_string(), root)];
    while let Some((path, node)) = stack.pop() {
        if after_path
            .map(|after| path.as_str() <= after)
            .unwrap_or(false)
        {
            if node.kind == n1_indexdb::NamespaceEntryKind::Directory {
                let mut children = sharded_list_all_namespace_children(fragment, &node.node_id)?;
                children.sort_by(|left, right| right.name.cmp(&left.name));
                for child in children {
                    let child_path = join_path(&path, &child.name);
                    stack.push((child_path, child));
                }
            }
            continue;
        }
        visit(path.clone(), node.clone())?;
        visited += 1;
        limit = limit.saturating_sub(1);
        if limit == 0 {
            break;
        }
        if node.kind != n1_indexdb::NamespaceEntryKind::Directory {
            continue;
        }
        let mut children = sharded_list_all_namespace_children(fragment, &node.node_id)?;
        children.sort_by(|left, right| right.name.cmp(&left.name));
        for child in children {
            let child_path = join_path(&path, &child.name);
            stack.push((child_path, child));
        }
    }
    Ok(visited)
}

fn sharded_list_subtree_nodes(
    fragment: &str,
    root_node_id: &str,
    root_path: &str,
    after_path: Option<&str>,
    limit: usize,
) -> Result<Vec<(String, n1_indexdb::NamespaceNode)>, AppError> {
    let mut rows = Vec::new();
    sharded_visit_subtree_nodes(
        fragment,
        root_node_id,
        root_path,
        after_path,
        limit,
        |path, node| {
            rows.push((path, node));
            Ok(())
        },
    )?;
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(rows)
}

fn sharded_list_trash_dir_page(
    fragment: &str,
    parent: &str,
    limit: usize,
    continuation_token: Option<&str>,
    fetch_limit: usize,
) -> Result<(Vec<NamespaceEntry>, Option<String>), AppError> {
    if parent.is_empty() {
        let mut entries =
            sharded_list_trash_roots(fragment, Some(n1_indexdb::TrashEntryState::Trashed))?
                .into_iter()
                .map(|root| trash_root_to_namespace_entry(fragment, &root))
                .filter(|entry| {
                    continuation_token
                        .map(|after| entry.path.as_str() > after)
                        .unwrap_or(true)
                })
                .take(fetch_limit)
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
    for root in sharded_list_trash_roots(fragment, Some(n1_indexdb::TrashEntryState::Trashed))? {
        if parent_path(&root.original_path) == parent {
            let entry = trash_root_to_namespace_entry(fragment, &root);
            let key = format!("{}\0{}", entry.path, root.trash_entry_id);
            if seen.insert(key) {
                entries.push(entry);
            }
            continue;
        }

        if let Some(relative_to_parent) = root.original_path.strip_prefix(&format!("{parent}/")) {
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
            sharded_resolve_relative_node(fragment, &root.root_node_id, relative_parent)?
        else {
            continue;
        };
        if parent_node.kind != n1_indexdb::NamespaceEntryKind::Directory {
            continue;
        }
        let children =
            sharded_list_namespace_children(fragment, &parent_node.node_id, None, fetch_limit)?;
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

fn sharded_trashed_entry(fragment: &str, path: &str) -> Result<Option<NamespaceEntry>, AppError> {
    for root in sharded_list_trash_roots(fragment, Some(n1_indexdb::TrashEntryState::Trashed))? {
        if root.original_path == path {
            return Ok(Some(trash_root_to_namespace_entry(fragment, &root)));
        }
        let Some(relative) = path.strip_prefix(&format!("{}/", root.original_path)) else {
            continue;
        };
        if let Some(node) = sharded_resolve_relative_node(fragment, &root.root_node_id, relative)? {
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
    Ok(None)
}

fn sharded_list_trash_object_summaries(
    fragment: &str,
    prefix: Option<&str>,
    after_path: Option<&str>,
    limit: usize,
    include_internal: bool,
) -> Result<Vec<n1_indexdb::NamespaceObjectSummary>, AppError> {
    let mut summaries = Vec::new();
    for root in sharded_list_trash_roots(fragment, Some(n1_indexdb::TrashEntryState::Trashed))? {
        if summaries.len() >= limit {
            break;
        }
        if root.kind == n1_indexdb::NamespaceEntryKind::File {
            if prefix
                .map(|prefix| !root.original_path.starts_with(prefix))
                .unwrap_or(false)
            {
                continue;
            }
            if !include_internal && is_internal_namespace_path(&root.original_path) {
                continue;
            }
            if after_path
                .map(|after| root.original_path.as_str() <= after)
                .unwrap_or(false)
            {
                continue;
            }
            let trash_parent = trash_parent_node_id(&root.trash_entry_id);
            let summary = match sharded_get_namespace_child_home_or_fallback(
                fragment,
                &trash_parent,
                &root.original_name,
            )? {
                Some(node) => sharded_file_head_for_node(fragment, &node)?
                    .map(|head| namespace_summary_from_node_head(root.original_path.clone(), head)),
                None => None,
            }
            .or_else(|| {
                root.object_id
                    .as_ref()
                    .zip(root.head_version_id.as_ref())
                    .and_then(|(object_id, version_id)| {
                        read_live_version(fragment, object_id, version_id)
                            .ok()
                            .flatten()
                            .and_then(|metadata| {
                                namespace_summary_from_metadata(
                                    root.original_path.clone(),
                                    metadata,
                                )
                                .ok()
                            })
                    })
            });
            if let Some(summary) = summary {
                summaries.push(summary);
            }
            continue;
        }
        sharded_visit_subtree_nodes(
            fragment,
            &root.root_node_id,
            &root.original_path,
            after_path,
            limit.saturating_sub(summaries.len()),
            |path, node| {
                if node.kind != n1_indexdb::NamespaceEntryKind::File {
                    return Ok(());
                }
                if prefix
                    .map(|prefix| !path.starts_with(prefix))
                    .unwrap_or(false)
                {
                    return Ok(());
                }
                if !include_internal && is_internal_namespace_path(&path) {
                    return Ok(());
                }
                if let Some(head) = sharded_file_head_for_node(fragment, &node)? {
                    summaries.push(namespace_summary_from_node_head(path, head));
                }
                Ok(())
            },
        )?;
    }
    summaries.sort_by(|left, right| left.path.cmp(&right.path));
    summaries.truncate(limit);
    Ok(summaries)
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

fn is_internal_namespace_path(path: &str) -> bool {
    path.split('/')
        .any(|segment| segment == DIR_MARKER_NAME || segment == TRASH_VIRTUAL_NAME)
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
    head: n1_indexdb::NamespaceFileHead,
) -> Result<ObjectHead, AppError> {
    Ok(ObjectHead {
        fragment: fragment.to_string(),
        object_id: entry
            .object_id
            .clone()
            .unwrap_or_else(|| head.object_id.clone()),
        current_version_id: head.version_id.clone(),
        storage_object_id: head.object_id,
        content_type: head.content_type,
        total_size_bytes: head.total_size_bytes,
        lifecycle: entry.lifecycle.clone(),
        created_at_unix: head.created_at_unix,
        updated_at_unix: head.updated_at_unix,
    })
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

fn namespace_shard_count(fragment: &str) -> Result<u16, AppError> {
    fragment_index_partition_count(fragment)
}

fn namespace_shard_file(shard_id: u16) -> String {
    format!("{}{:04x}.ndxdb", INDEXDB_SHARD_FILE_PREFIX, shard_id)
}

fn namespace_shard_owner_index(fragment: &str, shard_id: u16, root_count: usize) -> usize {
    if root_count <= 1 {
        return 0;
    }
    (namespace_owner_index(fragment, root_count) + usize::from(shard_id)) % root_count
}

fn namespace_shard_paths(fragment: &str, shard_id: u16) -> Result<Vec<PathBuf>, AppError> {
    let manager = ObjectPathManager;
    let roots = manager.get_all_index_roots();
    if roots.is_empty() {
        return Err(AppError::InternalServerError(
            "no index roots configured for namespace indexdb".to_string(),
        ));
    }
    let owner_idx = namespace_shard_owner_index(fragment, shard_id, roots.len());
    let shard_file = namespace_shard_file(shard_id);
    let mut ordered = Vec::with_capacity(roots.len());
    ordered.push(
        roots[owner_idx]
            .join(fragment)
            .join(INDEXDB_DIR)
            .join(&shard_file),
    );
    for (idx, root) in roots.into_iter().enumerate() {
        if idx == owner_idx {
            continue;
        }
        ordered.push(root.join(fragment).join(INDEXDB_DIR).join(&shard_file));
    }
    Ok(ordered)
}

fn hash_parts(parts: &[&str]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update([0u8]);
    }
    hasher.finalize().into()
}

fn shard_id_from_hash(fragment: &str, hash: [u8; 32]) -> Result<u16, AppError> {
    let shard_count = namespace_shard_count(fragment)?;
    Ok((u16::from(hash[0]) << 8 | u16::from(hash[1])) % shard_count)
}

fn namespace_node_shard_id(
    fragment: &str,
    parent_node_id: Option<&str>,
    name: &str,
) -> Result<u16, AppError> {
    let Some(parent_node_id) = parent_node_id else {
        return Ok(0);
    };
    shard_id_from_hash(
        fragment,
        hash_parts(&["namespace-node", fragment, parent_node_id, name]),
    )
}

fn namespace_trash_shard_id(fragment: &str, trash_entry_id: &str) -> Result<u16, AppError> {
    shard_id_from_hash(
        fragment,
        hash_parts(&["namespace-trash", fragment, trash_entry_id]),
    )
}

fn namespace_pending_shard_id(fragment: &str, version_id: &str) -> Result<u16, AppError> {
    shard_id_from_hash(
        fragment,
        hash_parts(&["namespace-pending-upload", fragment, version_id]),
    )
}

fn open_namespace_owner_shard_db(fragment: &str, shard_id: u16) -> Result<Arc<Database>, AppError> {
    let paths = namespace_shard_paths(fragment, shard_id)?;
    let owner_path = paths[0].clone();
    let owner_cached = global_namespace_runtime().dbs.contains_key(&owner_path);
    if !owner_cached {
        ensure_namespace_owner_shard_recent(fragment, shard_id, &owner_path)?;
    }
    if let Some(parent) = owner_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| AppError::Io(err, format!("mkdir {}", parent.display())))?;
    }
    global_namespace_runtime().open_db_path(owner_path)
}

fn ensure_namespace_owner_shard_recent(
    fragment: &str,
    shard_id: u16,
    owner_path: &Path,
) -> Result<(), AppError> {
    let runtime = global_namespace_runtime();
    if runtime.verified_owner_shards.contains_key(owner_path) {
        return Ok(());
    }
    if let Some(best_path) =
        best_existing_namespace_shard_db_path_by_generation(fragment, shard_id)?
        && best_path != owner_path
    {
        runtime.dbs.remove(owner_path);
        runtime.db_touch_ticks.remove(owner_path);
        runtime.verified_owner_shards.remove(owner_path);
        with_partition_commit_lock(owner_path, || {
            copy_namespace_db_file(&best_path, owner_path)
        })?;
        runtime.dbs.remove(owner_path);
        runtime.db_touch_ticks.remove(owner_path);
    }
    runtime
        .verified_owner_shards
        .insert(owner_path.to_path_buf(), ());
    Ok(())
}

fn open_existing_namespace_shard_db(
    fragment: &str,
    shard_id: u16,
) -> Result<Option<Arc<Database>>, AppError> {
    let paths = namespace_shard_paths(fragment, shard_id)?;
    let owner_path = paths[0].clone();
    if owner_path.exists() {
        match global_namespace_runtime().open_db_path(owner_path.clone()) {
            Ok(db) => return Ok(Some(db)),
            Err(err) => {
                global_namespace_runtime().dbs.remove(&owner_path);
                global_namespace_runtime()
                    .db_touch_ticks
                    .remove(&owner_path);
                warn!(
                    "namespace shard read fallback: owner copy {} is unreadable: {}; trying a follower copy",
                    owner_path.display(),
                    err
                );
            }
        }
    }
    let Some(path) = best_existing_namespace_shard_db_path(fragment, shard_id)? else {
        return Ok(None);
    };
    match global_namespace_runtime().open_db_path(path.clone()) {
        Ok(db) => Ok(Some(db)),
        Err(err) => {
            global_namespace_runtime().dbs.remove(&path);
            global_namespace_runtime().db_touch_ticks.remove(&path);
            warn!(
                "namespace shard read fallback: selected copy {} is unreadable: {}",
                path.display(),
                err
            );
            Ok(None)
        }
    }
}

fn open_all_existing_namespace_shard_dbs(
    fragment: &str,
    shard_id: u16,
) -> Result<Vec<(Arc<Database>, bool)>, AppError> {
    let paths = namespace_shard_paths(fragment, shard_id)?;
    let mut out = Vec::new();
    for (idx, path) in paths.into_iter().enumerate() {
        if !path.exists() {
            continue;
        }
        match global_namespace_runtime().open_db_path(path.clone()) {
            Ok(db) => out.push((db, idx == 0)),
            Err(err) => {
                global_namespace_runtime().dbs.remove(&path);
                global_namespace_runtime().db_touch_ticks.remove(&path);
                warn!(
                    "namespace shard read fallback: skipping unreadable copy {}: {}",
                    path.display(),
                    err
                );
            }
        }
    }
    Ok(out)
}

fn add_created_namespace_nodes_to_mutations(
    fragment: &str,
    created: Vec<(n1_indexdb::NamespaceNode, PartitionMeta)>,
    mutations: &mut BTreeMap<u16, NamespaceReplicaMutation>,
) -> Result<(), AppError> {
    for (node, partition_meta) in created {
        let shard_id =
            namespace_node_shard_id(fragment, node.parent_node_id.as_deref(), &node.name)?;
        let mutation = mutations.entry(shard_id).or_default();
        mutation.partition_meta = Some(partition_meta);
        mutation.upsert_nodes.push(node);
    }
    Ok(())
}

fn set_namespace_mutation_partition_meta(
    mutation: &mut NamespaceReplicaMutation,
    partition_meta: PartitionMeta,
) {
    let replace = mutation
        .partition_meta
        .as_ref()
        .map(|existing| {
            partition_meta.committed_generation > existing.committed_generation
                || (partition_meta.committed_generation == existing.committed_generation
                    && partition_meta.updated_at > existing.updated_at)
        })
        .unwrap_or(true);
    if replace {
        mutation.partition_meta = Some(partition_meta);
    }
}

fn add_namespace_move_to_mutations(
    move_result: ShardedNamespaceMove,
    mutations: &mut BTreeMap<u16, NamespaceReplicaMutation>,
) {
    if move_result.old_shard == move_result.new_shard {
        let mutation = mutations.entry(move_result.old_shard).or_default();
        set_namespace_mutation_partition_meta(mutation, move_result.old_partition_meta);
        mutation.move_nodes.push(NamespaceNodeMoveReplay {
            node_id: move_result.moved_node.node_id,
            parent_node_id: move_result
                .moved_node
                .parent_node_id
                .unwrap_or_else(|| NAMESPACE_ROOT_NODE_ID.to_string()),
            name: move_result.moved_node.name,
            changed_at_unix: move_result.moved_node.updated_at_unix,
        });
        return;
    }

    let old_mutation = mutations.entry(move_result.old_shard).or_default();
    set_namespace_mutation_partition_meta(old_mutation, move_result.old_partition_meta);
    old_mutation
        .delete_nodes
        .push(move_result.moved_node.node_id.clone());

    let new_mutation = mutations.entry(move_result.new_shard).or_default();
    if let Some(partition_meta) = move_result.new_partition_meta {
        set_namespace_mutation_partition_meta(new_mutation, partition_meta);
    }
    new_mutation.upsert_nodes.push(move_result.moved_node);
    if let Some(file_head) = move_result.file_head {
        new_mutation.upsert_file_heads.push(file_head);
    }
}

fn replicate_namespace_shard_mutations(
    fragment: &str,
    mutations: &BTreeMap<u16, NamespaceReplicaMutation>,
) {
    for (shard_id, mutation) in mutations {
        if let Err(err) = replicate_namespace_shard_mutation(fragment, *shard_id, mutation) {
            warn!(
                "namespace shard replica logical apply failed for fragment {} shard {:04}: {}",
                fragment, shard_id, err
            );
        }
    }
}

fn replicate_namespace_shard_mutation(
    fragment: &str,
    shard_id: u16,
    mutation: &NamespaceReplicaMutation,
) -> Result<(), AppError> {
    if mutation.is_empty() {
        return Ok(());
    }
    let paths = namespace_shard_paths(fragment, shard_id)?;
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
            let runtime = global_namespace_runtime();
            let db = runtime.open_db_path(replica_path.clone())?;
            let write_txn = db
                .begin_write()
                .map_err(|err| map_indexdb_error("namespace shard replica begin_write", err))?;
            for node_id in &mutation.delete_nodes {
                write_txn
                    .n1_delete_namespace_node(node_id)
                    .map_err(|err| map_indexdb_error("namespace shard replica node delete", err))?;
            }
            for node in &mutation.upsert_nodes {
                write_txn
                    .n1_upsert_namespace_node(node)
                    .map_err(|err| map_indexdb_error("namespace shard replica node upsert", err))?;
            }
            for move_node in &mutation.move_nodes {
                write_txn
                    .n1_move_namespace_node(
                        &move_node.node_id,
                        &move_node.parent_node_id,
                        &move_node.name,
                        move_node.changed_at_unix,
                    )
                    .map_err(|err| map_indexdb_error("namespace shard replica node move", err))?;
            }
            for head in &mutation.upsert_file_heads {
                write_txn
                    .n1_upsert_namespace_file_head(head)
                    .map_err(|err| {
                        map_indexdb_error("namespace shard replica file head upsert", err)
                    })?;
            }
            for root in &mutation.upsert_trash_roots {
                write_txn
                    .n1_upsert_namespace_trash_root(root)
                    .map_err(|err| {
                        map_indexdb_error("namespace shard replica trash root upsert", err)
                    })?;
            }
            for (trash_entry_id, state, changed_at_unix) in &mutation.set_trash_root_states {
                write_txn
                    .n1_set_namespace_trash_root_state(trash_entry_id, *state, *changed_at_unix)
                    .map_err(|err| {
                        map_indexdb_error("namespace shard replica trash root state", err)
                    })?;
            }
            for node in &mutation.upsert_nodes {
                let applied = write_txn
                    .n1_get_namespace_node(&node.node_id)
                    .map_err(|err| map_indexdb_error("namespace shard replica node verify", err))?
                    .ok_or_else(|| {
                        AppError::InternalServerError(format!(
                            "namespace shard replica missing upserted node {}",
                            node.node_id
                        ))
                    })?;
                if applied.parent_node_id != node.parent_node_id
                    || applied.name != node.name
                    || applied.kind != node.kind
                    || applied.object_id != node.object_id
                    || applied.head_version_id != node.head_version_id
                {
                    return Err(AppError::InternalServerError(format!(
                        "namespace shard replica node {} did not match owner mutation",
                        node.node_id
                    )));
                }
            }
            for move_node in &mutation.move_nodes {
                let applied = write_txn
                    .n1_get_namespace_node(&move_node.node_id)
                    .map_err(|err| map_indexdb_error("namespace shard replica move verify", err))?
                    .ok_or_else(|| {
                        AppError::InternalServerError(format!(
                            "namespace shard replica missing moved node {}",
                            move_node.node_id
                        ))
                    })?;
                if applied.parent_node_id.as_deref() != Some(move_node.parent_node_id.as_str())
                    || applied.name != move_node.name
                {
                    return Err(AppError::InternalServerError(format!(
                        "namespace shard replica moved node {} did not reach target edge",
                        move_node.node_id
                    )));
                }
            }
            for head in &mutation.upsert_file_heads {
                let applied = write_txn
                    .n1_get_namespace_file_head(&head.node_id)
                    .map_err(|err| {
                        map_indexdb_error("namespace shard replica file head verify", err)
                    })?
                    .ok_or_else(|| {
                        AppError::InternalServerError(format!(
                            "namespace shard replica missing file head {}",
                            head.node_id
                        ))
                    })?;
                if applied.object_id != head.object_id || applied.version_id != head.version_id {
                    return Err(AppError::InternalServerError(format!(
                        "namespace shard replica file head {} did not match owner mutation",
                        head.node_id
                    )));
                }
            }
            for root in &mutation.upsert_trash_roots {
                let applied = write_txn
                    .n1_get_namespace_trash_root(&root.trash_entry_id)
                    .map_err(|err| {
                        map_indexdb_error("namespace shard replica trash root verify", err)
                    })?
                    .ok_or_else(|| {
                        AppError::InternalServerError(format!(
                            "namespace shard replica missing trash root {}",
                            root.trash_entry_id
                        ))
                    })?;
                if applied.root_node_id != root.root_node_id
                    || applied.original_path != root.original_path
                    || applied.state != root.state
                {
                    return Err(AppError::InternalServerError(format!(
                        "namespace shard replica trash root {} did not match owner mutation",
                        root.trash_entry_id
                    )));
                }
            }
            for (trash_entry_id, state, _) in &mutation.set_trash_root_states {
                let applied = write_txn
                    .n1_get_namespace_trash_root(trash_entry_id)
                    .map_err(|err| {
                        map_indexdb_error("namespace shard replica trash state verify", err)
                    })?
                    .ok_or_else(|| {
                        AppError::InternalServerError(format!(
                            "namespace shard replica missing trash root {} for state update",
                            trash_entry_id
                        ))
                    })?;
                if applied.state != *state {
                    return Err(AppError::InternalServerError(format!(
                        "namespace shard replica trash root {} did not reach state {:?}",
                        trash_entry_id, state
                    )));
                }
            }
            if let Some(partition_meta) = &mutation.partition_meta {
                replay_indexdb_metadata_generation(&write_txn, partition_meta)?;
            }
            write_txn
                .commit()
                .map_err(|err| map_indexdb_error("namespace shard replica commit", err))
        });

        if let Err(err) = apply_result {
            warn!(
                "namespace shard replica logical apply failed for {}: {}; falling back to owner copy",
                replica_path.display(),
                err
            );
            let runtime = global_namespace_runtime();
            runtime.dbs.remove(&replica_path);
            runtime.db_touch_ticks.remove(&replica_path);
            if let Err(copy_err) = with_partition_commit_lock(&replica_path, || {
                copy_namespace_db_file(&owner_path, &replica_path)
            }) {
                warn!(
                    "namespace shard replica catch-up failed for {} -> {}: {}",
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

fn replicate_namespace_shards_by_copy(fragment: &str, shard_ids: &BTreeSet<u16>) {
    for shard_id in shard_ids {
        let Ok(paths) = namespace_shard_paths(fragment, *shard_id) else {
            continue;
        };
        if paths.len() <= 1 {
            continue;
        }
        let owner_path = paths[0].clone();
        if !owner_path.exists() {
            continue;
        }
        for replica_path in paths.into_iter().skip(1) {
            global_namespace_runtime().dbs.remove(&replica_path);
            global_namespace_runtime()
                .db_touch_ticks
                .remove(&replica_path);
            if let Err(err) = with_partition_commit_lock(&replica_path, || {
                copy_namespace_db_file(&owner_path, &replica_path)
            }) {
                warn!(
                    "namespace shard replica catch-up failed for {} -> {}: {}",
                    owner_path.display(),
                    replica_path.display(),
                    err
                );
            }
            global_namespace_runtime().dbs.remove(&replica_path);
            global_namespace_runtime()
                .db_touch_ticks
                .remove(&replica_path);
        }
    }
}

fn best_existing_namespace_shard_db_path(
    fragment: &str,
    shard_id: u16,
) -> Result<Option<PathBuf>, AppError> {
    let infos = namespace_shard_paths(fragment, shard_id)?
        .into_iter()
        .filter_map(|path| match inspect_namespace_db_file(&path) {
            Ok(Some(info)) => Some(info),
            Ok(None) => None,
            Err(err) => {
                warn!(
                    "namespace shard read fallback: ignoring unreadable copy {}: {}",
                    path.display(),
                    err
                );
                global_namespace_runtime().dbs.remove(&path);
                global_namespace_runtime().db_touch_ticks.remove(&path);
                None
            }
        })
        .collect::<Vec<_>>();
    if infos.is_empty() {
        return Ok(None);
    }

    let mut hash_counts: HashMap<[u8; 32], usize> = HashMap::new();
    for info in &infos {
        *hash_counts.entry(info.metadata_hash).or_default() += 1;
    }

    let canonical_hash = choose_canonical_metadata_hash(&hash_counts);
    let candidates = infos
        .iter()
        .filter(|info| {
            canonical_hash
                .map(|hash| info.metadata_hash == hash)
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    let mut best: Option<&NamespaceReplicaFileInfo> = None;
    for info in candidates {
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

fn best_existing_namespace_shard_db_path_by_generation(
    fragment: &str,
    shard_id: u16,
) -> Result<Option<PathBuf>, AppError> {
    let mut best: Option<(PathBuf, u64, u64)> = None;
    for path in namespace_shard_paths(fragment, shard_id)? {
        let metadata = match fs::metadata(&path) {
            Ok(metadata) => metadata,
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                ) =>
            {
                continue;
            }
            Err(err) => return Err(AppError::Io(err, format!("stat {}", path.display()))),
        };
        let len = metadata.len();
        let generation = match open_n1_indexdb(&path).and_then(|db| read_partition_meta_exact(&db))
        {
            Ok(Some(meta)) => meta.committed_generation,
            Ok(None) => 0,
            Err(err) => {
                warn!(
                    "namespace shard owner catch-up: ignoring unreadable copy {}: {}",
                    path.display(),
                    err
                );
                global_namespace_runtime().dbs.remove(&path);
                global_namespace_runtime().db_touch_ticks.remove(&path);
                continue;
            }
        };

        match &best {
            Some((_, best_generation, best_len))
                if *best_generation > generation
                    || (*best_generation == generation
                        && (*best_generation > 0 || *best_len >= len)) => {}
            _ => best = Some((path, generation, len)),
        }
    }
    Ok(best.map(|(path, _, _)| path))
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
        Err(err)
            if matches!(
                err.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
            ) =>
        {
            return Ok(None);
        }
        Err(err) => return Err(AppError::Io(err, format!("stat {}", path.display()))),
    };
    let modified_at = metadata
        .modified()
        .map_err(|err| AppError::Io(err, format!("mtime {}", path.display())))?;
    let db = open_n1_indexdb(path)?;
    let generation = read_partition_meta_exact(&db)?
        .map(|meta| meta.committed_generation)
        .unwrap_or(0);
    let metadata_hash = namespace_indexdb_semantic_hash(&db)?;
    Ok(Some(NamespaceReplicaFileInfo {
        path: path.to_path_buf(),
        modified_at,
        len: metadata.len(),
        generation,
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
            if let Some(head) = db
                .n1_get_namespace_file_head(&node.node_id)
                .map_err(|e| map_indexdb_error("namespace file head semantic hash get", e))?
            {
                hash_field(&mut hasher, "namespace-file-head");
                hash_field(&mut hasher, &head.node_id);
                hash_field(&mut hasher, &head.object_id);
                hash_field(&mut hasher, &head.version_id);
                hash_field(&mut hasher, &head.upload_session_id);
                hash_field(&mut hasher, &head.owner_uid);
                hash_field(&mut hasher, &head.content_type);
                hash_field(&mut hasher, &head.total_size_bytes.to_string());
                hash_field(&mut hasher, &head.created_at_unix.to_string());
                hash_field(&mut hasher, if head.is_delete_marker { "1" } else { "0" });
                hash_field(&mut hasher, &head.stripe_size_bytes.to_string());
                hash_field(&mut hasher, &head.updated_at_unix.to_string());
            }
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
        .generation
        .cmp(&current.generation)
        .then_with(|| candidate.len.cmp(&current.len))
        .then_with(|| candidate.modified_at.cmp(&current.modified_at))
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
    source_db
        .checkpoint()
        .map_err(|err| map_indexdb_error("namespace replica source checkpoint", err))?;
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
