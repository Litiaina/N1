use super::disk_manager::{
    ListObjectsResult, ObjectMetadata, ObjectPathManager, PartMetadata, ReservationReceipt,
};
use super::fragment_metadata::fragment_index_partition_count;
use super::index::{IndexSessionState, SessionPartReceipt, TrashCatalogEntry};
use super::upload::UploadSessionManifest;
use crate::enums::app_error_enum::AppError;
use base64::Engine;
use base64::engine::general_purpose;
use chrono::Utc;
use indexdb::{Database, ReadableDatabase, ReadableTable, TableDefinition, TableError};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, LazyLock, Mutex};
use tracing::warn;
use ulid::Ulid;

const INDEXDB_DIR: &str = "indexdb";
const INDEXDB_FILE: &str = "index.ndxdb";
const VERSION_KEY_SEPARATOR: char = '\u{001f}';
const VERSION_KEY_SEPARATOR_STR: &str = "\u{001f}";

const OBJECT_HEADS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("object_heads");
const OBJECT_VERSIONS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("object_versions");
const OBJECT_VERSION_STATES_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("object_version_states");
const TRASH_OBJECTS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("trash_objects");
const LIVE_NAMESPACE_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("live_namespace");
const SESSIONS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("sessions");
const SESSION_PARTS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("session_parts");
const SESSION_PART_RECEIPTS_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("session_part_receipts");
const PARTITION_META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("partition_meta");
const PARTITION_META_KEY: &str = "__partition__";

static DB_CACHE: LazyLock<Mutex<HashMap<PathBuf, CachedDatabaseEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static DB_CACHE_TOUCH_TICK: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(1));
static PARTITION_OP_LOCKS: LazyLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
const MAX_CACHED_DBS: usize = 128;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum ObjectState {
    Live,
    Trash,
    Deleted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VersionRecord {
    metadata: ObjectMetadata,
    recorded_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VersionStateRecord {
    state: ObjectState,
    updated_at: i64,
    generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TrashObjectRecord {
    version_ids: Vec<String>,
    deleted_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TrashCatalogRecord {
    partition_id: u16,
    deleted_at: i64,
    latest_metadata: ObjectMetadata,
    version_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PartitionMeta {
    committed_generation: u64,
    updated_at: i64,
}

struct CachedDatabaseEntry {
    db: Arc<Database>,
    last_used_tick: u64,
}

struct VersionUpsertMutation {
    partition_meta: PartitionMeta,
    row_key: String,
    version_bytes: Vec<u8>,
    state_bytes: Vec<u8>,
    head_key: String,
    head_bytes: Option<Vec<u8>>,
}

struct SessionRemoveMutation {
    partition_meta: PartitionMeta,
    expected_parts: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionRow {
    manifest: UploadSessionManifest,
    reservation: Option<ReservationReceipt>,
    last_activity_at: i64,
    finalized_at: Option<i64>,
    aborted_at: Option<i64>,
}

impl SessionRow {
    fn into_state(self) -> IndexSessionState {
        IndexSessionState {
            manifest: self.manifest,
            reservation: self.reservation,
            parts: Default::default(),
            part_receipts: Default::default(),
            last_activity_at: self.last_activity_at,
            finalized_at: self.finalized_at,
            aborted_at: self.aborted_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HeadRecord {
    fragment: String,
    object_key: String,
    version_id: String,
    upload_session_id: String,
    owner_uid: String,
    content_type: String,
    total_size_bytes: u64,
    created_at: i64,
    is_delete_marker: bool,
    stripe_size_bytes: usize,
    state: ObjectState,
    generation: u64,
    updated_at: i64,
}

impl HeadRecord {
    fn from_metadata(
        metadata: &ObjectMetadata,
        state: ObjectState,
        generation: u64,
        updated_at: i64,
    ) -> Self {
        Self {
            fragment: metadata.fragment.clone(),
            object_key: metadata.object_key.clone(),
            version_id: metadata.version_id.clone(),
            upload_session_id: metadata.upload_session_id.clone(),
            owner_uid: metadata.owner_uid.clone(),
            content_type: metadata.content_type.clone(),
            total_size_bytes: metadata.total_size_bytes,
            created_at: metadata.created_at,
            is_delete_marker: metadata.is_delete_marker,
            stripe_size_bytes: metadata.stripe_size_bytes,
            state,
            generation,
            updated_at,
        }
    }

    fn to_summary_metadata(&self) -> ObjectMetadata {
        ObjectMetadata {
            object_key: self.object_key.clone(),
            fragment: self.fragment.clone(),
            version_id: self.version_id.clone(),
            upload_session_id: self.upload_session_id.clone(),
            owner_uid: self.owner_uid.clone(),
            content_type: self.content_type.clone(),
            total_size_bytes: self.total_size_bytes,
            created_at: self.created_at,
            parts: Vec::new(),
            is_delete_marker: self.is_delete_marker,
            stripe_size_bytes: self.stripe_size_bytes,
        }
    }

    fn with_state(&self, state: ObjectState, generation: u64, updated_at: i64) -> Self {
        let mut next = self.clone();
        next.state = state;
        next.generation = generation;
        next.updated_at = updated_at;
        next
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListingCursor {
    state: ObjectState,
    prefix: Option<String>,
    delimiter: Option<String>,
    include_internal: bool,
    last_key: Option<String>,
    resume_skip_prefix: Option<String>,
}

#[derive(Debug)]
struct NamespaceListingItem {
    object_key: String,
    summary: ObjectMetadata,
}

struct NamespacePartitionRange {
    _db: Arc<Database>,
    iter: indexdb::Range<'static, &'static str, &'static [u8]>,
    next: Option<NamespaceListingItem>,
}

#[derive(Debug, Clone)]
struct IndexRoute {
    partition_id: u16,
    roots: Vec<PathBuf>,
}

fn canonical_key(object_key: &str) -> String {
    ObjectPathManager::canonical_object_key(object_key)
}

fn is_internal_object_key(object_key: &str) -> bool {
    canonical_key(object_key)
        .split('/')
        .any(|segment| segment == ".n1fsd_dir" || segment == ".n1-trash")
}

fn compare_version_id_recency(left: &str, right: &str) -> Ordering {
    match (Ulid::from_string(left).ok(), Ulid::from_string(right).ok()) {
        (Some(left_ulid), Some(right_ulid)) => left_ulid.cmp(&right_ulid).then(left.cmp(right)),
        _ => left.cmp(right),
    }
}

fn compare_version_recency(
    left_created_at: i64,
    left_version_id: &str,
    right_created_at: i64,
    right_version_id: &str,
) -> Ordering {
    left_created_at
        .cmp(&right_created_at)
        .then_with(|| compare_version_id_recency(left_version_id, right_version_id))
}

fn metadata_is_newer(candidate: &ObjectMetadata, current: &ObjectMetadata) -> bool {
    compare_version_recency(
        candidate.created_at,
        &candidate.version_id,
        current.created_at,
        &current.version_id,
    ) == Ordering::Greater
}

fn hash_key(parts: &[&str]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update([0u8]);
    }
    hasher.finalize().into()
}

fn fragment_partition_count(fragment: &str) -> Result<u16, AppError> {
    fragment_index_partition_count(fragment)
}

fn fragment_owner_index(fragment: &str, root_count: usize) -> usize {
    if root_count <= 1 {
        return 0;
    }
    let hash = hash_key(&["fragment", fragment]);
    usize::from(u16::from(hash[0]) << 8 | u16::from(hash[1])) % root_count
}

fn object_bucket_id(fragment: &str, object_key: &str) -> Result<u16, AppError> {
    let partition_count = fragment_partition_count(fragment)?;
    let hash = hash_key(&["object", fragment, &canonical_key(object_key)]);
    Ok((u16::from(hash[0]) << 8 | u16::from(hash[1])) % partition_count)
}

fn bucket_key_prefix(bucket_id: u16) -> String {
    format!("{:04x}{}", bucket_id, VERSION_KEY_SEPARATOR)
}

fn head_row_key(fragment: &str, object_key: &str) -> Result<String, AppError> {
    Ok(format!(
        "{}{}",
        bucket_key_prefix(object_bucket_id(fragment, object_key)?),
        canonical_key(object_key)
    ))
}

fn object_version_row_prefix(fragment: &str, object_key: &str) -> Result<String, AppError> {
    Ok(format!(
        "{}{}{}",
        bucket_key_prefix(object_bucket_id(fragment, object_key)?),
        canonical_key(object_key),
        VERSION_KEY_SEPARATOR
    ))
}

fn route_for_hash(fragment: &str, hash: [u8; 32]) -> Result<IndexRoute, AppError> {
    let partition_count = fragment_partition_count(fragment)?;
    let partition_id = (u16::from(hash[0]) << 8 | u16::from(hash[1])) % partition_count;
    route_for_partition(fragment, partition_id)
}

fn route_for_partition(fragment: &str, partition_id: u16) -> Result<IndexRoute, AppError> {
    let partition_count = fragment_partition_count(fragment)?;
    if partition_id >= partition_count {
        return Err(AppError::InternalServerError(format!(
            "partition {:04x} is out of range for fragment {} with {} partitions",
            partition_id, fragment, partition_count
        )));
    }
    let roots = ObjectPathManager::new().get_all_index_roots();
    if roots.is_empty() {
        return Err(AppError::InternalServerError(
            "No index roots configured".to_string(),
        ));
    }

    let owner_idx = fragment_owner_index(fragment, roots.len());
    let mut selected = Vec::with_capacity(roots.len());
    selected.push(roots[owner_idx].clone());
    for (idx, root) in roots.into_iter().enumerate() {
        if idx != owner_idx {
            selected.push(root);
        }
    }

    Ok(IndexRoute {
        partition_id,
        roots: selected,
    })
}

fn fragment_route(fragment: &str) -> Result<IndexRoute, AppError> {
    route_for_partition(fragment, 0)
}

fn object_route(fragment: &str, object_key: &str) -> Result<IndexRoute, AppError> {
    route_for_hash(
        fragment,
        hash_key(&["object", fragment, &canonical_key(object_key)]),
    )
}

fn session_route(fragment: &str, version_id: &str) -> Result<IndexRoute, AppError> {
    route_for_hash(fragment, hash_key(&["session", fragment, version_id]))
}

fn db_path(root: &Path, fragment: &str, partition_id: u16) -> PathBuf {
    let _ = partition_id;
    root.join(fragment).join(INDEXDB_DIR).join(INDEXDB_FILE)
}

fn version_row_key(fragment: &str, object_key: &str, version_id: &str) -> Result<String, AppError> {
    Ok(format!(
        "{}{}",
        object_version_row_prefix(fragment, object_key)?,
        version_id
    ))
}

fn version_row_prefix(fragment: &str, object_key: &str) -> Result<String, AppError> {
    object_version_row_prefix(fragment, object_key)
}

fn parse_version_key(row_key: &str) -> Option<(&str, &str)> {
    row_key.rsplit_once(VERSION_KEY_SEPARATOR_STR)
}

fn session_part_row_prefix(version_id: &str) -> String {
    format!("{}{}", version_id, VERSION_KEY_SEPARATOR)
}

fn session_part_row_key(version_id: &str, part_index: u64) -> String {
    format!("{}{:020}", session_part_row_prefix(version_id), part_index)
}

fn destructive_partition_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().clamp(2, 8))
        .unwrap_or(4)
}

fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, AppError> {
    rmp_serde::to_vec(value)
        .map_err(|e| AppError::InternalServerError(format!("indexdb serialize: {}", e)))
}

fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, AppError> {
    rmp_serde::from_slice(bytes)
        .map_err(|e| AppError::InternalServerError(format!("indexdb deserialize: {}", e)))
}

fn map_indexdb_error(context: &str, err: impl std::fmt::Display) -> AppError {
    AppError::InternalServerError(format!("indexdb {}: {}", context, err))
}

fn evict_cached_db(path: &Path) {
    if let Ok(mut cache) = DB_CACHE.lock() {
        cache.remove(path);
    }
}

#[cfg(test)]
pub(crate) fn evict_cached_db_for_test(path: &Path) {
    evict_cached_db(path);
}

#[cfg(test)]
pub(crate) fn clear_cached_dbs_for_test() {
    if let Ok(mut cache) = DB_CACHE.lock() {
        cache.clear();
    }
}

#[cfg(test)]
pub(crate) fn clear_live_namespace_indexes_for_test(fragment: &str) -> Result<(), AppError> {
    let route = fragment_route(fragment)?;
    for root in route.roots {
        let path = db_path(&root, fragment, route.partition_id);
        if !path.exists() {
            continue;
        }
        let db = get_or_open_db(&path)?;
        let write_txn = db
            .begin_write()
            .map_err(|e| map_indexdb_error("begin_write", e))?;
        {
            let mut table = match write_txn.open_table(LIVE_NAMESPACE_TABLE) {
                Ok(table) => table,
                Err(TableError::TableDoesNotExist(_)) => continue,
                Err(err) => return Err(map_indexdb_error("open live_namespace", err)),
            };
            let mut keys = Vec::new();
            let mut iter = table
                .iter()
                .map_err(|e| map_indexdb_error("live_namespace iter", e))?;
            while let Some(entry) = iter.next() {
                let (key, _) =
                    entry.map_err(|e| map_indexdb_error("live_namespace iter next", e))?;
                keys.push(key.value().to_string());
            }
            for key in keys {
                let _ = table
                    .remove(key.as_str())
                    .map_err(|e| map_indexdb_error("live_namespace remove", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| map_indexdb_error("commit live_namespace clear", e))?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn count_live_namespace_rows_for_test(fragment: &str) -> Result<usize, AppError> {
    let mut total = 0usize;
    let route = fragment_route(fragment)?;
    let path = owner_db_path(&route, fragment);
    if path.exists() {
        let db = get_or_open_db(&path)?;
        let read_txn = db
            .begin_read()
            .map_err(|e| map_indexdb_error("begin_read", e))?;
        let table = match read_txn.open_table(LIVE_NAMESPACE_TABLE) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(0),
            Err(err) => return Err(map_indexdb_error("open live_namespace", err)),
        };
        let mut iter = table
            .iter()
            .map_err(|e| map_indexdb_error("live_namespace iter", e))?;
        while let Some(entry) = iter.next() {
            let _ = entry.map_err(|e| map_indexdb_error("live_namespace iter next", e))?;
            total = total.saturating_add(1);
        }
    }
    Ok(total)
}

fn atomic_copy_db_file(src: &Path, dst: &Path) -> Result<(), AppError> {
    let parent = dst.parent().ok_or_else(|| {
        AppError::InternalServerError(format!("missing parent for {}", dst.display()))
    })?;
    fs::create_dir_all(parent)
        .map_err(|e| AppError::Io(e, format!("mkdir {}", parent.display())))?;
    let file_name = dst
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("partition");
    let tmp = parent.join(format!(".{}.tmp.{}", file_name, Ulid::new()));
    fs::copy(src, &tmp)
        .map_err(|e| AppError::Io(e, format!("copy {} -> {}", src.display(), tmp.display())))?;
    fs::rename(&tmp, dst)
        .map_err(|e| AppError::Io(e, format!("rename {} -> {}", tmp.display(), dst.display())))?;
    Ok(())
}

fn get_or_open_db(path: &Path) -> Result<Arc<Database>, AppError> {
    let touch_tick = DB_CACHE_TOUCH_TICK.fetch_add(1, AtomicOrdering::Relaxed);
    let mut cache = DB_CACHE
        .lock()
        .map_err(|_| AppError::InternalServerError("indexdb cache lock poisoned".to_string()))?;
    if let Some(entry) = cache.get_mut(path) {
        entry.last_used_tick = touch_tick;
        return Ok(entry.db.clone());
    }

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| AppError::Io(e, format!("mkdir {}", parent.display())))?;
    }

    let db = Arc::new(
        std::panic::catch_unwind(|| Database::create(path))
            .map_err(|_| {
                AppError::InternalServerError(format!(
                    "indexdb open {} panicked",
                    path.display()
                ))
            })?
            .map_err(|e| map_indexdb_error(&format!("open {}", path.display()), e))?,
    );
    cache.insert(
        path.to_path_buf(),
        CachedDatabaseEntry {
            db: db.clone(),
            last_used_tick: touch_tick,
        },
    );

    if cache.len() > MAX_CACHED_DBS {
        let mut evictable: Vec<(PathBuf, u64)> = cache
            .iter()
            .filter_map(|(candidate_path, entry)| {
                (Arc::strong_count(&entry.db) == 1)
                    .then_some((candidate_path.clone(), entry.last_used_tick))
            })
            .collect();
        evictable.sort_by_key(|(_, last_used_tick)| *last_used_tick);

        while cache.len() > MAX_CACHED_DBS {
            let Some((candidate_path, _)) = evictable.first().cloned() else {
                break;
            };
            cache.remove(&candidate_path);
            evictable.remove(0);
        }
    }

    Ok(db)
}

fn open_fragment_db_for_read(fragment: &str) -> Result<Option<Arc<Database>>, AppError> {
    let route = fragment_route(fragment)?;
    open_route_db_for_read(fragment, &route)
}

fn open_route_db_for_read(
    fragment: &str,
    route: &IndexRoute,
) -> Result<Option<Arc<Database>>, AppError> {
    let owner_path = owner_db_path(route, fragment);
    let owner_available = owner_path.exists();
    let mut owner_with_meta: Option<(u64, Arc<Database>)> = None;
    let mut owner_without_meta: Option<Arc<Database>> = None;
    let mut best_follower: Option<(u64, Arc<Database>)> = None;
    let mut best_follower_without_meta: Option<Arc<Database>> = None;
    for root in &route.roots {
        let path = db_path(root, fragment, route.partition_id);
        if path.exists() {
            let Some(db) = (match get_or_open_db(&path) {
                Ok(db) => Some(db),
                Err(err) => {
                    warn!(
                        "indexdb read fallback: failed to open {}: {}",
                        path.display(),
                        err
                    );
                    None
                }
            }) else {
                continue;
            };

            if root != &route.roots[0] && best_follower_without_meta.is_none() {
                best_follower_without_meta = Some(db.clone());
            }

            let meta = match read_partition_meta_exact(&db) {
                Ok(Some(meta)) => Some(meta),
                Ok(None) => match with_partition_commit_lock(&owner_path, || {
                    ensure_partition_meta_exact(&db)
                }) {
                    Ok(meta) => Some(meta),
                    Err(err) => {
                        if root == &route.roots[0] {
                            warn!(
                                "indexdb owner read self-heal failed for {}: {}. continuing with owner copy",
                                path.display(),
                                err
                            );
                        } else {
                            warn!(
                                "indexdb read fallback: {} missing partition metadata and self-heal failed: {}",
                                path.display(),
                                err
                            );
                        }
                        None
                    }
                },
                Err(err) => {
                    if root == &route.roots[0] {
                        warn!(
                            "indexdb owner read metadata check failed for {}: {}. continuing with owner copy",
                            path.display(),
                            err
                        );
                    } else {
                        warn!(
                            "indexdb read fallback: failed to read partition metadata from {}: {}",
                            path.display(),
                            err
                        );
                    }
                    None
                }
            };

            if root == &route.roots[0] {
                if meta.is_some() {
                    owner_with_meta = meta
                        .as_ref()
                        .map(|meta| (meta.committed_generation, db.clone()));
                }
                if owner_with_meta.is_none() {
                    owner_without_meta = Some(db);
                }
                continue;
            }

            let Some(meta) = meta else {
                continue;
            };

            match &best_follower {
                Some((best_generation, _)) if *best_generation >= meta.committed_generation => {}
                _ => best_follower = Some((meta.committed_generation, db)),
            }
        }
    }
    match (owner_with_meta, best_follower) {
        (Some((owner_generation, owner_db)), Some((follower_generation, follower_db))) => {
            if follower_generation > owner_generation {
                warn!(
                    "indexdb read fallback: follower copy for fragment {} partition {:04x} is newer than owner copy (owner_gen={}, follower_gen={}); using follower copy",
                    fragment, route.partition_id, owner_generation, follower_generation
                );
                return Ok(Some(follower_db));
            }
            return Ok(Some(owner_db));
        }
        (Some((_, owner_db)), None) => return Ok(Some(owner_db)),
        (None, Some((_, follower_db))) => return Ok(Some(follower_db)),
        (None, None) => {}
    }
    if let Some(db) = owner_without_meta {
        warn!(
            "indexdb read fallback: owner copy for fragment {} partition {:04x} is missing valid partition metadata; using owner copy as last resort",
            fragment, route.partition_id
        );
        return Ok(Some(db));
    }
    if !owner_available {
        if let Some(db) = best_follower_without_meta {
            warn!(
                "indexdb read fallback: owner copy unavailable for fragment {} partition {:04x}; using follower copy without partition metadata",
                fragment, route.partition_id
            );
            return Ok(Some(db));
        }
    }
    Ok(None)
}

fn owner_db_path(route: &IndexRoute, fragment: &str) -> PathBuf {
    db_path(&route.roots[0], fragment, route.partition_id)
}

fn select_authoritative_partition_db_path(
    route: &IndexRoute,
    fragment: &str,
) -> Result<Option<(PathBuf, u64)>, AppError> {
    let owner_path = owner_db_path(route, fragment);
    let mut best: Option<(PathBuf, u64)> = None;
    let mut first_existing: Option<PathBuf> = None;

    for root in &route.roots {
        let candidate_path = db_path(root, fragment, route.partition_id);
        if !candidate_path.exists() {
            continue;
        }
        if first_existing.is_none() {
            first_existing = Some(candidate_path.clone());
        }

        let generation = match get_or_open_db(&candidate_path)
            .and_then(|db| read_partition_meta_exact(&db))
        {
            Ok(Some(meta)) => meta.committed_generation,
            Ok(None) => 0,
            Err(err) => {
                warn!(
                    "indexdb authoritative replica scan failed for {}: {}",
                    candidate_path.display(),
                    err
                );
                0
            }
        };

        match &best {
            Some((best_path, best_generation))
                if *best_generation > generation
                    || (*best_generation == generation && best_path == &owner_path) => {}
            _ => best = Some((candidate_path, generation)),
        }
    }

    Ok(best.or_else(|| first_existing.map(|path| (path, 0))))
}

fn seed_owner_partition_db_from_authoritative_copy(
    route: &IndexRoute,
    fragment: &str,
) -> Result<(), AppError> {
    let owner_path = owner_db_path(route, fragment);
    let Some((source_path, _)) = select_authoritative_partition_db_path(route, fragment)? else {
        return Ok(());
    };

    if source_path == owner_path {
        return Ok(());
    }

    evict_cached_db(&owner_path);
    atomic_copy_db_file(&source_path, &owner_path)?;
    Ok(())
}

fn open_owner_db_for_write(route: &IndexRoute, fragment: &str) -> Result<Arc<Database>, AppError> {
    seed_owner_partition_db_from_authoritative_copy(route, fragment)?;
    get_or_open_db(&owner_db_path(route, fragment))
}

fn with_partition_commit_lock<T, F>(path: &Path, op: F) -> Result<T, AppError>
where
    F: FnOnce() -> Result<T, AppError>,
{
    let lock = {
        let mut locks = PARTITION_OP_LOCKS.lock().map_err(|_| {
            AppError::InternalServerError("indexdb partition lock poisoned".to_string())
        })?;
        locks
            .entry(path.to_path_buf())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    };
    let _guard = lock.lock().map_err(|_| {
        AppError::InternalServerError("indexdb partition lock poisoned".to_string())
    })?;
    op()
}

fn replicate_owner_partition_db(fragment: &str, route: &IndexRoute) -> Result<(), AppError> {
    if route.roots.len() <= 1 {
        return Ok(());
    }

    let owner_path = owner_db_path(route, fragment);
    let source_path = select_authoritative_partition_db_path(route, fragment)?
        .map(|(path, _)| path)
        .ok_or_else(|| {
        AppError::InternalServerError(format!(
            "no partition db replicas exist for fragment {} partition {:04x}",
            fragment, route.partition_id
        ))
    })?;

    if source_path != owner_path {
        warn!(
            "indexdb replica sync: follower copy for fragment {} partition {:04x} is authoritative (source={}, owner={})",
            fragment,
            route.partition_id,
            source_path.display(),
            owner_path.display()
        );
    }

    for replica_root in &route.roots {
        let replica_path = db_path(replica_root, fragment, route.partition_id);
        if replica_path == source_path {
            continue;
        }
        evict_cached_db(&replica_path);
        if let Err(err) = atomic_copy_db_file(&source_path, &replica_path) {
            warn!(
                "indexdb replica catch-up failed for {} -> {}: {}",
                source_path.display(),
                replica_path.display(),
                err
            );
        }
    }

    Ok(())
}

fn replicate_session_mutation(
    fragment: &str,
    route: &IndexRoute,
    version_id: &str,
    row: &SessionRow,
    part: Option<&PartMetadata>,
    receipt: Option<&SessionPartReceipt>,
    partition_meta: &PartitionMeta,
) -> Result<(), AppError> {
    if route.roots.len() <= 1 {
        return Ok(());
    }

    for replica_root in route.roots.iter().skip(1) {
        let replica_path = db_path(replica_root, fragment, route.partition_id);
        let replica_db = match get_or_open_db(&replica_path) {
            Ok(db) => db,
            Err(err) => {
                warn!(
                    "session replica open failed for {}: {}",
                    replica_path.display(),
                    err
                );
                continue;
            }
        };
        if let Err(err) = apply_session_mutation_replica_exact(
            &replica_db,
            version_id,
            row,
            part,
            receipt,
            partition_meta,
        ) {
            warn!(
                "session replica update failed for {} on {}: {}",
                version_id,
                replica_path.display(),
                err
            );
        }
    }

    Ok(())
}

fn replicate_session_remove(
    fragment: &str,
    route: &IndexRoute,
    version_id: &str,
    mutation: &SessionRemoveMutation,
) -> Result<(), AppError> {
    if route.roots.len() <= 1 {
        return Ok(());
    }

    for replica_root in route.roots.iter().skip(1) {
        let replica_path = db_path(replica_root, fragment, route.partition_id);
        let replica_db = match get_or_open_db(&replica_path) {
            Ok(db) => db,
            Err(err) => {
                warn!(
                    "session replica open failed for {}: {}",
                    replica_path.display(),
                    err
                );
                continue;
            }
        };
        if let Err(err) = apply_session_remove_replica_exact(&replica_db, version_id, mutation) {
            warn!(
                "session replica removal failed for {} on {}: {}",
                version_id,
                replica_path.display(),
                err
            );
        }
    }

    Ok(())
}

pub fn has_object_partition_db(fragment: &str, object_key: &str) -> Result<bool, AppError> {
    let route = object_route(fragment, object_key)?;
    Ok(route
        .roots
        .iter()
        .any(|root| db_path(root, fragment, route.partition_id).exists()))
}

pub fn object_partition_id(fragment: &str, object_key: &str) -> Result<u16, AppError> {
    Ok(object_route(fragment, object_key)?.partition_id)
}

pub fn ensure_object_owner_partition_available(
    fragment: &str,
    object_key: &str,
) -> Result<(), AppError> {
    let route = object_route(fragment, object_key)?;
    let owner_path = owner_db_path(&route, fragment);
    with_partition_commit_lock(&owner_path, || {
        if let Some(parent) = owner_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                AppError::ServiceUnavailable(format!(
                    "authoritative owner index root unavailable: mkdir {} failed: {}",
                    parent.display(),
                    e
                ))
            })?;
        }
        let _ = open_owner_db_for_write(&route, fragment).map_err(|e| {
            AppError::ServiceUnavailable(format!(
                "authoritative owner index root unavailable: {}",
                e
            ))
        })?;
        Ok(())
    })
}

pub fn has_session_partition_db(fragment: &str, version_id: &str) -> Result<bool, AppError> {
    let route = session_route(fragment, version_id)?;
    Ok(route
        .roots
        .iter()
        .any(|root| db_path(root, fragment, route.partition_id).exists()))
}

pub fn has_any_fragment_partition_db(fragment: &str) -> Result<bool, AppError> {
    let route = fragment_route(fragment)?;
    Ok(route
        .roots
        .iter()
        .any(|root| db_path(root, fragment, route.partition_id).exists()))
}

pub fn sync_fragment_partition_replicas(fragment: &str) -> Result<(), AppError> {
    let route = fragment_route(fragment)?;
    if route.roots.len() <= 1 {
        return Ok(());
    }
    let owner_path = owner_db_path(&route, fragment);
    with_partition_commit_lock(&owner_path, || {
        if owner_path.exists() {
            replicate_owner_partition_db(fragment, &route)?;
        }
        Ok(())
    })?;
    Ok(())
}

fn read_head_exact(
    db: &Database,
    fragment: &str,
    object_key: &str,
) -> Result<Option<HeadRecord>, AppError> {
    let read_txn = db
        .begin_read()
        .map_err(|e| map_indexdb_error("begin_read", e))?;
    let table = match read_txn.open_table(OBJECT_HEADS_TABLE) {
        Ok(table) => table,
        Err(TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(err) => return Err(map_indexdb_error("open object_heads", err)),
    };
    let key = head_row_key(fragment, object_key)?;
    let Some(value) = table
        .get(key.as_str())
        .map_err(|e| map_indexdb_error("head get", e))?
    else {
        return Ok(None);
    };
    deserialize(value.value()).map(Some)
}

fn read_version_exact(
    db: &Database,
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<Option<VersionRecord>, AppError> {
    let read_txn = db
        .begin_read()
        .map_err(|e| map_indexdb_error("begin_read", e))?;
    let table = match read_txn.open_table(OBJECT_VERSIONS_TABLE) {
        Ok(table) => table,
        Err(TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(err) => return Err(map_indexdb_error("open object_versions", err)),
    };
    let key = version_row_key(fragment, object_key, version_id)?;
    let Some(value) = table
        .get(key.as_str())
        .map_err(|e| map_indexdb_error("version get", e))?
    else {
        return Ok(None);
    };
    deserialize(value.value()).map(Some)
}

fn read_version_state_exact(
    db: &Database,
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<Option<VersionStateRecord>, AppError> {
    let read_txn = db
        .begin_read()
        .map_err(|e| map_indexdb_error("begin_read", e))?;
    let table = match read_txn.open_table(OBJECT_VERSION_STATES_TABLE) {
        Ok(table) => table,
        Err(TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(err) => return Err(map_indexdb_error("open object_version_states", err)),
    };
    let key = version_row_key(fragment, object_key, version_id)?;
    let Some(value) = table
        .get(key.as_str())
        .map_err(|e| map_indexdb_error("version_state get", e))?
    else {
        return Ok(None);
    };
    deserialize(value.value()).map(Some)
}

fn read_partition_meta_exact(db: &Database) -> Result<Option<PartitionMeta>, AppError> {
    let read_txn = db
        .begin_read()
        .map_err(|e| map_indexdb_error("begin_read", e))?;
    let table = match read_txn.open_table(PARTITION_META_TABLE) {
        Ok(table) => table,
        Err(TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(err) => return Err(map_indexdb_error("open partition_meta", err)),
    };
    let Some(value) = table
        .get(PARTITION_META_KEY)
        .map_err(|e| map_indexdb_error("partition_meta get", e))?
    else {
        return Ok(None);
    };
    deserialize(value.value()).map(Some)
}

fn derive_partition_generation(db: &Database) -> Result<u64, AppError> {
    let read_txn = db
        .begin_read()
        .map_err(|e| map_indexdb_error("begin_read", e))?;
    let mut max_generation = 0u64;

    let heads = match read_txn.open_table(OBJECT_HEADS_TABLE) {
        Ok(table) => Some(table),
        Err(TableError::TableDoesNotExist(_)) => None,
        Err(err) => return Err(map_indexdb_error("open object_heads", err)),
    };
    if let Some(table) = heads {
        let mut iter = table
            .iter()
            .map_err(|e| map_indexdb_error("head iter", e))?;
        while let Some(entry) = iter.next() {
            let (_key, value) = entry.map_err(|e| map_indexdb_error("head iter next", e))?;
            let head: HeadRecord = deserialize(value.value())?;
            max_generation = max_generation.max(head.generation);
        }
    }

    let states = match read_txn.open_table(OBJECT_VERSION_STATES_TABLE) {
        Ok(table) => Some(table),
        Err(TableError::TableDoesNotExist(_)) => None,
        Err(err) => return Err(map_indexdb_error("open object_version_states", err)),
    };
    if let Some(table) = states {
        let mut iter = table
            .iter()
            .map_err(|e| map_indexdb_error("version_state iter", e))?;
        while let Some(entry) = iter.next() {
            let (_key, value) =
                entry.map_err(|e| map_indexdb_error("version_state iter next", e))?;
            let state: VersionStateRecord = deserialize(value.value())?;
            max_generation = max_generation.max(state.generation);
        }
    }

    Ok(max_generation)
}

fn ensure_partition_meta_exact(db: &Database) -> Result<PartitionMeta, AppError> {
    if let Some(meta) = read_partition_meta_exact(db)? {
        return Ok(meta);
    }

    let now = Utc::now().timestamp();
    let derived_generation = derive_partition_generation(db)?.max(1);
    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    let meta = {
        let mut table = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        if let Some(existing) = table
            .get(PARTITION_META_KEY)
            .map_err(|e| map_indexdb_error("partition_meta get", e))?
        {
            deserialize(existing.value())?
        } else {
            let meta = PartitionMeta {
                committed_generation: derived_generation,
                updated_at: now,
            };
            let bytes = serialize(&meta)?;
            table
                .insert(PARTITION_META_KEY, bytes.as_slice())
                .map_err(|e| map_indexdb_error("partition_meta insert", e))?;
            meta
        }
    };
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit partition_meta self-heal", e))?;
    Ok(meta)
}

fn next_partition_generation(
    meta: &mut indexdb::Table<&str, &[u8]>,
    updated_at: i64,
) -> Result<u64, AppError> {
    let current = meta
        .get(PARTITION_META_KEY)
        .map_err(|e| map_indexdb_error("partition_meta get", e))?
        .map(|value| deserialize::<PartitionMeta>(value.value()))
        .transpose()?
        .map(|value| value.committed_generation)
        .unwrap_or(0);
    let next = current.saturating_add(1);
    let bytes = serialize(&PartitionMeta {
        committed_generation: next,
        updated_at,
    })?;
    meta.insert(PARTITION_META_KEY, bytes.as_slice())
        .map_err(|e| map_indexdb_error("partition_meta insert", e))?;
    Ok(next)
}

fn compare_head_recency(
    candidate_created_at: i64,
    candidate_version_id: &str,
    head: &HeadRecord,
) -> Ordering {
    compare_version_recency(
        candidate_created_at,
        candidate_version_id,
        head.created_at,
        &head.version_id,
    )
}

fn candidate_should_replace_head(
    current_head: Option<&HeadRecord>,
    metadata: &ObjectMetadata,
    candidate_state: ObjectState,
) -> bool {
    match current_head {
        None => true,
        Some(head) if head.version_id == metadata.version_id => head.state != ObjectState::Deleted,
        Some(head) if candidate_state == ObjectState::Live && head.state != ObjectState::Live => {
            true
        }
        Some(head) => {
            compare_head_recency(metadata.created_at, &metadata.version_id, head)
                == Ordering::Greater
        }
    }
}

fn version_is_suppressed_by_tombstone(
    head: Option<&HeadRecord>,
    metadata: &ObjectMetadata,
) -> bool {
    matches!(
        head,
        Some(head)
            if head.state == ObjectState::Deleted
                && compare_head_recency(metadata.created_at, &metadata.version_id, head)
                    != Ordering::Greater
    )
}

fn next_head_generation(current_head: Option<&HeadRecord>) -> u64 {
    current_head
        .map(|head| head.generation.saturating_add(1))
        .unwrap_or(1)
}

fn write_head_exact(
    heads: &mut indexdb::Table<&str, &[u8]>,
    head: &HeadRecord,
) -> Result<(), AppError> {
    let bytes = serialize(head)?;
    let key = head_row_key(&head.fragment, &head.object_key)?;
    heads
        .insert(key.as_str(), bytes.as_slice())
        .map_err(|e| map_indexdb_error("head insert", e))?;
    Ok(())
}

fn write_live_namespace_exact(
    live_namespace: &mut indexdb::Table<&str, &[u8]>,
    metadata: &ObjectMetadata,
) -> Result<(), AppError> {
    let bytes = serialize(metadata)?;
    live_namespace
        .insert(
            canonical_key(&metadata.object_key).as_str(),
            bytes.as_slice(),
        )
        .map_err(|e| map_indexdb_error("live_namespace insert", e))?;
    Ok(())
}

fn remove_live_namespace_exact(
    live_namespace: &mut indexdb::Table<&str, &[u8]>,
    object_key: &str,
) -> Result<(), AppError> {
    let _ = live_namespace
        .remove(canonical_key(object_key).as_str())
        .map_err(|e| map_indexdb_error("live_namespace remove", e))?;
    Ok(())
}

fn sync_live_namespace_from_head_exact(
    live_namespace: &mut indexdb::Table<&str, &[u8]>,
    head: &HeadRecord,
) -> Result<(), AppError> {
    if head.state == ObjectState::Live {
        write_live_namespace_exact(live_namespace, &head.to_summary_metadata())
    } else {
        remove_live_namespace_exact(live_namespace, &head.object_key)
    }
}

fn write_head_with_namespace_exact(
    heads: &mut indexdb::Table<&str, &[u8]>,
    live_namespace: &mut indexdb::Table<&str, &[u8]>,
    head: &HeadRecord,
) -> Result<(), AppError> {
    write_head_exact(heads, head)?;
    sync_live_namespace_from_head_exact(live_namespace, head)
}

fn remove_head_exact(
    heads: &mut indexdb::Table<&str, &[u8]>,
    fragment: &str,
    object_key: &str,
) -> Result<(), AppError> {
    let key = head_row_key(fragment, object_key)?;
    let _ = heads
        .remove(key.as_str())
        .map_err(|e| map_indexdb_error("head remove", e))?;
    Ok(())
}

fn remove_head_with_namespace_exact(
    heads: &mut indexdb::Table<&str, &[u8]>,
    live_namespace: &mut indexdb::Table<&str, &[u8]>,
    fragment: &str,
    object_key: &str,
) -> Result<(), AppError> {
    remove_head_exact(heads, fragment, object_key)?;
    remove_live_namespace_exact(live_namespace, object_key)
}

fn load_latest_remaining_version_exact(
    versions: &indexdb::Table<&str, &[u8]>,
    states: &indexdb::Table<&str, &[u8]>,
    fragment: &str,
    object_key: &str,
) -> Result<Option<(ObjectMetadata, ObjectState)>, AppError> {
    let prefix = version_row_prefix(fragment, object_key)?;
    let mut iter = states
        .range(prefix.as_str()..)
        .map_err(|e| map_indexdb_error("version_state range", e))?;
    let mut latest_remaining: Option<(ObjectMetadata, ObjectState)> = None;

    while let Some(entry) = iter.next() {
        let (row_key, value) =
            entry.map_err(|e| map_indexdb_error("version_state range next", e))?;
        let row_key = row_key.value();
        if !row_key.starts_with(&prefix) {
            break;
        }
        let visibility: VersionStateRecord = deserialize(value.value())?;
        if visibility.state == ObjectState::Deleted {
            continue;
        }
        let Some(version_value) = versions
            .get(row_key)
            .map_err(|e| map_indexdb_error("version get", e))?
        else {
            continue;
        };
        let metadata = deserialize::<VersionRecord>(version_value.value())?.metadata;
        match &latest_remaining {
            Some((current, _)) if !metadata_is_newer(&metadata, current) => {}
            _ => latest_remaining = Some((metadata, visibility.state)),
        }
    }

    Ok(latest_remaining)
}

fn rewrite_head_from_remaining_versions_exact(
    heads: &mut indexdb::Table<&str, &[u8]>,
    live_namespace: &mut indexdb::Table<&str, &[u8]>,
    versions: &indexdb::Table<&str, &[u8]>,
    states: &indexdb::Table<&str, &[u8]>,
    fragment: &str,
    object_key: &str,
    current_head: Option<&HeadRecord>,
    updated_at: i64,
) -> Result<(), AppError> {
    if let Some((metadata, state)) =
        load_latest_remaining_version_exact(versions, states, fragment, object_key)?
    {
        let next_head = HeadRecord::from_metadata(
            &metadata,
            state,
            next_head_generation(current_head),
            updated_at,
        );
        write_head_with_namespace_exact(heads, live_namespace, &next_head)?;
    } else {
        remove_head_with_namespace_exact(heads, live_namespace, fragment, object_key)?;
    }
    Ok(())
}

fn list_versions_exact(
    db: &Database,
    fragment: &str,
    object_key: &str,
    state: ObjectState,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let read_txn = db
        .begin_read()
        .map_err(|e| map_indexdb_error("begin_read", e))?;
    let versions = match read_txn.open_table(OBJECT_VERSIONS_TABLE) {
        Ok(table) => table,
        Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(err) => return Err(map_indexdb_error("open object_versions", err)),
    };
    let states = match read_txn.open_table(OBJECT_VERSION_STATES_TABLE) {
        Ok(table) => table,
        Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(err) => return Err(map_indexdb_error("open object_version_states", err)),
    };
    let head = read_head_exact(db, fragment, object_key)?;

    let prefix = version_row_prefix(fragment, object_key)?;
    let mut iter = states
        .range(prefix.as_str()..)
        .map_err(|e| map_indexdb_error("version_state range", e))?;
    let mut out = Vec::new();
    while let Some(entry) = iter.next() {
        let (row_key, value) =
            entry.map_err(|e| map_indexdb_error("version_state range next", e))?;
        let row_key = row_key.value();
        if !row_key.starts_with(&prefix) {
            break;
        }
        let visibility: VersionStateRecord = deserialize(value.value())?;
        if visibility.state != state {
            continue;
        }
        let Some(version_value) = versions
            .get(row_key)
            .map_err(|e| map_indexdb_error("version get", e))?
        else {
            continue;
        };
        let record: VersionRecord = deserialize(version_value.value())?;
        if version_is_suppressed_by_tombstone(head.as_ref(), &record.metadata) {
            continue;
        }
        out.push(record.metadata);
    }
    out.sort_by(|left, right| {
        compare_version_recency(
            right.created_at,
            &right.version_id,
            left.created_at,
            &left.version_id,
        )
    });
    Ok(out)
}

fn write_version_on_db(
    db: &Database,
    metadata: &ObjectMetadata,
    state: ObjectState,
) -> Result<VersionUpsertMutation, AppError> {
    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    let mutation = {
        let mut partition_meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let mut heads = write_txn
            .open_table(OBJECT_HEADS_TABLE)
            .map_err(|e| map_indexdb_error("open object_heads", e))?;
        let mut live_namespace = write_txn
            .open_table(LIVE_NAMESPACE_TABLE)
            .map_err(|e| map_indexdb_error("open live_namespace", e))?;
        let mut versions = write_txn
            .open_table(OBJECT_VERSIONS_TABLE)
            .map_err(|e| map_indexdb_error("open object_versions", e))?;
        let mut states = write_txn
            .open_table(OBJECT_VERSION_STATES_TABLE)
            .map_err(|e| map_indexdb_error("open object_version_states", e))?;
        let now = Utc::now().timestamp();
        let committed_generation = next_partition_generation(&mut partition_meta, now)?;
        let row_key = version_row_key(
            &metadata.fragment,
            &metadata.object_key,
            &metadata.version_id,
        )?;
        let record = VersionRecord {
            metadata: metadata.clone(),
            recorded_at: now,
        };
        let bytes = serialize(&record)?;
        versions
            .insert(row_key.as_str(), bytes.as_slice())
            .map_err(|e| map_indexdb_error("version insert", e))?;
        let visibility = VersionStateRecord {
            state,
            updated_at: now,
            generation: committed_generation,
        };
        let visibility_bytes = serialize(&visibility)?;
        states
            .insert(row_key.as_str(), visibility_bytes.as_slice())
            .map_err(|e| map_indexdb_error("version_state insert", e))?;
        let current_head = heads
            .get(head_row_key(&metadata.fragment, &metadata.object_key)?.as_str())
            .map_err(|e| map_indexdb_error("head get", e))?
            .map(|value| deserialize::<HeadRecord>(value.value()))
            .transpose()?;
        let head_key = head_row_key(&metadata.fragment, &metadata.object_key)?;
        let head_bytes = if candidate_should_replace_head(current_head.as_ref(), metadata, state) {
            let head = HeadRecord::from_metadata(
                metadata,
                state,
                next_head_generation(current_head.as_ref()),
                now,
            );
            write_head_with_namespace_exact(&mut heads, &mut live_namespace, &head)?;
            Some(serialize(&head)?)
        } else {
            None
        };
        VersionUpsertMutation {
            partition_meta: PartitionMeta {
                committed_generation,
                updated_at: now,
            },
            row_key,
            version_bytes: bytes,
            state_bytes: visibility_bytes,
            head_key,
            head_bytes,
        }
    };
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit version write", e))?;
    Ok(mutation)
}

fn change_version_state_on_db(
    db: &Database,
    fragment: &str,
    object_key: &str,
    version_id: &str,
    target_state: ObjectState,
    expected_state: Option<ObjectState>,
) -> Result<Option<(ObjectMetadata, PartitionMeta)>, AppError> {
    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    let (record, partition_meta) = {
        let mut partition_meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let mut heads = write_txn
            .open_table(OBJECT_HEADS_TABLE)
            .map_err(|e| map_indexdb_error("open object_heads", e))?;
        let mut live_namespace = write_txn
            .open_table(LIVE_NAMESPACE_TABLE)
            .map_err(|e| map_indexdb_error("open live_namespace", e))?;
        let versions = write_txn
            .open_table(OBJECT_VERSIONS_TABLE)
            .map_err(|e| map_indexdb_error("open object_versions", e))?;
        let mut states = write_txn
            .open_table(OBJECT_VERSION_STATES_TABLE)
            .map_err(|e| map_indexdb_error("open object_version_states", e))?;
        let mut trash_objects = write_txn
            .open_table(TRASH_OBJECTS_TABLE)
            .map_err(|e| map_indexdb_error("open trash_objects", e))?;
        let now = Utc::now().timestamp();
        let committed_generation = next_partition_generation(&mut partition_meta, now)?;
        let row_key = version_row_key(fragment, object_key, version_id)?;
        let current_head = heads
            .get(head_row_key(fragment, object_key)?.as_str())
            .map_err(|e| map_indexdb_error("head get", e))?
            .map(|value| deserialize::<HeadRecord>(value.value()))
            .transpose()?;
        let metadata = {
            let Some(existing) = versions
                .get(row_key.as_str())
                .map_err(|e| map_indexdb_error("version get", e))?
            else {
                return Ok(None);
            };
            deserialize::<VersionRecord>(existing.value())?.metadata
        };
        let existing_state = {
            let Some(existing) = states
                .get(row_key.as_str())
                .map_err(|e| map_indexdb_error("version_state get", e))?
            else {
                return Ok(None);
            };
            deserialize::<VersionStateRecord>(existing.value())?
        };
        if let Some(expected) = expected_state {
            if existing_state.state != expected {
                return Ok(None);
            }
        }
        let next_state = VersionStateRecord {
            state: target_state,
            updated_at: now,
            generation: committed_generation,
        };
        let bytes = serialize(&next_state)?;
        states
            .insert(row_key.as_str(), bytes.as_slice())
            .map_err(|e| map_indexdb_error("version_state insert", e))?;
        if let Some(head) = current_head.as_ref() {
            if head.version_id == version_id {
                let next_head = HeadRecord::from_metadata(
                    &metadata,
                    target_state,
                    next_head_generation(current_head.as_ref()),
                    now,
                );
                write_head_with_namespace_exact(&mut heads, &mut live_namespace, &next_head)?;
            } else if target_state == ObjectState::Live
                && expected_state == Some(ObjectState::Trash)
            {
                let restored_head = HeadRecord::from_metadata(
                    &metadata,
                    ObjectState::Live,
                    next_head_generation(current_head.as_ref()),
                    now,
                );
                write_head_with_namespace_exact(&mut heads, &mut live_namespace, &restored_head)?;
            }
        } else if target_state == ObjectState::Live && expected_state == Some(ObjectState::Trash) {
            let restored_head = HeadRecord::from_metadata(&metadata, ObjectState::Live, 1, now);
            write_head_with_namespace_exact(&mut heads, &mut live_namespace, &restored_head)?;
        }

        let trash_key = canonical_key(object_key);
        if target_state == ObjectState::Trash {
            let mut existing = trash_objects
                .get(trash_key.as_str())
                .map_err(|e| map_indexdb_error("trash_objects get", e))?
                .map(|value| deserialize::<TrashObjectRecord>(value.value()))
                .transpose()?
                .unwrap_or(TrashObjectRecord {
                    version_ids: Vec::new(),
                    deleted_at: now,
                });
            existing.deleted_at = now;
            if !existing
                .version_ids
                .iter()
                .any(|candidate| candidate == version_id)
            {
                existing.version_ids.push(version_id.to_string());
                existing
                    .version_ids
                    .sort_by(|left, right| compare_version_id_recency(left, right));
            }
            let bytes = serialize(&existing)?;
            trash_objects
                .insert(trash_key.as_str(), bytes.as_slice())
                .map_err(|e| map_indexdb_error("trash_objects insert", e))?;
        } else if expected_state == Some(ObjectState::Trash) {
            let maybe_existing = trash_objects
                .get(trash_key.as_str())
                .map_err(|e| map_indexdb_error("trash_objects get", e))?
                .map(|value| deserialize::<TrashObjectRecord>(value.value()))
                .transpose()?;
            if let Some(mut existing) = maybe_existing {
                existing
                    .version_ids
                    .retain(|candidate| candidate != version_id);
                if existing.version_ids.is_empty() {
                    let _ = trash_objects
                        .remove(trash_key.as_str())
                        .map_err(|e| map_indexdb_error("trash_objects remove", e))?;
                } else {
                    let bytes = serialize(&existing)?;
                    trash_objects
                        .insert(trash_key.as_str(), bytes.as_slice())
                        .map_err(|e| map_indexdb_error("trash_objects insert", e))?;
                }
            }
        }
        (
            metadata,
            PartitionMeta {
                committed_generation,
                updated_at: now,
            },
        )
    };
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit version state write", e))?;
    Ok(Some((record, partition_meta)))
}

fn delete_version_on_db(
    db: &Database,
    fragment: &str,
    object_key: &str,
    version_id: &str,
    expected_state: Option<ObjectState>,
) -> Result<Option<(ObjectMetadata, PartitionMeta)>, AppError> {
    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    let (record, partition_meta) = {
        let mut partition_meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let mut heads = write_txn
            .open_table(OBJECT_HEADS_TABLE)
            .map_err(|e| map_indexdb_error("open object_heads", e))?;
        let mut live_namespace = write_txn
            .open_table(LIVE_NAMESPACE_TABLE)
            .map_err(|e| map_indexdb_error("open live_namespace", e))?;
        let mut versions = write_txn
            .open_table(OBJECT_VERSIONS_TABLE)
            .map_err(|e| map_indexdb_error("open object_versions", e))?;
        let mut states = write_txn
            .open_table(OBJECT_VERSION_STATES_TABLE)
            .map_err(|e| map_indexdb_error("open object_version_states", e))?;
        let mut trash_objects = write_txn
            .open_table(TRASH_OBJECTS_TABLE)
            .map_err(|e| map_indexdb_error("open trash_objects", e))?;
        let now = Utc::now().timestamp();
        let committed_generation = next_partition_generation(&mut partition_meta, now)?;
        let row_key = version_row_key(fragment, object_key, version_id)?;
        let current_head = heads
            .get(head_row_key(fragment, object_key)?.as_str())
            .map_err(|e| map_indexdb_error("head get", e))?
            .map(|value| deserialize::<HeadRecord>(value.value()))
            .transpose()?;
        let existing_state = states
            .get(row_key.as_str())
            .map_err(|e| map_indexdb_error("version_state get", e))?
            .map(|value| deserialize::<VersionStateRecord>(value.value()))
            .transpose()?;
        let Some(existing_state) = existing_state else {
            return Ok(None);
        };
        if expected_state.is_some() && expected_state != Some(existing_state.state) {
            return Ok(None);
        }
        let removed_record = {
            let Some(existing_value) = versions
                .remove(row_key.as_str())
                .map_err(|e| map_indexdb_error("version remove", e))?
            else {
                return Ok(None);
            };
            deserialize::<VersionRecord>(existing_value.value())?
        };
        let _ = states
            .remove(row_key.as_str())
            .map_err(|e| map_indexdb_error("version_state remove", e))?;
        if existing_state.state == ObjectState::Trash {
            let trash_key = canonical_key(object_key);
            let maybe_existing = trash_objects
                .get(trash_key.as_str())
                .map_err(|e| map_indexdb_error("trash_objects get", e))?
                .map(|value| deserialize::<TrashObjectRecord>(value.value()))
                .transpose()?;
            if let Some(mut existing) = maybe_existing {
                existing
                    .version_ids
                    .retain(|candidate| candidate != version_id);
                if existing.version_ids.is_empty() {
                    let _ = trash_objects
                        .remove(trash_key.as_str())
                        .map_err(|e| map_indexdb_error("trash_objects remove", e))?;
                } else {
                    let bytes = serialize(&existing)?;
                    trash_objects
                        .insert(trash_key.as_str(), bytes.as_slice())
                        .map_err(|e| map_indexdb_error("trash_objects insert", e))?;
                }
            }
        }
        rewrite_head_from_remaining_versions_exact(
            &mut heads,
            &mut live_namespace,
            &versions,
            &states,
            fragment,
            object_key,
            current_head.as_ref(),
            now,
        )?;
        (
            removed_record,
            PartitionMeta {
                committed_generation,
                updated_at: now,
            },
        )
    };
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit version delete", e))?;
    Ok(Some((record.metadata, partition_meta)))
}

fn bulk_rewrite_keys_state_on_db(
    db: &Database,
    fragment: &str,
    object_keys: &[String],
    from_state: ObjectState,
    to_state: ObjectState,
) -> Result<(HashMap<String, Vec<String>>, PartitionMeta), AppError> {
    let plans = {
        let read_txn = db
            .begin_read()
            .map_err(|e| map_indexdb_error("begin_read", e))?;
        let heads = read_txn
            .open_table(OBJECT_HEADS_TABLE)
            .map_err(|e| map_indexdb_error("open object_heads", e))?;
        let states = read_txn
            .open_table(OBJECT_VERSION_STATES_TABLE)
            .map_err(|e| map_indexdb_error("open object_version_states", e))?;
        let mut plans = Vec::<(String, Option<HeadRecord>, Vec<String>, Vec<String>)>::new();

        for object_key in object_keys {
            let current_head = heads
                .get(head_row_key(fragment, object_key)?.as_str())
                .map_err(|e| map_indexdb_error("head get", e))?
                .map(|value| deserialize::<HeadRecord>(value.value()))
                .transpose()?;
            let prefix = version_row_prefix(fragment, object_key)?;
            let mut iter = states
                .range(prefix.as_str()..)
                .map_err(|e| map_indexdb_error("version_state range", e))?;
            let mut version_ids = Vec::new();
            let mut row_keys = Vec::new();
            while let Some(entry) = iter.next() {
                let (row_key, value) =
                    entry.map_err(|e| map_indexdb_error("version_state range next", e))?;
                let row_key = row_key.value();
                if !row_key.starts_with(&prefix) {
                    break;
                }
                let record: VersionStateRecord = deserialize(value.value())?;
                if record.state != from_state {
                    continue;
                }
                let (_, version_id) = parse_version_key(row_key).ok_or_else(|| {
                    AppError::InternalServerError("invalid version row key".to_string())
                })?;
                version_ids.push(version_id.to_string());
                row_keys.push(row_key.to_string());
            }
            if !version_ids.is_empty() {
                plans.push((object_key.clone(), current_head, version_ids, row_keys));
            }
        }

        plans
    };

    if plans.is_empty() {
        return Ok((
            HashMap::new(),
            PartitionMeta {
                committed_generation: 0,
                updated_at: 0,
            },
        ));
    }

    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    let (touched, partition_meta) = {
        let mut partition_meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let mut heads = write_txn
            .open_table(OBJECT_HEADS_TABLE)
            .map_err(|e| map_indexdb_error("open object_heads", e))?;
        let mut live_namespace = write_txn
            .open_table(LIVE_NAMESPACE_TABLE)
            .map_err(|e| map_indexdb_error("open live_namespace", e))?;
        let mut states = write_txn
            .open_table(OBJECT_VERSION_STATES_TABLE)
            .map_err(|e| map_indexdb_error("open object_version_states", e))?;
        let mut trash_objects = write_txn
            .open_table(TRASH_OBJECTS_TABLE)
            .map_err(|e| map_indexdb_error("open trash_objects", e))?;
        let now = Utc::now().timestamp();
        let committed_generation = next_partition_generation(&mut partition_meta, now)?;
        let partition_meta = PartitionMeta {
            committed_generation,
            updated_at: now,
        };
        let mut touched = HashMap::new();

        for (object_key, current_head, mut version_ids, row_keys) in plans {
            for row_key in row_keys {
                let bytes = serialize(&VersionStateRecord {
                    state: to_state,
                    updated_at: partition_meta.updated_at,
                    generation: partition_meta.committed_generation,
                })?;
                states
                    .insert(row_key.as_str(), bytes.as_slice())
                    .map_err(|e| map_indexdb_error("bulk version_state rewrite", e))?;
            }

            match current_head.as_ref() {
                Some(head) if head.state == from_state => {
                    let next_head = head.with_state(
                        to_state,
                        next_head_generation(current_head.as_ref()),
                        partition_meta.updated_at,
                    );
                    write_head_with_namespace_exact(&mut heads, &mut live_namespace, &next_head)?;
                }
                Some(_) => {}
                None => {
                    return Err(AppError::InternalServerError(format!(
                        "indexdb missing head for {} while rewriting version visibility",
                        canonical_key(&object_key)
                    )));
                }
            }

            let trash_key = canonical_key(&object_key);
            if from_state == ObjectState::Live && to_state == ObjectState::Trash {
                let mut merged = trash_objects
                    .get(trash_key.as_str())
                    .map_err(|e| map_indexdb_error("trash_objects get", e))?
                    .map(|value| deserialize::<TrashObjectRecord>(value.value()))
                    .transpose()?
                    .unwrap_or(TrashObjectRecord {
                        version_ids: Vec::new(),
                        deleted_at: partition_meta.updated_at,
                    });
                merged.deleted_at = partition_meta.updated_at;
                merged.version_ids.extend(version_ids.iter().cloned());
                merged
                    .version_ids
                    .sort_by(|left, right| compare_version_id_recency(left, right));
                merged.version_ids.dedup();
                let bytes = serialize(&merged)?;
                trash_objects
                    .insert(trash_key.as_str(), bytes.as_slice())
                    .map_err(|e| map_indexdb_error("trash_objects insert", e))?;
            } else if from_state == ObjectState::Trash && to_state == ObjectState::Live {
                let maybe_existing = trash_objects
                    .get(trash_key.as_str())
                    .map_err(|e| map_indexdb_error("trash_objects get", e))?
                    .map(|value| deserialize::<TrashObjectRecord>(value.value()))
                    .transpose()?;
                if let Some(mut existing) = maybe_existing {
                    existing.version_ids.retain(|version_id| {
                        !version_ids.iter().any(|candidate| candidate == version_id)
                    });
                    if existing.version_ids.is_empty() {
                        let _ = trash_objects
                            .remove(trash_key.as_str())
                            .map_err(|e| map_indexdb_error("trash_objects remove", e))?;
                    } else {
                        let bytes = serialize(&existing)?;
                        trash_objects
                            .insert(trash_key.as_str(), bytes.as_slice())
                            .map_err(|e| map_indexdb_error("trash_objects insert", e))?;
                    }
                }
            }

            version_ids.sort_by(|left, right| compare_version_id_recency(left, right));
            touched.insert(object_key, version_ids);
        }

        (touched, partition_meta)
    };
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit bulk version rewrite", e))?;
    Ok((touched, partition_meta))
}

fn apply_restore_trash_entries_exact(
    db: &Database,
    fragment: &str,
    object_versions: &HashMap<String, Vec<String>>,
    partition_meta: &PartitionMeta,
    strict: bool,
) -> Result<HashMap<String, Vec<String>>, AppError> {
    if object_versions.is_empty() {
        return Ok(HashMap::new());
    }

    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    let touched = {
        let mut meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let mut heads = write_txn
            .open_table(OBJECT_HEADS_TABLE)
            .map_err(|e| map_indexdb_error("open object_heads", e))?;
        let mut live_namespace = write_txn
            .open_table(LIVE_NAMESPACE_TABLE)
            .map_err(|e| map_indexdb_error("open live_namespace", e))?;
        let versions = write_txn
            .open_table(OBJECT_VERSIONS_TABLE)
            .map_err(|e| map_indexdb_error("open object_versions", e))?;
        let mut states = write_txn
            .open_table(OBJECT_VERSION_STATES_TABLE)
            .map_err(|e| map_indexdb_error("open object_version_states", e))?;
        let mut trash_objects = write_txn
            .open_table(TRASH_OBJECTS_TABLE)
            .map_err(|e| map_indexdb_error("open trash_objects", e))?;
        write_partition_meta_exact(&mut meta, partition_meta)?;

        let mut touched = HashMap::<String, Vec<String>>::new();
        for (object_key, raw_version_ids) in object_versions {
            let current_head = heads
                .get(head_row_key(fragment, object_key)?.as_str())
                .map_err(|e| map_indexdb_error("head get", e))?
                .map(|value| deserialize::<HeadRecord>(value.value()))
                .transpose()?;

            let mut restored_ids = Vec::new();
            let mut latest_restored: Option<ObjectMetadata> = None;
            for version_id in raw_version_ids.iter().collect::<BTreeSet<_>>() {
                let row_key = version_row_key(fragment, object_key, version_id)?;
                let current_state = {
                    let Some(state_value) = states
                        .get(row_key.as_str())
                        .map_err(|e| map_indexdb_error("version_state get", e))?
                    else {
                        if strict {
                            return Err(AppError::InternalServerError(format!(
                                "missing trash state for {}:{}",
                                object_key, version_id
                            )));
                        }
                        continue;
                    };
                    deserialize::<VersionStateRecord>(state_value.value())?
                };
                if current_state.state != ObjectState::Trash {
                    if strict {
                        return Err(AppError::InternalServerError(format!(
                            "unexpected state for {}:{} expected Trash got {:?}",
                            object_key, version_id, current_state.state
                        )));
                    }
                    continue;
                }
                let Some(version_value) = versions
                    .get(row_key.as_str())
                    .map_err(|e| map_indexdb_error("version get", e))?
                else {
                    if strict {
                        return Err(AppError::InternalServerError(format!(
                            "missing version row for {}:{}",
                            object_key, version_id
                        )));
                    }
                    continue;
                };
                let metadata = deserialize::<VersionRecord>(version_value.value())?.metadata;
                match &latest_restored {
                    Some(current) if !metadata_is_newer(&metadata, current) => {}
                    _ => latest_restored = Some(metadata),
                }
                let bytes = serialize(&VersionStateRecord {
                    state: ObjectState::Live,
                    updated_at: partition_meta.updated_at,
                    generation: partition_meta.committed_generation,
                })?;
                states
                    .insert(row_key.as_str(), bytes.as_slice())
                    .map_err(|e| map_indexdb_error("version_state insert", e))?;
                restored_ids.push((*version_id).clone());
            }

            if restored_ids.is_empty() {
                continue;
            }

            match current_head.as_ref() {
                Some(head) if head.state == ObjectState::Trash => {
                    if restored_ids
                        .iter()
                        .any(|version_id| version_id == &head.version_id)
                    {
                        let next_head = head.with_state(
                            ObjectState::Live,
                            next_head_generation(current_head.as_ref()),
                            partition_meta.updated_at,
                        );
                        write_head_with_namespace_exact(
                            &mut heads,
                            &mut live_namespace,
                            &next_head,
                        )?;
                    } else if let Some(metadata) = latest_restored.as_ref() {
                        let next_head = HeadRecord::from_metadata(
                            metadata,
                            ObjectState::Live,
                            next_head_generation(current_head.as_ref()),
                            partition_meta.updated_at,
                        );
                        write_head_with_namespace_exact(
                            &mut heads,
                            &mut live_namespace,
                            &next_head,
                        )?;
                    }
                }
                None => {
                    if let Some(metadata) = latest_restored.as_ref() {
                        let next_head = HeadRecord::from_metadata(
                            metadata,
                            ObjectState::Live,
                            1,
                            partition_meta.updated_at,
                        );
                        write_head_with_namespace_exact(
                            &mut heads,
                            &mut live_namespace,
                            &next_head,
                        )?;
                    }
                }
                _ => {}
            }

            let trash_key = canonical_key(object_key);
            let maybe_existing = trash_objects
                .get(trash_key.as_str())
                .map_err(|e| map_indexdb_error("trash_objects get", e))?
                .map(|value| deserialize::<TrashObjectRecord>(value.value()))
                .transpose()?;
            if let Some(mut existing) = maybe_existing {
                existing.version_ids.retain(|version_id| {
                    !restored_ids.iter().any(|candidate| candidate == version_id)
                });
                if existing.version_ids.is_empty() {
                    let _ = trash_objects
                        .remove(trash_key.as_str())
                        .map_err(|e| map_indexdb_error("trash_objects remove", e))?;
                } else {
                    let bytes = serialize(&existing)?;
                    trash_objects
                        .insert(trash_key.as_str(), bytes.as_slice())
                        .map_err(|e| map_indexdb_error("trash_objects insert", e))?;
                }
            }

            restored_ids.sort_by(|left, right| compare_version_id_recency(left, right));
            touched.insert(object_key.clone(), restored_ids);
        }

        touched
    };
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit manifest trash restore", e))?;
    Ok(touched)
}

fn restore_trash_entries_on_db(
    db: &Database,
    fragment: &str,
    object_versions: &HashMap<String, Vec<String>>,
) -> Result<(HashMap<String, Vec<String>>, PartitionMeta), AppError> {
    if object_versions.is_empty() {
        return Ok((
            HashMap::new(),
            PartitionMeta {
                committed_generation: 0,
                updated_at: 0,
            },
        ));
    }
    let existing_meta = ensure_partition_meta_exact(db)?;
    let partition_meta = PartitionMeta {
        committed_generation: existing_meta.committed_generation.saturating_add(1),
        updated_at: Utc::now().timestamp(),
    };
    let touched =
        apply_restore_trash_entries_exact(db, fragment, object_versions, &partition_meta, false)?;
    Ok((touched, partition_meta))
}

fn apply_restore_trash_entries_replica_exact(
    db: &Database,
    fragment: &str,
    object_versions: &HashMap<String, Vec<String>>,
    partition_meta: &PartitionMeta,
) -> Result<(), AppError> {
    let _ = apply_restore_trash_entries_exact(db, fragment, object_versions, partition_meta, true)?;
    Ok(())
}

fn delete_all_versions_for_key_on_db(
    db: &Database,
    fragment: &str,
    object_key: &str,
) -> Result<Option<(Vec<String>, PartitionMeta)>, AppError> {
    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    let (version_ids, partition_meta) = {
        let mut partition_meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let mut heads = write_txn
            .open_table(OBJECT_HEADS_TABLE)
            .map_err(|e| map_indexdb_error("open object_heads", e))?;
        let mut live_namespace = write_txn
            .open_table(LIVE_NAMESPACE_TABLE)
            .map_err(|e| map_indexdb_error("open live_namespace", e))?;
        let mut versions = write_txn
            .open_table(OBJECT_VERSIONS_TABLE)
            .map_err(|e| map_indexdb_error("open object_versions", e))?;
        let mut states = write_txn
            .open_table(OBJECT_VERSION_STATES_TABLE)
            .map_err(|e| map_indexdb_error("open object_version_states", e))?;
        let mut trash_objects = write_txn
            .open_table(TRASH_OBJECTS_TABLE)
            .map_err(|e| map_indexdb_error("open trash_objects", e))?;
        let now = Utc::now().timestamp();
        let committed_generation = next_partition_generation(&mut partition_meta, now)?;
        let prefix = version_row_prefix(fragment, object_key)?;
        let mut iter = versions
            .range(prefix.as_str()..)
            .map_err(|e| map_indexdb_error("version range", e))?;
        let mut row_keys = Vec::new();
        let mut version_ids = Vec::new();
        while let Some(entry) = iter.next() {
            let (row_key, _value) =
                entry.map_err(|e| map_indexdb_error("version range next", e))?;
            let row_key = row_key.value();
            if !row_key.starts_with(&prefix) {
                break;
            }
            if let Some((_, version_id)) = parse_version_key(row_key) {
                version_ids.push(version_id.to_string());
            }
            row_keys.push(row_key.to_string());
        }
        if row_keys.is_empty() {
            return Ok(None);
        }
        for row_key in &row_keys {
            let _ = versions
                .remove(row_key.as_str())
                .map_err(|e| map_indexdb_error("version remove", e))?;
            let _ = states
                .remove(row_key.as_str())
                .map_err(|e| map_indexdb_error("version_state remove", e))?;
        }
        remove_head_with_namespace_exact(&mut heads, &mut live_namespace, fragment, object_key)?;
        let _ = trash_objects
            .remove(canonical_key(object_key).as_str())
            .map_err(|e| map_indexdb_error("trash_objects remove", e))?;
        (
            version_ids,
            PartitionMeta {
                committed_generation,
                updated_at: now,
            },
        )
    };
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit delete object", e))?;
    let mut version_ids = version_ids;
    version_ids.sort_by(|left, right| compare_version_id_recency(left, right));
    Ok(Some((version_ids, partition_meta)))
}

fn delete_versions_for_key_with_state_on_db(
    db: &Database,
    fragment: &str,
    object_key: &str,
    target_state: ObjectState,
) -> Result<Option<(Vec<String>, PartitionMeta)>, AppError> {
    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    let (version_ids, partition_meta) = {
        let mut partition_meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let mut heads = write_txn
            .open_table(OBJECT_HEADS_TABLE)
            .map_err(|e| map_indexdb_error("open object_heads", e))?;
        let mut live_namespace = write_txn
            .open_table(LIVE_NAMESPACE_TABLE)
            .map_err(|e| map_indexdb_error("open live_namespace", e))?;
        let mut versions = write_txn
            .open_table(OBJECT_VERSIONS_TABLE)
            .map_err(|e| map_indexdb_error("open object_versions", e))?;
        let mut states = write_txn
            .open_table(OBJECT_VERSION_STATES_TABLE)
            .map_err(|e| map_indexdb_error("open object_version_states", e))?;
        let mut trash_objects = write_txn
            .open_table(TRASH_OBJECTS_TABLE)
            .map_err(|e| map_indexdb_error("open trash_objects", e))?;
        let now = Utc::now().timestamp();
        let committed_generation = next_partition_generation(&mut partition_meta, now)?;
        let current_head = heads
            .get(head_row_key(fragment, object_key)?.as_str())
            .map_err(|e| map_indexdb_error("head get", e))?
            .map(|value| deserialize::<HeadRecord>(value.value()))
            .transpose()?;
        let prefix = version_row_prefix(fragment, object_key)?;
        let mut iter = versions
            .range(prefix.as_str()..)
            .map_err(|e| map_indexdb_error("version range", e))?;
        let mut row_keys_to_delete = Vec::new();
        let mut version_ids = Vec::new();
        while let Some(entry) = iter.next() {
            let (row_key, _value) =
                entry.map_err(|e| map_indexdb_error("version range next", e))?;
            let row_key = row_key.value();
            if !row_key.starts_with(&prefix) {
                break;
            }
            let Some(state_value) = states
                .get(row_key)
                .map_err(|e| map_indexdb_error("version_state get", e))?
            else {
                continue;
            };
            let state = deserialize::<VersionStateRecord>(state_value.value())?.state;
            if state == target_state {
                if let Some((_, version_id)) = parse_version_key(row_key) {
                    version_ids.push(version_id.to_string());
                }
                row_keys_to_delete.push(row_key.to_string());
            }
        }

        if row_keys_to_delete.is_empty() {
            return Ok(None);
        }

        for row_key in &row_keys_to_delete {
            let _ = versions
                .remove(row_key.as_str())
                .map_err(|e| map_indexdb_error("version remove", e))?;
            let _ = states
                .remove(row_key.as_str())
                .map_err(|e| map_indexdb_error("version_state remove", e))?;
        }

        rewrite_head_from_remaining_versions_exact(
            &mut heads,
            &mut live_namespace,
            &versions,
            &states,
            fragment,
            object_key,
            current_head.as_ref(),
            now,
        )?;

        if target_state == ObjectState::Trash {
            let trash_key = canonical_key(object_key);
            let _ = trash_objects
                .remove(trash_key.as_str())
                .map_err(|e| map_indexdb_error("trash_objects remove", e))?;
        }

        (
            version_ids,
            PartitionMeta {
                committed_generation,
                updated_at: now,
            },
        )
    };
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit delete versions by state", e))?;
    let mut version_ids = version_ids;
    version_ids.sort_by(|left, right| compare_version_id_recency(left, right));
    Ok(Some((version_ids, partition_meta)))
}

fn receipt_from_part(part: &PartMetadata) -> Result<SessionPartReceipt, AppError> {
    let mut shard_checksums = Vec::with_capacity(part.shards.len());
    for shard in &part.shards {
        let checksum = u32::from_str_radix(shard.checksum.trim(), 16).unwrap_or_default();
        shard_checksums.push(checksum);
    }
    let checksum = part
        .checksum
        .as_deref()
        .and_then(|value| u32::from_str_radix(value.trim(), 16).ok());
    Ok(SessionPartReceipt {
        part_index: part.part_index,
        size_bytes: part.size_bytes,
        shard_size_bytes: part
            .shards
            .first()
            .map(|shard| shard.size_bytes)
            .unwrap_or(0),
        created_at: part.created_at,
        checksum,
        shard_checksums,
    })
}

fn load_session_row_exact(db: &Database, version_id: &str) -> Result<Option<SessionRow>, AppError> {
    let read_txn = db
        .begin_read()
        .map_err(|e| map_indexdb_error("begin_read", e))?;
    let table = match read_txn.open_table(SESSIONS_TABLE) {
        Ok(table) => table,
        Err(TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(err) => return Err(map_indexdb_error("open sessions", err)),
    };
    let Some(value) = table
        .get(version_id)
        .map_err(|e| map_indexdb_error("session get", e))?
    else {
        return Ok(None);
    };
    deserialize(value.value()).map(Some)
}

fn load_session_exact(
    db: &Database,
    version_id: &str,
) -> Result<Option<IndexSessionState>, AppError> {
    Ok(load_session_row_exact(db, version_id)?.map(SessionRow::into_state))
}

fn write_partition_meta_exact(
    meta: &mut indexdb::Table<&str, &[u8]>,
    partition_meta: &PartitionMeta,
) -> Result<(), AppError> {
    let bytes = serialize(partition_meta)?;
    meta.insert(PARTITION_META_KEY, bytes.as_slice())
        .map_err(|e| map_indexdb_error("partition_meta insert", e))?;
    Ok(())
}

fn write_session_mutation_exact(
    db: &Database,
    version_id: &str,
    row: &SessionRow,
    part: Option<&PartMetadata>,
    receipt: Option<&SessionPartReceipt>,
) -> Result<PartitionMeta, AppError> {
    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    let now = Utc::now().timestamp();
    let partition_meta = {
        let mut meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let committed_generation = next_partition_generation(&mut meta, now)?;
        let mut sessions = write_txn
            .open_table(SESSIONS_TABLE)
            .map_err(|e| map_indexdb_error("open sessions", e))?;
        let mut parts = write_txn
            .open_table(SESSION_PARTS_TABLE)
            .map_err(|e| map_indexdb_error("open session_parts", e))?;
        let mut receipts = write_txn
            .open_table(SESSION_PART_RECEIPTS_TABLE)
            .map_err(|e| map_indexdb_error("open session_part_receipts", e))?;
        let row_bytes = serialize(row)?;
        sessions
            .insert(version_id, row_bytes.as_slice())
            .map_err(|e| map_indexdb_error("session insert", e))?;
        if let Some(part) = part {
            let part_bytes = serialize(part)?;
            let part_key = session_part_row_key(version_id, part.part_index);
            parts
                .insert(part_key.as_str(), part_bytes.as_slice())
                .map_err(|e| map_indexdb_error("session_part insert", e))?;
        }
        if let Some(receipt) = receipt {
            let receipt_bytes = serialize(receipt)?;
            let receipt_key = session_part_row_key(version_id, receipt.part_index);
            receipts
                .insert(receipt_key.as_str(), receipt_bytes.as_slice())
                .map_err(|e| map_indexdb_error("session_part_receipt insert", e))?;
        }
        PartitionMeta {
            committed_generation,
            updated_at: now,
        }
    };
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit session mutation", e))?;
    Ok(partition_meta)
}

fn apply_session_mutation_replica_exact(
    db: &Database,
    version_id: &str,
    row: &SessionRow,
    part: Option<&PartMetadata>,
    receipt: Option<&SessionPartReceipt>,
    partition_meta: &PartitionMeta,
) -> Result<(), AppError> {
    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    {
        let mut meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let mut sessions = write_txn
            .open_table(SESSIONS_TABLE)
            .map_err(|e| map_indexdb_error("open sessions", e))?;
        let mut parts = write_txn
            .open_table(SESSION_PARTS_TABLE)
            .map_err(|e| map_indexdb_error("open session_parts", e))?;
        let mut receipts = write_txn
            .open_table(SESSION_PART_RECEIPTS_TABLE)
            .map_err(|e| map_indexdb_error("open session_part_receipts", e))?;
        write_partition_meta_exact(&mut meta, partition_meta)?;
        let row_bytes = serialize(row)?;
        sessions
            .insert(version_id, row_bytes.as_slice())
            .map_err(|e| map_indexdb_error("session insert", e))?;
        if let Some(part) = part {
            let part_bytes = serialize(part)?;
            let part_key = session_part_row_key(version_id, part.part_index);
            parts
                .insert(part_key.as_str(), part_bytes.as_slice())
                .map_err(|e| map_indexdb_error("session_part insert", e))?;
        }
        if let Some(receipt) = receipt {
            let receipt_bytes = serialize(receipt)?;
            let receipt_key = session_part_row_key(version_id, receipt.part_index);
            receipts
                .insert(receipt_key.as_str(), receipt_bytes.as_slice())
                .map_err(|e| map_indexdb_error("session_part_receipt insert", e))?;
        }
    }
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit session replica write", e))?;
    Ok(())
}

fn list_prefixed_keys(
    db: &Database,
    table_def: TableDefinition<&str, &[u8]>,
    prefix: &str,
) -> Result<Vec<String>, AppError> {
    let read_txn = db
        .begin_read()
        .map_err(|e| map_indexdb_error("begin_read", e))?;
    let table = match read_txn.open_table(table_def) {
        Ok(table) => table,
        Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(err) => return Err(map_indexdb_error("open prefixed table", err)),
    };
    let mut iter = table
        .range(prefix..)
        .map_err(|e| map_indexdb_error("prefixed range", e))?;
    let mut keys = Vec::new();
    while let Some(entry) = iter.next() {
        let (key, _value) = entry.map_err(|e| map_indexdb_error("prefixed range next", e))?;
        let key = key.value();
        if !key.starts_with(prefix) {
            break;
        }
        keys.push(key.to_string());
    }
    Ok(keys)
}

fn load_session_receipts_exact(
    db: &Database,
    version_id: &str,
) -> Result<Vec<SessionPartReceipt>, AppError> {
    let prefix = session_part_row_prefix(version_id);
    let read_txn = db
        .begin_read()
        .map_err(|e| map_indexdb_error("begin_read", e))?;
    let table = match read_txn.open_table(SESSION_PART_RECEIPTS_TABLE) {
        Ok(table) => table,
        Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(err) => return Err(map_indexdb_error("open session_part_receipts", err)),
    };
    let mut iter = table
        .range(prefix.as_str()..)
        .map_err(|e| map_indexdb_error("session_part_receipt range", e))?;
    let mut receipts = Vec::new();
    while let Some(entry) = iter.next() {
        let (key, value) =
            entry.map_err(|e| map_indexdb_error("session_part_receipt range next", e))?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        receipts.push(deserialize(value.value())?);
    }
    receipts.sort_by_key(|receipt: &SessionPartReceipt| receipt.part_index);
    Ok(receipts)
}

fn load_session_parts_exact(
    db: &Database,
    version_id: &str,
) -> Result<Vec<PartMetadata>, AppError> {
    let prefix = session_part_row_prefix(version_id);
    let read_txn = db
        .begin_read()
        .map_err(|e| map_indexdb_error("begin_read", e))?;
    let table = match read_txn.open_table(SESSION_PARTS_TABLE) {
        Ok(table) => table,
        Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(err) => return Err(map_indexdb_error("open session_parts", err)),
    };
    let mut iter = table
        .range(prefix.as_str()..)
        .map_err(|e| map_indexdb_error("session_part range", e))?;
    let mut parts = Vec::new();
    while let Some(entry) = iter.next() {
        let (key, value) = entry.map_err(|e| map_indexdb_error("session_part range next", e))?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        parts.push(deserialize(value.value())?);
    }
    parts.sort_by_key(|part: &PartMetadata| part.part_index);
    Ok(parts)
}

fn remove_session_exact(
    db: &Database,
    version_id: &str,
) -> Result<Option<SessionRemoveMutation>, AppError> {
    let row = load_session_row_exact(db, version_id)?;
    let expected_parts = row.as_ref().map(|row| row.manifest.expected_parts);
    let has_row = row.is_some();
    let (part_keys, receipt_keys) = if expected_parts.is_some() {
        (Vec::new(), Vec::new())
    } else {
        let part_prefix = session_part_row_prefix(version_id);
        (
            list_prefixed_keys(db, SESSION_PARTS_TABLE, &part_prefix)?,
            list_prefixed_keys(db, SESSION_PART_RECEIPTS_TABLE, &part_prefix)?,
        )
    };
    if !has_row && part_keys.is_empty() && receipt_keys.is_empty() {
        return Ok(None);
    }

    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    let mutation = {
        let mut meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let mut sessions = write_txn
            .open_table(SESSIONS_TABLE)
            .map_err(|e| map_indexdb_error("open sessions", e))?;
        let mut parts = write_txn
            .open_table(SESSION_PARTS_TABLE)
            .map_err(|e| map_indexdb_error("open session_parts", e))?;
        let mut receipts = write_txn
            .open_table(SESSION_PART_RECEIPTS_TABLE)
            .map_err(|e| map_indexdb_error("open session_part_receipts", e))?;
        let now = Utc::now().timestamp();
        let committed_generation = next_partition_generation(&mut meta, now)?;
        let _ = sessions
            .remove(version_id)
            .map_err(|e| map_indexdb_error("session remove", e))?;
        if let Some(expected_parts) = expected_parts {
            for part_index in 0..expected_parts {
                let part_key = session_part_row_key(version_id, part_index);
                let _ = parts
                    .remove(part_key.as_str())
                    .map_err(|e| map_indexdb_error("session_part remove", e))?;
                let _ = receipts
                    .remove(part_key.as_str())
                    .map_err(|e| map_indexdb_error("session_part_receipt remove", e))?;
            }
        } else {
            for key in part_keys {
                let _ = parts
                    .remove(key.as_str())
                    .map_err(|e| map_indexdb_error("session_part remove", e))?;
            }
            for key in receipt_keys {
                let _ = receipts
                    .remove(key.as_str())
                    .map_err(|e| map_indexdb_error("session_part_receipt remove", e))?;
            }
        }
        SessionRemoveMutation {
            partition_meta: PartitionMeta {
                committed_generation,
                updated_at: now,
            },
            expected_parts,
        }
    };
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit session remove", e))?;
    Ok(Some(mutation))
}

fn apply_session_remove_replica_exact(
    db: &Database,
    version_id: &str,
    mutation: &SessionRemoveMutation,
) -> Result<(), AppError> {
    let (part_keys, receipt_keys) = if mutation.expected_parts.is_some() {
        (Vec::new(), Vec::new())
    } else {
        let part_prefix = session_part_row_prefix(version_id);
        (
            list_prefixed_keys(db, SESSION_PARTS_TABLE, &part_prefix)?,
            list_prefixed_keys(db, SESSION_PART_RECEIPTS_TABLE, &part_prefix)?,
        )
    };
    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    {
        let mut meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let mut sessions = write_txn
            .open_table(SESSIONS_TABLE)
            .map_err(|e| map_indexdb_error("open sessions", e))?;
        let mut parts = write_txn
            .open_table(SESSION_PARTS_TABLE)
            .map_err(|e| map_indexdb_error("open session_parts", e))?;
        let mut receipts = write_txn
            .open_table(SESSION_PART_RECEIPTS_TABLE)
            .map_err(|e| map_indexdb_error("open session_part_receipts", e))?;
        write_partition_meta_exact(&mut meta, &mutation.partition_meta)?;
        let _ = sessions
            .remove(version_id)
            .map_err(|e| map_indexdb_error("session remove", e))?;
        if let Some(expected_parts) = mutation.expected_parts {
            for part_index in 0..expected_parts {
                let part_key = session_part_row_key(version_id, part_index);
                let _ = parts
                    .remove(part_key.as_str())
                    .map_err(|e| map_indexdb_error("session_part remove", e))?;
                let _ = receipts
                    .remove(part_key.as_str())
                    .map_err(|e| map_indexdb_error("session_part_receipt remove", e))?;
            }
        } else {
            for key in part_keys {
                let _ = parts
                    .remove(key.as_str())
                    .map_err(|e| map_indexdb_error("session_part remove", e))?;
            }
            for key in receipt_keys {
                let _ = receipts
                    .remove(key.as_str())
                    .map_err(|e| map_indexdb_error("session_part_receipt remove", e))?;
            }
        }
    }
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit session replica remove", e))?;
    Ok(())
}

fn replicate_object_mutation<F>(
    fragment: &str,
    route: &IndexRoute,
    context: &str,
    apply: F,
) -> Result<(), AppError>
where
    F: Fn(&Database) -> Result<(), AppError>,
{
    if route.roots.len() <= 1 {
        return Ok(());
    }

    let owner_path = owner_db_path(route, fragment);
    if !owner_path.exists() {
        return Err(AppError::InternalServerError(format!(
            "owner partition db missing after {}: {}",
            context,
            owner_path.display()
        )));
    }

    for replica_root in route.roots.iter().skip(1) {
        let replica_path = db_path(replica_root, fragment, route.partition_id);
        let apply_result = {
            let replica_db = match get_or_open_db(&replica_path) {
                Ok(db) => db,
                Err(err) => {
                    warn!(
                        "object replica open failed for {} during {}: {}",
                        replica_path.display(),
                        context,
                        err
                    );
                    evict_cached_db(&replica_path);
                    if let Err(copy_err) = atomic_copy_db_file(&owner_path, &replica_path) {
                        warn!(
                            "object replica catch-up failed for {} -> {} after open failure: {}",
                            owner_path.display(),
                            replica_path.display(),
                            copy_err
                        );
                    }
                    continue;
                }
            };
            let result = apply(&replica_db);
            drop(replica_db);
            result
        };

        if let Err(err) = apply_result {
            warn!(
                "object replica logical apply failed for {} during {}: {}; falling back to owner copy",
                replica_path.display(),
                context,
                err
            );
            evict_cached_db(&replica_path);
            if let Err(copy_err) = atomic_copy_db_file(&owner_path, &replica_path) {
                warn!(
                    "object replica catch-up failed for {} -> {}: {}",
                    owner_path.display(),
                    replica_path.display(),
                    copy_err
                );
            }
        }
    }

    Ok(())
}

fn apply_write_version_replica_exact(
    db: &Database,
    mutation: &VersionUpsertMutation,
) -> Result<(), AppError> {
    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    {
        let mut meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let mut heads = write_txn
            .open_table(OBJECT_HEADS_TABLE)
            .map_err(|e| map_indexdb_error("open object_heads", e))?;
        let mut live_namespace = write_txn
            .open_table(LIVE_NAMESPACE_TABLE)
            .map_err(|e| map_indexdb_error("open live_namespace", e))?;
        let mut versions = write_txn
            .open_table(OBJECT_VERSIONS_TABLE)
            .map_err(|e| map_indexdb_error("open object_versions", e))?;
        let mut states = write_txn
            .open_table(OBJECT_VERSION_STATES_TABLE)
            .map_err(|e| map_indexdb_error("open object_version_states", e))?;
        write_partition_meta_exact(&mut meta, &mutation.partition_meta)?;
        versions
            .insert(mutation.row_key.as_str(), mutation.version_bytes.as_slice())
            .map_err(|e| map_indexdb_error("version insert", e))?;
        states
            .insert(mutation.row_key.as_str(), mutation.state_bytes.as_slice())
            .map_err(|e| map_indexdb_error("version_state insert", e))?;
        if let Some(head_bytes) = mutation.head_bytes.as_ref() {
            let head: HeadRecord = deserialize(head_bytes)?;
            heads
                .insert(mutation.head_key.as_str(), head_bytes.as_slice())
                .map_err(|e| map_indexdb_error("head insert", e))?;
            sync_live_namespace_from_head_exact(&mut live_namespace, &head)?;
        }
    }
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit version replica write", e))?;
    Ok(())
}

fn apply_change_version_state_replica_exact(
    db: &Database,
    fragment: &str,
    object_key: &str,
    version_id: &str,
    target_state: ObjectState,
    expected_state: Option<ObjectState>,
    partition_meta: &PartitionMeta,
) -> Result<(), AppError> {
    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    {
        let mut meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let mut heads = write_txn
            .open_table(OBJECT_HEADS_TABLE)
            .map_err(|e| map_indexdb_error("open object_heads", e))?;
        let mut live_namespace = write_txn
            .open_table(LIVE_NAMESPACE_TABLE)
            .map_err(|e| map_indexdb_error("open live_namespace", e))?;
        let versions = write_txn
            .open_table(OBJECT_VERSIONS_TABLE)
            .map_err(|e| map_indexdb_error("open object_versions", e))?;
        let mut states = write_txn
            .open_table(OBJECT_VERSION_STATES_TABLE)
            .map_err(|e| map_indexdb_error("open object_version_states", e))?;
        write_partition_meta_exact(&mut meta, partition_meta)?;
        let row_key = version_row_key(fragment, object_key, version_id)?;
        let current_head = heads
            .get(head_row_key(fragment, object_key)?.as_str())
            .map_err(|e| map_indexdb_error("head get", e))?
            .map(|value| deserialize::<HeadRecord>(value.value()))
            .transpose()?;
        let metadata = {
            let Some(existing) = versions
                .get(row_key.as_str())
                .map_err(|e| map_indexdb_error("version get", e))?
            else {
                return Err(AppError::InternalServerError(format!(
                    "replica missing version row for {}:{}",
                    object_key, version_id
                )));
            };
            deserialize::<VersionRecord>(existing.value())?.metadata
        };
        let existing_state = {
            let Some(existing) = states
                .get(row_key.as_str())
                .map_err(|e| map_indexdb_error("version_state get", e))?
            else {
                return Err(AppError::InternalServerError(format!(
                    "replica missing version state for {}:{}",
                    object_key, version_id
                )));
            };
            deserialize::<VersionStateRecord>(existing.value())?
        };
        if let Some(expected) = expected_state {
            if existing_state.state != expected {
                return Err(AppError::InternalServerError(format!(
                    "replica version state mismatch for {}:{} expected {:?} got {:?}",
                    object_key, version_id, expected, existing_state.state
                )));
            }
        }
        let next_state = VersionStateRecord {
            state: target_state,
            updated_at: partition_meta.updated_at,
            generation: partition_meta.committed_generation,
        };
        let bytes = serialize(&next_state)?;
        states
            .insert(row_key.as_str(), bytes.as_slice())
            .map_err(|e| map_indexdb_error("version_state insert", e))?;
        if let Some(head) = current_head.as_ref() {
            if head.version_id == version_id {
                let next_head = HeadRecord::from_metadata(
                    &metadata,
                    target_state,
                    next_head_generation(current_head.as_ref()),
                    partition_meta.updated_at,
                );
                write_head_with_namespace_exact(&mut heads, &mut live_namespace, &next_head)?;
            } else if target_state == ObjectState::Live
                && expected_state == Some(ObjectState::Trash)
            {
                let restored_head = HeadRecord::from_metadata(
                    &metadata,
                    ObjectState::Live,
                    next_head_generation(current_head.as_ref()),
                    partition_meta.updated_at,
                );
                write_head_with_namespace_exact(&mut heads, &mut live_namespace, &restored_head)?;
            }
        } else if target_state == ObjectState::Live && expected_state == Some(ObjectState::Trash) {
            let restored_head = HeadRecord::from_metadata(
                &metadata,
                ObjectState::Live,
                1,
                partition_meta.updated_at,
            );
            write_head_with_namespace_exact(&mut heads, &mut live_namespace, &restored_head)?;
        }
    }
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit version state replica write", e))?;
    Ok(())
}

fn apply_delete_version_replica_exact(
    db: &Database,
    fragment: &str,
    object_key: &str,
    version_id: &str,
    expected_state: Option<ObjectState>,
    partition_meta: &PartitionMeta,
) -> Result<(), AppError> {
    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    {
        let mut meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let mut heads = write_txn
            .open_table(OBJECT_HEADS_TABLE)
            .map_err(|e| map_indexdb_error("open object_heads", e))?;
        let mut live_namespace = write_txn
            .open_table(LIVE_NAMESPACE_TABLE)
            .map_err(|e| map_indexdb_error("open live_namespace", e))?;
        let mut versions = write_txn
            .open_table(OBJECT_VERSIONS_TABLE)
            .map_err(|e| map_indexdb_error("open object_versions", e))?;
        let mut states = write_txn
            .open_table(OBJECT_VERSION_STATES_TABLE)
            .map_err(|e| map_indexdb_error("open object_version_states", e))?;
        write_partition_meta_exact(&mut meta, partition_meta)?;
        let row_key = version_row_key(fragment, object_key, version_id)?;
        let current_head = heads
            .get(head_row_key(fragment, object_key)?.as_str())
            .map_err(|e| map_indexdb_error("head get", e))?
            .map(|value| deserialize::<HeadRecord>(value.value()))
            .transpose()?;
        let existing_state = states
            .get(row_key.as_str())
            .map_err(|e| map_indexdb_error("version_state get", e))?
            .map(|value| deserialize::<VersionStateRecord>(value.value()))
            .transpose()?
            .ok_or_else(|| {
                AppError::InternalServerError(format!(
                    "replica missing version state for {}:{}",
                    object_key, version_id
                ))
            })?;
        if expected_state.is_some() && expected_state != Some(existing_state.state) {
            return Err(AppError::InternalServerError(format!(
                "replica version state mismatch for {}:{} expected {:?} got {:?}",
                object_key, version_id, expected_state, existing_state.state
            )));
        }
        {
            let Some(existing_value) = versions
                .remove(row_key.as_str())
                .map_err(|e| map_indexdb_error("version remove", e))?
            else {
                return Err(AppError::InternalServerError(format!(
                    "replica missing version row for {}:{}",
                    object_key, version_id
                )));
            };
            let _ = deserialize::<VersionRecord>(existing_value.value())?;
        };
        let _ = states
            .remove(row_key.as_str())
            .map_err(|e| map_indexdb_error("version_state remove", e))?;
        rewrite_head_from_remaining_versions_exact(
            &mut heads,
            &mut live_namespace,
            &versions,
            &states,
            fragment,
            object_key,
            current_head.as_ref(),
            partition_meta.updated_at,
        )?;
    }
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit version replica delete", e))?;
    Ok(())
}

fn apply_bulk_rewrite_keys_state_replica_exact(
    db: &Database,
    fragment: &str,
    object_keys: &[String],
    from_state: ObjectState,
    to_state: ObjectState,
    partition_meta: &PartitionMeta,
) -> Result<(), AppError> {
    let plans = {
        let read_txn = db
            .begin_read()
            .map_err(|e| map_indexdb_error("begin_read", e))?;
        let heads = read_txn
            .open_table(OBJECT_HEADS_TABLE)
            .map_err(|e| map_indexdb_error("open object_heads", e))?;
        let states = read_txn
            .open_table(OBJECT_VERSION_STATES_TABLE)
            .map_err(|e| map_indexdb_error("open object_version_states", e))?;
        let mut plans = Vec::<(String, Option<HeadRecord>, Vec<String>)>::new();

        for object_key in object_keys {
            let current_head = heads
                .get(head_row_key(fragment, object_key)?.as_str())
                .map_err(|e| map_indexdb_error("head get", e))?
                .map(|value| deserialize::<HeadRecord>(value.value()))
                .transpose()?;
            let prefix = version_row_prefix(fragment, object_key)?;
            let mut iter = states
                .range(prefix.as_str()..)
                .map_err(|e| map_indexdb_error("version_state range", e))?;
            let mut row_keys = Vec::new();
            while let Some(entry) = iter.next() {
                let (row_key, value) =
                    entry.map_err(|e| map_indexdb_error("version_state range next", e))?;
                let row_key = row_key.value();
                if !row_key.starts_with(&prefix) {
                    break;
                }
                let record: VersionStateRecord = deserialize(value.value())?;
                if record.state != from_state {
                    continue;
                }
                row_keys.push(row_key.to_string());
            }
            if !row_keys.is_empty() {
                plans.push((object_key.clone(), current_head, row_keys));
            }
        }

        plans
    };

    if plans.is_empty() {
        return Err(AppError::InternalServerError(format!(
            "replica missing rewritable versions from {:?} to {:?}",
            from_state, to_state
        )));
    }

    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    {
        let mut meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let mut heads = write_txn
            .open_table(OBJECT_HEADS_TABLE)
            .map_err(|e| map_indexdb_error("open object_heads", e))?;
        let mut live_namespace = write_txn
            .open_table(LIVE_NAMESPACE_TABLE)
            .map_err(|e| map_indexdb_error("open live_namespace", e))?;
        let mut states = write_txn
            .open_table(OBJECT_VERSION_STATES_TABLE)
            .map_err(|e| map_indexdb_error("open object_version_states", e))?;
        write_partition_meta_exact(&mut meta, partition_meta)?;

        for (object_key, current_head, row_keys) in plans {
            for row_key in row_keys {
                let bytes = serialize(&VersionStateRecord {
                    state: to_state,
                    updated_at: partition_meta.updated_at,
                    generation: partition_meta.committed_generation,
                })?;
                states
                    .insert(row_key.as_str(), bytes.as_slice())
                    .map_err(|e| map_indexdb_error("bulk version_state rewrite", e))?;
            }

            match current_head.as_ref() {
                Some(head) if head.state == from_state => {
                    let next_head = head.with_state(
                        to_state,
                        next_head_generation(current_head.as_ref()),
                        partition_meta.updated_at,
                    );
                    write_head_with_namespace_exact(&mut heads, &mut live_namespace, &next_head)?;
                }
                Some(_) => {}
                None => {
                    return Err(AppError::InternalServerError(format!(
                        "replica missing head for {} while rewriting version visibility",
                        canonical_key(&object_key)
                    )));
                }
            }
        }
    }
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit bulk version replica rewrite", e))?;
    Ok(())
}

fn apply_delete_all_versions_for_key_replica_exact(
    db: &Database,
    fragment: &str,
    object_key: &str,
    partition_meta: &PartitionMeta,
) -> Result<(), AppError> {
    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    {
        let mut meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let mut heads = write_txn
            .open_table(OBJECT_HEADS_TABLE)
            .map_err(|e| map_indexdb_error("open object_heads", e))?;
        let mut live_namespace = write_txn
            .open_table(LIVE_NAMESPACE_TABLE)
            .map_err(|e| map_indexdb_error("open live_namespace", e))?;
        let mut versions = write_txn
            .open_table(OBJECT_VERSIONS_TABLE)
            .map_err(|e| map_indexdb_error("open object_versions", e))?;
        let mut states = write_txn
            .open_table(OBJECT_VERSION_STATES_TABLE)
            .map_err(|e| map_indexdb_error("open object_version_states", e))?;
        write_partition_meta_exact(&mut meta, partition_meta)?;
        let prefix = version_row_prefix(fragment, object_key)?;
        let mut iter = versions
            .range(prefix.as_str()..)
            .map_err(|e| map_indexdb_error("version range", e))?;
        let mut row_keys = Vec::new();
        while let Some(entry) = iter.next() {
            let (row_key, _value) =
                entry.map_err(|e| map_indexdb_error("version range next", e))?;
            let row_key = row_key.value();
            if !row_key.starts_with(&prefix) {
                break;
            }
            row_keys.push(row_key.to_string());
        }
        if row_keys.is_empty() {
            return Err(AppError::InternalServerError(format!(
                "replica missing versions for object key {}",
                object_key
            )));
        }
        for row_key in &row_keys {
            let _ = versions
                .remove(row_key.as_str())
                .map_err(|e| map_indexdb_error("version remove", e))?;
            let _ = states
                .remove(row_key.as_str())
                .map_err(|e| map_indexdb_error("version_state remove", e))?;
        }
        remove_head_with_namespace_exact(&mut heads, &mut live_namespace, fragment, object_key)?;
    }
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit delete object replica", e))?;
    Ok(())
}

fn apply_delete_versions_for_key_with_state_replica_exact(
    db: &Database,
    fragment: &str,
    object_key: &str,
    target_state: ObjectState,
    partition_meta: &PartitionMeta,
) -> Result<(), AppError> {
    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    {
        let mut meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let mut heads = write_txn
            .open_table(OBJECT_HEADS_TABLE)
            .map_err(|e| map_indexdb_error("open object_heads", e))?;
        let mut live_namespace = write_txn
            .open_table(LIVE_NAMESPACE_TABLE)
            .map_err(|e| map_indexdb_error("open live_namespace", e))?;
        let mut versions = write_txn
            .open_table(OBJECT_VERSIONS_TABLE)
            .map_err(|e| map_indexdb_error("open object_versions", e))?;
        let mut states = write_txn
            .open_table(OBJECT_VERSION_STATES_TABLE)
            .map_err(|e| map_indexdb_error("open object_version_states", e))?;
        write_partition_meta_exact(&mut meta, partition_meta)?;
        let current_head = heads
            .get(head_row_key(fragment, object_key)?.as_str())
            .map_err(|e| map_indexdb_error("head get", e))?
            .map(|value| deserialize::<HeadRecord>(value.value()))
            .transpose()?;
        let prefix = version_row_prefix(fragment, object_key)?;
        let mut iter = versions
            .range(prefix.as_str()..)
            .map_err(|e| map_indexdb_error("version range", e))?;
        let mut row_keys_to_delete = Vec::new();
        while let Some(entry) = iter.next() {
            let (row_key, _value) =
                entry.map_err(|e| map_indexdb_error("version range next", e))?;
            let row_key = row_key.value();
            if !row_key.starts_with(&prefix) {
                break;
            }
            let Some(state_value) = states
                .get(row_key)
                .map_err(|e| map_indexdb_error("version_state get", e))?
            else {
                continue;
            };
            let state = deserialize::<VersionStateRecord>(state_value.value())?.state;
            if state == target_state {
                row_keys_to_delete.push(row_key.to_string());
            }
        }

        if row_keys_to_delete.is_empty() {
            return Err(AppError::InternalServerError(format!(
                "replica missing {:?} versions for object key {}",
                target_state, object_key
            )));
        }

        for row_key in &row_keys_to_delete {
            let _ = versions
                .remove(row_key.as_str())
                .map_err(|e| map_indexdb_error("version remove", e))?;
            let _ = states
                .remove(row_key.as_str())
                .map_err(|e| map_indexdb_error("version_state remove", e))?;
        }

        rewrite_head_from_remaining_versions_exact(
            &mut heads,
            &mut live_namespace,
            &versions,
            &states,
            fragment,
            object_key,
            current_head.as_ref(),
            partition_meta.updated_at,
        )?;
    }
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit delete versions by state replica", e))?;
    Ok(())
}

fn max_string(left: Option<&str>, right: Option<&str>) -> Option<String> {
    match (left, right) {
        (Some(left), Some(right)) => Some(if left >= right { left } else { right }.to_string()),
        (Some(left), None) => Some(left.to_string()),
        (None, Some(right)) => Some(right.to_string()),
        (None, None) => None,
    }
}

fn normalize_prefix(prefix: Option<String>, delimiter: Option<&str>) -> Option<String> {
    let prefix = prefix
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    match (prefix, delimiter) {
        (value, Some("/")) | (value, _) => value,
    }
}

fn prefix_children_prefix(prefix: Option<&str>, delimiter: Option<&str>) -> Option<String> {
    match (prefix, delimiter) {
        (Some(prefix), Some("/")) if !prefix.ends_with('/') => Some(format!("{prefix}/")),
        _ => None,
    }
}

fn encode_cursor(cursor: &ListingCursor) -> Result<String, AppError> {
    let bytes = serde_json::to_vec(cursor)
        .map_err(|e| AppError::InternalServerError(format!("encode list cursor: {}", e)))?;
    Ok(general_purpose::STANDARD.encode(bytes))
}

fn decode_cursor(token: &str) -> Result<ListingCursor, AppError> {
    let bytes = general_purpose::STANDARD
        .decode(token)
        .map_err(|_| AppError::BadRequest("invalid listing continuation token".to_string()))?;
    serde_json::from_slice(&bytes)
        .map_err(|_| AppError::BadRequest("invalid listing continuation token".to_string()))
}

fn load_current_metadata_for_head(
    fragment: &str,
    head: &HeadRecord,
) -> Result<Option<ObjectMetadata>, AppError> {
    if head.state == ObjectState::Deleted {
        return Ok(None);
    }
    let route = object_route(fragment, &head.object_key)?;
    let Some(db) = open_route_db_for_read(fragment, &route)? else {
        return Ok(None);
    };
    let Some(record) = read_version_exact(&db, fragment, &head.object_key, &head.version_id)?
    else {
        return Ok(None);
    };
    Ok(Some(record.metadata))
}

fn collect_live_namespace_entries_on_db(
    db: &Database,
) -> Result<HashMap<String, ObjectMetadata>, AppError> {
    let read_txn = db
        .begin_read()
        .map_err(|e| map_indexdb_error("begin_read", e))?;
    let heads = match read_txn.open_table(OBJECT_HEADS_TABLE) {
        Ok(table) => table,
        Err(TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
        Err(err) => return Err(map_indexdb_error("open object_heads", err)),
    };
    let mut entries = HashMap::new();
    let mut iter = heads
        .iter()
        .map_err(|e| map_indexdb_error("head iter", e))?;
    while let Some(entry) = iter.next() {
        let (_key, value) = entry.map_err(|e| map_indexdb_error("head iter next", e))?;
        let head: HeadRecord = deserialize(value.value())?;
        if head.state == ObjectState::Live {
            entries.insert(canonical_key(&head.object_key), head.to_summary_metadata());
        }
    }
    Ok(entries)
}

fn rewrite_live_namespace_on_db(
    db: &Database,
    entries: &HashMap<String, ObjectMetadata>,
) -> Result<(), AppError> {
    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    {
        let mut table = write_txn
            .open_table(LIVE_NAMESPACE_TABLE)
            .map_err(|e| map_indexdb_error("open live_namespace", e))?;
        let mut existing_keys = Vec::new();
        let mut iter = table
            .iter()
            .map_err(|e| map_indexdb_error("live_namespace iter", e))?;
        while let Some(entry) = iter.next() {
            let (key, _) = entry.map_err(|e| map_indexdb_error("live_namespace iter next", e))?;
            existing_keys.push(key.value().to_string());
        }
        for key in existing_keys {
            let _ = table
                .remove(key.as_str())
                .map_err(|e| map_indexdb_error("live_namespace remove", e))?;
        }
        for (object_key, metadata) in entries {
            write_live_namespace_exact(&mut table, metadata)?;
            debug_assert_eq!(
                canonical_key(object_key),
                canonical_key(&metadata.object_key)
            );
        }
    }
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit live_namespace rewrite", e))?;
    Ok(())
}

fn live_namespace_needs_rebuild(db: &Database) -> Result<bool, AppError> {
    let read_txn = db
        .begin_read()
        .map_err(|e| map_indexdb_error("begin_read", e))?;
    let has_live_heads = match read_txn.open_table(OBJECT_HEADS_TABLE) {
        Ok(heads) => {
            let mut iter = heads
                .iter()
                .map_err(|e| map_indexdb_error("head iter", e))?;
            let mut found = false;
            while let Some(entry) = iter.next() {
                let (_key, value) = entry.map_err(|e| map_indexdb_error("head iter next", e))?;
                let head: HeadRecord = deserialize(value.value())?;
                if head.state == ObjectState::Live {
                    found = true;
                    break;
                }
            }
            found
        }
        Err(TableError::TableDoesNotExist(_)) => false,
        Err(err) => return Err(map_indexdb_error("open object_heads", err)),
    };
    match read_txn.open_table(LIVE_NAMESPACE_TABLE) {
        Ok(table) => {
            let mut iter = table
                .iter()
                .map_err(|e| map_indexdb_error("live_namespace iter", e))?;
            let namespace_empty = iter
                .next()
                .transpose()
                .map_err(|e| map_indexdb_error("live_namespace iter next", e))?
                .is_none();
            Ok(namespace_empty && has_live_heads)
        }
        Err(TableError::TableDoesNotExist(_)) => Ok(has_live_heads),
        Err(err) => Err(map_indexdb_error("open live_namespace", err)),
    }
}

fn ensure_fragment_live_namespace_table(fragment: &str) -> Result<(), AppError> {
    let route = fragment_route(fragment)?;
    let owner_path = owner_db_path(&route, fragment);
    if owner_path.exists() {
        let db = get_or_open_db(&owner_path)?;
        if live_namespace_needs_rebuild(&db)? {
            let entries = collect_live_namespace_entries_on_db(&db)?;
            rewrite_live_namespace_on_db(&db, &entries)?;
            replicate_owner_partition_db(fragment, &route)?;
        }
        return Ok(());
    }

    for root in route.roots.iter().skip(1) {
        let path = db_path(root, fragment, route.partition_id);
        if !path.exists() {
            continue;
        }
        let db = get_or_open_db(&path)?;
        if live_namespace_needs_rebuild(&db)? {
            let entries = collect_live_namespace_entries_on_db(&db)?;
            rewrite_live_namespace_on_db(&db, &entries)?;
        }
        break;
    }
    Ok(())
}

fn advance_namespace_partition(
    partition: &mut NamespacePartitionRange,
    prefix: Option<&str>,
    prefix_children: Option<&str>,
    include_internal: bool,
    after_key: Option<&str>,
) -> Result<(), AppError> {
    partition.next = None;
    while let Some(entry) = partition.iter.next() {
        let (key, value) = entry.map_err(|e| map_indexdb_error("live_namespace next", e))?;
        let key = key.value();
        if let Some(after) = after_key {
            if key <= after {
                continue;
            }
        }
        if let Some(prefix) = prefix {
            let matches_prefix = match prefix_children {
                Some(children_prefix) => key == prefix || key.starts_with(children_prefix),
                None => key.starts_with(prefix),
            };
            if !matches_prefix {
                if !key.starts_with(prefix) && key > prefix {
                    return Ok(());
                }
                continue;
            }
        }
        if !include_internal && is_internal_object_key(key) {
            continue;
        }
        let summary: ObjectMetadata = deserialize(value.value())?;
        partition.next = Some(NamespaceListingItem {
            object_key: key.to_string(),
            summary,
        });
        return Ok(());
    }
    Ok(())
}

fn load_live_namespace_partition_ranges(
    fragment: &str,
    prefix: Option<&str>,
    prefix_children: Option<&str>,
    include_internal: bool,
    after_key: Option<&str>,
) -> Result<Vec<NamespacePartitionRange>, AppError> {
    let start_hint = max_string(after_key, prefix);
    ensure_fragment_live_namespace_table(fragment)?;
    let Some(db) = open_fragment_db_for_read(fragment)? else {
        return Ok(Vec::new());
    };
    let read_txn = db
        .begin_read()
        .map_err(|e| map_indexdb_error("begin_read", e))?;
    let table = match read_txn.open_table(LIVE_NAMESPACE_TABLE) {
        Ok(table) => table,
        Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(err) => return Err(map_indexdb_error("open live_namespace", err)),
    };
    let start = start_hint.as_deref().unwrap_or("");
    let iter = table
        .range(start..)
        .map_err(|e| map_indexdb_error("live_namespace range", e))?;
    let mut partition = NamespacePartitionRange {
        _db: db,
        iter,
        next: None,
    };
    advance_namespace_partition(
        &mut partition,
        prefix,
        prefix_children,
        include_internal,
        after_key,
    )?;
    if partition.next.is_some() {
        Ok(vec![partition])
    } else {
        Ok(Vec::new())
    }
}

fn scan_live_namespace_page(
    fragment: &str,
    prefix: Option<String>,
    delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
    hydrate_full_metadata: bool,
) -> Result<ListObjectsResult, AppError> {
    let delimiter = delimiter
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if delimiter.as_deref() != Some("/") && delimiter.is_some() {
        return Err(AppError::BadRequest(
            "only '/' delimiter is supported".to_string(),
        ));
    }

    let normalized_prefix = normalize_prefix(prefix, delimiter.as_deref());
    let prefix_children =
        prefix_children_prefix(normalized_prefix.as_deref(), delimiter.as_deref());
    let cursor = continuation_token
        .map(|token| decode_cursor(&token))
        .transpose()?;
    if let Some(cursor) = &cursor {
        if cursor.state != ObjectState::Live
            || cursor.prefix != normalized_prefix
            || cursor.delimiter != delimiter
            || cursor.include_internal != include_internal
        {
            return Err(AppError::BadRequest(
                "listing continuation token does not match the current query".to_string(),
            ));
        }
    }

    let after_key = cursor.as_ref().and_then(|value| value.last_key.as_deref());
    let mut skip_prefix = cursor
        .as_ref()
        .and_then(|value| value.resume_skip_prefix.clone());
    let limit = limit.clamp(1, 1000);
    let mut partitions = load_live_namespace_partition_ranges(
        fragment,
        normalized_prefix.as_deref(),
        prefix_children.as_deref(),
        include_internal,
        after_key,
    )?;
    let mut objects = Vec::with_capacity(limit);
    let mut common_prefixes = Vec::new();
    let mut last_emitted_common_prefix: Option<String> = None;
    let mut last_key_for_cursor: Option<String> = None;
    let mut resume_skip_prefix: Option<String> = None;
    let mut page_full = false;

    loop {
        let Some((idx, current_key)) = partitions
            .iter()
            .enumerate()
            .filter_map(|(idx, partition)| {
                partition
                    .next
                    .as_ref()
                    .map(|item| (idx, item.object_key.clone()))
            })
            .min_by(|left, right| left.1.cmp(&right.1))
        else {
            break;
        };

        let item = partitions[idx].next.take().ok_or_else(|| {
            AppError::InternalServerError("namespace listing cursor desync".into())
        })?;

        if let Some(prefix) = skip_prefix.clone() {
            if item.object_key.starts_with(&prefix) {
                advance_namespace_partition(
                    &mut partitions[idx],
                    normalized_prefix.as_deref(),
                    prefix_children.as_deref(),
                    include_internal,
                    after_key,
                )?;
                continue;
            }
            skip_prefix = None;
        }

        last_key_for_cursor = Some(current_key.clone());

        if delimiter.as_deref() == Some("/") {
            let suffix = if let Some(prefix) = normalized_prefix.as_deref() {
                if item.object_key == prefix {
                    ""
                } else if let Some(children_prefix) = prefix_children.as_deref() {
                    item.object_key
                        .strip_prefix(children_prefix)
                        .unwrap_or_default()
                } else {
                    item.object_key.strip_prefix(prefix).unwrap_or_default()
                }
            } else {
                item.object_key.as_str()
            };
            if !suffix.is_empty() {
                if let Some(pos) = suffix.find('/') {
                    let common_prefix = format!(
                        "{}{}",
                        prefix_children
                            .as_deref()
                            .or(normalized_prefix.as_deref())
                            .unwrap_or(""),
                        &suffix[..pos + 1]
                    );
                    if last_emitted_common_prefix.as_deref() != Some(common_prefix.as_str()) {
                        common_prefixes.push(common_prefix.clone());
                        last_emitted_common_prefix = Some(common_prefix.clone());
                        skip_prefix = Some(common_prefix.clone());
                        if objects.len() + common_prefixes.len() >= limit {
                            resume_skip_prefix = Some(common_prefix);
                            page_full = true;
                            break;
                        }
                    }
                    advance_namespace_partition(
                        &mut partitions[idx],
                        normalized_prefix.as_deref(),
                        prefix_children.as_deref(),
                        include_internal,
                        after_key,
                    )?;
                    continue;
                }
            }
        }

        let metadata = if hydrate_full_metadata {
            match read_live_version(fragment, &item.summary.object_key, &item.summary.version_id)? {
                Some(metadata) => metadata,
                None => {
                    advance_namespace_partition(
                        &mut partitions[idx],
                        normalized_prefix.as_deref(),
                        prefix_children.as_deref(),
                        include_internal,
                        after_key,
                    )?;
                    continue;
                }
            }
        } else {
            item.summary
        };
        objects.push(metadata);
        if objects.len() + common_prefixes.len() >= limit {
            page_full = true;
            break;
        }

        advance_namespace_partition(
            &mut partitions[idx],
            normalized_prefix.as_deref(),
            prefix_children.as_deref(),
            include_internal,
            after_key,
        )?;
    }

    let continuation_token = if page_full {
        Some(encode_cursor(&ListingCursor {
            state: ObjectState::Live,
            prefix: normalized_prefix,
            delimiter,
            include_internal,
            last_key: last_key_for_cursor,
            resume_skip_prefix,
        })?)
    } else {
        None
    };

    Ok(ListObjectsResult {
        objects,
        common_prefixes,
        continuation_token,
    })
}

pub fn open_session(
    manifest: &UploadSessionManifest,
    reservation: Option<ReservationReceipt>,
) -> Result<(), AppError> {
    let route = session_route(&manifest.fragment, &manifest.version_id)?;
    let row = SessionRow {
        manifest: manifest.clone(),
        reservation,
        last_activity_at: manifest.created_at,
        finalized_at: None,
        aborted_at: None,
    };
    let owner_path = owner_db_path(&route, &manifest.fragment);
    with_partition_commit_lock(&owner_path, || {
        let owner_db = open_owner_db_for_write(&route, &manifest.fragment)?;
        let partition_meta =
            write_session_mutation_exact(&owner_db, &manifest.version_id, &row, None, None)?;
        replicate_session_mutation(
            &manifest.fragment,
            &route,
            &manifest.version_id,
            &row,
            None,
            None,
            &partition_meta,
        )?;
        Ok(())
    })
}

pub fn commit_session_part(
    fragment: &str,
    version_id: &str,
    part: &PartMetadata,
) -> Result<(), AppError> {
    let route = session_route(fragment, version_id)?;
    let owner_path = owner_db_path(&route, fragment);
    with_partition_commit_lock(&owner_path, || {
        let owner_db = open_owner_db_for_write(&route, fragment)?;
        let Some(mut row) = load_session_row_exact(&owner_db, version_id)? else {
            return Ok(());
        };
        row.last_activity_at = part.created_at.max(row.last_activity_at);
        let receipt = receipt_from_part(part)?;
        let partition_meta =
            write_session_mutation_exact(&owner_db, version_id, &row, Some(part), Some(&receipt))?;
        replicate_session_mutation(
            fragment,
            &route,
            version_id,
            &row,
            Some(part),
            Some(&receipt),
            &partition_meta,
        )?;
        Ok(())
    })
}

pub fn commit_session_part_receipt(
    fragment: &str,
    version_id: &str,
    receipt: &SessionPartReceipt,
) -> Result<(), AppError> {
    let route = session_route(fragment, version_id)?;
    let owner_path = owner_db_path(&route, fragment);
    with_partition_commit_lock(&owner_path, || {
        let owner_db = open_owner_db_for_write(&route, fragment)?;
        let Some(mut row) = load_session_row_exact(&owner_db, version_id)? else {
            return Ok(());
        };
        row.last_activity_at = receipt.created_at.max(row.last_activity_at);
        let partition_meta =
            write_session_mutation_exact(&owner_db, version_id, &row, None, Some(receipt))?;
        replicate_session_mutation(
            fragment,
            &route,
            version_id,
            &row,
            None,
            Some(receipt),
            &partition_meta,
        )?;
        Ok(())
    })
}

pub fn load_session(
    fragment: &str,
    version_id: &str,
) -> Result<Option<IndexSessionState>, AppError> {
    let route = session_route(fragment, version_id)?;
    let Some(db) = open_route_db_for_read(fragment, &route)? else {
        return Ok(None);
    };
    load_session_exact(&db, version_id)
}

pub fn read_session_part(
    fragment: &str,
    version_id: &str,
    part_index: u64,
) -> Result<Option<PartMetadata>, AppError> {
    let route = session_route(fragment, version_id)?;
    let Some(db) = open_route_db_for_read(fragment, &route)? else {
        return Ok(None);
    };
    let read_txn = db
        .begin_read()
        .map_err(|e| map_indexdb_error("begin_read", e))?;
    let table = match read_txn.open_table(SESSION_PARTS_TABLE) {
        Ok(table) => table,
        Err(TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(err) => return Err(map_indexdb_error("open session_parts", err)),
    };
    let part_key = session_part_row_key(version_id, part_index);
    let Some(value) = table
        .get(part_key.as_str())
        .map_err(|e| map_indexdb_error("session_part get", e))?
    else {
        return Ok(None);
    };
    deserialize(value.value()).map(Some)
}

pub fn load_session_part_receipts(
    fragment: &str,
    version_id: &str,
    manifest: &UploadSessionManifest,
) -> Result<Option<Vec<Option<SessionPartReceipt>>>, AppError> {
    let route = session_route(fragment, version_id)?;
    let Some(db) = open_route_db_for_read(fragment, &route)? else {
        return Ok(None);
    };
    let mut receipts = vec![None; manifest.expected_parts as usize];
    for receipt in load_session_receipts_exact(&db, version_id)? {
        let part_index = receipt.part_index;
        if part_index >= manifest.expected_parts {
            continue;
        }
        receipts[part_index as usize] = Some(receipt);
    }
    for part in load_session_parts_exact(&db, version_id)? {
        if part.part_index >= manifest.expected_parts {
            continue;
        }
        if receipts[part.part_index as usize].is_some() {
            continue;
        }
        receipts[part.part_index as usize] = Some(receipt_from_part(&part)?);
    }
    if receipts.iter().all(|entry| entry.is_none()) {
        return Ok(None);
    }
    Ok(Some(receipts))
}

pub fn remove_session(fragment: &str, version_id: &str) -> Result<bool, AppError> {
    let route = session_route(fragment, version_id)?;
    let owner_path = owner_db_path(&route, fragment);
    with_partition_commit_lock(&owner_path, || {
        if !owner_path.exists() {
            return Ok(false);
        }
        let owner_db = open_owner_db_for_write(&route, fragment)?;
        let Some(mutation) = remove_session_exact(&owner_db, version_id)? else {
            return Ok(false);
        };
        replicate_session_remove(fragment, &route, version_id, &mutation)?;
        Ok(true)
    })
}

pub fn list_active_sessions(fragment: &str) -> Result<Vec<IndexSessionState>, AppError> {
    let mut out = Vec::new();
    let Some(db) = open_fragment_db_for_read(fragment)? else {
        return Ok(out);
    };
    let read_txn = db
        .begin_read()
        .map_err(|e| map_indexdb_error("begin_read", e))?;
    let table = match read_txn.open_table(SESSIONS_TABLE) {
        Ok(table) => table,
        Err(TableError::TableDoesNotExist(_)) => return Ok(out),
        Err(err) => return Err(map_indexdb_error("open sessions", err)),
    };
    let mut iter = table
        .iter()
        .map_err(|e| map_indexdb_error("session iter", e))?;
    while let Some(entry) = iter.next() {
        let (_key, value) = entry.map_err(|e| map_indexdb_error("session iter next", e))?;
        let row: SessionRow = deserialize(value.value())?;
        if row.finalized_at.is_none() && row.aborted_at.is_none() {
            out.push(row.into_state());
        }
    }
    out.sort_by(|left, right| {
        right
            .manifest
            .created_at
            .cmp(&left.manifest.created_at)
            .then_with(|| right.manifest.version_id.cmp(&left.manifest.version_id))
    });
    Ok(out)
}

fn write_version_on_owner(metadata: &ObjectMetadata, state: ObjectState) -> Result<(), AppError> {
    let route = object_route(&metadata.fragment, &metadata.object_key)?;
    let owner_path = owner_db_path(&route, &metadata.fragment);
    with_partition_commit_lock(&owner_path, || {
        let owner_db = open_owner_db_for_write(&route, &metadata.fragment)?;
        let mutation = write_version_on_db(&owner_db, metadata, state)?;
        replicate_object_mutation(&metadata.fragment, &route, "version upsert", |replica_db| {
            apply_write_version_replica_exact(replica_db, &mutation)
        })?;
        Ok(())
    })
}

pub fn upsert_live_version(metadata: &ObjectMetadata) -> Result<(), AppError> {
    write_version_on_owner(metadata, ObjectState::Live)
}

pub fn read_live_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let route = object_route(fragment, object_key)?;
    let Some(db) = open_route_db_for_read(fragment, &route)? else {
        return Ok(None);
    };
    let Some(record) = read_version_exact(&db, fragment, object_key, version_id)? else {
        return Ok(None);
    };
    let Some(state) = read_version_state_exact(&db, fragment, object_key, version_id)? else {
        return Ok(None);
    };
    if state.state != ObjectState::Live {
        return Ok(None);
    }
    let head = read_head_exact(&db, fragment, object_key)?;
    if version_is_suppressed_by_tombstone(head.as_ref(), &record.metadata) {
        return Ok(None);
    }
    Ok(Some(record.metadata))
}

fn load_trash_catalog_entries_on_db(
    db: &Database,
    fragment: &str,
) -> Result<Vec<(String, TrashCatalogRecord)>, AppError> {
    let read_txn = db
        .begin_read()
        .map_err(|e| map_indexdb_error("begin_read", e))?;
    let trash_objects = match read_txn.open_table(TRASH_OBJECTS_TABLE) {
        Ok(table) => table,
        Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(err) => return Err(map_indexdb_error("open trash_objects", err)),
    };
    let versions = match read_txn.open_table(OBJECT_VERSIONS_TABLE) {
        Ok(table) => table,
        Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(err) => return Err(map_indexdb_error("open object_versions", err)),
    };
    let mut out = Vec::new();
    let mut iter = trash_objects
        .iter()
        .map_err(|e| map_indexdb_error("trash_objects iter", e))?;
    while let Some(entry) = iter.next() {
        let (key, value) = entry.map_err(|e| map_indexdb_error("trash_objects iter next", e))?;
        let object_key = key.value().to_string();
        let record: TrashObjectRecord = deserialize(value.value())?;
        let mut latest_metadata: Option<ObjectMetadata> = None;
        let mut version_ids = Vec::new();
        for version_id in &record.version_ids {
            let row_key = version_row_key(fragment, &object_key, version_id)?;
            let Some(version_value) = versions
                .get(row_key.as_str())
                .map_err(|e| map_indexdb_error("version get", e))?
            else {
                continue;
            };
            let metadata = deserialize::<VersionRecord>(version_value.value())?.metadata;
            match &latest_metadata {
                Some(current) if !metadata_is_newer(&metadata, current) => {}
                _ => latest_metadata = Some(metadata),
            }
            version_ids.push(version_id.clone());
        }
        if let Some(latest_metadata) = latest_metadata {
            version_ids.sort_by(|left, right| compare_version_id_recency(left, right));
            version_ids.dedup();
            let partition_id = object_partition_id(fragment, &object_key)?;
            out.push((
                object_key,
                TrashCatalogRecord {
                    partition_id,
                    deleted_at: record.deleted_at,
                    latest_metadata,
                    version_ids,
                },
            ));
        }
    }
    Ok(out)
}

fn load_all_trash_catalog_entries(
    fragment: &str,
) -> Result<Vec<(String, TrashCatalogRecord)>, AppError> {
    let Some(db) = open_fragment_db_for_read(fragment)? else {
        return Ok(Vec::new());
    };
    load_trash_catalog_entries_on_db(&db, fragment)
}

fn scan_trash_catalog_page(
    fragment: &str,
    prefix: Option<String>,
    delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
    hydrate_full_metadata: bool,
) -> Result<ListObjectsResult, AppError> {
    let delimiter = delimiter
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if delimiter.as_deref() != Some("/") && delimiter.is_some() {
        return Err(AppError::BadRequest(
            "only '/' delimiter is supported".to_string(),
        ));
    }
    let normalized_prefix = normalize_prefix(prefix, delimiter.as_deref());
    let cursor = continuation_token
        .map(|token| decode_cursor(&token))
        .transpose()?;
    if let Some(cursor) = &cursor {
        if cursor.state != ObjectState::Trash
            || cursor.prefix != normalized_prefix
            || cursor.delimiter != delimiter
            || cursor.include_internal != include_internal
        {
            return Err(AppError::BadRequest(
                "listing continuation token does not match the current query".to_string(),
            ));
        }
    }

    let after_key = cursor.as_ref().and_then(|value| value.last_key.as_deref());
    let prefix_children =
        prefix_children_prefix(normalized_prefix.as_deref(), delimiter.as_deref());
    let mut objects = Vec::new();
    let mut common_prefixes = Vec::new();
    let mut last_key: Option<String> = None;
    let mut page_full = false;
    let mut skip_prefix = cursor
        .as_ref()
        .and_then(|value| value.resume_skip_prefix.clone());

    let mut entries = load_all_trash_catalog_entries(fragment)?;
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    for (object_key, record) in entries {
        if !include_internal && is_internal_object_key(&object_key) {
            continue;
        }
        if let Some(prefix) = normalized_prefix.as_deref() {
            if !object_key.starts_with(prefix) {
                continue;
            }
        }
        if let Some(after_key) = after_key {
            if object_key.as_str() <= after_key {
                continue;
            }
        }
        if let Some(prefix) = skip_prefix.clone() {
            if object_key.starts_with(&prefix) {
                continue;
            }
            skip_prefix = None;
        }

        last_key = Some(object_key.clone());
        if delimiter.as_deref() == Some("/") {
            let suffix = if let Some(prefix) = normalized_prefix.as_deref() {
                if object_key == prefix {
                    ""
                } else if let Some(children_prefix) = prefix_children.as_deref() {
                    object_key.strip_prefix(children_prefix).unwrap_or_default()
                } else {
                    object_key.strip_prefix(prefix).unwrap_or_default()
                }
            } else {
                object_key.as_str()
            };
            if !suffix.is_empty() {
                if let Some(pos) = suffix.find('/') {
                    let common_prefix = format!(
                        "{}{}",
                        prefix_children
                            .as_deref()
                            .or(normalized_prefix.as_deref())
                            .unwrap_or(""),
                        &suffix[..pos + 1]
                    );
                    if common_prefixes.last().map(String::as_str) != Some(common_prefix.as_str()) {
                        common_prefixes.push(common_prefix.clone());
                        skip_prefix = Some(common_prefix);
                        if objects.len() + common_prefixes.len() >= limit {
                            page_full = true;
                            break;
                        }
                    }
                    continue;
                }
            }
        }

        objects.push(if hydrate_full_metadata {
            record.latest_metadata.clone()
        } else {
            record.latest_metadata
        });
        if objects.len() + common_prefixes.len() >= limit {
            page_full = true;
            break;
        }
    }

    let continuation_token = if page_full {
        Some(encode_cursor(&ListingCursor {
            state: ObjectState::Trash,
            prefix: normalized_prefix,
            delimiter,
            include_internal,
            last_key,
            resume_skip_prefix: skip_prefix,
        })?)
    } else {
        None
    };

    Ok(ListObjectsResult {
        objects,
        common_prefixes,
        continuation_token,
    })
}

pub fn upsert_trash_version(metadata: &ObjectMetadata) -> Result<(), AppError> {
    write_version_on_owner(metadata, ObjectState::Trash)
}

pub fn read_trash_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let route = object_route(fragment, object_key)?;
    let Some(db) = open_route_db_for_read(fragment, &route)? else {
        return Ok(None);
    };
    let Some(record) = read_version_exact(&db, fragment, object_key, version_id)? else {
        return Ok(None);
    };
    let Some(state) = read_version_state_exact(&db, fragment, object_key, version_id)? else {
        return Ok(None);
    };
    if state.state != ObjectState::Trash {
        return Ok(None);
    }
    Ok(Some(record.metadata))
}

pub fn list_live_versions(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let route = object_route(fragment, object_key)?;
    let Some(db) = open_route_db_for_read(fragment, &route)? else {
        return Ok(Vec::new());
    };
    list_versions_exact(&db, fragment, object_key, ObjectState::Live)
}

pub fn list_trash_versions(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let route = object_route(fragment, object_key)?;
    let Some(db) = open_route_db_for_read(fragment, &route)? else {
        return Ok(Vec::new());
    };
    list_versions_exact(&db, fragment, object_key, ObjectState::Trash)
}

pub fn read_current_live_head(
    fragment: &str,
    object_key: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let route = object_route(fragment, object_key)?;
    let Some(db) = open_route_db_for_read(fragment, &route)? else {
        return Ok(None);
    };
    let Some(head) = read_head_exact(&db, fragment, object_key)? else {
        return Ok(None);
    };
    if head.state != ObjectState::Live {
        return Ok(None);
    }
    load_current_metadata_for_head(fragment, &head)
}

pub fn read_latest_trash(
    fragment: &str,
    object_key: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    Ok(list_trash_versions(fragment, object_key)?
        .into_iter()
        .next())
}

pub fn scan_live_heads_page(
    fragment: &str,
    prefix: Option<String>,
    delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    ensure_fragment_live_namespace_table(fragment)?;
    scan_live_namespace_page(
        fragment,
        prefix,
        delimiter,
        limit,
        continuation_token,
        include_internal,
        false,
    )
}

pub fn scan_trash_heads_page(
    fragment: &str,
    prefix: Option<String>,
    delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    scan_trash_catalog_page(
        fragment,
        prefix,
        delimiter,
        limit,
        continuation_token,
        include_internal,
        false,
    )
}

pub fn scan_live_full_metadata_page(
    fragment: &str,
    prefix: Option<String>,
    delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    ensure_fragment_live_namespace_table(fragment)?;
    scan_live_namespace_page(
        fragment,
        prefix,
        delimiter,
        limit,
        continuation_token,
        include_internal,
        true,
    )
}

pub fn scan_trash_full_metadata_page(
    fragment: &str,
    prefix: Option<String>,
    delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    scan_trash_catalog_page(
        fragment,
        prefix,
        delimiter,
        limit,
        continuation_token,
        include_internal,
        true,
    )
}

pub fn list_live_heads(fragment: &str) -> Result<Vec<ObjectMetadata>, AppError> {
    let mut continuation = None;
    let mut out = Vec::new();
    loop {
        let page = scan_live_heads_page(fragment, None, None, 1000, continuation.clone(), true)?;
        out.extend(page.objects);
        if page.continuation_token.is_none() {
            break;
        }
        continuation = page.continuation_token;
    }
    Ok(out)
}

pub fn list_trash_heads(fragment: &str) -> Result<Vec<ObjectMetadata>, AppError> {
    Ok(load_all_trash_catalog_entries(fragment)?
        .into_iter()
        .map(|(_, record)| record.latest_metadata)
        .collect())
}

pub fn list_object_keys_with_trash_versions(fragment: &str) -> Result<Vec<String>, AppError> {
    Ok(load_all_trash_catalog_entries(fragment)?
        .into_iter()
        .map(|(object_key, _)| object_key)
        .collect())
}

pub fn list_active_trash_partition_ids(fragment: &str) -> Result<BTreeSet<u16>, AppError> {
    Ok(load_all_trash_catalog_entries(fragment)?
        .into_iter()
        .map(|(_, record)| record.partition_id)
        .collect())
}

pub fn list_partition_trash_entries(
    fragment: &str,
    partition_id: u16,
) -> Result<HashMap<String, Vec<String>>, AppError> {
    Ok(load_all_trash_catalog_entries(fragment)?
        .into_iter()
        .filter(|(_, record)| record.partition_id == partition_id)
        .map(|(object_key, record)| (object_key, record.version_ids))
        .collect())
}

pub fn list_trash_catalog(fragment: &str) -> Result<Vec<TrashCatalogEntry>, AppError> {
    Ok(load_all_trash_catalog_entries(fragment)?
        .into_iter()
        .map(|(object_key, record)| TrashCatalogEntry {
            object_key,
            version_ids: record.version_ids,
            deleted_at: record.deleted_at,
        })
        .collect())
}

pub fn list_trashed_object_version_ids_batch(
    fragment: &str,
    object_keys: &[String],
) -> Result<HashMap<String, Vec<String>>, AppError> {
    let mut out = HashMap::new();
    for key in object_keys {
        let version_ids = list_trash_versions(fragment, key)?
            .into_iter()
            .map(|metadata| metadata.version_id)
            .collect::<Vec<_>>();
        if !version_ids.is_empty() {
            out.insert(key.clone(), version_ids);
        }
    }
    Ok(out)
}

struct PartitionTrashClearPlan {
    row_keys_to_delete: Vec<String>,
    touched_keys: BTreeSet<String>,
}

fn plan_partition_trash_clear(
    db: &Database,
    fragment: &str,
) -> Result<PartitionTrashClearPlan, AppError> {
    let read_txn = db
        .begin_read()
        .map_err(|e| map_indexdb_error("begin_read", e))?;
    let trash_objects = match read_txn.open_table(TRASH_OBJECTS_TABLE) {
        Ok(table) => table,
        Err(TableError::TableDoesNotExist(_)) => {
            return Ok(PartitionTrashClearPlan {
                row_keys_to_delete: Vec::new(),
                touched_keys: BTreeSet::new(),
            });
        }
        Err(err) => return Err(map_indexdb_error("open trash_objects", err)),
    };
    let versions = match read_txn.open_table(OBJECT_VERSIONS_TABLE) {
        Ok(table) => table,
        Err(TableError::TableDoesNotExist(_)) => {
            return Ok(PartitionTrashClearPlan {
                row_keys_to_delete: Vec::new(),
                touched_keys: BTreeSet::new(),
            });
        }
        Err(err) => return Err(map_indexdb_error("open object_versions", err)),
    };
    let mut row_keys_to_delete = Vec::new();
    let mut touched_keys = BTreeSet::new();
    let mut iter = trash_objects
        .iter()
        .map_err(|e| map_indexdb_error("trash_objects iter", e))?;
    while let Some(entry) = iter.next() {
        let (object_key, value) =
            entry.map_err(|e| map_indexdb_error("trash_objects iter next", e))?;
        let object_key = object_key.value().to_string();
        let record: TrashObjectRecord = deserialize(value.value())?;
        if record.version_ids.is_empty() {
            continue;
        }
        for version_id in &record.version_ids {
            let row_key = version_row_key(fragment, &object_key, version_id)?;
            let Some(_version_value) = versions
                .get(row_key.as_str())
                .map_err(|e| map_indexdb_error("version get", e))?
            else {
                continue;
            };
            row_keys_to_delete.push(row_key);
        }
        touched_keys.insert(object_key);
    }

    Ok(PartitionTrashClearPlan {
        row_keys_to_delete,
        touched_keys,
    })
}

fn apply_partition_trash_clear_plan(
    db: &Database,
    fragment: &str,
    plan: &PartitionTrashClearPlan,
    partition_meta: &PartitionMeta,
) -> Result<usize, AppError> {
    if plan.row_keys_to_delete.is_empty() {
        return Ok(0);
    }

    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    let removed = {
        let mut meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let mut heads = write_txn
            .open_table(OBJECT_HEADS_TABLE)
            .map_err(|e| map_indexdb_error("open object_heads", e))?;
        let mut live_namespace = write_txn
            .open_table(LIVE_NAMESPACE_TABLE)
            .map_err(|e| map_indexdb_error("open live_namespace", e))?;
        let mut versions = write_txn
            .open_table(OBJECT_VERSIONS_TABLE)
            .map_err(|e| map_indexdb_error("open object_versions", e))?;
        let mut states = write_txn
            .open_table(OBJECT_VERSION_STATES_TABLE)
            .map_err(|e| map_indexdb_error("open object_version_states", e))?;
        let mut trash_objects = write_txn
            .open_table(TRASH_OBJECTS_TABLE)
            .map_err(|e| map_indexdb_error("open trash_objects", e))?;
        write_partition_meta_exact(&mut meta, partition_meta)?;

        for row_key in &plan.row_keys_to_delete {
            let _ = versions
                .remove(row_key.as_str())
                .map_err(|e| map_indexdb_error("version remove", e))?;
            let _ = states
                .remove(row_key.as_str())
                .map_err(|e| map_indexdb_error("version_state remove", e))?;
        }

        for object_key in &plan.touched_keys {
            let _ = trash_objects
                .remove(canonical_key(object_key).as_str())
                .map_err(|e| map_indexdb_error("trash_objects remove", e))?;
            let current_head = heads
                .get(head_row_key(fragment, object_key)?.as_str())
                .map_err(|e| map_indexdb_error("head get", e))?
                .map(|value| deserialize::<HeadRecord>(value.value()))
                .transpose()?;
            let prefix = version_row_prefix(fragment, object_key)?;
            let mut remaining_iter = states
                .range(prefix.as_str()..)
                .map_err(|e| map_indexdb_error("version_state range", e))?;
            let mut latest_remaining: Option<(ObjectMetadata, ObjectState)> = None;

            while let Some(entry) = remaining_iter.next() {
                let (row_key, value) =
                    entry.map_err(|e| map_indexdb_error("version_state range next", e))?;
                let row_key = row_key.value();
                if !row_key.starts_with(&prefix) {
                    break;
                }
                let visibility: VersionStateRecord = deserialize(value.value())?;
                if visibility.state == ObjectState::Deleted {
                    continue;
                }
                let Some(version_value) = versions
                    .get(row_key)
                    .map_err(|e| map_indexdb_error("version get", e))?
                else {
                    continue;
                };
                let metadata = deserialize::<VersionRecord>(version_value.value())?.metadata;
                match &latest_remaining {
                    Some((current, _)) if !metadata_is_newer(&metadata, current) => {}
                    _ => latest_remaining = Some((metadata, visibility.state)),
                }
            }

            if let Some((metadata, state)) = latest_remaining {
                let next_head = HeadRecord::from_metadata(
                    &metadata,
                    state,
                    next_head_generation(current_head.as_ref()),
                    partition_meta.updated_at,
                );
                write_head_with_namespace_exact(&mut heads, &mut live_namespace, &next_head)?;
            } else {
                remove_head_with_namespace_exact(
                    &mut heads,
                    &mut live_namespace,
                    fragment,
                    object_key,
                )?;
            }
        }

        plan.row_keys_to_delete.len()
    };
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit clear partition trash", e))?;
    Ok(removed)
}

fn clear_partition_trash_on_db(
    db: &Database,
    fragment: &str,
) -> Result<(usize, PartitionMeta), AppError> {
    let plan = plan_partition_trash_clear(db, fragment)?;
    if plan.row_keys_to_delete.is_empty() {
        return Ok((
            0,
            PartitionMeta {
                committed_generation: 0,
                updated_at: 0,
            },
        ));
    }

    let existing_meta = ensure_partition_meta_exact(db)?;
    let partition_meta = PartitionMeta {
        committed_generation: existing_meta.committed_generation.saturating_add(1),
        updated_at: Utc::now().timestamp(),
    };

    let removed = apply_partition_trash_clear_plan(db, fragment, &plan, &partition_meta)?;
    Ok((removed, partition_meta))
}

fn apply_clear_partition_trash_replica_exact(
    db: &Database,
    fragment: &str,
    partition_meta: &PartitionMeta,
) -> Result<(), AppError> {
    let plan = plan_partition_trash_clear(db, fragment)?;
    let _ = apply_partition_trash_clear_plan(db, fragment, &plan, partition_meta)?;
    Ok(())
}

pub fn move_object_version_to_trash_with_metadata(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let route = object_route(fragment, object_key)?;
    let owner_path = owner_db_path(&route, fragment);
    let moved = with_partition_commit_lock(&owner_path, || {
        if !owner_path.exists() {
            return Ok(None);
        }
        let owner_db = open_owner_db_for_write(&route, fragment)?;
        let changed = change_version_state_on_db(
            &owner_db,
            fragment,
            object_key,
            version_id,
            ObjectState::Trash,
            Some(ObjectState::Live),
        )?;
        let Some((metadata, partition_meta)) = changed else {
            return Ok(None);
        };
        replicate_object_mutation(
            fragment,
            &route,
            "move object version to trash",
            |replica_db| {
                apply_change_version_state_replica_exact(
                    replica_db,
                    fragment,
                    object_key,
                    version_id,
                    ObjectState::Trash,
                    Some(ObjectState::Live),
                    &partition_meta,
                )
            },
        )?;
        Ok(Some(metadata))
    })?;
    Ok(moved)
}

pub fn restore_object_version_from_trash_with_metadata(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let route = object_route(fragment, object_key)?;
    let owner_path = owner_db_path(&route, fragment);
    let restored = with_partition_commit_lock(&owner_path, || {
        if !owner_path.exists() {
            return Ok(None);
        }
        let owner_db = open_owner_db_for_write(&route, fragment)?;
        let changed = change_version_state_on_db(
            &owner_db,
            fragment,
            object_key,
            version_id,
            ObjectState::Live,
            Some(ObjectState::Trash),
        )?;
        let Some((metadata, partition_meta)) = changed else {
            return Ok(None);
        };
        replicate_object_mutation(
            fragment,
            &route,
            "restore object version from trash",
            |replica_db| {
                apply_change_version_state_replica_exact(
                    replica_db,
                    fragment,
                    object_key,
                    version_id,
                    ObjectState::Live,
                    Some(ObjectState::Trash),
                    &partition_meta,
                )
            },
        )?;
        Ok(Some(metadata))
    })?;
    Ok(restored)
}

pub fn move_object_to_trash_fast(
    fragment: &str,
    object_key: &str,
) -> Result<Option<Vec<String>>, AppError> {
    Ok(move_objects_to_trash_batch(fragment, &[object_key.to_string()])?.remove(object_key))
}

pub fn move_objects_to_trash_batch(
    fragment: &str,
    object_keys: &[String],
) -> Result<HashMap<String, Vec<String>>, AppError> {
    let mut grouped = HashMap::<u16, Vec<String>>::new();
    for object_key in object_keys {
        grouped
            .entry(object_partition_id(fragment, object_key)?)
            .or_default()
            .push(object_key.clone());
    }
    let grouped = grouped.into_iter().collect::<Vec<_>>();

    let mut moved = HashMap::new();
    let write_parallelism = destructive_partition_parallelism().min(grouped.len().max(1));
    for chunk in grouped.chunks(write_parallelism) {
        let fragment = fragment.to_string();
        let chunk_results = std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(chunk.len());
            for (partition_id, keys) in chunk {
                let fragment = fragment.clone();
                let keys = keys.clone();
                let partition_id = *partition_id;
                handles.push(scope.spawn(
                    move || -> Result<HashMap<String, Vec<String>>, AppError> {
                        let route = route_for_partition(&fragment, partition_id)?;
                        let owner_path = owner_db_path(&route, &fragment);
                        if !owner_path.exists() {
                            return Ok(HashMap::new());
                        }
                        with_partition_commit_lock(&owner_path, || {
                            let owner_db = open_owner_db_for_write(&route, &fragment)?;
                            let (changed, partition_meta) = bulk_rewrite_keys_state_on_db(
                                &owner_db,
                                &fragment,
                                &keys,
                                ObjectState::Live,
                                ObjectState::Trash,
                            )?;
                            if !changed.is_empty() {
                                replicate_object_mutation(
                                    &fragment,
                                    &route,
                                    "move objects to trash batch",
                                    |replica_db| {
                                        apply_bulk_rewrite_keys_state_replica_exact(
                                            replica_db,
                                            &fragment,
                                            &keys,
                                            ObjectState::Live,
                                            ObjectState::Trash,
                                            &partition_meta,
                                        )
                                    },
                                )?;
                            }
                            Ok(changed)
                        })
                    },
                ));
            }

            let mut results = Vec::with_capacity(handles.len());
            for handle in handles {
                let batch = handle.join().map_err(|_| {
                    AppError::InternalServerError(
                        "indexdb trash delete worker panicked".to_string(),
                    )
                })??;
                results.push(batch);
            }
            Ok::<Vec<HashMap<String, Vec<String>>>, AppError>(results)
        })?;

        for batch in chunk_results {
            moved.extend(batch);
        }
    }

    Ok(moved)
}

pub fn restore_object_from_trash_fast(
    fragment: &str,
    object_key: &str,
) -> Result<Option<Vec<String>>, AppError> {
    let version_ids = list_trash_versions(fragment, object_key)?
        .into_iter()
        .map(|metadata| metadata.version_id)
        .collect::<Vec<_>>();
    if version_ids.is_empty() {
        return Ok(None);
    }
    let mut manifest = HashMap::new();
    manifest.insert(object_key.to_string(), version_ids.clone());
    let restored = restore_objects_from_trash_manifest_batch(fragment, &manifest)?;
    Ok(restored.get(object_key).cloned())
}

pub fn restore_objects_from_trash_batch(
    fragment: &str,
    object_keys: &[String],
) -> Result<HashMap<String, Vec<String>>, AppError> {
    let manifest = list_trashed_object_version_ids_batch(fragment, object_keys)?;
    restore_objects_from_trash_manifest_batch(fragment, &manifest)
}

pub fn restore_objects_from_trash_manifest_batch(
    fragment: &str,
    object_versions: &HashMap<String, Vec<String>>,
) -> Result<HashMap<String, Vec<String>>, AppError> {
    if object_versions.is_empty() {
        return Ok(HashMap::new());
    }

    let mut grouped_versions = HashMap::<u16, HashMap<String, Vec<String>>>::new();
    for (object_key, version_ids) in object_versions {
        let partition_id = object_partition_id(fragment, object_key)?;
        grouped_versions
            .entry(partition_id)
            .or_default()
            .insert(object_key.clone(), version_ids.clone());
    }

    let grouped_entries = grouped_versions.into_iter().collect::<Vec<_>>();
    let mut restored = HashMap::new();
    let restore_parallelism = destructive_partition_parallelism().min(grouped_entries.len().max(1));
    for chunk in grouped_entries.chunks(restore_parallelism) {
        let fragment = fragment.to_string();
        let chunk_results = std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(chunk.len());
            for (partition_id, entries) in chunk {
                let fragment = fragment.clone();
                let entries = entries.clone();
                let partition_id = *partition_id;
                handles.push(scope.spawn(
                    move || -> Result<HashMap<String, Vec<String>>, AppError> {
                        let route = route_for_partition(&fragment, partition_id)?;
                        let owner_path = owner_db_path(&route, &fragment);
                        with_partition_commit_lock(&owner_path, || {
                            let owner_db = open_owner_db_for_write(&route, &fragment)?;
                            let (changed, partition_meta) =
                                restore_trash_entries_on_db(&owner_db, &fragment, &entries)?;
                            if !changed.is_empty() {
                                replicate_object_mutation(
                                    &fragment,
                                    &route,
                                    "restore objects from trash batch",
                                    |replica_db| {
                                        apply_restore_trash_entries_replica_exact(
                                            replica_db,
                                            &fragment,
                                            &entries,
                                            &partition_meta,
                                        )
                                    },
                                )?;
                            }
                            Ok(changed)
                        })
                    },
                ));
            }

            let mut results = Vec::with_capacity(handles.len());
            for handle in handles {
                let batch = handle.join().map_err(|_| {
                    AppError::InternalServerError(
                        "indexdb trash restore worker panicked".to_string(),
                    )
                })??;
                results.push(batch);
            }
            Ok::<Vec<HashMap<String, Vec<String>>>, AppError>(results)
        })?;

        for batch in chunk_results {
            restored.extend(batch);
        }
    }

    Ok(restored)
}

pub fn delete_all_trash_versions_for_key(
    fragment: &str,
    object_key: &str,
) -> Result<Option<Vec<String>>, AppError> {
    let route = object_route(fragment, object_key)?;
    let owner_path = owner_db_path(&route, fragment);
    with_partition_commit_lock(&owner_path, || {
        if !owner_path.exists() {
            return Ok(None);
        }
        let owner_db = open_owner_db_for_write(&route, fragment)?;
        let deleted = delete_versions_for_key_with_state_on_db(
            &owner_db,
            fragment,
            object_key,
            ObjectState::Trash,
        )?;
        if let Some((version_ids, partition_meta)) = deleted {
            replicate_object_mutation(
                fragment,
                &route,
                "delete trash versions for key",
                |replica_db| {
                    apply_delete_versions_for_key_with_state_replica_exact(
                        replica_db,
                        fragment,
                        object_key,
                        ObjectState::Trash,
                        &partition_meta,
                    )
                },
            )?;
            Ok(Some(version_ids))
        } else {
            Ok(None)
        }
    })
}

pub fn delete_object_fast(
    fragment: &str,
    object_key: &str,
) -> Result<Option<Vec<String>>, AppError> {
    let route = object_route(fragment, object_key)?;
    let owner_path = owner_db_path(&route, fragment);
    let deleted = with_partition_commit_lock(&owner_path, || {
        if !owner_path.exists() {
            return Ok(None);
        }
        let owner_db = open_owner_db_for_write(&route, fragment)?;
        let deleted = delete_all_versions_for_key_on_db(&owner_db, fragment, object_key)?;
        if let Some((version_ids, partition_meta)) = deleted {
            replicate_object_mutation(fragment, &route, "delete object", |replica_db| {
                apply_delete_all_versions_for_key_replica_exact(
                    replica_db,
                    fragment,
                    object_key,
                    &partition_meta,
                )
            })?;
            Ok(Some(version_ids))
        } else {
            Ok(None)
        }
    })?;
    Ok(deleted)
}

pub fn delete_live_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<bool, AppError> {
    let route = object_route(fragment, object_key)?;
    let owner_path = owner_db_path(&route, fragment);
    let deleted = with_partition_commit_lock(&owner_path, || {
        if !owner_path.exists() {
            return Ok(false);
        }
        let owner_db = open_owner_db_for_write(&route, fragment)?;
        let deleted = delete_version_on_db(
            &owner_db,
            fragment,
            object_key,
            version_id,
            Some(ObjectState::Live),
        )?
        .map(|(_metadata, partition_meta)| partition_meta);
        if let Some(partition_meta) = deleted {
            replicate_object_mutation(fragment, &route, "delete live version", |replica_db| {
                apply_delete_version_replica_exact(
                    replica_db,
                    fragment,
                    object_key,
                    version_id,
                    Some(ObjectState::Live),
                    &partition_meta,
                )
            })?;
            Ok(true)
        } else {
            Ok(false)
        }
    })?;
    Ok(deleted)
}

pub fn delete_trash_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<bool, AppError> {
    let route = object_route(fragment, object_key)?;
    let owner_path = owner_db_path(&route, fragment);
    let deleted = with_partition_commit_lock(&owner_path, || {
        if !owner_path.exists() {
            return Ok(false);
        }
        let owner_db = open_owner_db_for_write(&route, fragment)?;
        let deleted = delete_version_on_db(
            &owner_db,
            fragment,
            object_key,
            version_id,
            Some(ObjectState::Trash),
        )?
        .map(|(_metadata, partition_meta)| partition_meta);
        if let Some(partition_meta) = deleted {
            replicate_object_mutation(fragment, &route, "delete trash version", |replica_db| {
                apply_delete_version_replica_exact(
                    replica_db,
                    fragment,
                    object_key,
                    version_id,
                    Some(ObjectState::Trash),
                    &partition_meta,
                )
            })?;
        } else {
            return Ok(false);
        }
        Ok(true)
    })?;
    Ok(deleted)
}

pub fn clear_fragment_trash(fragment: &str, object_key: Option<&str>) -> Result<(), AppError> {
    if let Some(object_key) = object_key {
        let _ = delete_all_trash_versions_for_key(fragment, object_key)?;
        return Ok(());
    }
    let touched_partitions = list_active_trash_partition_ids(fragment)?;
    let _ = clear_fragment_trash_for_partitions(fragment, &touched_partitions)?;
    Ok(())
}

pub fn clear_fragment_trash_for_partitions(
    fragment: &str,
    touched_partitions: &BTreeSet<u16>,
) -> Result<usize, AppError> {
    if touched_partitions.is_empty() {
        return Ok(0);
    }
    let route = fragment_route(fragment)?;
    let owner_path = owner_db_path(&route, fragment);
    with_partition_commit_lock(&owner_path, || {
        if !owner_path.exists() {
            return Ok(0usize);
        }
        let owner_db = open_owner_db_for_write(&route, fragment)?;
        let (removed, partition_meta) = clear_partition_trash_on_db(&owner_db, fragment)?;
        if removed > 0 {
            replicate_object_mutation(fragment, &route, "clear fragment trash", |replica_db| {
                apply_clear_partition_trash_replica_exact(replica_db, fragment, &partition_meta)
            })?;
        }
        Ok(removed)
    })
}

pub fn clear_fragment_trash_partition(
    fragment: &str,
    partition_id: u16,
) -> Result<usize, AppError> {
    let mut touched = BTreeSet::new();
    touched.insert(partition_id);
    clear_fragment_trash_for_partitions(fragment, &touched)
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct SessionLayoutStats {
    pub session_row_bytes: usize,
    pub receipt_rows: usize,
    pub part_rows: usize,
    pub owner_db_bytes: u64,
}

#[cfg(test)]
pub(crate) fn session_layout_stats(
    fragment: &str,
    version_id: &str,
) -> Result<Option<SessionLayoutStats>, AppError> {
    let route = session_route(fragment, version_id)?;
    let owner_path = owner_db_path(&route, fragment);
    if !owner_path.exists() {
        return Ok(None);
    }
    let db = get_or_open_db(&owner_path)?;
    let read_txn = db
        .begin_read()
        .map_err(|e| map_indexdb_error("begin_read", e))?;
    let sessions = match read_txn.open_table(SESSIONS_TABLE) {
        Ok(table) => table,
        Err(TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(err) => return Err(map_indexdb_error("open sessions", err)),
    };
    let Some(value) = sessions
        .get(version_id)
        .map_err(|e| map_indexdb_error("session get", e))?
    else {
        return Ok(None);
    };
    let prefix = session_part_row_prefix(version_id);
    let receipt_rows = list_prefixed_keys(&db, SESSION_PART_RECEIPTS_TABLE, &prefix)?.len();
    let part_rows = list_prefixed_keys(&db, SESSION_PARTS_TABLE, &prefix)?.len();
    let owner_db_bytes = fs::metadata(owner_path)
        .map_err(|e| AppError::Io(e, "session layout stats metadata".into()))?
        .len();
    Ok(Some(SessionLayoutStats {
        session_row_bytes: value.value().len(),
        receipt_rows,
        part_rows,
        owner_db_bytes,
    }))
}
