use super::disk_manager::{
    ListObjectsResult, ObjectMetadata, ObjectPathManager, PartMetadata, ReservationReceipt,
};
use super::index::{IndexSessionState, SessionPartReceipt};
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
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};
use tracing::warn;
use ulid::Ulid;

const INDEXDB_DIR: &str = "indexdb";
const INDEXDB_PARTITIONS_DIR: &str = "partitions";
const INDEX_PARTITION_COUNT: u16 = 256;
const VERSION_KEY_SEPARATOR: char = '\u{001f}';
const VERSION_KEY_SEPARATOR_STR: &str = "\u{001f}";

const OBJECT_HEADS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("object_heads");
const OBJECT_VERSIONS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("object_versions");
const OBJECT_VERSION_STATES_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("object_version_states");
const SESSIONS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("sessions");
const PARTITION_META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("partition_meta");
const PARTITION_META_KEY: &str = "__partition__";

static DB_CACHE: LazyLock<Mutex<HashMap<PathBuf, Arc<Database>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static PARTITION_OP_LOCKS: LazyLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

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
struct PartitionMeta {
    committed_generation: u64,
    updated_at: i64,
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

    fn tombstone(
        fragment: &str,
        object_key: &str,
        generation: u64,
        updated_at: i64,
        previous: Option<&HeadRecord>,
        removed: Option<&ObjectMetadata>,
    ) -> Self {
        let source = removed
            .map(TombstoneSource::Metadata)
            .or_else(|| previous.map(TombstoneSource::Head));
        Self {
            fragment: fragment.to_string(),
            object_key: canonical_key(object_key),
            version_id: source
                .as_ref()
                .map(|value| value.version_id().to_string())
                .unwrap_or_default(),
            upload_session_id: source
                .as_ref()
                .map(|value| value.upload_session_id().to_string())
                .unwrap_or_default(),
            owner_uid: source
                .as_ref()
                .map(|value| value.owner_uid().to_string())
                .unwrap_or_else(|| fragment.to_string()),
            content_type: source
                .as_ref()
                .map(|value| value.content_type().to_string())
                .unwrap_or_default(),
            total_size_bytes: source
                .as_ref()
                .map(|value| value.total_size_bytes())
                .unwrap_or(0),
            created_at: source
                .as_ref()
                .map(|value| value.created_at())
                .unwrap_or(updated_at),
            is_delete_marker: source
                .as_ref()
                .map(|value| value.is_delete_marker())
                .unwrap_or(true),
            stripe_size_bytes: source
                .as_ref()
                .map(|value| value.stripe_size_bytes())
                .unwrap_or_default(),
            state: ObjectState::Deleted,
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

enum TombstoneSource<'a> {
    Metadata(&'a ObjectMetadata),
    Head(&'a HeadRecord),
}

impl TombstoneSource<'_> {
    fn version_id(&self) -> &str {
        match self {
            Self::Metadata(metadata) => &metadata.version_id,
            Self::Head(head) => &head.version_id,
        }
    }

    fn upload_session_id(&self) -> &str {
        match self {
            Self::Metadata(metadata) => &metadata.upload_session_id,
            Self::Head(head) => &head.upload_session_id,
        }
    }

    fn owner_uid(&self) -> &str {
        match self {
            Self::Metadata(metadata) => &metadata.owner_uid,
            Self::Head(head) => &head.owner_uid,
        }
    }

    fn content_type(&self) -> &str {
        match self {
            Self::Metadata(metadata) => &metadata.content_type,
            Self::Head(head) => &head.content_type,
        }
    }

    fn total_size_bytes(&self) -> u64 {
        match self {
            Self::Metadata(metadata) => metadata.total_size_bytes,
            Self::Head(head) => head.total_size_bytes,
        }
    }

    fn created_at(&self) -> i64 {
        match self {
            Self::Metadata(metadata) => metadata.created_at,
            Self::Head(head) => head.created_at,
        }
    }

    fn is_delete_marker(&self) -> bool {
        match self {
            Self::Metadata(metadata) => metadata.is_delete_marker,
            Self::Head(head) => head.is_delete_marker,
        }
    }

    fn stripe_size_bytes(&self) -> usize {
        match self {
            Self::Metadata(metadata) => metadata.stripe_size_bytes,
            Self::Head(head) => head.stripe_size_bytes,
        }
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
struct ListingItem {
    object_key: String,
    head: HeadRecord,
}

struct PartitionRange {
    iter: indexdb::Range<'static, &'static str, &'static [u8]>,
    next: Option<ListingItem>,
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

fn route_for_hash(hash: [u8; 32]) -> Result<IndexRoute, AppError> {
    let partition_id = (u16::from(hash[0]) << 8 | u16::from(hash[1])) % INDEX_PARTITION_COUNT;
    route_for_partition(partition_id)
}

fn route_for_partition(partition_id: u16) -> Result<IndexRoute, AppError> {
    let roots = ObjectPathManager::new().get_all_index_roots();
    if roots.is_empty() {
        return Err(AppError::InternalServerError(
            "No index roots configured".to_string(),
        ));
    }

    let owner_idx = usize::from(partition_id) % roots.len();
    let mut selected = vec![roots[owner_idx].clone()];
    if roots.len() > 1 {
        let replica_offset = 1 + ((usize::from(partition_id >> 8)) % (roots.len() - 1));
        let replica_idx = (owner_idx + replica_offset) % roots.len();
        if replica_idx != owner_idx {
            selected.push(roots[replica_idx].clone());
        }
    }

    Ok(IndexRoute {
        partition_id,
        roots: selected,
    })
}

fn object_route(fragment: &str, object_key: &str) -> Result<IndexRoute, AppError> {
    route_for_hash(hash_key(&["object", fragment, &canonical_key(object_key)]))
}

fn session_route(fragment: &str, version_id: &str) -> Result<IndexRoute, AppError> {
    route_for_hash(hash_key(&["session", fragment, version_id]))
}

fn db_path(root: &Path, fragment: &str, partition_id: u16) -> PathBuf {
    root.join(fragment)
        .join(INDEXDB_DIR)
        .join(INDEXDB_PARTITIONS_DIR)
        .join(format!("{:04x}.ndxdb", partition_id))
}

fn version_row_key(object_key: &str, version_id: &str) -> String {
    format!(
        "{}{}{}",
        canonical_key(object_key),
        VERSION_KEY_SEPARATOR,
        version_id
    )
}

fn version_row_prefix(object_key: &str) -> String {
    format!("{}{}", canonical_key(object_key), VERSION_KEY_SEPARATOR)
}

fn parse_version_key(row_key: &str) -> Option<(&str, &str)> {
    row_key.split_once(VERSION_KEY_SEPARATOR_STR)
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
    let bytes = fs::read(src).map_err(|e| AppError::Io(e, format!("read {}", src.display())))?;
    fs::write(&tmp, bytes).map_err(|e| AppError::Io(e, format!("write {}", tmp.display())))?;
    fs::rename(&tmp, dst)
        .map_err(|e| AppError::Io(e, format!("rename {} -> {}", tmp.display(), dst.display())))?;
    Ok(())
}

fn get_or_open_db(path: &Path) -> Result<Arc<Database>, AppError> {
    let mut cache = DB_CACHE
        .lock()
        .map_err(|_| AppError::InternalServerError("indexdb cache lock poisoned".to_string()))?;
    if let Some(db) = cache.get(path) {
        return Ok(db.clone());
    }

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| AppError::Io(e, format!("mkdir {}", parent.display())))?;
    }

    let db = Arc::new(
        Database::create(path)
            .map_err(|e| map_indexdb_error(&format!("open {}", path.display()), e))?,
    );
    cache.insert(path.to_path_buf(), db.clone());
    Ok(db)
}

fn open_partition_db_for_read(
    fragment: &str,
    partition_id: u16,
) -> Result<Option<Arc<Database>>, AppError> {
    let route = route_for_partition(partition_id)?;
    open_route_db_for_read(fragment, &route)
}

fn open_route_db_for_read(
    fragment: &str,
    route: &IndexRoute,
) -> Result<Option<Arc<Database>>, AppError> {
    let owner_path = owner_db_path(route, fragment);
    let mut best_follower: Option<(u64, Arc<Database>)> = None;
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
                return Ok(Some(db));
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
    Ok(best_follower.map(|(_, db)| db))
}

fn owner_db_path(route: &IndexRoute, fragment: &str) -> PathBuf {
    db_path(&route.roots[0], fragment, route.partition_id)
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
    if !owner_path.exists() {
        return Err(AppError::InternalServerError(format!(
            "owner partition db missing after commit: {}",
            owner_path.display()
        )));
    }

    for replica_root in route.roots.iter().skip(1) {
        let replica_path = db_path(replica_root, fragment, route.partition_id);
        evict_cached_db(&replica_path);
        if let Err(err) = atomic_copy_db_file(&owner_path, &replica_path) {
            warn!(
                "indexdb follower catch-up failed for {} -> {}: {}",
                owner_path.display(),
                replica_path.display(),
                err
            );
        }
    }

    Ok(())
}

fn has_partition_db(fragment: &str, partition_id: u16) -> Result<bool, AppError> {
    let route = route_for_partition(partition_id)?;
    Ok(route
        .roots
        .iter()
        .any(|root| db_path(root, fragment, partition_id).exists()))
}

pub fn has_object_partition_db(fragment: &str, object_key: &str) -> Result<bool, AppError> {
    let route = object_route(fragment, object_key)?;
    Ok(route
        .roots
        .iter()
        .any(|root| db_path(root, fragment, route.partition_id).exists()))
}

pub fn has_session_partition_db(fragment: &str, version_id: &str) -> Result<bool, AppError> {
    let route = session_route(fragment, version_id)?;
    Ok(route
        .roots
        .iter()
        .any(|root| db_path(root, fragment, route.partition_id).exists()))
}

pub fn has_any_fragment_partition_db(fragment: &str) -> Result<bool, AppError> {
    for partition_id in 0..INDEX_PARTITION_COUNT {
        if has_partition_db(fragment, partition_id)? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn sync_fragment_partition_replicas(fragment: &str) -> Result<(), AppError> {
    for partition_id in 0..INDEX_PARTITION_COUNT {
        let route = route_for_partition(partition_id)?;
        if route.roots.len() <= 1 {
            continue;
        }
        let owner_path = owner_db_path(&route, fragment);
        with_partition_commit_lock(&owner_path, || {
            if owner_path.exists() {
                replicate_owner_partition_db(fragment, &route)?;
            }
            Ok(())
        })?;
    }
    Ok(())
}

fn read_head_exact(db: &Database, object_key: &str) -> Result<Option<HeadRecord>, AppError> {
    let read_txn = db
        .begin_read()
        .map_err(|e| map_indexdb_error("begin_read", e))?;
    let table = match read_txn.open_table(OBJECT_HEADS_TABLE) {
        Ok(table) => table,
        Err(TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(err) => return Err(map_indexdb_error("open object_heads", err)),
    };
    let Some(value) = table
        .get(canonical_key(object_key).as_str())
        .map_err(|e| map_indexdb_error("head get", e))?
    else {
        return Ok(None);
    };
    deserialize(value.value()).map(Some)
}

fn read_version_exact(
    db: &Database,
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
    let key = version_row_key(object_key, version_id);
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
    let key = version_row_key(object_key, version_id);
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
) -> bool {
    match current_head {
        None => true,
        Some(head) if head.version_id == metadata.version_id => head.state != ObjectState::Deleted,
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
    heads
        .insert(canonical_key(&head.object_key).as_str(), bytes.as_slice())
        .map_err(|e| map_indexdb_error("head insert", e))?;
    Ok(())
}

fn list_versions_exact(
    db: &Database,
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
    let head = read_head_exact(db, object_key)?;

    let prefix = version_row_prefix(object_key);
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
) -> Result<(), AppError> {
    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    {
        let mut partition_meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let mut heads = write_txn
            .open_table(OBJECT_HEADS_TABLE)
            .map_err(|e| map_indexdb_error("open object_heads", e))?;
        let mut versions = write_txn
            .open_table(OBJECT_VERSIONS_TABLE)
            .map_err(|e| map_indexdb_error("open object_versions", e))?;
        let mut states = write_txn
            .open_table(OBJECT_VERSION_STATES_TABLE)
            .map_err(|e| map_indexdb_error("open object_version_states", e))?;
        let now = Utc::now().timestamp();
        let committed_generation = next_partition_generation(&mut partition_meta, now)?;
        let row_key = version_row_key(&metadata.object_key, &metadata.version_id);
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
            .get(canonical_key(&metadata.object_key).as_str())
            .map_err(|e| map_indexdb_error("head get", e))?
            .map(|value| deserialize::<HeadRecord>(value.value()))
            .transpose()?;
        if candidate_should_replace_head(current_head.as_ref(), metadata) {
            let head = HeadRecord::from_metadata(
                metadata,
                state,
                next_head_generation(current_head.as_ref()),
                now,
            );
            write_head_exact(&mut heads, &head)?;
        }
    }
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit version write", e))?;
    Ok(())
}

fn change_version_state_on_db(
    db: &Database,
    object_key: &str,
    version_id: &str,
    target_state: ObjectState,
    expected_state: Option<ObjectState>,
) -> Result<Option<ObjectMetadata>, AppError> {
    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    let record = {
        let mut partition_meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let mut heads = write_txn
            .open_table(OBJECT_HEADS_TABLE)
            .map_err(|e| map_indexdb_error("open object_heads", e))?;
        let versions = write_txn
            .open_table(OBJECT_VERSIONS_TABLE)
            .map_err(|e| map_indexdb_error("open object_versions", e))?;
        let mut states = write_txn
            .open_table(OBJECT_VERSION_STATES_TABLE)
            .map_err(|e| map_indexdb_error("open object_version_states", e))?;
        let now = Utc::now().timestamp();
        let committed_generation = next_partition_generation(&mut partition_meta, now)?;
        let row_key = version_row_key(object_key, version_id);
        let current_head = heads
            .get(canonical_key(object_key).as_str())
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
                write_head_exact(&mut heads, &next_head)?;
            } else if target_state == ObjectState::Live
                && expected_state == Some(ObjectState::Trash)
            {
                let restored_head = HeadRecord::from_metadata(
                    &metadata,
                    ObjectState::Live,
                    next_head_generation(current_head.as_ref()),
                    now,
                );
                write_head_exact(&mut heads, &restored_head)?;
            }
        } else if target_state == ObjectState::Live && expected_state == Some(ObjectState::Trash) {
            let restored_head = HeadRecord::from_metadata(&metadata, ObjectState::Live, 1, now);
            write_head_exact(&mut heads, &restored_head)?;
        }
        metadata
    };
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit version state write", e))?;
    Ok(Some(record))
}

fn delete_version_on_db(
    db: &Database,
    object_key: &str,
    version_id: &str,
    expected_state: Option<ObjectState>,
) -> Result<Option<ObjectMetadata>, AppError> {
    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    let record = {
        let mut partition_meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let mut heads = write_txn
            .open_table(OBJECT_HEADS_TABLE)
            .map_err(|e| map_indexdb_error("open object_heads", e))?;
        let mut versions = write_txn
            .open_table(OBJECT_VERSIONS_TABLE)
            .map_err(|e| map_indexdb_error("open object_versions", e))?;
        let mut states = write_txn
            .open_table(OBJECT_VERSION_STATES_TABLE)
            .map_err(|e| map_indexdb_error("open object_version_states", e))?;
        let now = Utc::now().timestamp();
        let _committed_generation = next_partition_generation(&mut partition_meta, now)?;
        let row_key = version_row_key(object_key, version_id);
        let current_head = heads
            .get(canonical_key(object_key).as_str())
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
        if current_head
            .as_ref()
            .map(|head| head.version_id == version_id)
            .unwrap_or(false)
        {
            let fragment = current_head
                .as_ref()
                .map(|head| head.fragment.as_str())
                .unwrap_or(removed_record.metadata.fragment.as_str());
            let tombstone = HeadRecord::tombstone(
                fragment,
                object_key,
                next_head_generation(current_head.as_ref()),
                now,
                current_head.as_ref(),
                Some(&removed_record.metadata),
            );
            write_head_exact(&mut heads, &tombstone)?;
        }
        removed_record
    };
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit version delete", e))?;
    Ok(Some(record.metadata))
}

fn bulk_rewrite_key_state_on_db(
    db: &Database,
    object_key: &str,
    from_state: ObjectState,
    to_state: ObjectState,
) -> Result<Vec<String>, AppError> {
    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    let touched = {
        let mut partition_meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let mut heads = write_txn
            .open_table(OBJECT_HEADS_TABLE)
            .map_err(|e| map_indexdb_error("open object_heads", e))?;
        let mut states = write_txn
            .open_table(OBJECT_VERSION_STATES_TABLE)
            .map_err(|e| map_indexdb_error("open object_version_states", e))?;
        let now = Utc::now().timestamp();
        let committed_generation = next_partition_generation(&mut partition_meta, now)?;
        let current_head = heads
            .get(canonical_key(object_key).as_str())
            .map_err(|e| map_indexdb_error("head get", e))?
            .map(|value| deserialize::<HeadRecord>(value.value()))
            .transpose()?;
        let prefix = version_row_prefix(object_key);
        let mut iter = states
            .range(prefix.as_str()..)
            .map_err(|e| map_indexdb_error("version_state range", e))?;
        let mut touched = Vec::new();
        let mut rewrites = Vec::new();
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
            touched.push(version_id.to_string());
            rewrites.push((
                row_key.to_string(),
                serialize(&VersionStateRecord {
                    state: to_state,
                    updated_at: now,
                    generation: committed_generation,
                })?,
            ));
        }

        if touched.is_empty() {
            return Ok(Vec::new());
        }

        for (row_key, bytes) in &rewrites {
            states
                .insert(row_key.as_str(), bytes.as_slice())
                .map_err(|e| map_indexdb_error("bulk version_state rewrite", e))?;
        }
        match current_head.as_ref() {
            Some(head) if head.state == from_state => {
                let next_head =
                    head.with_state(to_state, next_head_generation(current_head.as_ref()), now);
                write_head_exact(&mut heads, &next_head)?;
            }
            Some(_) => {}
            None => {
                return Err(AppError::InternalServerError(format!(
                    "indexdb missing head for {} while rewriting version visibility",
                    canonical_key(object_key)
                )));
            }
        }
        touched
    };
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit bulk version rewrite", e))?;
    let mut touched = touched;
    touched.sort_by(|left, right| compare_version_id_recency(left, right));
    Ok(touched)
}

fn delete_all_versions_for_key_on_db(
    db: &Database,
    object_key: &str,
) -> Result<Vec<String>, AppError> {
    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    let version_ids = {
        let mut partition_meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let mut heads = write_txn
            .open_table(OBJECT_HEADS_TABLE)
            .map_err(|e| map_indexdb_error("open object_heads", e))?;
        let mut versions = write_txn
            .open_table(OBJECT_VERSIONS_TABLE)
            .map_err(|e| map_indexdb_error("open object_versions", e))?;
        let mut states = write_txn
            .open_table(OBJECT_VERSION_STATES_TABLE)
            .map_err(|e| map_indexdb_error("open object_version_states", e))?;
        let now = Utc::now().timestamp();
        let _committed_generation = next_partition_generation(&mut partition_meta, now)?;
        let current_head = heads
            .get(canonical_key(object_key).as_str())
            .map_err(|e| map_indexdb_error("head get", e))?
            .map(|value| deserialize::<HeadRecord>(value.value()))
            .transpose()?;
        let prefix = version_row_prefix(object_key);
        let mut iter = versions
            .range(prefix.as_str()..)
            .map_err(|e| map_indexdb_error("version range", e))?;
        let mut row_keys = Vec::new();
        let mut version_ids = Vec::new();
        let mut latest_removed: Option<ObjectMetadata> = None;
        while let Some(entry) = iter.next() {
            let (row_key, value) = entry.map_err(|e| map_indexdb_error("version range next", e))?;
            let row_key = row_key.value();
            if !row_key.starts_with(&prefix) {
                break;
            }
            let record: VersionRecord = deserialize(value.value())?;
            match &latest_removed {
                Some(current) if !metadata_is_newer(&record.metadata, current) => {}
                _ => latest_removed = Some(record.metadata.clone()),
            }
            if let Some((_, version_id)) = parse_version_key(row_key) {
                version_ids.push(version_id.to_string());
            }
            row_keys.push(row_key.to_string());
        }
        if row_keys.is_empty() {
            return Ok(Vec::new());
        }
        for row_key in &row_keys {
            let _ = versions
                .remove(row_key.as_str())
                .map_err(|e| map_indexdb_error("version remove", e))?;
            let _ = states
                .remove(row_key.as_str())
                .map_err(|e| map_indexdb_error("version_state remove", e))?;
        }
        let fragment = current_head
            .as_ref()
            .map(|head| head.fragment.as_str())
            .or_else(|| {
                latest_removed
                    .as_ref()
                    .map(|metadata| metadata.fragment.as_str())
            })
            .unwrap_or_default();
        let tombstone = HeadRecord::tombstone(
            fragment,
            object_key,
            next_head_generation(current_head.as_ref()),
            now,
            current_head.as_ref(),
            latest_removed.as_ref(),
        );
        write_head_exact(&mut heads, &tombstone)?;
        version_ids
    };
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit delete object", e))?;
    let mut version_ids = version_ids;
    version_ids.sort_by(|left, right| compare_version_id_recency(left, right));
    Ok(version_ids)
}

fn session_receipts_from_state(
    state: &IndexSessionState,
    manifest: &UploadSessionManifest,
) -> Vec<Option<SessionPartReceipt>> {
    let mut receipts = vec![None; manifest.expected_parts as usize];
    for (part_index, receipt) in &state.part_receipts {
        if *part_index >= manifest.expected_parts {
            continue;
        }
        receipts[*part_index as usize] = Some(receipt.clone());
    }
    for (part_index, part) in &state.parts {
        if *part_index >= manifest.expected_parts {
            continue;
        }
        if receipts[*part_index as usize].is_some() {
            continue;
        }
        let mut shard_checksums = Vec::with_capacity(part.shards.len());
        for shard in &part.shards {
            let checksum = u32::from_str_radix(shard.checksum.trim(), 16).unwrap_or_default();
            shard_checksums.push(checksum);
        }
        let checksum = part
            .checksum
            .as_deref()
            .and_then(|value| u32::from_str_radix(value.trim(), 16).ok());
        receipts[*part_index as usize] = Some(SessionPartReceipt {
            part_index: *part_index,
            size_bytes: part.size_bytes,
            created_at: part.created_at,
            checksum,
            shard_checksums,
        });
    }
    receipts
}

fn load_session_exact(
    db: &Database,
    version_id: &str,
) -> Result<Option<IndexSessionState>, AppError> {
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

fn write_session_exact(
    db: &Database,
    version_id: &str,
    state: &IndexSessionState,
) -> Result<(), AppError> {
    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    {
        let mut meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let mut table = write_txn
            .open_table(SESSIONS_TABLE)
            .map_err(|e| map_indexdb_error("open sessions", e))?;
        let _ = next_partition_generation(&mut meta, Utc::now().timestamp())?;
        let bytes = serialize(state)?;
        table
            .insert(version_id, bytes.as_slice())
            .map_err(|e| map_indexdb_error("session insert", e))?;
    }
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit session write", e))?;
    Ok(())
}

fn remove_session_exact(db: &Database, version_id: &str) -> Result<bool, AppError> {
    let write_txn = db
        .begin_write()
        .map_err(|e| map_indexdb_error("begin_write", e))?;
    let removed = {
        let mut meta = write_txn
            .open_table(PARTITION_META_TABLE)
            .map_err(|e| map_indexdb_error("open partition_meta", e))?;
        let mut table = write_txn
            .open_table(SESSIONS_TABLE)
            .map_err(|e| map_indexdb_error("open sessions", e))?;
        let _ = next_partition_generation(&mut meta, Utc::now().timestamp())?;
        table
            .remove(version_id)
            .map_err(|e| map_indexdb_error("session remove", e))?
            .is_some()
    };
    write_txn
        .commit()
        .map_err(|e| map_indexdb_error("commit session remove", e))?;
    Ok(removed)
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

fn advance_partition(
    partition: &mut PartitionRange,
    prefix: Option<&str>,
    prefix_children: Option<&str>,
    include_internal: bool,
    after_key: Option<&str>,
) -> Result<(), AppError> {
    partition.next = None;
    while let Some(entry) = partition.iter.next() {
        let (key, value) = entry.map_err(|e| map_indexdb_error("head range next", e))?;
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
        let head: HeadRecord = deserialize(value.value())?;
        partition.next = Some(ListingItem {
            object_key: key.to_string(),
            head,
        });
        return Ok(());
    }
    Ok(())
}

fn load_partition_ranges(
    fragment: &str,
    prefix: Option<&str>,
    prefix_children: Option<&str>,
    include_internal: bool,
    after_key: Option<&str>,
) -> Result<Vec<PartitionRange>, AppError> {
    let start_hint = max_string(after_key, prefix);
    let mut out = Vec::new();
    for partition_id in 0..INDEX_PARTITION_COUNT {
        let Some(db) = open_partition_db_for_read(fragment, partition_id)? else {
            continue;
        };
        let read_txn = db
            .begin_read()
            .map_err(|e| map_indexdb_error("begin_read", e))?;
        let table = match read_txn.open_table(OBJECT_HEADS_TABLE) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => continue,
            Err(err) => return Err(map_indexdb_error("open object_heads", err)),
        };
        let start = start_hint.as_deref().unwrap_or("");
        let iter = table
            .range(start..)
            .map_err(|e| map_indexdb_error("head range", e))?;
        let mut partition = PartitionRange { iter, next: None };
        advance_partition(
            &mut partition,
            prefix,
            prefix_children,
            include_internal,
            after_key,
        )?;
        if partition.next.is_some() {
            out.push(partition);
        }
    }
    Ok(out)
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
    let Some(record) = read_version_exact(&db, &head.object_key, &head.version_id)? else {
        return Ok(None);
    };
    Ok(Some(record.metadata))
}

fn scan_heads_page(
    fragment: &str,
    state: ObjectState,
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
        if cursor.state != state
            || cursor.prefix != normalized_prefix
            || cursor.delimiter != delimiter
            || cursor.include_internal != include_internal
        {
            return Err(AppError::BadRequest(
                "listing continuation token does not match the current query".to_string(),
            ));
        }
    }

    let limit = limit.clamp(1, 1000);
    let after_key = cursor.as_ref().and_then(|value| value.last_key.as_deref());
    let mut skip_prefix = cursor
        .as_ref()
        .and_then(|value| value.resume_skip_prefix.clone());
    let mut partitions = load_partition_ranges(
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

        let item = partitions[idx]
            .next
            .take()
            .ok_or_else(|| AppError::InternalServerError("listing cursor desync".to_string()))?;
        last_key_for_cursor = Some(current_key.clone());

        if item.head.state != state {
            advance_partition(
                &mut partitions[idx],
                normalized_prefix.as_deref(),
                prefix_children.as_deref(),
                include_internal,
                after_key,
            )?;
            continue;
        }

        if let Some(prefix) = skip_prefix.clone() {
            if item.object_key.starts_with(&prefix) {
                advance_partition(
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
                    advance_partition(
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
            match load_current_metadata_for_head(fragment, &item.head)? {
                Some(metadata) => metadata,
                None => {
                    advance_partition(
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
            item.head.to_summary_metadata()
        };
        objects.push(metadata);
        if objects.len() + common_prefixes.len() >= limit {
            page_full = true;
            break;
        }

        advance_partition(
            &mut partitions[idx],
            normalized_prefix.as_deref(),
            prefix_children.as_deref(),
            include_internal,
            after_key,
        )?;
    }

    let continuation_token =
        if page_full || partitions.iter().any(|partition| partition.next.is_some()) {
            Some(encode_cursor(&ListingCursor {
                state,
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
    let state = IndexSessionState {
        manifest: manifest.clone(),
        reservation,
        parts: Default::default(),
        part_receipts: Default::default(),
        last_activity_at: manifest.created_at,
        finalized_at: None,
        aborted_at: None,
    };
    let owner_path = owner_db_path(&route, &manifest.fragment);
    with_partition_commit_lock(&owner_path, || {
        let owner_db = get_or_open_db(&owner_path)?;
        write_session_exact(&owner_db, &manifest.version_id, &state)?;
        replicate_owner_partition_db(&manifest.fragment, &route)?;
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
        let owner_db = get_or_open_db(&owner_path)?;
        let Some(mut state) = load_session_exact(&owner_db, version_id)? else {
            return Ok(());
        };
        state.parts.insert(part.part_index, part.clone());
        state.last_activity_at = part.created_at.max(state.last_activity_at);
        write_session_exact(&owner_db, version_id, &state)?;
        replicate_owner_partition_db(fragment, &route)?;
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
        let owner_db = get_or_open_db(&owner_path)?;
        let Some(mut state) = load_session_exact(&owner_db, version_id)? else {
            return Ok(());
        };
        state
            .part_receipts
            .insert(receipt.part_index, receipt.clone());
        state.last_activity_at = receipt.created_at.max(state.last_activity_at);
        write_session_exact(&owner_db, version_id, &state)?;
        replicate_owner_partition_db(fragment, &route)?;
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
    let Some(state) = load_session(fragment, version_id)? else {
        return Ok(None);
    };
    Ok(state.parts.get(&part_index).cloned())
}

pub fn load_session_part_receipts(
    fragment: &str,
    version_id: &str,
    manifest: &UploadSessionManifest,
) -> Result<Option<Vec<Option<SessionPartReceipt>>>, AppError> {
    let Some(state) = load_session(fragment, version_id)? else {
        return Ok(None);
    };
    let receipts = session_receipts_from_state(&state, manifest);
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
        let owner_db = get_or_open_db(&owner_path)?;
        let removed = remove_session_exact(&owner_db, version_id)?;
        if removed {
            replicate_owner_partition_db(fragment, &route)?;
        }
        Ok(removed)
    })
}

pub fn list_active_sessions(fragment: &str) -> Result<Vec<IndexSessionState>, AppError> {
    let mut out = Vec::new();
    for partition_id in 0..INDEX_PARTITION_COUNT {
        let Some(db) = open_partition_db_for_read(fragment, partition_id)? else {
            continue;
        };
        let read_txn = db
            .begin_read()
            .map_err(|e| map_indexdb_error("begin_read", e))?;
        let table = match read_txn.open_table(SESSIONS_TABLE) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => continue,
            Err(err) => return Err(map_indexdb_error("open sessions", err)),
        };
        let mut iter = table
            .iter()
            .map_err(|e| map_indexdb_error("session iter", e))?;
        while let Some(entry) = iter.next() {
            let (_key, value) = entry.map_err(|e| map_indexdb_error("session iter next", e))?;
            let state: IndexSessionState = deserialize(value.value())?;
            if state.finalized_at.is_none() && state.aborted_at.is_none() {
                out.push(state);
            }
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
        let owner_db = get_or_open_db(&owner_path)?;
        write_version_on_db(&owner_db, metadata, state)?;
        replicate_owner_partition_db(&metadata.fragment, &route)?;
        Ok(())
    })
}

pub fn upsert_live_version(metadata: &ObjectMetadata) -> Result<(), AppError> {
    write_version_on_owner(metadata, ObjectState::Live)
}

pub fn upsert_trash_version(metadata: &ObjectMetadata) -> Result<(), AppError> {
    write_version_on_owner(metadata, ObjectState::Trash)
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
    let Some(record) = read_version_exact(&db, object_key, version_id)? else {
        return Ok(None);
    };
    let Some(state) = read_version_state_exact(&db, object_key, version_id)? else {
        return Ok(None);
    };
    if state.state != ObjectState::Live {
        return Ok(None);
    }
    let head = read_head_exact(&db, object_key)?;
    if version_is_suppressed_by_tombstone(head.as_ref(), &record.metadata) {
        return Ok(None);
    }
    Ok(Some(record.metadata))
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
    let Some(record) = read_version_exact(&db, object_key, version_id)? else {
        return Ok(None);
    };
    let Some(state) = read_version_state_exact(&db, object_key, version_id)? else {
        return Ok(None);
    };
    if state.state != ObjectState::Trash {
        return Ok(None);
    }
    let head = read_head_exact(&db, object_key)?;
    if version_is_suppressed_by_tombstone(head.as_ref(), &record.metadata) {
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
    list_versions_exact(&db, object_key, ObjectState::Live)
}

pub fn list_trash_versions(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let route = object_route(fragment, object_key)?;
    let Some(db) = open_route_db_for_read(fragment, &route)? else {
        return Ok(Vec::new());
    };
    list_versions_exact(&db, object_key, ObjectState::Trash)
}

pub fn read_current_live_head(
    fragment: &str,
    object_key: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let route = object_route(fragment, object_key)?;
    let Some(db) = open_route_db_for_read(fragment, &route)? else {
        return Ok(None);
    };
    let Some(head) = read_head_exact(&db, object_key)? else {
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
    let route = object_route(fragment, object_key)?;
    let Some(db) = open_route_db_for_read(fragment, &route)? else {
        return Ok(None);
    };
    let Some(head) = read_head_exact(&db, object_key)? else {
        return Ok(None);
    };
    if head.state != ObjectState::Trash {
        return Ok(None);
    }
    load_current_metadata_for_head(fragment, &head)
}

pub fn scan_live_heads_page(
    fragment: &str,
    prefix: Option<String>,
    delimiter: Option<String>,
    limit: usize,
    continuation_token: Option<String>,
    include_internal: bool,
) -> Result<ListObjectsResult, AppError> {
    scan_heads_page(
        fragment,
        ObjectState::Live,
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
    scan_heads_page(
        fragment,
        ObjectState::Trash,
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
    scan_heads_page(
        fragment,
        ObjectState::Live,
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
    scan_heads_page(
        fragment,
        ObjectState::Trash,
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
    let mut continuation = None;
    let mut out = Vec::new();
    loop {
        let page = scan_trash_heads_page(fragment, None, None, 1000, continuation.clone(), true)?;
        out.extend(page.objects);
        if page.continuation_token.is_none() {
            break;
        }
        continuation = page.continuation_token;
    }
    Ok(out)
}

pub fn move_object_version_to_trash_with_metadata(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let route = object_route(fragment, object_key)?;
    let owner_path = owner_db_path(&route, fragment);
    with_partition_commit_lock(&owner_path, || {
        if !owner_path.exists() {
            return Ok(None);
        }
        let owner_db = get_or_open_db(&owner_path)?;
        let moved = change_version_state_on_db(
            &owner_db,
            object_key,
            version_id,
            ObjectState::Trash,
            Some(ObjectState::Live),
        )?;
        if moved.is_some() {
            replicate_owner_partition_db(fragment, &route)?;
        }
        Ok(moved)
    })
}

pub fn restore_object_version_from_trash_with_metadata(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let route = object_route(fragment, object_key)?;
    let owner_path = owner_db_path(&route, fragment);
    with_partition_commit_lock(&owner_path, || {
        if !owner_path.exists() {
            return Ok(None);
        }
        let owner_db = get_or_open_db(&owner_path)?;
        let restored = change_version_state_on_db(
            &owner_db,
            object_key,
            version_id,
            ObjectState::Live,
            Some(ObjectState::Trash),
        )?;
        if restored.is_some() {
            replicate_owner_partition_db(fragment, &route)?;
        }
        Ok(restored)
    })
}

pub fn move_object_to_trash_fast(
    fragment: &str,
    object_key: &str,
) -> Result<Option<Vec<String>>, AppError> {
    let route = object_route(fragment, object_key)?;
    let owner_path = owner_db_path(&route, fragment);
    with_partition_commit_lock(&owner_path, || {
        if !owner_path.exists() {
            return Ok(None);
        }
        let owner_db = get_or_open_db(&owner_path)?;
        let moved = bulk_rewrite_key_state_on_db(
            &owner_db,
            object_key,
            ObjectState::Live,
            ObjectState::Trash,
        )?;
        if moved.is_empty() {
            Ok(None)
        } else {
            replicate_owner_partition_db(fragment, &route)?;
            Ok(Some(moved))
        }
    })
}

pub fn restore_object_from_trash_fast(
    fragment: &str,
    object_key: &str,
) -> Result<Option<Vec<String>>, AppError> {
    let route = object_route(fragment, object_key)?;
    let owner_path = owner_db_path(&route, fragment);
    with_partition_commit_lock(&owner_path, || {
        if !owner_path.exists() {
            return Ok(None);
        }
        let owner_db = get_or_open_db(&owner_path)?;
        let restored = bulk_rewrite_key_state_on_db(
            &owner_db,
            object_key,
            ObjectState::Trash,
            ObjectState::Live,
        )?;
        if restored.is_empty() {
            Ok(None)
        } else {
            replicate_owner_partition_db(fragment, &route)?;
            Ok(Some(restored))
        }
    })
}

pub fn delete_object_fast(
    fragment: &str,
    object_key: &str,
) -> Result<Option<Vec<String>>, AppError> {
    let route = object_route(fragment, object_key)?;
    let owner_path = owner_db_path(&route, fragment);
    with_partition_commit_lock(&owner_path, || {
        if !owner_path.exists() {
            return Ok(None);
        }
        let owner_db = get_or_open_db(&owner_path)?;
        let deleted = delete_all_versions_for_key_on_db(&owner_db, object_key)?;
        if deleted.is_empty() {
            Ok(None)
        } else {
            replicate_owner_partition_db(fragment, &route)?;
            Ok(Some(deleted))
        }
    })
}

pub fn delete_live_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<bool, AppError> {
    let route = object_route(fragment, object_key)?;
    let owner_path = owner_db_path(&route, fragment);
    with_partition_commit_lock(&owner_path, || {
        if !owner_path.exists() {
            return Ok(false);
        }
        let owner_db = get_or_open_db(&owner_path)?;
        let deleted =
            delete_version_on_db(&owner_db, object_key, version_id, Some(ObjectState::Live))?
                .is_some();
        if deleted {
            replicate_owner_partition_db(fragment, &route)?;
        }
        Ok(deleted)
    })
}

pub fn delete_trash_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<bool, AppError> {
    let route = object_route(fragment, object_key)?;
    let owner_path = owner_db_path(&route, fragment);
    with_partition_commit_lock(&owner_path, || {
        if !owner_path.exists() {
            return Ok(false);
        }
        let owner_db = get_or_open_db(&owner_path)?;
        let deleted =
            delete_version_on_db(&owner_db, object_key, version_id, Some(ObjectState::Trash))?
                .is_some();
        if deleted {
            replicate_owner_partition_db(fragment, &route)?;
        }
        Ok(deleted)
    })
}

pub fn clear_fragment_trash(fragment: &str, object_key: Option<&str>) -> Result<(), AppError> {
    if let Some(object_key) = object_key {
        for metadata in list_trash_versions(fragment, object_key)? {
            let _ = delete_trash_version(fragment, object_key, &metadata.version_id)?;
        }
        return Ok(());
    }

    for partition_id in 0..INDEX_PARTITION_COUNT {
        let Some(db) = open_partition_db_for_read(fragment, partition_id)? else {
            continue;
        };
        let read_txn = db
            .begin_read()
            .map_err(|e| map_indexdb_error("begin_read", e))?;
        let table = match read_txn.open_table(OBJECT_HEADS_TABLE) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => continue,
            Err(err) => return Err(map_indexdb_error("open object_heads", err)),
        };
        let mut iter = table
            .iter()
            .map_err(|e| map_indexdb_error("head iter", e))?;
        let mut trash_keys = Vec::new();
        while let Some(entry) = iter.next() {
            let (_key, value) = entry.map_err(|e| map_indexdb_error("head iter next", e))?;
            let head: HeadRecord = deserialize(value.value())?;
            if head.state == ObjectState::Trash {
                trash_keys.push(head.object_key);
            }
        }
        for object_key in trash_keys {
            for metadata in list_trash_versions(fragment, &object_key)? {
                let _ = delete_trash_version(fragment, &object_key, &metadata.version_id)?;
            }
        }
    }
    Ok(())
}
